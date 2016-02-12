/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.daemon.impl;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.FunctionTask;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.FunctionInfo.FunctionResource;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState.ResourceType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.util.ResourceDownloader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class localizes and manages jars for the functions allowed inside LLAP.
 */
public class FunctionLocalizer implements GenericUDFBridge.UdfWhitelistChecker {
  private static final String DIR_NAME = "fnresources";
  private static final Logger LOG = LoggerFactory.getLogger(FunctionLocalizer.class);
  private ResourceDownloader resourceDownloader;
  private final LinkedBlockingQueue<LocalizerWork> workQueue = new LinkedBlockingQueue<>();
  private volatile boolean isClosed = false;
  private final List<String> recentlyLocalizedJars = new LinkedList<String>();
  private final List<String> recentlyLocalizedClasses = new LinkedList<String>();
  private final Thread workThread;
  private final File localDir;
  private final Configuration conf;
  private final URLClassLoader executorClassloader;


  private final IdentityHashMap<Class<?>, Boolean> allowedUdfClasses = new IdentityHashMap<>();
  private final ConcurrentHashMap<String, FnResources> resourcesByFn = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<URI, RefCountedResource> localFiles = new ConcurrentHashMap<>();

  public FunctionLocalizer(Configuration conf, String localDir) {
    this.conf = conf;
    this.localDir = new File(localDir, DIR_NAME);
    this.executorClassloader = (URLClassLoader)Utilities.createUDFClassLoader(
        (URLClassLoader)Thread.currentThread().getContextClassLoader(), new String[]{});
    this.workThread = new Thread(new Runnable() {
      @Override
      public void run() {
        runWorkThread();
      }
    });
  }

  public void init() throws IOException {
    if (localDir.exists()) {
      // TODO: We don't want some random jars of unknown provenance sitting around. Or do we care?
      //       Ideally, we should try to reuse jars and verify using some checksum.
      FileUtils.deleteDirectory(localDir);
    }
    this.resourceDownloader = new ResourceDownloader(conf, localDir.getAbsolutePath());
    workThread.start();
  }

  public boolean isUdfAllowed(Class<?> clazz) {
    return FunctionRegistry.isBuiltInFuncClass(clazz) || allowedUdfClasses.containsKey(clazz);
  }

  public ClassLoader getClassLoader() {
    return executorClassloader;
  }

  public void startLocalizeAllFunctions() throws HiveException {
    Hive hive = Hive.get(false);
    // Do not allow embedded metastore in LLAP unless we are in test.
    try {
      hive.getMSC(HiveConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST), true);
    } catch (MetaException e) {
      throw new HiveException(e);
    }
    List<Function> fns = hive.getAllFunctions();
    for (Function fn : fns) {
      String fqfn = fn.getDbName() + "." + fn.getFunctionName();
      List<ResourceUri> resources = fn.getResourceUris();
      if (resources == null || resources.isEmpty()) continue; // Nothing to localize.
      FnResources result = new FnResources();
      resourcesByFn.put(fqfn, result);
      workQueue.add(new LocalizeFn(fqfn, resources, result, fn.getClassName(), false));
    }
    workQueue.add(new RefreshClassloader());
  }

  public void close() {
    isClosed = true;
    workThread.interrupt();
    try {
      workThread.join(1000); // Give it some time, then don't delay shutdown too much.
    } catch (InterruptedException e) {
      LOG.info("Interrupted during close");
    }
  }

  private void runWorkThread() {
    while (true) {
      if (isClosed) {
        deleteAllLocalResources();
        return;
      }
      LocalizerWork lw = null;
      try {
        lw = workQueue.take();
      } catch (InterruptedException ex) {
        LOG.debug("Localizer thread interrupted");
        isClosed = true;
      }
      if (isClosed) {
        deleteAllLocalResources();
        return;
      }
      try {
        lw.run(this);
      } catch (InterruptedException ex) {
        LOG.debug("Localizer thread interrupted");
        isClosed = true;
      } catch (Exception ex) {
        LOG.error("Failed to run " + lw, ex);
      }
    }
  }

  private interface LocalizerWork {
    void run(FunctionLocalizer parent)
        throws URISyntaxException, IOException, InterruptedException;
  }

  private static class LocalizeFn implements LocalizerWork {
    private final List<ResourceUri> resources;
    private final FnResources result;
    private final String fqfn;
    private final boolean doRefreshClassloader;
    private final String className;
    public LocalizeFn(String fqfn, List<ResourceUri> resources, FnResources result,
        String className, boolean doRefreshClassloader) {
      this.resources = resources;
      this.result = result;
      this.fqfn = fqfn;
      this.className = className;
      this.doRefreshClassloader = doRefreshClassloader;
    }

    public void run(FunctionLocalizer parent) throws URISyntaxException, IOException {
      parent.localizeFunctionResources(fqfn, resources, className, result, doRefreshClassloader);
    }

    public String toString() {
      return "localize " + resources.size() + " resources for " + fqfn;
    }
  }

  private static class RefreshClassloader implements LocalizerWork {
    public void run(FunctionLocalizer parent) throws URISyntaxException, IOException {
      parent.refreshClassloader();
    }

    public String toString() {
      return "load the recently localized jars";
    }
  }

  private void deleteAllLocalResources() {
    try {
      executorClassloader.close();
    } catch (Exception ex) {
      LOG.info("Failed to close the classloader", ex.getMessage());
    }
    resourcesByFn.clear();
    for (RefCountedResource rcr : localFiles.values()) {
      for (FunctionResource fr : rcr.resources) {
        // We ignore refcounts (and errors) for now.
        File file = new File(fr.getResourceURI());
        try {
          if (!file.delete()) {
            LOG.info("Failed to delete " + file);
          }
        } catch (Exception ex) {
          LOG.info("Failed to delete " + file + ": " + ex.getMessage());
        }
      }
    }
  }

  public void refreshClassloader() throws IOException {
    if (recentlyLocalizedJars.isEmpty()) return;
    String[] jars = recentlyLocalizedJars.toArray(new String[0]);
    recentlyLocalizedJars.clear();
    ClassLoader updatedCl = null;
    try {
      updatedCl = Utilities.addToClassPath(executorClassloader, jars);
      if (LOG.isInfoEnabled()) {
        LOG.info("Added " + jars.length + " jars to classpath");
      }
    } catch (Throwable t) {
      // TODO: we could fall back to trying one by one and only ignore the failed ones.
      logRefreshError("Unable to localize jars: ", jars, t);
      return; // logRefreshError always throws.
    }
    if (updatedCl != executorClassloader) {
      throw new AssertionError("Classloader was replaced despite using UDFClassLoader: new "
          + updatedCl + ", old " + executorClassloader);
    }
    String[] classNames = recentlyLocalizedClasses.toArray(jars);
    recentlyLocalizedClasses.clear();
    try {
      for (String className : classNames) {
        allowedUdfClasses.put(Class.forName(className, false, executorClassloader), Boolean.TRUE);
      }
    } catch (Throwable t) {
      // TODO: we could fall back to trying one by one and only ignore the failed ones.
      logRefreshError("Unable to instantiate localized classes: ", classNames, t);
      return;  // logRefreshError always throws.
    }
  }

  private void logRefreshError(String what, String[] items, Throwable t) throws IOException {
    for (String item : items) {
      what += (item + ", ");
    }
    throw new IOException(what, t);
  }

  private void localizeFunctionResources(String fqfn, List<ResourceUri> resources,
      String className, FnResources result, boolean doRefreshClassloader) throws URISyntaxException, IOException {
    // We will download into fn-scoped subdirectories to avoid name collisions (we assume there
    // are no collisions within the same fn). That doesn't mean we download for every fn.
    if (LOG.isInfoEnabled()) {
      LOG.info("Localizing " + resources.size() + " resources for " + fqfn);
    }
    for (ResourceUri resource : resources) {
      URI srcUri = ResourceDownloader.createURI(resource.getUri());
      ResourceType rt = FunctionTask.getResourceType(resource.getResourceType());
      localizeOneResource(fqfn, srcUri, rt, result);
    }
    recentlyLocalizedClasses.add(className);
    if (doRefreshClassloader) {
      refreshClassloader();
    }
  }

  private void localizeOneResource(String fqfn, URI srcUri, ResourceType rt, FnResources result)
      throws URISyntaxException, IOException {
    RefCountedResource rcr = localFiles.get(srcUri);
    if (rcr != null && rcr.refCount > 0) {
      logFilesUsed("Reusing", fqfn, srcUri, rcr);
      ++rcr.refCount;
      result.addResources(rcr);
      return;
    }
    rcr = new RefCountedResource();
    List<URI> localUris = resourceDownloader.downloadExternal(srcUri, fqfn, false);
    if (localUris == null || localUris.isEmpty()) {
      LOG.error("Cannot download " + srcUri + " for " + fqfn);
      return;
    }
    rcr.resources = new ArrayList<>();
    for (URI uri : localUris) {
      // Reuse the same type for all. Only Ivy can return more than one, probably all jars.
      String path = uri.getPath();
      rcr.resources.add(new FunctionResource(rt, path));
      if (rt == ResourceType.JAR) {
        recentlyLocalizedJars.add(path);
      }
    }
    ++rcr.refCount;
    logFilesUsed("Using", fqfn, srcUri, rcr);
    localFiles.put(srcUri, rcr);
    result.addResources(rcr);
  }

  private void logFilesUsed(String what, String fqfn, URI srcUri, RefCountedResource rcr) {
    if (!LOG.isInfoEnabled()) return;
    String desc = (rcr.resources.size() == 1
        ? rcr.resources.get(0).toString() : (rcr.resources.size() + " files"));
    LOG.info(what + " files [" + desc + "] for [" + srcUri + "] resource for " + fqfn);
  }

  private static class RefCountedResource {
    List<FunctionResource> resources;
    int refCount = 0;
  }

  private static class FnResources {
    final List<FunctionResource> localResources = new ArrayList<>();
    final List<RefCountedResource> originals = new ArrayList<>();
    public void addResources(RefCountedResource rcr) {
      localResources.addAll(rcr.resources);
      originals.add(rcr);
    }
  }
}
