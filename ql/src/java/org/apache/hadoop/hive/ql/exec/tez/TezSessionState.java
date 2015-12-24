/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.tez;


import java.util.Collection;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.security.auth.login.LoginException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.llap.security.LlapTokenIdentifier;
import org.apache.hadoop.hive.llap.security.LlapTokenProvider;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.PreWarmVertex;
import org.apache.tez.dag.api.SessionNotRunning;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.serviceplugins.api.ContainerLauncherDescriptor;
import org.apache.tez.serviceplugins.api.ServicePluginsDescriptor;
import org.apache.tez.serviceplugins.api.TaskCommunicatorDescriptor;
import org.apache.tez.serviceplugins.api.TaskSchedulerDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds session state related to Tez
 */
public class TezSessionState {

  private static final Logger LOG = LoggerFactory.getLogger(TezSessionState.class.getName());
  private static final String TEZ_DIR = "_tez_session_dir";
  public static final String LLAP_SERVICE = "LLAP";
  private static final String LLAP_SCHEDULER = "org.apache.tez.dag.app.rm.LlapTaskSchedulerService";
  private static final String LLAP_LAUNCHER = "org.apache.hadoop.hive.llap.tezplugins.LlapContainerLauncher";
  private static final String LLAP_TASK_COMMUNICATOR = "org.apache.hadoop.hive.llap.tezplugins.LlapTaskCommunicator";

  private HiveConf conf;
  private Path tezScratchDir;
  private LocalResource appJarLr;
  private TezClient session;
  private Future<TezClient> sessionFuture;
  /** Console used for user feedback during async session opening. */
  private LogHelper console;
  private String sessionId;
  private final DagUtils utils;
  private String queueName;
  private boolean defaultQueue = false;
  private String user;

  private final Set<String> additionalFilesNotFromConf = new HashSet<String>();
  private final Set<LocalResource> localizedResources = new HashSet<LocalResource>();
  private boolean doAsEnabled;

  /**
   * Constructor. We do not automatically connect, because we only want to
   * load tez classes when the user has tez installed.
   */
  public TezSessionState(DagUtils utils) {
    this.utils = utils;
  }

  public String toString() {
    return "sessionId=" + sessionId + ", queueName=" + queueName + ", user=" + user
        + ", doAs=" + doAsEnabled + ", isOpen=" + isOpen();
  }

  /**
   * Constructor. We do not automatically connect, because we only want to
   * load tez classes when the user has tez installed.
   */
  public TezSessionState(String sessionId) {
    this(DagUtils.getInstance());
    this.sessionId = sessionId;
  }

  public boolean isOpening() {
    if (session != null || sessionFuture == null) return false;
    try {
      session = sessionFuture.get(0, TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    } catch (CancellationException e) {
      return false;
    } catch (TimeoutException e) {
      return true;
    }
    return false;
  }

  public boolean isOpen() {
    if (session != null) return true;
    if (sessionFuture == null) return false;
    try {
      session = sessionFuture.get(0, TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    } catch (TimeoutException | CancellationException e) {
      return false;
    }
    return true;
  }


  /**
   * Get all open sessions. Only used to clean up at shutdown.
   * @return List<TezSessionState>
   */
  public static String makeSessionId() {
    return UUID.randomUUID().toString();
  }

  public void open(HiveConf conf)
      throws IOException, LoginException, URISyntaxException, TezException {
    Set<String> noFiles = null;
    open(conf, noFiles, null);
  }

  /**
   * Creates a tez session. A session is tied to either a cli/hs2 session. You can
   * submit multiple DAGs against a session (as long as they are executed serially).
   * @throws IOException
   * @throws URISyntaxException
   * @throws LoginException
   * @throws TezException
   * @throws InterruptedException
   */
  public void open(HiveConf conf, String[] additionalFiles)
    throws IOException, LoginException, IllegalArgumentException, URISyntaxException, TezException {
    openInternal(conf, setFromArray(additionalFiles), false, null, null);
  }

  private static Set<String> setFromArray(String[] additionalFiles) {
    if (additionalFiles == null) return null;
    Set<String> files = new HashSet<>();
    for (String originalFile : additionalFiles) {
      files.add(originalFile);
    }
    return files;
  }

  public void beginOpen(HiveConf conf, String[] additionalFiles, LogHelper console)
    throws IOException, LoginException, IllegalArgumentException, URISyntaxException, TezException {
    openInternal(conf, setFromArray(additionalFiles), true, console, null);
  }

  public void open(HiveConf conf, Collection<String> additionalFiles, Path scratchDir)
      throws LoginException, IOException, URISyntaxException, TezException {
    openInternal(conf, additionalFiles, false, null, scratchDir);
  }

  protected void openInternal(final HiveConf conf, Collection<String> additionalFiles,
      boolean isAsync, LogHelper console, Path scratchDir) throws IOException, LoginException,
        IllegalArgumentException, URISyntaxException, TezException {
    this.conf = conf;
    this.queueName = conf.get("tez.queue.name");
    this.doAsEnabled = conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS);

    final boolean llapMode = "llap".equals(HiveConf.getVar(
        conf, HiveConf.ConfVars.HIVE_EXECUTION_MODE));

    UserGroupInformation ugi = Utils.getUGI();
    user = ugi.getShortUserName();
    LOG.info("User of session id " + sessionId + " is " + user);

    // create the tez tmp dir
    tezScratchDir = scratchDir == null ? createTezDir(sessionId) : scratchDir;

    additionalFilesNotFromConf.clear();
    if (additionalFiles != null) {
      additionalFilesNotFromConf.addAll(additionalFiles);
    }

    refreshLocalResourcesFromConf(conf);

    // unless already installed on all the cluster nodes, we'll have to
    // localize hive-exec.jar as well.
    appJarLr = createJarLocalResource(utils.getExecJarPathLocal());

    // configuration for the application master
    final Map<String, LocalResource> commonLocalResources = new HashMap<String, LocalResource>();
    commonLocalResources.put(utils.getBaseName(appJarLr), appJarLr);
    for (LocalResource lr : localizedResources) {
      commonLocalResources.put(utils.getBaseName(lr), lr);
    }

    if (llapMode) {
      // add configs for llap-daemon-site.xml + localize llap jars
      // they cannot be referred to directly as it would be a circular depedency
      conf.addResource("llap-daemon-site.xml");
      try {
        final File daemonJar =
            new File(Utilities.jarFinderGetJar(Class
                .forName("org.apache.hadoop.hive.llap.io.api.impl.LlapInputFormat")));
        final LocalResource daemonLr =
            createJarLocalResource(daemonJar.toURI().toURL().toExternalForm());
        commonLocalResources.put(utils.getBaseName(daemonLr), daemonLr);
      } catch (ClassNotFoundException ce) {
        throw new IOException("Cannot find LlapInputFormat in the classpath", ce);
      }

      try {
        final File registryJar =
            new File(Utilities.jarFinderGetJar(Class
                .forName("org.apache.hadoop.registry.client.api.RegistryOperations")));
        final LocalResource registryLr =
            createJarLocalResource(registryJar.toURI().toURL().toExternalForm());
        commonLocalResources.put(utils.getBaseName(registryLr), registryLr);
      } catch (ClassNotFoundException ce) {
        throw new IOException("Cannot find Hadoop Registry in the classpath", ce);
      }
    }

    // Create environment for AM.
    Map<String, String> amEnv = new HashMap<String, String>();
    MRHelpers.updateEnvBasedOnMRAMEnv(conf, amEnv);

    // and finally we're ready to create and start the session
    // generate basic tez config
    final TezConfiguration tezConfig = new TezConfiguration(conf);

    // set up the staging directory to use
    tezConfig.set(TezConfiguration.TEZ_AM_STAGING_DIR, tezScratchDir.toUri().toString());
    conf.stripHiddenConfigurations(tezConfig);

    ServicePluginsDescriptor servicePluginsDescriptor;

    Credentials llapCredentials = null;
    if (llapMode) {
      if (UserGroupInformation.isSecurityEnabled()) {
        LlapTokenProvider tp = LlapProxy.getOrInitTokenProvider(conf);
        Token<LlapTokenIdentifier> token = tp.getDelegationToken();
        LOG.info("Obtained a token: " + token);
        llapCredentials = new Credentials();
        llapCredentials.addToken(LlapTokenIdentifier.KIND_NAME, token);
      }
      UserPayload servicePluginPayload = TezUtils.createUserPayloadFromConf(tezConfig);
      // we need plugins to handle llap and uber mode
      servicePluginsDescriptor = ServicePluginsDescriptor.create(true,
          new TaskSchedulerDescriptor[] { TaskSchedulerDescriptor.create(
              LLAP_SERVICE, LLAP_SCHEDULER).setUserPayload(servicePluginPayload) },
          new ContainerLauncherDescriptor[] { ContainerLauncherDescriptor.create(
              LLAP_SERVICE, LLAP_LAUNCHER) },
          new TaskCommunicatorDescriptor[] { TaskCommunicatorDescriptor.create(
              LLAP_SERVICE, LLAP_TASK_COMMUNICATOR).setUserPayload(servicePluginPayload) });
    } else {
      servicePluginsDescriptor = ServicePluginsDescriptor.create(true);
    }

    // container prewarming. tell the am how many containers we need
    if (HiveConf.getBoolVar(conf, ConfVars.HIVE_PREWARM_ENABLED)) {
      int n = HiveConf.getIntVar(conf, ConfVars.HIVE_PREWARM_NUM_CONTAINERS);
      n = Math.max(tezConfig.getInt(
          TezConfiguration.TEZ_AM_SESSION_MIN_HELD_CONTAINERS,
          TezConfiguration.TEZ_AM_SESSION_MIN_HELD_CONTAINERS_DEFAULT), n);
      tezConfig.setInt(TezConfiguration.TEZ_AM_SESSION_MIN_HELD_CONTAINERS, n);
    }

    final TezClient session = TezClient.newBuilder("HIVE-" + sessionId, tezConfig)
        .setIsSession(true).setLocalResources(commonLocalResources)
        .setCredentials(llapCredentials).setServicePluginDescriptor(servicePluginsDescriptor)
        .build();

    LOG.info("Opening new Tez Session (id: " + sessionId
        + ", scratch dir: " + tezScratchDir + ")");

    TezJobMonitor.initShutdownHook();
    if (!isAsync) {
      startSessionAndContainers(session, conf, commonLocalResources, tezConfig, false);
      this.session = session;
    } else {
      FutureTask<TezClient> sessionFuture = new FutureTask<>(new Callable<TezClient>() {
        @Override
        public TezClient call() throws Exception {
          try {
            return startSessionAndContainers(session, conf, commonLocalResources, tezConfig, true);
          } catch (Throwable t) {
            LOG.error("Failed to start Tez session", t);
            throw (t instanceof Exception) ? (Exception)t : new Exception(t);
          }
        }
      });
      new Thread(sessionFuture, "Tez session start thread").start();
      // We assume here nobody will try to get session before open() returns.
      this.console = console;
      this.sessionFuture = sessionFuture;
    }
  }

  private TezClient startSessionAndContainers(TezClient session, HiveConf conf,
      Map<String, LocalResource> commonLocalResources, TezConfiguration tezConfig,
      boolean isOnThread) throws TezException, IOException {
    session.start();
    boolean isSuccessful = false;
    try {
      if (HiveConf.getBoolVar(conf, ConfVars.HIVE_PREWARM_ENABLED)) {
        int n = HiveConf.getIntVar(conf, ConfVars.HIVE_PREWARM_NUM_CONTAINERS);
        LOG.info("Prewarming " + n + " containers  (id: " + sessionId
            + ", scratch dir: " + tezScratchDir + ")");
        PreWarmVertex prewarmVertex = utils.createPreWarmVertex(
            tezConfig, n, commonLocalResources);
        try {
          session.preWarm(prewarmVertex);
        } catch (IOException ie) {
          if (!isOnThread && ie.getMessage().contains("Interrupted while waiting")) {
            LOG.warn("Hive Prewarm threw an exception ", ie);
          } else {
            throw ie;
          }
        }
      }
      try {
        session.waitTillReady();
      } catch (InterruptedException ie) {
        if (isOnThread) throw new IOException(ie);
        //ignore
      }
      isSuccessful = true;
      return session;
    } finally {
      if (isOnThread && !isSuccessful) {
        closeAndIgnoreExceptions(session);
      }
    }
  }

  private static void closeAndIgnoreExceptions(TezClient session) {
    try {
      session.stop();
    } catch (SessionNotRunning nr) {
      // Ignore.
    } catch (IOException | TezException ex) {
      LOG.info("Failed to close Tez session after failure to initialize: " + ex.getMessage());
    }
  }

  public void endOpen() throws InterruptedException, CancellationException {
    if (this.session != null || this.sessionFuture == null) return;
    try {
      this.session = this.sessionFuture.get();
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public void refreshLocalResourcesFromConf(HiveConf conf)
    throws IOException, LoginException, IllegalArgumentException, URISyntaxException, TezException {

    String dir = tezScratchDir.toString();

    localizedResources.clear();

    // these are local resources set through add file, jar, etc
    List<LocalResource> lrs = utils.localizeTempFilesFromConf(dir, conf);
    if (lrs != null) {
      localizedResources.addAll(lrs);
    }

    // these are local resources that are set through the mr "tmpjars" property
    List<LocalResource> handlerLr = utils.localizeTempFiles(dir, conf,
      additionalFilesNotFromConf.toArray(new String[additionalFilesNotFromConf.size()]));

    if (handlerLr != null) {
      localizedResources.addAll(handlerLr);
    }
  }

  public boolean hasResources(String[] localAmResources) {
    if (localAmResources == null || localAmResources.length == 0) return true;
    if (additionalFilesNotFromConf.isEmpty()) return false;
    for (String s : localAmResources) {
      if (!additionalFilesNotFromConf.contains(s)) return false;
    }
    return true;
  }

  /**
   * Close a tez session. Will cleanup any tez/am related resources. After closing a session no
   * further DAGs can be executed against it.
   *
   * @param keepTmpDir
   *          whether or not to remove the scratch dir at the same time.
   * @throws Exception
   */
  public void close(boolean keepTmpDir) throws Exception {
    if (session != null) {
      LOG.info("Closing Tez Session");
      closeClient(session);
    } else if (sessionFuture != null) {
      sessionFuture.cancel(true);
      TezClient asyncSession = null;
      try {
        asyncSession = sessionFuture.get(); // In case it was done and noone looked at it.
      } catch (ExecutionException | CancellationException e) {
        // ignore
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // ignore
      }
      if (asyncSession != null) {
        LOG.info("Closing Tez Session");
        closeClient(asyncSession);
      }
    }

    if (!keepTmpDir) {
      cleanupScratchDir();
    }
    session = null;
    sessionFuture = null;
    console = null;
    tezScratchDir = null;
    conf = null;
    appJarLr = null;
    additionalFilesNotFromConf.clear();
    localizedResources.clear();
  }

  public Set<String> getAdditionalFilesNotFromConf() {
    return additionalFilesNotFromConf;
  }

  private void closeClient(TezClient client) throws TezException,
      IOException {
    try {
      client.stop();
    } catch (SessionNotRunning nr) {
      // ignore
    }
  }

  public void cleanupScratchDir () throws IOException {
    FileSystem fs = tezScratchDir.getFileSystem(conf);
    fs.delete(tezScratchDir, true);
    tezScratchDir = null;
  }

  public String getSessionId() {
    return sessionId;
  }

  public TezClient getSession() {
    if (session == null && sessionFuture != null) {
      if (!sessionFuture.isDone()) {
        console.printInfo("Waiting for Tez session and AM to be ready...");
      }
      try {
        session = sessionFuture.get();
      } catch (InterruptedException e) {
        console.printInfo("Interrupted while waiting for the session");
        Thread.currentThread().interrupt();
        return null;
      } catch (ExecutionException e) {
        console.printInfo("Failed to get session");
        throw new RuntimeException(e);
      } catch (CancellationException e) {
        console.printInfo("Cancelled while waiting for the session");
        return null;
      }
    }
    return session;
  }

  public Path getTezScratchDir() {
    return tezScratchDir;
  }

  public LocalResource getAppJarLr() {
    return appJarLr;
  }

  /**
   * createTezDir creates a temporary directory in the scratchDir folder to
   * be used with Tez. Assumes scratchDir exists.
   */
  private Path createTezDir(String sessionId) throws IOException {
    // tez needs its own scratch dir (per session)
    Path tezDir = new Path(SessionState.get().getHdfsScratchDirURIString(), TEZ_DIR);
    tezDir = new Path(tezDir, sessionId);
    FileSystem fs = tezDir.getFileSystem(conf);
    FsPermission fsPermission = new FsPermission(HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCHDIRPERMISSION));
    fs.mkdirs(tezDir, fsPermission);
    // Make sure the path is normalized (we expect validation to pass since we just created it).
    tezDir = DagUtils.validateTargetDir(tezDir, conf).getPath();
    // don't keep the directory around on non-clean exit
    fs.deleteOnExit(tezDir);

    return tezDir;
  }

  /**
   * Returns a local resource representing a jar.
   * This resource will be used to execute the plan on the cluster.
   * @param localJarPath Local path to the jar to be localized.
   * @return LocalResource corresponding to the localized hive exec resource.
   * @throws IOException when any file system related call fails.
   * @throws LoginException when we are unable to determine the user.
   * @throws URISyntaxException when current jar location cannot be determined.
   */
  private LocalResource createJarLocalResource(String localJarPath)
      throws IOException, LoginException, IllegalArgumentException,
      FileNotFoundException {
    FileStatus destDirStatus = utils.getHiveJarDirectory(conf);
    assert destDirStatus != null;
    Path destDirPath = destDirStatus.getPath();

    Path localFile = new Path(localJarPath);
    String sha = getSha(localFile);

    String destFileName = localFile.getName();

    // Now, try to find the file based on SHA and name. Currently we require exact name match.
    // We could also allow cutting off versions and other stuff provided that SHA matches...
    destFileName = FilenameUtils.removeExtension(destFileName) + "-" + sha
        + FilenameUtils.EXTENSION_SEPARATOR + FilenameUtils.getExtension(destFileName);

    if (LOG.isDebugEnabled()) {
      LOG.debug("The destination file name for [" + localJarPath + "] is " + destFileName);
    }

    // TODO: if this method is ever called on more than one jar, getting the dir and the
    //       list need to be refactored out to be done only once.
    Path destFile = new Path(destDirPath.toString() + "/" + destFileName);
    return utils.localizeResource(localFile, destFile, LocalResourceType.FILE, conf);
  }

  private String getSha(Path localFile) throws IOException, IllegalArgumentException {
    InputStream is = null;
    try {
      FileSystem localFs = FileSystem.getLocal(conf);
      is = localFs.open(localFile);
      return DigestUtils.sha256Hex(is);
    } finally {
      if (is != null) {
        is.close();
      }
    }
  }
  public void setQueueName(String queueName) {
    this.queueName = queueName;
  }

  public String getQueueName() {
    return queueName;
  }

  public void setDefault() {
    defaultQueue  = true;
  }

  public boolean isDefault() {
    return defaultQueue;
  }

  public HiveConf getConf() {
    return conf;
  }

  public List<LocalResource> getLocalizedResources() {
    return new ArrayList<LocalResource>(localizedResources);
  }

  public String getUser() {
    return user;
  }

  public boolean getDoAsEnabled() {
    return doAsEnabled;
  }
}
