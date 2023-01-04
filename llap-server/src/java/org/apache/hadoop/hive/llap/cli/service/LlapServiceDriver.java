/*
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

package org.apache.hadoop.hive.llap.cli.service;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfUtil;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.llap.cli.LlapSliderUtils;
import org.apache.hadoop.hive.llap.configuration.LlapDaemonConfiguration;
import org.apache.hadoop.hive.llap.daemon.impl.LlapConstants;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.client.ServiceClient;
import org.apache.hadoop.yarn.service.utils.CoreFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Starts the llap daemon. */
public class LlapServiceDriver {
  private static final Logger LOG = LoggerFactory.getLogger(LlapServiceDriver.class.getName());

  private static final String LLAP_RELATIVE_PACKAGE_DIR = "/package/LLAP/";
  private static final String OUTPUT_DIR_PREFIX = "llap-yarn-";

  /**
   * This is a working configuration for the instance to merge various variables.
   * It is not written out for llap server usage
   */
  private final HiveConf conf;
  private final LlapServiceCommandLine cl;

  public LlapServiceDriver(LlapServiceCommandLine cl) throws Exception {
    this.cl = cl;

    SessionState ss = SessionState.get();
    this.conf = (ss != null) ? ss.getConf() : new HiveConf(SessionState.class);

    HiveConfUtil.copyFromProperties(cl.getConfig(), this.conf);

    if (conf == null) {
      throw new Exception("Cannot load any configuration to run command");
    }
  }

  private int run() throws Exception {
    Properties propsDirectOptions = new Properties();

    // Working directory.
    Path tmpDir = new Path(cl.getDirectory());

    long t0 = System.nanoTime();

    FileSystem fs = FileSystem.get(conf);
    FileSystem rawFs = FileSystem.getLocal(conf).getRawFileSystem();

    int threadCount = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
    ExecutorService executor = Executors.newFixedThreadPool(threadCount,
            new ThreadFactoryBuilder().setNameFormat("llap-pkg-%d").build());

    int rc = 0;
    try {

      setupConf(propsDirectOptions);

      URL logger = conf.getResource(LlapConstants.LOG4j2_PROPERTIES_FILE);
      if (logger == null) {
        throw new Exception("Unable to find required config file: llap-daemon-log4j2.properties");
      }

      Path home = new Path(System.getenv("HIVE_HOME"));
      Path scriptParent = new Path(new Path(home, "scripts"), "llap");
      Path scripts = new Path(scriptParent, "bin");

      if (!rawFs.exists(home)) {
        throw new Exception("Unable to find HIVE_HOME:" + home);
      } else if (!rawFs.exists(scripts)) {
        LOG.warn("Unable to find llap scripts:" + scripts);
      }

      String javaHome = getJavaHome();

      LlapTarComponentGatherer tarComponentGatherer = new LlapTarComponentGatherer(cl, conf, propsDirectOptions,
          fs, rawFs, executor, tmpDir);
      tarComponentGatherer.createDirs();
      tarComponentGatherer.submitTarComponentGatherTasks();

      // TODO: need to move from Python to Java for the rest of the script.
      LlapConfigJsonCreator lcjCreator = new LlapConfigJsonCreator(conf, rawFs, tmpDir, cl.getCache(), cl.getXmx(),
          javaHome);
      lcjCreator.createLlapConfigJson();

      LOG.debug("Config Json generation took " + (System.nanoTime() - t0) + " ns");

      tarComponentGatherer.waitForFinish();

      if (cl.isStarting()) {
        rc = startLlap(tmpDir, scriptParent);
      } else {
        rc = 0;
      }
    } finally {
      executor.shutdown();
      rawFs.close();
      fs.close();
    }

    if (rc == 0) {
      LOG.debug("Exiting successfully");
    } else {
      LOG.info("Exiting with rc = " + rc);
    }
    return rc;
  }

  private void setupConf(Properties propsDirectOptions) throws Exception {
    // needed so that the file is actually loaded into configuration.
    for (String f : LlapDaemonConfiguration.DAEMON_CONFIGS) {
      conf.addResource(f);
      if (conf.getResource(f) == null) {
        throw new Exception("Unable to find required config file: " + f);
      }
    }
    for (String f : LlapDaemonConfiguration.SSL_DAEMON_CONFIGS) {
      conf.addResource(f);
    }

    conf.reloadConfiguration();

    populateConfWithLlapProperties(conf, cl.getConfig());

    if (cl.getName() != null) {
      // update service registry configs - caveat: this has nothing to do with the actual settings as read by the AM
      // if needed, use --hiveconf llap.daemon.service.hosts=@llap0 to dynamically switch between instances
      conf.set(ConfVars.LLAP_DAEMON_SERVICE_HOSTS.varname, "@" + cl.getName());
      propsDirectOptions.setProperty(ConfVars.LLAP_DAEMON_SERVICE_HOSTS.varname, "@" + cl.getName());
    }

    if (cl.getLogger() != null) {
      HiveConf.setVar(conf, ConfVars.LLAP_DAEMON_LOGGER, cl.getLogger());
      propsDirectOptions.setProperty(ConfVars.LLAP_DAEMON_LOGGER.varname, cl.getLogger());
    }

    boolean isDirect = HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_ALLOCATOR_DIRECT);

    String cacheStr = LlapUtil.humanReadableByteCount(cl.getCache());
    String sizeStr = LlapUtil.humanReadableByteCount(cl.getSize());
    String xmxStr = LlapUtil.humanReadableByteCount(cl.getXmx());

    if (cl.getSize() != -1) {
      if (cl.getCache() != -1) {
        if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_ALLOCATOR_MAPPED)) {
          // direct heap allocations need to be safer
          Preconditions.checkArgument(cl.getCache() < cl.getSize(), "Cache size (" + cacheStr + ") has to be smaller" +
              " than the container sizing (" + sizeStr + ")");
        } else if (cl.getCache() < cl.getSize()) {
          LOG.warn("Note that this might need YARN physical memory monitoring to be turned off "
              + "(yarn.nodemanager.pmem-check-enabled=false)");
        }
      }
      if (cl.getXmx() != -1) {
        Preconditions.checkArgument(cl.getXmx() < cl.getSize(), "Working memory (Xmx=" + xmxStr + ") has to be" +
            " smaller than the container sizing (" + sizeStr + ")");
      }
      if (isDirect && !HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_ALLOCATOR_MAPPED)) {
        // direct and not memory mapped
        Preconditions.checkArgument(cl.getXmx() + cl.getCache() <= cl.getSize(), "Working memory (Xmx=" +
            xmxStr + ") + cache size (" + cacheStr + ") has to be smaller than the container sizing (" + sizeStr + ")");
      }
    }

    if (cl.getExecutors() != -1) {
      conf.setLong(ConfVars.LLAP_DAEMON_NUM_EXECUTORS.varname, cl.getExecutors());
      propsDirectOptions.setProperty(ConfVars.LLAP_DAEMON_NUM_EXECUTORS.varname, String.valueOf(cl.getExecutors()));
      // TODO: vcpu settings - possibly when DRFA works right
    }

    if (cl.getIoThreads() != -1) {
      conf.setLong(ConfVars.LLAP_IO_THREADPOOL_SIZE.varname, cl.getIoThreads());
      propsDirectOptions.setProperty(ConfVars.LLAP_IO_THREADPOOL_SIZE.varname, String.valueOf(cl.getIoThreads()));
    }

    long cache = cl.getCache();
    if (cache != -1) {
      conf.set(HiveConf.ConfVars.LLAP_IO_MEMORY_MAX_SIZE.varname, Long.toString(cache));
      propsDirectOptions.setProperty(HiveConf.ConfVars.LLAP_IO_MEMORY_MAX_SIZE.varname, Long.toString(cache));
    }

    long xmx = cl.getXmx();
    if (xmx != -1) {
      // Needs more explanation here
      // Xmx is not the max heap value in JDK8. You need to subtract 50% of the survivor fraction
      // from this, to get actual usable memory before it goes into GC
      long xmxMb = (xmx / (1024L * 1024L));
      conf.setLong(ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB.varname, xmxMb);
      propsDirectOptions.setProperty(ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB.varname, String.valueOf(xmxMb));
    }

    long containerSize = cl.getSize();
    if (containerSize == -1) {
      long heapSize = xmx;
      if (!isDirect) {
        heapSize += cache;
      }
      containerSize = Math.min((long)(heapSize * 1.2), heapSize + 1024L * 1024 * 1024);
      if (isDirect) {
        containerSize += cache;
      }
    }
    long containerSizeMB = containerSize / (1024 * 1024);
    long minAllocMB = conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, -1);
    String containerSizeStr = LlapUtil.humanReadableByteCount(containerSize);
    Preconditions.checkArgument(containerSizeMB >= minAllocMB, "Container size (" + containerSizeStr + ") should be " +
        "greater than minimum allocation(" + LlapUtil.humanReadableByteCount(minAllocMB * 1024L * 1024L) + ")");
    conf.setLong(ConfVars.LLAP_DAEMON_YARN_CONTAINER_MB.varname, containerSizeMB);
    propsDirectOptions.setProperty(ConfVars.LLAP_DAEMON_YARN_CONTAINER_MB.varname, String.valueOf(containerSizeMB));

    LOG.info("Memory settings: container memory: {} executor memory: {} cache memory: {}", containerSizeStr, xmxStr,
        cacheStr);

    if (!StringUtils.isEmpty(cl.getLlapQueueName())) {
      conf.set(ConfVars.LLAP_DAEMON_QUEUE_NAME.varname, cl.getLlapQueueName());
      propsDirectOptions.setProperty(ConfVars.LLAP_DAEMON_QUEUE_NAME.varname, cl.getLlapQueueName());
    }
  }

  private String getJavaHome() {
    String javaHome = cl.getJavaPath();
    if (StringUtils.isEmpty(javaHome)) {
      javaHome = System.getenv("JAVA_HOME");
      String jreHome = System.getProperty("java.home");
      if (javaHome == null) {
        javaHome = jreHome;
      } else if (!javaHome.equals(jreHome)) {
        LOG.warn("Java versions might not match : JAVA_HOME=[{}],process jre=[{}]", javaHome, jreHome);
      }
    }
    if (StringUtils.isEmpty(javaHome)) {
      throw new RuntimeException(
          "Could not determine JAVA_HOME from command line parameters, environment or system properties");
    }
    LOG.info("Using [{}] for JAVA_HOME", javaHome);
    return javaHome;
  }

  private static void populateConfWithLlapProperties(Configuration conf, Properties properties) {
    for(Entry<Object, Object> props : properties.entrySet()) {
      String key = (String) props.getKey();
      if (HiveConf.getLlapDaemonConfVars().contains(key)) {
        conf.set(key, (String) props.getValue());
      } else {
        if (key.startsWith(HiveConf.PREFIX_LLAP) || key.startsWith(HiveConf.PREFIX_HIVE_LLAP)) {
          LOG.warn("Adding key [{}] even though it is not in the set of known llap-server keys", key);
          conf.set(key, (String) props.getValue());
        } else {
          LOG.warn("Ignoring unknown llap server parameter: [{}]", key);
        }
      }
    }
  }

  private int startLlap(Path tmpDir, Path scriptParent) throws IOException, InterruptedException {
    int rc;
    String version = System.getenv("HIVE_VERSION");
    if (StringUtils.isEmpty(version)) {
      version = DateTimeFormatter.BASIC_ISO_DATE.format(LocalDateTime.now());
    }

    String outputDir = cl.getOutput();
    Path packageDir = null;
    if (outputDir == null) {
      outputDir = OUTPUT_DIR_PREFIX + version;
      packageDir = new Path(Paths.get(".").toAbsolutePath().toString(), OUTPUT_DIR_PREFIX + version);
    } else {
      packageDir = new Path(outputDir);
    }

    rc = runPackagePy(tmpDir, scriptParent, version, outputDir);
    if (rc == 0) {
      String tarballName = cl.getName() + "-" + version + ".tar.gz";
      startCluster(conf, cl.getName(), tarballName, packageDir, conf.getVar(ConfVars.LLAP_DAEMON_QUEUE_NAME));
    }
    return rc;
  }

  private int runPackagePy(Path tmpDir, Path scriptParent, String version, String outputDir)
      throws IOException, InterruptedException {
    Path scriptPath = new Path(new Path(scriptParent, "yarn"), "package.py");
    List<String> scriptArgs = new ArrayList<>(cl.getArgs().length + 7);
    scriptArgs.addAll(Arrays.asList("python", scriptPath.toString(), "--input", tmpDir.toString(), "--output",
        outputDir, "--javaChild"));
    scriptArgs.addAll(Arrays.asList(cl.getArgs()));

    LOG.debug("Calling package.py via: " + scriptArgs);
    ProcessBuilder builder = new ProcessBuilder(scriptArgs);
    builder.redirectError(ProcessBuilder.Redirect.INHERIT);
    builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
    builder.environment().put("HIVE_VERSION", version);
    return builder.start().waitFor();
  }

  private void startCluster(Configuration conf, String name, String packageName, Path packageDir, String queue) {
    LOG.info("Starting cluster with " + name + ", " + packageName + ", " + queue + ", " + packageDir);
    ServiceClient sc;
    try {
      sc = LlapSliderUtils.createServiceClient(conf);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    try {
      try {
        LOG.info("Executing the stop command");
        sc.actionStop(name, true);
      } catch (Exception ex) { // Ignore exceptions from stop
        LOG.info(ex.getLocalizedMessage());
      }
      try {
        LOG.info("Executing the destroy command");
        sc.actionDestroy(name);
      } catch (Exception ex) { // Ignore exceptions from destroy
        LOG.info(ex.getLocalizedMessage());
      }
      LOG.info("Uploading the app tarball");
      CoreFileSystem fs = new CoreFileSystem(conf);
      String llapPackageDir = HiveConf.getVar(conf, HiveConf.ConfVars.LLAP_HDFS_PACKAGE_DIR)
              + LLAP_RELATIVE_PACKAGE_DIR;
      fs.createWithPermissions(new Path(llapPackageDir), FsPermission.getDirDefault());
      fs.copyLocalFileToHdfs(new File(packageDir.toString(), packageName), new Path(llapPackageDir),
          new FsPermission("755"));

      LOG.info("Executing the launch command");
      File yarnfile = new File(new Path(packageDir, "Yarnfile").toString());
      Long lifetime = null; // unlimited lifetime
      sc.actionLaunch(yarnfile.getAbsolutePath(), name, lifetime, queue);
      LOG.debug("Started the cluster via service API");
    } catch (YarnException | IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        sc.close();
      } catch (IOException e) {
        LOG.info("Failed to close service client", e);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    LlapServiceCommandLine cl = new LlapServiceCommandLine(args);
    int ret = 0;
    try {
      ret = new LlapServiceDriver(cl).run();
    } catch (Throwable t) {
      System.err.println("Failed: " + t.getMessage());
      t.printStackTrace();
      ret = 3;
    } finally {
      LOG.info("LLAP service driver finished");
    }
    LOG.debug("Completed processing - exiting with " + ret);
    System.exit(ret);
  }
}
