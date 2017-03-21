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

package org.apache.hadoop.hive.llap.cli;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.llap.configuration.LlapDaemonConfiguration;
import org.apache.hadoop.hive.llap.daemon.impl.LlapConstants;
import org.apache.hadoop.hive.llap.daemon.impl.StaticPermanentFunctionChecker;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.tezplugins.LlapTezUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hive.common.CompressionUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.cli.LlapOptionsProcessor.LlapOptions;
import org.apache.hadoop.hive.llap.io.api.impl.LlapInputFormat;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.util.ResourceDownloader;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.eclipse.jetty.server.ssl.SslSocketConnector;
import org.joda.time.DateTime;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class LlapServiceDriver {
  protected static final Logger LOG = LoggerFactory.getLogger(LlapServiceDriver.class.getName());

  private static final String[] DEFAULT_AUX_CLASSES = new String[] {
  "org.apache.hive.hcatalog.data.JsonSerDe","org.apache.hadoop.hive.druid.DruidStorageHandler" };
  private static final String HBASE_SERDE_CLASS = "org.apache.hadoop.hive.hbase.HBaseSerDe";
  private static final String[] NEEDED_CONFIGS = LlapDaemonConfiguration.DAEMON_CONFIGS;
  private static final String[] OPTIONAL_CONFIGS = LlapDaemonConfiguration.SSL_DAEMON_CONFIGS;
  private static final String OUTPUT_DIR_PREFIX = "llap-slider-";

  // This is not a config that users set in hive-site. It's only use is to share information
  // between the java component of the service driver and the python component.
  private static final String CONFIG_CLUSTER_NAME = "private.hive.llap.servicedriver.cluster.name";

  /**
   * This is a working configuration for the instance to merge various variables.
   * It is not written out for llap server usage
   */
  private final HiveConf conf;

  public LlapServiceDriver() {
    SessionState ss = SessionState.get();
    conf = (ss != null) ? ss.getConf() : new HiveConf(SessionState.class);
  }

  public static void main(String[] args) throws Exception {
    LOG.info("LLAP service driver invoked with arguments={}", args);
    int ret = 0;
    try {
      ret = new LlapServiceDriver().run(args);
    } catch (Throwable t) {
      System.err.println("Failed: " + t.getMessage());
      t.printStackTrace();
      ret = 3;
    } finally {
      LOG.info("LLAP service driver finished");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Completed processing - exiting with " + ret);
    }
    System.exit(ret);
  }


  private static Configuration resolve(Configuration configured, Properties direct,
                                       Properties hiveconf) {
    Configuration conf = new Configuration(false);

    populateConf(configured, conf, hiveconf, "CLI hiveconf");
    populateConf(configured, conf, direct, "CLI direct");

    return conf;
  }

  private static void populateConf(Configuration configured, Configuration target,
                                   Properties properties, String source) {
    for (Entry<Object, Object> entry : properties.entrySet()) {
      String key = (String) entry.getKey();
      String val = configured.get(key);
      if (val != null) {
        target.set(key, val, source);
      }
    }
  }

  static void populateConfWithLlapProperties(Configuration conf, Properties properties) {
    for(Entry<Object, Object> props : properties.entrySet()) {
      String key = (String) props.getKey();
      if (HiveConf.getLlapDaemonConfVars().contains(key)) {
        conf.set(key, (String) props.getValue());
      } else {
        if (key.startsWith(HiveConf.PREFIX_LLAP) || key.startsWith(HiveConf.PREFIX_HIVE_LLAP)) {
          LOG.warn("Adding key [{}] even though it is not in the set of known llap-server keys");
          conf.set(key, (String) props.getValue());
        } else {
          LOG.warn("Ignoring unknown llap server parameter: [{}]", key);
        }
      }
    }
  }

  private static abstract class NamedCallable<T> implements Callable<T> {
    public final String taskName;
    public NamedCallable (String name) {
      this.taskName = name;
    }
    public String getName() {
      return taskName;
    }
  }

  private int run(String[] args) throws Exception {
    LlapOptionsProcessor optionsProcessor = new LlapOptionsProcessor();
    final LlapOptions options = optionsProcessor.processOptions(args);

    final Properties propsDirectOptions = new Properties();

    if (options == null) {
      // help
      return 1;
    }

    // Working directory.
    Path tmpDir = new Path(options.getDirectory());

    if (conf == null) {
      throw new Exception("Cannot load any configuration to run command");
    }

    final long t0 = System.nanoTime();

    final FileSystem fs = FileSystem.get(conf);
    final FileSystem lfs = FileSystem.getLocal(conf).getRawFileSystem();

    int threadCount = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
    final ExecutorService executor = Executors.newFixedThreadPool(threadCount,
            new ThreadFactoryBuilder().setNameFormat("llap-pkg-%d").build());
    final CompletionService<Void> asyncRunner = new ExecutorCompletionService<Void>(executor);

    int rc = 0;
    try {

      // needed so that the file is actually loaded into configuration.
      for (String f : NEEDED_CONFIGS) {
        conf.addResource(f);
        if (conf.getResource(f) == null) {
          throw new Exception("Unable to find required config file: " + f);
        }
      }
      for (String f : OPTIONAL_CONFIGS) {
        conf.addResource(f);
      }

      conf.reloadConfiguration();

      populateConfWithLlapProperties(conf, options.getConfig());

      if (options.getName() != null) {
        // update service registry configs - caveat: this has nothing to do with the actual settings
        // as read by the AM
        // if needed, use --hiveconf llap.daemon.service.hosts=@llap0 to dynamically switch between
        // instances
        conf.set(ConfVars.LLAP_DAEMON_SERVICE_HOSTS.varname, "@" + options.getName());
        propsDirectOptions.setProperty(ConfVars.LLAP_DAEMON_SERVICE_HOSTS.varname,
            "@" + options.getName());
      }

      if (options.getLogger() != null) {
        HiveConf.setVar(conf, ConfVars.LLAP_DAEMON_LOGGER, options.getLogger());
        propsDirectOptions.setProperty(ConfVars.LLAP_DAEMON_LOGGER.varname, options.getLogger());
      }
      boolean isDirect = HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_ALLOCATOR_DIRECT);

      if (options.getSize() != -1) {
        if (options.getCache() != -1) {
          if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_ALLOCATOR_MAPPED) == false) {
            // direct heap allocations need to be safer
            Preconditions.checkArgument(options.getCache() < options.getSize(), "Cache size ("
                + LlapUtil.humanReadableByteCount(options.getCache()) + ") has to be smaller"
                + " than the container sizing (" + LlapUtil.humanReadableByteCount(options.getSize()) + ")");
          } else if (options.getCache() < options.getSize()) {
            LOG.warn("Note that this might need YARN physical memory monitoring to be turned off "
                + "(yarn.nodemanager.pmem-check-enabled=false)");
          }
        }
        if (options.getXmx() != -1) {
          Preconditions.checkArgument(options.getXmx() < options.getSize(), "Working memory (Xmx="
              + LlapUtil.humanReadableByteCount(options.getXmx()) + ") has to be"
              + " smaller than the container sizing (" + LlapUtil.humanReadableByteCount(options.getSize())
              + ")");
        }
        if (isDirect && !HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_ALLOCATOR_MAPPED)) {
          // direct and not memory mapped
          Preconditions.checkArgument(options.getXmx() + options.getCache() <= options.getSize(),
            "Working memory (Xmx=" + LlapUtil.humanReadableByteCount(options.getXmx()) + ") + cache size ("
              + LlapUtil.humanReadableByteCount(options.getCache()) + ") has to be smaller than the container sizing ("
              + LlapUtil.humanReadableByteCount(options.getSize()) + ")");
        }
      }


      if (options.getExecutors() != -1) {
        conf.setLong(ConfVars.LLAP_DAEMON_NUM_EXECUTORS.varname, options.getExecutors());
        propsDirectOptions.setProperty(ConfVars.LLAP_DAEMON_NUM_EXECUTORS.varname,
            String.valueOf(options.getExecutors()));
        // TODO: vcpu settings - possibly when DRFA works right
      }

      if (options.getIoThreads() != -1) {
        conf.setLong(ConfVars.LLAP_IO_THREADPOOL_SIZE.varname, options.getIoThreads());
        propsDirectOptions.setProperty(ConfVars.LLAP_IO_THREADPOOL_SIZE.varname,
            String.valueOf(options.getIoThreads()));
      }

      long cache = -1, xmx = -1;
      if (options.getCache() != -1) {
        cache = options.getCache();
        conf.set(HiveConf.ConfVars.LLAP_IO_MEMORY_MAX_SIZE.varname, Long.toString(cache));
        propsDirectOptions.setProperty(HiveConf.ConfVars.LLAP_IO_MEMORY_MAX_SIZE.varname,
            Long.toString(cache));
      }

      if (options.getXmx() != -1) {
        // Needs more explanation here
        // Xmx is not the max heap value in JDK8. You need to subtract 50% of the survivor fraction
        // from this, to get actual usable memory before it goes into GC
        xmx = options.getXmx();
        long xmxMb = (xmx / (1024L * 1024L));
        conf.setLong(ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB.varname, xmxMb);
        propsDirectOptions.setProperty(ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB.varname,
            String.valueOf(xmxMb));
      }

      long size = options.getSize();
      if (size == -1) {
        long heapSize = xmx;
        if (!isDirect) {
          heapSize += cache;
        }
        size = Math.min((long)(heapSize * 1.2), heapSize + 1024L*1024*1024);
        if (isDirect) {
          size += cache;
        }
      }
      long containerSize = size / (1024 * 1024);
      final long minAlloc = conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, -1);
      Preconditions.checkArgument(containerSize >= minAlloc, "Container size ("
          + LlapUtil.humanReadableByteCount(options.getSize()) + ") should be greater"
          + " than minimum allocation(" + LlapUtil.humanReadableByteCount(minAlloc * 1024L * 1024L) + ")");
      conf.setLong(ConfVars.LLAP_DAEMON_YARN_CONTAINER_MB.varname, containerSize);
      propsDirectOptions.setProperty(ConfVars.LLAP_DAEMON_YARN_CONTAINER_MB.varname,
          String.valueOf(containerSize));

      LOG.info("Memory settings: container memory: {} executor memory: {} cache memory: {}",
        LlapUtil.humanReadableByteCount(options.getSize()),
        LlapUtil.humanReadableByteCount(options.getXmx()),
        LlapUtil.humanReadableByteCount(options.getCache()));

      if (options.getLlapQueueName() != null && !options.getLlapQueueName().isEmpty()) {
        conf.set(ConfVars.LLAP_DAEMON_QUEUE_NAME.varname, options.getLlapQueueName());
        propsDirectOptions.setProperty(ConfVars.LLAP_DAEMON_QUEUE_NAME.varname,
            options.getLlapQueueName());
      }

      final URL logger = conf.getResource(LlapConstants.LOG4j2_PROPERTIES_FILE);

      if (null == logger) {
        throw new Exception("Unable to find required config file: llap-daemon-log4j2.properties");
      }

      Path home = new Path(System.getenv("HIVE_HOME"));
      Path scriptParent = new Path(new Path(home, "scripts"), "llap");
      Path scripts = new Path(scriptParent, "bin");

      if (!lfs.exists(home)) {
        throw new Exception("Unable to find HIVE_HOME:" + home);
      } else if (!lfs.exists(scripts)) {
        LOG.warn("Unable to find llap scripts:" + scripts);
      }

      final Path libDir = new Path(tmpDir, "lib");
      final Path tezDir = new Path(libDir, "tez");
      final Path udfDir = new Path(libDir, "udfs");
      final Path confPath = new Path(tmpDir, "conf");
      lfs.mkdirs(confPath);

      NamedCallable<Void> downloadTez = new NamedCallable<Void>("downloadTez") {
        @Override
        public Void call() throws Exception {
          synchronized (fs) {
            String tezLibs = conf.get(TezConfiguration.TEZ_LIB_URIS);
            if (tezLibs == null) {
              LOG.warn("Missing tez.lib.uris in tez-site.xml");
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug("Copying tez libs from " + tezLibs);
            }
            lfs.mkdirs(tezDir);
            fs.copyToLocalFile(new Path(tezLibs), new Path(libDir, "tez.tar.gz"));
            CompressionUtils.unTar(new Path(libDir, "tez.tar.gz").toString(), tezDir.toString(),
                true);
            lfs.delete(new Path(libDir, "tez.tar.gz"), false);
          }
          return null;
        }
      };

      NamedCallable<Void> copyLocalJars = new NamedCallable<Void>("copyLocalJars") {
        @Override
        public Void call() throws Exception {
          Class<?>[] dependencies = new Class<?>[] { LlapDaemonProtocolProtos.class, // llap-common
              LlapTezUtils.class, // llap-tez
              LlapInputFormat.class, // llap-server
              HiveInputFormat.class, // hive-exec
              SslSocketConnector.class, // hive-common (https deps)
              RegistryUtils.ServiceRecordMarshal.class, // ZK registry
              // log4j2
              com.lmax.disruptor.RingBuffer.class, // disruptor
              org.apache.logging.log4j.Logger.class, // log4j-api
              org.apache.logging.log4j.core.Appender.class, // log4j-core
              org.apache.logging.slf4j.Log4jLogger.class, // log4j-slf4j
              // log4j-1.2-API needed for NDC
              org.apache.log4j.NDC.class, };

          for (Class<?> c : dependencies) {
            Path jarPath = new Path(Utilities.jarFinderGetJar(c));
            lfs.copyFromLocalFile(jarPath, libDir);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Copying " + jarPath + " to " + libDir);
            }
          }
          return null;
        }
      };

      // copy default aux classes (json/hbase)

      NamedCallable<Void> copyAuxJars = new NamedCallable<Void>("copyAuxJars") {
        @Override
        public Void call() throws Exception {
          for (String className : DEFAULT_AUX_CLASSES) {
            localizeJarForClass(lfs, libDir, className, false);
          }
          Collection<String> codecs = conf.getStringCollection("io.compression.codecs");
          if (codecs != null) {
            for (String codecClassName : codecs) {
              localizeJarForClass(lfs, libDir, codecClassName, false);
            }
          }

          if (options.getIsHBase()) {
            try {
              localizeJarForClass(lfs, libDir, HBASE_SERDE_CLASS, true);
              Job fakeJob = new Job(new JobConf()); // HBase API is convoluted.
              TableMapReduceUtil.addDependencyJars(fakeJob);
              Collection<String> hbaseJars =
                  fakeJob.getConfiguration().getStringCollection("tmpjars");
              for (String jarPath : hbaseJars) {
                if (!jarPath.isEmpty()) {
                  lfs.copyFromLocalFile(new Path(jarPath), libDir);
                }
              }
            } catch (Throwable t) {
              String err =
                  "Failed to add HBase jars. Use --auxhbase=false to avoid localizing them";
              LOG.error(err);
              System.err.println(err);
              throw new RuntimeException(t);
            }
          }

          HashSet<String> auxJars = new HashSet<>();
          // There are many ways to have AUX jars in Hive... sigh
          if (options.getIsHiveAux()) {
            // Note: we don't add ADDED jars, RELOADABLE jars, etc. That is by design; there are too many ways
            // to add jars in Hive, some of which are session/etc. specific. Env + conf + arg should be enough.
            addAuxJarsToSet(auxJars, conf.getAuxJars());
            addAuxJarsToSet(auxJars, System.getenv("HIVE_AUX_JARS_PATH"));
            LOG.info("Adding the following aux jars from the environment and configs: " + auxJars);
          }

          addAuxJarsToSet(auxJars, options.getAuxJars());
          for (String jarPath : auxJars) {
            lfs.copyFromLocalFile(new Path(jarPath), libDir);
          }
          return null;
        }

        private void addAuxJarsToSet(HashSet<String> auxJarSet, String auxJars) {
          if (auxJars != null && !auxJars.isEmpty()) {
            // TODO: transitive dependencies warning?
            String[] jarPaths = auxJars.split(",");
            for (String jarPath : jarPaths) {
              if (!jarPath.isEmpty()) {
                auxJarSet.add(jarPath);
              }
            }
          }
        }
      };

      NamedCallable<Void> copyUdfJars = new NamedCallable<Void>("copyUdfJars") {
        @Override
        public Void call() throws Exception {
          // UDFs
          final Set<String> allowedUdfs;

          if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_ALLOW_PERMANENT_FNS)) {
            synchronized (fs) {
              allowedUdfs = downloadPermanentFunctions(conf, udfDir);
            }
          } else {
            allowedUdfs = Collections.emptySet();
          }

          PrintWriter udfStream =
              new PrintWriter(lfs.create(new Path(confPath,
                  StaticPermanentFunctionChecker.PERMANENT_FUNCTIONS_LIST)));
          for (String udfClass : allowedUdfs) {
            udfStream.println(udfClass);
          }

          udfStream.close();
          return null;
        }
      };

      String java_home;
      if (options.getJavaPath() == null || options.getJavaPath().isEmpty()) {
        java_home = System.getenv("JAVA_HOME");
        String jre_home = System.getProperty("java.home");
        if (java_home == null) {
          java_home = jre_home;
        } else if (!java_home.equals(jre_home)) {
          LOG.warn("Java versions might not match : JAVA_HOME=[{}],process jre=[{}]", java_home,
              jre_home);
        }
      } else {
        java_home = options.getJavaPath();
      }
      if (java_home == null || java_home.isEmpty()) {
        throw new RuntimeException(
            "Could not determine JAVA_HOME from command line parameters, environment or system properties");
      }
      LOG.info("Using [{}] for JAVA_HOME", java_home);

      NamedCallable<Void> copyConfigs = new NamedCallable<Void>("copyConfigs") {
        @Override
        public Void call() throws Exception {
          // Copy over the mandatory configs for the package.
          for (String f : NEEDED_CONFIGS) {
            copyConfig(lfs, confPath, f);
          }
          for (String f : OPTIONAL_CONFIGS) {
            try {
              copyConfig(lfs, confPath, f);
            } catch (Throwable t) {
              LOG.info("Error getting an optional config " + f + "; ignoring: " + t.getMessage());
            }
          }
          createLlapDaemonConfig(lfs, confPath, conf, propsDirectOptions, options.getConfig());
          setUpLogAndMetricConfigs(lfs, logger, confPath);
          return null;
        }
      };

      @SuppressWarnings("unchecked")
      final NamedCallable<Void>[] asyncWork =
          new NamedCallable[] {
          downloadTez,
          copyUdfJars,
          copyLocalJars,
          copyAuxJars,
          copyConfigs };
      @SuppressWarnings("unchecked")
      final Future<Void>[] asyncResults = new Future[asyncWork.length];
      for (int i = 0; i < asyncWork.length; i++) {
        asyncResults[i] = asyncRunner.submit(asyncWork[i]);
      }

      // TODO: need to move from Python to Java for the rest of the script.
      JSONObject configs = createConfigJson(containerSize, cache, xmx, java_home);
      writeConfigJson(tmpDir, lfs, configs);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Config generation took " + (System.nanoTime() - t0) + " ns");
      }
      for (int i = 0; i < asyncWork.length; i++) {
        final long t1 = System.nanoTime();
        asyncResults[i].get();
        final long t2 = System.nanoTime();
        if (LOG.isDebugEnabled()) {
          LOG.debug(asyncWork[i].getName() + " waited for " + (t2 - t1) + " ns");
        }
      }
      if (options.isStarting()) {
        String version = System.getenv("HIVE_VERSION");
        if (version == null || version.isEmpty()) {
          version = DateTime.now().toString("ddMMMyyyy");
        }

        String outputDir = options.getOutput();
        Path packageDir = null;
        if (outputDir == null) {
          outputDir = OUTPUT_DIR_PREFIX + version;
          packageDir = new Path(Paths.get(".").toAbsolutePath().toString(),
              OUTPUT_DIR_PREFIX + version);
        } else {
          packageDir = new Path(outputDir);
        }
        rc = runPackagePy(args, tmpDir, scriptParent, version, outputDir);
        if (rc == 0) {
          LlapSliderUtils.startCluster(conf, options.getName(), "llap-" + version + ".zip",
              packageDir, HiveConf.getVar(conf, ConfVars.LLAP_DAEMON_QUEUE_NAME));
        }
      } else {
        rc = 0;
      }
    } finally {
      executor.shutdown();
      lfs.close();
      fs.close();
    }

    if (rc == 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Exiting successfully");
      }
    } else {
      LOG.info("Exiting with rc = " + rc);
    }
    return rc;
  }

  private int runPackagePy(String[] args, Path tmpDir, Path scriptParent,
      String version, String outputDir) throws IOException, InterruptedException {
    Path scriptPath = new Path(new Path(scriptParent, "slider"), "package.py");
    List<String> scriptArgs = new ArrayList<>(args.length + 7);
    scriptArgs.add("python");
    scriptArgs.add(scriptPath.toString());
    scriptArgs.add("--input");
    scriptArgs.add(tmpDir.toString());
    scriptArgs.add("--output");
    scriptArgs.add(outputDir);
    scriptArgs.add("--javaChild");
    for (String arg : args) {
      scriptArgs.add(arg);
    }
    LOG.debug("Calling package.py via: " + scriptArgs);
    ProcessBuilder builder = new ProcessBuilder(scriptArgs);
    builder.redirectError(ProcessBuilder.Redirect.INHERIT);
    builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
    builder.environment().put("HIVE_VERSION", version);
    return builder.start().waitFor();
  }

  private void writeConfigJson(Path tmpDir, final FileSystem lfs,
      JSONObject configs) throws IOException, JSONException {
    FSDataOutputStream os = lfs.create(new Path(tmpDir, "config.json"));
    OutputStreamWriter w = new OutputStreamWriter(os);
    configs.write(w);
    w.close();
    os.close();
  }

  private JSONObject createConfigJson(long containerSize, long cache, long xmx,
      String java_home) throws JSONException {
    // extract configs for processing by the python fragments in Slider
    JSONObject configs = new JSONObject();

    configs.put("java.home", java_home);

    configs.put(ConfVars.LLAP_DAEMON_YARN_CONTAINER_MB.varname,
        HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_YARN_CONTAINER_MB));
    configs.put(ConfVars.LLAP_DAEMON_YARN_CONTAINER_MB.varname, containerSize);

    configs.put(HiveConf.ConfVars.LLAP_IO_MEMORY_MAX_SIZE.varname,
        HiveConf.getSizeVar(conf, HiveConf.ConfVars.LLAP_IO_MEMORY_MAX_SIZE));

    configs.put(HiveConf.ConfVars.LLAP_ALLOCATOR_DIRECT.varname,
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_ALLOCATOR_DIRECT));

    configs.put(ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB.varname,
        HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB));

    configs.put(ConfVars.LLAP_DAEMON_VCPUS_PER_INSTANCE.varname,
        HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_VCPUS_PER_INSTANCE));

    configs.put(ConfVars.LLAP_DAEMON_NUM_EXECUTORS.varname,
        HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_NUM_EXECUTORS));

    // Let YARN pick the queue name, if it isn't provided in hive-site, or via the command-line
    if (HiveConf.getVar(conf, ConfVars.LLAP_DAEMON_QUEUE_NAME) != null) {
      configs.put(ConfVars.LLAP_DAEMON_QUEUE_NAME.varname,
          HiveConf.getVar(conf, ConfVars.LLAP_DAEMON_QUEUE_NAME));
    }

    // Propagate the cluster name to the script.
    String clusterHosts = HiveConf.getVar(conf, ConfVars.LLAP_DAEMON_SERVICE_HOSTS);
    if (!StringUtils.isEmpty(clusterHosts) && clusterHosts.startsWith("@")
        && clusterHosts.length() > 1) {
      configs.put(CONFIG_CLUSTER_NAME, clusterHosts.substring(1));
    }

    configs.put(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, -1));

    configs.put(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, -1));

    long maxDirect = (xmx > 0 && cache > 0 && xmx < cache * 1.25) ? (long) (cache * 1.25) : -1;
    configs.put("max_direct_memory", Long.toString(maxDirect));
    return configs;
  }

  private Set<String> downloadPermanentFunctions(Configuration conf, Path udfDir) throws HiveException,
      URISyntaxException, IOException {
    Map<String,String> udfs = new HashMap<String, String>();
    HiveConf hiveConf = new HiveConf();
    // disable expensive operations on the metastore
    hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_INIT_METADATA_COUNT_ENABLED, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_METRICS, false);
    // performance problem: ObjectStore does its own new HiveConf()
    Hive hive = Hive.getWithFastCheck(hiveConf, false);
    ResourceDownloader resourceDownloader =
        new ResourceDownloader(conf, udfDir.toUri().normalize().getPath());
    List<Function> fns = hive.getAllFunctions();
    Set<URI> srcUris = new HashSet<>();
    for (Function fn : fns) {
      String fqfn = fn.getDbName() + "." + fn.getFunctionName();
      if (udfs.containsKey(fn.getClassName())) {
        LOG.warn("Duplicate function names found for " + fn.getClassName() + " with " + fqfn
            + " and " + udfs.get(fn.getClassName()));
      }
      udfs.put(fn.getClassName(), fqfn);
      List<ResourceUri> resources = fn.getResourceUris();
      if (resources == null || resources.isEmpty()) {
        LOG.warn("Missing resources for " + fqfn);
        continue;
      }
      for (ResourceUri resource : resources) {
        srcUris.add(ResourceDownloader.createURI(resource.getUri()));
      }
    }
    for (URI srcUri : srcUris) {
      List<URI> localUris = resourceDownloader.downloadExternal(srcUri, null, false);
      for(URI dst : localUris) {
        LOG.warn("Downloaded " + dst + " from " + srcUri);
      }
    }
    return udfs.keySet();
  }

  private void localizeJarForClass(FileSystem lfs, Path libDir, String className, boolean doThrow)
      throws IOException {
    String jarPath = null;
    boolean hasException = false;
    try {
      Class<?> auxClass = Class.forName(className);
      jarPath = Utilities.jarFinderGetJar(auxClass);
    } catch (Throwable t) {
      if (doThrow) {
        throw (t instanceof IOException) ? (IOException)t : new IOException(t);
      }
      hasException = true;
      String err = "Cannot find a jar for [" + className + "] due to an exception ("
          + t.getMessage() + "); not packaging the jar";
      LOG.error(err);
      System.err.println(err);
    }
    if (jarPath != null) {
      lfs.copyFromLocalFile(new Path(jarPath), libDir);
    } else if (!hasException) {
      String err = "Cannot find a jar for [" + className + "]; not packaging the jar";
      if (doThrow) {
        throw new IOException(err);
      }
      LOG.error(err);
      System.err.println(err);
    }
  }

  /**
   *
   * @param lfs filesystem on which file will be generated
   * @param confPath path wher the config will be generated
   * @param configured the base configuration instances
   * @param direct properties specified directly - i.e. using the properties exact option
   * @param hiveconf properties specifried via --hiveconf
   * @throws IOException
   */
  private void createLlapDaemonConfig(FileSystem lfs, Path confPath, Configuration configured,
                                      Properties direct, Properties hiveconf) throws IOException {
    FSDataOutputStream confStream =
        lfs.create(new Path(confPath, LlapDaemonConfiguration.LLAP_DAEMON_SITE));

    Configuration llapDaemonConf = resolve(configured, direct, hiveconf);

    llapDaemonConf.writeXml(confStream);
    confStream.close();
  }

  private void copyConfig(FileSystem lfs, Path confPath, String f) throws IOException {
    HiveConf.getBoolVar(new Configuration(false), ConfVars.LLAP_CLIENT_CONSISTENT_SPLITS);
    // they will be file:// URLs
    lfs.copyFromLocalFile(new Path(conf.getResource(f).toString()), confPath);
  }

  private void setUpLogAndMetricConfigs(final FileSystem lfs, final URL logger,
      final Path confPath) throws IOException {
    // logger can be a resource stream or a real file (cannot use copy)
    InputStream loggerContent = logger.openStream();
    IOUtils.copyBytes(loggerContent,
        lfs.create(new Path(confPath, "llap-daemon-log4j2.properties"), true), conf, true);

    String metricsFile = LlapConstants.LLAP_HADOOP_METRICS2_PROPERTIES_FILE;
    URL metrics2 = conf.getResource(metricsFile);
    if (metrics2 == null) {
      LOG.warn(LlapConstants.LLAP_HADOOP_METRICS2_PROPERTIES_FILE + " cannot be found."
          + " Looking for " + LlapConstants.HADOOP_METRICS2_PROPERTIES_FILE);
      metricsFile = LlapConstants.HADOOP_METRICS2_PROPERTIES_FILE;
      metrics2 = conf.getResource(metricsFile);
    }
    if (metrics2 != null) {
      InputStream metrics2FileStream = metrics2.openStream();
      IOUtils.copyBytes(metrics2FileStream,
          lfs.create(new Path(confPath, metricsFile), true), conf, true);
      LOG.info("Copied hadoop metrics2 properties file from " + metrics2);
    } else {
      LOG.warn("Cannot find " + LlapConstants.LLAP_HADOOP_METRICS2_PROPERTIES_FILE + " or "
          + LlapConstants.HADOOP_METRICS2_PROPERTIES_FILE + " in classpath.");
    }
  }
}
