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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.llap.configuration.LlapDaemonConfiguration;
import org.apache.hadoop.hive.llap.daemon.impl.LlapDaemon;
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
import org.json.JSONObject;

import com.google.common.base.Preconditions;

public class LlapServiceDriver {

  protected static final Logger LOG = LoggerFactory.getLogger(LlapServiceDriver.class.getName());

  private static final String[] DEFAULT_AUX_CLASSES = new String[] {
  "org.apache.hive.hcatalog.data.JsonSerDe" };
  private static final String HBASE_SERDE_CLASS = "org.apache.hadoop.hive.hbase.HBaseSerDe";
  private static final String[] NEEDED_CONFIGS = LlapDaemonConfiguration.DAEMON_CONFIGS;
  private static final String[] OPTIONAL_CONFIGS = LlapDaemonConfiguration.SSL_DAEMON_CONFIGS;

  // This is not a config that users set in hive-site. It's only use is to share information
  // between the java component of the service driver and the python component.
  private static final String CONFIG_CLUSTER_NAME = "private.hive.llap.servicedriver.cluster.name";

  /**
   * This is a working configuration for the instance to merge various variables.
   * It is not written out for llap server usage
   */
  private final Configuration conf;

  public LlapServiceDriver() {
    SessionState ss = SessionState.get();
    conf = (ss != null) ? ss.getConf() : new HiveConf(SessionState.class);
  }

  public static void main(String[] args) throws Exception {
    LOG.info("LLAP service driver invoked with arguments={}", args);
    int ret = 0;
    try {
      new LlapServiceDriver().run(args);
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

  private void run(String[] args) throws Exception {
    LlapOptionsProcessor optionsProcessor = new LlapOptionsProcessor();
    LlapOptions options = optionsProcessor.processOptions(args);

    Properties propsDirectOptions = new Properties();

    if (options == null) {
      // help
      return;
    }

    // Working directory.
    Path tmpDir = new Path(options.getDirectory());

    if (conf == null) {
      throw new Exception("Cannot load any configuration to run command");
    }

    FileSystem fs = FileSystem.get(conf);
    FileSystem lfs = FileSystem.getLocal(conf).getRawFileSystem();

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

    if (options.getSize() != -1) {
      if (options.getCache() != -1) {
        if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_ALLOCATOR_MAPPED) == false) {
          // direct heap allocations need to be safer
          Preconditions.checkArgument(options.getCache() < options.getSize(),
              "Cache size (" + humanReadableByteCount(options.getCache()) + ") has to be smaller" +
                  " than the container sizing (" + humanReadableByteCount(options.getSize()) + ")");
        } else if (options.getCache() < options.getSize()) {
          LOG.warn("Note that this might need YARN physical memory monitoring to be turned off "
              + "(yarn.nodemanager.pmem-check-enabled=false)");
        }
      }
      if (options.getXmx() != -1) {
        Preconditions.checkArgument(options.getXmx() < options.getSize(),
            "Working memory (Xmx=" + humanReadableByteCount(options.getXmx()) + ") has to be" +
                " smaller than the container sizing (" +
                humanReadableByteCount(options.getSize()) + ")");
      }
      if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_ALLOCATOR_DIRECT)
          && false == HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_ALLOCATOR_MAPPED)) {
        // direct and not memory mapped
        Preconditions.checkArgument(options.getXmx() + options.getCache() < options.getSize(),
            "Working memory + cache (Xmx="+ humanReadableByteCount(options.getXmx()) +
                " + cache=" + humanReadableByteCount(options.getCache()) + ")"
                + " has to be smaller than the container sizing (" +
                humanReadableByteCount(options.getSize()) + ")");
      }
    }

    // This parameter is read in package.py - and nowhere else. Does not need to be part of HiveConf - that's just confusing.
    final long minAlloc = conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, -1);
    long containerSize = -1;
    if (options.getSize() != -1) {
      containerSize = options.getSize() / (1024 * 1024);
      Preconditions.checkArgument(containerSize >= minAlloc,
          "Container size (" + humanReadableByteCount(options.getSize()) + ") should be greater" +
              " than minimum allocation(" + humanReadableByteCount(minAlloc * 1024L * 1024L) + ")");
      conf.setLong(ConfVars.LLAP_DAEMON_YARN_CONTAINER_MB.varname, containerSize);
      propsDirectOptions.setProperty(ConfVars.LLAP_DAEMON_YARN_CONTAINER_MB.varname, String.valueOf(containerSize));
    }

    if (options.getExecutors() != -1) {
      conf.setLong(ConfVars.LLAP_DAEMON_NUM_EXECUTORS.varname, options.getExecutors());
      propsDirectOptions.setProperty(ConfVars.LLAP_DAEMON_NUM_EXECUTORS.varname, String.valueOf(options.getExecutors()));
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
      // from this, to get actual usable  memory before it goes into GC
      xmx = options.getXmx();
      long xmxMb = (long)(xmx / (1024 * 1024));
      conf.setLong(ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB.varname, xmxMb);
      propsDirectOptions.setProperty(ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB.varname,
          String.valueOf(xmxMb));
    }

    if (options.getLlapQueueName() != null && !options.getLlapQueueName().isEmpty()) {
      conf.set(ConfVars.LLAP_DAEMON_QUEUE_NAME.varname, options.getLlapQueueName());
      propsDirectOptions
          .setProperty(ConfVars.LLAP_DAEMON_QUEUE_NAME.varname, options.getLlapQueueName());
    }

    URL logger = conf.getResource(LlapDaemon.LOG4j2_PROPERTIES_FILE);

    if (null == logger) {
      throw new Exception("Unable to find required config file: llap-daemon-log4j2.properties");
    }

    Path home = new Path(System.getenv("HIVE_HOME"));
    Path scripts = new Path(new Path(new Path(home, "scripts"), "llap"), "bin");

    if (!lfs.exists(home)) {
      throw new Exception("Unable to find HIVE_HOME:" + home);
    } else if (!lfs.exists(scripts)) {
      LOG.warn("Unable to find llap scripts:" + scripts);
    }


    Path libDir = new Path(tmpDir, "lib");
    Path tezDir = new Path(libDir, "tez");
    Path udfDir = new Path(libDir, "udfs");

    String tezLibs = conf.get(TezConfiguration.TEZ_LIB_URIS);
    if (tezLibs == null) {
      LOG.warn("Missing tez.lib.uris in tez-site.xml");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Copying tez libs from " + tezLibs);
    }
    lfs.mkdirs(tezDir);
    fs.copyToLocalFile(new Path(tezLibs), new Path(libDir, "tez.tar.gz"));
    CompressionUtils.unTar(new Path(libDir, "tez.tar.gz").toString(), tezDir.toString(), true);
    lfs.delete(new Path(libDir, "tez.tar.gz"), false);

    Class<?>[] dependencies = new Class<?>[] {
        LlapDaemonProtocolProtos.class, // llap-common
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
        org.apache.log4j.NDC.class,
    };

    for (Class<?> c : dependencies) {
      Path jarPath = new Path(Utilities.jarFinderGetJar(c));
      lfs.copyFromLocalFile(jarPath, libDir);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Copying " + jarPath + " to " + libDir);
      }
    }


    // copy default aux classes (json/hbase)

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
        Collection<String> hbaseJars = fakeJob.getConfiguration().getStringCollection("tmpjars");
        for (String jarPath : hbaseJars) {
          if (!jarPath.isEmpty()) {
            lfs.copyFromLocalFile(new Path(jarPath), libDir);
          }
        }
      } catch (Throwable t) {
        String err = "Failed to add HBase jars. Use --auxhbase=false to avoid localizing them";
        LOG.error(err);
        System.err.println(err);
        throw new RuntimeException(t);
      }
    }

    String auxJars = options.getAuxJars();
    if (auxJars != null && !auxJars.isEmpty()) {
      // TODO: transitive dependencies warning?
      String[] jarPaths = auxJars.split(",");
      for (String jarPath : jarPaths) {
        if (!jarPath.isEmpty()) {
          lfs.copyFromLocalFile(new Path(jarPath), libDir);
        }
      }
    }

    // UDFs
    final Set<String> allowedUdfs;
    
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_ALLOW_PERMANENT_FNS)) {
      allowedUdfs = downloadPermanentFunctions(conf, udfDir);
    } else {
      allowedUdfs = Collections.emptySet();
    }

    String java_home;
    if (options.getJavaPath() == null || options.getJavaPath().isEmpty()) {
      java_home = System.getenv("JAVA_HOME");
      String jre_home = System.getProperty("java.home");
      if (java_home == null) {
        java_home = jre_home;
      } else if (!java_home.equals(jre_home)) {
        LOG.warn("Java versions might not match : JAVA_HOME=[{}],process jre=[{}]",
            java_home, jre_home);
      }
    } else {
      java_home = options.getJavaPath();
    }
    if (java_home == null || java_home.isEmpty()) {
      throw new RuntimeException(
          "Could not determine JAVA_HOME from command line parameters, environment or system properties");
    }
    LOG.info("Using [{}] for JAVA_HOME", java_home);

    Path confPath = new Path(tmpDir, "conf");
    lfs.mkdirs(confPath);

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

    // logger can be a resource stream or a real file (cannot use copy)
    InputStream loggerContent = logger.openStream();
    IOUtils.copyBytes(loggerContent,
        lfs.create(new Path(confPath, "llap-daemon-log4j2.properties"), true), conf, true);

    String metricsFile = LlapDaemon.LLAP_HADOOP_METRICS2_PROPERTIES_FILE;
    URL metrics2 = conf.getResource(metricsFile);
    if (metrics2 == null) {
      LOG.warn(LlapDaemon.LLAP_HADOOP_METRICS2_PROPERTIES_FILE + " cannot be found." +
          " Looking for " + LlapDaemon.HADOOP_METRICS2_PROPERTIES_FILE);
      metricsFile = LlapDaemon.HADOOP_METRICS2_PROPERTIES_FILE;
      metrics2 = conf.getResource(metricsFile);
    }
    if (metrics2 != null) {
      InputStream metrics2FileStream = metrics2.openStream();
      IOUtils.copyBytes(metrics2FileStream, lfs.create(new Path(confPath, metricsFile), true),
          conf, true);
      LOG.info("Copied hadoop metrics2 properties file from " + metrics2);
    } else {
      LOG.warn("Cannot find " + LlapDaemon.LLAP_HADOOP_METRICS2_PROPERTIES_FILE + " or " +
          LlapDaemon.HADOOP_METRICS2_PROPERTIES_FILE + " in classpath.");
    }

    PrintWriter udfStream =
        new PrintWriter(lfs.create(new Path(confPath, StaticPermanentFunctionChecker.PERMANENT_FUNCTIONS_LIST)));
    for (String udfClass : allowedUdfs) {
      udfStream.println(udfClass);
    }
    
    udfStream.close();

    // extract configs for processing by the python fragments in Slider
    JSONObject configs = new JSONObject();

    configs.put("java.home", java_home);

    configs.put(ConfVars.LLAP_DAEMON_YARN_CONTAINER_MB.varname, HiveConf.getIntVar(conf,
        ConfVars.LLAP_DAEMON_YARN_CONTAINER_MB));
    configs.put(ConfVars.LLAP_DAEMON_YARN_CONTAINER_MB.varname, containerSize);

    configs.put(HiveConf.ConfVars.LLAP_IO_MEMORY_MAX_SIZE.varname,
        HiveConf.getSizeVar(conf, HiveConf.ConfVars.LLAP_IO_MEMORY_MAX_SIZE));

    configs.put(HiveConf.ConfVars.LLAP_ALLOCATOR_DIRECT.varname,
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_ALLOCATOR_DIRECT));

    configs.put(ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB.varname, HiveConf.getIntVar(conf,
        ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB));

    configs.put(ConfVars.LLAP_DAEMON_VCPUS_PER_INSTANCE.varname, HiveConf.getIntVar(conf,
        ConfVars.LLAP_DAEMON_VCPUS_PER_INSTANCE));

    configs.put(ConfVars.LLAP_DAEMON_NUM_EXECUTORS.varname, HiveConf.getIntVar(conf,
        ConfVars.LLAP_DAEMON_NUM_EXECUTORS));

    // Let YARN pick the queue name, if it isn't provided in hive-site, or via the command-line
    if (HiveConf.getVar(conf, ConfVars.LLAP_DAEMON_QUEUE_NAME) != null) {
      configs.put(ConfVars.LLAP_DAEMON_QUEUE_NAME.varname,
          HiveConf.getVar(conf, ConfVars.LLAP_DAEMON_QUEUE_NAME));
    }

    // Propagate the cluster name to the script.
    String clusterHosts = HiveConf.getVar(conf, ConfVars.LLAP_DAEMON_SERVICE_HOSTS);
    if (!StringUtils.isEmpty(clusterHosts) && clusterHosts.startsWith("@") &&
        clusterHosts.length() > 1) {
      configs.put(CONFIG_CLUSTER_NAME, clusterHosts.substring(1));
    }

    configs.put(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, -1));

    configs.put(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, -1));

    long maxDirect = (xmx > 0 && cache > 0 && xmx < cache * 1.25) ? (long)(cache * 1.25) : -1;
    configs.put("max_direct_memory", Long.toString(maxDirect));

    FSDataOutputStream os = lfs.create(new Path(tmpDir, "config.json"));
    OutputStreamWriter w = new OutputStreamWriter(os);
    configs.write(w);
    w.close();
    os.close();

    lfs.close();
    fs.close();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Exiting successfully");
    }
  }

  private Set<String> downloadPermanentFunctions(Configuration conf, Path udfDir) throws HiveException,
      URISyntaxException, IOException {
    Map<String,String> udfs = new HashMap<String, String>();
    Hive hive = Hive.get(false);
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

  private String humanReadableByteCount(long bytes) {
    int unit = 1024;
    if (bytes < unit) {
      return bytes + "B";
    }
    int exp = (int) (Math.log(bytes) / Math.log(unit));
    String suffix = "KMGTPE".charAt(exp-1) + "";
    return String.format("%.2f%sB", bytes / Math.pow(unit, exp), suffix);
  }
}
