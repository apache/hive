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
import java.io.OutputStreamWriter;
import java.net.URL;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Collection;

import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.tezplugins.LlapTezUtils;
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
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.json.JSONObject;

import com.google.common.base.Preconditions;

public class LlapServiceDriver {

  protected static final Logger LOG = LoggerFactory.getLogger(LlapServiceDriver.class.getName());
  private static final String[] DEFAULT_AUX_CLASSES = new String[] {
  "org.apache.hive.hcatalog.data.JsonSerDe" };
  private static final String HBASE_SERDE_CLASS = "org.apache.hadoop.hive.hbase.HBaseSerDe";
  private static final String[] NEEDED_CONFIGS = {
    "tez-site.xml", "hive-site.xml", "llap-daemon-site.xml", "core-site.xml" };
  private static final String[] OPTIONAL_CONFIGS = { "ssl-server.xml" };


  private final Configuration conf;

  public LlapServiceDriver() {
    SessionState ss = SessionState.get();
    conf = (ss != null) ? ss.getConf() : new HiveConf(SessionState.class);
  }

  public static void main(String[] args) throws Exception {
    int ret = 0;
    try {
      new LlapServiceDriver().run(args);
    } catch (Throwable t) {
      System.err.println("Failed: " + t.getMessage());
      t.printStackTrace();
      ret = 3;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Completed processing - exiting with " + ret);
    }
    System.exit(ret);
  }

  /**
   * Intersect llap-daemon-site.xml configuration properties against an existing Configuration
   * object, while resolving any ${} parameters that might be present.
   * 
   * @param raw
   * @return configuration object which is a slice of configured
   */
  public static Configuration resolve(Configuration configured, String first, String... resources) {
    Configuration defaults = new Configuration(false);

    defaults.addResource(first);

    for (String resource : resources) {
      defaults.addResource(resource);
    }

    Configuration slice = new Configuration(false);
    // for everything in defaults, slice out those from the configured
    for (Map.Entry<String, String> kv : defaults) {
      slice.set(kv.getKey(), configured.get(kv.getKey()));
    }

    return slice;
  }

  private void run(String[] args) throws Exception {
    LlapOptionsProcessor optionsProcessor = new LlapOptionsProcessor();
    LlapOptions options = optionsProcessor.processOptions(args);

    if (options == null) {
      // help
      return;
    }

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

    if (options.getName() != null) {
      // update service registry configs - caveat: this has nothing to do with the actual settings
      // as read by the AM
      // if needed, use --hiveconf llap.daemon.service.hosts=@llap0 to dynamically switch between
      // instances
      conf.set(ConfVars.LLAP_DAEMON_SERVICE_HOSTS.varname, "@" + options.getName());
    }

    if (options.getSize() != -1) {
      if (options.getCache() != -1) {
        Preconditions.checkArgument(options.getCache() < options.getSize(),
            "Cache has to be smaller than the container sizing");
      }
      if (options.getXmx() != -1) {
        Preconditions.checkArgument(options.getXmx() < options.getSize(),
            "Working memory has to be smaller than the container sizing");
      }
      if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_ALLOCATOR_DIRECT)) {
        Preconditions.checkArgument(options.getXmx() + options.getCache() < options.getSize(),
            "Working memory + cache has to be smaller than the containing sizing ");
      }
    }

    final long minAlloc = conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, -1);
    if (options.getSize() != -1) {
      final long containerSize = options.getSize() / (1024 * 1024);
      Preconditions.checkArgument(containerSize >= minAlloc,
          "Container size should be greater than minimum allocation(%s)", minAlloc + "m");
      conf.setLong(ConfVars.LLAP_DAEMON_YARN_CONTAINER_MB.varname, containerSize);
    }

    if (options.getExecutors() != -1) {
      conf.setLong(ConfVars.LLAP_DAEMON_NUM_EXECUTORS.varname, options.getExecutors());
      // TODO: vcpu settings - possibly when DRFA works right
    }

    if (options.getCache() != -1) {
      conf.set(HiveConf.ConfVars.LLAP_IO_MEMORY_MAX_SIZE.varname,
          Long.toString(options.getCache()));
    }

    if (options.getXmx() != -1) {
      // Needs more explanation here
      // Xmx is not the max heap value in JDK8
      // You need to subtract 50% of the survivor fraction from this, to get actual usable memory before it goes into GC 
      conf.setLong(ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB.varname, (long)(options.getXmx())
          / (1024 * 1024));
    }

    for (Entry<Object, Object> props : options.getConfig().entrySet()) {
      conf.set((String) props.getKey(), (String) props.getValue());
    }

    URL logger = conf.getResource("llap-daemon-log4j2.properties");

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

    String tezLibs = conf.get("tez.lib.uris");
    if (tezLibs == null) {
      LOG.warn("Missing tez.lib.uris in tez-site.xml");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Copying tez libs from " + tezLibs);
    }
    lfs.mkdirs(libDir);
    fs.copyToLocalFile(new Path(tezLibs), new Path(libDir, "tez.tar.gz"));
    CompressionUtils.unTar(new Path(libDir, "tez.tar.gz").toString(), libDir.toString(), true);
    lfs.delete(new Path(libDir, "tez.tar.gz"), false);

    // llap-common
    lfs.copyFromLocalFile(new Path(Utilities.jarFinderGetJar(LlapDaemonProtocolProtos.class)), libDir);
    // llap-tez
    lfs.copyFromLocalFile(new Path(Utilities.jarFinderGetJar(LlapTezUtils.class)), libDir);
    // llap-server
    lfs.copyFromLocalFile(new Path(Utilities.jarFinderGetJar(LlapInputFormat.class)), libDir);
    // hive-exec
    lfs.copyFromLocalFile(new Path(Utilities.jarFinderGetJar(HiveInputFormat.class)), libDir);

    // copy default aux classes (json/hbase)

    for (String className : DEFAULT_AUX_CLASSES) {
      localizeJarForClass(lfs, libDir, className, false);
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

    Path confPath = new Path(tmpDir, "conf");
    lfs.mkdirs(confPath);

    for (String f : NEEDED_CONFIGS) {
      copyConfig(options, lfs, confPath, f);
    }
    for (String f : OPTIONAL_CONFIGS) {
      try {
        copyConfig(options, lfs, confPath, f);
      } catch (Throwable t) {
        LOG.info("Error getting an optional config " + f + "; ignoring: " + t.getMessage());
      }
    }

    lfs.copyFromLocalFile(new Path(logger.toString()), confPath);

    String java_home = System.getenv("JAVA_HOME");
    String jre_home = System.getProperty("java.home");
    if (java_home == null) {
      java_home = jre_home;
    } else if (!java_home.equals(jre_home)) {
      LOG.warn("Java versions might not match : JAVA_HOME=%s,process jre=%s", 
          java_home, jre_home);
    }

    // extract configs for processing by the python fragments in Slider
    JSONObject configs = new JSONObject();

    configs.put("java.home", java_home);

    configs.put(ConfVars.LLAP_DAEMON_YARN_CONTAINER_MB.varname, HiveConf.getIntVar(conf,
        ConfVars.LLAP_DAEMON_YARN_CONTAINER_MB));

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

    configs.put(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, -1));

    configs.put(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, -1));

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

// TODO#: assumes throw
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
      String err =
          "Cannot find a jar for [" + className + "] due to an exception (" + t.getMessage()
              + "); not packaging the jar";
      LOG.error(err, t);
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

  private void copyConfig(
      LlapOptions options, FileSystem lfs, Path confPath, String f) throws IOException {
    if (f.equals("llap-daemon-site.xml")) {
      FSDataOutputStream confStream = lfs.create(new Path(confPath, f));

      Configuration copy = resolve(conf, "llap-daemon-site.xml");

      for (Entry<Object, Object> props : options.getConfig().entrySet()) {
        // overrides
        copy.set((String) props.getKey(), (String) props.getValue());
      }

      copy.writeXml(confStream);
      confStream.close();
    } else {
      // they will be file:// URLs
      lfs.copyFromLocalFile(new Path(conf.getResource(f).toString()), confPath);
    }
  }
}
