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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hive.hcatalog.templeton.tool.JobState;
import org.apache.hive.hcatalog.templeton.tool.TempletonUtils;
import org.apache.hive.hcatalog.templeton.tool.ZooKeeperCleanup;
import org.apache.hive.hcatalog.templeton.tool.ZooKeeperStorage;

/**
 * The configuration for Templeton.  This merges the normal Hadoop
 * configuration with the Templeton specific variables.
 *
 * The Templeton configuration variables are described in
 * templeton-default.xml
 *
 * The Templeton specific configuration is split into two layers
 *
 * 1. webhcat-default.xml - All the configuration variables that
 *    Templeton needs.  These are the defaults that ship with the app
 *    and should only be changed be the app developers.
 *
 * 2. webhcat-site.xml - The (possibly empty) configuration that the
 *    system administrator can set variables for their Hadoop cluster.
 *
 * The configuration files are loaded in this order with later files
 * overriding earlier ones.
 *
 * To find the configuration files, we first attempt to load a file
 * from the CLASSPATH and then look in the directory specified in the
 * TEMPLETON_HOME environment variable.
 *
 * In addition the configuration files may access the special env
 * variable env for all environment variables.  For example, the
 * hadoop executable could be specified using:
 *<pre>
 *      ${env.HADOOP_PREFIX}/bin/hadoop
 *</pre>
 */
public class AppConfig extends Configuration {
  public static final String[] HADOOP_CONF_FILENAMES = {
    "core-default.xml", "core-site.xml", "mapred-default.xml", "mapred-site.xml", "hdfs-site.xml"
  };

  public static final String[] HADOOP_PREFIX_VARS = {
    "HADOOP_PREFIX", "HADOOP_HOME"
  };

  public static final String TEMPLETON_HOME_VAR = "TEMPLETON_HOME";
  public static final String WEBHCAT_CONF_DIR = "WEBHCAT_CONF_DIR";

  public static final String[] TEMPLETON_CONF_FILENAMES = {
    "webhcat-default.xml",
    "webhcat-site.xml"
  };

  public enum JobsListOrder {
    lexicographicalasc,
    lexicographicaldesc,
  }

  public static final String PORT                = "templeton.port";
  public static final String JETTY_CONFIGURATION = "templeton.jetty.configuration";
  public static final String EXEC_ENCODING_NAME  = "templeton.exec.encoding";
  public static final String EXEC_ENVS_NAME      = "templeton.exec.envs";
  public static final String EXEC_MAX_BYTES_NAME = "templeton.exec.max-output-bytes";
  public static final String EXEC_MAX_PROCS_NAME = "templeton.exec.max-procs";
  public static final String EXEC_TIMEOUT_NAME   = "templeton.exec.timeout";
  public static final String HADOOP_QUEUE_NAME   = "templeton.hadoop.queue.name";
  public static final String ENABLE_JOB_RECONNECT_DEFAULT = "templeton.enable.job.reconnect.default";
  public static final String HADOOP_NAME         = "templeton.hadoop";
  public static final String HADOOP_CONF_DIR     = "templeton.hadoop.conf.dir";
  public static final String HCAT_NAME           = "templeton.hcat";
  public static final String PYTHON_NAME         = "templeton.python";
  public static final String HIVE_ARCHIVE_NAME   = "templeton.hive.archive";
  public static final String HIVE_PATH_NAME      = "templeton.hive.path";
  public static final String MAPPER_MEMORY_MB    = "templeton.mapper.memory.mb";
  public static final String MR_AM_MEMORY_MB     = "templeton.mr.am.memory.mb";
  public static final String TEMPLETON_JOBSLIST_ORDER = "templeton.jobs.listorder";

  /*
   * These parameters controls the maximum number of concurrent job submit/status/list
   * operations in templeton service. If more number of concurrent requests comes then
   * they will be rejected with BusyException.
   */
  public static final String JOB_SUBMIT_MAX_THREADS = "templeton.parallellism.job.submit";
  public static final String JOB_STATUS_MAX_THREADS = "templeton.parallellism.job.status";
  public static final String JOB_LIST_MAX_THREADS = "templeton.parallellism.job.list";

  /*
   * These parameters controls the maximum time job submit/status/list operation is
   * executed in templeton service. On time out, the execution is interrupted and
   * TimeoutException is returned to client. On time out
   *   For list and status operation, there is no action needed as they are read requests.
   *   For submit operation, we do best effort to kill the job if its generated. Enabling
   *     this parameter may have following side effects
   *     1) There is a possibility for having active job for some time when the client gets
   *        response for submit operation and a list operation from client could potential
   *        show the newly created job which may eventually be killed with no guarantees.
   *     2) If submit operation retried by client then there is a possibility of duplicate
   *        jobs triggered.
   *
   * Time out configs should be configured in seconds.
   *
   */
  public static final String JOB_SUBMIT_TIMEOUT   = "templeton.job.submit.timeout";
  public static final String JOB_STATUS_TIMEOUT   = "templeton.job.status.timeout";
  public static final String JOB_LIST_TIMEOUT   = "templeton.job.list.timeout";

  /*
   * If task execution time out is configured for submit operation then job may need to
   * be killed on execution time out. These parameters controls the maximum number of
   * retries and retry wait time in seconds for executing the time out task.
   */
  public static final String JOB_TIMEOUT_TASK_RETRY_COUNT = "templeton.job.timeout.task.retry.count";
  public static final String JOB_TIMEOUT_TASK_RETRY_INTERVAL = "templeton.job.timeout.task.retry.interval";

  /**
   * see webhcat-default.xml
   */
  public static final String HIVE_HOME_PATH      = "templeton.hive.home";
  /**
   * see webhcat-default.xml
   */
  public static final String HCAT_HOME_PATH      = "templeton.hcat.home";
  /**
   * is a comma separated list of name=value pairs;
   * In case some value is itself a comma-separated list, the comma needs to
   * be escaped with {@link org.apache.hadoop.util.StringUtils#ESCAPE_CHAR}.  See other usage
   * of escape/unescape methods in {@link org.apache.hadoop.util.StringUtils} in webhcat.
   */
  public static final String HIVE_PROPS_NAME     = "templeton.hive.properties";
  public static final String SQOOP_ARCHIVE_NAME  = "templeton.sqoop.archive";
  public static final String SQOOP_PATH_NAME     = "templeton.sqoop.path";
  public static final String SQOOP_HOME_PATH     = "templeton.sqoop.home";
  public static final String LIB_JARS_NAME       = "templeton.libjars";
  public static final String PIG_ARCHIVE_NAME    = "templeton.pig.archive";
  public static final String PIG_PATH_NAME       = "templeton.pig.path";
  public static final String STREAMING_JAR_NAME  = "templeton.streaming.jar";
  public static final String OVERRIDE_JARS_NAME  = "templeton.override.jars";
  public static final String OVERRIDE_JARS_ENABLED = "templeton.override.enabled";
  public static final String TEMPLETON_CONTROLLER_MR_CHILD_OPTS 
    = "templeton.controller.mr.child.opts";
  public static final String TEMPLETON_CONTROLLER_MR_AM_JAVA_OPTS
    = "templeton.controller.mr.am.java.opts";

  public static final String KERBEROS_SECRET     = "templeton.kerberos.secret";
  public static final String KERBEROS_PRINCIPAL  = "templeton.kerberos.principal";
  public static final String KERBEROS_KEYTAB     = "templeton.kerberos.keytab";

  public static final String CALLBACK_INTERVAL_NAME
    = "templeton.callback.retry.interval";
  public static final String CALLBACK_RETRY_NAME
    = "templeton.callback.retry.attempts";

  //Hadoop property names (set by templeton logic)
  public static final String HADOOP_END_INTERVAL_NAME = "job.end.retry.interval";
  public static final String HADOOP_END_RETRY_NAME    = "job.end.retry.attempts";
  public static final String HADOOP_END_URL_NAME      = "job.end.notification.url";
  public static final String HADOOP_SPECULATIVE_NAME
    = "mapred.map.tasks.speculative.execution";
  public static final String HADOOP_CHILD_JAVA_OPTS = "mapred.child.java.opts";
  public static final String HADOOP_MAP_MEMORY_MB = "mapreduce.map.memory.mb";
  public static final String HADOOP_MR_AM_JAVA_OPTS = "yarn.app.mapreduce.am.command-opts";
  public static final String HADOOP_MR_AM_MEMORY_MB = "yarn.app.mapreduce.am.resource.mb";
  public static final String UNIT_TEST_MODE     = "templeton.unit.test.mode";
  /**
   * comma-separated list of artifacts to add to HADOOP_CLASSPATH evn var in
   * LaunchMapper before launching Hive command
   */
  public static final String HIVE_EXTRA_FILES = "templeton.hive.extra.files";

  public static final String XSRF_FILTER_ENABLED = "templeton.xsrf.filter.enabled";
  public static final String FRAME_OPTIONS_FILETER = "templeton.frame.options.filter";

  private static final Logger LOG = LoggerFactory.getLogger(AppConfig.class);

  public AppConfig() {
    init();
    LOG.info("Using Hadoop version " + VersionInfo.getVersion());
  }

  private void init() {
    for (Map.Entry<String, String> e : System.getenv().entrySet())
      set("env." + e.getKey(), e.getValue());

    String templetonDir = getTempletonDir();
    for (String fname : TEMPLETON_CONF_FILENAMES) {
      logConfigLoadAttempt(templetonDir + File.separator + fname);
      if (! loadOneClasspathConfig(fname))
        loadOneFileConfig(templetonDir, fname);
    }
    String hadoopConfDir = getHadoopConfDir();
    for (String fname : HADOOP_CONF_FILENAMES) {
      logConfigLoadAttempt(hadoopConfDir + File.separator + fname);
      loadOneFileConfig(hadoopConfDir, fname);
    }
    ProxyUserSupport.processProxyuserConfig(this);
    handleHiveProperties();
    LOG.info(dumpEnvironent());
  }
  /**
   * When auto-shipping hive tar (for example when hive query or pig script
   * is submitted via webhcat), Hive client is launched on some remote node where Hive has not
   * been installed.  We need pass some properties to that client to make sure it connects to the
   * right Metastore, configures Tez, etc.  Here we look for such properties in hive config,
   * and set a comma-separated list of key values in {@link #HIVE_PROPS_NAME}.
   * The HIVE_CONF_HIDDEN_LIST should be handled separately too - this also should be copied from
   * the hive config to the webhcat config if not defined there.
   * Note that the user may choose to set the same keys in HIVE_PROPS_NAME directly, in which case
   * those values should take precedence.
   */
  private void handleHiveProperties() {
    HiveConf hiveConf = new HiveConf();//load hive-site.xml from classpath
    List<String> interestingPropNames = Arrays.asList(
        HiveConf.ConfVars.METASTOREURIS.varname,
        HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname,
        HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI.varname,
        HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname,
        HiveConf.ConfVars.HIVE_CONF_HIDDEN_LIST.varname);

    //each items is a "key=value" format
    List<String> webhcatHiveProps = new ArrayList<String>(hiveProps());
    for(String interestingPropName : interestingPropNames) {
      String value = hiveConf.get(interestingPropName);
      if(value != null) {
        boolean found = false;
        for(String whProp : webhcatHiveProps) {
          if(whProp.startsWith(interestingPropName + "=")) {
            found = true;
            break;
          }
        }
        if(!found) {
          webhcatHiveProps.add(interestingPropName + "=" + value);
        }
      }
    }
    StringBuilder hiveProps = new StringBuilder();
    for(String whProp : webhcatHiveProps) {
      //make sure to escape separator char in prop values
      hiveProps.append(hiveProps.length() > 0 ? "," : "").append(StringUtils.escapeString(whProp));
    }
    set(HIVE_PROPS_NAME, hiveProps.toString());
    // Setting the hidden list
    String hiddenProperties = hiveConf.get(HiveConf.ConfVars.HIVE_CONF_HIDDEN_LIST.varname);
    if (this.get(HiveConf.ConfVars.HIVE_CONF_HIDDEN_LIST.varname) == null
        && hiddenProperties!=null) {
      set(HiveConf.ConfVars.HIVE_CONF_HIDDEN_LIST.varname, hiddenProperties);
    }
  }

  private static void logConfigLoadAttempt(String path) {
    LOG.info("Attempting to load config file: " + path);
  }

  /**
   * Dumps all env and config state.  Should be called once on WebHCat start up to facilitate 
   * support/debugging.  Later it may be worth adding a REST call which will return this data.
   */
  private String dumpEnvironent() {
    StringBuilder sb = TempletonUtils.dumpPropMap("========WebHCat System.getenv()========", System.getenv());
    sb.append("START========WebHCat AppConfig.iterator()========: \n");
    HiveConfUtil.dumpConfig(this, sb);
    sb.append("END========WebHCat AppConfig.iterator()========: \n");

    sb.append(TempletonUtils.dumpPropMap("========WebHCat System.getProperties()========", System.getProperties()));

    sb.append(HiveConfUtil.dumpConfig(new HiveConf()));
    return sb.toString();
  }

  public JobsListOrder getListJobsOrder() {
    String requestedOrder = get(TEMPLETON_JOBSLIST_ORDER);
    if (requestedOrder != null) {
      try {
        return JobsListOrder.valueOf(requestedOrder.toLowerCase());
      }
      catch(IllegalArgumentException ex) {
        LOG.warn("Ignoring setting " + TEMPLETON_JOBSLIST_ORDER + " configured with in-correct value " + requestedOrder);
      }
    }

    // Default to lexicographicalasc
    return JobsListOrder.lexicographicalasc;
  }

  public void startCleanup() {
    JobState.getStorageInstance(this).startCleanup(this);
  }

  public String getHadoopConfDir() {
    return get(HADOOP_CONF_DIR);
  }

  public static String getTempletonDir() {
    return System.getenv(TEMPLETON_HOME_VAR);
  }
  public static String getWebhcatConfDir() {
    return System.getenv(WEBHCAT_CONF_DIR);
  }

  private boolean loadOneFileConfig(String dir, String fname) {
    if (dir != null) {
      File f = new File(dir, fname);
      if (f.exists()) {
        addResource(new Path(f.getAbsolutePath()));
        LOG.info("loaded config file " + f.getAbsolutePath());
        return true;
      }
    }
    return false;
  }

  private boolean loadOneClasspathConfig(String fname) {
    URL x = getResource(fname);
    if (x != null) {
      addResource(x);
      LOG.info("loaded config from classpath " + x);
      return true;
    }

    return false;
  }

  public String jettyConfiguration() { return get(JETTY_CONFIGURATION); }
  public String libJars()          { return get(LIB_JARS_NAME); }
  public String hadoopQueueName()  { return get(HADOOP_QUEUE_NAME); }
  public String enableJobReconnectDefault() { return get(ENABLE_JOB_RECONNECT_DEFAULT); }
  public String clusterHadoop()    { return get(HADOOP_NAME); }
  public String clusterHcat()      { return get(HCAT_NAME); }
  public String clusterPython()    { return get(PYTHON_NAME); }
  public String pigPath()          { return get(PIG_PATH_NAME); }
  public String pigArchive()       { return get(PIG_ARCHIVE_NAME); }
  public String hivePath()         { return get(HIVE_PATH_NAME); }
  public String hiveArchive()      { return get(HIVE_ARCHIVE_NAME); }
  public String sqoopPath()        { return get(SQOOP_PATH_NAME); }
  public String sqoopArchive()     { return get(SQOOP_ARCHIVE_NAME); }
  public String sqoopHome()        { return get(SQOOP_HOME_PATH); }
  public String streamingJar()     { return get(STREAMING_JAR_NAME); }
  public String kerberosSecret()   { return get(KERBEROS_SECRET); }
  public String kerberosPrincipal(){ return get(KERBEROS_PRINCIPAL); }
  public String kerberosKeytab()   { return get(KERBEROS_KEYTAB); }
  public String controllerMRChildOpts() { 
    return get(TEMPLETON_CONTROLLER_MR_CHILD_OPTS); 
  }
  public String controllerAMChildOpts() {
    return get(TEMPLETON_CONTROLLER_MR_AM_JAVA_OPTS);
  }
  public String mapperMemoryMb()   { return get(MAPPER_MEMORY_MB); }
  public String amMemoryMb() {
    return get(MR_AM_MEMORY_MB);
  }

  /**
   * @see  #HIVE_PROPS_NAME
   */
  public Collection<String> hiveProps() {
    String[] props= StringUtils.split(get(HIVE_PROPS_NAME));
    //since raw data was (possibly) escaped to make split work,
    //now need to remove escape chars so they don't interfere with downstream processing
    if (props == null) {
      return Collections.emptyList();
    } else {
      for(int i = 0; i < props.length; i++) {
        props[i] = TempletonUtils.unEscapeString(props[i]);
      }
      return Arrays.asList(props);
    }
  }

  public String[] overrideJars() {
    if (getBoolean(OVERRIDE_JARS_ENABLED, true))
      return getStrings(OVERRIDE_JARS_NAME);
    else
      return null;
  }
  public String overrideJarsString() {
    if (getBoolean(OVERRIDE_JARS_ENABLED, true))
      return get(OVERRIDE_JARS_NAME);
    else
      return null;
  }

  public long zkCleanupInterval()  {
    return getLong(ZooKeeperCleanup.ZK_CLEANUP_INTERVAL,
      (1000L * 60L * 60L * 12L));
  }

  public long zkMaxAge() {
    return getLong(ZooKeeperCleanup.ZK_CLEANUP_MAX_AGE,
      (1000L * 60L * 60L * 24L * 7L));
  }

  public String zkHosts()          { return get(ZooKeeperStorage.ZK_HOSTS); }
  public int zkSessionTimeout()    { return getInt(ZooKeeperStorage.ZK_SESSION_TIMEOUT, 30000); }
}
