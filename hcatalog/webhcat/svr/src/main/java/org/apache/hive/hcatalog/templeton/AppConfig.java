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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton;

import java.io.File;
import java.io.StringBufferInputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hive.hcatalog.templeton.tool.JobState;
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

  public static final String PORT                = "templeton.port";
  public static final String EXEC_ENCODING_NAME  = "templeton.exec.encoding";
  public static final String EXEC_ENVS_NAME      = "templeton.exec.envs";
  public static final String EXEC_MAX_BYTES_NAME = "templeton.exec.max-output-bytes";
  public static final String EXEC_MAX_PROCS_NAME = "templeton.exec.max-procs";
  public static final String EXEC_TIMEOUT_NAME   = "templeton.exec.timeout";
  public static final String HADOOP_QUEUE_NAME   = "templeton.hadoop.queue.name";
  public static final String HADOOP_NAME         = "templeton.hadoop";
  public static final String HADOOP_CONF_DIR     = "templeton.hadoop.conf.dir";
  public static final String HCAT_NAME           = "templeton.hcat";
  public static final String PYTHON_NAME         = "templeton.python";
  public static final String HIVE_ARCHIVE_NAME   = "templeton.hive.archive";
  public static final String HIVE_PATH_NAME      = "templeton.hive.path";
  /**
   * see webhcat-default.xml
   */
  public static final String HIVE_HOME_PATH      = "templeton.hive.home";
  /**
   * see webhcat-default.xml
   */
  public static final String HCAT_HOME_PATH      = "templeton.hcat.home";
  public static final String HIVE_PROPS_NAME     = "templeton.hive.properties";
  public static final String LIB_JARS_NAME       = "templeton.libjars";
  public static final String PIG_ARCHIVE_NAME    = "templeton.pig.archive";
  public static final String PIG_PATH_NAME       = "templeton.pig.path";
  public static final String STREAMING_JAR_NAME  = "templeton.streaming.jar";
  public static final String TEMPLETON_JAR_NAME  = "templeton.jar";
  public static final String OVERRIDE_JARS_NAME  = "templeton.override.jars";
  public static final String OVERRIDE_JARS_ENABLED = "templeton.override.enabled";
  public static final String TEMPLETON_CONTROLLER_MR_CHILD_OPTS 
    = "templeton.controller.mr.child.opts";

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
  public static final String UNIT_TEST_MODE     = "templeton.unit.test.mode";


  private static final Log LOG = LogFactory.getLog(AppConfig.class);

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
    LOG.info(dumpEnvironent());
  }
  private static void logConfigLoadAttempt(String path) {
    LOG.info("Attempting to load config file: " + path);
  }

  /**
   * Dumps all env and config state.  Should be called once on WebHCat start up to facilitate 
   * support/debugging.  Later it may be worth adding a REST call which will return this data.
   */
  private String dumpEnvironent() {
    StringBuilder sb = new StringBuilder("WebHCat environment:\n");
    Map<String, String> env = System.getenv();
    List<String> propKeys = new ArrayList<String>(env.keySet());
    Collections.sort(propKeys);
    for(String propKey : propKeys) {
      sb.append(propKey).append('=').append(env.get(propKey)).append('\n');
    }
    sb.append("Configration properties: \n");
    Iterator<Map.Entry<String, String>> configIter = this.iterator();
    List<Map.Entry<String, String>> configVals = new ArrayList<Map.Entry<String, String>>();
    while(configIter.hasNext()) {
      configVals.add(configIter.next());
    }
    Collections.sort(configVals, new Comparator<Map.Entry<String, String>> () {
      @Override
      public int compare(Map.Entry<String, String> ent, Map.Entry<String, String> ent2) {
        return ent.getKey().compareTo(ent2.getKey());
      }
    });
    for(Map.Entry<String, String> entry : configVals) {
      sb.append(entry.getKey()).append('=').append(entry.getValue()).append('\n');
    }
    return sb.toString();
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

  public String templetonJar()     { return get(TEMPLETON_JAR_NAME); }
  public String libJars()          { return get(LIB_JARS_NAME); }
  public String hadoopQueueName()  { return get(HADOOP_QUEUE_NAME); }
  public String clusterHadoop()    { return get(HADOOP_NAME); }
  public String clusterHcat()      { return get(HCAT_NAME); }
  public String clusterPython()    { return get(PYTHON_NAME); }
  public String pigPath()          { return get(PIG_PATH_NAME); }
  public String pigArchive()       { return get(PIG_ARCHIVE_NAME); }
  public String hivePath()         { return get(HIVE_PATH_NAME); }
  public String hiveArchive()      { return get(HIVE_ARCHIVE_NAME); }
  public String streamingJar()     { return get(STREAMING_JAR_NAME); }
  public String kerberosSecret()   { return get(KERBEROS_SECRET); }
  public String kerberosPrincipal(){ return get(KERBEROS_PRINCIPAL); }
  public String kerberosKeytab()   { return get(KERBEROS_KEYTAB); }
  public String controllerMRChildOpts() { 
    return get(TEMPLETON_CONTROLLER_MR_CHILD_OPTS); 
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
