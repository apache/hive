/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.spark;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.compress.utils.CharsetNames;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.spark.client.SparkClientUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hive.spark.client.rpc.RpcConfiguration;
import org.apache.spark.SparkConf;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;

public class HiveSparkClientFactory {
  protected static final transient Logger LOG = LoggerFactory.getLogger(HiveSparkClientFactory.class);

  private static final String SPARK_DEFAULT_CONF_FILE = "spark-defaults.conf";
  private static final String SPARK_DEFAULT_MASTER = "yarn";
  private static final String SPARK_DEFAULT_DEPLOY_MODE = "cluster";
  private static final String SPARK_DEFAULT_APP_NAME = "Hive on Spark";
  private static final String SPARK_DEFAULT_SERIALIZER = "org.apache.spark.serializer.KryoSerializer";
  private static final String SPARK_DEFAULT_REFERENCE_TRACKING = "false";
  private static final String SPARK_WAIT_APP_COMPLETE = "spark.yarn.submit.waitAppCompletion";
  private static final String SPARK_DEPLOY_MODE = "spark.submit.deployMode";
  @VisibleForTesting
  public static final String SPARK_CLONE_CONFIGURATION = "spark.hadoop.cloneConf";

  public static HiveSparkClient createHiveSparkClient(HiveConf hiveconf, String sessionId) throws Exception {
    Map<String, String> sparkConf = initiateSparkConf(hiveconf, sessionId);

    // Submit spark job through local spark context while spark master is local mode, otherwise submit
    // spark job through remote spark context.
    String master = sparkConf.get("spark.master");
    if (master.equals("local") || master.startsWith("local[")) {
      // With local spark context, all user sessions share the same spark context.
      return LocalHiveSparkClient.getInstance(generateSparkConf(sparkConf), hiveconf);
    } else {
      return new RemoteHiveSparkClient(hiveconf, sparkConf, sessionId);
    }
  }

  public static Map<String, String> initiateSparkConf(HiveConf hiveConf, String sessionId) {
    Map<String, String> sparkConf = new HashMap<String, String>();
    HBaseConfiguration.addHbaseResources(hiveConf);

    // set default spark configurations.
    sparkConf.put("spark.master", SPARK_DEFAULT_MASTER);
    final String appNameKey = "spark.app.name";
    String appName = hiveConf.get(appNameKey);
    final String sessionIdString = " (sessionId = " + sessionId + ")";
    if (appName == null) {
      if (sessionId == null) {
        appName = SPARK_DEFAULT_APP_NAME;
      } else {
        appName = SPARK_DEFAULT_APP_NAME + sessionIdString;
      }
    } else {
      appName = appName + sessionIdString;
    }
    sparkConf.put(appNameKey, appName);
    sparkConf.put("spark.serializer", SPARK_DEFAULT_SERIALIZER);
    sparkConf.put("spark.kryo.referenceTracking", SPARK_DEFAULT_REFERENCE_TRACKING);

    // load properties from spark-defaults.conf.
    InputStream inputStream = null;
    try {
      inputStream = HiveSparkClientFactory.class.getClassLoader()
        .getResourceAsStream(SPARK_DEFAULT_CONF_FILE);
      if (inputStream != null) {
        LOG.info("loading spark properties from: " + SPARK_DEFAULT_CONF_FILE);
        Properties properties = new Properties();
        properties.load(new InputStreamReader(inputStream, CharsetNames.UTF_8));
        for (String propertyName : properties.stringPropertyNames()) {
          if (propertyName.startsWith("spark")) {
            String value = properties.getProperty(propertyName);
            sparkConf.put(propertyName, properties.getProperty(propertyName));
            LOG.debug(String.format(
              "load spark property from %s (%s -> %s).",
              SPARK_DEFAULT_CONF_FILE, propertyName, LogUtils.maskIfPassword(propertyName,value)));
          }
        }
      }
    } catch (IOException e) {
      LOG.info("Failed to open spark configuration file: "
        + SPARK_DEFAULT_CONF_FILE, e);
    } finally {
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (IOException e) {
          LOG.debug("Failed to close inputstream.", e);
        }
      }
    }

    // load properties from hive configurations, including both spark.* properties,
    // properties for remote driver RPC, and yarn properties for Spark on YARN mode.
    String sparkMaster = hiveConf.get("spark.master");
    if (sparkMaster == null) {
      sparkMaster = sparkConf.get("spark.master");
      hiveConf.set("spark.master", sparkMaster);
    }
    String deployMode = null;
    if (!SparkClientUtilities.isLocalMaster(sparkMaster)) {
      deployMode = hiveConf.get(SPARK_DEPLOY_MODE);
      if (deployMode == null) {
        deployMode = sparkConf.get(SPARK_DEPLOY_MODE);
        if (deployMode == null) {
          deployMode = SparkClientUtilities.getDeployModeFromMaster(sparkMaster);
        }
        if (deployMode == null) {
          deployMode = SPARK_DEFAULT_DEPLOY_MODE;
        }
        hiveConf.set(SPARK_DEPLOY_MODE, deployMode);
      }
    }
    if (SessionState.get() != null && SessionState.get().getConf() != null) {
      SessionState.get().getConf().set("spark.master", sparkMaster);
      if (deployMode != null) {
        SessionState.get().getConf().set(SPARK_DEPLOY_MODE, deployMode);
      }
    }
    if (SparkClientUtilities.isYarnClusterMode(sparkMaster, deployMode)) {
      sparkConf.put("spark.yarn.maxAppAttempts", "1");
    }
    for (Map.Entry<String, String> entry : hiveConf) {
      String propertyName = entry.getKey();
      if (propertyName.startsWith("spark")) {
        String value = hiveConf.get(propertyName);
        sparkConf.put(propertyName, value);
        LOG.debug(String.format(
          "load spark property from hive configuration (%s -> %s).",
          propertyName, LogUtils.maskIfPassword(propertyName,value)));
      } else if (propertyName.startsWith("yarn") &&
          SparkClientUtilities.isYarnMaster(sparkMaster)) {
        String value = hiveConf.get(propertyName);
        // Add spark.hadoop prefix for yarn properties as SparkConf only accept properties
        // started with spark prefix, Spark would remove spark.hadoop prefix lately and add
        // it to its hadoop configuration.
        sparkConf.put("spark.hadoop." + propertyName, value);
        LOG.debug(String.format(
          "load yarn property from hive configuration in %s mode (%s -> %s).",
          sparkMaster, propertyName, LogUtils.maskIfPassword(propertyName,value)));
      } else if (propertyName.equals(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY)) {
        String value = hiveConf.get(propertyName);
        if (value != null && !value.isEmpty()) {
          sparkConf.put("spark.hadoop." + propertyName, value);
        }
      } else if (propertyName.startsWith("hbase") || propertyName.startsWith("zookeeper.znode")) {
        // Add HBase related configuration to Spark because in security mode, Spark needs it
        // to generate hbase delegation token for Spark. This is a temp solution to deal with
        // Spark problem.
        String value = hiveConf.get(propertyName);
        sparkConf.put("spark.hadoop." + propertyName, value);
        LOG.debug(String.format(
          "load HBase configuration (%s -> %s).", propertyName, LogUtils.maskIfPassword(propertyName,value)));
      } else if (propertyName.startsWith("oozie")) {
        String value = hiveConf.get(propertyName);
        sparkConf.put("spark." + propertyName, value);
        LOG.debug(String.format(
          "Pass Oozie configuration (%s -> %s).", propertyName, LogUtils.maskIfPassword(propertyName,value)));
      }

      if (RpcConfiguration.HIVE_SPARK_RSC_CONFIGS.contains(propertyName)) {
        String value = RpcConfiguration.getValue(hiveConf, propertyName);
        sparkConf.put(propertyName, value);
        LOG.debug(String.format(
          "load RPC property from hive configuration (%s -> %s).",
          propertyName, LogUtils.maskIfPassword(propertyName,value)));
      }
    }

    final boolean optShuffleSerDe = hiveConf.getBoolVar(
        HiveConf.ConfVars.SPARK_OPTIMIZE_SHUFFLE_SERDE);

    Set<String> classes = Sets.newHashSet(
        Splitter.on(",").trimResults().omitEmptyStrings().split(
            Strings.nullToEmpty(sparkConf.get("spark.kryo.classesToRegister"))));
    classes.add(Writable.class.getName());
    classes.add(VectorizedRowBatch.class.getName());
    if (!optShuffleSerDe) {
      classes.add(HiveKey.class.getName());
      classes.add(BytesWritable.class.getName());
    } else {
      sparkConf.put("spark.kryo.registrator", SparkClientUtilities.HIVE_KRYO_REG_NAME);
    }
    sparkConf.put("spark.kryo.classesToRegister", Joiner.on(",").join(classes));

    // set yarn queue name
    final String sparkQueueNameKey = "spark.yarn.queue";
    if (SparkClientUtilities.isYarnMaster(sparkMaster) && hiveConf.get(sparkQueueNameKey) == null) {
      String queueName = hiveConf.get("mapreduce.job.queuename");
      if (queueName != null) {
        sparkConf.put(sparkQueueNameKey, queueName);
      }
    }

    // Disable it to avoid verbose app state report in yarn-cluster mode
    if (SparkClientUtilities.isYarnClusterMode(sparkMaster, deployMode) &&
        sparkConf.get(SPARK_WAIT_APP_COMPLETE) == null) {
      sparkConf.put(SPARK_WAIT_APP_COMPLETE, "false");
    }

    // Force Spark configs to be cloned by default
    sparkConf.putIfAbsent(SPARK_CLONE_CONFIGURATION, "true");


    // Set the credential provider passwords if found, if there is job specific password
    // the credential provider location is set directly in the execute method of LocalSparkClient
    // and submit method of RemoteHiveSparkClient when the job config is created
    String password = HiveConfUtil.getJobCredentialProviderPassword(hiveConf);
    if(password != null) {
      addCredentialProviderPassword(sparkConf, password);
    }
    return sparkConf;
  }

  private static void addCredentialProviderPassword(Map<String, String> sparkConf,
      String jobCredstorePassword) {
    sparkConf.put("spark.yarn.appMasterEnv.HADOOP_CREDSTORE_PASSWORD", jobCredstorePassword);
    sparkConf.put("spark.executorEnv.HADOOP_CREDSTORE_PASSWORD", jobCredstorePassword);
  }

  static SparkConf generateSparkConf(Map<String, String> conf) {
    SparkConf sparkConf = new SparkConf(false);
    for (Map.Entry<String, String> entry : conf.entrySet()) {
      sparkConf.set(entry.getKey(), entry.getValue());
    }
    return sparkConf;
  }
}
