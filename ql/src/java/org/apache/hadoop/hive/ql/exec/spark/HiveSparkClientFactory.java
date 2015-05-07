/**
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

import org.apache.commons.compress.utils.CharsetNames;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.spark.client.rpc.RpcConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;

public class HiveSparkClientFactory {
  protected static final transient Log LOG = LogFactory.getLog(HiveSparkClientFactory.class);

  private static final String SPARK_DEFAULT_CONF_FILE = "spark-defaults.conf";
  private static final String SPARK_DEFAULT_MASTER = "yarn-cluster";
  private static final String SPARK_DEFAULT_APP_NAME = "Hive on Spark";
  private static final String SPARK_DEFAULT_SERIALIZER = "org.apache.spark.serializer.KryoSerializer";
  private static final String SPARK_DEFAULT_REFERENCE_TRACKING = "false";

  public static HiveSparkClient createHiveSparkClient(HiveConf hiveconf)
    throws IOException, SparkException {

    Map<String, String> sparkConf = initiateSparkConf(hiveconf);
    // Submit spark job through local spark context while spark master is local mode, otherwise submit
    // spark job through remote spark context.
    String master = sparkConf.get("spark.master");
    if (master.equals("local") || master.startsWith("local[")) {
      // With local spark context, all user sessions share the same spark context.
      return LocalHiveSparkClient.getInstance(generateSparkConf(sparkConf));
    } else {
      return new RemoteHiveSparkClient(hiveconf, sparkConf);
    }
  }

  public static Map<String, String> initiateSparkConf(HiveConf hiveConf) {
    Map<String, String> sparkConf = new HashMap<String, String>();

    // set default spark configurations.
    sparkConf.put("spark.master", SPARK_DEFAULT_MASTER);
    sparkConf.put("spark.app.name", SPARK_DEFAULT_APP_NAME);
    sparkConf.put("spark.serializer", SPARK_DEFAULT_SERIALIZER);
    sparkConf.put("spark.kryo.referenceTracking", SPARK_DEFAULT_REFERENCE_TRACKING);

    // load properties from spark-defaults.conf.
    InputStream inputStream = null;
    try {
      inputStream = HiveSparkClientFactory.class.getClassLoader()
        .getResourceAsStream(SPARK_DEFAULT_CONF_FILE);
      if (inputStream != null) {
        LOG.info("loading spark properties from:" + SPARK_DEFAULT_CONF_FILE);
        Properties properties = new Properties();
        properties.load(new InputStreamReader(inputStream, CharsetNames.UTF_8));
        for (String propertyName : properties.stringPropertyNames()) {
          if (propertyName.startsWith("spark")) {
            String value = properties.getProperty(propertyName);
            sparkConf.put(propertyName, properties.getProperty(propertyName));
            LOG.info(String.format(
              "load spark property from %s (%s -> %s).",
              SPARK_DEFAULT_CONF_FILE, propertyName, value));
          }
        }
      }
    } catch (IOException e) {
      LOG.info("Failed to open spark configuration file:"
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
    }
    if (sparkMaster.equals("yarn-cluster")) {
      sparkConf.put("spark.yarn.maxAppAttempts", "1");
    }
    for (Map.Entry<String, String> entry : hiveConf) {
      String propertyName = entry.getKey();
      if (propertyName.startsWith("spark")) {
        String value = hiveConf.get(propertyName);
        sparkConf.put(propertyName, value);
        LOG.info(String.format(
          "load spark property from hive configuration (%s -> %s).",
          propertyName, value));
      } else if (propertyName.startsWith("yarn") &&
        (sparkMaster.equals("yarn-client") || sparkMaster.equals("yarn-cluster"))) {
        String value = hiveConf.get(propertyName);
        // Add spark.hadoop prefix for yarn properties as SparkConf only accept properties
        // started with spark prefix, Spark would remove spark.hadoop prefix lately and add
        // it to its hadoop configuration.
        sparkConf.put("spark.hadoop." + propertyName, value);
        LOG.info(String.format(
          "load yarn property from hive configuration in %s mode (%s -> %s).",
          sparkMaster, propertyName, value));
      }
      if (RpcConfiguration.HIVE_SPARK_RSC_CONFIGS.contains(propertyName)) {
        String value = RpcConfiguration.getValue(hiveConf, propertyName);
        sparkConf.put(propertyName, value);
        LOG.info(String.format(
          "load RPC property from hive configuration (%s -> %s).",
          propertyName, value));
      }
    }

    Set<String> classes = Sets.newHashSet(
      Splitter.on(",").trimResults().omitEmptyStrings().split(
        Strings.nullToEmpty(sparkConf.get("spark.kryo.classesToRegister"))));
    classes.add(VectorizedRowBatch.class.getName());
    classes.add(BytesWritable.class.getName());
    classes.add(HiveKey.class.getName());
    sparkConf.put(
      "spark.kryo.classesToRegister", Joiner.on(",").join(classes));

    return sparkConf;
  }

  static SparkConf generateSparkConf(Map<String, String> conf) {
    SparkConf sparkConf = new SparkConf(false);
    for (Map.Entry<String, String> entry : conf.entrySet()) {
      sparkConf.set(entry.getKey(), entry.getValue());
    }
    return sparkConf;
  }
}
