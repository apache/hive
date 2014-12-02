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

import org.apache.commons.compress.utils.CharsetNames;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;

public class HiveSparkClientFactory {
  protected static transient final Log LOG = LogFactory.getLog(HiveSparkClientFactory.class);

  private static final String SPARK_DEFAULT_CONF_FILE = "spark-defaults.conf";
  private static final String SPARK_DEFAULT_MASTER = "local";
  private static final String SPARK_DEFAULT_APP_NAME = "Hive on Spark";

  public static HiveSparkClient createHiveSparkClient(Configuration configuration)
    throws IOException, SparkException {

    Map<String, String> conf = initiateSparkConf(configuration);
    // Submit spark job through local spark context while spark master is local mode, otherwise submit
    // spark job through remote spark context.
    String master = conf.get("spark.master");
    if (master.equals("local") || master.startsWith("local[")) {
      // With local spark context, all user sessions share the same spark context.
      return LocalHiveSparkClient.getInstance(generateSparkConf(conf));
    } else {
      return new RemoteHiveSparkClient(conf);
    }
  }

  public static Map<String, String> initiateSparkConf(Configuration hiveConf) {
    Map<String, String> sparkConf = new HashMap<String, String>();

    // set default spark configurations.
    sparkConf.put("spark.master", SPARK_DEFAULT_MASTER);
    sparkConf.put("spark.app.name", SPARK_DEFAULT_APP_NAME);
    sparkConf.put("spark.serializer",
      "org.apache.spark.serializer.KryoSerializer");
    sparkConf.put("spark.default.parallelism", "1");

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
              "load spark configuration from %s (%s -> %s).",
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

    // load properties from hive configurations.
    for (Map.Entry<String, String> entry : hiveConf) {
      String propertyName = entry.getKey();
      if (propertyName.startsWith("spark")) {
        String value = hiveConf.get(propertyName);
        sparkConf.put(propertyName, value);
        LOG.info(String.format(
          "load spark configuration from hive configuration (%s -> %s).",
          propertyName, value));
      }
    }

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
