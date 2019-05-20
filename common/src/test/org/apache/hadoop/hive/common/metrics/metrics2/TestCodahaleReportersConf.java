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
package org.apache.hadoop.hive.common.metrics.metrics2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.InvocationTargetException;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hive.common.metrics.MetricsTestUtils;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * Unit tests for Codahale reporter config backward compatibility
 */
public class TestCodahaleReportersConf {

  private static File workDir = new File(System.getProperty("test.tmp.dir"));
  private static File jsonReportFile;

  @After
  public void after() throws Exception {
    MetricsFactory.close();
  }

  /**
   * Tests that the deprecated HIVE_METRICS_REPORTER config is used if the HIVE_CODAHALE_METRICS_REPORTER_CLASSES is missing.
   */
  @Test
  public void testFallbackToDeprecatedConfig() throws Exception {

    HiveConf conf = new HiveConf();

    jsonReportFile = new File(workDir, "json_reporting");
    jsonReportFile.delete();

    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "local");
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_CLASS, CodahaleMetrics.class.getCanonicalName());
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_REPORTER, "JMX, JSON");
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_JSON_FILE_LOCATION, jsonReportFile.toString());
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_JSON_FILE_INTERVAL, "100ms");

    MetricsFactory.init(conf);

    int runs = 5;
    for (int i = 0; i < runs; i++) {
      MetricsFactory.getInstance().incrementCounter("count2");
    }

    // we expect json file to be updated
    byte[] jsonData = MetricsTestUtils.getFileData(jsonReportFile.getAbsolutePath(), 2000, 3);
    ObjectMapper objectMapper = new ObjectMapper();

    JsonNode rootNode = objectMapper.readTree(jsonData);
    JsonNode countersNode = rootNode.path("counters");
    JsonNode methodCounterNode = countersNode.path("count2");
    JsonNode countNode = methodCounterNode.path("count");
    Assert.assertEquals(countNode.asInt(), 5);
  }

  /**
   * Tests that the deprecated HIVE_METRICS_REPORTER config is not used if
   * HIVE_CODAHALE_METRICS_REPORTER_CLASSES is present.
   *
   * The deprecated config specifies json reporters whereas the newer one doesn't. Validates that
   * the JSON file is not created.
   */
  @Test
  public void testNoFallback() throws Exception {

    HiveConf conf = new HiveConf();

    jsonReportFile = new File(workDir, "json_reporting");
    jsonReportFile.delete();

    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "local");
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_CLASS, CodahaleMetrics.class.getCanonicalName());
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_REPORTER, "JMX, JSON");
    conf.setVar(HiveConf.ConfVars.HIVE_CODAHALE_METRICS_REPORTER_CLASSES,
             "org.apache.hadoop.hive.common.metrics.metrics2.JmxMetricsReporter");
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_JSON_FILE_LOCATION, jsonReportFile.toString());
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_JSON_FILE_INTERVAL, "100ms");

    MetricsFactory.init(conf);

    int runs = 5;
    for (int i = 0; i < runs; i++) {
      MetricsFactory.getInstance().incrementCounter("count2");
    }

    Assert.assertFalse(jsonReportFile.exists());
  }

  /**
   * Tests that the deprecated HIVE_METRICS_REPORTER config is not used if
   * HIVE_CODAHALE_METRICS_REPORTER_CLASSES is present but incorrect.
   *
   * The deprecated config specifies json reporters whereas the newer one doesn't. Validates that
   * the JSON file is not created.
   */
  @Test
  public void testNoFallbackOnIncorrectConf() throws Exception {

    HiveConf conf = new HiveConf();

    jsonReportFile = new File(workDir, "json_reporting");
    jsonReportFile.delete();

    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "local");
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_CLASS, CodahaleMetrics.class.getCanonicalName());
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_REPORTER, "JMX, JSON");
    conf.setVar(HiveConf.ConfVars.HIVE_CODAHALE_METRICS_REPORTER_CLASSES,
        "org.apache.hadoop.hive.common.metrics.NonExistentReporter");
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_JSON_FILE_LOCATION, jsonReportFile.toString());
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_JSON_FILE_INTERVAL, "100ms");

    try {
      MetricsFactory.init(conf);
    } catch (InvocationTargetException expectedException) {

    }

    Assert.assertFalse(jsonReportFile.exists());
  }
}
