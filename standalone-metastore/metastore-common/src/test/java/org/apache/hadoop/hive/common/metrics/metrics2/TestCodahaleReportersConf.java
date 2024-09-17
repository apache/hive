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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hive.common.metrics.MetricsTestUtils;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for Codahale reporter config backward compatibility
 */
@org.junit.Ignore("HIVE-23945")
public class TestCodahaleReportersConf {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestCodahaleReportersConf.class);
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

    Configuration conf = new Configuration();

    jsonReportFile = new File(workDir, "json_reporting");
    jsonReportFile.delete();

    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "local");
    conf.set(MetastoreConf.ConfVars.METRICS_CLASS.getHiveName(), CodahaleMetrics.class.getCanonicalName());
    conf.set(MetastoreConf.ConfVars.HIVE_METRICS_REPORTER.getHiveName(), "JMX, JSON");
    conf.set(MetastoreConf.ConfVars.METRICS_JSON_FILE_LOCATION.getHiveName(), jsonReportFile.toString());
    conf.setTimeDuration(MetastoreConf.ConfVars.METRICS_JSON_FILE_INTERVAL.getHiveName(), 100,
        TimeUnit.MILLISECONDS);

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

    Configuration conf = new Configuration();

    jsonReportFile = new File(workDir, "json_reporting");
    jsonReportFile.delete();

    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "local");
    conf.set(MetastoreConf.ConfVars.METRICS_CLASS.getHiveName(), CodahaleMetrics.class.getCanonicalName());
    conf.set(MetastoreConf.ConfVars.HIVE_METRICS_REPORTER.getHiveName(), "JMX, JSON");
    conf.set(MetastoreConf.ConfVars.HIVE_CODAHALE_METRICS_REPORTER_CLASSES.getHiveName(),
        "org.apache.hadoop.hive.common.metrics.metrics2.JmxMetricsReporter");
    conf.set(MetastoreConf.ConfVars.METRICS_JSON_FILE_LOCATION.getHiveName(), jsonReportFile.toString());
    conf.setTimeDuration(MetastoreConf.ConfVars.METRICS_JSON_FILE_INTERVAL.getHiveName(), 100,
        TimeUnit.MILLISECONDS);

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

    Configuration conf = new Configuration();

    jsonReportFile = new File(workDir, "json_reporting");
    jsonReportFile.delete();

    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "local");
    conf.set(MetastoreConf.ConfVars.METRICS_CLASS.getHiveName(), CodahaleMetrics.class.getCanonicalName());
    conf.set(MetastoreConf.ConfVars.HIVE_METRICS_REPORTER.getHiveName(), "JMX, JSON");
    conf.set(MetastoreConf.ConfVars.HIVE_CODAHALE_METRICS_REPORTER_CLASSES.getHiveName(),
        "org.apache.hadoop.hive.common.metrics.NonExistentReporter");
    conf.set(MetastoreConf.ConfVars.METRICS_JSON_FILE_LOCATION.getHiveName(), jsonReportFile.toString());
    conf.setTimeDuration(MetastoreConf.ConfVars.METRICS_JSON_FILE_INTERVAL.getHiveName(), 100,
        TimeUnit.MILLISECONDS);

    try {
      MetricsFactory.init(conf);
    } catch (InvocationTargetException expectedException) {

    }

    Assert.assertFalse(jsonReportFile.exists());
  }

  /**
   * Tests if MetricsFactory is initialised properly when Metrics2Reporter is tried to add more than once.
   *
   */
  @Test
  public void testMetricsFactoryInitMetrics2ReporterAddedTwice() throws Exception {
    Configuration conf = new Configuration();

    jsonReportFile = File.createTempFile("TestCodahaleMetrics", ".json");
    LOGGER.info("Json metrics saved in {}", jsonReportFile.getAbsolutePath());

    conf.set(MetastoreConf.ConfVars.METRICS_CLASS.getHiveName(), CodahaleMetrics.class.getCanonicalName());
    conf.set(MetastoreConf.ConfVars.HIVE_CODAHALE_METRICS_REPORTER_CLASSES.getHiveName(),
            "org.apache.hadoop.hive.common.metrics.metrics2.Metrics2Reporter, "
                    + "org.apache.hadoop.hive.common.metrics.metrics2.Metrics2Reporter");
    conf.set(MetastoreConf.ConfVars.METRICS_JSON_FILE_LOCATION.getHiveName(), jsonReportFile.getAbsolutePath());
    conf.setTimeDuration(MetastoreConf.ConfVars.METRICS_JSON_FILE_INTERVAL.getHiveName(), 2000,
            TimeUnit.MILLISECONDS);

    MetricsFactory.init(conf);
    Assert.assertNotNull(MetricsFactory.getInstance());
    // closing MetricsFactory and re-initiating it to check if everything works fine
    MetricsFactory.close();
    MetricsFactory.init(conf);
    Assert.assertNotNull(MetricsFactory.getInstance());
  }
}
