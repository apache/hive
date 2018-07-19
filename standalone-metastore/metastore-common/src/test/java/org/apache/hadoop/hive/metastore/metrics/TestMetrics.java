/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.metrics;

import com.codahale.metrics.Counter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

@Category(MetastoreUnitTest.class)
public class TestMetrics {
  private static final long REPORT_INTERVAL = 1;

  @Before
  public void shutdownMetrics() {
    Metrics.shutdown();
  }

  @Test
  public void jsonReporter() throws Exception {
    File jsonReportFile = File.createTempFile("TestMetrics", ".json");
    String jsonFile = jsonReportFile.getAbsolutePath();

    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METRICS_REPORTERS, "json");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METRICS_JSON_FILE_LOCATION, jsonFile);
    MetastoreConf.setTimeVar(conf, MetastoreConf.ConfVars.METRICS_JSON_FILE_INTERVAL, REPORT_INTERVAL,
        TimeUnit.SECONDS);

    Metrics.initialize(conf);
    Counter counter = Metrics.getOrCreateCounter("my-counter");

    for (int i = 0; i < 5; i++) {
      counter.inc();
      // Make sure it has a chance to dump it.
      Thread.sleep(REPORT_INTERVAL * 1000 + REPORT_INTERVAL * 1000 / 2);
      String json = new String(MetricsTestUtils.getFileData(jsonFile, 200, 10));
      MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, "my-counter",
          i + 1);
    }
  }

  @Test
  public void allReporters() throws Exception {
    String jsonFile = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") +
        "TestMetricsOutput.json";
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METRICS_REPORTERS, "json,jmx,console,hadoop");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METRICS_JSON_FILE_LOCATION, jsonFile);

    Metrics.initialize(conf);

    Assert.assertEquals(4, Metrics.getReporters().size());
  }

  @Test
  public void allReportersHiveConfig() throws Exception {
    String jsonFile = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") +
        "TestMetricsOutput.json";
    Configuration conf = MetastoreConf.newMetastoreConf();
    conf.set(MetastoreConf.ConfVars.HIVE_CODAHALE_METRICS_REPORTER_CLASSES.getHiveName(),
        "org.apache.hadoop.hive.common.metrics.metrics2.JsonFileMetricsReporter," +
            "org.apache.hadoop.hive.common.metrics.metrics2.JmxMetricsReporter," +
            "org.apache.hadoop.hive.common.metrics.metrics2.ConsoleMetricsReporter," +
            "org.apache.hadoop.hive.common.metrics.metrics2.Metrics2Reporter");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METRICS_JSON_FILE_LOCATION, jsonFile);

    Metrics.initialize(conf);

    Assert.assertEquals(4, Metrics.getReporters().size());
  }

  @Test
  public void allReportersOldHiveConfig() throws Exception {
    String jsonFile = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") +
        "TestMetricsOutput.json";
    Configuration conf = MetastoreConf.newMetastoreConf();
    conf.set(MetastoreConf.ConfVars.HIVE_METRICS_REPORTER.getHiveName(),
        "JSON_FILE,JMX,CONSOLE,HADOOP2");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METRICS_JSON_FILE_LOCATION, jsonFile);

    Metrics.initialize(conf);

    Assert.assertEquals(4, Metrics.getReporters().size());
  }

  @Test
  public void defaults() throws Exception {
    String jsonFile = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") +
        "TestMetricsOutput.json";
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METRICS_JSON_FILE_LOCATION, jsonFile);
    Metrics.initialize(conf);

    Assert.assertEquals(2, Metrics.getReporters().size());
  }

  // Stolen from Hive's MetricsTestUtils.  Probably should break it out into it's own class.
  private static class MetricsTestUtils {

    static final MetricsCategory COUNTER = new MetricsCategory("counters", "count");
    static final MetricsCategory TIMER = new MetricsCategory("timers", "count");
    static final MetricsCategory GAUGE = new MetricsCategory("gauges", "value");
    static final MetricsCategory METER = new MetricsCategory("meters", "count");

    static class MetricsCategory {
      String category;
      String metricsHandle;
      MetricsCategory(String category, String metricsHandle) {
        this.category = category;
        this.metricsHandle = metricsHandle;
      }
    }

    static void verifyMetricsJson(String json, MetricsCategory category, String metricsName,
                                         Object expectedValue) throws Exception {
      JsonNode jsonNode = getJsonNode(json, category, metricsName);
      Assert.assertEquals(expectedValue.toString(), jsonNode.asText());
    }

    static JsonNode getJsonNode(String json, MetricsCategory category, String metricsName) throws Exception {
      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode rootNode = objectMapper.readTree(json);
      JsonNode categoryNode = rootNode.path(category.category);
      JsonNode metricsNode = categoryNode.path(metricsName);
      return metricsNode.path(category.metricsHandle);
    }

    static byte[] getFileData(String path, int timeoutInterval, int tries) throws Exception {
      File file = new File(path);
      do {
        Thread.sleep(timeoutInterval);
        tries--;
      } while (tries > 0 && !file.exists());
      return Files.readAllBytes(Paths.get(path));
    }
  }
}
