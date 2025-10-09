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

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.testutils.CapturingLogAppender;
import org.apache.logging.log4j.Level;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.hamcrest.MatcherAssert.assertThat;


@Category(MetastoreUnitTest.class)
public class TestMetrics {
  private static final long REPORT_INTERVAL = 1;

  @Before
  public void shutdownMetrics() {
    Metrics.shutdown();
  }

  @Test
  public void slf4jReporter() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METRICS_REPORTERS, "slf4j");
    MetastoreConf.setTimeVar(conf,
        MetastoreConf.ConfVars.METRICS_SLF4J_LOG_FREQUENCY_MINS, REPORT_INTERVAL, TimeUnit.SECONDS);

    // 1. Verify the default level (INFO)
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METRICS_SLF4J_LOG_LEVEL, "INFO");
    validateSlf4jReporter(conf, Level.INFO);

    // 2. Verify an overridden level (DEBUG)
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METRICS_SLF4J_LOG_LEVEL, "DEBUG");
    validateSlf4jReporter(conf, Level.DEBUG);
  }

  private void validateSlf4jReporter(Configuration conf, Level level) throws Exception {
    initializeMetrics(conf);
    Counter counter = Metrics.getOrCreateCounter("my-counter");
    counter.inc(5);
    // Make sure it has a chance to dump it.
    Thread.sleep(REPORT_INTERVAL * 1000 + REPORT_INTERVAL * 1000 / 2);
    final List<String> capturedLogMessages = CapturingLogAppender.findLogMessagesContaining(level,"my-counter");
    Assert.assertTrue("Not a single counter message was logged from metrics when " +
            "configured for SLF4J metric reporting at level " + level + "!",
        capturedLogMessages.size() > 0);
    final String logMessage = capturedLogMessages.get(0);
    Assert.assertTrue("Counter value is incorrect on captured log message: \"" + logMessage + "\"",
        logMessage.contains("count=5"));
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

    initializeMetrics(conf);
    Counter counter = Metrics.getOrCreateCounter("my-counter");
    MapMetrics mapMetrics = Metrics.getOrCreateMapMetrics("my-map");

    for (int i = 0; i < 5; i++) {
      counter.inc();
      mapMetrics.update(ImmutableMap.of("const", 1000, "var", i * 100));
      // Make sure it has a chance to dump it.
      Thread.sleep(REPORT_INTERVAL * 1000 + REPORT_INTERVAL * 1000 / 2);
      String json = new String(MetricsTestUtils.getFileData(jsonFile, 200, 10));
      MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, "my-counter",
          i + 1);
      MetricsTestUtils.verifyMapMetricsJson(json, "my-map",
          ImmutableMap.of("const", 1000, "var", i * 100));
    }
  }

  @Test
  public void testJsonStructure() throws Exception {
    File jsonReportFile = File.createTempFile("TestMetrics", ".json");
    String jsonFile = jsonReportFile.getAbsolutePath();

    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METRICS_REPORTERS, "json");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METRICS_JSON_FILE_LOCATION, jsonFile);
    MetastoreConf.setTimeVar(conf, MetastoreConf.ConfVars.METRICS_JSON_FILE_INTERVAL,
        REPORT_INTERVAL, TimeUnit.SECONDS);

    initializeMetrics(conf);

    Counter openConnections = Metrics.getOpenConnectionsCounter();
    openConnections.inc();

    Thread.sleep(REPORT_INTERVAL * 1000 + REPORT_INTERVAL * 1000 / 2);

    String json = new String(MetricsTestUtils.getFileData(jsonFile, 200, 10));

    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, "buffers.direct.capacity");
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, "memory.heap.used");
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, "threads.count");
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, "classLoading.loaded");

    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER,
        MetricsConstants.OPEN_CONNECTIONS, 1);
  }

  @Test
  public void allReporters() throws Exception {
    String jsonFile = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") +
        "TestMetricsOutput.json";
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METRICS_REPORTERS, "json,jmx,console,hadoop");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METRICS_JSON_FILE_LOCATION, jsonFile);

    initializeMetrics(conf);

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

    initializeMetrics(conf);

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

    initializeMetrics(conf);

    Assert.assertEquals(4, Metrics.getReporters().size());
  }

  @Test
  public void defaults() throws Exception {
    String jsonFile = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") +
        "TestMetricsOutput.json";
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METRICS_JSON_FILE_LOCATION, jsonFile);
    initializeMetrics(conf);

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

    static void verifyMapMetricsJson(String rawJson, String mBeanField,
        Map<String, Integer> expectedValue) throws Exception {
      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode rootNode = objectMapper.readTree(rawJson);
      JsonNode mbeansNode = rootNode.get("mbeans");
      Assert.assertTrue(mbeansNode instanceof ObjectNode);

      JsonNode mBeanObj = mbeansNode.get(mBeanField);
      Assert.assertTrue(mBeanObj instanceof ObjectNode);

      Map<String, Integer> content = objectMapper.convertValue(mBeanObj, new TypeReference<Map<String, Integer>>(){});
      assertThat(content, CoreMatchers.is(expectedValue));
    }

    static void verifyMetricsJson(String json, MetricsCategory category, String metricsName,
        Object expectedValue) throws Exception {
      JsonNode jsonNode = getJsonNode(json, category, metricsName);
      Assert.assertTrue(String.format("%s.%s.%s should not be empty", category.category,
          metricsName, category.metricsHandle), !jsonNode.asText().isEmpty());

      if (expectedValue != null) {
        Assert.assertEquals(expectedValue.toString(), jsonNode.asText());
      }
    }

    static void verifyMetricsJson(String json, MetricsCategory category, String metricsName)
        throws Exception {
      verifyMetricsJson(json, category, metricsName, null);
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

  private void initializeMetrics(Configuration conf) throws Exception {
    Field field = Metrics.class.getDeclaredField("self");
    field.setAccessible(true);

    Constructor<?> cons = Metrics.class.getDeclaredConstructor(Configuration.class);
    cons.setAccessible(true);

    field.set(null, cons.newInstance(conf));
  }
}
