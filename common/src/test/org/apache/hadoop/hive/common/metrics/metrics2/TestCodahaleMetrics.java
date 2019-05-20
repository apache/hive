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

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.hive.common.metrics.MetricsTestUtils;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.common.MetricsVariable;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for new Metrics subsystem.
 */
public class TestCodahaleMetrics {

  private static final Path tmpDir = Paths.get(System.getProperty("java.io.tmpdir"));
  private static File jsonReportFile;
  private static MetricRegistry metricRegistry;
  private static final long REPORT_INTERVAL_MS = 100;

  @BeforeClass
  public static void setUp() throws Exception {
    if (!tmpDir.toFile().exists()) {
      System.out.println("Creating directory " + tmpDir);
      Files.createDirectories(tmpDir,
              PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-xr-x")));
    }

    HiveConf conf = new HiveConf();

    jsonReportFile = File.createTempFile("TestCodahaleMetrics", ".json");
    System.out.println("Json metrics saved in " + jsonReportFile.getAbsolutePath());

    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_CLASS, CodahaleMetrics.class.getCanonicalName());
    conf.setVar(HiveConf.ConfVars.HIVE_CODAHALE_METRICS_REPORTER_CLASSES,
        "org.apache.hadoop.hive.common.metrics.metrics2.JsonFileMetricsReporter, "
            + "org.apache.hadoop.hive.common.metrics.metrics2.JmxMetricsReporter");
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_JSON_FILE_LOCATION, jsonReportFile.getAbsolutePath());
    conf.setTimeVar(HiveConf.ConfVars.HIVE_METRICS_JSON_FILE_INTERVAL, REPORT_INTERVAL_MS,
        TimeUnit.MILLISECONDS);

    MetricsFactory.init(conf);
    metricRegistry = ((CodahaleMetrics) MetricsFactory.getInstance()).getMetricRegistry();
  }

  @AfterClass
  public static void cleanup() {
    try {
      MetricsFactory.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    jsonReportFile.delete();
  }

  @Test
  public void testScope() throws Exception {
    int runs = 5;
    for (int i = 0; i < runs; i++) {
      MetricsFactory.getInstance().startStoredScope("method1");
      MetricsFactory.getInstance().endStoredScope("method1");
      Timer timer = metricRegistry.getTimers().get("method1");
      Assert.assertEquals(i + 1, timer.getCount());
    }

    Timer timer = metricRegistry.getTimers().get("method1");
    Assert.assertTrue(timer.getMeanRate() > 0);
  }


  @Test
  public void testCount() throws Exception {
    int runs = 5;
    for (int i = 0; i < runs; i++) {
      MetricsFactory.getInstance().incrementCounter("count1");
      Counter counter = metricRegistry.getCounters().get("count1");
      Assert.assertEquals(i + 1, counter.getCount());
    }
  }

  @Test
  public void testConcurrency() throws Exception {
    int threads = 4;
    ExecutorService executorService = Executors.newFixedThreadPool(threads);
    for (int i=0; i< threads; i++) {
      final int n = i;
      executorService.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          MetricsFactory.getInstance().startStoredScope("method2");
          MetricsFactory.getInstance().endStoredScope("method2");
          return null;
        }
      });
    }
    executorService.shutdown();
    assertTrue(executorService.awaitTermination(10000, TimeUnit.MILLISECONDS));
    Timer timer = metricRegistry.getTimers().get("method2");
    Assert.assertEquals(4, timer.getCount());
    Assert.assertTrue(timer.getMeanRate() > 0);
  }

  /**
   * Test JSON reporter.
   * <ul>
   *   <li>increment the counter value</li>
   *   <li>wait a bit for the new repor to be written</li>
   *   <li>read the value from JSON file</li>
   *   <li>verify that the value matches expectation</li>
   * </ul>
   * This check is repeated a few times to verify that the values are updated over time.
   * @throws Exception if fails to read counter value
   */
  @Test
  public void testFileReporting() throws Exception {
    int runs = 5;
    String  counterName = "count2";
    for (int i = 0; i < runs; i++) {
      MetricsFactory.getInstance().incrementCounter(counterName);
      sleep(REPORT_INTERVAL_MS + REPORT_INTERVAL_MS / 2);
      Assert.assertEquals(i + 1, getCounterValue(counterName));
    }
  }

  class TestMetricsVariable implements MetricsVariable {
    private int gaugeVal;

    @Override
    public Object getValue() {
      return gaugeVal;
    }
    public void setValue(int gaugeVal) {
      this.gaugeVal = gaugeVal;
    }
  };

  @Test
  public void testGauge() throws Exception {
    TestMetricsVariable testVar = new TestMetricsVariable();
    testVar.setValue(20);

    MetricsFactory.getInstance().addGauge("gauge1", testVar);
    String json = ((CodahaleMetrics) MetricsFactory.getInstance()).dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, "gauge1", testVar.getValue());


    testVar.setValue(40);
    json = ((CodahaleMetrics) MetricsFactory.getInstance()).dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, "gauge1", testVar.getValue());
  }

  @Test
  public void testMeter() throws Exception {

    String json = ((CodahaleMetrics) MetricsFactory.getInstance()).dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.METER, "meter", "");

    MetricsFactory.getInstance().markMeter("meter");
    json = ((CodahaleMetrics) MetricsFactory.getInstance()).dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.METER, "meter", "1");

    MetricsFactory.getInstance().markMeter("meter");
    json = ((CodahaleMetrics) MetricsFactory.getInstance()).dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.METER, "meter", "2");
  }

  /**
   * Read counter value from JSON metric report
   * @param name counter name
   * @return counter value
   * @throws FileNotFoundException if file doesn't exist
   */
  private int getCounterValue(String name) throws FileNotFoundException {
    JsonParser parser = new JsonParser();
    JsonElement element = parser.parse(new FileReader(jsonReportFile.getAbsolutePath()));
    JsonObject jobj = element.getAsJsonObject();
    jobj = jobj.getAsJsonObject("counters").getAsJsonObject(name);
    return jobj.get("count").getAsInt();
  }
}
