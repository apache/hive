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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for new Metrics subsystem.
 */
public class TestCodahaleMetrics {

  private static File workDir = new File(System.getProperty("test.tmp.dir"));
  private static File jsonReportFile;
  public static MetricRegistry metricRegistry;

  @Before
  public void before() throws Exception {
    HiveConf conf = new HiveConf();

    jsonReportFile = new File(workDir, "json_reporting");
    jsonReportFile.delete();
    String defaultFsName = ShimLoader.getHadoopShims().getHadoopConfNames().get("HADOOPFS");
    conf.set(defaultFsName, "local");
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_CLASS, CodahaleMetrics.class.getCanonicalName());
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_REPORTER, MetricsReporting.JSON_FILE.name() + "," + MetricsReporting.JMX.name());
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_JSON_FILE_LOCATION, jsonReportFile.toString());
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_JSON_FILE_INTERVAL, "100ms");

    MetricsFactory.init(conf);
    metricRegistry = ((CodahaleMetrics) MetricsFactory.getInstance()).getMetricRegistry();
  }

  @After
  public void after() throws Exception {
    MetricsFactory.close();
  }

  @Test
  public void testScope() throws Exception {
    int runs = 5;
    for (int i = 0; i < runs; i++) {
      MetricsFactory.getInstance().startScope("method1");
      MetricsFactory.getInstance().endScope("method1");
    }

    Timer timer = metricRegistry.getTimers().get("api_method1");
    Assert.assertEquals(5, timer.getCount());
    Assert.assertTrue(timer.getMeanRate() > 0);
  }


  @Test
  public void testCount() throws Exception {
    int runs = 5;
    for (int i = 0; i < runs; i++) {
      MetricsFactory.getInstance().incrementCounter("count1");
    }
    Counter counter = metricRegistry.getCounters().get("count1");
    Assert.assertEquals(5L, counter.getCount());
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
          MetricsFactory.getInstance().startScope("method2");
          MetricsFactory.getInstance().endScope("method2");
          return null;
        }
      });
    }
    executorService.shutdown();
    assertTrue(executorService.awaitTermination(10000, TimeUnit.MILLISECONDS));
    Timer timer = metricRegistry.getTimers().get("api_method2");
    Assert.assertEquals(4, timer.getCount());
    Assert.assertTrue(timer.getMeanRate() > 0);
  }

  @Test
  public void testFileReporting() throws Exception {
    int runs = 5;
    for (int i = 0; i < runs; i++) {
      MetricsFactory.getInstance().incrementCounter("count2");
      Thread.sleep(100);
    }

    Thread.sleep(2000);
    byte[] jsonData = Files.readAllBytes(Paths.get(jsonReportFile.getAbsolutePath()));
    ObjectMapper objectMapper = new ObjectMapper();

    JsonNode rootNode = objectMapper.readTree(jsonData);
    JsonNode countersNode = rootNode.path("counters");
    JsonNode methodCounterNode = countersNode.path("count2");
    JsonNode countNode = methodCounterNode.path("count");
    Assert.assertEquals(countNode.asInt(), 5);
  }
}
