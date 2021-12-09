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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

// Stolen from Hive's MetricsTestUtils.
public class MetricsTestUtils {
  public static final MetricsCategory COUNTER = new MetricsCategory("counters", "count");
  public static final MetricsCategory TIMER = new MetricsCategory("timers", "count");
  public static final MetricsCategory GAUGE = new MetricsCategory("gauges", "value");
  public static final MetricsCategory METER = new MetricsCategory("meters", "count");

  static class MetricsCategory {
    String category;
    String metricsHandle;

    MetricsCategory(String category, String metricsHandle) {
      this.category = category;
      this.metricsHandle = metricsHandle;
    }
  }

  public static void verifyMetrics(MetricRegistry metricRegistry, MetricsCategory category, String metricsName,
      Object expectedValue) throws Exception {
    verifyMetricsJson(dumpJson(metricRegistry), category, metricsName, expectedValue);
  }

  public static void verifyMetricsJson(String json, MetricsCategory category, String metricsName, Object expectedValue)
      throws Exception {
    JsonNode jsonNode = getJsonNode(json, category, metricsName);
    Assert.assertTrue(
        String.format("%s.%s.%s should not be empty", category.category, metricsName, category.metricsHandle),
        !jsonNode.asText().isEmpty());

    if (expectedValue != null) {
      Assert.assertEquals(expectedValue.toString(), jsonNode.asText());
    }
  }

  static void verifyMetricsJson(String json, MetricsCategory category, String metricsName) throws Exception {
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

  static String dumpJson(MetricRegistry metricRegistry) throws Exception {
    ObjectMapper jsonMapper =
        new ObjectMapper().registerModule(new MetricsModule(TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS, false));
    return jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(metricRegistry);
  }
}
