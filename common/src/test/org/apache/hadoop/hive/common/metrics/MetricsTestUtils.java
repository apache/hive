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
package org.apache.hadoop.hive.common.metrics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utilities for codahale metrics verification.
 */
public class MetricsTestUtils {

  public static final MetricsCategory COUNTER = new MetricsCategory("counters", "count");
  public static final MetricsCategory TIMER = new MetricsCategory("timers", "count");
  public static final MetricsCategory GAUGE = new MetricsCategory("gauges", "value");

  static class MetricsCategory {
    String category;
    String metricsHandle;
    MetricsCategory(String category, String metricsHandle) {
      this.category = category;
      this.metricsHandle = metricsHandle;
    }
  }

  public static void verifyMetricFile(File jsonReportFile, MetricsCategory category, String metricsName,
    Object expectedValue) throws Exception {
    JsonNode jsonNode = getJsonNode(jsonReportFile, category, metricsName);
    Assert.assertEquals(expectedValue.toString(), jsonNode.asText());
  }

  private static JsonNode getJsonNode(File jsonReportFile, MetricsCategory category, String metricsName) throws Exception {
    byte[] jsonData = Files.readAllBytes(Paths.get(jsonReportFile.getAbsolutePath()));
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode rootNode = objectMapper.readTree(jsonData);
    JsonNode categoryNode = rootNode.path(category.category);
    JsonNode metricsNode = categoryNode.path(metricsName);
    return metricsNode.path(category.metricsHandle);
  }
}
