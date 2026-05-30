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

package org.apache.hive.kubernetes.operator.autoscaling;

import java.util.HashMap;
import java.util.Map;

/**
 * Parses Prometheus text exposition format (from JMX Exporter /metrics).
 * Only extracts metric name → value pairs; labels are stripped.
 * For metrics with labels, the full line (name + labels) is used as key.
 */
public final class PrometheusTextParser {

  private PrometheusTextParser() {
  }

  /**
   * Parse Prometheus text format into metric-name → value map.
   * Lines with labels are keyed as "metric_name{labels}" to preserve identity.
   * Duplicate metric names (e.g. from multiple label sets) are summed.
   */
  public static Map<String, Double> parse(String body) {
    Map<String, Double> result = new HashMap<>();
    if (body == null || body.isEmpty()) {
      return result;
    }
    for (String line : body.split("\n")) {
      if (line.isEmpty() || line.charAt(0) == '#') {
        continue;
      }
      // Format: metric_name[{labels}] value [timestamp]
      // We extract metric_name (without labels) and value.
      String metricKey;
      String valuePart;
      int braceStart = line.indexOf('{');
      if (braceStart >= 0) {
        int braceEnd = line.indexOf('}', braceStart);
        if (braceEnd < 0) {
          continue;
        }
        metricKey = line.substring(0, braceStart);
        valuePart = line.substring(braceEnd + 1).trim();
      } else {
        int spaceIdx = line.indexOf(' ');
        if (spaceIdx < 0) {
          continue;
        }
        metricKey = line.substring(0, spaceIdx);
        valuePart = line.substring(spaceIdx + 1).trim();
      }
      // valuePart may contain "value timestamp" — take only value
      int spaceInValue = valuePart.indexOf(' ');
      if (spaceInValue > 0) {
        valuePart = valuePart.substring(0, spaceInValue);
      }
      try {
        double value = Double.parseDouble(valuePart);
        // Sum duplicates (multiple label sets for same metric name)
        result.merge(metricKey, value, Double::sum);
      } catch (NumberFormatException e) {
        // Skip NaN, +Inf, -Inf, or malformed values
      }
    }
    return result;
  }

  /**
   * Parse and return per-label-set metrics (preserving labels in key).
   * Key format: "metric_name{label=value,...}"
   */
  public static Map<String, Double> parseWithLabels(String body) {
    Map<String, Double> result = new HashMap<>();
    if (body == null || body.isEmpty()) {
      return result;
    }
    for (String line : body.split("\n")) {
      if (line.isEmpty() || line.charAt(0) == '#') {
        continue;
      }
      String metricKey;
      String valuePart;
      int braceStart = line.indexOf('{');
      if (braceStart >= 0) {
        int braceEnd = line.indexOf('}', braceStart);
        if (braceEnd < 0) {
          continue;
        }
        metricKey = line.substring(0, braceEnd + 1);
        valuePart = line.substring(braceEnd + 1).trim();
      } else {
        int spaceIdx = line.indexOf(' ');
        if (spaceIdx < 0) {
          continue;
        }
        metricKey = line.substring(0, spaceIdx);
        valuePart = line.substring(spaceIdx + 1).trim();
      }
      int spaceInValue = valuePart.indexOf(' ');
      if (spaceInValue > 0) {
        valuePart = valuePart.substring(0, spaceInValue);
      }
      try {
        double value = Double.parseDouble(valuePart);
        result.put(metricKey, value);
      } catch (NumberFormatException e) {
        // Skip
      }
    }
    return result;
  }
}
