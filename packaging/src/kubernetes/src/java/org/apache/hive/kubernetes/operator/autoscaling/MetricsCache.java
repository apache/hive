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

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe cache for scraped Prometheus metrics from pods.
 * Entries become stale after a configurable duration (typically 3x the scrape interval)
 * and are treated as absent when read.
 */
public class MetricsCache {

  private record CachedResult(List<PodMetrics> metrics, Instant scrapedAt) {}

  private final ConcurrentHashMap<String, CachedResult> cache = new ConcurrentHashMap<>();

  /**
   * Stores scraped metrics for a component.
   *
   * @param key format: "namespace/clusterName/component"
   * @param metrics the scraped pod metrics
   */
  public void put(String key, List<PodMetrics> metrics) {
    cache.put(key, new CachedResult(metrics, Instant.now()));
  }

  /**
   * Returns cached metrics if present and not stale, otherwise an empty list.
   *
   * @param key          format: "namespace/clusterName/component"
   * @param maxStaleSecs maximum age in seconds before the entry is considered stale
   * @return the cached metrics, or an empty list if absent or stale
   */
  public List<PodMetrics> getOrEmpty(String key, int maxStaleSecs) {
    CachedResult result = cache.get(key);
    if (result == null
        || Instant.now().isAfter(result.scrapedAt().plusSeconds(maxStaleSecs))) {
      return Collections.emptyList();
    }
    return result.metrics();
  }

  /**
   * Removes all entries whose key starts with the given prefix.
   * Used for cleanup when a HiveCluster is deleted.
   */
  public void removeByPrefix(String prefix) {
    cache.keySet().removeIf(k -> k.startsWith(prefix));
  }
}
