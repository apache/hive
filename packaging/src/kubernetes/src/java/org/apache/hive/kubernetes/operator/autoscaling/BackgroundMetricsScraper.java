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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs periodic metrics scraping in the background so that the JOSDK reconcile
 * thread is never blocked by HTTP calls to pod JMX exporters.
 * <p>
 * Each component gets its own scheduled task that writes results to a shared
 * {@link MetricsCache}. The reconciler reads from that cache (non-blocking).
 */
public class BackgroundMetricsScraper {

  private static final Logger LOG = LoggerFactory.getLogger(BackgroundMetricsScraper.class);

  private final ScheduledExecutorService scheduler;
  private final MetricsScraper scraper;
  private final MetricsCache cache;
  // Key: "namespace/clusterName/component" → active scrape task
  private final ConcurrentHashMap<String, ScheduledFuture<?>> activeTasks =
      new ConcurrentHashMap<>();
  // Tracks registered intervals to detect spec changes
  private final ConcurrentHashMap<String, Integer> registeredIntervals =
      new ConcurrentHashMap<>();

  public BackgroundMetricsScraper(MetricsScraper scraper, MetricsCache cache) {
    this.scraper = scraper;
    this.cache = cache;
    this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "hive-metrics-scraper");
      t.setDaemon(true);
      return t;
    });
  }

  /**
   * Registers (or updates) a periodic scrape task for a component.
   * Idempotent — only recreates the task if the interval has changed.
   *
   * @param namespace     the Kubernetes namespace
   * @param clusterName   the HiveCluster name
   * @param component     the component name (e.g., "hiveserver2")
   * @param selector      label selector for pod listing
   * @param metricsPort   the JMX exporter port
   * @param intervalSecs  how often to scrape (from AutoscalingSpec)
   */
  public void registerOrUpdate(String namespace, String clusterName,
      String component, Map<String, String> selector,
      int metricsPort, int intervalSecs) {
    String key = namespace + "/" + clusterName + "/" + component;
    Integer existing = registeredIntervals.get(key);
    if (existing != null && existing == intervalSecs) {
      return; // Already registered with same interval
    }

    // Cancel existing task if interval changed
    ScheduledFuture<?> oldTask = activeTasks.remove(key);
    if (oldTask != null) {
      oldTask.cancel(false);
    }

    ScheduledFuture<?> future = scheduler.scheduleWithFixedDelay(
        () -> scrapeAndStore(key, namespace, selector, metricsPort),
        0, intervalSecs, TimeUnit.SECONDS);

    activeTasks.put(key, future);
    registeredIntervals.put(key, intervalSecs);
    LOG.debug("Registered background scrape for {} (interval={}s)", key, intervalSecs);
  }

  /**
   * Unregisters all scrape tasks for a deleted cluster.
   */
  public void unregisterCluster(String namespace, String clusterName) {
    String prefix = namespace + "/" + clusterName + "/";
    activeTasks.entrySet().removeIf(entry -> {
      if (entry.getKey().startsWith(prefix)) {
        entry.getValue().cancel(false);
        return true;
      }
      return false;
    });
    registeredIntervals.keySet().removeIf(k -> k.startsWith(prefix));
    cache.removeByPrefix(prefix);
    LOG.debug("Unregistered background scrape tasks for {}/{}", namespace, clusterName);
  }

  /**
   * Shuts down the background scheduler. Called on operator shutdown.
   */
  public void shutdown() {
    scheduler.shutdownNow();
  }

  private void scrapeAndStore(String key, String namespace,
      Map<String, String> selector, int metricsPort) {
    try {
      List<PodMetrics> metrics = scraper.scrape(namespace, selector, metricsPort);
      cache.put(key, metrics);
    } catch (Exception e) {
      // Do not update cache on failure — staleness check handles it
      LOG.debug("Background scrape failed for {}: {}", key, e.getMessage());
    }
  }
}
