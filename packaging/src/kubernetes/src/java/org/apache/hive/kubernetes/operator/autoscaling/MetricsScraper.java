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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scrapes Prometheus-format metrics from JMX Exporter endpoints on pods.
 * Uses pod IPs directly (no Service or Prometheus intermediary).
 * All pods are scraped concurrently to avoid blocking the reconciler loop.
 */
public class MetricsScraper {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsScraper.class);
  private static final Duration TIMEOUT = Duration.ofSeconds(5);

  private final KubernetesClient client;
  private final HttpClient httpClient;

  public MetricsScraper(KubernetesClient client) {
    this.client = client;
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(TIMEOUT)
        .build();
  }

  /**
   * Scrape metrics from all ready pods matching the given label selector.
   * Pods are scraped concurrently — total wall-clock time is bounded by
   * a single pod's timeout (5s) regardless of pod count.
   *
   * @param namespace the namespace to query
   * @param selector  label selector (e.g., app.kubernetes.io/component=hiveserver2)
   * @param metricsPort the port on which the Prometheus JMX Exporter serves metrics
   * @return list of per-pod metrics (empty if no pods or all fail)
   */
  public List<PodMetrics> scrape(String namespace, Map<String, String> selector, int metricsPort) {
    List<Pod> pods;
    try {
      pods = client.pods()
          .inNamespace(namespace)
          .withLabels(selector)
          .list()
          .getItems();
    } catch (Exception e) {
      LOG.warn("Failed to list pods in {}/{}: {}", namespace, selector, e.getMessage());
      return Collections.emptyList();
    }

    // Filter to ready pods with IPs
    List<Pod> scrapeable = new ArrayList<>();
    for (Pod pod : pods) {
      if (isPodReady(pod) && pod.getStatus().getPodIP() != null
          && !pod.getStatus().getPodIP().isEmpty()) {
        scrapeable.add(pod);
      }
    }

    if (scrapeable.isEmpty()) {
      return Collections.emptyList();
    }

    // Scrape all pods concurrently
    List<CompletableFuture<PodMetrics>> futures = new ArrayList<>(scrapeable.size());
    for (Pod pod : scrapeable) {
      String podName = pod.getMetadata().getName();
      String podIp = pod.getStatus().getPodIP();
      futures.add(fetchMetricsAsync(podIp, metricsPort)
          .thenApply(body -> new PodMetrics(podName, PrometheusTextParser.parse(body)))
          .exceptionally(ex -> {
            LOG.debug("Failed to scrape metrics from pod {}: {}", podName, ex.getMessage());
            return null;
          }));
    }

    // Wait for all to complete (bounded by TIMEOUT per pod, but all run in parallel)
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

    List<PodMetrics> results = new ArrayList<>();
    for (CompletableFuture<PodMetrics> f : futures) {
      PodMetrics pm = f.join();
      if (pm != null) {
        results.add(pm);
      }
    }
    return results;
  }

  private CompletableFuture<String> fetchMetricsAsync(String podIp, int metricsPort) {
    URI uri = URI.create("http://" + podIp + ":" + metricsPort + "/metrics");
    HttpRequest request = HttpRequest.newBuilder()
        .uri(uri)
        .timeout(TIMEOUT)
        .GET()
        .build();
    return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
        .thenApply(response -> {
          if (response.statusCode() != 200) {
            throw new RuntimeException("HTTP " + response.statusCode() + " from " + uri);
          }
          return response.body();
        });
  }

  public static boolean isPodReady(Pod pod) {
    if (pod.getStatus() == null || pod.getStatus().getConditions() == null) {
      return false;
    }
    return pod.getStatus().getConditions().stream()
        .anyMatch(c -> "Ready".equals(c.getType()) && "True".equals(c.getStatus()));
  }
}
