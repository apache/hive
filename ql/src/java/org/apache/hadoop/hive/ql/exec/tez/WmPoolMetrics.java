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
package org.apache.hadoop.hive.ql.exec.tez;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.common.MetricsVariable;
import org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.impl.MsInfo;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableMetric;

/**
 * A wrapper for metrics for single WM pool. This outputs metrics both to Codahale and standard
 * Hadoop metrics, in parallel. The codahale output is prefixed with pool name and is mostly
 * for the JMX view, or to look at when Hadoop metrics are not set up. Hadoop metrics are output
 * because they can be tagged properly rather than prefixed, so they are better for dashboards.
 */
public class WmPoolMetrics implements MetricsSource {
  private final String poolName, sourceName;
  private MetricsSystem ms;
  @SuppressWarnings("unused") // Metrics system will get this via reflection 0_o
  private final MetricsRegistry registry;

  // Codahale. We just include the pool name in the counter name.
  private List<String> codahaleGaugeNames;
  private Map<String, MutableMetric> allMetrics;

  @Metric("Number of guaranteed cluster executors given to queries")
  MutableGaugeInt numExecutors;
  @Metric("Number of guaranteed cluster executors allocated")
  MutableGaugeInt numExecutorsMax;
  @Metric("Number of parallel queries allowed to run")
  MutableGaugeInt numParallelQueries;
  @Metric("Number of queries running")
  MutableCounterInt numRunningQueries;
  @Metric("Number of queries queued")
  MutableCounterInt numQueuedQueries;

  // TODO: these would need to be propagated from AM via progress.
  // @Metric("Number of allocated guaranteed executors in use"),
  // @Metric("Number of speculative executors in use")

  public WmPoolMetrics(String poolName, MetricsSystem ms) {
    this.poolName = poolName;
    this.sourceName = "WmPoolMetrics." + poolName;
    this.ms = ms;

    this.registry = new MetricsRegistry(sourceName);
  }


  public void initAfterRegister() {
    // Make sure we capture the same metrics as Hadoop2 metrics system, via annotations.
    if (allMetrics != null) return;
    allMetrics = new HashMap<>();
    for (Field field : this.getClass().getDeclaredFields()) {
      for (Annotation annotation : field.getAnnotations()) {
        if (!(annotation instanceof Metric)) continue;
        try {
          field.setAccessible(true);
          allMetrics.put(field.getName(), (MutableMetric) field.get(this));
        } catch (IllegalAccessException ex) {
          break; // Not expected, access by the same class.
        }
        break;
      }
    }

    // Set up codahale if enabled; we cannot tag the values so just prefix them for the JMX view.
    Metrics chMetrics = MetricsFactory.getInstance();
    if (!(chMetrics instanceof CodahaleMetrics)) return;

    List<String> codahaleNames = new ArrayList<>();
    for (Map.Entry<String, MutableMetric> e : allMetrics.entrySet()) {
      MutableMetric metric = e.getValue();
      MetricsVariable<?> var = null;
      if (metric instanceof MutableCounterInt) {
        var = new CodahaleCounterWrapper((MutableCounterInt) metric);
      } else if (metric instanceof MutableGaugeInt) {
        var = new CodahaleGaugeWrapper((MutableGaugeInt) metric);
      }
      if (var == null) continue; // Unexpected metric type.
      String name = "WM_" + poolName + "_" + e.getKey();
      codahaleNames.add(name);
      chMetrics.addGauge(name, var);
    }
    this.codahaleGaugeNames = codahaleNames;
  }


  public void setParallelQueries(int size) {
    numParallelQueries.set(size);
  }

  public void setExecutors(int allocation) {
    numExecutors.set(allocation);
  }

  public void setMaxExecutors(int allocation) {
    numExecutorsMax.set(allocation);
  }

  public void addQueuedQuery() {
    numQueuedQueries.incr();
  }

  public void addRunningQuery() {
    numRunningQueries.incr();
  }

  public void removeQueuedQueries(int num) {
    numQueuedQueries.incr(-num);
  }

  public void removeRunningQueries(int num) {
    numRunningQueries.incr(-num);
  }

  public void moveQueuedToRunning() {
    numQueuedQueries.incr(-1);
    numRunningQueries.incr();
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    // We could also have one metricssource for all the pools and add all the pools to the collector
    // in its getMetrics call (as separate records). Not clear if that's supported.
    // Also, we'd have to initialize the metrics ourselves instead of using @Metric annotation.
    MetricsRecordBuilder rb = collector.addRecord("WmPoolMetrics." + poolName)
        .setContext("HS2").tag(MsInfo.SessionId, poolName);
    if (allMetrics == null) {
      initAfterRegister(); // This happens if register calls getMetrics.
    }
    for (MutableMetric metric : allMetrics.values()) {
      metric.snapshot(rb, all);
    }
  }

  public static WmPoolMetrics create(String poolName, MetricsSystem ms) {
    WmPoolMetrics metrics = new WmPoolMetrics(poolName, ms);
    metrics = ms.register(metrics.sourceName, "WM " + poolName + " pool metrics", metrics);
    metrics.initAfterRegister();
    return metrics;
  }

  public void destroy() {
    ms.unregisterSource(sourceName);
    ms = null;
    if (codahaleGaugeNames != null) {
      Metrics metrics = MetricsFactory.getInstance();
      for (String chgName : codahaleGaugeNames) {
        metrics.removeGauge(chgName);
      }
      codahaleGaugeNames = null;
    }
  }

  private static class CodahaleGaugeWrapper implements MetricsVariable<Integer> {
    private final MutableGaugeInt mm;

    public CodahaleGaugeWrapper(MutableGaugeInt mm) {
      this.mm = mm;
    }

    @Override
    public Integer getValue() {
      return mm.value();
    }
  }

  private static class CodahaleCounterWrapper implements MetricsVariable<Integer> {
    private final MutableCounterInt mm;

    public CodahaleCounterWrapper(MutableCounterInt mm) {
      this.mm = mm;
    }

    @Override
    public Integer getValue() {
      return mm.value();
    }
  }

}
