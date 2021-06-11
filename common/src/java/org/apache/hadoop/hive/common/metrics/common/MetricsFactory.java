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
package org.apache.hadoop.hive.common.metrics.common;

import org.apache.hadoop.hive.conf.HiveConf;

import java.lang.reflect.Constructor;
import java.util.Objects;

/**
 * Class that manages a static Metric instance for this process.
 */
public class MetricsFactory {

  private static Metrics metrics = new NoOpMetrics();

  /**
   * Initializes static Metrics instance.
   *
   * @throws ReflectiveOperationException if metrics class fails to load
   * @throws IllegalStateException if
   *           {@code MetricsFactory is already initialized}
   * @throws NullPointerException if {@code conf} is {@code null}
   */
  public static void init(HiveConf conf) throws ReflectiveOperationException {
    Objects.requireNonNull(conf);
    if (metrics != null) {
      throw new IllegalStateException("Metrics factory is already initialized");
    }
    Class<?> metricsClass = conf.getClassByName(conf.getVar(HiveConf.ConfVars.HIVE_METRICS_CLASS));
    Constructor<?> constructor = metricsClass.getConstructor(HiveConf.class);
    metrics = Metrics.class.cast(constructor.newInstance(conf));
  }

  /**
   * Returns static Metrics instance, null if not initialized or closed.
   */
  public static Metrics getInstance() {
    return metrics;
  }

  /**
   * Closes and removes static Metrics instance.
   *
   * @throws Exception if the Metrics provider fails to close
   */
  public static void close() throws Exception {
    metrics.close();
  }

  /**
   * Dummy Metrics class which performs no metrics operations.
   */
  private static final class NoOpMetrics implements Metrics {

    @Override
    public void close() throws Exception {
    }

    @Override
    public void startStoredScope(String name) {
    }

    @Override
    public void endStoredScope(String name) {
    }

    @Override
    public MetricsScope createScope(String name) {
      return new MetricsScope() {
      };
    }

    @Override
    public void endScope(MetricsScope scope) {
    }

    @Override
    public Long incrementCounter(String name) {
      return null;
    }

    @Override
    public Long incrementCounter(String name, long increment) {
      return null;
    }

    @Override
    public Long decrementCounter(String name) {
      return null;
    }

    @Override
    public Long decrementCounter(String name, long decrement) {
      return null;
    }

    @Override
    public void addGauge(String name, MetricsVariable<?> variable) {
    }

    @Override
    public void removeGauge(String name) {
    }

    @Override
    public void addRatio(String name, MetricsVariable<Integer> numerator, MetricsVariable<Integer> denominator) {
    }

    @Override
    public void markMeter(String name) {
    }
    
  }
}
