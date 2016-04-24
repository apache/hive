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
package org.apache.hadoop.hive.llap.metrics;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;

/**
 * Metrics system for llap daemon. We do not use DefaultMetricsSystem here to safegaurd against
 * Tez accidentally shutting it down.
 */
public enum LlapMetricsSystem  {
  INSTANCE;

  private AtomicReference<MetricsSystem> impl =
      new AtomicReference<MetricsSystem>(new MetricsSystemImpl());

  /**
   * Convenience method to initialize the metrics system
   * @param prefix  for the metrics system configuration
   * @return the metrics system instance
   */
  public static MetricsSystem initialize(String prefix) {
    return INSTANCE.impl.get().init(prefix);
  }

  /**
   * @return the metrics system object
   */
  public static MetricsSystem instance() {
    return INSTANCE.impl.get();
  }

  /**
   * Shutdown the metrics system
   */
  public static void shutdown() {
    INSTANCE.impl.get().shutdown();
  }
}
