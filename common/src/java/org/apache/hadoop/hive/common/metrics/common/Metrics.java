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
package org.apache.hadoop.hive.common.metrics.common;

/**
 * Generic Metics interface.
 */
public interface Metrics {

  /**
   * Deinitializes the Metrics system.
   */
  public void close() throws Exception;

  /**
   *
   * @param name starts a scope of a given name.  Scopes is stored as thread-local variable.
   */
  public void startStoredScope(String name);

  /**
   * Closes the stored scope of a given name.
   * Note that this must be called on the same thread as where the scope was started.
   * @param name
   */
  public void endStoredScope(String name);

  /**
   * Create scope with given name and returns it.
   * @param name
   * @return
   */
  public MetricsScope createScope(String name);

  /**
   * Close the given scope.
   * @param scope
   */
  public void endScope(MetricsScope scope);

  //Counter-related methods

  /**
   * Increments a counter of the given name by 1.
   * @param name
   * @return
   */
  public Long incrementCounter(String name);

  /**
   * Increments a counter of the given name by "increment"
   * @param name
   * @param increment
   * @return
   */
  public Long incrementCounter(String name, long increment);


  /**
   * Decrements a counter of the given name by 1.
   * @param name
   * @return
   */
  public Long decrementCounter(String name);

  /**
   * Decrements a counter of the given name by "decrement"
   * @param name
   * @param decrement
   * @return
   */
  public Long decrementCounter(String name, long decrement);


  /**
   * Adds a metrics-gauge to track variable.  For example, number of open database connections.
   * @param name name of gauge
   * @param variable variable to track.
   */
  public void addGauge(String name, final MetricsVariable variable);

  /**
   * Add a ratio metric to track the correlation between two variables
   * @param name name of the ratio gauge
   * @param numerator numerator of the ratio
   * @param denominator denominator of the ratio
   */
  public void addRatio(String name, MetricsVariable<Integer> numerator,
                           MetricsVariable<Integer> denominator);

  /**
   * Mark an event occurance for a meter. Meters measure the rate of an event and track
   * 1/5/15 minute moving averages
   * @param name name of the meter
   */
  public void markMeter(String name);
}
