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

import java.io.IOException;

import org.apache.hadoop.hive.conf.HiveConf;

import java.io.IOException;

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
   * @throws IOException
   */
  public void startStoredScope(String name) throws IOException;

  /**
   * Closes the stored scope of a given name.
   * Note that this must be called on the same thread as where the scope was started.
   * @param name
   * @throws IOException
   */
  public void endStoredScope(String name) throws IOException;

  /**
   * Create scope with given name and returns it.
   * @param name
   * @return
   * @throws IOException
   */
  public MetricsScope createScope(String name) throws IOException;

  /**
   * Close the given scope.
   * @param scope
   * @throws IOException
   */
  public void endScope(MetricsScope scope) throws IOException;

  //Counter-related methods

  /**
   * Increments a counter of the given name by 1.
   * @param name
   * @return
   * @throws IOException
   */
  public Long incrementCounter(String name) throws IOException;

  /**
   * Increments a counter of the given name by "increment"
   * @param name
   * @param increment
   * @return
   * @throws IOException
   */
  public Long incrementCounter(String name, long increment) throws IOException;


  /**
   * Decrements a counter of the given name by 1.
   * @param name
   * @return
   * @throws IOException
   */
  public Long decrementCounter(String name) throws IOException;

  /**
   * Decrements a counter of the given name by "decrement"
   * @param name
   * @param decrement
   * @return
   * @throws IOException
   */
  public Long decrementCounter(String name, long decrement) throws IOException;


  /**
   * Adds a metrics-gauge to track variable.  For example, number of open database connections.
   * @param name name of gauge
   * @param variable variable to track.
   * @throws IOException
   */
  public void addGauge(String name, final MetricsVariable variable);
}
