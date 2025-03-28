/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import java.util.concurrent.TimeUnit;

/**
 * Any task that will run as a separate thread in the metastore should implement this
 * interface.
 */
public interface MetastoreTaskThread extends Configurable, Runnable {

  /**
   * Get the frequency at which the thread should be scheduled in the thread pool.  You must call
   * {@link #setConf(Configuration)} before calling this method.
   * @param unit TimeUnit to express the frequency in.
   * @return frequency
   */
  long runFrequency(TimeUnit unit);

  /**
   * Gets the initial delay before the first execution.
   *
   * Defaults to {@link #runFrequency(TimeUnit)}
   */
  default long initialDelay(TimeUnit unit) {
    return runFrequency(unit);
  }

  /**
   * Should use mutex support to allow only one copy of this task running across the warehouse.
   * @param useMutex true for enabling the mutex, false otherwise
   */
  default void enforceMutex(boolean useMutex) {
    // no-op
  }
}
