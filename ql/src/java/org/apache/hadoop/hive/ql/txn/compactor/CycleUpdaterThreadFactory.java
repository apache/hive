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
package org.apache.hadoop.hive.ql.txn.compactor;

/**
 * Factory for the {@link CycleUpdaterThread} class
 */
final class CycleUpdaterThreadFactory {

  private static final long DEFAULT_UPDATE_INTERVAL_IN_MILLISECONDS = 1_000L;

  /**
   * Private constructor
   */
  private CycleUpdaterThreadFactory() {
    throw new IllegalStateException("No instance for you");
  }

  /**
   * Returns a Cycle Updater daemon thread named after the gauge
   * @param gaugeName name of the gauge that required to be not null
   * @param startedAt a system time when the measurement started
   * @return the constructed thread
   */
  static CycleUpdaterThread getCycleUpdaterThreadForGauge(String gaugeName, long startedAt) {
    CycleUpdaterThread thread = new CycleUpdaterThread(gaugeName, startedAt, DEFAULT_UPDATE_INTERVAL_IN_MILLISECONDS);
    thread.setName(CycleUpdaterThread.class.getSimpleName() + " - " + gaugeName);
    thread.setDaemon(true);
    return thread;
  }
}
