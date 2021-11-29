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

import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This thread simply updates a gauge metric by the given interval with the time elapsed since start
 */
class CycleUpdaterThread extends Thread {

  private static final String CLASS_NAME = CycleUpdaterThread.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  private final String gaugeName;
  private final long startTime;
  private final long updateInterval;

  /**
   * Constructor
   * @param gaugeName name of the gauge which obtained from {@link org.apache.hadoop.hive.metastore.metrics.Metrics}
   * @param startTime the start of the measurement
   * @param updateInterval the update interval
   */
  CycleUpdaterThread(String gaugeName, long startTime, long updateInterval) {
    this.gaugeName = gaugeName;
    this.startTime = startTime;
    this.updateInterval = updateInterval;
  }

  @Override
  public void run() {
    while (!isInterrupted()) {
      updateMetric();

      try {
        Thread.sleep(updateInterval);
      } catch (InterruptedException e) {
        LOG.debug("Thread has been interrupted - this is normal");
      }
    }
  }

  @Override
  public void interrupt() {
    LOG.debug("Interrupting for {}", gaugeName);
    super.interrupt();
    updateMetric();
  }

  private void updateMetric() {
    updateMetric((int)(System.currentTimeMillis() - startTime));
  }

  private void updateMetric(int elapsed) {
    LOG.debug("Updating gauge metric {} to {}", gaugeName, elapsed);
    Metrics.getOrCreateGauge(gaugeName)
        .set(elapsed);
  }
}
