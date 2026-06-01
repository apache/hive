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

package org.apache.hadoop.hive.ql.exec.tez.monitoring.yarnqueue;

import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Immutable snapshot of YARN queue resource metrics at a point in time.
 * Created by {@link YarnQueueMetricsCollector} after each successful YARN RM fetch,
 * stored in {@link QueueMetricsState}, and read by TezProgressMonitor for display.
 */
public final class QueueMetricsSnapshot {
  private static final Logger LOG = LoggerFactory.getLogger(QueueMetricsSnapshot.class);

  private final float memoryUsedGB;
  private final float memoryTotalGB;
  private final int vCoresUsed;
  private final int vCoresTotal;
  private final float capacityPercentage;
  private final float currentCapacityPercentage;
  private final int runningApps;
  private final int pendingApps;
  private final int allocatedContainers;
  private final int pendingContainers;
  private final long collectionTimestamp;

  public QueueMetricsSnapshot(QueueInfo queueInfo) {
    if (queueInfo == null) {
      throw new IllegalArgumentException("QueueInfo cannot be null");
    }
    this.collectionTimestamp = System.currentTimeMillis();
    QueueStatistics stats = queueInfo.getQueueStatistics();
    if (stats != null) {
      this.memoryUsedGB = stats.getAllocatedMemoryMB() / 1024.0f;
      this.memoryTotalGB = (stats.getAllocatedMemoryMB() + stats.getAvailableMemoryMB()) / 1024.0f;
      this.vCoresUsed = (int) stats.getAllocatedVCores();
      this.vCoresTotal = (int) (stats.getAllocatedVCores() + stats.getAvailableVCores());
      this.runningApps = (int) stats.getNumAppsRunning();
      this.pendingApps = (int) stats.getNumAppsPending();
      this.allocatedContainers = (int) stats.getAllocatedContainers();
      this.pendingContainers = (int) stats.getPendingContainers();
    } else {
      LOG.debug("QueueStatistics is null for queue, using zero values");
      this.memoryUsedGB = 0;
      this.memoryTotalGB = 0;
      this.vCoresUsed = 0;
      this.vCoresTotal = 0;
      this.runningApps = 0;
      this.pendingApps = 0;
      this.allocatedContainers = 0;
      this.pendingContainers = 0;
    }
    this.capacityPercentage = queueInfo.getCapacity() * 100;
    this.currentCapacityPercentage = queueInfo.getCurrentCapacity() * 100;
  }

  public float getMemoryUsedGB() {
    return memoryUsedGB;
  }

  public float getMemoryTotalGB() {
    return memoryTotalGB;
  }

  public int getVCoresUsed() {
    return vCoresUsed;
  }

  public int getVCoresTotal() {
    return vCoresTotal;
  }

  public float getCapacityPercentage() {
    return capacityPercentage;
  }

  public float getCurrentCapacityPercentage() {
    return currentCapacityPercentage;
  }

  public int getRunningApps() {
    return runningApps;
  }

  public int getPendingApps() {
    return pendingApps;
  }

  public int getAllocatedContainers() {
    return allocatedContainers;
  }

  public int getPendingContainers() {
    return pendingContainers;
  }

  public long getCollectionTimestamp() {
    return collectionTimestamp;
  }

  public String getMemoryPercentage() {
    if (memoryTotalGB > 0) {
      return String.format("%.2f%%", (memoryUsedGB / memoryTotalGB) * 100);
    }
    return "N/A";
  }

  public String getVCoresPercentage() {
    if (vCoresTotal > 0) {
      return String.format("%.2f%%", ((float) vCoresUsed / vCoresTotal) * 100);
    }
    return "N/A";
  }
}

