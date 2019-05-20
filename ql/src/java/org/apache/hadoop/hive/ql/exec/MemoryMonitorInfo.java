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

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;

/**
 * Contains information required for memory usage monitoring.
 **/

public class MemoryMonitorInfo implements Serializable {

  private static final long serialVersionUID = 1L;

  // Variables for LLAP hash table loading memory monitor
  private boolean isLlap;
  private int executorsPerNode;
  private int maxExecutorsOverSubscribeMemory;
  private double memoryOverSubscriptionFactor;
  private long noConditionalTaskSize;
  private long adjustedNoConditionalTaskSize;
  private long memoryCheckInterval;
  private double hashTableInflationFactor;
  private long threshold;

  public MemoryMonitorInfo() {
  }

  public MemoryMonitorInfo(boolean isLlap, int executorsPerNode, int maxExecutorsOverSubscribeMemory,
    double memoryOverSubscriptionFactor, long noConditionalTaskSize, long adjustedNoConditionalTaskSize,
    long memoryCheckInterval, double hashTableInflationFactor) {
    this.isLlap = isLlap;
    this.executorsPerNode = executorsPerNode;
    this.maxExecutorsOverSubscribeMemory = maxExecutorsOverSubscribeMemory;
    this.memoryOverSubscriptionFactor = memoryOverSubscriptionFactor;
    this.noConditionalTaskSize = noConditionalTaskSize;
    this.adjustedNoConditionalTaskSize = adjustedNoConditionalTaskSize;
    this.memoryCheckInterval = memoryCheckInterval;
    this.hashTableInflationFactor = hashTableInflationFactor;
    this.threshold = (long) (hashTableInflationFactor * adjustedNoConditionalTaskSize);
  }

  public MemoryMonitorInfo(MemoryMonitorInfo memoryMonitorInfo) {
    this.isLlap = memoryMonitorInfo.isLlap;
    this.executorsPerNode = memoryMonitorInfo.executorsPerNode;
    this.maxExecutorsOverSubscribeMemory = memoryMonitorInfo.maxExecutorsOverSubscribeMemory;
    this.memoryOverSubscriptionFactor = memoryMonitorInfo.memoryOverSubscriptionFactor;
    this.noConditionalTaskSize = memoryMonitorInfo.noConditionalTaskSize;
    this.adjustedNoConditionalTaskSize = memoryMonitorInfo.adjustedNoConditionalTaskSize;
    this.memoryCheckInterval = memoryMonitorInfo.memoryCheckInterval;
    this.hashTableInflationFactor = memoryMonitorInfo.hashTableInflationFactor;
    this.threshold = memoryMonitorInfo.threshold;
  }

  public int getExecutorsPerNode() {
    return executorsPerNode;
  }

  public void setExecutorsPerNode(final int executorsPerNode) {
    this.executorsPerNode = executorsPerNode;
  }

  public int getMaxExecutorsOverSubscribeMemory() {
    return maxExecutorsOverSubscribeMemory;
  }

  public void setMaxExecutorsOverSubscribeMemory(final int maxExecutorsOverSubscribeMemory) {
    this.maxExecutorsOverSubscribeMemory = maxExecutorsOverSubscribeMemory;
  }

  public double getMemoryOverSubscriptionFactor() {
    return memoryOverSubscriptionFactor;
  }

  public void setMemoryOverSubscriptionFactor(final double memoryOverSubscriptionFactor) {
    this.memoryOverSubscriptionFactor = memoryOverSubscriptionFactor;
  }

  public long getNoConditionalTaskSize() {
    return noConditionalTaskSize;
  }

  public void setNoConditionalTaskSize(final long noConditionalTaskSize) {
    this.noConditionalTaskSize = noConditionalTaskSize;
  }

  public long getAdjustedNoConditionalTaskSize() {
    return adjustedNoConditionalTaskSize;
  }

  public void setAdjustedNoConditionalTaskSize(final long adjustedNoConditionalTaskSize) {
    this.adjustedNoConditionalTaskSize = adjustedNoConditionalTaskSize;
  }

  public long getMemoryCheckInterval() {
    return memoryCheckInterval;
  }

  public void setMemoryCheckInterval(final long memoryCheckInterval) {
    this.memoryCheckInterval = memoryCheckInterval;
  }

  public double getHashTableInflationFactor() {
    return hashTableInflationFactor;
  }

  public void setHashTableInflationFactor(final double hashTableInflationFactor) {
    this.hashTableInflationFactor = hashTableInflationFactor;
  }

  public long getThreshold() {
    return threshold;
  }

  public void setLlap(final boolean llap) {
    isLlap = llap;
  }

  public boolean isLlap() {
    return isLlap;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append(" isLlap: ").append(isLlap);
    sb.append(" executorsPerNode: ").append(executorsPerNode);
    sb.append(" maxExecutorsOverSubscribeMemory: ").append(maxExecutorsOverSubscribeMemory);
    sb.append(" memoryOverSubscriptionFactor: ").append(memoryOverSubscriptionFactor);
    sb.append(" memoryCheckInterval: ").append(memoryCheckInterval);
    sb.append(" noConditionalTaskSize: ").append(noConditionalTaskSize);
    sb.append(" adjustedNoConditionalTaskSize: ").append(adjustedNoConditionalTaskSize);
    sb.append(" hashTableInflationFactor: ").append(hashTableInflationFactor);
    sb.append(" threshold: ").append(threshold);
    sb.append(" }");
    return sb.toString();
  }

  public boolean doMemoryMonitoring() {
    return isLlap && hashTableInflationFactor > 0.0d && noConditionalTaskSize > 0 &&
      memoryCheckInterval > 0;
  }

  public long getEffectiveThreshold(final long maxMemoryPerExecutor) {
    // guard against poor configuration of noconditional task size. We let hash table grow till 2/3'rd memory
    // available for container/executor
    return (long) Math.max(threshold, (2.0 / 3.0) * maxMemoryPerExecutor);
  }
}
