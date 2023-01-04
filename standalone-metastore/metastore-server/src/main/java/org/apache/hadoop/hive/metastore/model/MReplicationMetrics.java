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

package org.apache.hadoop.hive.metastore.model;

import org.apache.hadoop.hive.metastore.api.ReplicationMetrics;

/**
 * Describes Replication Metrics.
 */
public class MReplicationMetrics {
  private long scheduledExecutionId;
  private String policy;
  private long dumpExecutionId;
  private String metadata;
  private String progress;
  private int startTime;
  private String messageFormat;

  public MReplicationMetrics() {
  }

  public static ReplicationMetrics toThrift(MReplicationMetrics mReplicationMetric) {
    ReplicationMetrics ret = new ReplicationMetrics();
    ret.setScheduledExecutionId(mReplicationMetric.scheduledExecutionId);
    ret.setPolicy(mReplicationMetric.policy);
    ret.setMetadata(mReplicationMetric.metadata);
    ret.setProgress(mReplicationMetric.progress);
    ret.setDumpExecutionId(mReplicationMetric.dumpExecutionId);
    ret.setMessageFormat(mReplicationMetric.messageFormat);
    return ret;
  }

  public long getScheduledExecutionId() {
    return scheduledExecutionId;
  }

  public void setScheduledExecutionId(long scheduledExecutionId) {
    this.scheduledExecutionId = scheduledExecutionId;
  }

  public String getPolicy() {
    return policy;
  }

  public void setPolicy(String policy) {
    this.policy = policy;
  }

  public long getDumpExecutionId() {
    return dumpExecutionId;
  }

  public void setDumpExecutionId(long dumpExecutionId) {
    this.dumpExecutionId = dumpExecutionId;
  }

  public String getMetadata() {
    return metadata;
  }

  public void setMetadata(String metadata) {
    this.metadata = metadata;
  }

  public String getProgress() {
    return progress;
  }

  public void setProgress(String progress) {
    this.progress = progress;
  }

  public int getStartTime() {
    return startTime;
  }

  public void setStartTime(int startTime) {
    this.startTime = startTime;
  }

  public String getMessageFormat() {
    return messageFormat;
  }

  public void setMessageFormat(String messageFormat) {
    this.messageFormat = messageFormat;
  }
}
