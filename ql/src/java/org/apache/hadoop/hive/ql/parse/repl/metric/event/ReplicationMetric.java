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
package org.apache.hadoop.hive.ql.parse.repl.metric.event;

/**
 * Class for defining the replication metrics.
 */
public class ReplicationMetric {
  private long scheduledExecutionId;
  private String policy;
  private long dumpExecutionId;
  private Metadata metadata;
  private Progress progress;
  private String messageFormat;

  public ReplicationMetric(long scheduledExecutionId, String policy, long dumpExecutionId, Metadata metadata){
    this.scheduledExecutionId = scheduledExecutionId;
    this.policy = policy;
    this.dumpExecutionId = dumpExecutionId;
    this.metadata = metadata;
    this.progress = new Progress();
  }

  public ReplicationMetric(ReplicationMetric metric) {
    this.scheduledExecutionId = metric.scheduledExecutionId;
    this.policy = metric.policy;
    this.dumpExecutionId = metric.dumpExecutionId;
    this.metadata = new Metadata(metric.metadata);
    this.progress = new Progress(metric.progress);
  }

  public long getScheduledExecutionId() {
    return scheduledExecutionId;
  }


  public String getPolicy() {
    return policy;
  }

  public long getDumpExecutionId() {
    return dumpExecutionId;
  }

  public Progress getProgress() {
    return progress;
  }

  public void setMetadata(Metadata metadata) {
    this.metadata = metadata;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  public void setProgress(Progress progress) {
    this.progress = progress;
  }

  public String getMessageFormat() {
    return messageFormat;
  }

  public void setMessageFormat(String messageFormat) {
    this.messageFormat = messageFormat;
  }
}
