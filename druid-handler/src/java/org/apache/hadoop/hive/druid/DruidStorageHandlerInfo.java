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

package org.apache.hadoop.hive.druid;

import org.apache.hadoop.hive.druid.json.KafkaSupervisorReport;
import org.apache.hadoop.hive.ql.metadata.StorageHandlerInfo;

/**
 * DruidStorageHandlerInfo provides a runtime information for DruidStorageHandler.
 */
public class DruidStorageHandlerInfo implements StorageHandlerInfo {

  static final StorageHandlerInfo
      UNREACHABLE =
      (StorageHandlerInfo) () -> "Druid Overlord is Unreachable, Runtime Status : unknown";

  private final KafkaSupervisorReport kafkaSupervisorReport;

  DruidStorageHandlerInfo(KafkaSupervisorReport kafkaSupervisorReport) {
    this.kafkaSupervisorReport = kafkaSupervisorReport;
  }

  @Override public String formatAsText() {
    StringBuilder sb = new StringBuilder();
    sb.append("Druid Storage Handler Runtime Status for ")
        .append(kafkaSupervisorReport.getId())
        .append("\n")
        .append("kafkaPartitions=")
        .append(kafkaSupervisorReport.getPayload().getPartitions())
        .append("\n")
        .append("activeTasks=")
        .append(kafkaSupervisorReport.getPayload().getActiveTasks())
        .append("\n")
        .append("publishingTasks=")
        .append(kafkaSupervisorReport.getPayload().getPublishingTasks());

    if (kafkaSupervisorReport.getPayload().getLatestOffsets() != null) {
      sb.append("\n").append("latestOffsets=").append(kafkaSupervisorReport.getPayload().getLatestOffsets());
    }
    if (kafkaSupervisorReport.getPayload().getMinimumLag() != null) {
      sb.append("\n").append("minimumLag=").append(kafkaSupervisorReport.getPayload().getMinimumLag());
    }
    if (kafkaSupervisorReport.getPayload().getAggregateLag() != null) {
      sb.append("\n").append("aggregateLag=").append(kafkaSupervisorReport.getPayload().getAggregateLag());
    }
    if (kafkaSupervisorReport.getPayload().getOffsetsLastUpdated() != null) {
      sb.append("\n").append("lastUpdateTime=").append(kafkaSupervisorReport.getPayload().getOffsetsLastUpdated());
    }
    return sb.toString();
  }
}
