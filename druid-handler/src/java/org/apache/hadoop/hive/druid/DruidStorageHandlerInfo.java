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
package org.apache.hadoop.hive.druid;

import io.druid.java.util.common.StringUtils;

import org.apache.hadoop.hive.druid.json.KafkaSupervisorReport;
import org.apache.hadoop.hive.ql.metadata.StorageHandlerInfo;

/**
 * DruidStorageHandlerInfo provides a runtime information for DruidStorageHandler.
 */
@SuppressWarnings("serial")
public class DruidStorageHandlerInfo implements StorageHandlerInfo {

  public static final StorageHandlerInfo UNREACHABLE = new StorageHandlerInfo() {
    @Override
    public String formatAsText() {
      return "Druid Overlord is Unreachable, Runtime Status : unknown";
    }
  };

  private final KafkaSupervisorReport kafkaSupervisorReport;

  DruidStorageHandlerInfo(KafkaSupervisorReport kafkaSupervisorReport) {
    this.kafkaSupervisorReport = kafkaSupervisorReport;
  }

  @Override
  public String formatAsText() {
    StringBuilder sb = new StringBuilder();
    sb.append("Druid Storage Handler Runtime Status for " + kafkaSupervisorReport.getId());
    sb.append("\n");
    sb.append("kafkaPartitions=" + kafkaSupervisorReport.getPayload().getPartitions());
    sb.append("\n");
    sb.append("activeTasks=" + kafkaSupervisorReport.getPayload().getActiveTasks());
    sb.append("\n");
    sb.append("publishingTasks=" + kafkaSupervisorReport.getPayload().getPublishingTasks());
    if (kafkaSupervisorReport.getPayload().getLatestOffsets() != null) {
      sb.append("\n");
      sb.append("latestOffsets=" + kafkaSupervisorReport.getPayload().getLatestOffsets());
    }
    if (kafkaSupervisorReport.getPayload().getMinimumLag() != null) {
      sb.append("\n");
      sb.append("minimumLag=" + kafkaSupervisorReport.getPayload().getMinimumLag());
    }
    if (kafkaSupervisorReport.getPayload().getAggregateLag() != null) {
      sb.append("\n");
      sb.append("aggregateLag=" + kafkaSupervisorReport.getPayload().getAggregateLag());
    }
    if (kafkaSupervisorReport.getPayload().getOffsetsLastUpdated() != null) {
      sb.append("\n");
      sb.append("lastUpdateTime=" + kafkaSupervisorReport.getPayload().getOffsetsLastUpdated());
    }
    return sb.toString();
  }
}
