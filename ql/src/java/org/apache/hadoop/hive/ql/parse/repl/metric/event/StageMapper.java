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

import org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils;
import org.apache.hive.common.util.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.List;

/**
 * Class for deserializing the different stages of replication.
 */
public class StageMapper {

  @SuppressFBWarnings("UWF_UNWRITTEN_FIELD")
  private String name;

  private Status status = Status.IN_PROGRESS;

  private long startTime = 0;

  private long endTime = 0;

  private List<Metric> metrics = new ArrayList<>();

  private String errorLogPath;

  private SnapshotUtils.ReplSnapshotCount replSnapshotCount = new SnapshotUtils.ReplSnapshotCount();

  private String replStats;

  public StageMapper() {

  }
  public String getName() {
    return name;
  }

  public Status getStatus() {
    return status;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public List<Metric> getMetrics() {
    return metrics;
  }

  public String getErrorLogPath() {
    return errorLogPath;
  }

  public SnapshotUtils.ReplSnapshotCount getReplSnapshotCount() {
    return replSnapshotCount;
  }

  public String getReplStats() {
    return replStats;
  }

}
