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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class for defining the different stages of replication.
 */
public class Stage {
  private String name;
  private Status status;
  private long startTime;
  private long endTime;
  private Map<String, Metric> metrics = new HashMap<>();
  private String errorLogPath;
  private SnapshotUtils.ReplSnapshotCount replSnapshotCount = new SnapshotUtils.ReplSnapshotCount();
  private String replStats;

  public Stage() {

  }

  public Stage(String name, Status status, long startTime) {
    this.name = name;
    this.status = status;
    this.startTime = startTime;
  }

  public Stage(Stage stage) {
    this.name = stage.name;
    this.status = stage.status;
    this.startTime = stage.startTime;
    this.endTime = stage.endTime;
    for (Metric metric : stage.metrics.values()) {
      this.metrics.put(metric.getName(), new Metric(metric));
    }
    this.errorLogPath = stage.errorLogPath;
    this.replSnapshotCount = stage.replSnapshotCount;
    this.replStats = stage.replStats;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }


  public void addMetric(Metric metric) {
    this.metrics.put(metric.getName(), metric);
  }

  public Metric getMetricByName(String name) {
    return this.metrics.get(name);
  }

  public List<Metric> getMetrics() {
    return new ArrayList<>(metrics.values());
  }

  public String getErrorLogPath() {
    return errorLogPath;
  }

  public void setErrorLogPath(String errorLogPath) {
    this.errorLogPath = errorLogPath;
  }

  public void setReplSnapshotsCount(SnapshotUtils.ReplSnapshotCount replSnapshotCount) {
    this.replSnapshotCount = replSnapshotCount;
  }

  public SnapshotUtils.ReplSnapshotCount getReplSnapshotCount() {
    return replSnapshotCount;
  }


  public String getReplStats() {
    return replStats;
  }

  public void setReplStats(String replStats) {
    this.replStats = replStats;
  }

}
