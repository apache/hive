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
package org.apache.hadoop.hive.ql.exec.spark.status;

import org.apache.hadoop.hive.common.log.ProgressMonitor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * This class defines various parts of the progress update bar.
 * Progressbar is displayed in hive-cli and typically rendered using InPlaceUpdate.
 */
class SparkProgressMonitor implements ProgressMonitor {

  private Map<SparkStage, SparkStageProgress> progressMap;
  private long startTime;
  private static final int COLUMN_1_WIDTH = 16;

  SparkProgressMonitor(Map<SparkStage, SparkStageProgress> progressMap, long startTime) {
    this.progressMap = progressMap;
    this.startTime = startTime;
  }

  @Override
  public List<String> headers() {
    return Arrays.asList("STAGES", "ATTEMPT", "STATUS", "TOTAL", "COMPLETED", "RUNNING", "PENDING", "FAILED", "");
  }

  @Override
  public List<List<String>> rows() {
    List<List<String>> progressRows = new ArrayList<>();
    SortedSet<SparkStage> keys = new TreeSet<SparkStage>(progressMap.keySet());
    for (SparkStage stage : keys) {
      SparkStageProgress progress = progressMap.get(stage);
      final int complete = progress.getSucceededTaskCount();
      final int total = progress.getTotalTaskCount();
      final int running = progress.getRunningTaskCount();
      final int failed = progress.getFailedTaskCount();

      SparkJobMonitor.StageState state =
          total > 0 ? SparkJobMonitor.StageState.PENDING : SparkJobMonitor.StageState.FINISHED;
      if (complete > 0 || running > 0 || failed > 0) {
        if (complete < total) {
          state = SparkJobMonitor.StageState.RUNNING;
        } else {
          state = SparkJobMonitor.StageState.FINISHED;
        }
      }
      String attempt = String.valueOf(stage.getAttemptId());
      String stageName = "Stage-" +String.valueOf(stage.getStageId());
      String nameWithProgress = getNameWithProgress(stageName, complete, total);
      final int pending = total - complete - running;

      progressRows.add(Arrays
          .asList(nameWithProgress, attempt, state.toString(), String.valueOf(total), String.valueOf(complete),
              String.valueOf(running), String.valueOf(pending), String.valueOf(failed), ""));
    }
    return progressRows;
  }

  @Override
  public String footerSummary() {
    return String.format("STAGES: %02d/%02d", getCompletedStages(), progressMap.keySet().size());
  }

  @Override
  public long startTime() {
    return startTime;
  }

  @Override
  public String executionStatus() {
    if (getCompletedStages() == progressMap.keySet().size()) {
      return SparkJobMonitor.StageState.FINISHED.toString();
    } else {
      return SparkJobMonitor.StageState.RUNNING.toString();
    }
  }

  @Override
  public double progressedPercentage() {

    SortedSet<SparkStage> keys = new TreeSet<SparkStage>(progressMap.keySet());
    int sumTotal = 0;
    int sumComplete = 0;
    for (SparkStage stage : keys) {
      SparkStageProgress progress = progressMap.get(stage);
      final int complete = progress.getSucceededTaskCount();
      final int total = progress.getTotalTaskCount();
      sumTotal += total;
      sumComplete += complete;
    }
    double progress = (sumTotal == 0) ? 1.0f : (float) sumComplete / (float) sumTotal;
    return progress;
  }

  private int getCompletedStages() {
    int completed = 0;
    SortedSet<SparkStage> keys = new TreeSet<SparkStage>(progressMap.keySet());
    for (SparkStage stage : keys) {
      SparkStageProgress progress = progressMap.get(stage);
      final int complete = progress.getSucceededTaskCount();
      final int total = progress.getTotalTaskCount();
      if (total > 0 && complete == total) {
        completed++;
      }
    }
    return completed;
  }

  private String getNameWithProgress(String s, int complete, int total) {

    if (s == null) {
      return "";
    }
    float percent = total == 0 ? 1.0f : (float) complete / (float) total;
    // lets use the remaining space in column 1 as progress bar
    int spaceRemaining = COLUMN_1_WIDTH - s.length() - 1;
    String trimmedVName = s;

    // if the vertex name is longer than column 1 width, trim it down
    if (s.length() > COLUMN_1_WIDTH) {
      trimmedVName = s.substring(0, COLUMN_1_WIDTH - 2);
      trimmedVName += "..";
    } else {
      trimmedVName += " ";
    }
    StringBuilder result = new StringBuilder(trimmedVName);
    int toFill = (int) (spaceRemaining * percent);
    for (int i = 0; i < toFill; i++) {
      result.append(".");
    }
    return result.toString();
  }
}
