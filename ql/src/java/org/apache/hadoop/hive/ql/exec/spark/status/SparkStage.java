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

import java.util.Objects;

/**
 * Class to hold information that can be used to identify a Spark stage.
 */
public class SparkStage implements Comparable<SparkStage> {

  private int stageId;
  private int attemptId;

  public SparkStage(int stageId, int attemptId) {
    this.stageId = stageId;
    this.attemptId = attemptId;
  }

  public int getStageId() {
    return stageId;
  }

  public int getAttemptId() {
    return attemptId;
  }

  @Override
  public int compareTo(SparkStage stage) {
    if (this.stageId == stage.stageId) {
      return Integer.compare(this.attemptId, stage.attemptId);
    }
    return Integer.compare(this.stageId, stage.stageId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SparkStage that = (SparkStage) o;
    return getStageId() == that.getStageId() && getAttemptId() == that.getAttemptId();
  }

  @Override
  public int hashCode() {
    return Objects.hash(stageId, attemptId);
  }

  @Override
  public String toString() {
    return String.valueOf(this.stageId) + "_" + String.valueOf(this.attemptId);
  }
}
