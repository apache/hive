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

package org.apache.hadoop.hive.ql.parse;

import com.google.common.base.MoreObjects;

public class AlterTableBranchSpec<T> {

  public enum AlterBranchOperationType {
    CREATE_BRANCH
  }

  private final AlterBranchOperationType operationType;
  private final T operationParams;

  public AlterTableBranchSpec(AlterBranchOperationType type, T value) {
    this.operationType = type;
    this.operationParams = value;
  }

  public AlterBranchOperationType getOperationType() {
    return operationType;
  }

  public T getOperationParams() {
    return operationParams;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("operationType", operationType.name())
        .add("operationParams", operationParams).toString();
  }

  public static class CreateBranchSpec {

    private final String branchName;
    private final Long snapshotId;
    private final Long asOfTime;
    private final Long maxRefAgeMs;
    private final Integer minSnapshotsToKeep;
    private final Long maxSnapshotAgeMs;

    public String getBranchName() {
      return branchName;
    }

    public Long getSnapshotId() {
      return snapshotId;
    }

    public Long getAsOfTime() {
      return asOfTime;
    }

    public Long getMaxRefAgeMs() {
      return maxRefAgeMs;
    }

    public Integer getMinSnapshotsToKeep() {
      return minSnapshotsToKeep;
    }

    public Long getMaxSnapshotAgeMs() {
      return maxSnapshotAgeMs;
    }

    public CreateBranchSpec(String branchName, Long snapShotId, Long asOfTime, Long maxRefAgeMs,
        Integer minSnapshotsToKeep, Long maxSnapshotAgeMs) {
      this.branchName = branchName;
      this.snapshotId = snapShotId;
      this.asOfTime = asOfTime;
      this.maxRefAgeMs = maxRefAgeMs;
      this.minSnapshotsToKeep = minSnapshotsToKeep;
      this.maxSnapshotAgeMs = maxSnapshotAgeMs;
    }

    public String toString() {
      return MoreObjects.toStringHelper(this).add("branchName", branchName).add("snapshotId", snapshotId)
          .add("asOfTime", asOfTime).add("maxRefAgeMs", maxRefAgeMs).add("minSnapshotsToKeep", minSnapshotsToKeep)
          .add("maxSnapshotAgeMs", maxSnapshotAgeMs).toString();
    }
  }
}
