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
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;

public class AlterTableSnapshotRefSpec<T> {

  private final AlterTableType operationType;
  private final T operationParams;

  public AlterTableSnapshotRefSpec(AlterTableType type, T value) {
    this.operationType = type;
    this.operationParams = value;
  }

  public AlterTableType getOperationType() {
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

  public static class CreateSnapshotRefSpec {

    private String refName;
    private Long snapshotId;
    private Long asOfTime;
    private Long maxRefAgeMs;
    private Integer minSnapshotsToKeep;
    private Long maxSnapshotAgeMs;
    private String asOfTag;

    public String getRefName() {
      return refName;
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

    public String getAsOfTag() {
      return asOfTag;
    }

    public CreateSnapshotRefSpec(String refName, Long snapShotId, Long asOfTime, Long maxRefAgeMs,
                             Integer minSnapshotsToKeep, Long maxSnapshotAgeMs, String asOfTag) {
      this.refName = refName;
      this.snapshotId = snapShotId;
      this.asOfTime = asOfTime;
      this.maxRefAgeMs = maxRefAgeMs;
      this.minSnapshotsToKeep = minSnapshotsToKeep;
      this.maxSnapshotAgeMs = maxSnapshotAgeMs;
      this.asOfTag = asOfTag;
    }

    public String toString() {
      return MoreObjects.toStringHelper(this).add("refName", refName).add("snapshotId", snapshotId)
          .add("asOfTime", asOfTime).add("maxRefAgeMs", maxRefAgeMs).add("minSnapshotsToKeep", minSnapshotsToKeep)
          .add("maxSnapshotAgeMs", maxSnapshotAgeMs).omitNullValues().toString();
    }
  }
  public static class DropSnapshotRefSpec {

    private final String refName;
    private final boolean ifExists;

    public String getRefName() {
      return refName;
    }

    public boolean getIfExists() {
      return ifExists;
    }

    public DropSnapshotRefSpec(String refName, Boolean ifExists) {
      this.refName = refName;
      this.ifExists = ifExists;
    }

    public String toString() {
      return MoreObjects.toStringHelper(this).add("refName", refName).add("ifExists", ifExists).toString();
    }
  }
}
