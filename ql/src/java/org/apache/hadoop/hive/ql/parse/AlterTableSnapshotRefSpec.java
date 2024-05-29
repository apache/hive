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
import com.google.common.base.Preconditions;

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
    private boolean isReplace;
    private boolean ifNotExists;

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

    public boolean isReplace() {
      return isReplace;
    }

    public boolean isIfNotExists() {
      return ifNotExists;
    }

    public CreateSnapshotRefSpec(String refName, Long snapShotId, Long asOfTime, Long maxRefAgeMs,
        Integer minSnapshotsToKeep, Long maxSnapshotAgeMs, String asOfTag, boolean isReplace, boolean ifNotExists) {
      this.refName = refName;
      this.snapshotId = snapShotId;
      this.asOfTime = asOfTime;
      this.maxRefAgeMs = maxRefAgeMs;
      this.minSnapshotsToKeep = minSnapshotsToKeep;
      this.maxSnapshotAgeMs = maxSnapshotAgeMs;
      this.asOfTag = asOfTag;
      this.isReplace = isReplace;
      this.ifNotExists = ifNotExists;
    }

    public String toString() {
      return MoreObjects.toStringHelper(this).add("refName", refName).add("snapshotId", snapshotId)
          .add("asOfTime", asOfTime).add("maxRefAgeMs", maxRefAgeMs).add("minSnapshotsToKeep", minSnapshotsToKeep)
          .add("maxSnapshotAgeMs", maxSnapshotAgeMs).add("isReplace", isReplace).add("ifNotExists", ifNotExists)
          .omitNullValues().toString();
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

  public static class RenameSnapshotrefSpec {

    private final String sourceBranch;
    private final String targetBranch;

    public String getSourceBranchName() {
      return sourceBranch;
    }

    public String getTargetBranchName() {
      return targetBranch;
    }

    public RenameSnapshotrefSpec(String sourceBranch, String targetBranch) {
      this.sourceBranch = sourceBranch;
      this.targetBranch = targetBranch;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("sourceBranch", sourceBranch).add("targetBranch", targetBranch)
          .toString();
    }
  }

  public static class ReplaceSnapshotrefSpec {

    private final String sourceRef;
    private String targetBranch = null;
    private long targetSnapshot;

    boolean replaceBySnapshot = false;
    private long maxRefAgeMs = -1;
    private int minSnapshotsToKeep = -1;
    private long maxSnapshotAgeMs = -1;
    private boolean isReplaceBranch;

    public String getSourceRefName() {
      return sourceRef;
    }

    public String getTargetBranchName() {
      return targetBranch;
    }

    public boolean isReplaceBySnapshot() {
      return replaceBySnapshot;
    }

    public long getTargetSnapshot() {
      return targetSnapshot;
    }

    public ReplaceSnapshotrefSpec(String sourceRef, String targetBranch) {
      this.sourceRef = sourceRef;
      this.targetBranch = targetBranch;
    }

    public ReplaceSnapshotrefSpec(String sourceRef, long targetSnapshot) {
      this.sourceRef = sourceRef;
      this.targetSnapshot = targetSnapshot;
      replaceBySnapshot = true;
    }

    public void setMaxRefAgeMs(long maxRefAgeMs) {
      Preconditions.checkArgument(maxRefAgeMs > 0);
      this.maxRefAgeMs = maxRefAgeMs;
    }

    public void setMinSnapshotsToKeep(int minSnapshotsToKeep) {
      Preconditions.checkArgument(minSnapshotsToKeep > 0);
      this.minSnapshotsToKeep = minSnapshotsToKeep;
    }

    public void setMaxSnapshotAgeMs(long maxSnapshotAgeMs) {
      Preconditions.checkArgument(maxSnapshotAgeMs > 0);
      this.maxSnapshotAgeMs = maxSnapshotAgeMs;
    }

    public long getMaxRefAgeMs() {
      return maxRefAgeMs;
    }

    public int getMinSnapshotsToKeep() {
      return minSnapshotsToKeep;
    }

    public long getMaxSnapshotAgeMs() {
      return maxSnapshotAgeMs;
    }

    @Override
    public String toString() {
      MoreObjects.ToStringHelper stringHelper = MoreObjects.toStringHelper(this);
      stringHelper.add("sourceRef", sourceRef);
      stringHelper.add("replace", isReplaceBranch ? "Branch" : "Tag");
      if (replaceBySnapshot) {
        stringHelper.add("targetSnapshot", targetSnapshot);
      } else {
        stringHelper.add("targetBranch", targetBranch);
      }
      if (maxRefAgeMs != -1) {
        stringHelper.add("maxRefAgeMs", maxRefAgeMs);
      }
      if (minSnapshotsToKeep != -1) {
        stringHelper.add("minSnapshotsToKeep", minSnapshotsToKeep);
      }
      if (maxSnapshotAgeMs != -1) {
        stringHelper.add("maxSnapshotAgeMs", maxSnapshotAgeMs);
      }
      return stringHelper.toString();
    }

    public void setIsReplaceBranch() {
      this.isReplaceBranch = true;
    }

    public boolean isReplaceBranch() {
      return isReplaceBranch;
    }
  }
}
