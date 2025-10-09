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

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;

import java.util.Arrays;

/**
 * Execute operation specification. It stores the type of the operation and its parameters.
 * The following operations are supported
 * <ul>
 *   <li>Rollback</li>
 *   <li>EXPIRE_SNAPSHOT</li>
 * </ul>
 * @param <T> Value object class to store the operation specific parameters
 */
public class AlterTableExecuteSpec<T> {

  public enum ExecuteOperationType {
    ROLLBACK,
    EXPIRE_SNAPSHOT,
    SET_CURRENT_SNAPSHOT,
    FAST_FORWARD,
    CHERRY_PICK,
    DELETE_METADATA,
    DELETE_ORPHAN_FILES;
  }

  private final ExecuteOperationType operationType;
  private final T operationParams;

  public AlterTableExecuteSpec(ExecuteOperationType type, T value) {
    this.operationType = type;
    this.operationParams = value;
  }

  public ExecuteOperationType getOperationType() {
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

  /**
   * Value object class, that stores the rollback operation specific parameters
   * <ul>
   *   <li>Rollback type: it can be either version based or time based</li>
   *   <li>Rollback value: it should either a snapshot id or a timestamp in milliseconds</li>
   * </ul>
   */
  public static class RollbackSpec {

    public enum RollbackType {
      VERSION, TIME
    }

    private final RollbackType rollbackType;
    private final Long param;

    public RollbackSpec(RollbackType rollbackType, Long param) {
      this.rollbackType = rollbackType;
      this.param = param;
    }

    public RollbackType getRollbackType() {
      return rollbackType;
    }

    public Long getParam() {
      return param;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("rollbackType", rollbackType.name())
          .add("param", param).toString();
    }
  }

  /**
   * Value object class, that stores the expire snapshot operation specific parameters
   * <ul>
   *   <li>Expire snapshot value: it should be a timestamp in milliseconds</li>
   * </ul>
   */
  public static class ExpireSnapshotsSpec {
    private long timestampMillis = -1L;
    private String[] idsToExpire = null;

    private long fromTimestampMillis = -1L;

    private int numRetainLast = -1;

    public ExpireSnapshotsSpec(long timestampMillis) {
      this.timestampMillis = timestampMillis;
    }

    public ExpireSnapshotsSpec(String ids) {
      this.idsToExpire = ids.split(",");
    }

    public ExpireSnapshotsSpec(long fromTimestampMillis, long toTimestampMillis) {
      this.fromTimestampMillis = fromTimestampMillis;
      this.timestampMillis = toTimestampMillis;
    }

    public ExpireSnapshotsSpec(int numRetainLast) {
      this.numRetainLast = numRetainLast;
    }

    public Long getTimestampMillis() {
      return timestampMillis;
    }

    public Long getFromTimestampMillis() {
      return fromTimestampMillis;
    }

    public String[] getIdsToExpire() {
      return idsToExpire;
    }

    public int getNumRetainLast() {
      return numRetainLast;
    }

    public boolean isExpireByIds() {
      return idsToExpire != null;
    }

    public boolean isExpireByTimestampRange() {
      return timestampMillis != -1 && fromTimestampMillis != -1;
    }

    public boolean isExpireByRetainLast() {
      return numRetainLast != -1;
    }

    @Override
    public String toString() {
      MoreObjects.ToStringHelper stringHelper = MoreObjects.toStringHelper(this);
      if (isExpireByTimestampRange()) {
        stringHelper.add("fromTimestampMillis", fromTimestampMillis).add("toTimestampMillis", timestampMillis);
      } else if (isExpireByIds()) {
        stringHelper.add("idsToExpire", Arrays.toString(idsToExpire));
      } else if (isExpireByRetainLast()) {
        stringHelper.add("numRetainLast", numRetainLast);
      } else {
        stringHelper.add("timestampMillis", timestampMillis);
      }
      return stringHelper.toString();
    }
  }

  /**
   * Value object class, that stores the set snapshot version operation specific parameters
   * <ul>
   *   <li>snapshot Id: it should be a valid snapshot version or a SnapshotRef name</li>
   * </ul>
   */
  public static class SetCurrentSnapshotSpec {
    private final String snapshotIdOrRefName;

    public SetCurrentSnapshotSpec(String snapshotIdOrRefName) {
      this.snapshotIdOrRefName = snapshotIdOrRefName;
    }

    public String getSnapshotIdOrRefName() {
      return snapshotIdOrRefName;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("snapshotIdOrRefName", snapshotIdOrRefName).toString();
    }
  }

    /**
   * Value object class, that stores the fast-forward operation specific parameters.
   * <ul>
   *   <li>source branch: the branch which needs to be fast-forwarded</li>
     * <li>target branch: the branch to which the source branch needs to be fast-forwarded</li>
   * </ul>
   */
  public static class FastForwardSpec {
    private final String sourceBranch;
    private final String targetBranch;

    public FastForwardSpec(String sourceBranch, String targetBranch) {
      this.sourceBranch = sourceBranch;
      this.targetBranch = targetBranch;
    }

    public String getSourceBranch() {
      return sourceBranch;
    }

    public String getTargetBranch() {
      return targetBranch;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("sourceBranch", sourceBranch)
          .add("targetBranch", targetBranch).toString();
    }
  }

  /**
   * Value object class, that stores the cherry-pick operation specific parameters.
   * <ul>
   *   <li>snapshotId: the snapshotId which needs to be cherry-picked</li>
   * </ul>
   */
  public static class CherryPickSpec {
    private final long snapshotId;

    public CherryPickSpec(long snapshotId) {
      this.snapshotId = snapshotId;
    }

    public long getSnapshotId() {
      return snapshotId;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("snapshotId", snapshotId).toString();
    }
  }

  public static class DeleteMetadataSpec {
    private final String branchName;
    private final SearchArgument sarg;

    public DeleteMetadataSpec(String branchName, SearchArgument sarg) {
      this.branchName = branchName;
      this.sarg = sarg;
    }

    public String getBranchName() {
      return branchName;
    }

    public SearchArgument getSarg() {
      return sarg;
    }
  }

  /**
   * Value object class, that stores the delete orphan files operation specific parameters.
   * <ul>
   *   <li>timestampMillis: the time before which files should be considered to be deleted</li>
   * </ul>
   */
  public static class DeleteOrphanFilesDesc {
    private final long timestampMillis;

    public DeleteOrphanFilesDesc(long timestampMillis) {
      Preconditions.checkArgument(timestampMillis >= 0, "TimeStamp Millis shouldn't be negative");
      this.timestampMillis = timestampMillis;
    }

    public long getTimestampMillis() {
      return timestampMillis;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("timestampMillis", timestampMillis).toString();
    }
  }
}
