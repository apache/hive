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
    SET_CURRENT_SNAPSHOT
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
    private final long timestampMillis;

    public ExpireSnapshotsSpec(long timestampMillis) {
      this.timestampMillis = timestampMillis;
    }

    public Long getTimestampMillis() {
      return timestampMillis;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("timestampMillis", timestampMillis).toString();
    }
  }

  /**
   * Value object class, that stores the set snapshot version operation specific parameters
   * <ul>
   *   <li>snapshot Id: it should be a valid snapshot version</li>
   * </ul>
   */
  public static class SetCurrentSnapshotSpec {
    private final long snapshotId;

    public SetCurrentSnapshotSpec(Long snapshotId) {
      this.snapshotId = snapshotId;
    }

    public Long getSnapshotId() {
      return snapshotId;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("snapshotId", snapshotId).toString();
    }
  }
}
