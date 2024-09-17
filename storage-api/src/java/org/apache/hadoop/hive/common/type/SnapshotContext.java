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

package org.apache.hadoop.hive.common.type;

import java.util.Objects;

/**
 * Represents a table snapshot.
 * This is used by transferring relevant snapshot info using HiveStorageHandler API.
 * Currently, it wraps only Iceberg snapshotId to support decision-making whether to use
 * materialized view on Iceberg tables.
 */
public class SnapshotContext {
  public enum WriteOperationType {
    APPEND,
    REPLACE,
    OVERWRITE,
    DELETE,
    UNKNOWN
  }

  private long snapshotId;
  private WriteOperationType operation;
  private long addedRowCount;
  private long deletedRowCount;

  /**
   * Constructor for json serialization
   */
  private SnapshotContext() {
  }

  public SnapshotContext(long snapshotId) {
    this.snapshotId = snapshotId;
    this.operation = null;
    this.addedRowCount = 0;
    this.deletedRowCount = 0;
  }

  public SnapshotContext(long snapshotId, WriteOperationType operation, long addedRowCount, long deletedRowCount) {
    this.snapshotId = snapshotId;
    this.operation = operation;
    this.addedRowCount = addedRowCount;
    this.deletedRowCount = deletedRowCount;
  }

  /**
   * Unique identifier of this snapshot context and the underlying snapshot.
   * @return long snapshotId.
   */
  public long getSnapshotId() {
    return snapshotId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SnapshotContext that = (SnapshotContext) o;
    return snapshotId == that.snapshotId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(snapshotId);
  }

  public WriteOperationType getOperation() {
    return operation;
  }

  public long getAddedRowCount() {
    return addedRowCount;
  }

  public long getDeletedRowCount() {
    return deletedRowCount;
  }

  @Override
  public String toString() {
    return "SnapshotContext{" +
        "snapshotId=" + snapshotId +
        ", operation=" + operation +
        ", addedRowCount=" + addedRowCount +
        ", deletedRowCount=" + deletedRowCount +
        '}';
  }
}
