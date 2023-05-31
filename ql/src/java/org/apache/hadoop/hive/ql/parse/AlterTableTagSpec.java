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

public class AlterTableTagSpec <T>{

  public enum AlterTagOperationType {
    CREATE_TAG
  }

  private final AlterTagOperationType operationType;
  private final T operationParams;

  public AlterTableTagSpec(AlterTagOperationType type, T value) {
    this.operationType = type;
    this.operationParams = value;
  }

  public AlterTagOperationType getOperationType() {
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

  public static class CreateTagSpec {

    private final String tagName;
    private final Long snapshotId;
    private final Long asOfTime;
    private final Long maxRefAgeMs;

    public String getTagName() {
      return tagName;
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

    public CreateTagSpec(String tagName, Long snapShotId, Long asOfTime, Long maxRefAgeMs) {
      this.tagName = tagName;
      this.snapshotId = snapShotId;
      this.asOfTime = asOfTime;
      this.maxRefAgeMs = maxRefAgeMs;
    }

    public String toString() {
      return MoreObjects.toStringHelper(this).add("tagName", tagName).add("snapshotId", snapshotId)
          .add("asOfTime", asOfTime).add("maxRefAgeMs", maxRefAgeMs).toString();
    }
  }
}
