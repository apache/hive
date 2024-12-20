/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.vector;

/**
 * Hive's record representation, where data is provided by an underlying VectorizedRowBatch instance.
 */
public abstract class HiveRow {

  /**
   * Whether this row is marked as deleted or not. VRB implementation registers un-deleted rows in its 'selected' array.
   */
  private boolean deleted = false;

  /**
   * Returns an item/data from the row found on the provided row index.
   * @param rowIndex row index
   * @return data
   */
  public abstract Object get(int rowIndex);

  /**
   * Returns the original position of this row in the en-wrapping VRB instance.
   * @return batch index.
   */
  public abstract int physicalBatchIndex();

  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
  }

  public boolean isDeleted() {
    return deleted;
  }

}
