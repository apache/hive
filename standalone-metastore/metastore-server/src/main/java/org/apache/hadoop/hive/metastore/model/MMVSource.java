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

package org.apache.hadoop.hive.metastore.model;

import java.io.Serializable;

public class MMVSource {
  private MCreationMetadata creationMetadata;
  private MTable table;
  private long insertedCount;
  private long updatedCount;
  private long deletedCount;

  public static class PK implements Serializable {
    public MCreationMetadata.PK creationMetadata;
    public MTable.PK table;

    public PK() {}

    public String toString() {
      return String.format("%d,%d", creationMetadata.id, table.id);
    }

    public int hashCode() {
      return toString().hashCode();
    }

    public boolean equals(Object other) {
      if (other != null && (other instanceof MMVSource.PK)) {
        MMVSource.PK otherPK = (MMVSource.PK) other;
        return otherPK.creationMetadata.id == creationMetadata.id && otherPK.table.id == table.id;
      }
      return false;
    }
  }

  public MMVSource() {
  }

  public MCreationMetadata getCreationMetadata() {
    return creationMetadata;
  }

  public void setCreationMetadata(MCreationMetadata creationMetadata) {
    this.creationMetadata = creationMetadata;
  }

  public MTable getTable() {
    return table;
  }

  public void setTable(MTable table) {
    this.table = table;
  }

  public long getInsertedCount() {
    return insertedCount;
  }

  public void setInsertedCount(long insertedCount) {
    this.insertedCount = insertedCount;
  }

  public long getUpdatedCount() {
    return updatedCount;
  }

  public void setUpdatedCount(long updatedCount) {
    this.updatedCount = updatedCount;
  }

  public long getDeletedCount() {
    return deletedCount;
  }

  public void setDeletedCount(long deletedCount) {
    this.deletedCount = deletedCount;
  }
}
