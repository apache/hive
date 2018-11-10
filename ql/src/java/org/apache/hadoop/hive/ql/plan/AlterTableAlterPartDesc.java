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

package org.apache.hadoop.hive.ql.plan;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

public class AlterTableAlterPartDesc extends DDLDesc implements DDLDesc.DDLDescWithWriteId {
  private String fqTableName;
  private FieldSchema partKeySpec;
  private long writeId;

  public AlterTableAlterPartDesc() {
  }

  /**
   * @param fqTableName
   *          table containing the partition
   * @param partKeySpec
   */
  public AlterTableAlterPartDesc(String fqTableName, FieldSchema partKeySpec) {
    super();
    this.fqTableName = fqTableName;
    this.partKeySpec = partKeySpec;
  }

  public String getTableName() {
    return fqTableName;
  }

  public void setTableName(String tableName) {
    this.fqTableName = tableName;
  }

  public FieldSchema getPartKeySpec() {
    return partKeySpec;
  }

  public void setPartKeySpec(FieldSchema partKeySpec) {
    this.partKeySpec = partKeySpec;
  }

  @Override
  public void setWriteId(long writeId) {
    this.writeId = writeId;
  }

  @Override
  public String getFullTableName() {
    return fqTableName;
  }

  @Override
  public boolean mayNeedWriteId() {
    return true; // Checked before setting as the acid desc.
  }
}
