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

/**
 * A binding of a table column to one or more erasure policies.
 */
public class MErasurePolicyBinding {

  private long   tblId;                   // FK to existing TBLS, kept as a long per MErasureIndex precedent
  private String columnName;
  private String schemaField;
  private String rowLocator;
  private String columnFormat;            // JSON | MSGPACK | XML | PROTOBUF | AVRO
  private String resolutionMode;          // EXPLICIT | STRICTEST
  private String createdBy;
  private long   createdTs;

  public MErasurePolicyBinding() {
  }

  public MErasurePolicyBinding(long tblId, String columnName, String schemaField,
                               String rowLocator, String columnFormat,
                               String resolutionMode, String createdBy, long createdTs) {
    this.tblId = tblId;
    this.columnName = columnName;
    this.schemaField = schemaField;
    this.rowLocator = rowLocator;
    this.columnFormat = columnFormat;
    this.resolutionMode = resolutionMode;
    this.createdBy = createdBy;
    this.createdTs = createdTs;
  }

  public long getTblId() { return tblId; }
  public void setTblId(long tblId) { this.tblId = tblId; }

  public String getColumnName() { return columnName; }
  public void setColumnName(String columnName) { this.columnName = columnName; }

  public String getSchemaField() { return schemaField; }
  public void setSchemaField(String schemaField) { this.schemaField = schemaField; }

  public String getRowLocator() { return rowLocator; }
  public void setRowLocator(String rowLocator) { this.rowLocator = rowLocator; }

  public String getColumnFormat() { return columnFormat; }
  public void setColumnFormat(String columnFormat) { this.columnFormat = columnFormat; }

  public String getResolutionMode() { return resolutionMode; }
  public void setResolutionMode(String resolutionMode) { this.resolutionMode = resolutionMode; }

  public String getCreatedBy() { return createdBy; }
  public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }

  public long getCreatedTs() { return createdTs; }
  public void setCreatedTs(long createdTs) { this.createdTs = createdTs; }
}
