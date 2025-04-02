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

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.ql.ddl.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.ql.plan.Explain.Level;


/**
 * ColumnStatsDropWork implementation. ColumnStatsDropWork will drop the
 * colStats from the metastore. Work corresponds to statements like:
 * ALTER TABLE src_stat DROP STATISTICS for columns [comma separated list of columns];
 * ALTER TABLE src_stat_part PARTITION(partitionId=100) DROP STATISTICS for columns [comma separated list of columns];
 */
@Explain(displayName = "Column Stats Drop Work", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ColumnStatsDropWork implements Serializable, DDLDescWithWriteId {
  private static final long serialVersionUID = 1L;
  private final String partName;
  private final String dbName;
  private final String tableName;
  private final List<String> colNames;
  private long writeId;
  
  public ColumnStatsDropWork(String partName, String dbName, String tableName, List<String> colNames) {
    this.partName = partName;
    this.dbName = dbName;
    this.tableName = tableName;
    this.colNames = colNames;
  }

  public String getPartName() {
    return partName;
  }

  public String getDbName() {
    return dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public List<String> getColNames() {
    return colNames;
  }

  @Override
  public void setWriteId(long writeId) {
    this.writeId = writeId;
  }

  @Override
  public String getFullTableName() {
    return dbName + "." + tableName;
  }
}
