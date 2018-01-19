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
import java.util.Map;
import org.apache.hadoop.hive.ql.plan.Explain.Level;


/**
 * ColumnStatsUpdateWork implementation. ColumnStatsUpdateWork will persist the
 * colStats into metastore. Work corresponds to statement like ALTER TABLE
 * src_stat UPDATE STATISTICS for column key SET
 * ('numDVs'='1111','avgColLen'='1.111'); ALTER TABLE src_stat_part
 * PARTITION(partitionId=100) UPDATE STATISTICS for column value SET
 * ('maxColLen'='4444','avgColLen'='44.4');
 */
@Explain(displayName = "Column Stats Update Work", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ColumnStatsUpdateWork implements Serializable {
  private static final long serialVersionUID = 1L;
  private final String partName;
  private final Map<String, String> mapProp;
  private final String dbName;
  private final String tableName;
  private final String colName;
  private final String colType;

  public ColumnStatsUpdateWork(String partName,
      Map<String, String> mapProp,
      String dbName,
      String tableName,
      String colName, 
      String colType) {
    this.partName = partName;
    this.mapProp = mapProp;
    this.dbName = dbName;
    this.tableName = tableName;
    this.colName = colName;
    this.colType = colType;
  }

  @Override
  public String toString() {
    return null;
  }

  public String getPartName() {
    return partName;
  }

  public Map<String, String> getMapProp() {
    return mapProp;
  }

  public String dbName() {
    return dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getColName() {
    return colName;
  }

  public String getColType() {
    return colType;
  }
}
