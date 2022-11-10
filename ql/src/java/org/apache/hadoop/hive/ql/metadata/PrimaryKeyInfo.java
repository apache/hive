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

package org.apache.hadoop.hive.ql.metadata;

import java.io.Serializable;
import java.util.Map;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;

/**
 * PrimaryKeyInfo is a metadata structure containing the primary key associated with a table.
 * The fields include the table name, database name, constraint name, 
 * mapping of the position of the primary key column to the column name.
 * The position is one-based index.
 */
@SuppressWarnings("serial")
public class PrimaryKeyInfo implements Serializable {

  private Map<Integer, String> colNames;
  private String constraintName;
  private String tableName;
  private String databaseName;
  private String enable;
  private String validate;
  private String rely;

  public PrimaryKeyInfo() {}

  public PrimaryKeyInfo(List<SQLPrimaryKey> pks, String tableName, String databaseName) {
    this.tableName = tableName;
    this.databaseName = databaseName;
    this.colNames = new TreeMap<>();
    if (pks ==null) {
      return;
    }
    if (!pks.isEmpty()) {
      this.enable = pks.get(0).isEnable_cstr()? "ENABLE": "DISABLE";
      this.validate = pks.get(0).isValidate_cstr()? "VALIDATE": "NOVALIDATE";
      this.rely = pks.get(0).isRely_cstr()? "RELY": "NORELY";
    }
    for (SQLPrimaryKey pk : pks) {
      if (pk.getTable_db().equalsIgnoreCase(databaseName) &&
          pk.getTable_name().equalsIgnoreCase(tableName)) {
        colNames.put(pk.getKey_seq(), pk.getColumn_name());
        this.constraintName = pk.getPk_name();
      }
    }
  }

  public String getTableName() {
    return tableName;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public Map<Integer, String> getColNames() {
    return colNames;
  }

  public String getConstraintName() {
    return constraintName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public void setConstraintName(String constraintName) {
    this.constraintName = constraintName;
  }

  public void setColNames(Map<Integer, String> colNames) {
    this.colNames = colNames;
  }

  public String getEnable() {
    return enable;
  }

  public void setEnable(String enable) {
    this.enable = enable;
  }

  public String getValidate() {
    return validate;
  }

  public void setValidate(String validate) {
    this.validate = validate;
  }

  public String getRely() {
    return rely;
  }

  public void setRely(String rely) {
    this.rely = rely;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Primary Key for " + databaseName+"."+tableName+":");
    sb.append("[");
    if (colNames != null && colNames.size() > 0) {
      for (Map.Entry<Integer, String> me : colNames.entrySet()) {
        sb.append(me.getValue()+",");
      }
      sb.setLength(sb.length()-1);
    }
    sb.append("], Constraint Name: " + constraintName);
    return sb.toString();
  }

  public static boolean isNotEmpty(PrimaryKeyInfo info) {
    return info != null && !info.getColNames().isEmpty();
  }
}
