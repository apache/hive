/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.client.builder;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;

/**
 * Builder for {@link SQLForeignKey}.  Requires what {@link ConstraintBuilder} requires, plus
 * primary key
 * database, table, column and name.
 */
public class SQLForeignKeyBuilder extends ConstraintBuilder<SQLForeignKeyBuilder> {
  private String pkDb, pkTable, pkColumn, pkName;
  private int updateRule, deleteRule;

  public SQLForeignKeyBuilder() {
    updateRule = deleteRule = 0;
  }

  public SQLForeignKeyBuilder setPkDb(String pkDb) {
    this.pkDb = pkDb;
    return this;
  }

  public SQLForeignKeyBuilder setPkTable(String pkTable) {
    this.pkTable = pkTable;
    return this;
  }

  public SQLForeignKeyBuilder setPkColumn(String pkColumn) {
    this.pkColumn = pkColumn;
    return this;
  }

  public SQLForeignKeyBuilder setPkName(String pkName) {
    this.pkName = pkName;
    return this;
  }

  public SQLForeignKeyBuilder setPrimaryKey(SQLPrimaryKey pk) {
    pkDb = pk.getTable_db();
    pkTable = pk.getTable_name();
    pkColumn = pk.getColumn_name();
    pkName = pk.getPk_name();
    return this;
  }

  public SQLForeignKeyBuilder setUpdateRule(int updateRule) {
    this.updateRule = updateRule;
    return this;
  }

  public SQLForeignKeyBuilder setDeleteRule(int deleteRule) {
    this.deleteRule = deleteRule;
    return this;
  }

  public SQLForeignKey build() throws MetaException {
    checkBuildable("foreign_key");
    if (pkDb == null || pkTable == null || pkColumn == null || pkName == null) {
      throw new MetaException("You must provide the primary key database, table, column, and name");
    }
    return new SQLForeignKey(pkDb, pkTable, pkColumn, dbName, tableName, columnName, keySeq,
        updateRule, deleteRule, constraintName, pkName, enable, validate, rely);
  }
}
