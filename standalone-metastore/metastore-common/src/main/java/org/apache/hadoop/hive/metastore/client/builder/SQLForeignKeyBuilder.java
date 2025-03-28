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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for {@link SQLForeignKey}.  Requires what {@link ConstraintBuilder} requires, plus
 * primary key
 * database, table, column and name.
 */
public class SQLForeignKeyBuilder extends ConstraintBuilder<SQLForeignKeyBuilder> {
  private String pkDb, pkTable, pkName;
  private List<String> pkColumns;
  private int updateRule, deleteRule;

  public SQLForeignKeyBuilder() {
    super.setChild(this);
    updateRule = deleteRule = 0;
    pkColumns = new ArrayList<>();
    pkDb = Warehouse.DEFAULT_DATABASE_NAME;
  }

  public SQLForeignKeyBuilder setPkDb(String pkDb) {
    this.pkDb = pkDb;
    return this;
  }

  public SQLForeignKeyBuilder setPkTable(String pkTable) {
    this.pkTable = pkTable;
    return this;
  }

  public SQLForeignKeyBuilder addPkColumn(String pkColumn) {
    pkColumns.add(pkColumn);
    return this;
  }

  public SQLForeignKeyBuilder setPkName(String pkName) {
    this.pkName = pkName;
    return this;
  }

  public SQLForeignKeyBuilder fromPrimaryKey(List<SQLPrimaryKey> pk) {
    pkDb = pk.get(0).getTable_db();
    pkTable = pk.get(0).getTable_name();
    for (SQLPrimaryKey pkcol : pk) pkColumns.add(pkcol.getColumn_name());
    pkName = pk.get(0).getPk_name();
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

  public List<SQLForeignKey> build(Configuration conf) throws MetaException {
    checkBuildable("to_" + pkTable + "_foreign_key", conf);
    if (pkTable == null || pkColumns.isEmpty() || pkName == null) {
      throw new MetaException("You must provide the primary key table, columns, and name");
    }
    if (columns.size() != pkColumns.size()) {
      throw new MetaException("The number of foreign columns must match the number of primary key" +
          " columns");
    }
    List<SQLForeignKey> fk = new ArrayList<>(columns.size());
    for (int i = 0; i < columns.size(); i++) {
      SQLForeignKey keyCol = new SQLForeignKey(pkDb, pkTable, pkColumns.get(i), dbName, tableName,
          columns.get(i), getNextSeq(), updateRule, deleteRule, constraintName, pkName, enable,
          validate, rely);
      keyCol.setCatName(catName);
      fk.add(keyCol);
    }
    return fk;
  }
}
