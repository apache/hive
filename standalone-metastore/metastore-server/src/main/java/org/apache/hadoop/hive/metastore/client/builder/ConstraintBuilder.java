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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Base builder for all types of constraints.  Database name, table name, and column name
 * must be provided.
 * @param <T> Type of builder extending this.
 */
abstract class ConstraintBuilder<T> {
  protected String catName, dbName, tableName, constraintName;
  List<String> columns;
  protected boolean enable, validate, rely;
  private int nextSeq;
  private T child;

  protected ConstraintBuilder() {
    nextSeq = 1;
    enable = true;
    validate = rely = false;
    dbName = Warehouse.DEFAULT_DATABASE_NAME;
    columns = new ArrayList<>();
  }

  protected void setChild(T child) {
    this.child = child;
  }

  protected void checkBuildable(String defaultConstraintName, Configuration conf)
      throws MetaException {
    if (tableName == null || columns.isEmpty()) {
      throw new MetaException("You must provide table name and columns");
    }
    if (constraintName == null) {
      constraintName = tableName + "_" + defaultConstraintName;
    }
    if (catName == null) catName = MetaStoreUtils.getDefaultCatalog(conf);
  }

  protected int getNextSeq() {
    return nextSeq++;
  }

  public T setCatName(String catName) {
    this.catName = catName;
    return child;
  }

  public T setDbName(String dbName) {
    this.dbName = dbName;
    return child;
  }

  public T setTableName(String tableName) {
    this.tableName = tableName;
    return child;
  }

  public T onTable(Table table) {
    this.catName = table.getCatName();
    this.dbName = table.getDbName();
    this.tableName = table.getTableName();
    return child;
  }

  public T addColumn(String columnName) {
    this.columns.add(columnName);
    return child;
  }

  public T setConstraintName(String constraintName) {
    this.constraintName = constraintName;
    return child;
  }

  public T setEnable(boolean enable) {
    this.enable = enable;
    return child;
  }

  public T setValidate(boolean validate) {
    this.validate = validate;
    return child;
  }

  public T setRely(boolean rely) {
    this.rely = rely;
    return child;
  }
}
