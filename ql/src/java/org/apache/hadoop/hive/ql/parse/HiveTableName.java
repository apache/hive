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
package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * A utility class for {@link TableName}.
 */
public final class HiveTableName extends TableName {

  public HiveTableName(String catName, String dbName, String tableName) {
    super(catName, dbName, tableName);
  }

  /**
   * Get a {@link TableName} object based on a {@link Table}. This is basically a wrapper of
   * {@link TableName#fromString(String, String, String)} to throw a {@link SemanticException} in case of errors.
   * @param table the table
   * @return a {@link TableName}
   * @throws SemanticException
   */
  public static TableName of(Table table) throws SemanticException {
    return ofNullable(table.getCatName(), table.getTableName(), table.getDbName(), table.getSnapshotRef());
  }

  /**
   * Set a @{@link Table} object's table and db names based on the provided string.
   * @param dbTable the dbtable string
   * @param table the table to update
   * @throws SemanticException
   */
  public static void setFrom(String dbTable, Table table) throws SemanticException{
    TableName name = ofNullable(SessionState.get().getCurrentCatalog(), dbTable,
        SessionState.get().getCurrentDatabase(), null);
    table.setTableName(name.getTable());
    table.setDbName(name.getDb());
    table.setCatalogName(name.getCat());
  }

  public static TableName ofNullable(String catName, String dbTableName, String defaultDb, String tableMetaRef) throws SemanticException {
    if (dbTableName == null) {
      return new TableName(null, null, null);
    } else {
      try {
        return fromString(dbTableName, catName, defaultDb, tableMetaRef);
      } catch (IllegalArgumentException e) {
        throw new SemanticException(e);
      }
    }
  }

  /**
   * Accepts qualified name which is in the form of table, dbname.tablename or catalog.dbname.tablename and returns a
   * {@link TableName}. This method won't try to find the default db/catalog based on the session state.
   *
   * @param dbTableName not null
   * @return a {@link TableName}
   * @throws SemanticException if dbTableName is null
   * @deprecated use {@link #of(String)} instead and use the default db/catalog.
   */
  // to be @Deprecated
  public static TableName withNoDefault(String dbTableName) throws SemanticException {
    try {
      return fromString(dbTableName, null, null);
    } catch (IllegalArgumentException e) {
      throw new SemanticException(e);
    }
  }

  /**
   * Accepts qualified name which is in the form of table, dbname.tablename or catalog.dbname.tablename and returns a
   * {@link TableName}.
   *
   * @param dbTableName not null
   * @return a {@link TableName}
   * @throws SemanticException if dbTableName is null
   */
  public static TableName of(String dbTableName) throws SemanticException {
    try {
      return fromString(dbTableName, SessionState.get().getCurrentCatalog(), SessionState.get().getCurrentDatabase());
    } catch (IllegalArgumentException e) {
      throw new SemanticException(e);
    }
  }
}
