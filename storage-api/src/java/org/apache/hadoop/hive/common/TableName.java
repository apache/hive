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
package org.apache.hadoop.hive.common;

import java.io.Serializable;
import java.util.Objects;

/**
 * A container for a fully qualified table name, i.e. catalogname.databasename.tablename.  Also
 * includes utilities for string parsing.
 */
public class TableName implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Exception message thrown. */
  private static final String ILL_ARG_EXCEPTION_MSG =
      "Table name must be either <tablename>, <dbname>.<tablename> " + "or <catname>.<dbname>.<tablename>";

  /** Names of the related DB objects. */
  private final String cat;
  private final String db;
  private final String table;

  /**
   *
   * @param catName catalog name.  Cannot be null.  If you do not know it you can get it from
   *            SessionState.getCurrentCatalog() if you want to use the catalog from the current
   *            session, or from MetaStoreUtils.getDefaultCatalog() if you do not have a session
   *            or want to use the default catalog for the Hive instance.
   * @param dbName database name.  Cannot be null.  If you do not now it you can get it from
   *           SessionState.getCurrentDatabase() or use Warehouse.DEFAULT_DATABASE_NAME.
   * @param tableName  table name, cannot be null
   */
  public TableName(final String catName, final String dbName, final String tableName) {
    this.cat = catName;
    this.db = dbName;
    this.table = tableName;
  }

  /**
   * Build a TableName from a string of the form [[catalog.]database.]table.
   * @param name name in string form, not null
   * @param defaultCatalog default catalog to use if catalog is not in the name.  If you do not
   *                       know it you can get it from SessionState.getCurrentCatalog() if you
   *                       want to use the catalog from the current session, or from
   *                       MetaStoreUtils.getDefaultCatalog() if you do not have a session or
   *                       want to use the default catalog for the Hive instance.
   * @param defaultDatabase default database to use if database is not in the name.  If you do
   *                        not now it you can get it from SessionState.getCurrentDatabase() or
   *                        use Warehouse.DEFAULT_DATABASE_NAME.
   * @return TableName
   * @throws IllegalArgumentException if a non-null name is given
   */
  public static TableName fromString(final String name, final String defaultCatalog, final String defaultDatabase)
      throws IllegalArgumentException {
    if (name == null) {
      throw new IllegalArgumentException(String.join("", "Table value was null. ", ILL_ARG_EXCEPTION_MSG));
    }
    if (name.contains(DatabaseName.CAT_DB_TABLE_SEPARATOR)) {
      String[] names = name.split("\\.");
      if (names.length == 2) {
        return new TableName(defaultCatalog, names[0], names[1]);
      } else if (names.length == 3) {
        return new TableName(names[0], names[1], names[2]);
      } else {
        throw new IllegalArgumentException(ILL_ARG_EXCEPTION_MSG);
      }

    } else {
      return new TableName(defaultCatalog, defaultDatabase, name);
    }
  }

  public String getCat() {
    return cat;
  }

  public String getDb() {
    return db;
  }

  public String getTable() {
    return table;
  }

  /**
   * Get the name in db.table format, for use with stuff not yet converted to use the catalog.
   * Fair warning, that if the db is null, this will return null.tableName
   * @deprecated use {@link #getNotEmptyDbTable()} instead.
   */
  // to be @Deprecated
  public String getDbTable() {
    return db + DatabaseName.CAT_DB_TABLE_SEPARATOR + table;
  }

  /**
   * Get the name in `db`.`table` escaped format, if db is not empty, otherwise pass only the table name.
   */
  public String getEscapedNotEmptyDbTable() {
    return
        db == null || db.trim().isEmpty() ?
            "`" + table + "`" : "`" + db + "`" + DatabaseName.CAT_DB_TABLE_SEPARATOR + "`" + table + "`";
  }

  /**
   * Get the name in db.table format, if db is not empty, otherwise pass only the table name.
   */
  public String getNotEmptyDbTable() {
    return db == null || db.trim().isEmpty() ? table : db + DatabaseName.CAT_DB_TABLE_SEPARATOR + table;
  }

  /**
   * Get the name in db.table format, for use with stuff not yet converted to use the catalog.
   */
  public static String getDbTable(String dbName, String tableName) {
    return dbName + DatabaseName.CAT_DB_TABLE_SEPARATOR + tableName;
  }

  public static String getQualified(String catName, String dbName, String tableName) {
    return catName + DatabaseName.CAT_DB_TABLE_SEPARATOR + dbName + DatabaseName.CAT_DB_TABLE_SEPARATOR + tableName;
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableName tableName = (TableName) o;
    return Objects.equals(cat, tableName.cat) && Objects.equals(db, tableName.db) && Objects
        .equals(table, tableName.table);
  }

  @Override public int hashCode() {
    return Objects.hash(cat, db, table);
  }

  @Override
  public String toString() {
    return cat + DatabaseName.CAT_DB_TABLE_SEPARATOR + db + DatabaseName.CAT_DB_TABLE_SEPARATOR + table;
  }
}
