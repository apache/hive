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
import java.util.regex.Pattern;

/**
 * A container for a fully qualified table name, i.e. catalogname.databasename.tablename.  Also
 * includes utilities for string parsing.
 */
public class TableName implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Exception message thrown. */
  private static final String ILL_ARG_EXCEPTION_MSG =
      "Table name must be either <tablename>, <dbname>.<tablename> " + "or <catname>.<dbname>.<tablename>";
  public static final Pattern SNAPSHOT_REF = Pattern.compile("(?:branch_|tag_)(.*)");

  /** Names of the related DB objects. */
  private final String cat;
  private final String db;
  private final String table;
  private final String tableMetaRef;

  /**
   *
   * @param catName catalog name.  Cannot be null.  If you do not know it you can get it from
   *            SessionState.getCurrentCatalog() if you want to use the catalog from the current
   *            session, or from MetaStoreUtils.getDefaultCatalog() if you do not have a session
   *            or want to use the default catalog for the Hive instance.
   * @param dbName database name.  Cannot be null.  If you do not now it you can get it from
   *           SessionState.getCurrentDatabase() or use Warehouse.DEFAULT_DATABASE_NAME.
   * @param tableName  table name, cannot be null
   * @param tableMetaRef name
   *           Use this to query table meta ref, e.g. iceberg metadata table or branch
   */
  public TableName(final String catName, final String dbName, final String tableName, String tableMetaRef) {
    this.cat = catName;
    this.db = dbName;
    this.table = tableName;
    this.tableMetaRef = tableMetaRef;
  }

  public TableName(final String catName, final String dbName, final String tableName) {
    this(catName, dbName, tableName, null);
  }

  public static TableName fromString(final String name, final String defaultCatalog, final String defaultDatabase) {
    return fromString(name, defaultCatalog, defaultDatabase, null);
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
   * @param tableMetaRef When querying Iceberg meta ref, e.g. metadata table or branch, set this parameter.
   * @return TableName
   * @throws IllegalArgumentException if a non-null name is given
   */
  public static TableName fromString(final String name, final String defaultCatalog, final String defaultDatabase, String tableMetaRef)
      throws IllegalArgumentException {
    if (name == null) {
      throw new IllegalArgumentException(String.join("", "Table value was null. ", ILL_ARG_EXCEPTION_MSG));
    }
    if (name.contains(DatabaseName.CAT_DB_TABLE_SEPARATOR)) {
      String[] names = name.split("\\.");
      if (names.length == 2) {
        return new TableName(defaultCatalog, names[0], names[1], null);
      } else if (names.length == 3) {
        if (SNAPSHOT_REF.matcher(names[2]).matches()) {
          return new TableName(defaultCatalog, names[0], names[1], names[2]);
        } else {
          return new TableName(names[0], names[1], names[2], null);
        }
      } else {
        throw new IllegalArgumentException(ILL_ARG_EXCEPTION_MSG);
      }

    } else {
      return new TableName(defaultCatalog, defaultDatabase, name, tableMetaRef);
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

  public String getTableMetaRef() {
    return tableMetaRef;
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
    String metaRefName = tableMetaRef == null ? "" : "." + tableMetaRef;
    return db == null || db.trim().isEmpty() ? table : db + DatabaseName.CAT_DB_TABLE_SEPARATOR + table + metaRefName;
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
