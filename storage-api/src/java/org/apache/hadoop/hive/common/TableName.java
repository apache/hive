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

/**
 * A container for a fully qualified table name, i.e. catalogname.databasename.tablename.  Also
 * includes utilities for string parsing.
 */
public class TableName {
  private final String cat;
  private final String db;
  private final String table;

  /**
   *
   * @param cat catalog name.  Cannot be null.  If you do not know it you can get it from
   *            SessionState.getCurrentCatalog() if you want to use the catalog from the current
   *            session, or from MetaStoreUtils.getDefaultCatalog() if you do not have a session
   *            or want to use the default catalog for the Hive instance.
   * @param db database name.  Cannot be null.  If you do not now it you can get it from
   *           SessionState.getCurrentDatabase() or use Warehouse.DEFAULT_DATABASE_NAME.
   * @param table  table name, cannot be null
   */
  public TableName(String cat, String db, String table) {
    this.cat = cat;
    this.db = db;
    this.table = table;
  }

  /**
   * Build a TableName from a string of the form [[catalog.]database.]table.
   * @param name name in string form
   * @param defaultCatalog default catalog to use if catalog is not in the name.  If you do not
   *                       know it you can get it from SessionState.getCurrentCatalog() if you
   *                       want to use the catalog from the current session, or from
   *                       MetaStoreUtils.getDefaultCatalog() if you do not have a session or
   *                       want to use the default catalog for the Hive instance.
   * @param defaultDatabase default database to use if database is not in the name.  If you do
   *                        not now it you can get it from SessionState.getCurrentDatabase() or
   *                        use Warehouse.DEFAULT_DATABASE_NAME.
   * @return TableName
   */
  public static TableName fromString(String name, String defaultCatalog, String defaultDatabase) {
    if (name.contains(DatabaseName.CAT_DB_TABLE_SEPARATOR)) {
      String names[] = name.split("\\.");
      if (names.length == 2) {
        return new TableName(defaultCatalog, names[0], names[1]);
      } else if (names.length == 3) {
        return new TableName(names[0], names[1], names[2]);
      } else {
        throw new RuntimeException("Table name must be either <tablename>, <dbname>.<tablename> " +
            "or <catname>.<dbname>.<tablename>");
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
   */
  public String getDbTable() {
    return db + DatabaseName.CAT_DB_TABLE_SEPARATOR + table;

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

  @Override
  public int hashCode() {
    return (cat.hashCode() * 31 + db.hashCode()) * 31 + table.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null && obj instanceof TableName) {
      TableName that = (TableName)obj;
      return table.equals(that.table) && db.equals(that.db) && cat.equals(that.cat);
    }
    return false;
  }

  @Override
  public String toString() {
    return cat + DatabaseName.CAT_DB_TABLE_SEPARATOR + db + DatabaseName.CAT_DB_TABLE_SEPARATOR + table;
  }
}
