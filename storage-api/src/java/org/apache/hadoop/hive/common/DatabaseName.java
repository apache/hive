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
 * A container for fully qualified database name, i.e. catalogname.databasename.  Also contains
 * utilities for string parsing.
 */
public class DatabaseName {
  static final String CAT_DB_TABLE_SEPARATOR = ".";
  private final String cat;
  private final String db;

  /**
   *
   * @param cat catalog name.  This cannot be null.  If you don't know the value, then likely the
   *            right answer is to fetch it from SessionState.getCurrentCatalog() if you want to
   *            get the catalog being used in the current session or
   *            MetaStoreUtils.getDefaultCatalog() if you want to get the default catalog for
   *            this Hive instance.
   * @param db database name.  This cannot be null.
   */
  public DatabaseName(String cat, String db) {
    this.cat = cat;
    this.db = db;
  }

  /**
   * Build a DatabaseName from a string of the form [catalog.]database.
   * @param name name, can be "db" or "cat.db"
   * @param defaultCatalog default catalog to use if catalog name is not in the name.  This can
   *                       be null if you are absolutely certain that the catalog name is
   *                       embedded in name.  If you want the default catalog to be determined by
   *                       the session, use SessionState.getCurrentCatalog().  If you want it to
   *                       be determined by the default for the Hive instance or you are not in a
   *                       session, use MetaStoreUtils.getDefaultCatalog().
   * @return new DatabaseName object.
   */
  public static DatabaseName fromString(String name, String defaultCatalog) {
    if (name.contains(CAT_DB_TABLE_SEPARATOR)) {
      String[] names = name.split("\\.");
      if (names.length != 2) {
        throw new RuntimeException("Database name must be either <dbname> or <catname>.<dbname>");
      }
      return new DatabaseName(names[0], names[1]);
    } else {
      assert defaultCatalog != null;
      return new DatabaseName(defaultCatalog, name);
    }
  }

  public String getCat() {
    return cat;
  }

  public String getDb() {
    return db;
  }

  public static String getQualified(String catName, String dbName) {
    return catName + CAT_DB_TABLE_SEPARATOR + dbName;
  }

  @Override
  public int hashCode() {
    return cat.hashCode() * 31 + db.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null && obj instanceof DatabaseName) {
      DatabaseName that = (DatabaseName)obj;
      return db.equals(that.db) && cat.equals(that.cat);
    }
    return false;
  }

  @Override
  public String toString() {
    return cat + CAT_DB_TABLE_SEPARATOR + db;
  }
}
