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
package org.apache.hadoop.hive.metastore.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;

/**
 * A simple class to contain the full name of a table, catalog, database, and partition.
 */
public class FullTableName {
  public final String catName;
  public final String dbName;
  public final String tableName;

  /**
   *
   * @param concatenatedName Name in cat.db.table or db.table format
   * @param conf configuration object
   */
  public FullTableName(String concatenatedName, Configuration conf) throws MetaException {
    String[] components = concatenatedName.split("\\" + Warehouse.CAT_DB_TABLE_SEPARATOR);
    int index = 0;
    if (components.length == 2) {
      catName = MetaStoreUtils.getDefaultCatalog(conf);
    } else if (components.length == 3) {
      catName = components[index++];
    } else {
      throw new MetaException("Bad format for full table name, expected cat.db.table or db.table: "
          + concatenatedName);
    }
    dbName = components[index++];
    tableName = components[index];
  }

  @Override
  public String toString() {
    return catName + Warehouse.CAT_DB_TABLE_SEPARATOR + dbName + Warehouse.CAT_DB_TABLE_SEPARATOR
        + tableName;
  }
}
