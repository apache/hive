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

package org.apache.hadoop.hive.metastore;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.api.CreateTableRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * HiveMetaHook defines notification methods which are invoked as part
 * of transactions against the metastore, allowing external catalogs
 * such as HBase to be kept in sync with Hive's metastore.
 *
 *<p>
 *
 * Implementations can use {@link org.apache.hadoop.hive.metastore.utils.MetaStoreUtils#isExternalTable} to
 * distinguish external tables from managed tables.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface HiveMetaHook {

  String ALTER_TABLE_OPERATION_TYPE = "alterTableOpType";

  // These should remain in sync with AlterTableType enum
  List<String> allowedAlterTypes = ImmutableList.of("ADDPROPS", "DROPPROPS");
  String ALTERLOCATION = "ALTERLOCATION";
  String ALLOW_PARTITION_KEY_CHANGE = "allow_partition_key_change";
  String SET_PROPERTIES = "set_properties";
  String UNSET_PROPERTIES = "unset_properties";

  String TRANSLATED_TO_EXTERNAL = "TRANSLATED_TO_EXTERNAL";

  String TABLE_TYPE = "table_type";
  String EXTERNAL = "EXTERNAL";
  String ICEBERG = "ICEBERG";
  String HIVE_ICEBERG_STORAGE_HANDLER = "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler";
  
  String PROPERTIES_SEPARATOR = "'";
  String MIGRATE_HIVE_TO_ICEBERG = "migrate_hive_to_iceberg";
  String INITIALIZE_ROLLBACK_MIGRATION = "initialize_rollback_migration";
  // if this flag is set to true, the HMS call from HiveMetaStoreClient#alter_table() will be skipped
  String SKIP_METASTORE_ALTER = "skip_metastore_alter";

  String OLD_TABLE_NAME = "old_table_name";

  String OLD_DB_NAME = "old_db_name";

  /**
   * Called before a new table definition is added to the metastore
   * during CREATE TABLE.
   *
   * @param table new table definition
   */
  void preCreateTable(Table table)
    throws MetaException;

  /**
   * Called before a new table definition is added to the metastore
   * during CREATE TABLE.
   *
   * @param request the whole request to create a new table
   */
  default void preCreateTable(CreateTableRequest request)
          throws MetaException {
    preCreateTable(request.getTable());
  }

  /**
   * Called after failure adding a new table definition to the metastore
   * during CREATE TABLE.
   *
   * @param table new table definition
   */
  void rollbackCreateTable(Table table)
    throws MetaException;

  /**
   * Called after successfully adding a new table definition to the metastore
   * during CREATE TABLE.
   *
   * @param table new table definition
   */
  void commitCreateTable(Table table)
    throws MetaException;

  /**
   * Called before a table definition is removed from the metastore
   * during DROP TABLE.
   *
   * @param table table definition
   */
  void preDropTable(Table table)
    throws MetaException;

  /**
   * Called before a table definition is removed from the metastore
   * during DROP TABLE
   *
   * @param table table definition
   * @param deleteData whether to delete data as well; this should typically
   * be ignored in the case of an external table
   */
  default void preDropTable(Table table, boolean deleteData) throws MetaException {
    preDropTable(table);
  }

  /**
   * Called after failure removing a table definition from the metastore
   * during DROP TABLE.
   *
   * @param table table definition
   */
  void rollbackDropTable(Table table)
    throws MetaException;

  /**
   * Called after successfully removing a table definition from the metastore
   * during DROP TABLE.
   *
   * @param table table definition
   *
   * @param deleteData whether to delete data as well; this should typically
   * be ignored in the case of an external table
   */
  void commitDropTable(Table table, boolean deleteData)
    throws MetaException;

  /**
   * Called before a table is altered in the metastore
   * during ALTER TABLE.
   *
   * @param table new table definition
   */
  default void preAlterTable(Table table, EnvironmentContext context) throws MetaException {
    String alterOpType = (context == null || context.getProperties() == null) ?
        null : context.getProperties().get(ALTER_TABLE_OPERATION_TYPE);
    // By default allow only ADDPROPS and DROPPROPS.
    // alterOpType is null in case of stats update.
    if (alterOpType != null && !allowedAlterTypes.contains(alterOpType)){
      throw new MetaException(
          "ALTER TABLE can not be used for " + alterOpType + " to a non-native table ");
    }
  }

  /**
   * Called after a table is altered in the metastore during ALTER TABLE.
   * @param table new table definition
   * @param context environment context, containing information about the alter operation type
   */
  default void commitAlterTable(Table table, EnvironmentContext context) throws MetaException {
    // Do nothing
  }

  /**
   * Called after failure altering a table definition from the metastore
   * during ALTER TABLE
   * @param table new table definition
   * @param context context of the alter operation
   */
  default void rollbackAlterTable(Table table, EnvironmentContext context) throws MetaException {
    // Do nothing
  }

  /**
   * Called before deleting the data and statistics from the table in the metastore during TRUNCATE TABLE.
   * @param table table to be truncated
   * @param context context of the truncate operation
   * @throws MetaException
   */
  default void preTruncateTable(Table table, EnvironmentContext context) throws MetaException {
    preTruncateTable(table, context, null);
  }

  default void preTruncateTable(Table table, EnvironmentContext context, List<String> partNames) throws MetaException {
    // Do nothing
  }

  /**
   * Returns true if the HMS table should be created by the implementing class.
   * @return
   */
  default boolean createHMSTableInHook() {
    return false;
  }

  /**
   *  Set storage handler specific table properties
   * @param table
   */
  default void postGetTable(Table table) {
    // Do nothing
  }

  /**
   * Called before dropping the partitions from the table in the metastore during ALTER TABLE DROP PARTITION.
   * @param table table whose partition needs to be dropped
   * @param context context of the  operation
   * @param partExprs List of partition expressions
   * @throws MetaException
   */
  default void preDropPartitions(Table table,
      EnvironmentContext context, List<Pair<Integer, byte[]>> partExprs) throws MetaException {
    // Do nothing
  }
}
