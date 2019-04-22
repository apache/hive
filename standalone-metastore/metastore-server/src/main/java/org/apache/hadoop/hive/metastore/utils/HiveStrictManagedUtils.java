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

package org.apache.hadoop.hive.metastore.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.TableType;

public class HiveStrictManagedUtils {

  public static void validateStrictManagedTableWithThrow(Configuration conf, Table table)
      throws MetaException {
    String reason = validateStrictManagedTable(conf, table);
    if (reason != null) {
      throw new MetaException(reason);
    }
  }

  /**
   * Checks if the table is valid based on the rules for strict managed tables.
   * @param conf
   * @param table
   * @return  Null if the table is valid, otherwise a string message indicating why the table is invalid.
   */
  public static String validateStrictManagedTable(Configuration conf,
      Table table) {
    if (MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.STRICT_MANAGED_TABLES)) {
      if (table.isTemporary()) {
        // temp tables exempted from checks.
        return null;
      }

      TableType tableType = TableType.valueOf(table.getTableType());
      if (tableType == TableType.MANAGED_TABLE) {
        if (!MetaStoreServerUtils.isTransactionalTable(table.getParameters())) {
          return createValidationError(table, "Table is marked as a managed table but is not transactional.");
        }
        if (MetaStoreUtils.isNonNativeTable(table)) {
          return createValidationError(table, "Table is marked as a managed table but is non-native.");
        }
        if (isAvroTableWithExternalSchema(table)) {
          return createValidationError(table, "Managed Avro table has externally defined schema.");
        }
      } else if (tableType == TableType.EXTERNAL_TABLE) {
        if (MetaStoreServerUtils.isTransactionalTable(table.getParameters())) {
          return createValidationError(table, "Table is marked as a external table but it is transactional.");
        }
      }
    }

    // Table is valid
    return null;
  }

  private static final String AVRO_SERDE_CLASSNAME = "org.apache.hadoop.hive.serde2.avro.AvroSerDe";
  private static final String AVRO_SCHEMA_URL_PROPERTY = "avro.schema.url";

  public static boolean isAvroTableWithExternalSchema(Table table) {
    if (table.getSd().getSerdeInfo().getSerializationLib().equals(AVRO_SERDE_CLASSNAME)) {
      String schemaUrl = table.getParameters().get(AVRO_SCHEMA_URL_PROPERTY);
      if (schemaUrl != null && !schemaUrl.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  public static boolean isListBucketedTable(Table table) {
    return table.getSd().isStoredAsSubDirectories();
  }

  private static String createValidationError(Table table, String message) {
    StringBuilder sb = new StringBuilder();
    sb.append("Table ");
    sb.append(table.getDbName());
    sb.append(".");
    sb.append(table.getTableName());
    sb.append(" failed strict managed table checks due to the following reason: ");
    sb.append(message);
    return sb.toString();
  }
}
