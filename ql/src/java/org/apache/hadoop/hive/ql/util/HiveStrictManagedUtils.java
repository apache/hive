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

package org.apache.hadoop.hive.ql.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.NullRowsInputFormat;
import org.apache.hadoop.hive.ql.io.OneNullRowInputFormat;
import org.apache.hadoop.hive.ql.io.ZeroRowsInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.AvroTableProperties;

public class HiveStrictManagedUtils {

  private final static Set<String> EXEMPT_INPUTFORMATS =
      new HashSet<String>(Arrays.asList(NullRowsInputFormat.class.getName(),
          OneNullRowInputFormat.class.getName(), ZeroRowsInputFormat.class.getName()));


  public static void validateStrictManagedTable(Configuration conf, Table table)
      throws HiveException {
    String reason = validateStrictManagedTable(conf, table.getTTable());
    if (reason != null) {
      throw new HiveException(reason);
    }
  }

  /**
   * Checks if the table is valid based on the rules for strict managed tables.
   * @param conf
   * @param table
   * @return  Null if the table is valid, otherwise a string message indicating why the table is invalid.
   */
  public static String validateStrictManagedTable(Configuration conf,
      org.apache.hadoop.hive.metastore.api.Table table) {
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_STRICT_MANAGED_TABLES)) {
      if (table.isTemporary()) {
        // temp tables exempted from checks.
        return null;
      }

      TableType tableType = TableType.valueOf(table.getTableType());
      if (tableType == TableType.MANAGED_TABLE) {
        if (!AcidUtils.isTransactionalTable(table)) {
          String inputFormat = null;
          if (table.getSd() != null) {
            inputFormat = table.getSd().getInputFormat();
          }
          if (!EXEMPT_INPUTFORMATS.contains(inputFormat)) {
            return createValidationError(table, "Table is marked as a managed table but is not transactional.");
          }
        }
        if (MetaStoreUtils.isNonNativeTable(table)) {
          return createValidationError(table, "Table is marked as a managed table but is non-native.");
        }
        if (isAvroTableWithExternalSchema(table)) {
          return createValidationError(table, "Managed Avro table has externally defined schema.");
        }
      }
    }

    // Table is valid
    return null;
  }

  public static boolean isAvroTableWithExternalSchema(org.apache.hadoop.hive.metastore.api.Table table) {
    if (table.getSd().getSerdeInfo().getSerializationLib().equals(AvroSerDe.class.getName())) {
      String schemaUrl = table.getParameters().get(AvroTableProperties.SCHEMA_URL.getPropName());
      if (schemaUrl != null && !schemaUrl.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  public static boolean isListBucketedTable(org.apache.hadoop.hive.metastore.api.Table table) {
    return table.getSd().isStoredAsSubDirectories();
  }

  private static String createValidationError(org.apache.hadoop.hive.metastore.api.Table table, String message) {
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
