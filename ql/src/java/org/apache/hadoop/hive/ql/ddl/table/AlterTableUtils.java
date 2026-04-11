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

package org.apache.hadoop.hive.ql.ddl.table;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;

/**
 * Utilities used by some ALTER TABLE commands.
 */
public final class AlterTableUtils {
  private AlterTableUtils() {
    throw new UnsupportedOperationException("AlterTableUtils should not be instantiated");
  }

  /**
   * Validate if the given table/partition is eligible for update.
   */
  public static boolean allowOperationInReplicationScope(Hive db, String tableName, Map<String, String> partSpec,
      ReplicationSpec replicationSpec) throws HiveException {
    if ((null == replicationSpec) || (!replicationSpec.isInReplicationScope())) {
      // Always allow the operation if it is not in replication scope.
      return true;
    }

    // If the table/partition exist and is older than the event, then just apply the event else noop.
    Table existingTable = db.getTable(tableName, false);
    if (existingTable != null) {
      Map<String, String> dbParams = db.getDatabase(existingTable.getDbName()).getParameters();
      if (replicationSpec.allowEventReplacementInto(dbParams)) {
        // Table exists and is older than the update. Now, need to ensure if update allowed on the partition.
        if (partSpec != null) {
          Partition existingPtn = db.getPartition(existingTable, partSpec, false);
          return ((existingPtn != null) && replicationSpec.allowEventReplacementInto(dbParams));
        }

        // Replacement is allowed as the existing table is older than event
        return true;
      }
    }
    // The table is missing either due to drop/rename which follows the operation.
    // Or the existing table is newer than our update. So, don't allow the update.
    return false;
  }

  public static boolean isSchemaEvolutionEnabled(Table table, Configuration conf) {
    return AcidUtils.isTablePropertyTransactional(table.getMetadata()) ||
        HiveConf.getBoolVar(conf, ConfVars.HIVE_SCHEMA_EVOLUTION);
  }

  public static boolean isFullPartitionSpec(Table table, Map<String, String> partitionSpec) {
    for (FieldSchema partitionCol : table.getPartCols()) {
      if (partitionSpec.get(partitionCol.getName()) == null) {
        return false;
      }
    }
    return true;
  }
}
