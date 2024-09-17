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

package org.apache.hadoop.hive.ql.ddl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.Entity.Type;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.PartitionTransform;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.StorageFormat.StorageHandlerTypes;
import org.apache.hadoop.hive.ql.parse.TransformSpec;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hive.common.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_ICEBERG_STATS_SOURCE;

/**
 * Utilities used by some DDLOperations.
 */
public final class DDLUtils {
  private static final Logger LOG = LoggerFactory.getLogger("hive.ql.exec.DDLTask");

  private DDLUtils() {
    throw new UnsupportedOperationException("DDLUtils should not be instantiated");
  }

  /**
   * There are many places where "duplicate" Read/WriteEnity objects are added.  The way this was
   * initially implemented, the duplicate just replaced the previous object.
   * (work.getOutputs() is a Set and WriteEntity#equals() relies on name)
   * This may be benign for ReadEntity and perhaps was benign for WriteEntity before WriteType was
   * added. Now that WriteEntity has a WriteType it replaces it with one with possibly different
   * {@link org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType}. It's hard to imagine
   * how this is desirable.
   *
   * As of HIVE-14993, WriteEntity with different WriteType must be considered different.
   * So WriteEntity created in DDLTask cause extra output in golden files, but only because
   * DDLTask sets a different WriteType for the same Entity.
   *
   * In the spirit of bug-for-bug compatibility, this method ensures we only add new
   * WriteEntity if it's really new.
   *
   * @return {@code true} if item was added
   */
  public static boolean addIfAbsentByName(WriteEntity newWriteEntity, Set<WriteEntity> outputs) {
    for (WriteEntity writeEntity : outputs) {
      if (writeEntity.getName().equalsIgnoreCase(newWriteEntity.getName())) {
        LOG.debug("Ignoring request to add {} because {} is present", newWriteEntity.toStringDetail(),
            writeEntity.toStringDetail());
        return false;
      }
    }
    outputs.add(newWriteEntity);
    return true;
  }

  public static boolean addIfAbsentByName(WriteEntity newWriteEntity, DDLOperationContext context) {
    return addIfAbsentByName(newWriteEntity, context.getWork().getOutputs());
  }

  /**
   * Check if the given serde is valid.
   */
  public static void validateSerDe(String serdeName, DDLOperationContext context) throws HiveException {
    validateSerDe(serdeName, context.getConf());
  }

  public static void validateSerDe(String serdeName, HiveConf conf) throws HiveException {
    try {
      Deserializer d = ReflectionUtil.newInstance(conf.getClassByName(serdeName).
          asSubclass(Deserializer.class), conf);
      if (d != null) {
        LOG.debug("Found class for {}", serdeName);
      }
    } catch (Exception e) {
      throw new HiveException("Cannot validate serde: " + serdeName, e);
    }
  }

  /**
   * Validate if the given table/partition is eligible for update.
   *
   * @param db Database.
   * @param tableName Table name of format db.table
   * @param partSpec Partition spec for the partition
   * @param replicationSpec Replications specification
   *
   * @return boolean true if allow the operation
   * @throws HiveException
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

  public static void addServiceOutput(HiveConf conf, Set<WriteEntity> outputs) throws SemanticException {
    String hs2Hostname = getHS2Host(conf);
    if (hs2Hostname != null) {
      outputs.add(new WriteEntity(hs2Hostname, Type.SERVICE_NAME));
    }
  }

  private static String getHS2Host(HiveConf conf) throws SemanticException {
    if (SessionState.get().isHiveServerQuery()) {
      return SessionState.get().getHiveServer2Host();
    } else if (conf.getBoolVar(ConfVars.HIVE_TEST_AUTHORIZATION_SQLSTD_HS2_MODE)) {
      return "dummyHostnameForTest";
    }

    throw new SemanticException("Kill query is only supported in HiveServer2 (not hive cli)");
  }

  /**
   * Get the fully qualified name in the node.
   * E.g. the node of the form ^(DOT ^(DOT a b) c) will generate a name of the form "a.b.c".
   */
  public static String getFQName(ASTNode node) {
    if (node.getChildCount() == 0) {
      return node.getText();
    } else if (node.getChildCount() == 2) {
      return getFQName((ASTNode) node.getChild(0)) + "." + getFQName((ASTNode) node.getChild(1));
    } else if (node.getChildCount() == 3) {
      return getFQName((ASTNode) node.getChild(0)) + "." + getFQName((ASTNode) node.getChild(1)) + "." +
          getFQName((ASTNode) node.getChild(2));
    } else {
      return null;
    }
  }

  public static void addDbAndTableToOutputs(Database database, TableName tableName, TableType type, boolean isTemporary,
      Map<String, String> properties, Set<WriteEntity> outputs) {
    outputs.add(new WriteEntity(database, WriteEntity.WriteType.DDL_SHARED));

    Table table = new Table(tableName.getDb(), tableName.getTable());
    table.setParameters(properties);
    table.setTableType(type);
    table.setTemporary(isTemporary);
    outputs.add(new WriteEntity(table, WriteEntity.WriteType.DDL_NO_LOCK));
  }

  public static void setColumnsAndStorePartitionTransformSpecOfTable(
          List<FieldSchema> columns, List<FieldSchema> partitionColumns,
          HiveConf conf, Table tbl) {
    Optional<List<FieldSchema>> cols = Optional.ofNullable(columns);
    Optional<List<FieldSchema>> partCols = Optional.ofNullable(partitionColumns);
    HiveStorageHandler storageHandler = tbl.getStorageHandler();

    if (storageHandler != null && storageHandler.alwaysUnpartitioned()) {
      tbl.getSd().setCols(new ArrayList<>());
      cols.ifPresent(c -> tbl.getSd().getCols().addAll(c));
      if (partCols.isPresent() && !partCols.get().isEmpty()) {
        // Add the partition columns to the normal columns and save the transform to the session state
        tbl.getSd().getCols().addAll(partCols.get());
        List<TransformSpec> spec = PartitionTransform.getPartitionTransformSpec(partCols.get());
        SessionStateUtil.addResourceOrThrow(conf, hive_metastoreConstants.PARTITION_TRANSFORM_SPEC, spec);
      }
    } else {
      cols.ifPresent(tbl::setFields);
      partCols.ifPresent(tbl::setPartCols);
    }
  }

  public static void validateTableIsIceberg(org.apache.hadoop.hive.ql.metadata.Table table)
      throws SemanticException {
    String tableType = table.getParameters().get(HiveMetaHook.TABLE_TYPE);
    if (!HiveMetaHook.ICEBERG.equalsIgnoreCase(tableType)) {
      throw new SemanticException(String.format("Not an iceberg table: %s (type=%s)",
          table.getFullTableName(), tableType));
    }
  }

  public static boolean isIcebergTable(Table table) {
    return table.isNonNative() && 
            table.getStorageHandler().getType() == StorageHandlerTypes.ICEBERG;
  }

  public static boolean isIcebergStatsSource(HiveConf conf) {
    return conf.get(HIVE_ICEBERG_STATS_SOURCE.varname, HiveMetaHook.ICEBERG)
            .equalsIgnoreCase(HiveMetaHook.ICEBERG);
  }
}
