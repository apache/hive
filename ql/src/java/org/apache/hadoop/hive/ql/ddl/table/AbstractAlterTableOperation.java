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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.ddl.table.constraint.add.AlterTableAddConstraintOperation;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Operation process of running some alter table command that requires write id.
 */
public abstract class AbstractAlterTableOperation<T extends AbstractAlterTableDesc> extends DDLOperation<T> {
  public AbstractAlterTableOperation(DDLOperationContext context, T desc) {
    super(context, desc);
  }

  protected EnvironmentContext environmentContext;

  @Override
  public int execute() throws HiveException {
    if (!AlterTableUtils.allowOperationInReplicationScope(context.getDb(), desc.getDbTableName(), null,
        desc.getReplicationSpec())) {
      // no alter, the table is missing either due to drop/rename which follows the alter.
      // or the existing table is newer than our update.
      LOG.debug("DDLTask: Alter Table is skipped as table {} is newer than update", desc.getDbTableName());
      return 0;
    }

    Table oldTable = context.getDb().getTable(desc.getDbTableName());
    List<Partition> partitions = getPartitions(oldTable, desc.getPartitionSpec(), context);

    // Don't change the table object returned by the metastore, as we'll mess with its caches.
    Table table = oldTable.copy();

    environmentContext = initializeEnvironmentContext(oldTable, desc.getEnvironmentContext());

    if (partitions == null) {
      doAlteration(table, null);
    } else {
      for (Partition partition : partitions) {
        doAlteration(table, partition);
      }
    }

    finalizeAlterTableWithWriteIdOp(table, oldTable, partitions, context, environmentContext);
    return 0;
  }

  private List<Partition> getPartitions(Table tbl, Map<String, String> partSpec, DDLOperationContext context)
      throws HiveException {
    List<Partition> partitions = null;
    if (partSpec != null) {
      if (AlterTableUtils.isFullPartitionSpec(tbl, partSpec)) {
        partitions = new ArrayList<Partition>();
        Partition part = context.getDb().getPartition(tbl, partSpec, false);
        if (part == null) {
          // User provided a fully specified partition spec, but it doesn't exist, fail.
          throw new HiveException(ErrorMsg.INVALID_PARTITION,
                StringUtils.join(partSpec.keySet(), ',') + " for table " + tbl.getTableName());

        }
        partitions.add(part);
      } else {
        // AbstractBaseAlterTableAnalyzer has already checked if partial partition specs are allowed,
        // thus we should not need to check it here.
        partitions = context.getDb().getPartitions(tbl, partSpec);
      }
    }

    return partitions;
  }

  private EnvironmentContext initializeEnvironmentContext(Table table, EnvironmentContext environmentContext) {
    EnvironmentContext result = environmentContext == null ? new EnvironmentContext() : environmentContext;
    // do not need to update stats in alter table/partition operations
    if (result.getProperties() == null ||
        result.getProperties().get(StatsSetupConst.DO_NOT_UPDATE_STATS) == null) {
      result.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
    }
    HiveStorageHandler storageHandler = table.getStorageHandler();
    if (storageHandler != null) {
      storageHandler.prepareAlterTableEnvironmentContext(desc, result);
    }
    return result;
  }

  protected abstract void doAlteration(Table table, Partition partition) throws HiveException;

  protected StorageDescriptor getStorageDescriptor(Table tbl, Partition part) {
    return (part == null ? tbl.getTTable().getSd() : part.getTPartition().getSd());
  }

  private void finalizeAlterTableWithWriteIdOp(Table table, Table oldTable, List<Partition> partitions,
      DDLOperationContext context, EnvironmentContext environmentContext)
      throws HiveException {
    if (partitions == null) {
      updateModifiedParameters(table.getTTable().getParameters(), context.getConf());
      checkValidity(table, context);
    } else {
      for (Partition partition : partitions) {
        updateModifiedParameters(partition.getParameters(), context.getConf());
      }
    }

    try {
      environmentContext.putToProperties(HiveMetaHook.ALTER_TABLE_OPERATION_TYPE, desc.getType().name());
      if (desc.getType() == AlterTableType.ADDPROPS) {
        Map<String, String> oldTableParameters = oldTable.getParameters();
        environmentContext.putToProperties(HiveMetaHook.SET_PROPERTIES,
            table.getParameters().entrySet().stream()
                .filter(e -> !oldTableParameters.containsKey(e.getKey()) ||
                    !oldTableParameters.get(e.getKey()).equals(e.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.joining(HiveMetaHook.PROPERTIES_SEPARATOR)));
      } else if (desc.getType() == AlterTableType.DROPPROPS) {
        Map<String, String> newTableParameters = table.getParameters();
        environmentContext.putToProperties(HiveMetaHook.UNSET_PROPERTIES,
            oldTable.getParameters().entrySet().stream()
                .filter(e -> !newTableParameters.containsKey(e.getKey()))
                .map(Map.Entry::getKey)
                .collect(Collectors.joining(HiveMetaHook.PROPERTIES_SEPARATOR)));
      }
      if (partitions == null) {
        long writeId = desc.getWriteId() != null ? desc.getWriteId() : 0;
        try {
          context.getDb().alterTable(desc.getDbTableName(), table, desc.isCascade(), environmentContext, true, writeId);
        } catch (HiveException ex) {
          if (Boolean.valueOf(environmentContext.getProperties()
              .getOrDefault(HiveMetaHook.INITIALIZE_ROLLBACK_MIGRATION, "false"))) {
            // in case of rollback of alter table do the following:
            // 1. restore serde info and input/output format
            // 2. remove table columns which are used to be partition columns
            // 3. add partition columns
            table.getSd().setInputFormat(oldTable.getSd().getInputFormat());
            table.getSd().setOutputFormat(oldTable.getSd().getOutputFormat());
            table.getSd().setSerdeInfo(oldTable.getSd().getSerdeInfo());
            table.getSd().getCols().removeAll(oldTable.getPartitionKeys());
            table.setPartCols(oldTable.getPartitionKeys());

            table.getParameters().clear();
            table.getParameters().putAll(oldTable.getParameters());
            context.getDb().alterTable(desc.getDbTableName(), table, desc.isCascade(), environmentContext, true, writeId);
            throw new HiveException("Error occurred during hive table migration to iceberg. Table properties "
                + "and serde info was reverted to its original value. Partition info was lost during the migration "
                + "process, but it can be reverted by running MSCK REPAIR on table/partition level.\n"
                + "Retrying the migration without issuing MSCK REPAIR on a partitioned table will result in an empty "
                + "iceberg table.");
          } else {
            throw ex;
          }
        }
      } else {
        // Note: this is necessary for UPDATE_STATISTICS command, that operates via ADDPROPS (why?).
        //       For any other updates, we don't want to do txn check on partitions when altering table.
        boolean isTxn = false;
        if (desc.getPartitionSpec() != null && desc.getType() == AlterTableType.ADDPROPS) {
          // ADDPROPS is used to add replication properties like repl.last.id, which isn't
          // transactional change. In case of replication check for transactional properties
          // explicitly.
          Map<String, String> props = desc.getProps();
          if (desc.getReplicationSpec() != null && desc.getReplicationSpec().isInReplicationScope()) {
            isTxn = (props.get(StatsSetupConst.COLUMN_STATS_ACCURATE) != null);
          } else {
            isTxn = true;
          }
        }
        String qualifiedName = TableName.getDbTable(table.getTTable().getDbName(), table.getTTable().getTableName());
        context.getDb().alterPartitions(qualifiedName, partitions, environmentContext, isTxn);
      }
      // Add constraints if necessary
      if (desc instanceof AbstractAlterTableWithConstraintsDesc) {
        AlterTableAddConstraintOperation.addConstraints((AbstractAlterTableWithConstraintsDesc)desc,
            context.getDb());
      }
    } catch (InvalidOperationException e) {
      LOG.error("alter table: ", e);
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR);
    }

    // This is kind of hacky - the read entity contains the old table, whereas the write entity contains the new
    // table. This is needed for rename - both the old and the new table names are passed
    // Don't acquire locks for any of these, we have already asked for them in AbstractBaseAlterTableAnalyzer.
    if (partitions != null) {
      for (Partition partition : partitions) {
        context.getWork().getInputs().add(new ReadEntity(partition));
        DDLUtils.addIfAbsentByName(new WriteEntity(partition, WriteEntity.WriteType.DDL_NO_LOCK), context);
      }
    } else {
      context.getWork().getInputs().add(new ReadEntity(oldTable));
      DDLUtils.addIfAbsentByName(new WriteEntity(table, WriteEntity.WriteType.DDL_NO_LOCK), context);
    }
  }

  protected void checkValidity(Table table, DDLOperationContext context) throws HiveException {
    table.checkValidity(context.getConf());
  }

  private static void updateModifiedParameters(Map<String, String> params, HiveConf conf) throws HiveException {
    String user = SessionState.getUserFromAuthenticator();
    params.put("last_modified_by", user);
    params.put("last_modified_time", Long.toString(System.currentTimeMillis() / 1000));
  }
}
