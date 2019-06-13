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
package org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.table;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.partition.AlterTableAddPartitionDesc;
import org.apache.hadoop.hive.ql.ddl.table.partition.AlterTableDropPartitionDesc;
import org.apache.hadoop.hive.ql.exec.ReplCopyTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.repl.ReplExternalTables;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.ReplLoadOpType;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.TableEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.ReplicationState;
import org.apache.hadoop.hive.ql.exec.repl.util.TaskTracker;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.util.Context;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.util.PathUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ImportSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ImportTableDesc;
import org.apache.hadoop.hive.ql.plan.LoadMultiFilesDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc.LoadFileType;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.datanucleus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_ENABLE_MOVE_OPTIMIZATION;
import static org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.ReplicationState.PartitionState;
import static org.apache.hadoop.hive.ql.parse.ImportSemanticAnalyzer.isPartitioned;
import static org.apache.hadoop.hive.ql.parse.ImportSemanticAnalyzer.partSpecToString;

public class LoadPartitions {
  private static Logger LOG = LoggerFactory.getLogger(LoadPartitions.class);

  private final Context context;
  private final ReplLogger replLogger;
  private final TableContext tableContext;
  private final TableEvent event;
  private final TaskTracker tracker;
  private final AlterTableAddPartitionDesc lastReplicatedPartition;

  private final ImportTableDesc tableDesc;
  private Table table;

  public LoadPartitions(Context context, ReplLogger replLogger, TaskTracker tableTracker,
                        TableEvent event, String dbNameToLoadIn,
                        TableContext tableContext) throws HiveException {
    this(context, replLogger, tableContext, tableTracker, event, dbNameToLoadIn, null);
  }

  public LoadPartitions(Context context, ReplLogger replLogger, TableContext tableContext,
                        TaskTracker limiter, TableEvent event, String dbNameToLoadIn,
                        AlterTableAddPartitionDesc lastReplicatedPartition) throws HiveException {
    this.tracker = new TaskTracker(limiter);
    this.event = event;
    this.context = context;
    this.replLogger = replLogger;
    this.lastReplicatedPartition = lastReplicatedPartition;
    this.tableContext = tableContext;

    this.tableDesc = event.tableDesc(dbNameToLoadIn);
    this.table = ImportSemanticAnalyzer.tableIfExists(tableDesc, context.hiveDb);
  }

  public TaskTracker tasks() throws Exception {
    /*
    We are doing this both in load table and load partitions
     */
    Database parentDb = context.hiveDb.getDatabase(tableDesc.getDatabaseName());
    LoadTable.TableLocationTuple tableLocationTuple =
        LoadTable.tableLocation(tableDesc, parentDb, tableContext, context);
    tableDesc.setLocation(tableLocationTuple.location);

    if (table == null) {
      //new table
      table = tableDesc.toTable(context.hiveConf);
      if (isPartitioned(tableDesc)) {
        updateReplicationState(initialReplicationState());
        if (!forNewTable().hasReplicationState()) {
          // Add ReplStateLogTask only if no pending table load tasks left for next cycle
          Task<? extends Serializable> replLogTask
                  = ReplUtils.getTableReplLogTask(tableDesc, replLogger, context.hiveConf);
          tracker.addDependentTask(replLogTask);
        }
        return tracker;
      }
    } else {
      // existing
      if (table.isPartitioned()) {
        List<AlterTableAddPartitionDesc> partitionDescs = event.partitionDescriptions(tableDesc);
        if (!event.replicationSpec().isMetadataOnly() && !partitionDescs.isEmpty()) {
          updateReplicationState(initialReplicationState());
          if (!forExistingTable(lastReplicatedPartition).hasReplicationState()) {
            // Add ReplStateLogTask only if no pending table load tasks left for next cycle
            Task<? extends Serializable> replLogTask
                    = ReplUtils.getTableReplLogTask(tableDesc, replLogger, context.hiveConf);
            tracker.addDependentTask(replLogTask);
          }
          return tracker;
        }
      }
    }
    return tracker;
  }

  private void updateReplicationState(ReplicationState replicationState) {
    if (!tracker.canAddMoreTasks()) {
      tracker.setReplicationState(replicationState);
    }
  }

  private ReplicationState initialReplicationState() throws SemanticException {
    return new ReplicationState(
        new PartitionState(tableDesc.getTableName(), lastReplicatedPartition)
    );
  }

  private TaskTracker forNewTable() throws Exception {
    Iterator<AlterTableAddPartitionDesc> iterator = event.partitionDescriptions(tableDesc).iterator();
    while (iterator.hasNext() && tracker.canAddMoreTasks()) {
      AlterTableAddPartitionDesc currentPartitionDesc = iterator.next();
      /*
       the currentPartitionDesc cannot be inlined as we need the hasNext() to be evaluated post the
       current retrieved lastReplicatedPartition
      */
      addPartition(iterator.hasNext(), currentPartitionDesc, null);
    }
    return tracker;
  }

  private void addPartition(boolean hasMorePartitions, AlterTableAddPartitionDesc addPartitionDesc, Task<?> ptnRootTask)
          throws Exception {
    tracker.addTask(tasksForAddPartition(table, addPartitionDesc, ptnRootTask));
    if (hasMorePartitions && !tracker.canAddMoreTasks()) {
      ReplicationState currentReplicationState =
          new ReplicationState(new PartitionState(table.getTableName(), addPartitionDesc));
      updateReplicationState(currentReplicationState);
    }
  }

  /**
   * returns the root task for adding a partition
   */
  private Task<?> tasksForAddPartition(Table table, AlterTableAddPartitionDesc addPartitionDesc, Task<?> ptnRootTask)
          throws MetaException, HiveException {
    AlterTableAddPartitionDesc.PartitionDesc partSpec = addPartitionDesc.getPartition(0);
    Path sourceWarehousePartitionLocation = new Path(partSpec.getLocation());
    Path replicaWarehousePartitionLocation = locationOnReplicaWarehouse(table, partSpec);
    partSpec.setLocation(replicaWarehousePartitionLocation.toString());
    LOG.debug("adding dependent CopyWork/AddPart/MoveWork for partition "
        + partSpecToString(partSpec.getPartSpec()) + " with source location: "
        + partSpec.getLocation());

    Task<?> addPartTask = TaskFactory.get(
            new DDLWork(new HashSet<>(), new HashSet<>(), addPartitionDesc),
            context.hiveConf
    );

    Task<?> ckptTask = ReplUtils.getTableCheckpointTask(
            tableDesc,
            (HashMap<String, String>)partSpec.getPartSpec(),
            context.dumpDirectory,
            context.hiveConf
    );

    boolean isOnlyDDLOperation = event.replicationSpec().isMetadataOnly()
        || (TableType.EXTERNAL_TABLE.equals(table.getTableType())
        && !event.replicationSpec().isMigratingToExternalTable()
    );

    if (isOnlyDDLOperation) {
      // Set Checkpoint task as dependant to add partition tasks. So, if same dump is retried for
      // bootstrap, we skip current partition update.
      addPartTask.addDependentTask(ckptTask);
      if (ptnRootTask == null) {
        ptnRootTask = addPartTask;
      } else {
        ptnRootTask.addDependentTask(addPartTask);
      }
      return ptnRootTask;
    }

    Path stagingDir = replicaWarehousePartitionLocation;
    // if move optimization is enabled, copy the files directly to the target path. No need to create the staging dir.
    LoadFileType loadFileType;
    if (event.replicationSpec().isInReplicationScope() &&
            context.hiveConf.getBoolVar(REPL_ENABLE_MOVE_OPTIMIZATION)) {
      loadFileType = LoadFileType.IGNORE;
      if (event.replicationSpec().isMigratingToTxnTable()) {
        // Migrating to transactional tables in bootstrap load phase.
        // It is enough to copy all the original files under base_1 dir and so write-id is hardcoded to 1.
        // ReplTxnTask added earlier in the DAG ensure that the write-id=1 is made valid in HMS metadata.
        stagingDir = new Path(stagingDir, AcidUtils.baseDir(ReplUtils.REPL_BOOTSTRAP_MIGRATION_BASE_WRITE_ID));
      }
    } else {
       loadFileType = event.replicationSpec().isReplace() ? LoadFileType.REPLACE_ALL :
          (event.replicationSpec().isMigratingToTxnTable()
              ? LoadFileType.KEEP_EXISTING
              : LoadFileType.OVERWRITE_EXISTING);
      stagingDir = PathUtils.getExternalTmpPath(replicaWarehousePartitionLocation, context.pathInfo);
    }

    Task<?> copyTask = ReplCopyTask.getLoadCopyTask(
        event.replicationSpec(),
        sourceWarehousePartitionLocation,
        stagingDir,
        context.hiveConf
    );

    Task<?> movePartitionTask = null;
    if (loadFileType != LoadFileType.IGNORE) {
      // no need to create move task, if file is moved directly to target location.
      movePartitionTask = movePartitionTask(table, partSpec, stagingDir, loadFileType);
    }

    if (ptnRootTask == null) {
      ptnRootTask = copyTask;
    } else {
      ptnRootTask.addDependentTask(copyTask);
    }

    // Set Checkpoint task as dependant to the tail of add partition tasks. So, if same dump is
    // retried for bootstrap, we skip current partition update.
    copyTask.addDependentTask(addPartTask);
    if (movePartitionTask != null) {
      addPartTask.addDependentTask(movePartitionTask);
      movePartitionTask.addDependentTask(ckptTask);
    } else {
      addPartTask.addDependentTask(ckptTask);
    }
    return ptnRootTask;
  }

  /**
   * This will create the move of partition data from temp path to actual path
   */
  private Task<?> movePartitionTask(Table table, AlterTableAddPartitionDesc.PartitionDesc partSpec, Path tmpPath,
                                    LoadFileType loadFileType) {
    MoveWork moveWork = new MoveWork(new HashSet<>(), new HashSet<>(), null, null, false);
    if (AcidUtils.isTransactionalTable(table)) {
      if (event.replicationSpec().isMigratingToTxnTable()) {
        // Write-id is hardcoded to 1 so that for migration, we just move all original files under base_1 dir.
        // ReplTxnTask added earlier in the DAG ensure that the write-id is made valid in HMS metadata.
        LoadTableDesc loadTableWork = new LoadTableDesc(
                tmpPath, Utilities.getTableDesc(table), partSpec.getPartSpec(),
                loadFileType, ReplUtils.REPL_BOOTSTRAP_MIGRATION_BASE_WRITE_ID
        );
        loadTableWork.setInheritTableSpecs(false);
        loadTableWork.setStmtId(0);

        // Need to set insertOverwrite so base_1 is created instead of delta_1_1_0.
        loadTableWork.setInsertOverwrite(true);
        moveWork.setLoadTableWork(loadTableWork);
      } else {
        LoadMultiFilesDesc loadFilesWork = new LoadMultiFilesDesc(
                Collections.singletonList(tmpPath),
                Collections.singletonList(new Path(partSpec.getLocation())),
                true, null, null);
        moveWork.setMultiFilesDesc(loadFilesWork);
      }
    } else {
      LoadTableDesc loadTableWork = new LoadTableDesc(
              tmpPath, Utilities.getTableDesc(table), partSpec.getPartSpec(),
              loadFileType, 0L
      );
      loadTableWork.setInheritTableSpecs(false);
      moveWork.setLoadTableWork(loadTableWork);
    }
    moveWork.setIsInReplicationScope(event.replicationSpec().isInReplicationScope());

    return TaskFactory.get(moveWork, context.hiveConf);
  }

  /**
   * Since the table level location will be set by taking into account the base directory configuration
   * for external table, we don't have to do anything specific for partition location since it will always
   * be a child of the table level location.
   * Looks like replication does not handle a specific location provided for a partition and the partition
   * path will always be a child on target.
   */

  private Path locationOnReplicaWarehouse(Table table, AlterTableAddPartitionDesc.PartitionDesc partSpec)
      throws MetaException, HiveException {
    String child = Warehouse.makePartPath(partSpec.getPartSpec());
    if (tableDesc.isExternal()) {
      if (event.replicationSpec().isMigratingToExternalTable()) {
        return new Path(tableDesc.getLocation(), child);
      }
      String externalLocation =
          ReplExternalTables.externalTableLocation(context.hiveConf, partSpec.getLocation());
      return new Path(externalLocation);
    }

    if (tableDesc.getLocation() == null) {
      if (table.getDataLocation() == null) {
        Database parentDb = context.hiveDb.getDatabase(tableDesc.getDatabaseName());
        return new Path(
            context.warehouse.getDefaultTablePath(parentDb, tableDesc.getTableName(), tableDesc.isExternal()),
            child);
      } else {
        return new Path(table.getDataLocation().toString(), child);
      }
    } else {
      return new Path(tableDesc.getLocation(), child);
    }
  }

  private Task<?> dropPartitionTask(Table table, Map<String, String> partSpec) throws SemanticException {
    Task<DDLWork> dropPtnTask = null;
    Map<Integer, List<ExprNodeGenericFuncDesc>> partSpecsExpr =
            ReplUtils.genPartSpecs(table, Collections.singletonList(partSpec));
    if (partSpecsExpr.size() > 0) {
      AlterTableDropPartitionDesc dropPtnDesc = new AlterTableDropPartitionDesc(table.getFullyQualifiedName(),
          partSpecsExpr, true, event.replicationSpec());
      dropPtnTask = TaskFactory.get(
              new DDLWork(new HashSet<>(), new HashSet<>(), dropPtnDesc), context.hiveConf
      );
    }
    return dropPtnTask;
  }

  private TaskTracker forExistingTable(AlterTableAddPartitionDesc lastPartitionReplicated) throws Exception {
    boolean encounteredTheLastReplicatedPartition = (lastPartitionReplicated == null);
    Map<String, String> lastReplicatedPartSpec = null;
    if (!encounteredTheLastReplicatedPartition) {
      lastReplicatedPartSpec = lastPartitionReplicated.getPartition(0).getPartSpec();
      LOG.info("Start processing from partition info spec : {}",
          StringUtils.mapToString(lastReplicatedPartSpec));
    }

    Iterator<AlterTableAddPartitionDesc> partitionIterator = event.partitionDescriptions(tableDesc).iterator();
    while (!encounteredTheLastReplicatedPartition && partitionIterator.hasNext()) {
      AlterTableAddPartitionDesc addPartitionDesc = partitionIterator.next();
      Map<String, String> currentSpec = addPartitionDesc.getPartition(0).getPartSpec();
      encounteredTheLastReplicatedPartition = lastReplicatedPartSpec.equals(currentSpec);
    }

    while (partitionIterator.hasNext() && tracker.canAddMoreTasks()) {
      AlterTableAddPartitionDesc addPartitionDesc = partitionIterator.next();
      Map<String, String> partSpec = addPartitionDesc.getPartition(0).getPartSpec();
      Task<?> ptnRootTask = null;
      ReplLoadOpType loadPtnType = getLoadPartitionType(partSpec);
      switch (loadPtnType) {
        case LOAD_NEW:
          break;
        case LOAD_REPLACE:
          ptnRootTask = dropPartitionTask(table, partSpec);
          break;
        case LOAD_SKIP:
          continue;
        default:
          break;
      }
      addPartition(partitionIterator.hasNext(), addPartitionDesc, ptnRootTask);
    }
    return tracker;
  }

  private ReplLoadOpType getLoadPartitionType(Map<String, String> partSpec) throws InvalidOperationException, HiveException {
    Partition ptn = context.hiveDb.getPartition(table, partSpec, false);
    if (ptn == null) {
      return ReplLoadOpType.LOAD_NEW;
    }
    if (ReplUtils.replCkptStatus(tableContext.dbNameToLoadIn, ptn.getParameters(), context.dumpDirectory)) {
      return ReplLoadOpType.LOAD_SKIP;
    }
    return ReplLoadOpType.LOAD_REPLACE;
  }
}

