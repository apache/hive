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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.partition.add.AlterTableAddPartitionDesc;
import org.apache.hadoop.hive.ql.ddl.table.partition.drop.AlterTableDropPartitionDesc;
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
import org.apache.hadoop.hive.ql.parse.HiveTableName;
import org.apache.hadoop.hive.ql.parse.ImportSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_DUMP_SKIP_IMMUTABLE_DATA_COPY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_ENABLE_MOVE_OPTIMIZATION;
import static org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.ReplicationState.PartitionState;
import static org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.REPL_CHECKPOINT_KEY;
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
  private final ReplicationMetricCollector metricCollector;

  private final ImportTableDesc tableDesc;
  private Table table;

  public LoadPartitions(Context context, ReplLogger replLogger, TaskTracker tableTracker,
                        TableEvent event, String dbNameToLoadIn,
                        TableContext tableContext, ReplicationMetricCollector metricCollector) throws HiveException {
    this(context, replLogger, tableContext, tableTracker, event, dbNameToLoadIn, null,
      metricCollector);
  }

  public LoadPartitions(Context context, ReplLogger replLogger, TableContext tableContext,
                        TaskTracker limiter, TableEvent event, String dbNameToLoadIn,
                        AlterTableAddPartitionDesc lastReplicatedPartition,
                        ReplicationMetricCollector metricCollector) throws HiveException {
    this.tracker = new TaskTracker(limiter);
    this.event = event;
    this.context = context;
    this.replLogger = replLogger;
    this.lastReplicatedPartition = lastReplicatedPartition;
    this.tableContext = tableContext;

    this.tableDesc = event.tableDesc(dbNameToLoadIn);
    this.table = ImportSemanticAnalyzer.tableIfExists(tableDesc, context.hiveDb);
    this.metricCollector = metricCollector;
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
                  = ReplUtils.getTableReplLogTask(tableDesc, replLogger, context.hiveConf, metricCollector);
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
                    = ReplUtils.getTableReplLogTask(tableDesc, replLogger, context.hiveConf, metricCollector);
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

  private boolean isMetaDataOp() {
    return HiveConf.getBoolVar(context.hiveConf, REPL_DUMP_SKIP_IMMUTABLE_DATA_COPY) ||
            HiveConf.getBoolVar(context.hiveConf, REPL_DUMP_METADATA_ONLY);
  }

  /**
   * Get all partitions in a batch and consolidate them into single partition request.
   * Also, copy relevant stats and other information from original request.
   *
   * @throws SemanticException
   */
  private void addConsolidatedPartitionDesc() throws Exception {
  //Load partitions equal to batch size at one go for metadata only and for external tables.
    int maxTasks = context.hiveConf.getIntVar(HiveConf.ConfVars.REPL_LOAD_PARTITIONS_BATCH_SIZE);
    int currentPartitionCount = 0;
    List<AlterTableAddPartitionDesc> partitionDescs = event.partitionDescriptions(tableDesc);
    int totalPartitionCount = partitionDescs.size();
    while (currentPartitionCount < totalPartitionCount) {
      List<AlterTableAddPartitionDesc.PartitionDesc> partitions = new LinkedList<>();
      int pendingPartitionCount = totalPartitionCount - currentPartitionCount;
      int toPartitionCount = currentPartitionCount + Math.min(pendingPartitionCount, maxTasks);
      List<AlterTableAddPartitionDesc> partitionBatch = partitionDescs.subList(currentPartitionCount,
        toPartitionCount);
      for (AlterTableAddPartitionDesc addPartitionDesc : partitionBatch) {
        AlterTableAddPartitionDesc.PartitionDesc src = addPartitionDesc.getPartition(0);
        Map<String, String> partParams = src.getPartParams();
        if (partParams == null) {
          partParams = new HashMap<>();
        }
        partParams.put(REPL_CHECKPOINT_KEY, context.dumpDirectory);
        Path replicaWarehousePartitionLocation = locationOnReplicaWarehouse(table, src);
        partitions.add(new AlterTableAddPartitionDesc.PartitionDesc(
          src.getPartSpec(), replicaWarehousePartitionLocation.toString(), partParams, src.getInputFormat(),
          src.getOutputFormat(), src.getNumBuckets(), src.getCols(), src.getSerializationLib(),
          src.getSerdeParams(), src.getBucketCols(), src.getSortCols(), src.getColStats(),
          src.getWriteId()));
      }
      AlterTableAddPartitionDesc consolidatedPartitionDesc = new AlterTableAddPartitionDesc(tableDesc.getDatabaseName(),
        tableDesc.getTableName(), true, partitions);

      //don't need to add ckpt task separately. Added as part of add partition task
      addPartition((toPartitionCount < totalPartitionCount), consolidatedPartitionDesc, null);
      if (partitions.size() > 0) {
        LOG.info("Added {} partitions", partitions.size());
      }
      currentPartitionCount = toPartitionCount;
    }
  }

  private TaskTracker forNewTable() throws Exception {
    if (isMetaDataOp() || TableType.EXTERNAL_TABLE.equals(table.getTableType())) {
      // Place all partitions in single task to reduce load on HMS.
      addConsolidatedPartitionDesc();
      return tracker;
    }
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
    Task<?> addPartTask = TaskFactory.get(
      new DDLWork(new HashSet<>(), new HashSet<>(), addPartitionDesc),
      context.hiveConf
    );
    //checkpointing task already added as part of add batch of partition in case for metadata only and external tables
    if (isMetaDataOp() || TableType.EXTERNAL_TABLE.equals(table.getTableType())) {
      if (ptnRootTask == null) {
        ptnRootTask = addPartTask;
      } else {
        ptnRootTask.addDependentTask(addPartTask);
      }
      return ptnRootTask;
    }

    AlterTableAddPartitionDesc.PartitionDesc partSpec = addPartitionDesc.getPartition(0);
    Path sourceWarehousePartitionLocation = new Path(partSpec.getLocation());
    Path replicaWarehousePartitionLocation = locationOnReplicaWarehouse(table, partSpec);
    partSpec.setLocation(replicaWarehousePartitionLocation.toString());
    LOG.debug("adding dependent CopyWork/AddPart/MoveWork for partition "
      + partSpecToString(partSpec.getPartSpec()) + " with source location: "
      + partSpec.getLocation());
    Task<?> ckptTask = ReplUtils.getTableCheckpointTask(
      tableDesc,
      (HashMap<String, String>)partSpec.getPartSpec(),
      context.dumpDirectory,
      context.hiveConf
    );

    Path stagingDir = replicaWarehousePartitionLocation;
    // if move optimization is enabled, copy the files directly to the target path. No need to create the staging dir.
    LoadFileType loadFileType;
    if (event.replicationSpec().isInReplicationScope() &&
      context.hiveConf.getBoolVar(REPL_ENABLE_MOVE_OPTIMIZATION)) {
      loadFileType = LoadFileType.IGNORE;
    } else {
      loadFileType = event.replicationSpec().isReplace() ? LoadFileType.REPLACE_ALL : LoadFileType.OVERWRITE_EXISTING;
      stagingDir = PathUtils.getExternalTmpPath(replicaWarehousePartitionLocation, context.pathInfo);
    }
    boolean copyAtLoad = context.hiveConf.getBoolVar(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET);
    Task<?> copyTask = ReplCopyTask.getLoadCopyTask(
      event.replicationSpec(),
      new Path(event.dataPath() + Path.SEPARATOR + getPartitionName(sourceWarehousePartitionLocation)),
      stagingDir,
      context.hiveConf, copyAtLoad, false
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

  private String getPartitionName(Path partitionMetadataFullPath) {
    //Get partition name by removing the metadata base path.
    //Needed for getting the data path
    return partitionMetadataFullPath.toString().substring(event.metadataPath().toString().length());
  }

  /**
   * This will create the move of partition data from temp path to actual path
   */
  private Task<?> movePartitionTask(Table table, AlterTableAddPartitionDesc.PartitionDesc partSpec, Path tmpPath,
                                    LoadFileType loadFileType) {
    MoveWork moveWork = new MoveWork(new HashSet<>(), new HashSet<>(), null, null, false);
    if (AcidUtils.isTransactionalTable(table)) {
      LoadMultiFilesDesc loadFilesWork = new LoadMultiFilesDesc(
        Collections.singletonList(tmpPath),
        Collections.singletonList(new Path(partSpec.getLocation())),
        true, null, null);
      moveWork.setMultiFilesDesc(loadFilesWork);
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
      AlterTableDropPartitionDesc dropPtnDesc = new AlterTableDropPartitionDesc(HiveTableName.of(table),
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

