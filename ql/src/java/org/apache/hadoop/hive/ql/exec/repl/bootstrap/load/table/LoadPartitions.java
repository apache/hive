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
import org.apache.hadoop.hive.common.repl.ReplConst;
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
import org.apache.hadoop.hive.ql.exec.repl.ReplExternalTables;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.ReplLoadOpType;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.TableEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.ReplicationState;
import org.apache.hadoop.hive.ql.exec.repl.util.TaskTracker;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.util.Context;
import org.apache.hadoop.hive.ql.metadata.Hive;
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
import org.datanucleus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_DUMP_SKIP_IMMUTABLE_DATA_COPY;
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
  private final AlterTableAddPartitionDesc.PartitionDesc lastReplicatedPartitionDesc;
  private final PartitionState.Stage lastReplicatedStage;
  private final ReplicationMetricCollector metricCollector;

  private final ImportTableDesc tableDesc;
  private final List<String> tablesToBootstrap;
  private Table table;

  public LoadPartitions(Context context, ReplLogger replLogger, TaskTracker tableTracker, TableEvent event, String dbNameToLoadIn,
      TableContext tableContext, ReplicationMetricCollector metricCollector, List<String> tablesToBootstrap) throws HiveException {
    this(context, replLogger, tableContext, tableTracker, event, dbNameToLoadIn, null, metricCollector, null,
        PartitionState.Stage.PARTITION, tablesToBootstrap);
  }

  public LoadPartitions(Context context, ReplLogger replLogger, TableContext tableContext, TaskTracker limiter,
      TableEvent event, String dbNameToLoadIn, AlterTableAddPartitionDesc lastReplicatedPartition,
      ReplicationMetricCollector metricCollector, AlterTableAddPartitionDesc.PartitionDesc lastReplicatedPartitionDesc,
      PartitionState.Stage lastReplicatedStage, List<String> tablesToBootstrap) throws HiveException {
    this.tracker = new TaskTracker(limiter);
    this.event = event;
    this.context = context;
    this.replLogger = replLogger;
    this.lastReplicatedPartition = lastReplicatedPartition;
    this.tableContext = tableContext;
    this.tableDesc = event.tableDesc(dbNameToLoadIn);
    this.table = ImportSemanticAnalyzer.tableIfExists(tableDesc, context.hiveDb);
    this.metricCollector = metricCollector;
    this.lastReplicatedPartitionDesc = lastReplicatedPartitionDesc;
    this.lastReplicatedStage = lastReplicatedStage;
    this.tablesToBootstrap = tablesToBootstrap;
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
          Task<?> replLogTask
                  = ReplUtils.getTableReplLogTask(tableDesc, replLogger, context.hiveConf, metricCollector,
                  (new Path(context.dumpDirectory)).getParent().toString());
          tracker.addDependentTask(replLogTask);
        }
        return tracker;
      }
    } else {
      // existing
      if (table.isPartitioned()) {
        if (tablesToBootstrap.stream().anyMatch(table.getTableName()::equalsIgnoreCase)) {
          Hive hiveDb = Hive.get(context.hiveConf);
          // Collect the non-existing partitions to drop.
          List<Partition> partitions = hiveDb.getPartitions(table);
          List<String> newParts = event.partitions(tableDesc);
          for (Partition part : partitions) {
            if (!newParts.contains(part.getName())) {
              hiveDb.dropPartition(table.getDbName(), table.getTableName(), part.getValues(), true);
            }
          }
        }
        List<AlterTableAddPartitionDesc> partitionDescs = event.partitionDescriptions(tableDesc);
        if (!event.replicationSpec().isMetadataOnly() && !partitionDescs.isEmpty()) {
          updateReplicationState(initialReplicationState());
          if (!forExistingTable(lastReplicatedPartition).hasReplicationState()) {
            // Add ReplStateLogTask only if no pending table load tasks left for next cycle
            Task<?> replLogTask
                    = ReplUtils.getTableReplLogTask(tableDesc, replLogger, context.hiveConf, metricCollector,
                    (new Path(context.dumpDirectory)).getParent().toString());
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
   * @param lastAlterTableAddPartitionDesc
   */
  private void addConsolidatedPartitionDesc(AlterTableAddPartitionDesc lastAlterTableAddPartitionDesc) throws Exception {
    int maxTasks = 0;
    //Load partitions equal to batch size at one go for metadata only and for external tables.
    if (isMetaDataOp() || TableType.EXTERNAL_TABLE.equals(table.getTableType())) {
      maxTasks = context.hiveConf.getIntVar(HiveConf.ConfVars.REPL_LOAD_PARTITIONS_BATCH_SIZE);
    } else {
      maxTasks = context.hiveConf.getIntVar(HiveConf.ConfVars.REPL_LOAD_PARTITIONS_WITH_DATA_COPY_BATCH_SIZE);
    }
    int currentPartitionCount = 0;
    Iterator<AlterTableAddPartitionDesc> partitionIterator = event.partitionDescriptions(tableDesc).iterator();
    //If already a set of partitions are processed as part of previous run, we skip those
    if (lastAlterTableAddPartitionDesc != null) {
      while (partitionIterator.hasNext()) {
        currentPartitionCount++;
        AlterTableAddPartitionDesc addPartitionDesc = partitionIterator.next();
        if (lastAlterTableAddPartitionDesc.getPartitions().get(0).getPartSpec()
          .equals(addPartitionDesc.getPartitions().get(0).getPartSpec())) {
          break;
        }
      }
    }
    List<AlterTableAddPartitionDesc> partitionDescs = event.partitionDescriptions(tableDesc);
    int totalPartitionCount = partitionDescs.size();
    while (currentPartitionCount < totalPartitionCount) {
      List<AlterTableAddPartitionDesc.PartitionDesc> partitions = new LinkedList<>();
      int pendingPartitionCount = totalPartitionCount - currentPartitionCount;
      int toPartitionCount = currentPartitionCount + Math.min(pendingPartitionCount, maxTasks);
      List<AlterTableAddPartitionDesc> partitionBatch = partitionDescs.subList(currentPartitionCount,
        toPartitionCount);
      for (AlterTableAddPartitionDesc addPartitionDesc : partitionBatch) {
        AlterTableAddPartitionDesc.PartitionDesc src = addPartitionDesc.getPartitions().get(0);
        Map<String, String> partParams = src.getPartParams();
        if (partParams == null) {
          partParams = new HashMap<>();
        }
        partParams.put(ReplConst.REPL_TARGET_DB_PROPERTY, context.dumpDirectory);
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
      addPartition((toPartitionCount < totalPartitionCount), consolidatedPartitionDesc);
      if (!tracker.canAddMoreTasks()) {
        //No need to do processing as no more tasks can be added. Will be processed in next run. State is already
        //updated in add partition task
        return;
      }
      currentPartitionCount = toPartitionCount;
    }
  }

  private TaskTracker forNewTable() throws Exception {
    // Place all partitions in single task to reduce load on HMS.
    addConsolidatedPartitionDesc(null);
    return tracker;
  }

  private void addPartition(boolean hasMorePartitions, AlterTableAddPartitionDesc addPartitionDesc)
          throws Exception {
    boolean processingComplete = addTasksForPartition(table, addPartitionDesc, null);
    //If processing is not complete, means replication state is already updated with copy tasks which need
    //to be processed
    if (processingComplete && hasMorePartitions && !tracker.canAddMoreTasks()) {
      ReplicationState currentReplicationState =
          new ReplicationState(new PartitionState(table.getTableName(), addPartitionDesc));
      updateReplicationState(currentReplicationState);
    }
  }

  /**
   * returns the root task for adding all partitions in a batch
   */
  private boolean addTasksForPartition(Table table, AlterTableAddPartitionDesc addPartitionDesc,
                                    AlterTableAddPartitionDesc.PartitionDesc lastPartSpec)
          throws MetaException, HiveException {
    Task<?> addPartTask = TaskFactory.get(
      new DDLWork(new HashSet<>(), new HashSet<>(), addPartitionDesc,
              true, (new Path(context.dumpDirectory)).getParent().toString(), this.metricCollector),
      context.hiveConf
    );
    //checkpointing task already added as part of add batch of partition
    if (isMetaDataOp() || TableType.EXTERNAL_TABLE.equals(table.getTableType())) {
      tracker.addTask(addPartTask);
      return true;
    }
    //Add Copy task for all partitions
    boolean lastProcessedStageFound = false;
    for (AlterTableAddPartitionDesc.PartitionDesc partSpec : addPartitionDesc.getPartitions()) {
      if (!tracker.canAddMoreTasks()) {
        //update replication state with the copy task added with which it needs to proceed next
        ReplicationState currentReplicationState =
          new ReplicationState(new PartitionState(table.getTableName(), addPartitionDesc,
            partSpec, PartitionState.Stage.COPY));
        updateReplicationState(currentReplicationState);
        return false;
      }
      Path replicaWarehousePartitionLocation = locationOnReplicaWarehouse(table, partSpec);
      partSpec.setLocation(replicaWarehousePartitionLocation.toString());
      LOG.debug("adding dependent CopyWork for partition "
        + partSpecToString(partSpec.getPartSpec()) + " with source location: "
        + partSpec.getLocation());
      if (!lastProcessedStageFound && lastPartSpec != null &&
        lastPartSpec.getLocation() != partSpec.getLocation()) {
        //Don't process copy task if already processed as part of previous run
        continue;
      }
      lastProcessedStageFound = true;
      boolean copyAtLoad = context.hiveConf.getBoolVar(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET);
      Task<?> copyTask = ReplCopyTask.getLoadCopyTask(
        event.replicationSpec(),
        new Path(event.dataPath() + Path.SEPARATOR + Warehouse.makePartPath(partSpec.getPartSpec())),
        replicaWarehousePartitionLocation,
        context.hiveConf, copyAtLoad, false, (new Path(context.dumpDirectory)).getParent().toString(),
        this.metricCollector
      );
      tracker.addTask(copyTask);
    }
    //add partition metadata task once all the copy tasks are added
    tracker.addDependentTask(addPartTask);
    return true;
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
              new DDLWork(new HashSet<>(), new HashSet<>(), dropPtnDesc, true,
                      (new Path(context.dumpDirectory)).getParent().toString(), this.metricCollector), context.hiveConf
      );
    }
    return dropPtnTask;
  }

  private TaskTracker forExistingTable(AlterTableAddPartitionDesc lastPartitionReplicated) throws Exception {
    boolean encounteredTheLastReplicatedPartition = (lastPartitionReplicated == null);
    Map<String, String> lastReplicatedPartSpec = null;
    if (!encounteredTheLastReplicatedPartition) {
      lastReplicatedPartSpec = lastPartitionReplicated.getPartitions().get(0).getPartSpec();
      LOG.info("Start processing from partition info spec : {}",
          StringUtils.mapToString(lastReplicatedPartSpec));
    }

    Iterator<AlterTableAddPartitionDesc> partitionIterator = event.partitionDescriptions(tableDesc).iterator();
    while (!encounteredTheLastReplicatedPartition && partitionIterator.hasNext()) {
      AlterTableAddPartitionDesc addPartitionDesc = partitionIterator.next();
      Map<String, String> currentSpec = addPartitionDesc.getPartitions().get(0).getPartSpec();
      encounteredTheLastReplicatedPartition = lastReplicatedPartSpec.equals(currentSpec);
    }
    //Add Copy task pending for previous partition
    if (PartitionState.Stage.COPY.equals(lastReplicatedStage)) {
      addTasksForPartition(table, lastPartitionReplicated,
        lastReplicatedPartitionDesc);
    }
    boolean pendingPartitions = false;
    while (partitionIterator.hasNext() && tracker.canAddMoreTasks()) {
      pendingPartitions = true;
      AlterTableAddPartitionDesc addPartitionDesc = partitionIterator.next();
      AlterTableAddPartitionDesc.PartitionDesc src = addPartitionDesc.getPartitions().get(0);
      //Add check point task as part of add partition
      Map<String, String> partParams = new HashMap<>();
      partParams.put(ReplConst.REPL_TARGET_DB_PROPERTY, context.dumpDirectory);
      Path replicaWarehousePartitionLocation = locationOnReplicaWarehouse(table, src);
      src.setLocation(replicaWarehousePartitionLocation.toString());
      src.addPartParams(partParams);
      Map<String, String> partSpec = src.getPartSpec();

      ReplLoadOpType loadPtnType = getLoadPartitionType(partSpec);
      switch (loadPtnType) {
        case LOAD_NEW:
          break;
        case LOAD_REPLACE:
          tracker.addDependentTask(dropPartitionTask(table, partSpec));
          break;
        case LOAD_SKIP:
          continue;
        default:
          break;
      }
    }
    if (pendingPartitions) {
      addConsolidatedPartitionDesc(lastPartitionReplicated);
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

