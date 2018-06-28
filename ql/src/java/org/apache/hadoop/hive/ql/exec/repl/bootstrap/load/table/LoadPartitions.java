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
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.ReplCopyTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
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
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DropTableDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ImportTableDesc;
import org.apache.hadoop.hive.ql.plan.LoadMultiFilesDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc.LoadFileType;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.datanucleus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
  private final AddPartitionDesc lastReplicatedPartition;

  private final ImportTableDesc tableDesc;
  private Table table;

  public LoadPartitions(Context context, ReplLogger replLogger, TaskTracker tableTracker,
                        TableEvent event, String dbNameToLoadIn,
                        TableContext tableContext) throws HiveException, IOException {
    this(context, replLogger, tableContext, tableTracker, event, dbNameToLoadIn, null);
  }

  public LoadPartitions(Context context, ReplLogger replLogger, TableContext tableContext,
                        TaskTracker limiter, TableEvent event, String dbNameToLoadIn,
                        AddPartitionDesc lastReplicatedPartition) throws HiveException, IOException {
    this.tracker = new TaskTracker(limiter);
    this.event = event;
    this.context = context;
    this.replLogger = replLogger;
    this.lastReplicatedPartition = lastReplicatedPartition;
    this.tableContext = tableContext;

    this.tableDesc = tableContext.overrideProperties(event.tableDesc(dbNameToLoadIn));
    this.table = ImportSemanticAnalyzer.tableIfExists(tableDesc, context.hiveDb);
  }

  private String location() throws MetaException, HiveException {
    Database parentDb = context.hiveDb.getDatabase(tableDesc.getDatabaseName());
    if (!tableContext.waitOnPrecursor()) {
      return context.warehouse.getDefaultTablePath(
          parentDb, tableDesc.getTableName(), tableDesc.isExternal()).toString();
    } else {
      Path tablePath = context.warehouse.getDefaultTablePath(
          tableDesc.getDatabaseName(), tableDesc.getTableName(), tableDesc.isExternal());
      return context.warehouse.getDnsPath(tablePath).toString();
    }
  }

  public TaskTracker tasks() throws SemanticException {
    try {
      /*
      We are doing this both in load table and load partitions
       */
      if (tableDesc.getLocation() == null) {
        tableDesc.setLocation(location());
      }

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
          List<AddPartitionDesc> partitionDescs = event.partitionDescriptions(tableDesc);
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
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  private void updateReplicationState(ReplicationState replicationState) throws SemanticException {
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
    Database parentDb = context.hiveDb.getDatabase(tableDesc.getDatabaseName());
    // If table doesn't exist, allow creating a new one only if the database state is older than the update.
    // This in-turn applicable for partitions creation as well.
    if ((parentDb != null) && (!event.replicationSpec().allowReplacementInto(parentDb.getParameters()))) {
      return tracker;
    }

    Iterator<AddPartitionDesc> iterator = event.partitionDescriptions(tableDesc).iterator();
    while (iterator.hasNext() && tracker.canAddMoreTasks()) {
      AddPartitionDesc currentPartitionDesc = iterator.next();
      /*
       the currentPartitionDesc cannot be inlined as we need the hasNext() to be evaluated post the
       current retrieved lastReplicatedPartition
      */
      addPartition(iterator.hasNext(), currentPartitionDesc, null);
    }
    return tracker;
  }

  private void addPartition(boolean hasMorePartitions, AddPartitionDesc addPartitionDesc, Task<?> ptnRootTask)
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
  private Task<?> tasksForAddPartition(Table table, AddPartitionDesc addPartitionDesc, Task<?> ptnRootTask)
          throws MetaException, IOException, HiveException {
    Task<?> addPartTask = TaskFactory.get(
            new DDLWork(new HashSet<>(), new HashSet<>(), addPartitionDesc),
            context.hiveConf
    );
    if (event.replicationSpec().isMetadataOnly()) {
      if (ptnRootTask == null) {
        ptnRootTask = addPartTask;
      } else {
        ptnRootTask.addDependentTask(addPartTask);
      }
      return ptnRootTask;
    }

    AddPartitionDesc.OnePartitionDesc partSpec = addPartitionDesc.getPartition(0);
    Path sourceWarehousePartitionLocation = new Path(partSpec.getLocation());
    Path replicaWarehousePartitionLocation = locationOnReplicaWarehouse(table, partSpec);
    partSpec.setLocation(replicaWarehousePartitionLocation.toString());
    LOG.debug("adding dependent CopyWork/AddPart/MoveWork for partition "
            + partSpecToString(partSpec.getPartSpec()) + " with source location: "
            + partSpec.getLocation());

    Path tmpPath = PathUtils.getExternalTmpPath(replicaWarehousePartitionLocation, context.pathInfo);
    Task<?> copyTask = ReplCopyTask.getLoadCopyTask(
        event.replicationSpec(),
        sourceWarehousePartitionLocation,
        tmpPath,
        context.hiveConf
    );
    Task<?> movePartitionTask = movePartitionTask(table, partSpec, tmpPath);

    // Set Checkpoint task as dependant to add partition tasks. So, if same dump is retried for
    // bootstrap, we skip current partition update.
    Task<?> ckptTask = ReplUtils.getTableCheckpointTask(
            tableDesc,
            (HashMap<String, String>)partSpec.getPartSpec(),
            context.dumpDirectory,
            context.hiveConf
    );

    if (ptnRootTask == null) {
      ptnRootTask = copyTask;
    } else {
      ptnRootTask.addDependentTask(copyTask);
    }
    copyTask.addDependentTask(addPartTask);
    addPartTask.addDependentTask(movePartitionTask);
    movePartitionTask.addDependentTask(ckptTask);

    return ptnRootTask;
  }

  /**
   * This will create the move of partition data from temp path to actual path
   */
  private Task<?> movePartitionTask(Table table, AddPartitionDesc.OnePartitionDesc partSpec, Path tmpPath) {
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
              event.replicationSpec().isReplace() ? LoadFileType.REPLACE_ALL : LoadFileType.OVERWRITE_EXISTING, 0L
      );
      loadTableWork.setInheritTableSpecs(false);
      moveWork.setLoadTableWork(loadTableWork);
    }

    return TaskFactory.get(moveWork, context.hiveConf);
  }

  private Path locationOnReplicaWarehouse(Table table, AddPartitionDesc.OnePartitionDesc partSpec)
      throws MetaException, HiveException, IOException {
    String child = Warehouse.makePartPath(partSpec.getPartSpec());
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
      DropTableDesc dropPtnDesc = new DropTableDesc(table.getFullyQualifiedName(),
              partSpecsExpr, null, true, event.replicationSpec());
      dropPtnTask = TaskFactory.get(
              new DDLWork(new HashSet<>(), new HashSet<>(), dropPtnDesc), context.hiveConf
      );
    }
    return dropPtnTask;
  }

  private TaskTracker forExistingTable(AddPartitionDesc lastPartitionReplicated) throws Exception {
    boolean encounteredTheLastReplicatedPartition = (lastPartitionReplicated == null);
    Map<String, String> lastReplicatedPartSpec = null;
    if (!encounteredTheLastReplicatedPartition) {
      lastReplicatedPartSpec = lastPartitionReplicated.getPartition(0).getPartSpec();
      LOG.info("Start processing from partition info spec : {}",
          StringUtils.mapToString(lastReplicatedPartSpec));
    }

    Iterator<AddPartitionDesc> partitionIterator = event.partitionDescriptions(tableDesc).iterator();
    while (!encounteredTheLastReplicatedPartition && partitionIterator.hasNext()) {
      AddPartitionDesc addPartitionDesc = partitionIterator.next();
      Map<String, String> currentSpec = addPartitionDesc.getPartition(0).getPartSpec();
      encounteredTheLastReplicatedPartition = lastReplicatedPartSpec.equals(currentSpec);
    }

    while (partitionIterator.hasNext() && tracker.canAddMoreTasks()) {
      AddPartitionDesc addPartitionDesc = partitionIterator.next();
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

