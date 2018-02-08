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
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.ReplCopyTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.repl.ReplStateLogWork;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.AddDependencyToLeaves;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.TableEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.ReplicationState;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.TaskTracker;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.util.Context;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.util.PathUtils;
import org.apache.hadoop.hive.ql.exec.util.DAGTraversal;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ImportSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.ImportTableDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc.LoadFileType;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.datanucleus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
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
      return context.warehouse.getDefaultTablePath(parentDb, tableDesc.getTableName()).toString();
    } else {
      Path tablePath = new Path(
          context.warehouse.getDefaultDatabasePath(tableDesc.getDatabaseName()),
          MetaStoreUtils.encodeTableName(tableDesc.getTableName().toLowerCase())
      );
      return context.warehouse.getDnsPath(tablePath).toString();
    }
  }

  private void createTableReplLogTask() throws SemanticException {
    ReplStateLogWork replLogWork = new ReplStateLogWork(replLogger,
                                            tableDesc.getTableName(), tableDesc.tableType());
    Task<ReplStateLogWork> replLogTask = TaskFactory.get(replLogWork, context.hiveConf, true);

    if (tracker.tasks().isEmpty()) {
      tracker.addTask(replLogTask);
    } else {
      DAGTraversal.traverse(tracker.tasks(), new AddDependencyToLeaves(replLogTask));

      List<Task<? extends Serializable>> visited = new ArrayList<>();
      tracker.updateTaskCount(replLogTask, visited);
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

        table = new Table(tableDesc.getDatabaseName(), tableDesc.getTableName());
        if (isPartitioned(tableDesc)) {
          updateReplicationState(initialReplicationState());
          if (!forNewTable().hasReplicationState()) {
            // Add ReplStateLogTask only if no pending table load tasks left for next cycle
            createTableReplLogTask();
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
              createTableReplLogTask();
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
    Iterator<AddPartitionDesc> iterator = event.partitionDescriptions(tableDesc).iterator();
    while (iterator.hasNext() && tracker.canAddMoreTasks()) {
      AddPartitionDesc currentPartitionDesc = iterator.next();
      /*
       the currentPartitionDesc cannot be inlined as we need the hasNext() to be evaluated post the
       current retrieved lastReplicatedPartition
      */
      addPartition(iterator.hasNext(), currentPartitionDesc);
    }
    return tracker;
  }

  private void addPartition(boolean hasMorePartitions, AddPartitionDesc addPartitionDesc) throws Exception {
    tracker.addTask(tasksForAddPartition(table, addPartitionDesc));
    if (hasMorePartitions && !tracker.canAddMoreTasks()) {
      ReplicationState currentReplicationState =
          new ReplicationState(new PartitionState(table.getTableName(), addPartitionDesc));
      updateReplicationState(currentReplicationState);
    }
  }

  /**
   * returns the root task for adding a partition
   */
  private Task<? extends Serializable> tasksForAddPartition(Table table,
      AddPartitionDesc addPartitionDesc) throws MetaException, IOException, HiveException {
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

    Task<?> addPartTask = TaskFactory.get(
        new DDLWork(new HashSet<>(), new HashSet<>(), addPartitionDesc),
        context.hiveConf,
        true
    );

    Task<?> movePartitionTask = movePartitionTask(table, partSpec, tmpPath);

    copyTask.addDependentTask(addPartTask);
    addPartTask.addDependentTask(movePartitionTask);
    return copyTask;
  }

  /**
   * This will create the move of partition data from temp path to actual path
   */
  private Task<?> movePartitionTask(Table table, AddPartitionDesc.OnePartitionDesc partSpec,
      Path tmpPath) {
    LoadTableDesc loadTableWork = new LoadTableDesc(
        tmpPath, Utilities.getTableDesc(table), partSpec.getPartSpec(),
        event.replicationSpec().isReplace() ? LoadFileType.REPLACE_ALL : LoadFileType.OVERWRITE_EXISTING,
        SessionState.get().getTxnMgr().getCurrentTxnId()
    );
    loadTableWork.setInheritTableSpecs(false);
    MoveWork work = new MoveWork(new HashSet<>(), new HashSet<>(), loadTableWork, null, false);
    return TaskFactory.get(work, context.hiveConf, true);
  }

  private Path locationOnReplicaWarehouse(Table table, AddPartitionDesc.OnePartitionDesc partSpec)
      throws MetaException, HiveException, IOException {
    String child = Warehouse.makePartPath(partSpec.getPartSpec());
    if (tableDesc.getLocation() == null) {
      if (table.getDataLocation() == null) {
        Database parentDb = context.hiveDb.getDatabase(tableDesc.getDatabaseName());
        return new Path(
            context.warehouse.getDefaultTablePath(parentDb, tableDesc.getTableName()), child);
      } else {
        return new Path(table.getDataLocation().toString(), child);
      }
    } else {
      return new Path(tableDesc.getLocation(), child);
    }
  }

  private Task<? extends Serializable> alterSinglePartition(AddPartitionDesc desc,
      ReplicationSpec replicationSpec, Partition ptn) {
    desc.setReplaceMode(true);
    if ((replicationSpec != null) && (replicationSpec.isInReplicationScope())) {
      desc.setReplicationSpec(replicationSpec);
    }
    desc.getPartition(0).setLocation(ptn.getLocation()); // use existing location
    return TaskFactory.get(
        new DDLWork(new HashSet<>(), new HashSet<>(), desc),
        context.hiveConf,
        true
    );
  }

  private TaskTracker forExistingTable(AddPartitionDesc lastPartitionReplicated) throws Exception {
    boolean encounteredTheLastReplicatedPartition = (lastPartitionReplicated == null);
    Map<String, String> lastReplicatedPartSpec = null;
    if (!encounteredTheLastReplicatedPartition) {
      lastReplicatedPartSpec = lastPartitionReplicated.getPartition(0).getPartSpec();
      LOG.info("Start processing from partition info spec : {}",
          StringUtils.mapToString(lastReplicatedPartSpec));
    }

    ReplicationSpec replicationSpec = event.replicationSpec();
    Iterator<AddPartitionDesc> partitionIterator = event.partitionDescriptions(tableDesc).iterator();
    while (!encounteredTheLastReplicatedPartition && partitionIterator.hasNext()) {
      AddPartitionDesc addPartitionDesc = partitionIterator.next();
      Map<String, String> currentSpec = addPartitionDesc.getPartition(0).getPartSpec();
      encounteredTheLastReplicatedPartition = lastReplicatedPartSpec.equals(currentSpec);
    }

    while (partitionIterator.hasNext() && tracker.canAddMoreTasks()) {
      AddPartitionDesc addPartitionDesc = partitionIterator.next();
      Map<String, String> partSpec = addPartitionDesc.getPartition(0).getPartSpec();
      Partition ptn = context.hiveDb.getPartition(table, partSpec, false);
      if (ptn == null) {
        if (!replicationSpec.isMetadataOnly()) {
          addPartition(partitionIterator.hasNext(), addPartitionDesc);
        }
      } else {
        // If replicating, then the partition already existing means we need to replace, maybe, if
        // the destination ptn's repl.last.id is older than the replacement's.
        if (replicationSpec.allowReplacementInto(ptn.getParameters())) {
          if (replicationSpec.isMetadataOnly()) {
            tracker.addTask(alterSinglePartition(addPartitionDesc, replicationSpec, ptn));
            if (!tracker.canAddMoreTasks()) {
              tracker.setReplicationState(
                  new ReplicationState(
                      new PartitionState(table.getTableName(), addPartitionDesc)
                  )
              );
            }
          } else {
            addPartition(partitionIterator.hasNext(), addPartitionDesc);
          }
        } else {
          // ignore this ptn, do nothing, not an error.
        }
      }
    }
    return tracker;
  }
}

