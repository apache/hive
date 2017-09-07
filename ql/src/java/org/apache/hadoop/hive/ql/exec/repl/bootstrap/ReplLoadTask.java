/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.repl.bootstrap;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.ReplStateLogWork;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.BootstrapEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.ConstraintEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.DatabaseEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.FunctionEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.PartitionEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.TableEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.filesystem.BootstrapEventsIterator;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.filesystem.ConstraintEventsIterator;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.LoadConstraint;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.LoadDatabase;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.LoadFunction;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.TaskTracker;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.table.LoadPartitions;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.table.LoadTable;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.table.TableContext;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.util.Context;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.plan.ImportTableDesc;
import org.apache.hadoop.hive.ql.plan.api.StageType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.LoadDatabase.AlterDatabase;

public class ReplLoadTask extends Task<ReplLoadWork> implements Serializable {
  private final static int ZERO_TASKS = 0;

  @Override
  public String getName() {
    return "REPL_BOOTSTRAP_LOAD";
  }

  /**
   * Provides the root Tasks created as a result of this loadTask run which will be executed
   * by the driver. It does not track details across multiple runs of LoadTask.
   */
  private static class Scope {
    boolean database = false, table = false, partition = false;
    List<Task<? extends Serializable>> rootTasks = new ArrayList<>();
  }

  @Override
  protected int execute(DriverContext driverContext) {
    try {
      int maxTasks = conf.getIntVar(HiveConf.ConfVars.REPL_APPROX_MAX_LOAD_TASKS);
      Context context = new Context(conf, getHive());
      TaskTracker loadTaskTracker = new TaskTracker(maxTasks);
      /*
          for now for simplicity we are doing just one directory ( one database ), come back to use
          of multiple databases once we have the basic flow to chain creating of tasks in place for
          a database ( directory )
      */
      BootstrapEventsIterator iterator = work.iterator();
      ConstraintEventsIterator constraintIterator = work.constraintIterator();
      /*
      This is used to get hold of a reference during the current creation of tasks and is initialized
      with "0" tasks such that it will be non consequential in any operations done with task tracker
      compositions.
       */
      TaskTracker dbTracker = new TaskTracker(ZERO_TASKS);
      TaskTracker tableTracker = new TaskTracker(ZERO_TASKS);
      Scope scope = new Scope();
      boolean loadingConstraint = false;
      if (!iterator.hasNext() && constraintIterator.hasNext()) {
        loadingConstraint = true;
      }
      while ((iterator.hasNext() || (loadingConstraint && constraintIterator.hasNext())) && loadTaskTracker.canAddMoreTasks()) {
        BootstrapEvent next;
        if (!loadingConstraint) {
          next = iterator.next();
        } else {
          next = constraintIterator.next();
        }
        switch (next.eventType()) {
        case Database:
          DatabaseEvent dbEvent = (DatabaseEvent) next;
          dbTracker =
              new LoadDatabase(context, dbEvent, work.dbNameToLoadIn, loadTaskTracker)
                  .tasks();
          loadTaskTracker.update(dbTracker);
          if (work.hasDbState()) {
            loadTaskTracker.update(updateDatabaseLastReplID(maxTasks, context, scope));
          }
          work.updateDbEventState(dbEvent.toState());
          scope.database = true;
          scope.rootTasks.addAll(dbTracker.tasks());
          dbTracker.debugLog("database");
          break;
        case Table: {
          /*
              Implicit assumption here is that database level is processed first before table level,
              which will depend on the iterator used since it should provide the higher level directory
              listing before providing the lower level listing. This is also required such that
              the dbTracker /  tableTracker are setup correctly always.
           */
          TableContext tableContext =
              new TableContext(dbTracker, work.dbNameToLoadIn, work.tableNameToLoadIn);
          TableEvent tableEvent = (TableEvent) next;
          LoadTable loadTable = new LoadTable(tableEvent, context, iterator.replLogger(),
                                              tableContext, loadTaskTracker);
          tableTracker = loadTable.tasks();
          if (!scope.database) {
            scope.rootTasks.addAll(tableTracker.tasks());
            scope.table = true;
          }
          setUpDependencies(dbTracker, tableTracker);
          /*
            for table replication if we reach the max number of tasks then for the next run we will
            try to reload the same table again, this is mainly for ease of understanding the code
            as then we can avoid handling == > loading partitions for the table given that
            the creation of table lead to reaching max tasks vs,  loading next table since current
            one does not have partitions.
           */

          // for a table we explicitly try to load partitions as there is no separate partitions events.
          LoadPartitions loadPartitions =
              new LoadPartitions(context, iterator.replLogger(), loadTaskTracker, tableEvent,
                      work.dbNameToLoadIn, tableContext);
          TaskTracker partitionsTracker = loadPartitions.tasks();
          partitionsPostProcessing(iterator, scope, loadTaskTracker, tableTracker,
              partitionsTracker);
          tableTracker.debugLog("table");
          partitionsTracker.debugLog("partitions for table");
          break;
        }
        case Partition: {
          /*
              This will happen only when loading tables and we reach the limit of number of tasks we can create;
              hence we know here that the table should exist and there should be a lastPartitionName
          */
          PartitionEvent event = (PartitionEvent) next;
          TableContext tableContext = new TableContext(dbTracker, work.dbNameToLoadIn,
              work.tableNameToLoadIn);
          LoadPartitions loadPartitions =
              new LoadPartitions(context, iterator.replLogger(), tableContext, loadTaskTracker,
                      event.asTableEvent(), work.dbNameToLoadIn, event.lastPartitionReplicated());
          /*
               the tableTracker here should be a new instance and not an existing one as this can
               only happen when we break in between loading partitions.
           */
          TaskTracker partitionsTracker = loadPartitions.tasks();
          partitionsPostProcessing(iterator, scope, loadTaskTracker, tableTracker,
              partitionsTracker);
          partitionsTracker.debugLog("partitions");
          break;
        }
        case Function: {
          LoadFunction loadFunction = new LoadFunction(context, iterator.replLogger(),
                                              (FunctionEvent) next, work.dbNameToLoadIn, dbTracker);
          TaskTracker functionsTracker = loadFunction.tasks();
          if (!scope.database) {
            scope.rootTasks.addAll(functionsTracker.tasks());
          } else {
            setUpDependencies(dbTracker, functionsTracker);
          }
          loadTaskTracker.update(functionsTracker);
          functionsTracker.debugLog("functions");
          break;
        }
        case Constraint: {
          LoadConstraint loadConstraint =
              new LoadConstraint(context, (ConstraintEvent) next, work.dbNameToLoadIn, dbTracker);
          TaskTracker constraintTracker = loadConstraint.tasks();
          scope.rootTasks.addAll(constraintTracker.tasks());
          loadTaskTracker.update(constraintTracker);
          constraintTracker.debugLog("constraints");
        }
        }

        if (!loadingConstraint && !iterator.currentDbHasNext()) {
          createEndReplLogTask(context, scope, iterator.replLogger());
        }
      }
      boolean addAnotherLoadTask = iterator.hasNext() || loadTaskTracker.hasReplicationState()
          || constraintIterator.hasNext();
      createBuilderTask(scope.rootTasks, addAnotherLoadTask);
      if (!iterator.hasNext() && !constraintIterator.hasNext()) {
        loadTaskTracker.update(updateDatabaseLastReplID(maxTasks, context, scope));
        work.updateDbEventState(null);
      }
      this.childTasks = scope.rootTasks;
      LOG.info("Root Tasks / Total Tasks : {} / {} ", childTasks.size(), loadTaskTracker.numberOfTasks());
    } catch (Exception e) {
      LOG.error("failed replication", e);
      setException(e);
      return 1;
    }
    LOG.info("completed load task run : {}", work.executedLoadTask());
    return 0;
  }

  private Task<? extends Serializable> createEndReplLogTask(Context context, Scope scope,
                                                  ReplLogger replLogger) throws SemanticException {
    Database dbInMetadata = work.databaseEvent(context.hiveConf).dbInMetadata(work.dbNameToLoadIn);
    ReplStateLogWork replLogWork = new ReplStateLogWork(replLogger, dbInMetadata.getParameters());
    Task<ReplStateLogWork> replLogTask = TaskFactory.get(replLogWork, conf);
    if (null == scope.rootTasks) {
      scope.rootTasks.add(replLogTask);
    } else {
      dependency(scope.rootTasks, replLogTask);
    }
    return replLogTask;
  }

  /**
   * There was a database update done before and we want to make sure we update the last repl
   * id on this database as we are now going to switch to processing a new database.
   */
  private TaskTracker updateDatabaseLastReplID(int maxTasks, Context context, Scope scope)
      throws SemanticException {
  /*
    we don't want to put any limits on this task as this is essential before we start
    processing new database events.
   */
    TaskTracker taskTracker =
        new AlterDatabase(context, work.databaseEvent(context.hiveConf), work.dbNameToLoadIn,
            new TaskTracker(maxTasks)).tasks();
    scope.rootTasks.addAll(taskTracker.tasks());
    return taskTracker;
  }

  private void partitionsPostProcessing(BootstrapEventsIterator iterator,
      Scope scope, TaskTracker loadTaskTracker, TaskTracker tableTracker,
      TaskTracker partitionsTracker) throws SemanticException {
    setUpDependencies(tableTracker, partitionsTracker);
    if (!scope.database && !scope.table) {
      scope.rootTasks.addAll(partitionsTracker.tasks());
      scope.partition = true;
    }
    loadTaskTracker.update(tableTracker);
    loadTaskTracker.update(partitionsTracker);
    if (partitionsTracker.hasReplicationState()) {
      iterator.setReplicationState(partitionsTracker.replicationState());
    }
  }

  /*
      This sets up dependencies such that a child task is dependant on the parent to be complete.
   */
  private void setUpDependencies(TaskTracker parentTasks, TaskTracker childTasks) {
    for (Task<? extends Serializable> parentTask : parentTasks.tasks()) {
      for (Task<? extends Serializable> childTask : childTasks.tasks()) {
        parentTask.addDependentTask(childTask);
      }
    }
  }

  private void createBuilderTask(List<Task<? extends Serializable>> rootTasks,
      boolean shouldCreateAnotherLoadTask) {
  /*
    use loadTask as dependencyCollection
   */
    if (shouldCreateAnotherLoadTask) {
      Task<ReplLoadWork> loadTask = TaskFactory.get(work, conf);
      dependency(rootTasks, loadTask);
    }
  }

  /**
   * add the dependency to the leaf node
   */
  public static boolean dependency(List<Task<? extends Serializable>> tasks, Task<?> tailTask) {
    if (tasks == null || tasks.isEmpty()) {
      return true;
    }
    for (Task<? extends Serializable> task : tasks) {
      if (task == tailTask) {
        continue;
      }
      boolean leafNode = dependency(task.getChildTasks(), tailTask);
      if (leafNode) {
        task.addDependentTask(tailTask);
      }
    }
    return false;
  }

  @Override
  public StageType getType() {
    return StageType.REPL_BOOTSTRAP_LOAD;
  }
}
