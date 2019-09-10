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
package org.apache.hadoop.hive.ql.exec.repl;

import com.google.common.collect.Collections2;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.repl.ReplScope;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.database.alter.poperties.AlterDatabaseSetPropertiesDesc;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.BootstrapEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.ConstraintEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.DatabaseEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.FunctionEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.PartitionEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.TableEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.filesystem.BootstrapEventsIterator;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.filesystem.ConstraintEventsIterator;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.filesystem.FSTableEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.LoadConstraint;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.LoadDatabase;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.LoadFunction;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.table.LoadPartitions;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.table.LoadTable;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.table.TableContext;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.util.Context;
import org.apache.hadoop.hive.ql.exec.repl.incremental.IncrementalLoadTasksBuilder;
import org.apache.hadoop.hive.ql.exec.repl.util.AddDependencyToLeaves;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.TaskTracker;
import org.apache.hadoop.hive.ql.exec.util.DAGTraversal;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.PathBuilder;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.plan.api.StageType;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.LoadDatabase.AlterDatabase;

public class ReplLoadTask extends Task<ReplLoadWork> implements Serializable {
  private final static int ZERO_TASKS = 0;

  @Override
  public String getName() {
    return (work.isIncrementalLoad() ? "REPL_INCREMENTAL_LOAD" : "REPL_BOOTSTRAP_LOAD");
  }

  @Override
  public StageType getType() {
    return work.isIncrementalLoad() ? StageType.REPL_INCREMENTAL_LOAD : StageType.REPL_BOOTSTRAP_LOAD;
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
  public int execute(DriverContext driverContext) {
    Task<? extends Serializable> rootTask = work.getRootTask();
    if (rootTask != null) {
      rootTask.setChildTasks(null);
    }
    work.setRootTask(this);
    this.parentTasks = null;
    if (work.isIncrementalLoad()) {
      return executeIncrementalLoad(driverContext);
    } else {
      return executeBootStrapLoad(driverContext);
    }
  }

  private int executeBootStrapLoad(DriverContext driverContext) {
    try {
      int maxTasks = conf.getIntVar(HiveConf.ConfVars.REPL_APPROX_MAX_LOAD_TASKS);
      Context context = new Context(work.dumpDirectory, conf, getHive(),
              work.sessionStateLineageState, driverContext.getCtx());
      TaskTracker loadTaskTracker = new TaskTracker(maxTasks);
      /*
          for now for simplicity we are doing just one directory ( one database ), come back to use
          of multiple databases once we have the basic flow to chain creating of tasks in place for
          a database ( directory )
      */
      BootstrapEventsIterator iterator = work.bootstrapIterator();
      ConstraintEventsIterator constraintIterator = work.constraintsIterator();
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
      while ((iterator.hasNext() || (loadingConstraint && constraintIterator.hasNext()) ||
              (work.getPathsToCopyIterator().hasNext())) && loadTaskTracker.canAddMoreTasks()) {
        // First start the distcp tasks to copy the files related to external table. The distcp tasks should be
        // started first to avoid ddl task trying to create table/partition directory. Distcp task creates these
        // directory with proper permission and owner.
        if (work.getPathsToCopyIterator().hasNext()) {
          scope.rootTasks.addAll(new ExternalTableCopyTaskBuilder(work, conf).tasks(loadTaskTracker));
          break;
        }

        BootstrapEvent next;
        if (!loadingConstraint) {
          next = iterator.next();
        } else {
          next = constraintIterator.next();
        }
        switch (next.eventType()) {
        case Database:
          DatabaseEvent dbEvent = (DatabaseEvent) next;
          dbTracker = new LoadDatabase(context, dbEvent, work.dbNameToLoadIn, loadTaskTracker).tasks();
          loadTaskTracker.update(dbTracker);
          if (work.hasDbState()) {
            loadTaskTracker.update(updateDatabaseLastReplID(maxTasks, context, scope));
          }  else {
            // Scope might have set to database in some previous iteration of loop, so reset it to false if database
            // tracker has no tasks.
            scope.database = false;
          }
          work.updateDbEventState(dbEvent.toState());
          if (dbTracker.hasTasks()) {
            scope.rootTasks.addAll(dbTracker.tasks());
            scope.database = true;
          }
          dbTracker.debugLog("database");
          break;
        case Table: {
          /*
              Implicit assumption here is that database level is processed first before table level,
              which will depend on the iterator used since it should provide the higher level directory
              listing before providing the lower level listing. This is also required such that
              the dbTracker /  tableTracker are setup correctly always.
           */
          TableContext tableContext = new TableContext(dbTracker, work.dbNameToLoadIn);
          TableEvent tableEvent = (TableEvent) next;
          LoadTable loadTable = new LoadTable(tableEvent, context, iterator.replLogger(),
                                              tableContext, loadTaskTracker);
          tableTracker = loadTable.tasks(work.isIncrementalLoad());
          setUpDependencies(dbTracker, tableTracker);
          if (!scope.database && tableTracker.hasTasks()) {
            scope.rootTasks.addAll(tableTracker.tasks());
            scope.table = true;
          } else {
            // Scope might have set to table in some previous iteration of loop, so reset it to false if table
            // tracker has no tasks.
            scope.table = false;
          }

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
          TableContext tableContext = new TableContext(dbTracker, work.dbNameToLoadIn);
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

      boolean addAnotherLoadTask = iterator.hasNext()
          || loadTaskTracker.hasReplicationState()
          || constraintIterator.hasNext()
          || work.getPathsToCopyIterator().hasNext();

      if (addAnotherLoadTask) {
        createBuilderTask(scope.rootTasks);
      }

      // Update last repl ID of the database only if the current dump is not incremental. If bootstrap
      // is combined with incremental dump, it contains only tables to bootstrap. So, needn't change
      // last repl ID of the database.
      if (!iterator.hasNext() && !constraintIterator.hasNext() && !work.getPathsToCopyIterator().hasNext()
              && !work.isIncrementalLoad()) {
        loadTaskTracker.update(updateDatabaseLastReplID(maxTasks, context, scope));
        work.updateDbEventState(null);
      }
      this.childTasks = scope.rootTasks;
      /*
      Since there can be multiple rounds of this run all of which will be tied to the same
      query id -- generated in compile phase , adding a additional UUID to the end to print each run
      in separate files.
       */
      LOG.info("Root Tasks / Total Tasks : {} / {} ", childTasks.size(), loadTaskTracker.numberOfTasks());

      // Populate the driver context with the scratch dir info from the repl context, so that the temp dirs will be cleaned up later
      driverContext.getCtx().getFsScratchDirs().putAll(context.pathInfo.getFsScratchDirs());
    }  catch (RuntimeException e) {
      LOG.error("replication failed with run time exception", e);
      throw e;
    } catch (Exception e) {
      LOG.error("replication failed", e);
      setException(e);
      return ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode();
    }
    LOG.info("completed load task run : {}", work.executedLoadTask());
    return 0;
  }

  /**
   * Cleanup/drop tables from the given database which are bootstrapped by input dump dir.
   * @throws HiveException Failed to drop the tables.
   * @throws IOException File operations failure.
   * @throws InvalidInputException Invalid input dump directory.
   */
  private void cleanTablesFromBootstrap() throws HiveException, IOException, InvalidInputException {
    Path bootstrapDirectory = new PathBuilder(work.bootstrapDumpToCleanTables)
            .addDescendant(ReplUtils.INC_BOOTSTRAP_ROOT_DIR_NAME).build();
    FileSystem fs = bootstrapDirectory.getFileSystem(conf);

    if (!fs.exists(bootstrapDirectory)) {
      throw new InvalidInputException("Input bootstrap dump directory specified to clean tables from is invalid: "
              + bootstrapDirectory);
    }

    FileStatus[] fileStatuses = fs.listStatus(bootstrapDirectory, EximUtil.getDirectoryFilter(fs));
    if ((fileStatuses == null) || (fileStatuses.length == 0)) {
      throw new InvalidInputException("Input bootstrap dump directory specified to clean tables from is empty: "
              + bootstrapDirectory);
    }

    if (StringUtils.isNotBlank(work.dbNameToLoadIn) && (fileStatuses.length > 1)) {
      throw new InvalidInputException("Input bootstrap dump directory specified to clean tables from has multiple"
              + " DB dirs in the dump: " + bootstrapDirectory
              + " which is not allowed on single target DB: " + work.dbNameToLoadIn);
    }

    // Iterate over the DBs and tables listed in the input bootstrap dump directory to clean tables from.
    BootstrapEventsIterator bootstrapEventsIterator
            = new BootstrapEventsIterator(bootstrapDirectory.toString(), work.dbNameToLoadIn, false, conf);

    // This map will have only one entry if target database is renamed using input DB name from REPL LOAD.
    // For multiple DBs case, this map maintains the table names list against each DB.
    Map<String, List<String>> dbToTblsListMap = new HashMap<>();
    while (bootstrapEventsIterator.hasNext()) {
      BootstrapEvent event = bootstrapEventsIterator.next();
      if (event.eventType().equals(BootstrapEvent.EventType.Table)) {
        FSTableEvent tableEvent = (FSTableEvent) event;
        String dbName = (StringUtils.isBlank(work.dbNameToLoadIn) ? tableEvent.getDbName() : work.dbNameToLoadIn);
        List<String> tableNames;
        if (dbToTblsListMap.containsKey(dbName)) {
          tableNames = dbToTblsListMap.get(dbName);
        } else {
          tableNames = new ArrayList<>();
          dbToTblsListMap.put(dbName, tableNames);
        }
        tableNames.add(tableEvent.getTableName());
      }
    }

    // No tables listed in the given bootstrap dump directory specified to clean tables.
    if (dbToTblsListMap.isEmpty()) {
      LOG.info("No DB/tables are listed in the bootstrap dump: {} specified to clean tables.",
              bootstrapDirectory);
      return;
    }

    Hive db = getHive();
    for (Map.Entry<String, List<String>> dbEntry : dbToTblsListMap.entrySet()) {
      String dbName = dbEntry.getKey();
      List<String> tableNames = dbEntry.getValue();

      for (String table : tableNames) {
        db.dropTable(dbName + "." + table, true);
      }
      LOG.info("Tables listed in the Database: {} in the bootstrap dump: {} are cleaned",
              dbName, bootstrapDirectory);
    }
  }

  /**
   * If replication policy is changed between previous and current load, then the excluded tables in
   * the new replication policy will be dropped.
   * @throws HiveException Failed to get/drop the tables.
   */
  private void dropTablesExcludedInReplScope(ReplScope replScope) throws HiveException {
    // If all tables are included in replication scope, then nothing to be dropped.
    if ((replScope == null) || replScope.includeAllTables()) {
      return;
    }

    Hive db = getHive();
    String dbName = replScope.getDbName();

    // List all the tables that are excluded in the current repl scope.
    Iterable<String> tableNames = Collections2.filter(db.getAllTables(dbName),
        tableName -> {
          assert(tableName != null);
          return !tableName.toLowerCase().startsWith(
                  SemanticAnalyzer.VALUES_TMP_TABLE_NAME_PREFIX.toLowerCase())
                  && !replScope.tableIncludedInReplScope(tableName);
        });
    for (String table : tableNames) {
      db.dropTable(dbName + "." + table, true);
    }
    LOG.info("Tables in the Database: {} that are excluded in the replication scope are dropped.",
            dbName);
  }

  private void createEndReplLogTask(Context context, Scope scope,
                                    ReplLogger replLogger) throws SemanticException {
    Map<String, String> dbProps;
    if (work.isIncrementalLoad()) {
      dbProps = new HashMap<>();
      dbProps.put(ReplicationSpec.KEY.CURR_STATE_ID.toString(),
                  work.incrementalLoadTasksBuilder().eventTo().toString());
    } else {
      Database dbInMetadata = work.databaseEvent(context.hiveConf).dbInMetadata(work.dbNameToLoadIn);
      dbProps = dbInMetadata.getParameters();
    }
    ReplStateLogWork replLogWork = new ReplStateLogWork(replLogger, dbProps);
    Task<ReplStateLogWork> replLogTask = TaskFactory.get(replLogWork, conf);
    if (scope.rootTasks.isEmpty()) {
      scope.rootTasks.add(replLogTask);
    } else {
      DAGTraversal.traverse(scope.rootTasks,
          new AddDependencyToLeaves(Collections.singletonList(replLogTask)));
    }
  }

  /**
   * There was a database update done before and we want to make sure we update the last repl
   * id on this database as we are now going to switch to processing a new database.
   *
   * This has to be last task in the graph since if there are intermediate tasks and the last.repl.id
   * is a root level task then in the execution phase the root level tasks will get executed first,
   * however if any of the child tasks of the bootstrap load failed then even though the bootstrap has failed
   * the last repl status of the target database will return a valid value, which will not represent
   * the state of the database.
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

    AddDependencyToLeaves function = new AddDependencyToLeaves(taskTracker.tasks());
    DAGTraversal.traverse(scope.rootTasks, function);

    return taskTracker;
  }

  private void partitionsPostProcessing(BootstrapEventsIterator iterator,
      Scope scope, TaskTracker loadTaskTracker, TaskTracker tableTracker,
      TaskTracker partitionsTracker) {
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
    if (parentTasks.hasTasks()) {
      for (Task<? extends Serializable> parentTask : parentTasks.tasks()) {
        for (Task<? extends Serializable> childTask : childTasks.tasks()) {
          parentTask.addDependentTask(childTask);
        }
      }
    } else {
      for (Task<? extends Serializable> childTask : childTasks.tasks()) {
        parentTasks.addTask(childTask);
      }
    }
  }

  private void createBuilderTask(List<Task<? extends Serializable>> rootTasks) {
    // Use loadTask as dependencyCollection
    Task<ReplLoadWork> loadTask = TaskFactory.get(work, conf);
    DAGTraversal.traverse(rootTasks, new AddDependencyToLeaves(loadTask));
  }

  private int executeIncrementalLoad(DriverContext driverContext) {
    try {
      // If user has requested to cleanup any bootstrap dump, then just do it before incremental load.
      if (work.needCleanTablesFromBootstrap) {
        cleanTablesFromBootstrap();
        work.needCleanTablesFromBootstrap = false;
      }

      // If replication policy is changed between previous and current repl load, then drop the tables
      // that are excluded in the new replication policy.
      dropTablesExcludedInReplScope(work.currentReplScope);

      IncrementalLoadTasksBuilder builder = work.incrementalLoadTasksBuilder();

      // If incremental events are already applied, then check and perform if need to bootstrap any tables.
      if (!builder.hasMoreWork() && !work.getPathsToCopyIterator().hasNext()) {
        if (work.hasBootstrapLoadTasks()) {
          LOG.debug("Current incremental dump have tables to be bootstrapped. Switching to bootstrap "
                  + "mode after applying all events.");
          return executeBootStrapLoad(driverContext);
        }
      }

      List<Task<? extends Serializable>> childTasks = new ArrayList<>();
      int maxTasks = conf.getIntVar(HiveConf.ConfVars.REPL_APPROX_MAX_LOAD_TASKS);

      // First start the distcp tasks to copy the files related to external table. The distcp tasks should be
      // started first to avoid ddl task trying to create table/partition directory. Distcp task creates these
      // directory with proper permission and owner.
      TaskTracker tracker = new TaskTracker(maxTasks);
      if (work.getPathsToCopyIterator().hasNext()) {
        childTasks.addAll(new ExternalTableCopyTaskBuilder(work, conf).tasks(tracker));
      } else {
        childTasks.add(builder.build(driverContext, getHive(), LOG, tracker));
      }

      // If there are no more events to be applied, add a task to update the last.repl.id of the
      // target database to the event id of the last event considered by the dump. Next
      // incremental cycle won't consider the events in this dump again if it starts from this id.
      if (!builder.hasMoreWork() && !work.getPathsToCopyIterator().hasNext()) {
        // The name of the database to be loaded into is either specified directly in REPL LOAD
        // command i.e. when dbNameToLoadIn has a valid dbname or is available through dump
        // metadata during table level replication.
        String dbName = work.dbNameToLoadIn;
        if (dbName == null || StringUtils.isBlank(dbName)) {
          if (work.currentReplScope != null) {
            String replScopeDbName = work.currentReplScope.getDbName();
            if (replScopeDbName != null && !"*".equals(replScopeDbName)) {
              dbName = replScopeDbName;
            }
          }
        }

        // If we are replicating to multiple databases at a time, it's not
        // possible to know which all databases we are replicating into and hence we can not
        // update repl id in all those databases.
        if (StringUtils.isNotBlank(dbName)) {
          String lastEventid = builder.eventTo().toString();
          Map<String, String> mapProp = new HashMap<>();
          mapProp.put(ReplicationSpec.KEY.CURR_STATE_ID.toString(), lastEventid);

          AlterDatabaseSetPropertiesDesc alterDbDesc =
                  new AlterDatabaseSetPropertiesDesc(dbName, mapProp,
                          new ReplicationSpec(lastEventid, lastEventid));
          Task<? extends Serializable> updateReplIdTask =
                  TaskFactory.get(new DDLWork(new HashSet<>(), new HashSet<>(), alterDbDesc), conf);

          DAGTraversal.traverse(childTasks, new AddDependencyToLeaves(updateReplIdTask));
          LOG.debug("Added task to set last repl id of db " + dbName + " to " + lastEventid);
        }
      }

      // Either the incremental has more work or the external table file copy has more paths to process.
      // Once all the incremental events are applied and external tables file copies are done, enable
      // bootstrap of tables if exist.
      if (builder.hasMoreWork() || work.getPathsToCopyIterator().hasNext() || work.hasBootstrapLoadTasks()) {
        DAGTraversal.traverse(childTasks, new AddDependencyToLeaves(TaskFactory.get(work, conf)));
      }
      this.childTasks = childTasks;
      return 0;
    } catch (Exception e) {
      LOG.error("failed replication", e);
      setException(e);
      return 1;
    }
  }
}
