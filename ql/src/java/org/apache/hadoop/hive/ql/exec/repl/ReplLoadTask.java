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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.repl.ReplScope;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.database.alter.poperties.AlterDatabaseSetPropertiesDesc;
import org.apache.hadoop.hive.ql.ddl.view.create.CreateViewDesc;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.BootstrapEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.ConstraintEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.DatabaseEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.FunctionEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.PartitionEvent;
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
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.HiveTableName;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.parse.repl.load.MetaData;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Status;
import org.apache.hadoop.hive.ql.plan.api.StageType;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_DUMP_SKIP_IMMUTABLE_DATA_COPY;
import static org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.LoadDatabase.AlterDatabase;
import static org.apache.hadoop.hive.ql.exec.repl.ReplAck.LOAD_ACKNOWLEDGEMENT;
import static org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.RANGER_AUTHORIZER;

public class ReplLoadTask extends Task<ReplLoadWork> implements Serializable {
  private static final long serialVersionUID = 1L;
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
    boolean database = false, table = false;
    List<Task<?>> rootTasks = new ArrayList<>();
  }

  @Override
  public int execute() {
    try {
      Task<?> rootTask = work.getRootTask();
      if (rootTask != null) {
        rootTask.setChildTasks(null);
      }
      work.setRootTask(this);
      this.parentTasks = null;
      if (shouldLoadAtlasMetadata()) {
        addAtlasLoadTask();
      }
      if (shouldLoadAuthorizationMetadata()) {
        initiateAuthorizationLoadTask();
      }
      if (work.isIncrementalLoad()) {
        return executeIncrementalLoad();
      } else {
        return executeBootStrapLoad();
      }
    } catch (RuntimeException e) {
      LOG.error("replication failed with run time exception", e);
      try {
        work.getMetricCollector().reportEnd(Status.FAILED);
      } catch (SemanticException ex) {
        LOG.error("Failed to collect Metrics ", ex);
      }
      throw e;
    } catch (Exception e) {
      LOG.error("replication failed", e);
      setException(e);
      try {
        work.getMetricCollector().reportEnd(Status.FAILED);
      } catch (SemanticException ex) {
        LOG.error("Failed to collect Metrics ", ex);
      }
      return ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode();
    }
  }

  private boolean shouldLoadAuthorizationMetadata() {
    return conf.getBoolVar(HiveConf.ConfVars.REPL_INCLUDE_AUTHORIZATION_METADATA);
  }

  private void initiateAuthorizationLoadTask() throws SemanticException {
    if (RANGER_AUTHORIZER.equalsIgnoreCase(conf.getVar(HiveConf.ConfVars.REPL_AUTHORIZATION_PROVIDER_SERVICE))) {
      Path rangerLoadRoot = new Path(new Path(work.dumpDirectory).getParent(), ReplUtils.REPL_RANGER_BASE_DIR);
      LOG.info("Adding Import Ranger Metadata Task from {} ", rangerLoadRoot);
      RangerLoadWork rangerLoadWork = new RangerLoadWork(rangerLoadRoot, work.getSourceDbName(), work.dbNameToLoadIn,
          work.getMetricCollector());
      Task<RangerLoadWork> rangerLoadTask = TaskFactory.get(rangerLoadWork, conf);
      if (childTasks == null) {
        childTasks = new ArrayList<>();
      }
      childTasks.add(rangerLoadTask);
    } else {
      throw new SemanticException("Authorizer " + conf.getVar(HiveConf.ConfVars.REPL_AUTHORIZATION_PROVIDER_SERVICE)
              + " not supported for replication ");
    }
  }

  private void addAtlasLoadTask() throws HiveException {
    Path atlasDumpDir = new Path(new Path(work.dumpDirectory).getParent(), ReplUtils.REPL_ATLAS_BASE_DIR);
    LOG.info("Adding task to load Atlas metadata from {} ", atlasDumpDir);
    AtlasLoadWork atlasLoadWork = new AtlasLoadWork(work.getSourceDbName(), work.dbNameToLoadIn, atlasDumpDir,
        work.getMetricCollector());
    Task<?> atlasLoadTask = TaskFactory.get(atlasLoadWork, conf);
    if (childTasks == null) {
      childTasks = new ArrayList<>();
    }
    childTasks.add(atlasLoadTask);
  }

  private boolean shouldLoadAtlasMetadata() {
    return conf.getBoolVar(HiveConf.ConfVars.REPL_INCLUDE_ATLAS_METADATA);
  }

  private int executeBootStrapLoad() throws Exception {
    int maxTasks = conf.getIntVar(HiveConf.ConfVars.REPL_APPROX_MAX_LOAD_TASKS);
    Context loadContext = new Context(work.dumpDirectory, conf, getHive(),
        work.sessionStateLineageState, context);
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
    while ((iterator.hasNext() || (loadingConstraint && constraintIterator.hasNext()))
        && loadTaskTracker.canAddMoreTasks()) {
      BootstrapEvent next;
      if (!loadingConstraint) {
        next = iterator.next();
      } else {
        next = constraintIterator.next();
      }
      switch (next.eventType()) {
      case Database:
        DatabaseEvent dbEvent = (DatabaseEvent) next;
        dbTracker = new LoadDatabase(loadContext, dbEvent, work.dbNameToLoadIn, loadTaskTracker).tasks();
        loadTaskTracker.update(dbTracker);
        if (work.hasDbState()) {
          loadTaskTracker.update(updateDatabaseLastReplID(maxTasks, loadContext, scope));
        } else {
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
      case Table:
      /*
          Implicit assumption here is that database level is processed first before table level,
          which will depend on the iterator used since it should provide the higher level directory
          listing before providing the lower level listing. This is also required such that
          the dbTracker /  tableTracker are setup correctly always.
       */
        TableContext tableContext = new TableContext(dbTracker, work.dbNameToLoadIn);
        FSTableEvent tableEvent = (FSTableEvent) next;
        if (TableType.VIRTUAL_VIEW.name().equals(tableEvent.getMetaData().getTable().getTableType())) {
          tableTracker = new TaskTracker(1);
          tableTracker.addTask(createViewTask(tableEvent.getMetaData(), work.dbNameToLoadIn, conf));
        } else {
          LoadTable loadTable = new LoadTable(tableEvent, loadContext, iterator.replLogger(), tableContext,
              loadTaskTracker, work.getMetricCollector());
          tableTracker = loadTable.tasks(work.isIncrementalLoad());
        }

        setUpDependencies(dbTracker, tableTracker);
        if (!scope.database && tableTracker.hasTasks()) {
          scope.rootTasks.addAll(tableTracker.tasks());
          scope.table = true;
        } else {
          // Scope might have set to table in some previous iteration of loop, so reset it to false if table
          // tracker has no tasks.
          scope.table = false;
        }

        if (!TableType.VIRTUAL_VIEW.name().equals(tableEvent.getMetaData().getTable().getTableType())) {
        /*
          for table replication if we reach the max number of tasks then for the next run we will
          try to reload the same table again, this is mainly for ease of understanding the code
          as then we can avoid handling == > loading partitions for the table given that
          the creation of table lead to reaching max tasks vs,  loading next table since current
          one does not have partitions.
         */

          // for a table we explicitly try to load partitions as there is no separate partitions events.
          LoadPartitions loadPartitions =
              new LoadPartitions(loadContext, iterator.replLogger(), loadTaskTracker, tableEvent,
                  work.dbNameToLoadIn, tableContext, work.getMetricCollector());
          TaskTracker partitionsTracker = loadPartitions.tasks();
          partitionsPostProcessing(iterator, scope, loadTaskTracker, tableTracker,
              partitionsTracker);
          tableTracker.debugLog("table");
          partitionsTracker.debugLog("partitions for table");
        }
        break;
      case Partition:
      /*
          This will happen only when loading tables and we reach the limit of number of tasks we can create;
          hence we know here that the table should exist and there should be a lastPartitionName
      */
        addLoadPartitionTasks(loadContext, next, dbTracker, iterator, scope, loadTaskTracker, tableTracker);
        break;
      case Function:
        loadTaskTracker.update(addLoadFunctionTasks(loadContext, iterator, next, dbTracker, scope));
        break;
      case Constraint:
        loadTaskTracker.update(addLoadConstraintsTasks(loadContext, next, dbTracker, scope));
        break;
      default:
        break;
      }
      if (!loadingConstraint && !iterator.currentDbHasNext()) {
        createEndReplLogTask(loadContext, scope, iterator.replLogger());
      }
    }
    boolean addAnotherLoadTask = iterator.hasNext()
        || loadTaskTracker.hasReplicationState()
        || constraintIterator.hasNext();

    if (addAnotherLoadTask) {
      createBuilderTask(scope.rootTasks);
    }
    // Update last repl ID of the database only if the current dump is not incremental. If bootstrap
    // is combined with incremental dump, it contains only tables to bootstrap. So, needn't change
    // last repl ID of the database.
    if (!iterator.hasNext() && !constraintIterator.hasNext() && !work.isIncrementalLoad()) {
      loadTaskTracker.update(updateDatabaseLastReplID(maxTasks, loadContext, scope));
      work.updateDbEventState(null);
    }
    if (childTasks == null) {
      childTasks = new ArrayList<>();
    }
    childTasks.addAll(scope.rootTasks);
    /*
    Since there can be multiple rounds of this run all of which will be tied to the same
    query id -- generated in compile phase , adding a additional UUID to the end to print each run
    in separate files.
     */
    LOG.info("Root Tasks / Total Tasks : {} / {} ", childTasks.size(), loadTaskTracker.numberOfTasks());
    // Populate the driver context with the scratch dir info from the repl context, so that the
    // temp dirs will be cleaned up later
    context.getFsScratchDirs().putAll(loadContext.pathInfo.getFsScratchDirs());
    if (!HiveConf.getBoolVar(conf, REPL_DUMP_SKIP_IMMUTABLE_DATA_COPY)) {
      createReplLoadCompleteAckTask();
    }
    LOG.info("completed load task run : {}", work.executedLoadTask());
    return 0;
  }

  private TaskTracker addLoadPartitionTasks(Context loadContext, BootstrapEvent next, TaskTracker dbTracker,
                                            BootstrapEventsIterator iterator, Scope scope, TaskTracker loadTaskTracker,
                                            TaskTracker tableTracker) throws Exception {
    PartitionEvent event = (PartitionEvent) next;
    TableContext tableContext = new TableContext(dbTracker, work.dbNameToLoadIn);
    LoadPartitions loadPartitions =
        new LoadPartitions(loadContext, iterator.replLogger(), tableContext, loadTaskTracker,
        event.asTableEvent(), work.dbNameToLoadIn, event.lastPartitionReplicated(), work.getMetricCollector());
        /*
             the tableTracker here should be a new instance and not an existing one as this can
             only happen when we break in between loading partitions.
         */
    TaskTracker partitionsTracker = loadPartitions.tasks();
    partitionsPostProcessing(iterator, scope, loadTaskTracker, tableTracker,
        partitionsTracker);
    partitionsTracker.debugLog("partitions");
    return partitionsTracker;
  }

  private TaskTracker addLoadConstraintsTasks(Context loadContext,
                                              BootstrapEvent next,
                                              TaskTracker dbTracker,
                                              Scope scope) throws IOException, SemanticException {
    LoadConstraint loadConstraint =
        new LoadConstraint(loadContext, (ConstraintEvent) next, work.dbNameToLoadIn, dbTracker);
    TaskTracker constraintTracker = loadConstraint.tasks();
    scope.rootTasks.addAll(constraintTracker.tasks());
    constraintTracker.debugLog("constraints");
    return constraintTracker;
  }

  private TaskTracker addLoadFunctionTasks(Context loadContext, BootstrapEventsIterator iterator, BootstrapEvent next,
                                    TaskTracker dbTracker, Scope scope) throws IOException, SemanticException {
    LoadFunction loadFunction = new LoadFunction(loadContext, iterator.replLogger(),
        (FunctionEvent) next, work.dbNameToLoadIn, dbTracker, work.getMetricCollector());
    TaskTracker functionsTracker = loadFunction.tasks();
    if (!scope.database) {
      scope.rootTasks.addAll(functionsTracker.tasks());
    } else {
      setUpDependencies(dbTracker, functionsTracker);
    }
    functionsTracker.debugLog("functions");
    return functionsTracker;
  }

  public static Task<?> createViewTask(MetaData metaData, String dbNameToLoadIn, HiveConf conf)
      throws SemanticException {
    Table table = new Table(metaData.getTable());
    String dbName = dbNameToLoadIn == null ? table.getDbName() : dbNameToLoadIn;
    TableName tableName = HiveTableName.ofNullable(table.getTableName(), dbName);
    String dbDotView = tableName.getNotEmptyDbTable();

    String viewOriginalText = table.getViewOriginalText();
    String viewExpandedText = table.getViewExpandedText();
    if (!dbName.equals(table.getDbName())) {
      // TODO: If the DB name doesn't match with the metadata from dump, then need to rewrite the original and expanded
      // texts using new DB name. Currently it refers to the source database name.
    }

    CreateViewDesc desc = new CreateViewDesc(dbDotView, table.getAllCols(), null, table.getParameters(),
        table.getPartColNames(), false, false, viewOriginalText, viewExpandedText, table.getPartCols());

    desc.setReplicationSpec(metaData.getReplicationSpec());
    desc.setOwnerName(table.getOwner());

    return TaskFactory.get(new DDLWork(new HashSet<>(), new HashSet<>(), desc), conf);
  }

  /**
   * If replication policy is changed between previous and current load, then the excluded tables in
   * the new replication policy will be dropped.
   *
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
          assert (tableName != null);
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

  private void createReplLoadCompleteAckTask() {
    if ((work.isIncrementalLoad() && !work.incrementalLoadTasksBuilder().hasMoreWork() && !work.hasBootstrapLoadTasks())
        || (!work.isIncrementalLoad() && !work.hasBootstrapLoadTasks())) {
      //All repl load tasks are executed and status is 0, create the task to add the acknowledgement
      AckWork replLoadAckWork = new AckWork(
          new Path(work.dumpDirectory, LOAD_ACKNOWLEDGEMENT.toString()));
      Task<AckWork> loadAckWorkTask = TaskFactory.get(replLoadAckWork, conf);
      if (childTasks.isEmpty()) {
        childTasks.add(loadAckWorkTask);
      } else {
        DAGTraversal.traverse(childTasks, new AddDependencyToLeaves(Collections.singletonList(loadAckWorkTask)));
      }
    }
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
    ReplStateLogWork replLogWork = new ReplStateLogWork(replLogger, dbProps, work.getMetricCollector());
    Task<ReplStateLogWork> replLogTask = TaskFactory.get(replLogWork, conf);
    if (scope.rootTasks.isEmpty()) {
      scope.rootTasks.add(replLogTask);
    } else {
      DAGTraversal.traverse(scope.rootTasks, new AddDependencyToLeaves(Collections.singletonList(replLogTask)));
    }
  }

  /**
   * There was a database update done before and we want to make sure we update the last repl
   * id on this database as we are now going to switch to processing a new database.
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
      for (Task<?> parentTask : parentTasks.tasks()) {
        for (Task<?> childTask : childTasks.tasks()) {
          parentTask.addDependentTask(childTask);
        }
      }
    } else {
      for (Task<?> childTask : childTasks.tasks()) {
        parentTasks.addTask(childTask);
      }
    }
  }

  private void createBuilderTask(List<Task<?>> rootTasks) {
    // Use loadTask as dependencyCollection
    Task<ReplLoadWork> loadTask = TaskFactory.get(work, conf);
    DAGTraversal.traverse(rootTasks, new AddDependencyToLeaves(loadTask));
  }

  private int executeIncrementalLoad() throws Exception {
    // If replication policy is changed between previous and current repl load, then drop the tables
    // that are excluded in the new replication policy.
    dropTablesExcludedInReplScope(work.currentReplScope);
    IncrementalLoadTasksBuilder builder = work.incrementalLoadTasksBuilder();
    // If incremental events are already applied, then check and perform if need to bootstrap any tables.
    if (!builder.hasMoreWork() && work.isLastReplIDUpdated()) {
      if (work.hasBootstrapLoadTasks()) {
        LOG.debug("Current incremental dump have tables to be bootstrapped. Switching to bootstrap "
            + "mode after applying all events.");
        return executeBootStrapLoad();
      }
    }
    List<Task<?>> childTasks = new ArrayList<>();
    int maxTasks = conf.getIntVar(HiveConf.ConfVars.REPL_APPROX_MAX_LOAD_TASKS);
    TaskTracker tracker = new TaskTracker(maxTasks);
    childTasks.add(builder.build(context, getHive(), LOG, tracker));
    // If there are no more events to be applied, add a task to update the last.repl.id of the
    // target database to the event id of the last event considered by the dump. Next
    // incremental cycle won't consider the events in this dump again if it starts from this id.
    if (!builder.hasMoreWork()) {
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
        Task<?> updateReplIdTask =
            TaskFactory.get(new DDLWork(new HashSet<>(), new HashSet<>(), alterDbDesc), conf);
        DAGTraversal.traverse(childTasks, new AddDependencyToLeaves(updateReplIdTask));
        work.setLastReplIDUpdated(true);
        LOG.debug("Added task to set last repl id of db " + dbName + " to " + lastEventid);
      }
    }
    // Once all the incremental events are applied, enable bootstrap of tables if exist.
    if (builder.hasMoreWork() || work.hasBootstrapLoadTasks()) {
      DAGTraversal.traverse(childTasks, new AddDependencyToLeaves(TaskFactory.get(work, conf)));
    }
    if (this.childTasks == null) {
      this.childTasks = new ArrayList<>();
    }
    this.childTasks.addAll(childTasks);
    createReplLoadCompleteAckTask();
    return 0;
  }
}
