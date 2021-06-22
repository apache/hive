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

import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils;
import org.apache.hadoop.hive.ql.parse.repl.load.log.IncrementalLoadLogger;
import org.apache.thrift.TException;
import com.google.common.collect.Collections2;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.repl.ReplScope;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
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
import org.apache.hadoop.hive.ql.exec.repl.util.FileList;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.TaskTracker;
import org.apache.hadoop.hive.ql.exec.util.DAGTraversal;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.HiveTableName;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.parse.repl.load.MetaData;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.LinkedList;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_DUMP_SKIP_IMMUTABLE_DATA_COPY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_SNAPSHOT_DIFF_FOR_EXTERNAL_TABLE_COPY;
import static org.apache.hadoop.hive.ql.exec.repl.ReplAck.LOAD_METADATA;
import static org.apache.hadoop.hive.ql.exec.repl.ReplExternalTables.getExternalTableBaseDir;
import static org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.LoadDatabase.AlterDatabase;
import static org.apache.hadoop.hive.ql.exec.repl.ReplAck.LOAD_ACKNOWLEDGEMENT;
import static org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.RANGER_AUTHORIZER;
import static org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils.cleanupSnapshots;

public class ReplLoadTask extends Task<ReplLoadWork> implements Serializable {
  private static final long serialVersionUID = 1L;
  private final static int ZERO_TASKS = 0;
  private final String STAGE_NAME = "REPL_LOAD";

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
      long loadTaskStartTime = System.currentTimeMillis();
      SecurityUtils.reloginExpiringKeytabUser();
      Task<?> rootTask = work.getRootTask();
      if (rootTask != null) {
        rootTask.setChildTasks(null);
      }
      work.setRootTask(this);
      this.parentTasks = null;
      // Set distCp custom name corresponding to the replication policy.
      String mapRedCustomName = ReplUtils.getDistCpCustomName(conf, work.dbNameToLoadIn);
      conf.set(JobContext.JOB_NAME, mapRedCustomName);
      if (shouldLoadAtlasMetadata()) {
        addAtlasLoadTask();
      }
      if (shouldLoadAuthorizationMetadata()) {
        initiateAuthorizationLoadTask();
      }
      LOG.info("Data copy at load enabled : {}", conf.getBoolVar(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET));
      if (work.isIncrementalLoad()) {
        return executeIncrementalLoad(loadTaskStartTime);
      } else {
        return executeBootStrapLoad();
      }
    } catch (RuntimeException e) {
      LOG.error("replication failed with run time exception", e);
      setException(e);
      try {
        ReplUtils.handleException(true, e, new Path(work.getDumpDirectory()).getParent().toString(),
                work.getMetricCollector(), STAGE_NAME, conf);
      } catch (Exception ex){
        LOG.error("Failed to collect replication metrics: ", ex);
      }
      throw e;
    } catch (Exception e) {
      setException(e);
      int errorCode = ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode();
      try {
        return ReplUtils.handleException(true, e, new Path(work.getDumpDirectory()).getParent().toString(),
                work.getMetricCollector(), STAGE_NAME, conf);
      }
      catch (Exception ex) {
        LOG.error("Failed to collect replication metrics: ", ex);
        return errorCode;
      }
    }
  }

  private boolean shouldLoadAuthorizationMetadata() {
    return conf.getBoolVar(HiveConf.ConfVars.REPL_INCLUDE_AUTHORIZATION_METADATA);
  }

  private void initiateAuthorizationLoadTask() throws SemanticException {
    if (RANGER_AUTHORIZER.equalsIgnoreCase(conf.getVar(HiveConf.ConfVars.REPL_AUTHORIZATION_PROVIDER_SERVICE))) {
      Path rangerLoadRoot = new Path(new Path(work.dumpDirectory).getParent(), ReplUtils.REPL_RANGER_BASE_DIR);
      LOG.info("Adding Import Ranger Metadata Task from {} ", rangerLoadRoot);
      String targetDbName = StringUtils.isEmpty(work.dbNameToLoadIn) ? work.getSourceDbName() : work.dbNameToLoadIn;
      RangerLoadWork rangerLoadWork = new RangerLoadWork(rangerLoadRoot, work.getSourceDbName(), targetDbName,
          work.getMetricCollector());
      Task<RangerLoadWork> rangerLoadTask = TaskFactory.get(rangerLoadWork, conf);
      if (childTasks == null) {
        childTasks = new ArrayList<>();
      }
      childTasks.add(rangerLoadTask);
    } else {
      throw new SemanticException(ErrorMsg.REPL_INVALID_CONFIG_FOR_SERVICE.format("Authorizer " +
        conf.getVar(HiveConf.ConfVars.REPL_AUTHORIZATION_PROVIDER_SERVICE)
              + " not supported for replication ", ReplUtils.REPL_RANGER_SERVICE));
    }
  }

  private void addAtlasLoadTask() throws HiveException {
    Path atlasDumpDir = new Path(new Path(work.dumpDirectory).getParent(), ReplUtils.REPL_ATLAS_BASE_DIR);
    LOG.info("Adding task to load Atlas metadata from {} ", atlasDumpDir);
    String targetDbName = StringUtils.isEmpty(work.dbNameToLoadIn) ? work.getSourceDbName() : work.dbNameToLoadIn;
    AtlasLoadWork atlasLoadWork = new AtlasLoadWork(work.getSourceDbName(), targetDbName, atlasDumpDir,
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
    BootstrapEventsIterator iterator = work.bootstrapIterator();

    addLazyDataCopyTask(loadTaskTracker, iterator.replLogger());
    /*
        for now for simplicity we are doing just one directory ( one database ), come back to use
        of multiple databases once we have the basic flow to chain creating of tasks in place for
        a database ( directory )
    */
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
    boolean dbEventFound = false;
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
        dbTracker = new LoadDatabase(loadContext, dbEvent, work.dbNameToLoadIn, loadTaskTracker,
                work.getMetricCollector()).tasks();
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
          dbEventFound = true;
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
          tableTracker.addTask(createViewTask(tableEvent.getMetaData(), work.dbNameToLoadIn, conf,
                  (new Path(work.dumpDirectory).getParent()).toString(), work.getMetricCollector()));
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

      if (dbEventFound && conf.getBoolVar(HiveConf.ConfVars.REPL_RETAIN_CUSTOM_LOCATIONS_FOR_DB_ON_TARGET)) {
        // Force the database creation before the other event like table/partition etc, so that data copy path creation
        // can be achieved.
        LOG.info("Database event found, will be processed exclusively");
        break;
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

    if (conf.getBoolVar(REPL_SNAPSHOT_DIFF_FOR_EXTERNAL_TABLE_COPY)) {
      Path snapPath = SnapshotUtils.getSnapshotFileListPath(new Path(work.dumpDirectory));
      try {
        SnapshotUtils.getDFS(getExternalTableBaseDir(conf), conf)
            .rename(new Path(snapPath, EximUtil.FILE_LIST_EXTERNAL_SNAPSHOT_CURRENT),
                new Path(snapPath, EximUtil.FILE_LIST_EXTERNAL_SNAPSHOT_OLD), Options.Rename.OVERWRITE);
      } catch (FileNotFoundException fnf) {
        // Ignore if no file.
      }
    }
    return 0;
  }

  private void addLazyDataCopyTask(TaskTracker loadTaskTracker, ReplLogger replLogger) throws IOException {
    boolean dataCopyAtLoad = conf.getBoolVar(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET);
    if (dataCopyAtLoad) {
      if (work.getExternalTableDataCopyItr() == null) {
        Path extTableBackingFile = new Path(work.dumpDirectory, EximUtil.FILE_LIST_EXTERNAL);
        try(FileList fileList = new FileList(extTableBackingFile, conf)) {
          work.setExternalTableDataCopyItr(fileList);
        }
      }
      if (childTasks == null) {
        childTasks = new ArrayList<>();
      }
      List<Task<?>> externalTableCopyTasks = work.externalTableCopyTasks(loadTaskTracker, conf);
      LOG.debug("Scheduled {} external table copy tasks", externalTableCopyTasks.size());
      childTasks.addAll(externalTableCopyTasks);
      // If external table data copy tasks are present add a task to mark the end of data copy
      if (!externalTableCopyTasks.isEmpty() && !work.getExternalTableDataCopyItr().hasNext()) {
        ReplUtils.addLoggerTask(replLogger, childTasks, conf);
      }
    }
  }

  private TaskTracker addLoadPartitionTasks(Context loadContext, BootstrapEvent next, TaskTracker dbTracker,
                                            BootstrapEventsIterator iterator, Scope scope, TaskTracker loadTaskTracker,
                                            TaskTracker tableTracker) throws Exception {
    PartitionEvent event = (PartitionEvent) next;
    TableContext tableContext = new TableContext(dbTracker, work.dbNameToLoadIn);
    LoadPartitions loadPartitions =
        new LoadPartitions(loadContext, iterator.replLogger(), tableContext, loadTaskTracker,
        event.asTableEvent(), work.dbNameToLoadIn, event.lastPartitionReplicated(), work.getMetricCollector(),
          event.lastPartSpecReplicated(), event.lastStageReplicated());
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
        new LoadConstraint(loadContext, (ConstraintEvent) next, work.dbNameToLoadIn, dbTracker,
                (new Path(work.dumpDirectory)).getParent().toString(), work.getMetricCollector());
    TaskTracker constraintTracker = loadConstraint.tasks();
    scope.rootTasks.addAll(constraintTracker.tasks());
    constraintTracker.debugLog("constraints");
    return constraintTracker;
  }

  private TaskTracker addLoadFunctionTasks(Context loadContext, BootstrapEventsIterator iterator, BootstrapEvent next,
                                    TaskTracker dbTracker, Scope scope) throws IOException, SemanticException {
    LoadFunction loadFunction = new LoadFunction(loadContext, iterator.replLogger(),
            (FunctionEvent) next, work.dbNameToLoadIn, dbTracker, (new Path(work.dumpDirectory)).getParent().toString(),
            work.getMetricCollector());
    TaskTracker functionsTracker = loadFunction.tasks();
    if (!scope.database) {
      scope.rootTasks.addAll(functionsTracker.tasks());
    } else {
      setUpDependencies(dbTracker, functionsTracker);
    }
    functionsTracker.debugLog("functions");
    return functionsTracker;
  }

  public static Task<?> createViewTask(MetaData metaData, String dbNameToLoadIn, HiveConf conf,
                                       String dumpDirectory, ReplicationMetricCollector metricCollector)
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

    CreateViewDesc desc = new CreateViewDesc(dbDotView, table.getCols(), null, table.getParameters(),
            table.getPartColNames(), false, false, viewOriginalText, viewExpandedText, table.getPartCols());

    desc.setReplicationSpec(metaData.getReplicationSpec());
    desc.setOwnerName(table.getOwner());

    return TaskFactory.get(new DDLWork(new HashSet<>(), new HashSet<>(), desc, true,
            dumpDirectory, metricCollector), conf);
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
    if (!work.hasBootstrapLoadTasks()
            && (work.isIncrementalLoad() ? !work.incrementalLoadTasksBuilder().hasMoreWork() : true)) {
      //All repl load tasks are executed and status is 0, create the task to add the acknowledgement
      List<PreAckTask> listOfPreAckTasks = new LinkedList<>();
      listOfPreAckTasks.add(new PreAckTask() {
        @Override
        public void run() throws SemanticException {
          try {
            HiveMetaStoreClient metaStoreClient = new HiveMetaStoreClient(conf);
            long currentNotificationID = metaStoreClient.getCurrentNotificationEventId().getEventId();
            Path loadMetadataFilePath = new Path(work.dumpDirectory, LOAD_METADATA.toString());
            Utils.writeOutput(String.valueOf(currentNotificationID), loadMetadataFilePath, conf);
            LOG.info("Created LOAD Metadata file : {} with NotificationID : {}",
                    loadMetadataFilePath, currentNotificationID);
          } catch (TException ex) {
            throw new SemanticException(ex);
          }
        }
      });
      AckWork replLoadAckWork = new AckWork(new Path(work.dumpDirectory, LOAD_ACKNOWLEDGEMENT.toString()),
              work.getMetricCollector(), listOfPreAckTasks);
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
    ReplStateLogWork replLogWork = new ReplStateLogWork(replLogger, dbProps,
                                                        (new Path(work.dumpDirectory).getParent()).toString(),
                                                        work.getMetricCollector());
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
            new TaskTracker(maxTasks), work.getMetricCollector()).tasks();

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

  private int executeIncrementalLoad(long loadStartTime) throws Exception {
    // If replication policy is changed between previous and current repl load, then drop the tables
    // that are excluded in the new replication policy.
    if (work.replScopeModified) {
      dropTablesExcludedInReplScope(work.currentReplScope);
    }
    if (!MetaStoreUtils.isTargetOfReplication(getHive().getDatabase(work.dbNameToLoadIn))) {
      Map<String, String> props = new HashMap<>();
      props.put(ReplConst.TARGET_OF_REPLICATION, "true");
      AlterDatabaseSetPropertiesDesc setTargetDesc = new AlterDatabaseSetPropertiesDesc(work.dbNameToLoadIn, props, null);
      Task<?> addReplTargetPropTask =
              TaskFactory.get(new DDLWork(new HashSet<>(), new HashSet<>(), setTargetDesc, true,
                      work.dumpDirectory, work.getMetricCollector()), conf);
      if (this.childTasks == null) {
        this.childTasks = new ArrayList<>();
      }
      this.childTasks.add(addReplTargetPropTask);
    }
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
    addLazyDataCopyTask(tracker, builder.getReplLogger());
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
            TaskFactory.get(new DDLWork(new HashSet<>(), new HashSet<>(), alterDbDesc, true,
                    (new Path(work.dumpDirectory).getParent()).toString(), work.getMetricCollector()), conf);
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
    // Clean-up snapshots
    if (conf.getBoolVar(REPL_SNAPSHOT_DIFF_FOR_EXTERNAL_TABLE_COPY)) {
      cleanupSnapshots(new Path(work.getDumpDirectory()).getParent().getParent().getParent(),
          work.getSourceDbName().toLowerCase(), conf, null, true);
    }

    //pass the current time at the end of repl-load stage as the starting time of the first event.
    long currentTimestamp = System.currentTimeMillis();
    ((IncrementalLoadLogger)work.incrementalLoadTasksBuilder().getReplLogger()).initiateEventTimestamp(currentTimestamp);
    LOG.info("REPL_INCREMENTAL_LOAD stage duration : {} ms", currentTimestamp - loadStartTime);
    return 0;
  }
}
