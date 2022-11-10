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
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.drop.DropTableDesc;
import org.apache.hadoop.hive.ql.exec.ReplCopyTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.repl.ReplExternalTables;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.ReplLoadOpType;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.TableEvent;
import org.apache.hadoop.hive.ql.exec.repl.util.TaskTracker;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.util.Context;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.util.PathUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ImportSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.apache.hadoop.hive.ql.plan.ImportTableDesc;
import org.apache.hadoop.hive.ql.plan.LoadMultiFilesDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc.LoadFileType;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.ReplTxnWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.TreeMap;

import static org.apache.hadoop.hive.ql.parse.ImportSemanticAnalyzer.isPartitioned;

public class LoadTable {
  private final static Logger LOG = LoggerFactory.getLogger(LoadTable.class);
  //  private final Helper helper;
  private final Context context;
  private final ReplLogger replLogger;
  private final TableContext tableContext;
  private final TaskTracker tracker;
  private final TableEvent event;
  private final ReplicationMetricCollector metricCollector;

  public LoadTable(TableEvent event, Context context, ReplLogger replLogger,
      TableContext tableContext, TaskTracker limiter, ReplicationMetricCollector metricCollector) {
    this.event = event;
    this.context = context;
    this.replLogger = replLogger;
    this.tableContext = tableContext;
    this.tracker = new TaskTracker(limiter);
    this.metricCollector = metricCollector;
  }

  public TaskTracker tasks(boolean isBootstrapDuringInc, boolean isSecondFailover) throws Exception {
    // Path being passed to us is a table dump location. We go ahead and load it in as needed.
    // If tblName is null, then we default to the table name specified in _metadata, which is good.
    // or are both specified, in which case, that's what we are intended to create the new table as.
    if (event.shouldNotReplicate()) {
      return tracker;
    }
    String dbName = tableContext.dbNameToLoadIn; //this can never be null or empty;
    // Create table associated with the import
    // Executed if relevant, and used to contain all the other details about the table if not.
    ImportTableDesc tableDesc = event.tableDesc(dbName);
    Table table = ImportSemanticAnalyzer.tableIfExists(tableDesc, context.hiveDb);

    if (isSecondFailover) {
      tableDesc.setForceOverwriteTable();
    }

    // Normally, on import, trying to create a table or a partition in a db that does not yet exist
    // is a error condition. However, in the case of a REPL LOAD, it is possible that we are trying
    // to create tasks to create a table inside a db that as-of-now does not exist, but there is
    // a precursor Task waiting that will create it before this is encountered. Thus, we instantiate
    // defaults and do not error out in that case.
    // the above will change now since we are going to split replication load in multiple execution
    // tasks and hence we could have created the database earlier in which case the waitOnPrecursor will
    // be false and hence if db Not found we should error out.
    Database parentDb = context.hiveDb.getDatabase(tableDesc.getDatabaseName());
    if (parentDb == null) {
      if (!tableContext.waitOnPrecursor()) {
        throw new SemanticException(
            ErrorMsg.DATABASE_NOT_EXISTS.getMsg(tableDesc.getDatabaseName()));
      }
    }

    Task<?> tblRootTask = null;
    ReplLoadOpType loadTblType = getLoadTableType(table, isBootstrapDuringInc, isSecondFailover);
    switch (loadTblType) {
      case LOAD_NEW:
        break;
      case LOAD_REPLACE:
        tblRootTask = dropTableTask(table);
        break;
      case LOAD_SKIP:
        return tracker;
      default:
        break;
    }

    TableLocationTuple
        tableLocationTuple = tableLocation(tableDesc, parentDb, tableContext, context);
    tableDesc.setLocation(tableLocationTuple.location);

    /* Note: In the following section, Metadata-only import handling logic is
       interleaved with regular repl-import logic. The rule of thumb being
       followed here is that MD-only imports are essentially ALTERs. They do
       not load data, and should not be "creating" any metadata - they should
       be replacing instead. The only place it makes sense for a MD-only import
       to create is in the case of a table that's been dropped and recreated,
       or in the case of an unpartitioned table. In all other cases, it should
       behave like a noop or a pure MD alter.
    */
    newTableTasks(tableDesc, tblRootTask, tableLocationTuple, isSecondFailover && table != null);

    // Set Checkpoint task as dependant to create table task. So, if same dump is retried for
    // bootstrap, we skip current table update.
    Task<?> ckptTask = ReplUtils.getTableCheckpointTask(
            tableDesc,
            null,
            context.dumpDirectory,
            this.metricCollector,
            context.hiveConf
    );
    if (!isPartitioned(tableDesc)) {
      Task<?> replLogTask
              = ReplUtils.getTableReplLogTask(tableDesc, replLogger, context.hiveConf, metricCollector,
                                              (new Path(context.dumpDirectory)).getParent().toString());
      ckptTask.addDependentTask(replLogTask);
    }
    tracker.addDependentTask(ckptTask);
    return tracker;
  }

  private ReplLoadOpType getLoadTableType(Table table, boolean isBootstrapDuringInc, boolean isSecondFailover)
          throws InvalidOperationException, HiveException {
    // In case of second iteration of the optimised bootstrap, we don't drop & re create the table, instead we
    // re-write the metadata, in order to prevent deletion of the data in the target cluster. So, that while copying
    // data from the source cluster, we just operate on the modified/missing/additional files and can get rid of
    // copying the files which are already there on both cluster and are same.
    if (table == null || isSecondFailover) {
      return ReplLoadOpType.LOAD_NEW;
    }

    // In case user has asked for bootstrap of table during a incremental load, we replace the old one if present.
    // This is to make sure that the transactional info like write id etc for the table is consistent between the
    // source and target cluster. This is also to avoid mismatch between target and source cluster table type in case
    // migration and upgrade uses different conversion rule.
    if (isBootstrapDuringInc) {
      LOG.info("Table " + table.getTableName() + " will be replaced as bootstrap is requested during incremental load");
      return ReplLoadOpType.LOAD_REPLACE;
    }

    if (ReplUtils.replCkptStatus(table.getDbName(), table.getParameters(), context.dumpDirectory)) {
      return ReplLoadOpType.LOAD_SKIP;
    }
    return ReplLoadOpType.LOAD_REPLACE;
  }

  private void newTableTasks(ImportTableDesc tblDesc, Task<?> tblRootTask, TableLocationTuple tuple, boolean setLoc)
      throws Exception {
    Table table = tblDesc.toTable(context.hiveConf);
    ReplicationSpec replicationSpec = event.replicationSpec();
    if (!tblDesc.isExternal() && !setLoc) {
      tblDesc.setLocation(null);
    }
    Task<?> createTableTask =
        tblDesc.getCreateTableTask(new HashSet<>(), new HashSet<>(), context.hiveConf, true,
                (new Path(context.dumpDirectory)).getParent().toString(), metricCollector, true);
    if (tblRootTask == null) {
      tblRootTask = createTableTask;
    } else {
      tblRootTask.addDependentTask(createTableTask);
    }
    if (replicationSpec.isMetadataOnly()) {
      tracker.addTask(tblRootTask);
      return;
    }

    Task<?> parentTask = createTableTask;
    if (replicationSpec.isTransactionalTableDump()) {
      List<String> partNames = isPartitioned(tblDesc) ? event.partitions(tblDesc) : null;
      ReplTxnWork replTxnWork = new ReplTxnWork(tblDesc.getDatabaseName(), tblDesc.getTableName(), partNames,
              replicationSpec.getValidWriteIdList(), ReplTxnWork.OperationType.REPL_WRITEID_STATE,
              (new Path(context.dumpDirectory)).getParent().toString(), metricCollector);
      Task<?> replTxnTask = TaskFactory.get(replTxnWork, context.hiveConf);
      parentTask.addDependentTask(replTxnTask);
      parentTask = replTxnTask;
    }
    boolean shouldCreateLoadTableTask = (
        !isPartitioned(tblDesc)
            && !TableType.EXTERNAL_TABLE.equals(table.getTableType())
    ) || tuple.isConvertedFromManagedToExternal;
    if (shouldCreateLoadTableTask) {
      LOG.debug("adding dependent ReplTxnTask/CopyWork/MoveWork for table");
      Task<?> loadTableTask = loadTableTask(table, replicationSpec, table.getDataLocation(),
              event.dataPath());
      parentTask.addDependentTask(loadTableTask);
    }
    tracker.addTask(tblRootTask);
  }

  static class TableLocationTuple {
    final String location;
    private final boolean isConvertedFromManagedToExternal;

    TableLocationTuple(String location, boolean isConvertedFromManagedToExternal) {
      this.location = location;
      this.isConvertedFromManagedToExternal = isConvertedFromManagedToExternal;
    }
  }

  static TableLocationTuple tableLocation(ImportTableDesc tblDesc, Database parentDb,
      TableContext tableContext, Context context) throws MetaException, SemanticException {
    Warehouse wh = context.warehouse;
    Path defaultTablePath;
    if (parentDb == null) {
      defaultTablePath = wh.getDefaultTablePath(tblDesc.getDatabaseName(), tblDesc.getTableName(),
          tblDesc.isExternal());
    } else {
      defaultTablePath = wh.getDefaultTablePath(
          parentDb, tblDesc.getTableName(), tblDesc.isExternal()
      );
    }
    // dont use TableType.EXTERNAL_TABLE.equals(tblDesc.tableType()) since this comes in as managed always for tables.
    if (tblDesc.isExternal()) {
      if (tblDesc.getLocation() == null) {
        // this is the use case when the table got converted to external table as part of migration
        // related rules to be applied to replicated tables across different versions of hive.
        return new TableLocationTuple(wh.getDnsPath(defaultTablePath).toString(), true);
      }
      String currentLocation = new Path(tblDesc.getLocation()).toUri().getPath();
      String newLocation =
          ReplExternalTables.externalTableLocation(context.hiveConf, currentLocation);
      LOG.debug("external table {} data location is: {}", tblDesc.getTableName(), newLocation);
      return new TableLocationTuple(newLocation, false);
    }
    Path path = tableContext.waitOnPrecursor()
        ? wh.getDnsPath(defaultTablePath)
        : wh.getDefaultTablePath(parentDb, tblDesc.getTableName(), tblDesc.isExternal());
    return new TableLocationTuple(path.toString(), false);
  }

  private Task<?> loadTableTask(Table table, ReplicationSpec replicationSpec, Path tgtPath,
      Path fromURI) {
    Path dataPath = fromURI;
    Path tmpPath = tgtPath;

    // if acid tables, copy the files directly to the target path. No need to create the staging dir.
    LoadFileType loadFileType;
    if (replicationSpec.isInReplicationScope() && AcidUtils.isTransactionalTable(table)) {
      loadFileType = LoadFileType.IGNORE;
    } else {
      loadFileType = (replicationSpec.isReplace())
              ? LoadFileType.REPLACE_ALL : LoadFileType.OVERWRITE_EXISTING;
      tmpPath = PathUtils.getExternalTmpPath(tgtPath, context.pathInfo);
    }

    LOG.debug("adding dependent CopyWork/AddPart/MoveWork for table "
            + table.getCompleteName() + " with source location: "
            + dataPath.toString() + " and target location " + tgtPath.toString());

    boolean copyAtLoad = context.hiveConf.getBoolVar(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET);
    Task<?> copyTask = ReplCopyTask.getLoadCopyTask(replicationSpec, dataPath, tmpPath, context.hiveConf,
            copyAtLoad, false, (new Path(context.dumpDirectory)).getParent().toString(), metricCollector);
    MoveWork moveWork = new MoveWork(new HashSet<>(), new HashSet<>(), null, null, false,
                                     (new Path(context.dumpDirectory)).getParent().toString(), metricCollector,
                                      true);
    if (AcidUtils.isTransactionalTable(table)) {
      LoadMultiFilesDesc loadFilesWork = new LoadMultiFilesDesc(
        Collections.singletonList(tmpPath),
        Collections.singletonList(tgtPath),
        true, null, null);
      moveWork.setMultiFilesDesc(loadFilesWork);
    } else {
      LoadTableDesc loadTableWork = new LoadTableDesc(
              tmpPath, Utilities.getTableDesc(table), new TreeMap<>(),
              loadFileType, 0L
      );
      moveWork.setLoadTableWork(loadTableWork);
    }
    moveWork.setIsInReplicationScope(replicationSpec.isInReplicationScope());
    Task<?> loadTableTask = TaskFactory.get(moveWork, context.hiveConf);
    copyTask.addDependentTask(loadTableTask);
    return copyTask;
  }

  private Task<?> dropTableTask(Table table) {
    assert(table != null);
    DropTableDesc dropTblDesc = new DropTableDesc(table.getFullyQualifiedName(), true, false, event.replicationSpec());
    return TaskFactory.get(new DDLWork(new HashSet<>(), new HashSet<>(), dropTblDesc,
                                      true, (new Path(context.dumpDirectory)).getParent().toString(),
                                      this.metricCollector), context.hiveConf);
  }
}
