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
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ReplCopyTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.ReplLoadOpType;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.TableEvent;
import org.apache.hadoop.hive.ql.exec.repl.util.TaskTracker;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.util.Context;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.util.PathUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.ImportSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DropTableDesc;
import org.apache.hadoop.hive.ql.plan.ImportTableDesc;
import org.apache.hadoop.hive.ql.plan.LoadMultiFilesDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc.LoadFileType;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.ReplTxnWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
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

  public LoadTable(TableEvent event, Context context, ReplLogger replLogger,
                   TableContext tableContext, TaskTracker limiter)
      throws SemanticException, IOException {
    this.event = event;
    this.context = context;
    this.replLogger = replLogger;
    this.tableContext = tableContext;
    this.tracker = new TaskTracker(limiter);
  }

  public TaskTracker tasks() throws SemanticException {
    // Path being passed to us is a table dump location. We go ahead and load it in as needed.
    // If tblName is null, then we default to the table name specified in _metadata, which is good.
    // or are both specified, in which case, that's what we are intended to create the new table as.
    try {
      if (event.shouldNotReplicate()) {
        return tracker;
      }
      String dbName = tableContext.dbNameToLoadIn; //this can never be null or empty;
      // Create table associated with the import
      // Executed if relevant, and used to contain all the other details about the table if not.
      ImportTableDesc tableDesc = tableContext.overrideProperties(event.tableDesc(dbName));
      Table table = ImportSemanticAnalyzer.tableIfExists(tableDesc, context.hiveDb);

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
      ReplLoadOpType loadTblType = getLoadTableType(table);
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

      if (tableDesc.getLocation() == null) {
        tableDesc.setLocation(location(tableDesc, parentDb));
      }

  /* Note: In the following section, Metadata-only import handling logic is
     interleaved with regular repl-import logic. The rule of thumb being
     followed here is that MD-only imports are essentially ALTERs. They do
     not load data, and should not be "creating" any metadata - they should
     be replacing instead. The only place it makes sense for a MD-only import
     to create is in the case of a table that's been dropped and recreated,
     or in the case of an unpartitioned table. In all other cases, it should
     behave like a noop or a pure MD alter.
  */
      newTableTasks(tableDesc, tblRootTask);

      // Set Checkpoint task as dependant to create table task. So, if same dump is retried for
      // bootstrap, we skip current table update.
      Task<?> ckptTask = ReplUtils.getTableCheckpointTask(
              tableDesc,
              null,
              context.dumpDirectory,
              context.hiveConf
      );
      if (!isPartitioned(tableDesc)) {
        Task<? extends Serializable> replLogTask
                = ReplUtils.getTableReplLogTask(tableDesc, replLogger, context.hiveConf);
        ckptTask.addDependentTask(replLogTask);
      }
      tracker.addDependentTask(ckptTask);
      return tracker;
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  private ReplLoadOpType getLoadTableType(Table table) throws InvalidOperationException, HiveException {
    if (table == null) {
      return ReplLoadOpType.LOAD_NEW;
    }
    if (ReplUtils.replCkptStatus(table.getDbName(), table.getParameters(), context.dumpDirectory)) {
      return ReplLoadOpType.LOAD_SKIP;
    }
    return ReplLoadOpType.LOAD_REPLACE;
  }

  private void newTableTasks(ImportTableDesc tblDesc, Task<?> tblRootTask) throws Exception {
    Table table = tblDesc.toTable(context.hiveConf);
    ReplicationSpec replicationSpec = event.replicationSpec();
    Task<?> createTableTask =
        tblDesc.getCreateTableTask(new HashSet<>(), new HashSet<>(), context.hiveConf);
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
              replicationSpec.getValidWriteIdList(), ReplTxnWork.OperationType.REPL_WRITEID_STATE);
      Task<?> replTxnTask = TaskFactory.get(replTxnWork, context.hiveConf);
      parentTask.addDependentTask(replTxnTask);
      parentTask = replTxnTask;
    }
    if (!isPartitioned(tblDesc)) {
      LOG.debug("adding dependent ReplTxnTask/CopyWork/MoveWork for table");
      Task<?> loadTableTask =
          loadTableTask(table, replicationSpec, new Path(tblDesc.getLocation()),
              event.metadataPath());
      parentTask.addDependentTask(loadTableTask);
    }
    tracker.addTask(tblRootTask);
  }

  private String location(ImportTableDesc tblDesc, Database parentDb)
      throws MetaException, SemanticException {
    if (!tableContext.waitOnPrecursor()) {
      return context.warehouse.getDefaultTablePath(
          parentDb, tblDesc.getTableName(), tblDesc.isExternal()).toString();
    } else {
      Path tablePath = context.warehouse.getDefaultTablePath(
          tblDesc.getDatabaseName(), tblDesc.getTableName(), tblDesc.isExternal());
      return context.warehouse.getDnsPath(tablePath).toString();
    }
  }

  private Task<?> loadTableTask(Table table, ReplicationSpec replicationSpec, Path tgtPath,
      Path fromURI) {
    Path dataPath = new Path(fromURI, EximUtil.DATA_PATH_NAME);
    Path tmpPath = PathUtils.getExternalTmpPath(tgtPath, context.pathInfo);
    Task<?> copyTask =
        ReplCopyTask.getLoadCopyTask(replicationSpec, dataPath, tmpPath, context.hiveConf);

    MoveWork moveWork = new MoveWork(new HashSet<>(), new HashSet<>(), null, null, false);
    if (AcidUtils.isTransactionalTable(table)) {
      LoadMultiFilesDesc loadFilesWork = new LoadMultiFilesDesc(
              Collections.singletonList(tmpPath),
              Collections.singletonList(tgtPath),
              true, null, null);
      moveWork.setMultiFilesDesc(loadFilesWork);
    } else {
      LoadTableDesc loadTableWork = new LoadTableDesc(
              tmpPath, Utilities.getTableDesc(table), new TreeMap<>(),
              replicationSpec.isReplace() ? LoadFileType.REPLACE_ALL : LoadFileType.OVERWRITE_EXISTING, 0L
      );
      moveWork.setLoadTableWork(loadTableWork);
    }
    Task<?> loadTableTask = TaskFactory.get(moveWork, context.hiveConf);
    copyTask.addDependentTask(loadTableTask);
    return copyTask;
  }

  private Task<?> dropTableTask(Table table) {
    assert(table != null);
    DropTableDesc dropTblDesc = new DropTableDesc(table.getFullyQualifiedName(), table.getTableType(),
            true, false, event.replicationSpec());
    return TaskFactory.get(new DDLWork(new HashSet<>(), new HashSet<>(), dropTblDesc), context.hiveConf);
  }
}
