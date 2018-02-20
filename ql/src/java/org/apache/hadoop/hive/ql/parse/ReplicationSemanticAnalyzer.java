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
package org.apache.hadoop.hive.ql.parse;

import org.antlr.runtime.tree.Tree;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.ReplDumpWork;
import org.apache.hadoop.hive.ql.exec.repl.ReplStateLogWork;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.ReplLoadWork;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;
import org.apache.hadoop.hive.ql.parse.repl.load.EventDumpDirComparator;
import org.apache.hadoop.hive.ql.parse.repl.load.UpdatedMetaDataTracker;
import org.apache.hadoop.hive.ql.parse.repl.load.log.IncrementalLoadLogger;
import org.apache.hadoop.hive.ql.parse.repl.load.message.MessageHandler;
import org.apache.hadoop.hive.ql.plan.AlterDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.AlterTableDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DependencyCollectionWork;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_DBNAME;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_LIMIT;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_REPL_CONFIG;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_REPL_DUMP;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_REPL_LOAD;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_REPL_STATUS;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TABNAME;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TO;

public class ReplicationSemanticAnalyzer extends BaseSemanticAnalyzer {
  // Database name or pattern
  private String dbNameOrPattern;
  // Table name or pattern
  private String tblNameOrPattern;
  private Long eventFrom;
  private Long eventTo;
  private Integer maxEventLimit;
  // Base path for REPL LOAD
  private String path;
  // Added conf member to set the REPL command specific config entries without affecting the configs
  // of any other queries running in the session
  private HiveConf conf;

  // This will be set to true only if repl-status is fired with "WITH" keyword
  private boolean needNewdb;

  private static String testInjectDumpDir = null; // unit tests can overwrite this to affect default dump behaviour
  private static final String dumpSchema = "dump_dir,last_repl_id#string,string";

  public static final String FUNCTIONS_ROOT_DIR_NAME = "_functions";
  public static final String CONSTRAINTS_ROOT_DIR_NAME = "_constraints";

  ReplicationSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
    this.conf = new HiveConf(super.conf);
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    LOG.debug("ReplicationSemanticAanalyzer: analyzeInternal");
    LOG.debug(ast.getName() + ":" + ast.getToken().getText() + "=" + ast.getText());
    switch (ast.getToken().getType()) {
      case TOK_REPL_DUMP: {
        LOG.debug("ReplicationSemanticAnalyzer: analyzeInternal: dump");
        initReplDump(ast);
        analyzeReplDump(ast);
        break;
      }
      case TOK_REPL_LOAD: {
        LOG.debug("ReplicationSemanticAnalyzer: analyzeInternal: load");
        initReplLoad(ast);
        analyzeReplLoad(ast);
        break;
      }
      case TOK_REPL_STATUS: {
        LOG.debug("ReplicationSemanticAnalyzer: analyzeInternal: status");
        initReplStatus(ast);
        analyzeReplStatus(ast);
        break;
      }
      default: {
        throw new SemanticException("Unexpected root token");
      }
    }
  }

  private void initReplDump(ASTNode ast) {
    int numChildren = ast.getChildCount();
    dbNameOrPattern = PlanUtils.stripQuotes(ast.getChild(0).getText());
    // skip the first node, which is always required
    int currNode = 1;
    while (currNode < numChildren) {
      if (ast.getChild(currNode).getType() == TOK_REPL_CONFIG) {
        Map<String, String> replConfigs
            = DDLSemanticAnalyzer.getProps((ASTNode) ast.getChild(currNode).getChild(0));
        if (null != replConfigs) {
          for (Map.Entry<String, String> config : replConfigs.entrySet()) {
            conf.set(config.getKey(), config.getValue());
          }
        }
      } else if (ast.getChild(currNode).getType() == TOK_TABNAME) {
        // optional tblName was specified.
        tblNameOrPattern = PlanUtils.stripQuotes(ast.getChild(currNode).getChild(0).getText());
      } else {
        // TOK_FROM subtree
        Tree fromNode = ast.getChild(currNode);
        eventFrom = Long.parseLong(PlanUtils.stripQuotes(fromNode.getChild(0).getText()));
        // skip the first, which is always required
        int numChild = 1;
        while (numChild < fromNode.getChildCount()) {
          if (fromNode.getChild(numChild).getType() == TOK_TO) {
            eventTo =
                Long.parseLong(PlanUtils.stripQuotes(fromNode.getChild(numChild + 1).getText()));
            // skip the next child, since we already took care of it
            numChild++;
          } else if (fromNode.getChild(numChild).getType() == TOK_LIMIT) {
            maxEventLimit =
                Integer.parseInt(PlanUtils.stripQuotes(fromNode.getChild(numChild + 1).getText()));
            // skip the next child, since we already took care of it
            numChild++;
          }
          // move to the next child in FROM tree
          numChild++;
        }
      }
      // move to the next root node
      currNode++;
    }
  }

  // REPL DUMP
  private void analyzeReplDump(ASTNode ast) throws SemanticException {
    LOG.debug("ReplicationSemanticAnalyzer.analyzeReplDump: " + String.valueOf(dbNameOrPattern)
        + "." + String.valueOf(tblNameOrPattern) + " from " + String.valueOf(eventFrom) + " to "
        + String.valueOf(eventTo) + " maxEventLimit " + String.valueOf(maxEventLimit));
    try {
      ctx.setResFile(ctx.getLocalTmpPath());
      Task<ReplDumpWork> replDumpWorkTask = TaskFactory
          .get(new ReplDumpWork(
              dbNameOrPattern,
              tblNameOrPattern,
              eventFrom,
              eventTo,
              ErrorMsg.INVALID_PATH.getMsg(ast),
              maxEventLimit,
              ctx.getResFile().toUri().toString()
          ), conf, true);
      rootTasks.add(replDumpWorkTask);
      if (dbNameOrPattern != null) {
        for (String dbName : Utils.matchesDb(db, dbNameOrPattern)) {
          if (tblNameOrPattern != null) {
            for (String tblName : Utils.matchesTbl(db, dbName, tblNameOrPattern)) {
              inputs.add(new ReadEntity(db.getTable(dbName, tblName)));
            }
          } else {
            inputs.add(new ReadEntity(db.getDatabase(dbName)));
          }
        }
      }
      setFetchTask(createFetchTask(dumpSchema));
    } catch (Exception e) {
      // TODO : simple wrap & rethrow for now, clean up with error codes
      LOG.warn("Error during analyzeReplDump", e);
      throw new SemanticException(e);
    }
  }

  // REPL LOAD
  private void initReplLoad(ASTNode ast) throws SemanticException {
    path = PlanUtils.stripQuotes(ast.getChild(0).getText());
    int numChildren = ast.getChildCount();
    for (int i = 1; i < numChildren; i++) {
      ASTNode childNode = (ASTNode) ast.getChild(i);
      switch (childNode.getToken().getType()) {
        case TOK_DBNAME:
          dbNameOrPattern = PlanUtils.stripQuotes(childNode.getChild(0).getText());
          break;
        case TOK_TABNAME:
          tblNameOrPattern = PlanUtils.stripQuotes(childNode.getChild(0).getText());
          break;
        case TOK_REPL_CONFIG:
          Map<String, String> replConfigs
                  = DDLSemanticAnalyzer.getProps((ASTNode) childNode.getChild(0));
          if (null != replConfigs) {
            for (Map.Entry<String, String> config : replConfigs.entrySet()) {
              conf.set(config.getKey(), config.getValue());
            }
          }
          break;
        default:
          throw new SemanticException("Unrecognized token in REPL LOAD statement");
      }
    }
  }

  /*
   * Example dump dirs we need to be able to handle :
   *
   * for: hive.repl.rootdir = staging/
   * Then, repl dumps will be created in staging/<dumpdir>
   *
   * single-db-dump: staging/blah12345 will contain a db dir for the db specified
   *  blah12345/
   *   default/
   *    _metadata
   *    tbl1/
   *      _metadata
   *      dt=20160907/
   *        _files
   *    tbl2/
   *    tbl3/
   *    unptn_tbl/
   *      _metadata
   *      _files
   *
   * multi-db-dump: staging/bar12347 will contain dirs for each db covered
   * staging/
   *  bar12347/
   *   default/
   *     ...
   *   sales/
   *     ...
   *
   * single table-dump: staging/baz123 will contain a table object dump inside
   * staging/
   *  baz123/
   *    _metadata
   *    dt=20150931/
   *      _files
   *
   * incremental dump : staging/blue123 will contain dirs for each event inside.
   * staging/
   *  blue123/
   *    34/
   *    35/
   *    36/
   */
  private void analyzeReplLoad(ASTNode ast) throws SemanticException {
    LOG.debug("ReplSemanticAnalyzer.analyzeReplLoad: " + String.valueOf(dbNameOrPattern) + "."
        + String.valueOf(tblNameOrPattern) + " from " + String.valueOf(path));

    // for analyze repl load, we walk through the dir structure available in the path,
    // looking at each db, and then each table, and then setting up the appropriate
    // import job in its place.

    try {

      Path loadPath = new Path(path);
      final FileSystem fs = loadPath.getFileSystem(conf);

      if (!fs.exists(loadPath)) {
        // supposed dump path does not exist.
        throw new FileNotFoundException(loadPath.toUri().toString());
      }

      // Now, the dumped path can be one of three things:
      // a) It can be a db dump, in which case we expect a set of dirs, each with a
      // db name, and with a _metadata file in each, and table dirs inside that.
      // b) It can be a table dump dir, in which case we expect a _metadata dump of
      // a table in question in the dir, and individual ptn dir hierarchy.
      // c) A dump can be an incremental dump, which means we have several subdirs
      // each of which have the evid as the dir name, and each of which correspond
      // to a event-level dump. Currently, only CREATE_TABLE and ADD_PARTITION are
      // handled, so all of these dumps will be at a table/ptn level.

      // For incremental repl, we will have individual events which can
      // be other things like roles and fns as well.
      // At this point, all dump dirs should contain a _dumpmetadata file that
      // tells us what is inside that dumpdir.

      DumpMetaData dmd = new DumpMetaData(loadPath, conf);

      boolean evDump = false;
      if (dmd.isIncrementalDump()){
        LOG.debug("{} contains an incremental dump", loadPath);
        evDump = true;
      } else {
        LOG.debug("{} contains an bootstrap dump", loadPath);
      }

      if ((!evDump) && (tblNameOrPattern != null) && !(tblNameOrPattern.isEmpty())) {
        ReplLoadWork replLoadWork =
            new ReplLoadWork(conf, loadPath.toString(), dbNameOrPattern, tblNameOrPattern,
                queryState.getLineageState(), SessionState.get().getTxnMgr().getCurrentTxnId());
        rootTasks.add(TaskFactory.get(replLoadWork, conf, true));
        return;
      }

      FileStatus[] srcs = LoadSemanticAnalyzer.matchFilesOrDir(fs, loadPath);
      if (srcs == null || (srcs.length == 0)) {
        LOG.warn("Nothing to load at {}", loadPath.toUri().toString());
        return;
      }

      FileStatus[] dirsInLoadPath = fs.listStatus(loadPath, EximUtil.getDirectoryFilter(fs));

      if ((dirsInLoadPath == null) || (dirsInLoadPath.length == 0)) {
        throw new IllegalArgumentException("No data to load in path " + loadPath.toUri().toString());
      }

      if (!evDump){
        // not an event dump, not a table dump - thus, a db dump
        if ((dbNameOrPattern != null) && (dirsInLoadPath.length > 1)) {
          LOG.debug("Found multiple dirs when we expected 1:");
          for (FileStatus d : dirsInLoadPath) {
            LOG.debug("> " + d.getPath().toUri().toString());
          }
          throw new IllegalArgumentException(
              "Multiple dirs in "
                  + loadPath.toUri().toString()
                  + " does not correspond to REPL LOAD expecting to load to a singular destination point.");
        }

        ReplLoadWork replLoadWork = new ReplLoadWork(conf, loadPath.toString(), dbNameOrPattern,
            queryState.getLineageState(), SessionState.get().getTxnMgr().getCurrentTxnId());
        rootTasks.add(TaskFactory.get(replLoadWork, conf, true));
        //
        //        for (FileStatus dir : dirsInLoadPath) {
        //          analyzeDatabaseLoad(dbNameOrPattern, fs, dir);
        //        }
      } else {
        // Event dump, each sub-dir is an individual event dump.
        // We need to guarantee that the directory listing we got is in order of evid.
        Arrays.sort(dirsInLoadPath, new EventDumpDirComparator());

        Task<? extends Serializable> evTaskRoot = TaskFactory.get(new DependencyCollectionWork(), conf);
        Task<? extends Serializable> taskChainTail = evTaskRoot;

        ReplLogger replLogger = new IncrementalLoadLogger(dbNameOrPattern,
                loadPath.toString(), dirsInLoadPath.length);

        for (FileStatus dir : dirsInLoadPath){
          LOG.debug("Loading event from {} to {}.{}", dir.getPath().toUri(), dbNameOrPattern, tblNameOrPattern);

          // event loads will behave similar to table loads, with one crucial difference
          // precursor order is strict, and each event must be processed after the previous one.
          // The way we handle this strict order is as follows:
          // First, we start with a taskChainTail which is a dummy noop task (a DependecyCollectionTask)
          // at the head of our event chain. For each event we process, we tell analyzeTableLoad to
          // create tasks that use the taskChainTail as a dependency. Then, we collect all those tasks
          // and introduce a new barrier task(also a DependencyCollectionTask) which depends on all
          // these tasks. Then, this barrier task becomes our new taskChainTail. Thus, we get a set of
          // tasks as follows:
          //
          //                 --->ev1.task1--                          --->ev2.task1--
          //                /               \                        /               \
          //  evTaskRoot-->*---->ev1.task2---*--> ev1.barrierTask-->*---->ev2.task2---*->evTaskChainTail
          //                \               /
          //                 --->ev1.task3--
          //
          // Once this entire chain is generated, we add evTaskRoot to rootTasks, so as to execute the
          // entire chain

          String locn = dir.getPath().toUri().toString();
          DumpMetaData eventDmd = new DumpMetaData(new Path(locn), conf);
          MessageHandler.Context context = new MessageHandler.Context(dbNameOrPattern,
                                                          tblNameOrPattern, locn, taskChainTail,
                                                          eventDmd, conf, db, ctx, LOG);
          List<Task<? extends Serializable>> evTasks = analyzeEventLoad(context);

          if ((evTasks != null) && (!evTasks.isEmpty())){
            ReplStateLogWork replStateLogWork = new ReplStateLogWork(replLogger,
                                                          dir.getPath().getName(),
                                                          eventDmd.getDumpType().toString());
            Task<? extends Serializable> barrierTask = TaskFactory.get(replStateLogWork, conf);
            for (Task<? extends Serializable> t : evTasks){
              t.addDependentTask(barrierTask);
              LOG.debug("Added {}:{} as a precursor of barrier task {}:{}",
                  t.getClass(), t.getId(), barrierTask.getClass(), barrierTask.getId());
            }
            LOG.debug("Updated taskChainTail from {}:{} to {}:{}",
                taskChainTail.getClass(), taskChainTail.getId(), barrierTask.getClass(), barrierTask.getId());
            taskChainTail = barrierTask;
          }
        }

        // If any event is there and db name is known, then dump the start and end logs
        if (!evTaskRoot.equals(taskChainTail)) {
          Map<String, String> dbProps = new HashMap<>();
          dbProps.put(ReplicationSpec.KEY.CURR_STATE_ID.toString(), String.valueOf(dmd.getEventTo()));
          ReplStateLogWork replStateLogWork = new ReplStateLogWork(replLogger, dbProps);
          Task<? extends Serializable> barrierTask = TaskFactory.get(replStateLogWork, conf);
          taskChainTail.addDependentTask(barrierTask);
          LOG.debug("Added {}:{} as a precursor of barrier task {}:{}",
                  taskChainTail.getClass(), taskChainTail.getId(),
                  barrierTask.getClass(), barrierTask.getId());

          replLogger.startLog();
        }
        rootTasks.add(evTaskRoot);
      }

    } catch (Exception e) {
      // TODO : simple wrap & rethrow for now, clean up with error codes
      throw new SemanticException(e);
    }
  }

  private List<Task<? extends Serializable>> analyzeEventLoad(
          MessageHandler.Context context)
      throws SemanticException {
    MessageHandler messageHandler = context.dmd.getDumpType().handler();
    List<Task<? extends Serializable>> tasks = messageHandler.handle(context);

    if (context.precursor != null) {
      for (Task<? extends Serializable> t : tasks) {
        context.precursor.addDependentTask(t);
        LOG.debug("Added {}:{} as a precursor of {}:{}",
                context.precursor.getClass(), context.precursor.getId(), t.getClass(), t.getId());
      }
    }

    inputs.addAll(messageHandler.readEntities());
    outputs.addAll(messageHandler.writeEntities());
    return addUpdateReplStateTasks(StringUtils.isEmpty(context.tableName),
                            messageHandler.getUpdatedMetadata(), tasks);
  }

  private Task<? extends Serializable> tableUpdateReplStateTask(
                                                        String dbName,
                                                        String tableName,
                                                        Map<String, String> partSpec,
                                                        String replState,
                                                        Task<? extends Serializable> preCursor) {
    HashMap<String, String> mapProp = new HashMap<>();
    mapProp.put(ReplicationSpec.KEY.CURR_STATE_ID.toString(), replState);

    AlterTableDesc alterTblDesc =  new AlterTableDesc(
            AlterTableDesc.AlterTableTypes.ADDPROPS, new ReplicationSpec(replState, replState));
    alterTblDesc.setProps(mapProp);
    alterTblDesc.setOldName(StatsUtils.getFullyQualifiedTableName(dbName, tableName));
    alterTblDesc.setPartSpec((HashMap<String, String>)partSpec);

    Task<? extends Serializable> updateReplIdTask = TaskFactory.get(
                      new DDLWork(inputs, outputs, alterTblDesc), conf, true);

    // Link the update repl state task with dependency collection task
    if (preCursor != null) {
      preCursor.addDependentTask(updateReplIdTask);
      LOG.debug("Added {}:{} as a precursor of {}:{}",
              preCursor.getClass(), preCursor.getId(),
              updateReplIdTask.getClass(), updateReplIdTask.getId());
    }
    return updateReplIdTask;
  }

  private Task<? extends Serializable> dbUpdateReplStateTask(
                                                        String dbName,
                                                        String replState,
                                                        Task<? extends Serializable> preCursor) {
    HashMap<String, String> mapProp = new HashMap<>();
    mapProp.put(ReplicationSpec.KEY.CURR_STATE_ID.toString(), replState);

    AlterDatabaseDesc alterDbDesc = new AlterDatabaseDesc(
                            dbName, mapProp, new ReplicationSpec(replState, replState));
    Task<? extends Serializable> updateReplIdTask = TaskFactory.get(
                            new DDLWork(inputs, outputs, alterDbDesc), conf, true);

    // Link the update repl state task with dependency collection task
    if (preCursor != null) {
      preCursor.addDependentTask(updateReplIdTask);
      LOG.debug("Added {}:{} as a precursor of {}:{}",
              preCursor.getClass(), preCursor.getId(),
              updateReplIdTask.getClass(), updateReplIdTask.getId());
    }
    return updateReplIdTask;
  }

  private List<Task<? extends Serializable>> addUpdateReplStateTasks(
          boolean isDatabaseLoad,
          UpdatedMetaDataTracker updatedMetadata,
          List<Task<? extends Serializable>> importTasks) {
    String replState = updatedMetadata.getReplicationState();
    String dbName = updatedMetadata.getDatabase();
    String tableName = updatedMetadata.getTable();

    // If no import tasks generated by the event or no table updated for table level load, then no
    // need to update the repl state to any object.
    if (importTasks.isEmpty() || (!isDatabaseLoad && (tableName == null))) {
      LOG.debug("No objects need update of repl state: Either 0 import tasks or table level load");
      return importTasks;
    }

    // Create a barrier task for dependency collection of import tasks
    Task<? extends Serializable> barrierTask = TaskFactory.get(new DependencyCollectionWork(), conf);

    // Link import tasks to the barrier task which will in-turn linked with repl state update tasks
    for (Task<? extends Serializable> t : importTasks){
      t.addDependentTask(barrierTask);
      LOG.debug("Added {}:{} as a precursor of barrier task {}:{}",
              t.getClass(), t.getId(), barrierTask.getClass(), barrierTask.getId());
    }

    List<Task<? extends Serializable>> tasks = new ArrayList<>();
    Task<? extends Serializable> updateReplIdTask;

    // If any partition is updated, then update repl state in partition object
    for (final Map<String, String> partSpec : updatedMetadata.getPartitions()) {
      updateReplIdTask = tableUpdateReplStateTask(dbName, tableName, partSpec, replState, barrierTask);
      tasks.add(updateReplIdTask);
    }

    if (tableName != null) {
      // If any table/partition is updated, then update repl state in table object
      updateReplIdTask = tableUpdateReplStateTask(dbName, tableName, null, replState, barrierTask);
      tasks.add(updateReplIdTask);
    }

    // For table level load, need not update replication state for the database
    if (isDatabaseLoad) {
      // If any table/partition is updated, then update repl state in db object
      updateReplIdTask = dbUpdateReplStateTask(dbName, replState, barrierTask);
      tasks.add(updateReplIdTask);
    }

    // At least one task would have been added to update the repl state
    return tasks;
  }

  // REPL STATUS
  private void initReplStatus(ASTNode ast) throws SemanticException{
    needNewdb = false;
    dbNameOrPattern = PlanUtils.stripQuotes(ast.getChild(0).getText());
    int numChildren = ast.getChildCount();
    for (int i = 1; i < numChildren; i++) {
      ASTNode childNode = (ASTNode) ast.getChild(i);
      switch (childNode.getToken().getType()) {
      case TOK_TABNAME:
        tblNameOrPattern = PlanUtils.stripQuotes(childNode.getChild(0).getText());
        break;
      case TOK_REPL_CONFIG:
        Map<String, String> replConfigs
            = DDLSemanticAnalyzer.getProps((ASTNode) childNode.getChild(0));
        if (null != replConfigs) {
          for (Map.Entry<String, String> config : replConfigs.entrySet()) {
            conf.set(config.getKey(), config.getValue());
          }
        }
        needNewdb = true;
        break;
      default:
        throw new SemanticException("Unrecognized token in REPL STATUS statement");
      }
    }
  }

  private void analyzeReplStatus(ASTNode ast) throws SemanticException {
    LOG.debug("ReplicationSemanticAnalyzer.analyzeReplStatus: " + String.valueOf(dbNameOrPattern)
        + "." + String.valueOf(tblNameOrPattern));

    String replLastId = null;

    try {
      Hive newDb;
      if (needNewdb) {
        newDb = Hive.get(conf, false);
      } else {
        newDb = db;
      }

      if (tblNameOrPattern != null) {
        // Checking for status of table
        Table tbl = newDb.getTable(dbNameOrPattern, tblNameOrPattern);
        if (tbl != null) {
          inputs.add(new ReadEntity(tbl));
          Map<String, String> params = tbl.getParameters();
          if (params != null && (params.containsKey(ReplicationSpec.KEY.CURR_STATE_ID.toString()))) {
            replLastId = params.get(ReplicationSpec.KEY.CURR_STATE_ID.toString());
          }
        }
      } else {
        // Checking for status of a db

        Database database = newDb.getDatabase(dbNameOrPattern);
        if (database != null) {
          inputs.add(new ReadEntity(database));
          Map<String, String> params = database.getParameters();
          if (params != null && (params.containsKey(ReplicationSpec.KEY.CURR_STATE_ID.toString()))) {
            replLastId = params.get(ReplicationSpec.KEY.CURR_STATE_ID.toString());
          }
        }
      }
    } catch (HiveException e) {
      throw new SemanticException(e); // TODO : simple wrap & rethrow for now, clean up with error
                                      // codes
    }

    prepareReturnValues(Collections.singletonList(replLastId), "last_repl_id#string");
    setFetchTask(createFetchTask("last_repl_id#string"));
    LOG.debug("ReplicationSemanticAnalyzer.analyzeReplStatus: writing repl.last.id={} out to {}",
        String.valueOf(replLastId), ctx.getResFile(), conf);
  }

  private void prepareReturnValues(List<String> values, String schema) throws SemanticException {
    LOG.debug("prepareReturnValues : " + schema);
    for (String s : values) {
      LOG.debug("    > " + s);
    }
    ctx.setResFile(ctx.getLocalTmpPath());
    Utils.writeOutput(values, ctx.getResFile(), conf);
  }
}
