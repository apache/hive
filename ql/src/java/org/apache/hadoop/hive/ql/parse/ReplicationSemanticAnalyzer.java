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
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.common.repl.ReplScope;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.ReplAck;
import org.apache.hadoop.hive.ql.exec.repl.ReplDumpWork;
import org.apache.hadoop.hive.ql.exec.repl.ReplLoadWork;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;
import org.apache.hadoop.hive.ql.parse.repl.load.metric.BootstrapLoadMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.load.metric.IncrementalLoadMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.load.metric.OptimizedBootstrapLoadMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.load.metric.PreOptimizedBootstrapLoadMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Status;
import org.apache.hadoop.hive.ql.plan.PlanUtils;

import java.io.IOException;
import java.util.Map;
import java.util.List;
import java.util.Collections;
import java.util.Objects;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_QUERY_ID;
import static org.apache.hadoop.hive.ql.exec.repl.ReplAck.LOAD_ACKNOWLEDGEMENT;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_DBNAME;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_REPL_CONFIG;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_REPL_DUMP;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_REPL_LOAD;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_REPL_STATUS;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_REPL_TABLES;

public class ReplicationSemanticAnalyzer extends BaseSemanticAnalyzer {
  // Replication Scope
  private ReplScope replScope = new ReplScope();

  // Source DB Name for REPL LOAD
  private String sourceDbNameOrPattern;
  // Added conf member to set the REPL command specific config entries without affecting the configs
  // of any other queries running in the session
  private HiveConf conf;

  // By default, this will be same as that of super class BaseSemanticAnalyzer. But need to obtain again
  // if the Hive configs are received from WITH clause in REPL LOAD or REPL STATUS commands.
  private Hive db;

  private static final String dumpSchema = "dump_dir,last_repl_id#string,string";

  ReplicationSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
    this.db = super.db;
    this.conf = new HiveConf(super.conf);
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    LOG.debug("ReplicationSemanticAnalyzer: analyzeInternal");
    LOG.debug(ast.getName() + ":" + ast.getToken().getText() + "=" + ast.getText());
    // Some of the txn related configs were not set when ReplicationSemanticAnalyzer.conf was initialized.
    // It should be set first.
    setTxnConfigs();
    switch (ast.getToken().getType()) {
      case TOK_REPL_DUMP: {
        LOG.debug("ReplicationSemanticAnalyzer: analyzeInternal: dump");
        try {
          analyzeReplDump(ast);
        } catch (SemanticException e) {
            ReplUtils.reportStatusInReplicationMetrics("REPL_DUMP", ReplUtils.isErrorRecoverable(e)
                    ? Status.FAILED_ADMIN : Status.FAILED, null, conf, null, null);
          throw e;
        }
        break;
      }
      case TOK_REPL_LOAD: {
        LOG.debug("ReplicationSemanticAnalyzer: analyzeInternal: load");
        try {
          analyzeReplLoad(ast);
        } catch (SemanticException e) {
          if (!e.getMessage().equals(ErrorMsg.REPL_FAILED_WITH_NON_RECOVERABLE_ERROR.getMsg())) {
            ReplUtils.reportStatusInReplicationMetrics("REPL_LOAD", ReplUtils.isErrorRecoverable(e)
                    ? Status.FAILED_ADMIN : Status.FAILED, null, conf,  null, null);
          }
          throw e;
        }
        break;
      }
      case TOK_REPL_STATUS: {
        LOG.debug("ReplicationSemanticAnalyzer: analyzeInternal: status");
        analyzeReplStatus(ast);
        break;
      }
      default: {
        throw new SemanticException("Unexpected root token");
      }
    }
  }

  private void setTxnConfigs() {
    String validTxnList = queryState.getValidTxnList();
    if (validTxnList != null) {
      conf.set(ValidTxnList.VALID_TXNS_KEY, validTxnList);
    }
  }

  private void setReplDumpTablesList(Tree replTablesNode, ReplScope replScope) throws HiveException {
    int childCount = replTablesNode.getChildCount();
    assert(childCount <= 2);

    // Traverse the children which can be either just include tables list or both include
    // and exclude tables lists.
    String replScopeType = (replScope == this.replScope) ? "Current" : "Old";
    for (int listIdx = 0; listIdx < childCount; listIdx++) {
      String tableList = unescapeSQLString(replTablesNode.getChild(listIdx).getText());
      if (tableList.isEmpty()) {
        throw new SemanticException(ErrorMsg.REPL_INVALID_DB_OR_TABLE_PATTERN);
      }
      if (listIdx == 0) {
        LOG.info("{} ReplScope: Set Included Tables List: {}", replScopeType, tableList);
        replScope.setIncludedTablePatterns(tableList);
      } else {
        LOG.info("{} ReplScope: Set Excluded Tables List: {}", replScopeType, tableList);
        replScope.setExcludedTablePatterns(tableList);
      }
    }
  }

  private void initReplDump(ASTNode ast) throws HiveException {
    int numChildren = ast.getChildCount();

    String dbNameOrPattern = PlanUtils.stripQuotes(ast.getChild(0).getText());
    LOG.info("Current ReplScope: Set DB Name: {}", dbNameOrPattern);
    replScope.setDbName(dbNameOrPattern);

    // Skip the first node, which is always required
    int childIdx = 1;
    while (childIdx < numChildren) {
      Tree currNode = ast.getChild(childIdx);
      switch (currNode.getType()) {
      case TOK_REPL_CONFIG:
        Map<String, String> replConfigs = getProps((ASTNode) currNode.getChild(0));
        for (Map.Entry<String, String> config : replConfigs.entrySet()) {
          conf.set(config.getKey(), config.getValue());
        }
        break;
      case TOK_REPL_TABLES:
        setReplDumpTablesList(currNode, replScope);
        break;
      default:
        throw new SemanticException("Unrecognized token " + currNode.getType() + " in REPL DUMP statement.");
      }
      // Move to the next root node
      childIdx++;
    }

    List<String> databases = Utils.matchesDb(db, dbNameOrPattern);
    if (databases.size() == 0) {
      throw new SemanticException(ErrorMsg.REPL_SOURCE_DATABASE_NOT_FOUND.format(dbNameOrPattern));
    }
    for (String dbName : databases) {
      Database database = db.getDatabase(dbName);
      if (database != null) {
        Map<String, String> dbParams = database.getParameters();
        if (MetaStoreUtils.isTargetOfReplication(database)) {
          LOG.info("Triggering optimized bootstrap for database {} since it " +
                          "is marked as target of replication (repl.target.for) " +
                          "with {} set to {}",
                  dbName, ReplConst.REPL_FAILOVER_ENDPOINT,
                  Objects.nonNull(dbParams) ?
                          dbParams.get(ReplConst.REPL_FAILOVER_ENDPOINT)
                          : null
          );
        }
      } else {
        throw new SemanticException("Cannot dump database " + dbName + " as it does not exist");
      }
    }
  }

  // REPL DUMP
  private void analyzeReplDump(ASTNode ast) throws SemanticException {
    try {
      initReplDump(ast);
    } catch (HiveException e) {
      throw new SemanticException(e.getMessage(), e);
    }

    try {
      ctx.setResFile(ctx.getLocalTmpPath());
      Task<ReplDumpWork> replDumpWorkTask = TaskFactory
          .get(new ReplDumpWork(
              replScope,
              ASTErrorUtils.getMsg(ErrorMsg.INVALID_PATH.getMsg(), ast),
              ctx.getResFile().toUri().toString()
      ), conf);
      rootTasks.add(replDumpWorkTask);
      for (String dbName : Utils.matchesDb(db, replScope.getDbName())) {
        if (!replScope.includeAllTables()) {
          for (String tblName : Utils.matchesTbl(db, dbName, replScope)) {
            inputs.add(new ReadEntity(db.getTable(dbName, tblName)));
          }
        } else {
          inputs.add(new ReadEntity(db.getDatabase(dbName)));
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
  private void initReplLoad(ASTNode ast) throws HiveException {
    sourceDbNameOrPattern = PlanUtils.stripQuotes(ast.getChild(0).getText());
    int numChildren = ast.getChildCount();
    for (int i = 1; i < numChildren; i++) {
      ASTNode childNode = (ASTNode) ast.getChild(i);
      switch (childNode.getToken().getType()) {
      case TOK_DBNAME:
        replScope.setDbName(PlanUtils.stripQuotes(childNode.getChild(0).getText()));
        break;
      case TOK_REPL_CONFIG:
        setConfigs((ASTNode) childNode.getChild(0));
        break;
      case TOK_REPL_TABLES: //Accept TOK_REPL_TABLES for table level repl.Needn't do anything as dump path needs db only
        break;
        default:
          throw new SemanticException("Unrecognized token in REPL LOAD statement.");
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
    try {
      initReplLoad(ast);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }

    // For analyze repl load, we walk through the dir structure available in the path,
    // looking at each db, and then each table, and then setting up the appropriate
    // import job in its place.
    try {
      Objects.requireNonNull(sourceDbNameOrPattern, "REPL LOAD Source database name shouldn't be null");
      Objects.requireNonNull(replScope.getDbName(), "REPL LOAD Target database name shouldn't be null");
      Path loadPath = getCurrentLoadPath();

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

      //If repl status of target is greater than dumps, don't do anything as the load for the latest dump is done
      Path latestDumpPath = ReplUtils.getLatestDumpPath(ReplUtils
              .getEncodedDumpRootPath(conf, sourceDbNameOrPattern.toLowerCase()), conf);
      if (ReplUtils.failedWithNonRecoverableError(latestDumpPath, conf)) {
        Path nonRecoverableFile = new Path(latestDumpPath, ReplAck.NON_RECOVERABLE_MARKER.toString());
        ReplUtils.reportStatusInReplicationMetrics("REPL_LOAD", Status.SKIPPED,
                nonRecoverableFile.toString(), conf,  sourceDbNameOrPattern, null);
        throw new Exception(ErrorMsg.REPL_FAILED_WITH_NON_RECOVERABLE_ERROR.getMsg());
      }
      if (loadPath != null) {
        DumpMetaData dmd = new DumpMetaData(loadPath, conf);

        boolean evDump = false;
        // we will decide what hdfs locations needs to be copied over here as well.
        if (dmd.isIncrementalDump() || dmd.isOptimizedBootstrapDump() || dmd.isPreOptimizedBootstrapDump()) {
          LOG.debug("{} contains an incremental / Optimized bootstrap dump", loadPath);
          evDump = true;
        } else {
          LOG.debug("{} contains an bootstrap dump", loadPath);
        }

        ReplicationMetricCollector metricCollector = initReplicationLoadMetricCollector(loadPath.toString(), replScope.getDbName(), dmd);
        ReplLoadWork replLoadWork = new ReplLoadWork(conf, loadPath.toString(), sourceDbNameOrPattern,
                replScope.getDbName(),
                dmd.getReplScope(),
                queryState.getLineageState(), evDump, dmd.getEventTo(), dmd.getDumpExecutionId(),
                metricCollector, dmd.isReplScopeModified());
        rootTasks.add(TaskFactory.get(replLoadWork, conf));
        if (dmd.isPreOptimizedBootstrapDump()) {
          dmd.setOptimizedBootstrapToDumpMetadataFile(conf.getLong(Constants.SCHEDULED_QUERY_EXECUTIONID, 0L));
        }
      } else {
        ReplUtils.reportStatusInReplicationMetrics("REPL_LOAD", Status.SKIPPED, null, conf,  sourceDbNameOrPattern, null);
        LOG.warn("No dump to load or the previous dump already loaded");
      }
    } catch (Exception e) {
      // TODO : simple wrap & rethrow for now, clean up with error codes
      throw new SemanticException(e.getMessage(), e);
    }
  }

  private ReplicationMetricCollector initReplicationLoadMetricCollector(String dumpDirectory, String dbNameToLoadIn,
                                                                        DumpMetaData dmd) throws SemanticException {
    ReplicationMetricCollector collector;
    if (dmd.isPreOptimizedBootstrapDump() || dmd.isOptimizedBootstrapDump()) {
      Database dbToLoad = null;
      try {
        dbToLoad = db.getDatabase(dbNameToLoadIn);
      } catch (HiveException e) {
        throw new SemanticException(e.getMessage(), e);
      }
      if (dbToLoad == null) {
        throw new SemanticException(ErrorMsg.DATABASE_NOT_EXISTS, dbNameToLoadIn);
      }
      // db property ReplConst.FAILOVER_ENDPOINT is only set during planned failover.
      String failoverType = "";
      try {
        // check whether ReplConst.FAILOVER_ENDPOINT is set
        failoverType = MetaStoreUtils.isDbBeingPlannedFailedOver(db.getDatabase(dbNameToLoadIn)) ? ReplConst.FailoverType.PLANNED.toString() : ReplConst.FailoverType.UNPLANNED.toString();
      } catch (HiveException e) {
        throw new RuntimeException(e);
      }
      if (dmd.isPreOptimizedBootstrapDump()) {
        collector = new PreOptimizedBootstrapLoadMetricCollector(dbNameToLoadIn, dumpDirectory, dmd.getDumpExecutionId(), conf,
                MetaStoreUtils.FailoverEndpoint.TARGET.toString(), failoverType);
      } else {
        // db property ReplConst.FAILOVER_ENDPOINT is only set during planned failover.
        collector = new OptimizedBootstrapLoadMetricCollector(dbNameToLoadIn, dumpDirectory, dmd.getDumpExecutionId(), conf,
                MetaStoreUtils.FailoverEndpoint.TARGET.toString(), failoverType);
      }
    } else if (dmd.isBootstrapDump()) {
      collector = new BootstrapLoadMetricCollector(dbNameToLoadIn, dumpDirectory, dmd.getDumpExecutionId(), conf);
    } else {
      collector = new IncrementalLoadMetricCollector(dbNameToLoadIn, dumpDirectory, dmd.getDumpExecutionId(), conf);
    }
    return collector;
  }

  private Path getCurrentLoadPath() throws IOException {
    Path loadPathBase = ReplUtils.getEncodedDumpRootPath(conf, sourceDbNameOrPattern.toLowerCase());
    final FileSystem fs = loadPathBase.getFileSystem(conf);
    // Make fully qualified path for further use.
    loadPathBase = fs.makeQualified(loadPathBase);
    if (fs.exists(loadPathBase)) {
      FileStatus[] statuses = loadPathBase.getFileSystem(conf).listStatus(loadPathBase);
      if (statuses.length > 0) {
        //sort based on last modified. Recent one is at the beginning
        FileStatus latestUpdatedStatus = statuses[0];
        for (FileStatus status : statuses) {
          if (status.getModificationTime() > latestUpdatedStatus.getModificationTime()) {
            latestUpdatedStatus = status;
          }
        }
        Path hiveDumpPath = new Path(latestUpdatedStatus.getPath(), ReplUtils.REPL_HIVE_BASE_DIR);
        if (loadPathBase.getFileSystem(conf).exists(new Path(hiveDumpPath,
                ReplAck.DUMP_ACKNOWLEDGEMENT.toString()))
                && !loadPathBase.getFileSystem(conf).exists(new Path(hiveDumpPath, LOAD_ACKNOWLEDGEMENT.toString()))) {
          return hiveDumpPath;
        }
      }
    }
    return null;
  }

  private void setConfigs(ASTNode node) throws SemanticException {
    Map<String, String> replConfigs = getProps(node);
    for (Map.Entry<String, String> config : replConfigs.entrySet()) {
      String key = config.getKey();
      // don't set the query id in the config
      if (key.equalsIgnoreCase(HIVE_QUERY_ID.varname)) {
        String queryTag = config.getValue();
        if (!StringUtils.isEmpty(queryTag)) {
          QueryState.setApplicationTag(conf, queryTag);
        }
        queryState.setQueryTag(queryTag);
      } else {
        conf.set(key, config.getValue());
      }
    }

    // As hive conf is changed, need to get the Hive DB again with it.
    try {
      db = Hive.get(conf);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  // REPL STATUS
  private void initReplStatus(ASTNode ast) throws SemanticException{
    replScope.setDbName(PlanUtils.stripQuotes(ast.getChild(0).getText()));
    int numChildren = ast.getChildCount();
    for (int i = 1; i < numChildren; i++) {
      ASTNode childNode = (ASTNode) ast.getChild(i);
      if (childNode.getToken().getType() == TOK_REPL_CONFIG) {
        setConfigs((ASTNode) childNode.getChild(0));
      } else {
        throw new SemanticException("Unrecognized token in REPL STATUS statement.");
      }
    }
  }

  private void analyzeReplStatus(ASTNode ast) throws SemanticException {
    initReplStatus(ast);
    String dbNameOrPattern = replScope.getDbName();
    String replLastId = getReplStatus(dbNameOrPattern);
    prepareReturnValues(Collections.singletonList(replLastId), "last_repl_id#string");
    setFetchTask(createFetchTask("last_repl_id#string"));
    LOG.debug("ReplicationSemanticAnalyzer.analyzeReplStatus: writing repl.last.id={} out to {} using configuration {}",
        replLastId, ctx.getResFile(), conf);
  }

  private String getReplStatus(String dbNameOrPattern) throws SemanticException {
    try {
      // Checking for status of a db
      Database database = db.getDatabase(dbNameOrPattern);
      if (database != null) {
        inputs.add(new ReadEntity(database));
        Map<String, String> params = database.getParameters();
        if (params != null && (params.containsKey(ReplicationSpec.KEY.CURR_STATE_ID_SOURCE.toString()))) {
          return params.get(ReplicationSpec.KEY.CURR_STATE_ID_SOURCE.toString());
        }
      }
    } catch (HiveException e) {
      throw new SemanticException(e); // TODO : simple wrap & rethrow for now, clean up with error
      // codes
    }
    return null;
  }

  private void prepareReturnValues(List<String> values, String schema) throws SemanticException {
    LOG.debug("prepareReturnValues : " + schema);
    for (String s : values) {
      LOG.debug("    > " + s);
    }
    ctx.setResFile(ctx.getLocalTmpPath());
    Utils.writeOutput(Collections.singletonList(values), ctx.getResFile(), conf);
  }
}
