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
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.ReplDumpWork;
import org.apache.hadoop.hive.ql.exec.repl.ReplLoadWork;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;
import org.apache.hadoop.hive.ql.plan.PlanUtils;

import java.io.FileNotFoundException;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVEQUERYID;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_DBNAME;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_LIMIT;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_REPL_CONFIG;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_REPL_DUMP;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_REPL_LOAD;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_REPL_STATUS;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TABNAME;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TO;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_ENABLE_MOVE_OPTIMIZATION;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_MOVE_OPTIMIZED_FILE_SCHEMES;


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

  // By default, this will be same as that of super class BaseSemanticAnalyzer. But need to obtain again
  // if the Hive configs are received from WITH clause in REPL LOAD or REPL STATUS commands.
  private Hive db;

  private static String testInjectDumpDir = null; // unit tests can overwrite this to affect default dump behaviour
  private static final String dumpSchema = "dump_dir,last_repl_id#string,string";

  public static final String LAST_REPL_ID_KEY = "hive.repl.last.repl.id";
  public static final String FUNCTIONS_ROOT_DIR_NAME = "_functions";
  public static final String CONSTRAINTS_ROOT_DIR_NAME = "_constraints";

  ReplicationSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
    this.db = super.db;
    this.conf = new HiveConf(super.conf);
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    LOG.debug("ReplicationSemanticAanalyzer: analyzeInternal");
    LOG.debug(ast.getName() + ":" + ast.getToken().getText() + "=" + ast.getText());

    // Some of the txn related configs were not set when ReplicationSemanticAnalyzer.conf was initialized.
    // It should be set first.
    setTxnConfigs();
    switch (ast.getToken().getType()) {
      case TOK_REPL_DUMP: {
        LOG.debug("ReplicationSemanticAnalyzer: analyzeInternal: dump");
        try {
          initReplDump(ast);
        } catch (HiveException e) {
          throw new SemanticException(e.getMessage(), e);
        }
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

  private void setTxnConfigs() {
    String validTxnList = queryState.getConf().get(ValidTxnList.VALID_TXNS_KEY);
    if (validTxnList != null) {
      conf.set(ValidTxnList.VALID_TXNS_KEY, validTxnList);
    }
  }

  private void initReplDump(ASTNode ast) throws HiveException {
    int numChildren = ast.getChildCount();
    boolean isMetaDataOnly = false;
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
          isMetaDataOnly = HiveConf.getBoolVar(conf, REPL_DUMP_METADATA_ONLY);
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

    for (String dbName : Utils.matchesDb(db, dbNameOrPattern)) {
      Database database = db.getDatabase(dbName);
      if (database != null) {
        if (!isMetaDataOnly && !ReplChangeManager.isSourceOfReplication(database)) {
          LOG.error("Cannot dump database " + dbName +
                  " as it is not a source of replication (repl.source.for)");
          throw new SemanticException(ErrorMsg.REPL_DATABASE_IS_NOT_SOURCE_OF_REPLICATION.getMsg());
        }
      } else {
        throw new SemanticException("Cannot dump database " + dbName + " as it does not exist");
      }
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
      ), conf);
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

  private boolean ifEnableMoveOptimization(Path filePath, org.apache.hadoop.conf.Configuration conf) throws Exception {
    if (filePath == null) {
      throw new HiveException("filePath cannot be null");
    }

    URI uri = filePath.toUri();
    String scheme = uri.getScheme();
    scheme = StringUtils.isBlank(scheme) ? FileSystem.get(uri, conf).getScheme() : scheme;
    if (StringUtils.isBlank(scheme)) {
      throw new HiveException("Cannot get valid scheme for " + filePath);
    }
    LOG.info("scheme is " + scheme);
    String[] schmeList = conf.get(REPL_MOVE_OPTIMIZED_FILE_SCHEMES.varname).toLowerCase().split(",");
    for (String schemeIter : schmeList) {
      if (schemeIter.trim().equalsIgnoreCase(scheme.trim())) {
        return true;
      }
    }
    return false;
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
          setConfigs((ASTNode) childNode.getChild(0));
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
      assert(path != null);
      Path loadPath = new Path(path);
      final FileSystem fs = loadPath.getFileSystem(conf);

      // Make fully qualified path for further use.
      loadPath = fs.makeQualified(loadPath);

      if (!fs.exists(loadPath)) {
        // supposed dump path does not exist.
        LOG.error("File not found " + loadPath.toUri().toString());
        throw new FileNotFoundException(ErrorMsg.REPL_LOAD_PATH_NOT_FOUND.getMsg());
      }

      // Ths config is set to make sure that in case of s3 replication, move is skipped.
      try {
        Warehouse wh = new Warehouse(conf);
        Path filePath = wh.getWhRoot();
        if (ifEnableMoveOptimization(filePath, conf)) {
          conf.setBoolVar(REPL_ENABLE_MOVE_OPTIMIZATION, true);
          LOG.info(" Set move optimization to true for warehouse " + filePath.toString());
        }
      } catch (Exception e) {
        throw new SemanticException(e.getMessage(), e);
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

      ReplLoadWork replLoadWork = new ReplLoadWork(conf, loadPath.toString(), dbNameOrPattern,
              tblNameOrPattern, queryState.getLineageState(), evDump, dmd.getEventTo());
      rootTasks.add(TaskFactory.get(replLoadWork, conf));
    } catch (Exception e) {
      // TODO : simple wrap & rethrow for now, clean up with error codes
      throw new SemanticException(e.getMessage(), e);
    }
  }

  private void setConfigs(ASTNode node) throws SemanticException {
    Map<String, String> replConfigs = DDLSemanticAnalyzer.getProps(node);
    if (null != replConfigs) {
      for (Map.Entry<String, String> config : replConfigs.entrySet()) {
        String key = config.getKey();
        // don't set the query id in the config
        if (key.equalsIgnoreCase(HIVEQUERYID.varname)) {
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
  }

  // REPL STATUS
  private void initReplStatus(ASTNode ast) throws SemanticException{
    dbNameOrPattern = PlanUtils.stripQuotes(ast.getChild(0).getText());
    int numChildren = ast.getChildCount();
    for (int i = 1; i < numChildren; i++) {
      ASTNode childNode = (ASTNode) ast.getChild(i);
      switch (childNode.getToken().getType()) {
      case TOK_TABNAME:
        tblNameOrPattern = PlanUtils.stripQuotes(childNode.getChild(0).getText());
        break;
      case TOK_REPL_CONFIG:
        setConfigs((ASTNode) childNode.getChild(0));
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
      if (tblNameOrPattern != null) {
        // Checking for status of table
        Table tbl = db.getTable(dbNameOrPattern, tblNameOrPattern);
        if (tbl != null) {
          inputs.add(new ReadEntity(tbl));
          Map<String, String> params = tbl.getParameters();
          if (params != null && (params.containsKey(ReplicationSpec.KEY.CURR_STATE_ID.toString()))) {
            replLastId = params.get(ReplicationSpec.KEY.CURR_STATE_ID.toString());
          }
        }
      } else {
        // Checking for status of a db
        Database database = db.getDatabase(dbNameOrPattern);
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
