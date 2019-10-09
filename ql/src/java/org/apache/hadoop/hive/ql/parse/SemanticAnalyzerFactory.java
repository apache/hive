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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SemanticAnalyzerFactory.
 *
 */
public final class SemanticAnalyzerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(SemanticAnalyzerFactory.class);

  private SemanticAnalyzerFactory() {
    throw new UnsupportedOperationException("SemanticAnalyzerFactory should not be instantiated");
  }

  public static BaseSemanticAnalyzer get(QueryState queryState, ASTNode tree) throws SemanticException {
    BaseSemanticAnalyzer sem = getInternal(queryState, tree);
    if(queryState.getHiveOperation() == null) {
      String query = queryState.getQueryString();
      if(query != null && query.length() > 30) {
        query = query.substring(0, 30);
      }
      String msg = "Unknown HiveOperation for query='" + query + "' queryId=" + queryState.getQueryId();
      //throw new IllegalStateException(msg);
      LOG.debug(msg);
    }
    return sem;
  }
  
  private static BaseSemanticAnalyzer getInternal(QueryState queryState, ASTNode tree)
      throws SemanticException {
    if (tree.getToken() == null) {
      throw new RuntimeException("Empty Syntax Tree");
    } else {
      HiveOperation opType = HiveOperation.operationForToken(tree.getType());
      queryState.setCommandType(opType);

      if (DDLSemanticAnalyzerFactory.handles(tree.getType())) {
        return DDLSemanticAnalyzerFactory.getAnalyzer(tree, queryState);
      }

      switch (tree.getType()) {
      case HiveParser.TOK_EXPLAIN:
        return new ExplainSemanticAnalyzer(queryState);
      case HiveParser.TOK_EXPLAIN_SQ_REWRITE:
        return new ExplainSQRewriteSemanticAnalyzer(queryState);
      case HiveParser.TOK_LOAD:
        return new LoadSemanticAnalyzer(queryState);
      case HiveParser.TOK_EXPORT:
        if (AcidExportSemanticAnalyzer.isAcidExport(tree)) {
          return new AcidExportSemanticAnalyzer(queryState);
        }
        return new ExportSemanticAnalyzer(queryState);
      case HiveParser.TOK_IMPORT:
        return new ImportSemanticAnalyzer(queryState);
      case HiveParser.TOK_REPL_DUMP:
        return new ReplicationSemanticAnalyzer(queryState);
      case HiveParser.TOK_REPL_LOAD:
        return new ReplicationSemanticAnalyzer(queryState);
      case HiveParser.TOK_REPL_STATUS:
        return new ReplicationSemanticAnalyzer(queryState);
      case HiveParser.TOK_ALTERTABLE: {
        Tree child = tree.getChild(1);
        queryState.setCommandType(HiveOperation.operationForToken(child.getType()));
        return new DDLSemanticAnalyzer(queryState);
      }
      case HiveParser.TOK_ALTERVIEW: {
        Tree child = tree.getChild(1);
        switch (child.getType()) {
        case HiveParser.TOK_ALTERVIEW_PROPERTIES:
        case HiveParser.TOK_ALTERVIEW_DROPPROPERTIES:
        case HiveParser.TOK_ALTERVIEW_ADDPARTS:
        case HiveParser.TOK_ALTERVIEW_DROPPARTS:
        case HiveParser.TOK_ALTERVIEW_RENAME:
          opType = HiveOperation.operationForToken(child.getType());
          queryState.setCommandType(opType);
          return new DDLSemanticAnalyzer(queryState);
        }
        // TOK_ALTERVIEW_AS
        assert child.getType() == HiveParser.TOK_QUERY;
        queryState.setCommandType(HiveOperation.ALTERVIEW_AS);
        return new SemanticAnalyzer(queryState);
      }
      case HiveParser.TOK_ALTER_MATERIALIZED_VIEW: {
        Tree child = tree.getChild(1);
        switch (child.getType()) {
        case HiveParser.TOK_ALTER_MATERIALIZED_VIEW_REWRITE:
          opType = HiveOperation.operationForToken(child.getType());
          queryState.setCommandType(opType);
          return new DDLSemanticAnalyzer(queryState);
        case HiveParser.TOK_ALTER_MATERIALIZED_VIEW_REBUILD:
          opType = HiveOperation.operationForToken(child.getType());
          queryState.setCommandType(opType);
          return new MaterializedViewRebuildSemanticAnalyzer(queryState);
        }
        // Operation not recognized, set to null and let upper level handle this case
        queryState.setCommandType(null);
        return new DDLSemanticAnalyzer(queryState);
      }
      case HiveParser.TOK_DROPTABLE:
      case HiveParser.TOK_DROPVIEW:
      case HiveParser.TOK_DROP_MATERIALIZED_VIEW:
      case HiveParser.TOK_DESCTABLE:
      case HiveParser.TOK_MSCK:
      case HiveParser.TOK_SHOWTABLES:
      case HiveParser.TOK_SHOWCOLUMNS:
      case HiveParser.TOK_SHOW_TABLESTATUS:
      case HiveParser.TOK_SHOW_TBLPROPERTIES:
      case HiveParser.TOK_SHOW_CREATETABLE:
      case HiveParser.TOK_SHOWPARTITIONS:
      case HiveParser.TOK_SHOWLOCKS:
      case HiveParser.TOK_SHOWDBLOCKS:
      case HiveParser.TOK_SHOWCONF:
      case HiveParser.TOK_SHOWVIEWS:
      case HiveParser.TOK_SHOWMATERIALIZEDVIEWS:
      case HiveParser.TOK_LOCKTABLE:
      case HiveParser.TOK_UNLOCKTABLE:
      case HiveParser.TOK_TRUNCATETABLE:
      case HiveParser.TOK_CACHE_METADATA:
        return new DDLSemanticAnalyzer(queryState);

      case HiveParser.TOK_ANALYZE:
        return new ColumnStatsSemanticAnalyzer(queryState);

      case HiveParser.TOK_UPDATE_TABLE:
      case HiveParser.TOK_DELETE_FROM:
        return new UpdateDeleteSemanticAnalyzer(queryState);

      case HiveParser.TOK_MERGE:
        return new MergeSemanticAnalyzer(queryState);

      case HiveParser.TOK_START_TRANSACTION:
      case HiveParser.TOK_COMMIT:
      case HiveParser.TOK_ROLLBACK:
      case HiveParser.TOK_SET_AUTOCOMMIT:
      default:
        SemanticAnalyzer semAnalyzer = HiveConf
            .getBoolVar(queryState.getConf(), HiveConf.ConfVars.HIVE_CBO_ENABLED) ?
                new CalcitePlanner(queryState) : new SemanticAnalyzer(queryState);
        return semAnalyzer;
      }
    }
  }
}
