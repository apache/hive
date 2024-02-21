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
import org.apache.hadoop.hive.ql.parse.rewrite.DeleteRewriterFactory;
import org.apache.hadoop.hive.ql.parse.rewrite.MergeRewriterFactory;
import org.apache.hadoop.hive.ql.parse.rewrite.UpdateRewriterFactory;
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

      if (DDLSemanticAnalyzerFactory.handles(tree)) {
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
      case HiveParser.TOK_ALTERVIEW: {
        Tree child = tree.getChild(1);
        // TOK_ALTERVIEW_AS
        assert child.getType() == HiveParser.TOK_QUERY;
        queryState.setCommandType(HiveOperation.ALTERVIEW_AS);
        return new SemanticAnalyzer(queryState);
      }
      case HiveParser.TOK_ANALYZE:
        return new ColumnStatsSemanticAnalyzer(queryState);

      case HiveParser.TOK_UPDATE_TABLE:
        return new UpdateSemanticAnalyzer(queryState, new UpdateRewriterFactory(queryState.getConf()));
      case HiveParser.TOK_DELETE_FROM:
        return new DeleteSemanticAnalyzer(queryState, new DeleteRewriterFactory(queryState.getConf()));

      case HiveParser.TOK_MERGE:
        return new MergeSemanticAnalyzer(queryState, new MergeRewriterFactory(queryState.getConf()));

      case HiveParser.TOK_ALTER_SCHEDULED_QUERY:
      case HiveParser.TOK_CREATE_SCHEDULED_QUERY:
      case HiveParser.TOK_DROP_SCHEDULED_QUERY:
        return new ScheduledQueryAnalyzer(queryState);
      case HiveParser.TOK_EXECUTE:
        return new ExecuteStatementAnalyzer(queryState);
      case HiveParser.TOK_PREPARE:
        return new PrepareStatementAnalyzer(queryState);
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
