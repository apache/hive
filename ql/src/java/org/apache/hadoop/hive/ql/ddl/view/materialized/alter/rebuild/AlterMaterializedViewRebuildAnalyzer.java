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

package org.apache.hadoop.hive.ql.ddl.view.materialized.alter.rebuild;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analyzer for alter materialized view rebuild commands.
 */
@DDLType(types = HiveParser.TOK_ALTER_MATERIALIZED_VIEW_REBUILD)
public class AlterMaterializedViewRebuildAnalyzer extends CalcitePlanner {
  private static final Logger LOG = LoggerFactory.getLogger(AlterMaterializedViewRebuildAnalyzer.class);

  public AlterMaterializedViewRebuildAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    if (mvRebuildMode != MaterializationRebuildMode.NONE) {
      super.analyzeInternal(root);
      return;
    }

    ASTNode tableTree = (ASTNode) root.getChild(0);
    TableName tableName = getQualifiedTableName(tableTree);
    // If this was called from ScheduledQueryAnalyzer we do not want to execute the Alter materialized view statement
    // now. However query scheduler requires the fully qualified table name.
    if (ctx.isScheduledQuery()) {
      unparseTranslator.addTableNameTranslation(tableTree, SessionState.get().getCurrentDatabase());
      return;
    }
    ASTNode rewrittenAST = getRewrittenAST(tableName);

    mvRebuildMode = MaterializationRebuildMode.INSERT_OVERWRITE_REBUILD;
    mvRebuildDbName = tableName.getDb();
    mvRebuildName = tableName.getTable();

    LOG.debug("Rebuilding materialized view " + tableName.getNotEmptyDbTable());
    super.analyzeInternal(rewrittenAST);
  }

  private static final String REWRITTEN_INSERT_STATEMENT = "INSERT OVERWRITE TABLE %s %s";

  private ASTNode getRewrittenAST(TableName tableName) throws SemanticException {
    ASTNode rewrittenAST;
    // We need to go lookup the table and get the select statement and then parse it.
    try {
      Table table = getTableObjectByName(tableName.getNotEmptyDbTable(), true);
      if (!table.isMaterializedView()) {
        // Cannot rebuild not materialized view
        throw new SemanticException(ErrorMsg.REBUILD_NO_MATERIALIZED_VIEW);
      }

      // We need to use the expanded text for the materialized view, as it will contain
      // the qualified table aliases, etc.
      String viewText = table.getViewExpandedText();
      if (viewText.trim().isEmpty()) {
        throw new SemanticException(ErrorMsg.MATERIALIZED_VIEW_DEF_EMPTY);
      }

      Context ctx = new Context(queryState.getConf());
      String rewrittenInsertStatement = String.format(REWRITTEN_INSERT_STATEMENT,
          tableName.getEscapedNotEmptyDbTable(), viewText);
      rewrittenAST = ParseUtils.parse(rewrittenInsertStatement, ctx);
      this.ctx.addSubContext(ctx);

      if (!this.ctx.isExplainPlan() && AcidUtils.isTransactionalTable(table)) {
        // Acquire lock for the given materialized view. Only one rebuild per materialized view can be triggered at a
        // given time, as otherwise we might produce incorrect results if incremental maintenance is triggered.
        HiveTxnManager txnManager = getTxnMgr();
        LockState state;
        try {
          state = txnManager.acquireMaterializationRebuildLock(
              tableName.getDb(), tableName.getTable(), txnManager.getCurrentTxnId()).getState();
        } catch (LockException e) {
          throw new SemanticException("Exception acquiring lock for rebuilding the materialized view", e);
        }
        if (state != LockState.ACQUIRED) {
          throw new SemanticException(
              "Another process is rebuilding the materialized view " + tableName.getNotEmptyDbTable());
        }
      }
    } catch (Exception e) {
      throw new SemanticException(e);
    }
    return rewrittenAST;
  }
}
