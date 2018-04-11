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

import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MaterializedViewRebuildSemanticAnalyzer.
 * Rewrites ALTER MATERIALIZED VIEW _mv_name_ REBUILD statement into
 * INSERT OVERWRITE TABLE _mv_name_ _mv_query_ .
 */
public class MaterializedViewRebuildSemanticAnalyzer extends CalcitePlanner {

  private static final Logger LOG =
      LoggerFactory.getLogger(MaterializedViewRebuildSemanticAnalyzer.class);


  public MaterializedViewRebuildSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }


  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    if (mvRebuildMode != MaterializationRebuildMode.NONE) {
      super.analyzeInternal(ast);
      return;
    }

    String[] qualifiedTableName = getQualifiedTableName((ASTNode) ast.getChild(0));
    String dbDotTable = getDotName(qualifiedTableName);
    ASTNode rewrittenAST;
    // We need to go lookup the table and get the select statement and then parse it.
    try {
      Table tab = getTableObjectByName(dbDotTable, true);
      if (!tab.isMaterializedView()) {
        // Cannot rebuild not materialized view
        throw new SemanticException(ErrorMsg.REBUILD_NO_MATERIALIZED_VIEW);
      }
      // We need to use the expanded text for the materialized view, as it will contain
      // the qualified table aliases, etc.
      String viewText = tab.getViewExpandedText();
      if (viewText.trim().isEmpty()) {
        throw new SemanticException(ErrorMsg.MATERIALIZED_VIEW_DEF_EMPTY);
      }
      Context ctx = new Context(queryState.getConf());
      rewrittenAST = ParseUtils.parse("insert overwrite table " +
          "`" + qualifiedTableName[0] + "`.`" + qualifiedTableName[1] + "` " +
          viewText, ctx);
      this.ctx.addRewrittenStatementContext(ctx);

      if (!this.ctx.isExplainPlan() && AcidUtils.isTransactionalTable(tab)) {
        // Acquire lock for the given materialized view. Only one rebuild per materialized
        // view can be triggered at a given time, as otherwise we might produce incorrect
        // results if incremental maintenance is triggered.
        HiveTxnManager txnManager = SessionState.get().getTxnMgr();
        LockState state;
        try {
          state = txnManager.acquireMaterializationRebuildLock(
              qualifiedTableName[0], qualifiedTableName[1], txnManager.getCurrentTxnId()).getState();
        } catch (LockException e) {
          throw new SemanticException("Exception acquiring lock for rebuilding the materialized view", e);
        }
        if (state != LockState.ACQUIRED) {
          throw new SemanticException("Another process is rebuilding the materialized view " + dbDotTable);
        }
      }
    } catch (Exception e) {
      throw new SemanticException(e);
    }
    mvRebuildMode = MaterializationRebuildMode.INSERT_OVERWRITE_REBUILD;
    mvRebuildDbName = qualifiedTableName[0];
    mvRebuildName = qualifiedTableName[1];

    LOG.debug("Rebuilding materialized view " + dbDotTable);
    super.analyzeInternal(rewrittenAST);
  }
}
