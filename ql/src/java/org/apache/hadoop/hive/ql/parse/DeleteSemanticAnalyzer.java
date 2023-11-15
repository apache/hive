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

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.execute.AlterTableExecuteDesc;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.rewrite.DeleteStatement;
import org.apache.hadoop.hive.ql.parse.rewrite.RewriterFactory;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DeleteSemanticAnalyzer extends RewriteSemanticAnalyzer<DeleteStatement> {

  public DeleteSemanticAnalyzer(QueryState queryState, RewriterFactory<DeleteStatement> rewriterFactory)
      throws SemanticException {
    super(queryState, rewriterFactory);
  }

  @Override
  protected ASTNode getTargetTableNode(ASTNode tree) {
    // The first child should be the table we are updating / deleting from
    ASTNode tabName = (ASTNode) tree.getChild(0);
    assert tabName.getToken().getType() == HiveParser.TOK_TABNAME :
        "Expected tablename as first child of " + Context.Operation.DELETE + " but found " + tabName.getName();
    return tabName;
  }

  @Override
  protected void analyze(ASTNode tree, Table table, ASTNode tableName) throws SemanticException {
    List<? extends Node> children = tree.getChildren();

    ASTNode where = null;
    if (children.size() > 1) {
      where = (ASTNode) children.get(1);
      assert where.getToken().getType() == HiveParser.TOK_WHERE :
          "Expected where clause, but found " + where.getName();
    }

    boolean shouldTruncate = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_OPTIMIZE_REPLACE_DELETE_WITH_TRUNCATE)
        && where == null;
    if (shouldTruncate) {
      genTruncatePlan(table, tableName);
      return;
    } else if (tryMetadataUpdate(table, tableName, where)) {
      return;
    }

    rewriteAndAnalyze(new DeleteStatement(table, where), null);

    updateOutputs(table);
  }

  private void genTruncatePlan(Table table, ASTNode tabNameNode) throws SemanticException {
    String rewrittenQueryStr = "truncate " + getFullTableNameForSQL(tabNameNode);
    ParseUtils.ReparseResult rr = ParseUtils.parseRewrittenQuery(ctx, rewrittenQueryStr);
    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;

    BaseSemanticAnalyzer truncate = SemanticAnalyzerFactory.get(queryState, rewrittenTree);
    // Note: this will overwrite this.ctx with rewrittenCtx
    rewrittenCtx.setEnableUnparse(false);
    truncate.analyze(rewrittenTree, rewrittenCtx);

    rootTasks = truncate.getRootTasks();
    outputs = truncate.getOutputs();
    updateOutputs(table);
  }

  private boolean tryMetadataUpdate(Table table, ASTNode tabNameNode, ASTNode whereNode)
      throws SemanticException {
    // A feature flag on Hive to perform metadata delete on the source table.
    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_OPTIMIZE_METADATA_DELETE)) {
      return false;
    }
    if (whereNode == null || table.getStorageHandler() == null) {
      return false;
    }
    TableName tableName = getQualifiedTableName(tabNameNode);
    String whereClause = ctx.getTokenRewriteStream().toString(
        whereNode.getChild(0).getTokenStartIndex(), whereNode.getChild(0).getTokenStopIndex());
    StringBuilder sb = new StringBuilder("select * from ").append(getFullTableNameForSQL(tabNameNode))
        .append(" where ").append(whereClause);
    Context context = new Context(conf);
    ASTNode rewrittenTree;
    try {
      rewrittenTree = ParseUtils.parse(sb.toString(), context);
    } catch (ParseException pe) {
      throw new SemanticException(pe);
    }
    BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(queryState, rewrittenTree);
    sem.analyze(rewrittenTree, context);

    SearchArgument sarg = convertFilterExpressionInTS(table, sem);
    if (sarg == null) {
      return false;
    }
    if (!table.getStorageHandler().canPerformMetadataDelete(table, tableName.getTableMetaRef(), sarg)) {
      return false;
    }

    DDLWork ddlWork = createDDLWorkOfMetadataUpdate(tableName, sarg);
    rootTasks = Collections.singletonList(TaskFactory.get(ddlWork));
    inputs = sem.getInputs();
    outputs = sem.getOutputs();
    updateOutputs(table);
    return true;
  }

  private SearchArgument convertFilterExpressionInTS(Table table, BaseSemanticAnalyzer sem) {
    Map<String, TableScanOperator> topOps = sem.getParseContext().getTopOps();
    if (!topOps.containsKey(table.getTableName())) {
      return null;
    }
    ExprNodeGenericFuncDesc hiveFilter = topOps.get(table.getTableName()).getConf().getFilterExpr();
    if (hiveFilter == null) {
      return null;
    }
    ConvertAstToSearchArg.Result result = ConvertAstToSearchArg.createSearchArgument(ctx.getConf(), hiveFilter);
    if (result.isPartial()) {
      return null;
    }
    return result.getSearchArgument();
  }

  private DDLWork createDDLWorkOfMetadataUpdate(TableName tableName, SearchArgument sarg) throws SemanticException {
    AlterTableExecuteSpec.DeleteMetadataSpec deleteMetadataSpec =
        new AlterTableExecuteSpec.DeleteMetadataSpec(tableName.getTableMetaRef(), sarg);
    AlterTableExecuteSpec<AlterTableExecuteSpec.DeleteMetadataSpec> executeSpec =
        new AlterTableExecuteSpec<>(AlterTableExecuteSpec.ExecuteOperationType.DELETE_METADATA, deleteMetadataSpec);
    AlterTableExecuteDesc desc = new AlterTableExecuteDesc(tableName, null, executeSpec);
    return new DDLWork(getInputs(), getOutputs(), desc);
  }

  @Override
  protected boolean enableColumnStatsCollecting() {
    return false;
  }
}
