/**
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

import static org.apache.hadoop.hive.serde.Constants.LIST_COLUMNS;
import static org.apache.hadoop.hive.serde.Constants.LIST_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT;
import static org.apache.hadoop.util.StringUtils.stringifyException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.exec.AbstractMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.ExecDriver;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapRedTask;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RecordReader;
import org.apache.hadoop.hive.ql.exec.RecordWriter;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.GenMRFileSink1;
import org.apache.hadoop.hive.ql.optimizer.GenMROperator;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMapRedCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMRRedSink1;
import org.apache.hadoop.hive.ql.optimizer.GenMRRedSink2;
import org.apache.hadoop.hive.ql.optimizer.GenMRRedSink3;
import org.apache.hadoop.hive.ql.optimizer.GenMRRedSink4;
import org.apache.hadoop.hive.ql.optimizer.GenMRTableScan1;
import org.apache.hadoop.hive.ql.optimizer.GenMRUnion1;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.optimizer.MapJoinFactory;
import org.apache.hadoop.hive.ql.optimizer.Optimizer;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalContext;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalOptimizer;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;
import org.apache.hadoop.hive.ql.plan.CreateTableLikeDesc;
import org.apache.hadoop.hive.ql.plan.CreateViewDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc;
import org.apache.hadoop.hive.ql.plan.ExtractDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc.sampleDesc;
import org.apache.hadoop.hive.ql.plan.ForwardDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.LateralViewJoinDesc;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.LoadFileDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ScriptDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.UDTFDesc;
import org.apache.hadoop.hive.ql.plan.UnionDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFHash;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * Implementation of the semantic analyzer.
 */

public class SemanticAnalyzer extends BaseSemanticAnalyzer {
  private HashMap<TableScanOperator, ExprNodeDesc> opToPartPruner;
  private HashMap<String, Operator<? extends Serializable>> topOps;
  private HashMap<String, Operator<? extends Serializable>> topSelOps;
  private LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx;
  private List<LoadTableDesc> loadTableWork;
  private List<LoadFileDesc> loadFileWork;
  private Map<JoinOperator, QBJoinTree> joinContext;
  private final HashMap<TableScanOperator, Table> topToTable;
  private QB qb;
  private ASTNode ast;
  private int destTableId;
  private UnionProcContext uCtx;
  List<AbstractMapJoinOperator<? extends MapJoinDesc>> listMapJoinOpsNoReducer;
  private HashMap<TableScanOperator, sampleDesc> opToSamplePruner;
  Map<GroupByOperator, Set<String>> groupOpToInputTables;
  Map<String, PrunedPartitionList> prunedPartitions;
  private List<FieldSchema> resultSchema;
  private CreateViewDesc createVwDesc;
  private ASTNode viewSelect;
  private final UnparseTranslator unparseTranslator;

  private static class Phase1Ctx {
    String dest;
    int nextNum;
  }

  public SemanticAnalyzer(HiveConf conf) throws SemanticException {

    super(conf);

    opToPartPruner = new HashMap<TableScanOperator, ExprNodeDesc>();
    opToSamplePruner = new HashMap<TableScanOperator, sampleDesc>();
    topOps = new HashMap<String, Operator<? extends Serializable>>();
    topSelOps = new HashMap<String, Operator<? extends Serializable>>();
    loadTableWork = new ArrayList<LoadTableDesc>();
    loadFileWork = new ArrayList<LoadFileDesc>();
    opParseCtx = new LinkedHashMap<Operator<? extends Serializable>, OpParseContext>();
    joinContext = new HashMap<JoinOperator, QBJoinTree>();
    topToTable = new HashMap<TableScanOperator, Table>();
    destTableId = 1;
    uCtx = null;
    listMapJoinOpsNoReducer = new ArrayList<AbstractMapJoinOperator<? extends MapJoinDesc>>();
    groupOpToInputTables = new HashMap<GroupByOperator, Set<String>>();
    prunedPartitions = new HashMap<String, PrunedPartitionList>();
    unparseTranslator = new UnparseTranslator();
  }

  @Override
  protected void reset() {
    super.reset();
    loadTableWork.clear();
    loadFileWork.clear();
    topOps.clear();
    topSelOps.clear();
    destTableId = 1;
    idToTableNameMap.clear();
    qb = null;
    ast = null;
    uCtx = null;
    joinContext.clear();
    opParseCtx.clear();
    groupOpToInputTables.clear();
    prunedPartitions.clear();
  }

  public void init(ParseContext pctx) {
    opToPartPruner = pctx.getOpToPartPruner();
    opToSamplePruner = pctx.getOpToSamplePruner();
    topOps = pctx.getTopOps();
    topSelOps = pctx.getTopSelOps();
    opParseCtx = pctx.getOpParseCtx();
    loadTableWork = pctx.getLoadTableWork();
    loadFileWork = pctx.getLoadFileWork();
    joinContext = pctx.getJoinContext();
    ctx = pctx.getContext();
    destTableId = pctx.getDestTableId();
    idToTableNameMap = pctx.getIdToTableNameMap();
    uCtx = pctx.getUCtx();
    listMapJoinOpsNoReducer = pctx.getListMapJoinOpsNoReducer();
    qb = pctx.getQB();
    groupOpToInputTables = pctx.getGroupOpToInputTables();
    prunedPartitions = pctx.getPrunedPartitions();
    setLineageInfo(pctx.getLineageInfo());
  }

  public ParseContext getParseContext() {
    return new ParseContext(conf, qb, ast, opToPartPruner, topOps, topSelOps,
        opParseCtx, joinContext, topToTable, loadTableWork,
        loadFileWork, ctx, idToTableNameMap, destTableId, uCtx,
        listMapJoinOpsNoReducer, groupOpToInputTables, prunedPartitions,
        opToSamplePruner);
  }

  @SuppressWarnings("nls")
  public void doPhase1QBExpr(ASTNode ast, QBExpr qbexpr, String id, String alias)
      throws SemanticException {

    assert (ast.getToken() != null);
    switch (ast.getToken().getType()) {
    case HiveParser.TOK_QUERY: {
      QB qb = new QB(id, alias, true);
      doPhase1(ast, qb, initPhase1Ctx());
      qbexpr.setOpcode(QBExpr.Opcode.NULLOP);
      qbexpr.setQB(qb);
    }
      break;
    case HiveParser.TOK_UNION: {
      qbexpr.setOpcode(QBExpr.Opcode.UNION);
      // query 1
      assert (ast.getChild(0) != null);
      QBExpr qbexpr1 = new QBExpr(alias + "-subquery1");
      doPhase1QBExpr((ASTNode) ast.getChild(0), qbexpr1, id + "-subquery1",
          alias + "-subquery1");
      qbexpr.setQBExpr1(qbexpr1);

      // query 2
      assert (ast.getChild(0) != null);
      QBExpr qbexpr2 = new QBExpr(alias + "-subquery2");
      doPhase1QBExpr((ASTNode) ast.getChild(1), qbexpr2, id + "-subquery2",
          alias + "-subquery2");
      qbexpr.setQBExpr2(qbexpr2);
    }
      break;
    }
  }

  private LinkedHashMap<String, ASTNode> doPhase1GetAggregationsFromSelect(
      ASTNode selExpr) {
    // Iterate over the selects search for aggregation Trees.
    // Use String as keys to eliminate duplicate trees.
    LinkedHashMap<String, ASTNode> aggregationTrees = new LinkedHashMap<String, ASTNode>();
    for (int i = 0; i < selExpr.getChildCount(); ++i) {
      ASTNode sel = (ASTNode) selExpr.getChild(i).getChild(0);
      doPhase1GetAllAggregations(sel, aggregationTrees);
    }
    return aggregationTrees;
  }

  /**
   * DFS-scan the expressionTree to find all aggregation subtrees and put them
   * in aggregations.
   *
   * @param expressionTree
   * @param aggregations
   *          the key to the HashTable is the toStringTree() representation of
   *          the aggregation subtree.
   */
  private void doPhase1GetAllAggregations(ASTNode expressionTree,
      HashMap<String, ASTNode> aggregations) {
    int exprTokenType = expressionTree.getToken().getType();
    if (exprTokenType == HiveParser.TOK_FUNCTION
        || exprTokenType == HiveParser.TOK_FUNCTIONDI
        || exprTokenType == HiveParser.TOK_FUNCTIONSTAR) {
      assert (expressionTree.getChildCount() != 0);
      if (expressionTree.getChild(0).getType() == HiveParser.Identifier) {
        String functionName = unescapeIdentifier(expressionTree.getChild(0)
            .getText());
        if (FunctionRegistry.getGenericUDAFResolver(functionName) != null) {
          aggregations.put(expressionTree.toStringTree(), expressionTree);
          FunctionInfo fi = FunctionRegistry.getFunctionInfo(functionName);
          if (!fi.isNative()) {
            unparseTranslator.addIdentifierTranslation((ASTNode) expressionTree
                .getChild(0));
          }
          return;
        }
      }
    }
    for (int i = 0; i < expressionTree.getChildCount(); i++) {
      doPhase1GetAllAggregations((ASTNode) expressionTree.getChild(i),
          aggregations);
    }
  }

  private ASTNode doPhase1GetDistinctFuncExpr(
      HashMap<String, ASTNode> aggregationTrees) throws SemanticException {
    ASTNode expr = null;
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();
      assert (value != null);
      if (value.getToken().getType() == HiveParser.TOK_FUNCTIONDI) {
        if (expr == null) {
          expr = value;
        } else {
          throw new SemanticException(ErrorMsg.UNSUPPORTED_MULTIPLE_DISTINCTS
              .getMsg());
        }
      }
    }
    return expr;
  }

  /**
   * Goes though the tabref tree and finds the alias for the table. Once found,
   * it records the table name-> alias association in aliasToTabs. It also makes
   * an association from the alias to the table AST in parse info.
   *
   * @return the alias of the table
   */
  private String processTable(QB qb, ASTNode tabref) throws SemanticException {
    // For each table reference get the table name
    // and the alias (if alias is not present, the table name
    // is used as an alias)
    boolean tableSamplePresent = false;
    int aliasIndex = 0;
    if (tabref.getChildCount() == 2) {
      // tablename tablesample
      // OR
      // tablename alias
      ASTNode ct = (ASTNode) tabref.getChild(1);
      if (ct.getToken().getType() == HiveParser.TOK_TABLESAMPLE) {
        tableSamplePresent = true;
      } else {
        aliasIndex = 1;
      }
    } else if (tabref.getChildCount() == 3) {
      // table name table sample alias
      aliasIndex = 2;
      tableSamplePresent = true;
    }
    ASTNode tableTree = (ASTNode) (tabref.getChild(0));
    String alias = unescapeIdentifier(tabref.getChild(aliasIndex).getText());
    // If the alias is already there then we have a conflict
    if (qb.exists(alias)) {
      throw new SemanticException(ErrorMsg.AMBIGUOUS_TABLE_ALIAS.getMsg(tabref
          .getChild(aliasIndex)));
    }
    if (tableSamplePresent) {
      ASTNode sampleClause = (ASTNode) tabref.getChild(1);
      ArrayList<ASTNode> sampleCols = new ArrayList<ASTNode>();
      if (sampleClause.getChildCount() > 2) {
        for (int i = 2; i < sampleClause.getChildCount(); i++) {
          sampleCols.add((ASTNode) sampleClause.getChild(i));
        }
      }
      // TODO: For now only support sampling on up to two columns
      // Need to change it to list of columns
      if (sampleCols.size() > 2) {
        throw new SemanticException(ErrorMsg.SAMPLE_RESTRICTION.getMsg(tabref
            .getChild(0)));
      }
      qb.getParseInfo().setTabSample(
          alias,
          new TableSample(
          unescapeIdentifier(sampleClause.getChild(0).getText()),
          unescapeIdentifier(sampleClause.getChild(1).getText()),
          sampleCols));
      if (unparseTranslator.isEnabled()) {
        for (ASTNode sampleCol : sampleCols) {
          unparseTranslator.addIdentifierTranslation((ASTNode) sampleCol
              .getChild(0));
        }
      }
    }
    // Insert this map into the stats
    String table_name = unescapeIdentifier(tabref.getChild(0).getText());
    qb.setTabAlias(alias, table_name);

    qb.getParseInfo().setSrcForAlias(alias, tableTree);

    unparseTranslator.addIdentifierTranslation(tableTree);
    if (aliasIndex != 0) {
      unparseTranslator.addIdentifierTranslation((ASTNode) tabref
          .getChild(aliasIndex));
    }

    return alias;
  }

  private String processSubQuery(QB qb, ASTNode subq) throws SemanticException {

    // This is a subquery and must have an alias
    if (subq.getChildCount() != 2) {
      throw new SemanticException(ErrorMsg.NO_SUBQUERY_ALIAS.getMsg(subq));
    }
    ASTNode subqref = (ASTNode) subq.getChild(0);
    String alias = unescapeIdentifier(subq.getChild(1).getText());

    // Recursively do the first phase of semantic analysis for the subquery
    QBExpr qbexpr = new QBExpr(alias);

    doPhase1QBExpr(subqref, qbexpr, qb.getId(), alias);

    // If the alias is already there then we have a conflict
    if (qb.exists(alias)) {
      throw new SemanticException(ErrorMsg.AMBIGUOUS_TABLE_ALIAS.getMsg(subq
          .getChild(1)));
    }
    // Insert this map into the stats
    qb.setSubqAlias(alias, qbexpr);

    unparseTranslator.addIdentifierTranslation((ASTNode) subq.getChild(1));

    return alias;
  }

  private boolean isJoinToken(ASTNode node) {
    if ((node.getToken().getType() == HiveParser.TOK_JOIN)
        || (node.getToken().getType() == HiveParser.TOK_LEFTOUTERJOIN)
        || (node.getToken().getType() == HiveParser.TOK_RIGHTOUTERJOIN)
        || (node.getToken().getType() == HiveParser.TOK_FULLOUTERJOIN)
        || (node.getToken().getType() == HiveParser.TOK_LEFTSEMIJOIN)
        || (node.getToken().getType() == HiveParser.TOK_UNIQUEJOIN)) {
      return true;
    }

    return false;
  }

  /**
   * Given the AST with TOK_JOIN as the root, get all the aliases for the tables
   * or subqueries in the join.
   *
   * @param qb
   * @param join
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  private void processJoin(QB qb, ASTNode join) throws SemanticException {
    int numChildren = join.getChildCount();
    if ((numChildren != 2) && (numChildren != 3)
        && join.getToken().getType() != HiveParser.TOK_UNIQUEJOIN) {
      throw new SemanticException("Join with multiple children");
    }

    for (int num = 0; num < numChildren; num++) {
      ASTNode child = (ASTNode) join.getChild(num);
      if (child.getToken().getType() == HiveParser.TOK_TABREF) {
        processTable(qb, child);
      } else if (child.getToken().getType() == HiveParser.TOK_SUBQUERY) {
        processSubQuery(qb, child);
      } else if (child.getToken().getType() == HiveParser.TOK_LATERAL_VIEW) {
        // SELECT * FROM src1 LATERAL VIEW udtf() AS myTable JOIN src2 ...
        // is not supported. Instead, the lateral view must be in a subquery
        // SELECT * FROM (SELECT * FROM src1 LATERAL VIEW udtf() AS myTable) a
        // JOIN src2 ...
        throw new SemanticException(ErrorMsg.LATERAL_VIEW_WITH_JOIN
            .getMsg(join));
      } else if (isJoinToken(child)) {
        processJoin(qb, child);
      }
    }
  }

  /**
   * Given the AST with TOK_LATERAL_VIEW as the root, get the alias for the
   * table or subquery in the lateral view and also make a mapping from the
   * alias to all the lateral view AST's.
   *
   * @param qb
   * @param lateralView
   * @return the alias for the table/subquery
   * @throws SemanticException
   */

  private String processLateralView(QB qb, ASTNode lateralView)
      throws SemanticException {
    int numChildren = lateralView.getChildCount();

    assert (numChildren == 2);
    ASTNode next = (ASTNode) lateralView.getChild(1);

    String alias = null;

    switch (next.getToken().getType()) {
    case HiveParser.TOK_TABREF:
      alias = processTable(qb, next);
      break;
    case HiveParser.TOK_SUBQUERY:
      alias = processSubQuery(qb, next);
      break;
    case HiveParser.TOK_LATERAL_VIEW:
      alias = processLateralView(qb, next);
      break;
    default:
      throw new SemanticException(ErrorMsg.LATERAL_VIEW_INVALID_CHILD
          .getMsg(lateralView));
    }
    qb.getParseInfo().addLateralViewForAlias(alias, lateralView);
    return alias;
  }

  /**
   * Phase 1: (including, but not limited to):
   *
   * 1. Gets all the aliases for all the tables / subqueries and makes the
   * appropriate mapping in aliasToTabs, aliasToSubq 2. Gets the location of the
   * destination and names the clase "inclause" + i 3. Creates a map from a
   * string representation of an aggregation tree to the actual aggregation AST
   * 4. Creates a mapping from the clause name to the select expression AST in
   * destToSelExpr 5. Creates a mapping from a table alias to the lateral view
   * AST's in aliasToLateralViews
   *
   * @param ast
   * @param qb
   * @param ctx_1
   * @throws SemanticException
   */
  @SuppressWarnings({"fallthrough", "nls"})
  public void doPhase1(ASTNode ast, QB qb, Phase1Ctx ctx_1)
      throws SemanticException {

    QBParseInfo qbp = qb.getParseInfo();
    boolean skipRecursion = false;

    if (ast.getToken() != null) {
      skipRecursion = true;
      switch (ast.getToken().getType()) {
      case HiveParser.TOK_SELECTDI:
        qb.countSelDi();
        // fall through
      case HiveParser.TOK_SELECT:
        qb.countSel();
        qbp.setSelExprForClause(ctx_1.dest, ast);

        if (((ASTNode) ast.getChild(0)).getToken().getType() == HiveParser.TOK_HINTLIST) {
          qbp.setHints((ASTNode) ast.getChild(0));
        }

        LinkedHashMap<String, ASTNode> aggregations = doPhase1GetAggregationsFromSelect(ast);
        qbp.setAggregationExprsForClause(ctx_1.dest, aggregations);
        qbp.setDistinctFuncExprForClause(ctx_1.dest,
            doPhase1GetDistinctFuncExpr(aggregations));
        break;

      case HiveParser.TOK_WHERE:
        qbp.setWhrExprForClause(ctx_1.dest, ast);
        break;

      case HiveParser.TOK_DESTINATION:
        ctx_1.dest = "insclause-" + ctx_1.nextNum;
        ctx_1.nextNum++;

        // is there a insert in the subquery
        if (qbp.getIsSubQ()) {
          ASTNode ch = (ASTNode) ast.getChild(0);
          if ((ch.getToken().getType() != HiveParser.TOK_DIR)
              || (((ASTNode) ch.getChild(0)).getToken().getType() != HiveParser.TOK_TMP_FILE)) {
            throw new SemanticException(ErrorMsg.NO_INSERT_INSUBQUERY
                .getMsg(ast));
          }
        }

        qbp.setDestForClause(ctx_1.dest, (ASTNode) ast.getChild(0));
        break;

      case HiveParser.TOK_FROM:
        int child_count = ast.getChildCount();
        if (child_count != 1) {
          throw new SemanticException("Multiple Children " + child_count);
        }

        // Check if this is a subquery / lateral view
        ASTNode frm = (ASTNode) ast.getChild(0);
        if (frm.getToken().getType() == HiveParser.TOK_TABREF) {
          processTable(qb, frm);
        } else if (frm.getToken().getType() == HiveParser.TOK_SUBQUERY) {
          processSubQuery(qb, frm);
        } else if (frm.getToken().getType() == HiveParser.TOK_LATERAL_VIEW) {
          processLateralView(qb, frm);
        } else if (isJoinToken(frm)) {
          processJoin(qb, frm);
          qbp.setJoinExpr(frm);
        }
        break;

      case HiveParser.TOK_CLUSTERBY:
        // Get the clusterby aliases - these are aliased to the entries in the
        // select list
        qbp.setClusterByExprForClause(ctx_1.dest, ast);
        break;

      case HiveParser.TOK_DISTRIBUTEBY:
        // Get the distribute by aliases - these are aliased to the entries in
        // the
        // select list
        qbp.setDistributeByExprForClause(ctx_1.dest, ast);
        if (qbp.getClusterByForClause(ctx_1.dest) != null) {
          throw new SemanticException(ErrorMsg.CLUSTERBY_DISTRIBUTEBY_CONFLICT
              .getMsg(ast));
        } else if (qbp.getOrderByForClause(ctx_1.dest) != null) {
          throw new SemanticException(ErrorMsg.ORDERBY_DISTRIBUTEBY_CONFLICT
              .getMsg(ast));
        }
        break;

      case HiveParser.TOK_SORTBY:
        // Get the sort by aliases - these are aliased to the entries in the
        // select list
        qbp.setSortByExprForClause(ctx_1.dest, ast);
        if (qbp.getClusterByForClause(ctx_1.dest) != null) {
          throw new SemanticException(ErrorMsg.CLUSTERBY_SORTBY_CONFLICT
              .getMsg(ast));
        } else if (qbp.getOrderByForClause(ctx_1.dest) != null) {
          throw new SemanticException(ErrorMsg.ORDERBY_SORTBY_CONFLICT
              .getMsg(ast));
        }

        break;

      case HiveParser.TOK_ORDERBY:
        // Get the order by aliases - these are aliased to the entries in the
        // select list
        qbp.setOrderByExprForClause(ctx_1.dest, ast);
        if (qbp.getClusterByForClause(ctx_1.dest) != null) {
          throw new SemanticException(ErrorMsg.CLUSTERBY_ORDERBY_CONFLICT
              .getMsg(ast));
        }
        break;

      case HiveParser.TOK_GROUPBY:
        // Get the groupby aliases - these are aliased to the entries in the
        // select list
        if (qbp.getSelForClause(ctx_1.dest).getToken().getType() == HiveParser.TOK_SELECTDI) {
          throw new SemanticException(ErrorMsg.SELECT_DISTINCT_WITH_GROUPBY
              .getMsg(ast));
        }
        qbp.setGroupByExprForClause(ctx_1.dest, ast);
        skipRecursion = true;
        break;

      case HiveParser.TOK_LIMIT:
        qbp.setDestLimit(ctx_1.dest, new Integer(ast.getChild(0).getText()));
        break;

      case HiveParser.TOK_UNION:
        // currently, we dont support subq1 union subq2 - the user has to
        // explicitly say:
        // select * from (subq1 union subq2) subqalias
        if (!qbp.getIsSubQ()) {
          throw new SemanticException(ErrorMsg.UNION_NOTIN_SUBQ.getMsg());
        }

      default:
        skipRecursion = false;
        break;
      }
    }

    if (!skipRecursion) {
      // Iterate over the rest of the children
      int child_count = ast.getChildCount();
      for (int child_pos = 0; child_pos < child_count; ++child_pos) {

        // Recurse
        doPhase1((ASTNode) ast.getChild(child_pos), qb, ctx_1);
      }
    }
  }

  private void getMetaData(QBExpr qbexpr) throws SemanticException {
    if (qbexpr.getOpcode() == QBExpr.Opcode.NULLOP) {
      getMetaData(qbexpr.getQB());
    } else {
      getMetaData(qbexpr.getQBExpr1());
      getMetaData(qbexpr.getQBExpr2());
    }
  }

  @SuppressWarnings("nls")
  public void getMetaData(QB qb) throws SemanticException {
    try {

      LOG.info("Get metadata for source tables");

      // Go over the tables and populate the related structures.
      // We have to materialize the table alias list since we might
      // modify it in the middle for view rewrite.
      List<String> tabAliases = new ArrayList<String>(qb.getTabAliases());
      for (String alias : tabAliases) {
        String tab_name = qb.getTabNameForAlias(alias);
        Table tab = null;
        try {
          tab = db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tab_name);
        } catch (InvalidTableException ite) {
          throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(qb
              .getParseInfo().getSrcForAlias(alias)));
        }
        if (tab.isView()) {
          replaceViewReferenceWithDefinition(qb, tab, tab_name, alias);
          continue;
        }

        if (!InputFormat.class.isAssignableFrom(tab.getInputFormatClass())) {
          throw new SemanticException(ErrorMsg.INVALID_INPUT_FORMAT_TYPE
              .getMsg(qb.getParseInfo().getSrcForAlias(alias)));
        }

        qb.getMetaData().setSrcForAlias(alias, tab);
      }

      LOG.info("Get metadata for subqueries");
      // Go over the subqueries and getMetaData for these
      for (String alias : qb.getSubqAliases()) {
        QBExpr qbexpr = qb.getSubqForAlias(alias);
        getMetaData(qbexpr);
      }

      LOG.info("Get metadata for destination tables");
      // Go over all the destination structures and populate the related
      // metadata
      QBParseInfo qbp = qb.getParseInfo();

      for (String name : qbp.getClauseNamesForDest()) {
        ASTNode ast = qbp.getDestForClause(name);
        switch (ast.getToken().getType()) {
        case HiveParser.TOK_TAB: {
          tableSpec ts = new tableSpec(db, conf, ast);
          if (ts.tableHandle.isView()) {
            throw new SemanticException(ErrorMsg.DML_AGAINST_VIEW.getMsg());
          }

          Class<?> outputFormatClass = ts.tableHandle.getOutputFormatClass();
          if (!HiveOutputFormat.class.isAssignableFrom(outputFormatClass)) {
            throw new SemanticException(ErrorMsg.INVALID_OUTPUT_FORMAT_TYPE
                .getMsg(ast, "The class is " + outputFormatClass.toString()));
          }

          // tableSpec ts is got from the query (user specified),
          // which means the user didn't specify partitions in their query,
          // but whether the table itself is partitioned is not know.
          if (ts.partHandle == null) {
            // This is a table
            qb.getMetaData().setDestForAlias(name, ts.tableHandle);
            // has dynamic as well as static partitions
            if (ts.partSpec != null && ts.partSpec.size() > 0) {
              qb.getMetaData().setPartSpecForAlias(name, ts.partSpec);
            }
          } else {
            // This is a partition
            qb.getMetaData().setDestForAlias(name, ts.partHandle);
          }
          break;
        }

        case HiveParser.TOK_LOCAL_DIR:
        case HiveParser.TOK_DIR: {
          // This is a dfs file
          String fname = stripQuotes(ast.getChild(0).getText());
          if ((!qb.getParseInfo().getIsSubQ())
              && (((ASTNode) ast.getChild(0)).getToken().getType() == HiveParser.TOK_TMP_FILE)) {

            if (qb.isCTAS()) {
              qb.setIsQuery(false);

              // allocate a temporary output dir on the location of the table
              String location = conf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
              try {
                fname = ctx.getExternalTmpFileURI
                  (FileUtils.makeQualified(new Path(location), conf).toUri());

              } catch (Exception e) {
                throw new SemanticException("Error creating temporary folder on: "
                                            + location, e);
              }

            } else {
              qb.setIsQuery(true);
              fname = ctx.getMRTmpFileURI();
            }
            ctx.setResDir(new Path(fname));
          }
          qb.getMetaData().setDestForAlias(name, fname,
              (ast.getToken().getType() == HiveParser.TOK_DIR));
          break;
        }
        default:
          throw new SemanticException("Unknown Token Type "
              + ast.getToken().getType());
        }
      }
    } catch (HiveException e) {
      // Has to use full name to make sure it does not conflict with
      // org.apache.commons.lang.StringUtils
      LOG.error(stringifyException(e));
      throw new SemanticException(e.getMessage(), e);
    }
  }

  private void replaceViewReferenceWithDefinition(QB qb, Table tab,
      String tab_name, String alias) throws SemanticException {

    ParseDriver pd = new ParseDriver();
    ASTNode viewTree;
    final ASTNodeOrigin viewOrigin = new ASTNodeOrigin("VIEW", tab.getTableName(),
        tab.getViewExpandedText(), alias, qb.getParseInfo().getSrcForAlias(
        alias));
    try {
      String viewText = tab.getViewExpandedText();
      // Reparse text, passing null for context to avoid clobbering
      // the top-level token stream.
      ASTNode tree = pd.parse(viewText, null);
      tree = ParseUtils.findRootNonNullToken(tree);
      viewTree = tree;
      Dispatcher nodeOriginDispatcher = new Dispatcher() {
        public Object dispatch(Node nd, java.util.Stack<Node> stack,
            Object... nodeOutputs) {
          ((ASTNode) nd).setOrigin(viewOrigin);
          return null;
        }
      };
      GraphWalker nodeOriginTagger = new DefaultGraphWalker(
          nodeOriginDispatcher);
      nodeOriginTagger.startWalking(java.util.Collections
          .<Node> singleton(viewTree), null);
    } catch (ParseException e) {
      // A user could encounter this if a stored view definition contains
      // an old SQL construct which has been eliminated in a later Hive
      // version, so we need to provide full debugging info to help
      // with fixing the view definition.
      LOG.error(stringifyException(e));
      StringBuilder sb = new StringBuilder();
      sb.append(e.getMessage());
      ErrorMsg.renderOrigin(sb, viewOrigin);
      throw new SemanticException(sb.toString(), e);
    }
    QBExpr qbexpr = new QBExpr(alias);
    doPhase1QBExpr(viewTree, qbexpr, qb.getId(), alias);
    qb.rewriteViewToSubq(alias, tab_name, qbexpr);
  }

  private boolean isPresent(String[] list, String elem) {
    for (String s : list) {
      if (s.equals(elem)) {
        return true;
      }
    }

    return false;
  }

  @SuppressWarnings("nls")
  private void parseJoinCondPopulateAlias(QBJoinTree joinTree, ASTNode condn,
      ArrayList<String> leftAliases, ArrayList<String> rightAliases,
      ArrayList<String> fields) throws SemanticException {
    // String[] allAliases = joinTree.getAllAliases();
    switch (condn.getToken().getType()) {
    case HiveParser.TOK_TABLE_OR_COL:
      String tableOrCol = unescapeIdentifier(condn.getChild(0).getText()
          .toLowerCase());
      unparseTranslator.addIdentifierTranslation((ASTNode) condn.getChild(0));
      if (isPresent(joinTree.getLeftAliases(), tableOrCol)) {
        if (!leftAliases.contains(tableOrCol)) {
          leftAliases.add(tableOrCol);
        }
      } else if (isPresent(joinTree.getRightAliases(), tableOrCol)) {
        if (!rightAliases.contains(tableOrCol)) {
          rightAliases.add(tableOrCol);
        }
      } else {
        // We don't support columns without table prefix in JOIN condition right
        // now.
        // We need to pass Metadata here to know which table the column belongs
        // to.
        throw new SemanticException(ErrorMsg.INVALID_TABLE_ALIAS.getMsg(condn
            .getChild(0)));
      }
      break;

    case HiveParser.Identifier:
      // it may be a field name, return the identifier and let the caller decide
      // whether it is or not
      if (fields != null) {
        fields
            .add(unescapeIdentifier(condn.getToken().getText().toLowerCase()));
      }
      unparseTranslator.addIdentifierTranslation(condn);
      break;
    case HiveParser.Number:
    case HiveParser.StringLiteral:
    case HiveParser.TOK_CHARSETLITERAL:
    case HiveParser.KW_TRUE:
    case HiveParser.KW_FALSE:
      break;

    case HiveParser.TOK_FUNCTION:
      // check all the arguments
      for (int i = 1; i < condn.getChildCount(); i++) {
        parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(i),
            leftAliases, rightAliases, null);
      }
      break;

    default:
      // This is an operator - so check whether it is unary or binary operator
      if (condn.getChildCount() == 1) {
        parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(0),
            leftAliases, rightAliases, null);
      } else if (condn.getChildCount() == 2) {

        ArrayList<String> fields1 = null;
        // if it is a dot operator, remember the field name of the rhs of the
        // left semijoin
        if (joinTree.getNoSemiJoin() == false
            && condn.getToken().getType() == HiveParser.DOT) {
          // get the semijoin rhs table name and field name
          fields1 = new ArrayList<String>();
          int rhssize = rightAliases.size();
          parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(0),
              leftAliases, rightAliases, null);
          String rhsAlias = null;

          if (rightAliases.size() > rhssize) { // the new table is rhs table
            rhsAlias = rightAliases.get(rightAliases.size() - 1);
          }

          parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(1),
              leftAliases, rightAliases, fields1);
          if (rhsAlias != null && fields1.size() > 0) {
            joinTree.addRHSSemijoinColumns(rhsAlias, condn);
          }
        } else {
          parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(0),
              leftAliases, rightAliases, null);
          parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(1),
              leftAliases, rightAliases, fields1);
        }
      } else {
        throw new SemanticException(condn.toStringTree() + " encountered with "
            + condn.getChildCount() + " children");
      }
      break;
    }
  }

  private void populateAliases(ArrayList<String> leftAliases,
      ArrayList<String> rightAliases, ASTNode condn, QBJoinTree joinTree,
      ArrayList<String> leftSrc) throws SemanticException {
    if ((leftAliases.size() != 0) && (rightAliases.size() != 0)) {
      throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_1
          .getMsg(condn));
    }

    if (rightAliases.size() != 0) {
      assert rightAliases.size() == 1;
      joinTree.getExpressions().get(1).add(condn);
    } else if (leftAliases.size() != 0) {
      joinTree.getExpressions().get(0).add(condn);
      for (String s : leftAliases) {
        if (!leftSrc.contains(s)) {
          leftSrc.add(s);
        }
      }
    } else {
      throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_2
          .getMsg(condn));
    }
  }

  /**
   * Parse the join condition. If the condition is a join condition, throw an
   * error if it is not an equality. Otherwise, break it into left and right
   * expressions and store in the join tree. If the condition is a join filter,
   * add it to the filter list of join tree. The join condition can contains
   * conditions on both the left and tree trees and filters on either.
   * Currently, we only support equi-joins, so we throw an error if the
   * condition involves both subtrees and is not a equality. Also, we only
   * support AND i.e ORs are not supported currently as their semantics are not
   * very clear, may lead to data explosion and there is no usecase.
   *
   * @param joinTree
   *          jointree to be populated
   * @param joinCond
   *          join condition
   * @param leftSrc
   *          left sources
   * @throws SemanticException
   */
  private void parseJoinCondition(QBJoinTree joinTree, ASTNode joinCond,
      ArrayList<String> leftSrc) throws SemanticException {
    if (joinCond == null) {
      return;
    }

    switch (joinCond.getToken().getType()) {
    case HiveParser.KW_OR:
      throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_3
          .getMsg(joinCond));

    case HiveParser.KW_AND:
      parseJoinCondition(joinTree, (ASTNode) joinCond.getChild(0), leftSrc);
      parseJoinCondition(joinTree, (ASTNode) joinCond.getChild(1), leftSrc);
      break;

    case HiveParser.EQUAL:
      ASTNode leftCondn = (ASTNode) joinCond.getChild(0);
      ArrayList<String> leftCondAl1 = new ArrayList<String>();
      ArrayList<String> leftCondAl2 = new ArrayList<String>();
      parseJoinCondPopulateAlias(joinTree, leftCondn, leftCondAl1, leftCondAl2,
          null);

      ASTNode rightCondn = (ASTNode) joinCond.getChild(1);
      ArrayList<String> rightCondAl1 = new ArrayList<String>();
      ArrayList<String> rightCondAl2 = new ArrayList<String>();
      parseJoinCondPopulateAlias(joinTree, rightCondn, rightCondAl1,
          rightCondAl2, null);

      // is it a filter or a join condition
      if (((leftCondAl1.size() != 0) && (leftCondAl2.size() != 0))
          || ((rightCondAl1.size() != 0) && (rightCondAl2.size() != 0))) {
        throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_1
            .getMsg(joinCond));
      }

      if (leftCondAl1.size() != 0) {
        if ((rightCondAl1.size() != 0)
            || ((rightCondAl1.size() == 0) && (rightCondAl2.size() == 0))) {
          joinTree.getFilters().get(0).add(joinCond);
        } else if (rightCondAl2.size() != 0) {
          populateAliases(leftCondAl1, leftCondAl2, leftCondn, joinTree,
              leftSrc);
          populateAliases(rightCondAl1, rightCondAl2, rightCondn, joinTree,
              leftSrc);
        }
      } else if (leftCondAl2.size() != 0) {
        if ((rightCondAl2.size() != 0)
            || ((rightCondAl1.size() == 0) && (rightCondAl2.size() == 0))) {
          joinTree.getFilters().get(1).add(joinCond);
        } else if (rightCondAl1.size() != 0) {
          populateAliases(leftCondAl1, leftCondAl2, leftCondn, joinTree,
              leftSrc);
          populateAliases(rightCondAl1, rightCondAl2, rightCondn, joinTree,
              leftSrc);
        }
      } else if (rightCondAl1.size() != 0) {
        joinTree.getFilters().get(0).add(joinCond);
      } else {
        joinTree.getFilters().get(1).add(joinCond);
      }

      break;

    default:
      boolean isFunction = (joinCond.getType() == HiveParser.TOK_FUNCTION);

      // Create all children
      int childrenBegin = (isFunction ? 1 : 0);
      ArrayList<ArrayList<String>> leftAlias = new ArrayList<ArrayList<String>>(
          joinCond.getChildCount() - childrenBegin);
      ArrayList<ArrayList<String>> rightAlias = new ArrayList<ArrayList<String>>(
          joinCond.getChildCount() - childrenBegin);
      for (int ci = 0; ci < joinCond.getChildCount() - childrenBegin; ci++) {
        ArrayList<String> left = new ArrayList<String>();
        ArrayList<String> right = new ArrayList<String>();
        leftAlias.add(left);
        rightAlias.add(right);
      }

      for (int ci = childrenBegin; ci < joinCond.getChildCount(); ci++) {
        parseJoinCondPopulateAlias(joinTree, (ASTNode) joinCond.getChild(ci),
            leftAlias.get(ci - childrenBegin), rightAlias.get(ci
            - childrenBegin), null);
      }

      boolean leftAliasNull = true;
      for (ArrayList<String> left : leftAlias) {
        if (left.size() != 0) {
          leftAliasNull = false;
          break;
        }
      }

      boolean rightAliasNull = true;
      for (ArrayList<String> right : rightAlias) {
        if (right.size() != 0) {
          rightAliasNull = false;
          break;
        }
      }

      if (!leftAliasNull && !rightAliasNull) {
        throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_1
            .getMsg(joinCond));
      }

      if (!leftAliasNull) {
        joinTree.getFilters().get(0).add(joinCond);
      } else {
        joinTree.getFilters().get(1).add(joinCond);
      }

      break;
    }
  }

  @SuppressWarnings("nls")
  public <T extends Serializable> Operator<T> putOpInsertMap(Operator<T> op,
      RowResolver rr) {
    OpParseContext ctx = new OpParseContext(rr);
    opParseCtx.put(op, ctx);
    op.augmentPlan();
    return op;
  }

  @SuppressWarnings("nls")
  private Operator genFilterPlan(String dest, QB qb, Operator input)
      throws SemanticException {

    ASTNode whereExpr = qb.getParseInfo().getWhrForClause(dest);
    return genFilterPlan(qb, (ASTNode) whereExpr.getChild(0), input);
  }

  /**
   * create a filter plan. The condition and the inputs are specified.
   *
   * @param qb
   *          current query block
   * @param condn
   *          The condition to be resolved
   * @param input
   *          the input operator
   */
  @SuppressWarnings("nls")
  private Operator genFilterPlan(QB qb, ASTNode condn, Operator input)
      throws SemanticException {

    OpParseContext inputCtx = opParseCtx.get(input);
    RowResolver inputRR = inputCtx.getRR();
    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new FilterDesc(genExprNodeDesc(condn, inputRR), false), new RowSchema(
        inputRR.getColumnInfos()), input), inputRR);

    LOG.debug("Created Filter Plan for " + qb.getId() + " row schema: "
        + inputRR.toString());
    return output;
  }

  @SuppressWarnings("nls")
  private Integer genColListRegex(String colRegex, String tabAlias,
      String alias, ASTNode sel, ArrayList<ExprNodeDesc> col_list,
      RowResolver input, Integer pos, RowResolver output)
      throws SemanticException {

    // The table alias should exist
    if (tabAlias != null && !input.hasTableAlias(tabAlias)) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE_ALIAS.getMsg(sel));
    }

    // TODO: Have to put in the support for AS clause
    Pattern regex = null;
    try {
      regex = Pattern.compile(colRegex, Pattern.CASE_INSENSITIVE);
    } catch (PatternSyntaxException e) {
      throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(sel, e
          .getMessage()));
    }

    StringBuilder replacementText = new StringBuilder();
    int matched = 0;
    // This is the tab.* case
    // In this case add all the columns to the fieldList
    // from the input schema
    for (ColumnInfo colInfo : input.getColumnInfos()) {
      String name = colInfo.getInternalName();
      String[] tmp = input.reverseLookup(name);

      // Skip the colinfos which are not for this particular alias
      if (tabAlias != null && !tmp[0].equalsIgnoreCase(tabAlias)) {
        continue;
      }

      // Not matching the regex?
      if (!regex.matcher(tmp[1]).matches()) {
        continue;
      }

      ExprNodeColumnDesc expr = new ExprNodeColumnDesc(colInfo.getType(), name,
          colInfo.getTabAlias(), colInfo.getIsPartitionCol());
      col_list.add(expr);
      output.put(tmp[0], tmp[1],
          new ColumnInfo(getColumnInternalName(pos), colInfo.getType(), colInfo
          .getTabAlias(), colInfo.getIsPartitionCol()));
      pos = Integer.valueOf(pos.intValue() + 1);
      matched++;

      if (unparseTranslator.isEnabled()) {
        if (replacementText.length() > 0) {
          replacementText.append(", ");
        }
        replacementText.append(HiveUtils.unparseIdentifier(tmp[0]));
        replacementText.append(".");
        replacementText.append(HiveUtils.unparseIdentifier(tmp[1]));
      }
    }
    if (matched == 0) {
      throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(sel));
    }

    if (unparseTranslator.isEnabled()) {
      unparseTranslator.addTranslation(sel, replacementText.toString());
    }
    return pos;
  }

  public static String getColumnInternalName(int pos) {
    return HiveConf.getColumnInternalName(pos);
  }

  /**
   * If the user script command needs any modifications - do it here.
   */
  private String getFixedCmd(String cmd) {
    SessionState ss = SessionState.get();
    if (ss == null) {
      return cmd;
    }

    // for local mode - replace any references to packaged files by name with
    // the reference to the original file path
    if (ss.getConf().get("mapred.job.tracker", "local").equals("local")) {
      Set<String> files = ss
          .list_resource(SessionState.ResourceType.FILE, null);
      if ((files != null) && !files.isEmpty()) {
        int end = cmd.indexOf(" ");
        String prog = (end == -1) ? cmd : cmd.substring(0, end);
        String args = (end == -1) ? "" : cmd.substring(end, cmd.length());

        for (String oneFile : files) {
          Path p = new Path(oneFile);
          if (p.getName().equals(prog)) {
            cmd = oneFile + args;
            break;
          }
        }
      }
    }

    return cmd;
  }

  private TableDesc getTableDescFromSerDe(ASTNode child, String cols,
      String colTypes, boolean defaultCols) throws SemanticException {
    if (child.getType() == HiveParser.TOK_SERDENAME) {
      String serdeName = unescapeSQLString(child.getChild(0).getText());
      Class<? extends Deserializer> serdeClass = null;

      try {
        serdeClass = (Class<? extends Deserializer>) Class.forName(serdeName,
            true, JavaUtils.getClassLoader());
      } catch (ClassNotFoundException e) {
        throw new SemanticException(e);
      }

      TableDesc tblDesc = PlanUtils.getTableDesc(serdeClass, Integer
          .toString(Utilities.tabCode), cols, colTypes, defaultCols, true);
      // copy all the properties
      if (child.getChildCount() == 2) {
        ASTNode prop = (ASTNode) ((ASTNode) child.getChild(1)).getChild(0);
        for (int propChild = 0; propChild < prop.getChildCount(); propChild++) {
          String key = unescapeSQLString(prop.getChild(propChild).getChild(0)
              .getText());
          String value = unescapeSQLString(prop.getChild(propChild).getChild(1)
              .getText());
          tblDesc.getProperties().setProperty(key, value);
        }
      }
      return tblDesc;
    } else if (child.getType() == HiveParser.TOK_SERDEPROPS) {
      TableDesc tblDesc = PlanUtils.getDefaultTableDesc(Integer
          .toString(Utilities.ctrlaCode), cols, colTypes, defaultCols);
      int numChildRowFormat = child.getChildCount();
      for (int numC = 0; numC < numChildRowFormat; numC++) {
        ASTNode rowChild = (ASTNode) child.getChild(numC);
        switch (rowChild.getToken().getType()) {
        case HiveParser.TOK_TABLEROWFORMATFIELD:
          String fieldDelim = unescapeSQLString(rowChild.getChild(0).getText());
          tblDesc.getProperties()
              .setProperty(Constants.FIELD_DELIM, fieldDelim);
          tblDesc.getProperties().setProperty(Constants.SERIALIZATION_FORMAT,
              fieldDelim);

          if (rowChild.getChildCount() >= 2) {
            String fieldEscape = unescapeSQLString(rowChild.getChild(1)
                .getText());
            tblDesc.getProperties().setProperty(Constants.ESCAPE_CHAR,
                fieldEscape);
          }
          break;
        case HiveParser.TOK_TABLEROWFORMATCOLLITEMS:
          tblDesc.getProperties().setProperty(Constants.COLLECTION_DELIM,
              unescapeSQLString(rowChild.getChild(0).getText()));
          break;
        case HiveParser.TOK_TABLEROWFORMATMAPKEYS:
          tblDesc.getProperties().setProperty(Constants.MAPKEY_DELIM,
              unescapeSQLString(rowChild.getChild(0).getText()));
          break;
        case HiveParser.TOK_TABLEROWFORMATLINES:
          String lineDelim = unescapeSQLString(rowChild.getChild(0).getText());
          tblDesc.getProperties().setProperty(Constants.LINE_DELIM, lineDelim);
          if (!lineDelim.equals("\n") && !lineDelim.equals("10")) {
            throw new SemanticException(
                ErrorMsg.LINES_TERMINATED_BY_NON_NEWLINE.getMsg());
          }
          break;
        default:
          assert false;
        }
      }

      return tblDesc;
    }

    // should never come here
    return null;
  }

  private void failIfColAliasExists(Set<String> nameSet, String name)
      throws SemanticException {
    if (nameSet.contains(name)) {
      throw new SemanticException(ErrorMsg.COLUMN_ALIAS_ALREADY_EXISTS
          .getMsg(name));
    }
    nameSet.add(name);
  }

  @SuppressWarnings("nls")
  private Operator genScriptPlan(ASTNode trfm, QB qb, Operator input)
      throws SemanticException {
    // If there is no "AS" clause, the output schema will be "key,value"
    ArrayList<ColumnInfo> outputCols = new ArrayList<ColumnInfo>();
    int inputSerDeNum = 1, inputRecordWriterNum = 2;
    int outputSerDeNum = 4, outputRecordReaderNum = 5;
    int outputColsNum = 6;
    boolean outputColNames = false, outputColSchemas = false;
    int execPos = 3;
    boolean defaultOutputCols = false;

    // Go over all the children
    if (trfm.getChildCount() > outputColsNum) {
      ASTNode outCols = (ASTNode) trfm.getChild(outputColsNum);
      if (outCols.getType() == HiveParser.TOK_ALIASLIST) {
        outputColNames = true;
      } else if (outCols.getType() == HiveParser.TOK_TABCOLLIST) {
        outputColSchemas = true;
      }
    }

    // If column type is not specified, use a string
    if (!outputColNames && !outputColSchemas) {
      String intName = getColumnInternalName(0);
      ColumnInfo colInfo = new ColumnInfo(intName,
          TypeInfoFactory.stringTypeInfo, null, false);
      colInfo.setAlias("key");
      outputCols.add(colInfo);
      intName = getColumnInternalName(1);
      colInfo = new ColumnInfo(intName, TypeInfoFactory.stringTypeInfo, null,
          false);
      colInfo.setAlias("value");
      outputCols.add(colInfo);
      defaultOutputCols = true;
    } else {
      ASTNode collist = (ASTNode) trfm.getChild(outputColsNum);
      int ccount = collist.getChildCount();

      Set<String> colAliasNamesDuplicateCheck = new HashSet<String>();
      if (outputColNames) {
        for (int i = 0; i < ccount; ++i) {
          String colAlias = unescapeIdentifier(((ASTNode) collist.getChild(i))
              .getText());
          failIfColAliasExists(colAliasNamesDuplicateCheck, colAlias);
          String intName = getColumnInternalName(i);
          ColumnInfo colInfo = new ColumnInfo(intName,
              TypeInfoFactory.stringTypeInfo, null, false);
          colInfo.setAlias(colAlias);
          outputCols.add(colInfo);
        }
      } else {
        for (int i = 0; i < ccount; ++i) {
          ASTNode child = (ASTNode) collist.getChild(i);
          assert child.getType() == HiveParser.TOK_TABCOL;
          String colAlias = unescapeIdentifier(((ASTNode) child.getChild(0))
              .getText());
          failIfColAliasExists(colAliasNamesDuplicateCheck, colAlias);
          String intName = getColumnInternalName(i);
          ColumnInfo colInfo = new ColumnInfo(intName, TypeInfoUtils
              .getTypeInfoFromTypeString(getTypeStringFromAST((ASTNode) child
              .getChild(1))), null, false);
          colInfo.setAlias(colAlias);
          outputCols.add(colInfo);
        }
      }
    }

    RowResolver out_rwsch = new RowResolver();
    StringBuilder columns = new StringBuilder();
    StringBuilder columnTypes = new StringBuilder();

    for (int i = 0; i < outputCols.size(); ++i) {
      if (i != 0) {
        columns.append(",");
        columnTypes.append(",");
      }

      columns.append(outputCols.get(i).getInternalName());
      columnTypes.append(outputCols.get(i).getType().getTypeName());

      out_rwsch.put(qb.getParseInfo().getAlias(), outputCols.get(i).getAlias(),
          outputCols.get(i));
    }

    StringBuilder inpColumns = new StringBuilder();
    StringBuilder inpColumnTypes = new StringBuilder();
    ArrayList<ColumnInfo> inputSchema = opParseCtx.get(input).getRR()
        .getColumnInfos();
    for (int i = 0; i < inputSchema.size(); ++i) {
      if (i != 0) {
        inpColumns.append(",");
        inpColumnTypes.append(",");
      }

      inpColumns.append(inputSchema.get(i).getInternalName());
      inpColumnTypes.append(inputSchema.get(i).getType().getTypeName());
    }

    TableDesc outInfo;
    TableDesc errInfo;
    TableDesc inInfo;
    String defaultSerdeName = conf.getVar(HiveConf.ConfVars.HIVESCRIPTSERDE);
    Class<? extends Deserializer> serde;

    try {
      serde = (Class<? extends Deserializer>) Class.forName(defaultSerdeName,
          true, JavaUtils.getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new SemanticException(e);
    }

    // Input and Output Serdes
    if (trfm.getChild(inputSerDeNum).getChildCount() > 0) {
      inInfo = getTableDescFromSerDe((ASTNode) (((ASTNode) trfm
          .getChild(inputSerDeNum))).getChild(0), inpColumns.toString(),
          inpColumnTypes.toString(), false);
    } else {
      inInfo = PlanUtils.getTableDesc(serde, Integer
          .toString(Utilities.tabCode), inpColumns.toString(), inpColumnTypes
          .toString(), false, true);
    }

    if (trfm.getChild(outputSerDeNum).getChildCount() > 0) {
      outInfo = getTableDescFromSerDe((ASTNode) (((ASTNode) trfm
          .getChild(outputSerDeNum))).getChild(0), columns.toString(),
          columnTypes.toString(), false);
      // This is for backward compatibility. If the user did not specify the
      // output column list, we assume that there are 2 columns: key and value.
      // However, if the script outputs: col1, col2, col3 seperated by TAB, the
      // requirement is: key is col and value is (col2 TAB col3)
    } else {
      outInfo = PlanUtils.getTableDesc(serde, Integer
          .toString(Utilities.tabCode), columns.toString(), columnTypes
          .toString(), defaultOutputCols);
    }

    // Error stream always uses the default serde with a single column
    errInfo = PlanUtils.getTableDesc(serde, Integer.toString(Utilities.tabCode), "KEY");

    // Output record readers
    Class<? extends RecordReader> outRecordReader = getRecordReader((ASTNode) trfm
        .getChild(outputRecordReaderNum));
    Class<? extends RecordWriter> inRecordWriter = getRecordWriter((ASTNode) trfm
        .getChild(inputRecordWriterNum));
    Class<? extends RecordReader> errRecordReader = getDefaultRecordReader();

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new ScriptDesc(
        getFixedCmd(stripQuotes(trfm.getChild(execPos).getText())), inInfo,
        inRecordWriter, outInfo, outRecordReader, errRecordReader, errInfo),
        new RowSchema(out_rwsch.getColumnInfos()), input), out_rwsch);

    return output;
  }

  private Class<? extends RecordReader> getRecordReader(ASTNode node)
      throws SemanticException {
    String name;

    if (node.getChildCount() == 0) {
      name = conf.getVar(HiveConf.ConfVars.HIVESCRIPTRECORDREADER);
    } else {
      name = unescapeSQLString(node.getChild(0).getText());
    }

    try {
      return (Class<? extends RecordReader>) Class.forName(name, true,
          JavaUtils.getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new SemanticException(e);
    }
  }

  private Class<? extends RecordReader> getDefaultRecordReader()
      throws SemanticException {
    String name;

    name = conf.getVar(HiveConf.ConfVars.HIVESCRIPTRECORDREADER);

    try {
      return (Class<? extends RecordReader>) Class.forName(name, true,
          JavaUtils.getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new SemanticException(e);
    }
  }

  private Class<? extends RecordWriter> getRecordWriter(ASTNode node)
      throws SemanticException {
    String name;

    if (node.getChildCount() == 0) {
      name = conf.getVar(HiveConf.ConfVars.HIVESCRIPTRECORDWRITER);
    } else {
      name = unescapeSQLString(node.getChild(0).getText());
    }

    try {
      return (Class<? extends RecordWriter>) Class.forName(name, true,
          JavaUtils.getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new SemanticException(e);
    }
  }

  /**
   * This function is a wrapper of parseInfo.getGroupByForClause which
   * automatically translates SELECT DISTINCT a,b,c to SELECT a,b,c GROUP BY
   * a,b,c.
   */
  static List<ASTNode> getGroupByForClause(QBParseInfo parseInfo, String dest) {
    if (parseInfo.getSelForClause(dest).getToken().getType() == HiveParser.TOK_SELECTDI) {
      ASTNode selectExprs = parseInfo.getSelForClause(dest);
      List<ASTNode> result = new ArrayList<ASTNode>(selectExprs == null ? 0
          : selectExprs.getChildCount());
      if (selectExprs != null) {
        for (int i = 0; i < selectExprs.getChildCount(); ++i) {
          // table.column AS alias
          ASTNode grpbyExpr = (ASTNode) selectExprs.getChild(i).getChild(0);
          result.add(grpbyExpr);
        }
      }
      return result;
    } else {
      ASTNode grpByExprs = parseInfo.getGroupByForClause(dest);
      List<ASTNode> result = new ArrayList<ASTNode>(grpByExprs == null ? 0
          : grpByExprs.getChildCount());
      if (grpByExprs != null) {
        for (int i = 0; i < grpByExprs.getChildCount(); ++i) {
          ASTNode grpbyExpr = (ASTNode) grpByExprs.getChild(i);
          result.add(grpbyExpr);
        }
      }
      return result;
    }
  }

  private static String[] getColAlias(ASTNode selExpr, String defaultName,
      RowResolver inputRR) {
    String colAlias = null;
    String tabAlias = null;
    String[] colRef = new String[2];

    if (selExpr.getChildCount() == 2) {
      // return zz for "xx + yy AS zz"
      colAlias = unescapeIdentifier(selExpr.getChild(1).getText());
      colRef[0] = tabAlias;
      colRef[1] = colAlias;
      return colRef;
    }

    ASTNode root = (ASTNode) selExpr.getChild(0);
    if (root.getType() == HiveParser.TOK_TABLE_OR_COL) {
      colAlias = root.getChild(0).getText();
      colRef[0] = tabAlias;
      colRef[1] = colAlias;
      return colRef;
    }

    if (root.getType() == HiveParser.DOT) {
      ASTNode tab = (ASTNode) root.getChild(0);
      if (tab.getType() == HiveParser.TOK_TABLE_OR_COL) {
        String t = unescapeIdentifier(tab.getChild(0).getText());
        if (inputRR.hasTableAlias(t)) {
          tabAlias = t;
        }
      }

      // Return zz for "xx.zz" and "xx.yy.zz"
      ASTNode col = (ASTNode) root.getChild(1);
      if (col.getType() == HiveParser.Identifier) {
        colAlias = unescapeIdentifier(col.getText());
      }
    }

    if (colAlias == null) {
      // Return defaultName if selExpr is not a simple xx.yy.zz
      colAlias = defaultName;
    }

    colRef[0] = tabAlias;
    colRef[1] = colAlias;
    return colRef;
  }

  /**
   * Returns whether the pattern is a regex expression (instead of a normal
   * string). Normal string is a string with all alphabets/digits and "_".
   */
  private static boolean isRegex(String pattern) {
    for (int i = 0; i < pattern.length(); i++) {
      if (!Character.isLetterOrDigit(pattern.charAt(i))
          && pattern.charAt(i) != '_') {
        return true;
      }
    }
    return false;
  }

  private Operator<?> genSelectPlan(String dest, QB qb, Operator<?> input)
      throws SemanticException {
    ASTNode selExprList = qb.getParseInfo().getSelForClause(dest);

    Operator<?> op = genSelectPlan(selExprList, qb, input);
    LOG.debug("Created Select Plan for clause: " + dest);
    return op;
  }

  @SuppressWarnings("nls")
  private Operator<?> genSelectPlan(ASTNode selExprList, QB qb,
      Operator<?> input) throws SemanticException {
    LOG.debug("tree: " + selExprList.toStringTree());

    ArrayList<ExprNodeDesc> col_list = new ArrayList<ExprNodeDesc>();
    RowResolver out_rwsch = new RowResolver();
    ASTNode trfm = null;
    String alias = qb.getParseInfo().getAlias();
    Integer pos = Integer.valueOf(0);
    RowResolver inputRR = opParseCtx.get(input).getRR();
    // SELECT * or SELECT TRANSFORM(*)
    boolean selectStar = false;
    int posn = 0;
    boolean hintPresent = (selExprList.getChild(0).getType() == HiveParser.TOK_HINTLIST);
    if (hintPresent) {
      posn++;
    }

    boolean isInTransform = (selExprList.getChild(posn).getChild(0).getType() ==
      HiveParser.TOK_TRANSFORM);
    if (isInTransform) {
      trfm = (ASTNode) selExprList.getChild(posn).getChild(0);
    }

    // Detect queries of the form SELECT udtf(col) AS ...
    // by looking for a function as the first child, and then checking to see
    // if the function is a Generic UDTF. It's not as clean as TRANSFORM due to
    // the lack of a special token.
    boolean isUDTF = false;
    String udtfTableAlias = null;
    ArrayList<String> udtfColAliases = new ArrayList<String>();
    ASTNode udtfExpr = (ASTNode) selExprList.getChild(posn).getChild(0);
    GenericUDTF genericUDTF = null;

    int udtfExprType = udtfExpr.getType();
    if (udtfExprType == HiveParser.TOK_FUNCTION
        || udtfExprType == HiveParser.TOK_FUNCTIONSTAR) {
      String funcName = TypeCheckProcFactory.DefaultExprProcessor
          .getFunctionText(udtfExpr, true);
      FunctionInfo fi = FunctionRegistry.getFunctionInfo(funcName);
      if (fi != null) {
        genericUDTF = fi.getGenericUDTF();
      }
      isUDTF = (genericUDTF != null);
      if (isUDTF && !fi.isNative()) {
        unparseTranslator.addIdentifierTranslation((ASTNode) udtfExpr
            .getChild(0));
      }
    }

    if (isUDTF) {
      // Only support a single expression when it's a UDTF
      if (selExprList.getChildCount() > 1) {
        throw new SemanticException(ErrorMsg.UDTF_MULTIPLE_EXPR.getMsg());
      }
      // Require an AS for UDTFs for column aliases
      ASTNode selExpr = (ASTNode) selExprList.getChild(posn);
      if (selExpr.getChildCount() < 2) {
        throw new SemanticException(ErrorMsg.UDTF_REQUIRE_AS.getMsg());
      }
      // Get the column / table aliases from the expression. Start from 1 as
      // 0 is the TOK_FUNCTION
      for (int i = 1; i < selExpr.getChildCount(); i++) {
        ASTNode selExprChild = (ASTNode) selExpr.getChild(i);
        switch (selExprChild.getType()) {
        case HiveParser.Identifier:
          udtfColAliases.add(unescapeIdentifier(selExprChild.getText()));
          unparseTranslator.addIdentifierTranslation(selExprChild);
          break;
        case HiveParser.TOK_TABALIAS:
          assert (selExprChild.getChildCount() == 1);
          udtfTableAlias = unescapeIdentifier(selExprChild.getChild(0)
              .getText());
          unparseTranslator.addIdentifierTranslation((ASTNode) selExprChild
              .getChild(0));
          break;
        default:
          assert (false);
        }
      }
      LOG.debug("UDTF table alias is " + udtfTableAlias);
      LOG.debug("UDTF col aliases are " + udtfColAliases);
    }

    // The list of expressions after SELECT or SELECT TRANSFORM.
    ASTNode exprList;
    if (isInTransform) {
      exprList = (ASTNode) trfm.getChild(0);
    } else if (isUDTF) {
      exprList = udtfExpr;
    } else {
      exprList = selExprList;
    }

    LOG.debug("genSelectPlan: input = " + inputRR.toString());

    // For UDTF's, skip the function name to get the expressions
    int startPosn = isUDTF ? posn + 1 : posn;
    if (isInTransform) {
      startPosn = 0;
    }

    // Iterate over all expression (either after SELECT, or in SELECT TRANSFORM)
    for (int i = startPosn; i < exprList.getChildCount(); ++i) {

      // child can be EXPR AS ALIAS, or EXPR.
      ASTNode child = (ASTNode) exprList.getChild(i);
      boolean hasAsClause = (!isInTransform) && (child.getChildCount() == 2);

      // EXPR AS (ALIAS,...) parses, but is only allowed for UDTF's
      // This check is not needed and invalid when there is a transform b/c the
      // AST's are slightly different.
      if (!isInTransform && !isUDTF && child.getChildCount() > 2) {
        throw new SemanticException(ErrorMsg.INVALID_AS.getMsg());
      }

      // The real expression
      ASTNode expr;
      String tabAlias;
      String colAlias;

      if (isInTransform || isUDTF) {
        tabAlias = null;
        colAlias = "_C" + i;
        expr = child;
      } else {
        String[] colRef = getColAlias(child, "_C" + i, inputRR);
        tabAlias = colRef[0];
        colAlias = colRef[1];
        if (hasAsClause) {
          unparseTranslator.addIdentifierTranslation((ASTNode) child
              .getChild(1));
        }
        // Get rid of TOK_SELEXPR
        expr = (ASTNode) child.getChild(0);
      }

      if (expr.getType() == HiveParser.TOK_ALLCOLREF) {
        pos = genColListRegex(".*", expr.getChildCount() == 0 ? null
            : unescapeIdentifier(expr.getChild(0).getText().toLowerCase()),
            alias, expr, col_list, inputRR, pos, out_rwsch);
        selectStar = true;
      } else if (expr.getType() == HiveParser.TOK_TABLE_OR_COL && !hasAsClause
          && !inputRR.getIsExprResolver()
          && isRegex(unescapeIdentifier(expr.getChild(0).getText()))) {
        // In case the expression is a regex COL.
        // This can only happen without AS clause
        // We don't allow this for ExprResolver - the Group By case
        pos = genColListRegex(unescapeIdentifier(expr.getChild(0).getText()),
            null, alias, expr, col_list, inputRR, pos, out_rwsch);
      } else if (expr.getType() == HiveParser.DOT
          && expr.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
          && inputRR.hasTableAlias(unescapeIdentifier(expr.getChild(0)
          .getChild(0).getText().toLowerCase())) && !hasAsClause
          && !inputRR.getIsExprResolver()
          && isRegex(unescapeIdentifier(expr.getChild(1).getText()))) {
        // In case the expression is TABLE.COL (col can be regex).
        // This can only happen without AS clause
        // We don't allow this for ExprResolver - the Group By case
        pos = genColListRegex(unescapeIdentifier(expr.getChild(1).getText()),
            unescapeIdentifier(expr.getChild(0).getChild(0).getText()
            .toLowerCase()), alias, expr, col_list, inputRR, pos, out_rwsch);
      } else {
        // Case when this is an expression
        ExprNodeDesc exp = genExprNodeDesc(expr, inputRR);
        col_list.add(exp);
        if (!StringUtils.isEmpty(alias)
            && (out_rwsch.get(null, colAlias) != null)) {
          throw new SemanticException(ErrorMsg.AMBIGUOUS_COLUMN.getMsg(colAlias));
        }
        out_rwsch.put(tabAlias, colAlias, new ColumnInfo(
            getColumnInternalName(pos), exp.getTypeInfo(), tabAlias, false));

        pos = Integer.valueOf(pos.intValue() + 1);
      }
    }
    selectStar = selectStar && exprList.getChildCount() == posn + 1;

    ArrayList<String> columnNames = new ArrayList<String>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    for (int i = 0; i < col_list.size(); i++) {
      // Replace NULL with CAST(NULL AS STRING)
      if (col_list.get(i) instanceof ExprNodeNullDesc) {
        col_list.set(i, new ExprNodeConstantDesc(
            TypeInfoFactory.stringTypeInfo, null));
      }
      String outputCol = getColumnInternalName(i);
      colExprMap.put(outputCol, col_list.get(i));
      columnNames.add(outputCol);
    }

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new SelectDesc(col_list, columnNames, selectStar), new RowSchema(
        out_rwsch.getColumnInfos()), input), out_rwsch);

    output.setColumnExprMap(colExprMap);
    if (isInTransform) {
      output = genScriptPlan(trfm, qb, output);
    }

    if (isUDTF) {
      output = genUDTFPlan(genericUDTF, udtfTableAlias, udtfColAliases, qb,
          output);
    }
    LOG.debug("Created Select Plan row schema: " + out_rwsch.toString());
    return output;
  }

  /**
   * Class to store GenericUDAF related information.
   */
  static class GenericUDAFInfo {
    ArrayList<ExprNodeDesc> convertedParameters;
    GenericUDAFEvaluator genericUDAFEvaluator;
    TypeInfo returnType;
  }

  /**
   * Convert exprNodeDesc array to Typeinfo array.
   */
  static ArrayList<TypeInfo> getTypeInfo(ArrayList<ExprNodeDesc> exprs) {
    ArrayList<TypeInfo> result = new ArrayList<TypeInfo>();
    for (ExprNodeDesc expr : exprs) {
      result.add(expr.getTypeInfo());
    }
    return result;
  }

  /**
   * Convert exprNodeDesc array to Typeinfo array.
   */
  static ObjectInspector[] getStandardObjectInspector(ArrayList<TypeInfo> exprs) {
    ObjectInspector[] result = new ObjectInspector[exprs.size()];
    for (int i = 0; i < exprs.size(); i++) {
      result[i] = TypeInfoUtils
          .getStandardWritableObjectInspectorFromTypeInfo(exprs.get(i));
    }
    return result;
  }

  /**
   * Returns the GenericUDAFEvaluator for the aggregation. This is called once
   * for each GroupBy aggregation.
   */
  static GenericUDAFEvaluator getGenericUDAFEvaluator(String aggName,
      ArrayList<ExprNodeDesc> aggParameters, ASTNode aggTree,
      boolean isDistinct, boolean isAllColumns)
      throws SemanticException {
    ArrayList<TypeInfo> originalParameterTypeInfos = getTypeInfo(aggParameters);
    GenericUDAFEvaluator result = FunctionRegistry.getGenericUDAFEvaluator(
        aggName, originalParameterTypeInfos, isDistinct, isAllColumns);
    if (null == result) {
      String reason = "Looking for UDAF Evaluator\"" + aggName
          + "\" with parameters " + originalParameterTypeInfos;
      throw new SemanticException(ErrorMsg.INVALID_FUNCTION_SIGNATURE.getMsg(
          (ASTNode) aggTree.getChild(0), reason));
    }
    return result;
  }

  /**
   * Returns the GenericUDAFInfo struct for the aggregation.
   *
   * @param aggName
   *          The name of the UDAF.
   * @param aggParameters
   *          The exprNodeDesc of the original parameters
   * @param aggTree
   *          The ASTNode node of the UDAF in the query.
   * @return GenericUDAFInfo
   * @throws SemanticException
   *           when the UDAF is not found or has problems.
   */
  static GenericUDAFInfo getGenericUDAFInfo(GenericUDAFEvaluator evaluator,
      GenericUDAFEvaluator.Mode emode, ArrayList<ExprNodeDesc> aggParameters)
      throws SemanticException {

    GenericUDAFInfo r = new GenericUDAFInfo();

    // set r.genericUDAFEvaluator
    r.genericUDAFEvaluator = evaluator;

    // set r.returnType
    ObjectInspector returnOI = null;
    try {
      ObjectInspector[] aggObjectInspectors =
          getStandardObjectInspector(getTypeInfo(aggParameters));
      returnOI = r.genericUDAFEvaluator.init(emode, aggObjectInspectors);
      r.returnType = TypeInfoUtils.getTypeInfoFromObjectInspector(returnOI);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
    // set r.convertedParameters
    // TODO: type conversion
    r.convertedParameters = aggParameters;

    return r;
  }

  private static GenericUDAFEvaluator.Mode groupByDescModeToUDAFMode(
      GroupByDesc.Mode mode, boolean isDistinct) {
    switch (mode) {
    case COMPLETE:
      return GenericUDAFEvaluator.Mode.COMPLETE;
    case PARTIAL1:
      return GenericUDAFEvaluator.Mode.PARTIAL1;
    case PARTIAL2:
      return GenericUDAFEvaluator.Mode.PARTIAL2;
    case PARTIALS:
      return isDistinct ? GenericUDAFEvaluator.Mode.PARTIAL1
          : GenericUDAFEvaluator.Mode.PARTIAL2;
    case FINAL:
      return GenericUDAFEvaluator.Mode.FINAL;
    case HASH:
      return GenericUDAFEvaluator.Mode.PARTIAL1;
    case MERGEPARTIAL:
      return isDistinct ? GenericUDAFEvaluator.Mode.COMPLETE
          : GenericUDAFEvaluator.Mode.FINAL;
    default:
      throw new RuntimeException("internal error in groupByDescModeToUDAFMode");
    }
  }

  /**
   * Generate the GroupByOperator for the Query Block (parseInfo.getXXX(dest)).
   * The new GroupByOperator will be a child of the reduceSinkOperatorInfo.
   *
   * @param mode
   *          The mode of the aggregation (PARTIAL1 or COMPLETE)
   * @param genericUDAFEvaluators
   *          If not null, this function will store the mapping from Aggregation
   *          StringTree to the genericUDAFEvaluator in this parameter, so it
   *          can be used in the next-stage GroupBy aggregations.
   * @return the new GroupByOperator
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanGroupByOperator(QBParseInfo parseInfo,
      String dest, Operator reduceSinkOperatorInfo, GroupByDesc.Mode mode,
      Map<String, GenericUDAFEvaluator> genericUDAFEvaluators)
      throws SemanticException {
    RowResolver groupByInputRowResolver = opParseCtx
        .get(reduceSinkOperatorInfo).getRR();
    RowResolver groupByOutputRowResolver = new RowResolver();
    groupByOutputRowResolver.setIsExprResolver(true);
    ArrayList<ExprNodeDesc> groupByKeys = new ArrayList<ExprNodeDesc>();
    ArrayList<AggregationDesc> aggregations = new ArrayList<AggregationDesc>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      ColumnInfo exprInfo = groupByInputRowResolver.getExpression(grpbyExpr);

      if (exprInfo == null) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(grpbyExpr));
      }

      groupByKeys.add(new ExprNodeColumnDesc(exprInfo.getType(), exprInfo
          .getInternalName(), "", false));
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      groupByOutputRowResolver.putExpression(grpbyExpr,
          new ColumnInfo(field, exprInfo.getType(), null, false));
      colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
    }
    // For each aggregation
    HashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    assert (aggregationTrees != null);
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();

      // This is the GenericUDAF name
      String aggName = value.getChild(0).getText();

      // Convert children to aggParameters
      ArrayList<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>();
      // 0 is the function name
      for (int i = 1; i < value.getChildCount(); i++) {
        ASTNode paraExpr = (ASTNode) value.getChild(i);
        ColumnInfo paraExprInfo =
          groupByInputRowResolver.getExpression(paraExpr);
        if (paraExprInfo == null) {
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(paraExpr));
        }

        String paraExpression = paraExprInfo.getInternalName();
        assert (paraExpression != null);
        aggParameters.add(new ExprNodeColumnDesc(paraExprInfo.getType(),
            paraExprInfo.getInternalName(), paraExprInfo.getTabAlias(),
            paraExprInfo.getIsPartitionCol()));
      }

      boolean isDistinct = value.getType() == HiveParser.TOK_FUNCTIONDI;
      boolean isAllColumns = value.getType() == HiveParser.TOK_FUNCTIONSTAR;
      Mode amode = groupByDescModeToUDAFMode(mode, isDistinct);
      GenericUDAFEvaluator genericUDAFEvaluator = getGenericUDAFEvaluator(
          aggName, aggParameters, value, isDistinct, isAllColumns);
      assert (genericUDAFEvaluator != null);
      GenericUDAFInfo udaf = getGenericUDAFInfo(genericUDAFEvaluator, amode,
          aggParameters);
      aggregations.add(new AggregationDesc(aggName.toLowerCase(),
          udaf.genericUDAFEvaluator, udaf.convertedParameters, isDistinct,
          amode));
      String field = getColumnInternalName(groupByKeys.size()
          + aggregations.size() - 1);
      outputColumnNames.add(field);
      groupByOutputRowResolver.putExpression(value, new ColumnInfo(
          field, udaf.returnType, "", false));
      // Save the evaluator so that it can be used by the next-stage
      // GroupByOperators
      if (genericUDAFEvaluators != null) {
        genericUDAFEvaluators.put(entry.getKey(), genericUDAFEvaluator);
      }
    }

    Operator op = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new GroupByDesc(mode, outputColumnNames, groupByKeys, aggregations,
        false), new RowSchema(groupByOutputRowResolver.getColumnInfos()),
        reduceSinkOperatorInfo), groupByOutputRowResolver);
    op.setColumnExprMap(colExprMap);
    return op;
  }

  /**
   * Generate the GroupByOperator for the Query Block (parseInfo.getXXX(dest)).
   * The new GroupByOperator will be a child of the reduceSinkOperatorInfo.
   *
   * @param mode
   *          The mode of the aggregation (MERGEPARTIAL, PARTIAL2)
   * @param genericUDAFEvaluators
   *          The mapping from Aggregation StringTree to the
   *          genericUDAFEvaluator.
   * @param distPartAggr
   *          partial aggregation for distincts
   * @return the new GroupByOperator
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanGroupByOperator1(QBParseInfo parseInfo,
      String dest, Operator reduceSinkOperatorInfo, GroupByDesc.Mode mode,
      Map<String, GenericUDAFEvaluator> genericUDAFEvaluators,
      boolean distPartAgg) throws SemanticException {
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    RowResolver groupByInputRowResolver = opParseCtx
        .get(reduceSinkOperatorInfo).getRR();
    RowResolver groupByOutputRowResolver = new RowResolver();
    groupByOutputRowResolver.setIsExprResolver(true);
    ArrayList<ExprNodeDesc> groupByKeys = new ArrayList<ExprNodeDesc>();
    ArrayList<AggregationDesc> aggregations = new ArrayList<AggregationDesc>();
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      ColumnInfo exprInfo = groupByInputRowResolver.getExpression(grpbyExpr);

      if (exprInfo == null) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(grpbyExpr));
      }

      groupByKeys.add(new ExprNodeColumnDesc(exprInfo.getType(), exprInfo
          .getInternalName(), exprInfo.getTabAlias(), exprInfo
          .getIsPartitionCol()));
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      groupByOutputRowResolver.putExpression(grpbyExpr,
          new ColumnInfo(field, exprInfo.getType(), "", false));
      colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
    }

    HashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();
      String aggName = value.getChild(0).getText();
      ArrayList<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>();

      // If the function is distinct, partial aggregartion has not been done on
      // the client side.
      // If distPartAgg is set, the client is letting us know that partial
      // aggregation has not been done.
      // For eg: select a, count(b+c), count(distinct d+e) group by a
      // For count(b+c), if partial aggregation has been performed, then we
      // directly look for count(b+c).
      // Otherwise, we look for b+c.
      // For distincts, partial aggregation is never performed on the client
      // side, so always look for the parameters: d+e
      boolean partialAggDone = !(distPartAgg
          || (value.getToken().getType() == HiveParser.TOK_FUNCTIONDI));
      if (!partialAggDone) {
        // 0 is the function name
        for (int i = 1; i < value.getChildCount(); i++) {
          ASTNode paraExpr = (ASTNode) value.getChild(i);
          ColumnInfo paraExprInfo =
            groupByInputRowResolver.getExpression(paraExpr);
          if (paraExprInfo == null) {
            throw new SemanticException(ErrorMsg.INVALID_COLUMN
                .getMsg(paraExpr));
          }

          String paraExpression = paraExprInfo.getInternalName();
          assert (paraExpression != null);
          aggParameters.add(new ExprNodeColumnDesc(paraExprInfo.getType(),
              paraExprInfo.getInternalName(), paraExprInfo.getTabAlias(),
              paraExprInfo.getIsPartitionCol()));
        }
      } else {
        ColumnInfo paraExprInfo = groupByInputRowResolver.getExpression(value);
        if (paraExprInfo == null) {
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(value));
        }
        String paraExpression = paraExprInfo.getInternalName();
        assert (paraExpression != null);
        aggParameters.add(new ExprNodeColumnDesc(paraExprInfo.getType(),
            paraExpression, paraExprInfo.getTabAlias(), paraExprInfo
            .getIsPartitionCol()));
      }
      boolean isDistinct = (value.getType() == HiveParser.TOK_FUNCTIONDI);
      boolean isAllColumns = value.getType() == HiveParser.TOK_FUNCTIONSTAR;
      Mode amode = groupByDescModeToUDAFMode(mode, isDistinct);
      GenericUDAFEvaluator genericUDAFEvaluator = null;
      // For distincts, partial aggregations have not been done
      if (distPartAgg) {
        genericUDAFEvaluator = getGenericUDAFEvaluator(aggName, aggParameters,
            value, isDistinct, isAllColumns);
        assert (genericUDAFEvaluator != null);
        genericUDAFEvaluators.put(entry.getKey(), genericUDAFEvaluator);
      } else {
        genericUDAFEvaluator = genericUDAFEvaluators.get(entry.getKey());
        assert (genericUDAFEvaluator != null);
      }

      GenericUDAFInfo udaf = getGenericUDAFInfo(genericUDAFEvaluator, amode,
          aggParameters);
      aggregations.add(new AggregationDesc(aggName.toLowerCase(),
          udaf.genericUDAFEvaluator, udaf.convertedParameters,
          (mode != GroupByDesc.Mode.FINAL && isDistinct), amode));
      String field = getColumnInternalName(groupByKeys.size()
          + aggregations.size() - 1);
      outputColumnNames.add(field);
      groupByOutputRowResolver.putExpression(value, new ColumnInfo(
          field, udaf.returnType, "", false));
    }

    Operator op = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new GroupByDesc(mode, outputColumnNames, groupByKeys, aggregations,
        distPartAgg), new RowSchema(groupByOutputRowResolver
        .getColumnInfos()), reduceSinkOperatorInfo),
        groupByOutputRowResolver);
    op.setColumnExprMap(colExprMap);
    return op;
  }

  /**
   * Generate the map-side GroupByOperator for the Query Block
   * (qb.getParseInfo().getXXX(dest)). The new GroupByOperator will be a child
   * of the inputOperatorInfo.
   *
   * @param mode
   *          The mode of the aggregation (HASH)
   * @param genericUDAFEvaluators
   *          If not null, this function will store the mapping from Aggregation
   *          StringTree to the genericUDAFEvaluator in this parameter, so it
   *          can be used in the next-stage GroupBy aggregations.
   * @return the new GroupByOperator
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanMapGroupByOperator(QB qb, String dest,
      Operator inputOperatorInfo, GroupByDesc.Mode mode,
      Map<String, GenericUDAFEvaluator> genericUDAFEvaluators)
      throws SemanticException {

    RowResolver groupByInputRowResolver = opParseCtx.get(inputOperatorInfo)
        .getRR();
    QBParseInfo parseInfo = qb.getParseInfo();
    RowResolver groupByOutputRowResolver = new RowResolver();
    groupByOutputRowResolver.setIsExprResolver(true);
    ArrayList<ExprNodeDesc> groupByKeys = new ArrayList<ExprNodeDesc>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    ArrayList<AggregationDesc> aggregations = new ArrayList<AggregationDesc>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      ExprNodeDesc grpByExprNode = genExprNodeDesc(grpbyExpr,
          groupByInputRowResolver);

      groupByKeys.add(grpByExprNode);
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      groupByOutputRowResolver.putExpression(grpbyExpr,
          new ColumnInfo(field, grpByExprNode.getTypeInfo(), "", false));
      colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
    }

    // If there is a distinctFuncExp, add all parameters to the reduceKeys.
    if (parseInfo.getDistinctFuncExprForClause(dest) != null) {
      ASTNode value = parseInfo.getDistinctFuncExprForClause(dest);
      int numDistn = 0;
      // 0 is function name
      for (int i = 1; i < value.getChildCount(); i++) {
        ASTNode parameter = (ASTNode) value.getChild(i);
        if (groupByOutputRowResolver.getExpression(parameter) == null) {
          ExprNodeDesc distExprNode = genExprNodeDesc(parameter,
              groupByInputRowResolver);
          groupByKeys.add(distExprNode);
          numDistn++;
          String field = getColumnInternalName(grpByExprs.size() + numDistn - 1);
          outputColumnNames.add(field);
          groupByOutputRowResolver.putExpression(parameter, new ColumnInfo(field,
              distExprNode.getTypeInfo(), "", false));
          colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
        }
      }
    }

    // For each aggregation
    HashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    assert (aggregationTrees != null);

    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();
      String aggName = unescapeIdentifier(value.getChild(0).getText());
      ArrayList<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>();
      new ArrayList<Class<?>>();
      // 0 is the function name
      for (int i = 1; i < value.getChildCount(); i++) {
        ASTNode paraExpr = (ASTNode) value.getChild(i);
        ExprNodeDesc paraExprNode = genExprNodeDesc(paraExpr,
            groupByInputRowResolver);

        aggParameters.add(paraExprNode);
      }

      boolean isDistinct = value.getType() == HiveParser.TOK_FUNCTIONDI;
      boolean isAllColumns = value.getType() == HiveParser.TOK_FUNCTIONSTAR;
      Mode amode = groupByDescModeToUDAFMode(mode, isDistinct);

      GenericUDAFEvaluator genericUDAFEvaluator = getGenericUDAFEvaluator(
          aggName, aggParameters, value, isDistinct, isAllColumns);
      assert (genericUDAFEvaluator != null);
      GenericUDAFInfo udaf = getGenericUDAFInfo(genericUDAFEvaluator, amode,
          aggParameters);
      aggregations.add(new AggregationDesc(aggName.toLowerCase(),
          udaf.genericUDAFEvaluator, udaf.convertedParameters, isDistinct,
          amode));
      String field = getColumnInternalName(groupByKeys.size()
          + aggregations.size() - 1);
      outputColumnNames.add(field);
      groupByOutputRowResolver.putExpression(value, new ColumnInfo(
          field, udaf.returnType, "", false));
      // Save the evaluator so that it can be used by the next-stage
      // GroupByOperators
      if (genericUDAFEvaluators != null) {
        genericUDAFEvaluators.put(entry.getKey(), genericUDAFEvaluator);
      }
    }

    Operator op = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new GroupByDesc(mode, outputColumnNames, groupByKeys, aggregations,
        false), new RowSchema(groupByOutputRowResolver.getColumnInfos()),
        inputOperatorInfo), groupByOutputRowResolver);
    op.setColumnExprMap(colExprMap);
    return op;
  }

  /**
   * Generate the ReduceSinkOperator for the Group By Query Block
   * (qb.getPartInfo().getXXX(dest)). The new ReduceSinkOperator will be a child
   * of inputOperatorInfo.
   *
   * It will put all Group By keys and the distinct field (if any) in the
   * map-reduce sort key, and all other fields in the map-reduce value.
   *
   * @param numPartitionFields
   *          the number of fields for map-reduce partitioning. This is usually
   *          the number of fields in the Group By keys.
   * @return the new ReduceSinkOperator.
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanReduceSinkOperator(QB qb, String dest,
      Operator inputOperatorInfo, int numPartitionFields, int numReducers,
      boolean mapAggrDone) throws SemanticException {

    RowResolver reduceSinkInputRowResolver = opParseCtx.get(inputOperatorInfo)
        .getRR();
    QBParseInfo parseInfo = qb.getParseInfo();
    RowResolver reduceSinkOutputRowResolver = new RowResolver();
    reduceSinkOutputRowResolver.setIsExprResolver(true);
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    ArrayList<ExprNodeDesc> reduceKeys = new ArrayList<ExprNodeDesc>();
    // Pre-compute group-by keys and store in reduceKeys

    List<String> outputColumnNames = new ArrayList<String>();
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      ExprNodeDesc inputExpr = genExprNodeDesc(grpbyExpr,
          reduceSinkInputRowResolver);
      reduceKeys.add(inputExpr);
      if (reduceSinkOutputRowResolver.getExpression(grpbyExpr) == null) {
        outputColumnNames.add(getColumnInternalName(reduceKeys.size() - 1));
        String field = Utilities.ReduceField.KEY.toString() + "."
            + getColumnInternalName(reduceKeys.size() - 1);
        ColumnInfo colInfo = new ColumnInfo(field, reduceKeys.get(
            reduceKeys.size() - 1).getTypeInfo(), null, false);
        reduceSinkOutputRowResolver.putExpression(grpbyExpr, colInfo);
        colExprMap.put(colInfo.getInternalName(), inputExpr);
      } else {
        throw new SemanticException(ErrorMsg.DUPLICATE_GROUPBY_KEY
            .getMsg(grpbyExpr));
      }
    }

    // If there is a distinctFuncExp, add all parameters to the reduceKeys.
    if (parseInfo.getDistinctFuncExprForClause(dest) != null) {
      ASTNode value = parseInfo.getDistinctFuncExprForClause(dest);
      // 0 is function name
      for (int i = 1; i < value.getChildCount(); i++) {
        ASTNode parameter = (ASTNode) value.getChild(i);
        if (reduceSinkOutputRowResolver.getExpression(parameter) == null) {
          reduceKeys
              .add(genExprNodeDesc(parameter, reduceSinkInputRowResolver));
          outputColumnNames.add(getColumnInternalName(reduceKeys.size() - 1));
          String field = Utilities.ReduceField.KEY.toString() + "."
              + getColumnInternalName(reduceKeys.size() - 1);
          ColumnInfo colInfo = new ColumnInfo(field, reduceKeys.get(
              reduceKeys.size() - 1).getTypeInfo(), null, false);
          reduceSinkOutputRowResolver.putExpression(parameter, colInfo);
          colExprMap.put(colInfo.getInternalName(), reduceKeys.get(reduceKeys
              .size() - 1));
        }
      }
    }

    ArrayList<ExprNodeDesc> reduceValues = new ArrayList<ExprNodeDesc>();
    HashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);

    if (!mapAggrDone) {
      // Put parameters to aggregations in reduceValues
      for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
        ASTNode value = entry.getValue();
        // 0 is function name
        for (int i = 1; i < value.getChildCount(); i++) {
          ASTNode parameter = (ASTNode) value.getChild(i);
          if (reduceSinkOutputRowResolver.getExpression(parameter) == null) {
            reduceValues.add(genExprNodeDesc(parameter,
                reduceSinkInputRowResolver));
            outputColumnNames
                .add(getColumnInternalName(reduceValues.size() - 1));
            String field = Utilities.ReduceField.VALUE.toString() + "."
                + getColumnInternalName(reduceValues.size() - 1);
            reduceSinkOutputRowResolver.putExpression(parameter, new ColumnInfo(field,
                reduceValues.get(reduceValues.size() - 1).getTypeInfo(), null,
                false));
          }
        }
      }
    } else {
      // Put partial aggregation results in reduceValues
      int inputField = reduceKeys.size();

      for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {

        TypeInfo type = reduceSinkInputRowResolver.getColumnInfos().get(
            inputField).getType();
        reduceValues.add(new ExprNodeColumnDesc(type,
            getColumnInternalName(inputField), "", false));
        inputField++;
        outputColumnNames.add(getColumnInternalName(reduceValues.size() - 1));
        String field = Utilities.ReduceField.VALUE.toString() + "."
            + getColumnInternalName(reduceValues.size() - 1);
        reduceSinkOutputRowResolver.putExpression(entry.getValue(),
            new ColumnInfo(field, type, null, false));
      }
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(reduceKeys,
        reduceValues, outputColumnNames, true, -1, numPartitionFields,
        numReducers), new RowSchema(reduceSinkOutputRowResolver
        .getColumnInfos()), inputOperatorInfo), reduceSinkOutputRowResolver);
    rsOp.setColumnExprMap(colExprMap);
    return rsOp;
  }

  /**
   * Generate the second ReduceSinkOperator for the Group By Plan
   * (parseInfo.getXXX(dest)). The new ReduceSinkOperator will be a child of
   * groupByOperatorInfo.
   *
   * The second ReduceSinkOperator will put the group by keys in the map-reduce
   * sort key, and put the partial aggregation results in the map-reduce value.
   *
   * @param numPartitionFields
   *          the number of fields in the map-reduce partition key. This should
   *          always be the same as the number of Group By keys. We should be
   *          able to remove this parameter since in this phase there is no
   *          distinct any more.
   * @return the new ReduceSinkOperator.
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanReduceSinkOperator2MR(QBParseInfo parseInfo,
      String dest, Operator groupByOperatorInfo, int numPartitionFields,
      int numReducers) throws SemanticException {
    RowResolver reduceSinkInputRowResolver2 = opParseCtx.get(
        groupByOperatorInfo).getRR();
    RowResolver reduceSinkOutputRowResolver2 = new RowResolver();
    reduceSinkOutputRowResolver2.setIsExprResolver(true);
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    ArrayList<ExprNodeDesc> reduceKeys = new ArrayList<ExprNodeDesc>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    // Get group-by keys and store in reduceKeys
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      TypeInfo typeInfo = reduceSinkInputRowResolver2.getExpression(
          grpbyExpr).getType();
      ExprNodeColumnDesc inputExpr = new ExprNodeColumnDesc(typeInfo, field,
          "", false);
      reduceKeys.add(inputExpr);
      ColumnInfo colInfo = new ColumnInfo(Utilities.ReduceField.KEY.toString()
          + "." + field, typeInfo, "", false);
      reduceSinkOutputRowResolver2.putExpression(grpbyExpr, colInfo);
      colExprMap.put(colInfo.getInternalName(), inputExpr);
    }
    // Get partial aggregation results and store in reduceValues
    ArrayList<ExprNodeDesc> reduceValues = new ArrayList<ExprNodeDesc>();
    int inputField = reduceKeys.size();
    HashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      String field = getColumnInternalName(inputField);
      ASTNode t = entry.getValue();
      TypeInfo typeInfo = reduceSinkInputRowResolver2.getExpression(t)
          .getType();
      reduceValues.add(new ExprNodeColumnDesc(typeInfo, field, "", false));
      inputField++;
      String col = getColumnInternalName(reduceValues.size() - 1);
      outputColumnNames.add(col);
      reduceSinkOutputRowResolver2.putExpression(t, new ColumnInfo(
          Utilities.ReduceField.VALUE.toString() + "." + col, typeInfo, "",
          false));
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(reduceKeys,
        reduceValues, outputColumnNames, true, -1, numPartitionFields,
        numReducers), new RowSchema(reduceSinkOutputRowResolver2
        .getColumnInfos()), groupByOperatorInfo),
        reduceSinkOutputRowResolver2);

    rsOp.setColumnExprMap(colExprMap);
    return rsOp;
  }

  /**
   * Generate the second GroupByOperator for the Group By Plan
   * (parseInfo.getXXX(dest)). The new GroupByOperator will do the second
   * aggregation based on the partial aggregation results.
   *
   * @param mode
   *          the mode of aggregation (FINAL)
   * @param genericUDAFEvaluators
   *          The mapping from Aggregation StringTree to the
   *          genericUDAFEvaluator.
   * @return the new GroupByOperator
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanGroupByOperator2MR(QBParseInfo parseInfo,
      String dest, Operator reduceSinkOperatorInfo2, GroupByDesc.Mode mode,
      Map<String, GenericUDAFEvaluator> genericUDAFEvaluators)
      throws SemanticException {
    RowResolver groupByInputRowResolver2 = opParseCtx.get(
        reduceSinkOperatorInfo2).getRR();
    RowResolver groupByOutputRowResolver2 = new RowResolver();
    groupByOutputRowResolver2.setIsExprResolver(true);
    ArrayList<ExprNodeDesc> groupByKeys = new ArrayList<ExprNodeDesc>();
    ArrayList<AggregationDesc> aggregations = new ArrayList<AggregationDesc>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      ColumnInfo exprInfo = groupByInputRowResolver2.getExpression(grpbyExpr);
      if (exprInfo == null) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(grpbyExpr));
      }

      String expression = exprInfo.getInternalName();
      groupByKeys.add(new ExprNodeColumnDesc(exprInfo.getType(), expression,
          exprInfo.getTabAlias(), exprInfo.getIsPartitionCol()));
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      groupByOutputRowResolver2.putExpression(grpbyExpr,
          new ColumnInfo(field, exprInfo.getType(), "", false));
      colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
    }
    HashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ArrayList<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>();
      ASTNode value = entry.getValue();
      ColumnInfo paraExprInfo = groupByInputRowResolver2.getExpression(value);
      if (paraExprInfo == null) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(value));
      }
      String paraExpression = paraExprInfo.getInternalName();
      assert (paraExpression != null);
      aggParameters.add(new ExprNodeColumnDesc(paraExprInfo.getType(),
          paraExpression, paraExprInfo.getTabAlias(), paraExprInfo
          .getIsPartitionCol()));

      String aggName = value.getChild(0).getText();

      boolean isDistinct = value.getType() == HiveParser.TOK_FUNCTIONDI;
      boolean isStar = value.getType() == HiveParser.TOK_FUNCTIONSTAR;
      Mode amode = groupByDescModeToUDAFMode(mode, isDistinct);
      GenericUDAFEvaluator genericUDAFEvaluator = genericUDAFEvaluators
          .get(entry.getKey());
      assert (genericUDAFEvaluator != null);
      GenericUDAFInfo udaf = getGenericUDAFInfo(genericUDAFEvaluator, amode,
          aggParameters);
      aggregations
          .add(new AggregationDesc(
              aggName.toLowerCase(),
              udaf.genericUDAFEvaluator,
              udaf.convertedParameters,
              (mode != GroupByDesc.Mode.FINAL && value.getToken().getType() ==
                HiveParser.TOK_FUNCTIONDI),
              amode));
      String field = getColumnInternalName(groupByKeys.size()
          + aggregations.size() - 1);
      outputColumnNames.add(field);
      groupByOutputRowResolver2.putExpression(value, new ColumnInfo(
          field, udaf.returnType, "", false));
    }

    Operator op = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new GroupByDesc(mode, outputColumnNames, groupByKeys, aggregations,
        false), new RowSchema(groupByOutputRowResolver2.getColumnInfos()),
        reduceSinkOperatorInfo2), groupByOutputRowResolver2);
    op.setColumnExprMap(colExprMap);
    return op;
  }

  /**
   * Generate a Group-By plan using a single map-reduce job (3 operators will be
   * inserted):
   *
   * ReduceSink ( keys = (K1_EXP, K2_EXP, DISTINCT_EXP), values = (A1_EXP,
   * A2_EXP) ) SortGroupBy (keys = (KEY.0,KEY.1), aggregations =
   * (count_distinct(KEY.2), sum(VALUE.0), count(VALUE.1))) Select (final
   * selects).
   *
   * @param dest
   * @param qb
   * @param input
   * @return
   * @throws SemanticException
   *
   *           Generate a Group-By plan using 1 map-reduce job. Spray by the
   *           group by key, and sort by the distinct key (if any), and compute
   *           aggregates * The agggregation evaluation functions are as
   *           follows: Partitioning Key: grouping key
   *
   *           Sorting Key: grouping key if no DISTINCT grouping + distinct key
   *           if DISTINCT
   *
   *           Reducer: iterate/merge (mode = COMPLETE)
   **/
  @SuppressWarnings({"unused", "nls"})
  private Operator genGroupByPlan1MR(String dest, QB qb, Operator input)
      throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    int numReducers = -1;
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    if (grpByExprs.isEmpty()) {
      numReducers = 1;
    }

    // ////// 1. Generate ReduceSinkOperator
    Operator reduceSinkOperatorInfo = genGroupByPlanReduceSinkOperator(qb,
        dest, input, grpByExprs.size(), numReducers, false);

    // ////// 2. Generate GroupbyOperator
    Operator groupByOperatorInfo = genGroupByPlanGroupByOperator(parseInfo,
        dest, reduceSinkOperatorInfo, GroupByDesc.Mode.COMPLETE, null);

    return groupByOperatorInfo;
  }

  static ArrayList<GenericUDAFEvaluator> getUDAFEvaluators(
      ArrayList<AggregationDesc> aggs) {
    ArrayList<GenericUDAFEvaluator> result = new ArrayList<GenericUDAFEvaluator>();
    for (int i = 0; i < aggs.size(); i++) {
      result.add(aggs.get(i).getGenericUDAFEvaluator());
    }
    return result;
  }

  /**
   * Generate a Multi Group-By plan using a 2 map-reduce jobs.
   *
   * @param dest
   * @param qb
   * @param input
   * @return
   * @throws SemanticException
   *
   *           Generate a Group-By plan using a 2 map-reduce jobs. Spray by the
   *           distinct key in hope of getting a uniform distribution, and
   *           compute partial aggregates by the grouping key. Evaluate partial
   *           aggregates first, and spray by the grouping key to compute actual
   *           aggregates in the second phase. The agggregation evaluation
   *           functions are as follows: Partitioning Key: distinct key
   *
   *           Sorting Key: distinct key
   *
   *           Reducer: iterate/terminatePartial (mode = PARTIAL1)
   *
   *           STAGE 2
   *
   *           Partitioning Key: grouping key
   *
   *           Sorting Key: grouping key
   *
   *           Reducer: merge/terminate (mode = FINAL)
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlan2MRMultiGroupBy(String dest, QB qb,
      Operator input) throws SemanticException {

    // ////// Generate GroupbyOperator for a map-side partial aggregation
    Map<String, GenericUDAFEvaluator> genericUDAFEvaluators =
      new LinkedHashMap<String, GenericUDAFEvaluator>();

    QBParseInfo parseInfo = qb.getParseInfo();

    // ////// 2. Generate GroupbyOperator
    Operator groupByOperatorInfo = genGroupByPlanGroupByOperator1(parseInfo,
        dest, input, GroupByDesc.Mode.HASH, genericUDAFEvaluators, true);

    int numReducers = -1;
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);

    // ////// 3. Generate ReduceSinkOperator2
    Operator reduceSinkOperatorInfo2 = genGroupByPlanReduceSinkOperator2MR(
        parseInfo, dest, groupByOperatorInfo, grpByExprs.size(), numReducers);

    // ////// 4. Generate GroupbyOperator2
    Operator groupByOperatorInfo2 = genGroupByPlanGroupByOperator2MR(parseInfo,
        dest, reduceSinkOperatorInfo2, GroupByDesc.Mode.FINAL,
        genericUDAFEvaluators);

    return groupByOperatorInfo2;
  }

  /**
   * Generate a Group-By plan using a 2 map-reduce jobs (5 operators will be
   * inserted):
   *
   * ReduceSink ( keys = (K1_EXP, K2_EXP, DISTINCT_EXP), values = (A1_EXP,
   * A2_EXP) ) NOTE: If DISTINCT_EXP is null, partition by rand() SortGroupBy
   * (keys = (KEY.0,KEY.1), aggregations = (count_distinct(KEY.2), sum(VALUE.0),
   * count(VALUE.1))) ReduceSink ( keys = (0,1), values=(2,3,4)) SortGroupBy
   * (keys = (KEY.0,KEY.1), aggregations = (sum(VALUE.0), sum(VALUE.1),
   * sum(VALUE.2))) Select (final selects).
   *
   * @param dest
   * @param qb
   * @param input
   * @return
   * @throws SemanticException
   *
   *           Generate a Group-By plan using a 2 map-reduce jobs. Spray by the
   *           grouping key and distinct key (or a random number, if no distinct
   *           is present) in hope of getting a uniform distribution, and
   *           compute partial aggregates grouped by the reduction key (grouping
   *           key + distinct key). Evaluate partial aggregates first, and spray
   *           by the grouping key to compute actual aggregates in the second
   *           phase. The agggregation evaluation functions are as follows:
   *           Partitioning Key: random() if no DISTINCT grouping + distinct key
   *           if DISTINCT
   *
   *           Sorting Key: grouping key if no DISTINCT grouping + distinct key
   *           if DISTINCT
   *
   *           Reducer: iterate/terminatePartial (mode = PARTIAL1)
   *
   *           STAGE 2
   *
   *           Partitioning Key: grouping key
   *
   *           Sorting Key: grouping key if no DISTINCT grouping + distinct key
   *           if DISTINCT
   *
   *           Reducer: merge/terminate (mode = FINAL)
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlan2MR(String dest, QB qb, Operator input)
      throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    // ////// 1. Generate ReduceSinkOperator
    // There is a special case when we want the rows to be randomly distributed
    // to
    // reducers for load balancing problem. That happens when there is no
    // DISTINCT
    // operator. We set the numPartitionColumns to -1 for this purpose. This is
    // captured by WritableComparableHiveObject.hashCode() function.
    Operator reduceSinkOperatorInfo = genGroupByPlanReduceSinkOperator(qb,
        dest, input, (parseInfo.getDistinctFuncExprForClause(dest) == null ? -1
        : Integer.MAX_VALUE), -1, false);

    // ////// 2. Generate GroupbyOperator
    Map<String, GenericUDAFEvaluator> genericUDAFEvaluators =
      new LinkedHashMap<String, GenericUDAFEvaluator>();
    GroupByOperator groupByOperatorInfo = (GroupByOperator) genGroupByPlanGroupByOperator(
        parseInfo, dest, reduceSinkOperatorInfo, GroupByDesc.Mode.PARTIAL1,
        genericUDAFEvaluators);

    int numReducers = -1;
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    if (grpByExprs.isEmpty()) {
      numReducers = 1;
    }

    // ////// 3. Generate ReduceSinkOperator2
    Operator reduceSinkOperatorInfo2 = genGroupByPlanReduceSinkOperator2MR(
        parseInfo, dest, groupByOperatorInfo, grpByExprs.size(), numReducers);

    // ////// 4. Generate GroupbyOperator2
    Operator groupByOperatorInfo2 = genGroupByPlanGroupByOperator2MR(parseInfo,
        dest, reduceSinkOperatorInfo2, GroupByDesc.Mode.FINAL,
        genericUDAFEvaluators);

    return groupByOperatorInfo2;
  }

  private boolean optimizeMapAggrGroupBy(String dest, QB qb) {
    List<ASTNode> grpByExprs = getGroupByForClause(qb.getParseInfo(), dest);
    if ((grpByExprs != null) && !grpByExprs.isEmpty()) {
      return false;
    }

    if (qb.getParseInfo().getDistinctFuncExprForClause(dest) != null) {
      return false;
    }

    return true;
  }

  /**
   * Generate a Group-By plan using 1 map-reduce job. First perform a map-side
   * partial aggregation (to reduce the amount of data), at this point of time,
   * we may turn off map-side partial aggregation based on its performance. Then
   * spray by the group by key, and sort by the distinct key (if any), and
   * compute aggregates based on actual aggregates
   *
   * The agggregation evaluation functions are as follows: Mapper:
   * iterate/terminatePartial (mode = HASH)
   *
   * Partitioning Key: grouping key
   *
   * Sorting Key: grouping key if no DISTINCT grouping + distinct key if
   * DISTINCT
   *
   * Reducer: iterate/terminate if DISTINCT merge/terminate if NO DISTINCT (mode
   * = MERGEPARTIAL)
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanMapAggr1MR(String dest, QB qb,
      Operator inputOperatorInfo) throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    // ////// Generate GroupbyOperator for a map-side partial aggregation
    Map<String, GenericUDAFEvaluator> genericUDAFEvaluators =
      new LinkedHashMap<String, GenericUDAFEvaluator>();
    GroupByOperator groupByOperatorInfo = (GroupByOperator) genGroupByPlanMapGroupByOperator(
        qb, dest, inputOperatorInfo, GroupByDesc.Mode.HASH,
        genericUDAFEvaluators);

    groupOpToInputTables.put(groupByOperatorInfo, opParseCtx.get(
        inputOperatorInfo).getRR().getTableNames());
    int numReducers = -1;

    // Optimize the scenario when there are no grouping keys - only 1 reducer is
    // needed
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    if (grpByExprs.isEmpty()) {
      numReducers = 1;
    }

    // ////// Generate ReduceSink Operator
    Operator reduceSinkOperatorInfo = genGroupByPlanReduceSinkOperator(qb,
        dest, groupByOperatorInfo, grpByExprs.size(), numReducers, true);

    // This is a 1-stage map-reduce processing of the groupby. Tha map-side
    // aggregates was just used to
    // reduce output data. In case of distincts, partial results are not used,
    // and so iterate is again
    // invoked on the reducer. In case of non-distincts, partial results are
    // used, and merge is invoked
    // on the reducer.
    return genGroupByPlanGroupByOperator1(parseInfo, dest,
        reduceSinkOperatorInfo, GroupByDesc.Mode.MERGEPARTIAL,
        genericUDAFEvaluators, false);
  }

  /**
   * Generate a Group-By plan using a 2 map-reduce jobs. However, only 1
   * group-by plan is generated if the query involves no grouping key and no
   * distincts. In that case, the plan is same as generated by
   * genGroupByPlanMapAggr1MR. Otherwise, the following plan is generated: First
   * perform a map side partial aggregation (to reduce the amount of data). Then
   * spray by the grouping key and distinct key (or a random number, if no
   * distinct is present) in hope of getting a uniform distribution, and compute
   * partial aggregates grouped by the reduction key (grouping key + distinct
   * key). Evaluate partial aggregates first, and spray by the grouping key to
   * compute actual aggregates in the second phase. The agggregation evaluation
   * functions are as follows: Mapper: iterate/terminatePartial (mode = HASH)
   *
   * Partitioning Key: random() if no DISTINCT grouping + distinct key if
   * DISTINCT
   *
   * Sorting Key: grouping key if no DISTINCT grouping + distinct key if
   * DISTINCT
   *
   * Reducer: iterate/terminatePartial if DISTINCT merge/terminatePartial if NO
   * DISTINCT (mode = MERGEPARTIAL)
   *
   * STAGE 2
   *
   * Partitioining Key: grouping key
   *
   * Sorting Key: grouping key if no DISTINCT grouping + distinct key if
   * DISTINCT
   *
   * Reducer: merge/terminate (mode = FINAL)
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanMapAggr2MR(String dest, QB qb,
      Operator inputOperatorInfo) throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    // ////// Generate GroupbyOperator for a map-side partial aggregation
    Map<String, GenericUDAFEvaluator> genericUDAFEvaluators =
      new LinkedHashMap<String, GenericUDAFEvaluator>();
    GroupByOperator groupByOperatorInfo = (GroupByOperator) genGroupByPlanMapGroupByOperator(
        qb, dest, inputOperatorInfo, GroupByDesc.Mode.HASH,
        genericUDAFEvaluators);

    groupOpToInputTables.put(groupByOperatorInfo, opParseCtx.get(
        inputOperatorInfo).getRR().getTableNames());
    // Optimize the scenario when there are no grouping keys and no distinct - 2
    // map-reduce jobs are not needed
    // For eg: select count(1) from T where t.ds = ....
    if (!optimizeMapAggrGroupBy(dest, qb)) {

      // ////// Generate ReduceSink Operator
      Operator reduceSinkOperatorInfo = genGroupByPlanReduceSinkOperator(qb,
          dest, groupByOperatorInfo, (parseInfo
          .getDistinctFuncExprForClause(dest) == null ? -1
          : Integer.MAX_VALUE), -1, true);

      // ////// Generate GroupbyOperator for a partial aggregation
      Operator groupByOperatorInfo2 = genGroupByPlanGroupByOperator1(parseInfo,
          dest, reduceSinkOperatorInfo, GroupByDesc.Mode.PARTIALS,
          genericUDAFEvaluators, false);

      int numReducers = -1;
      List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
      if (grpByExprs.isEmpty()) {
        numReducers = 1;
      }

      // ////// Generate ReduceSinkOperator2
      Operator reduceSinkOperatorInfo2 = genGroupByPlanReduceSinkOperator2MR(
          parseInfo, dest, groupByOperatorInfo2, grpByExprs.size(), numReducers);

      // ////// Generate GroupbyOperator3
      return genGroupByPlanGroupByOperator2MR(parseInfo, dest,
          reduceSinkOperatorInfo2, GroupByDesc.Mode.FINAL,
          genericUDAFEvaluators);
    } else {
      // ////// Generate ReduceSink Operator
      Operator reduceSinkOperatorInfo = genGroupByPlanReduceSinkOperator(qb,
          dest, groupByOperatorInfo, getGroupByForClause(parseInfo, dest)
          .size(), 1, true);

      return genGroupByPlanGroupByOperator2MR(parseInfo, dest,
          reduceSinkOperatorInfo, GroupByDesc.Mode.FINAL, genericUDAFEvaluators);
    }
  }

  @SuppressWarnings("nls")
  private Operator genConversionOps(String dest, QB qb, Operator input)
      throws SemanticException {

    Integer dest_type = qb.getMetaData().getDestTypeForAlias(dest);
    switch (dest_type.intValue()) {
    case QBMetaData.DEST_TABLE: {
      qb.getMetaData().getDestTableForAlias(dest);
      break;
    }
    case QBMetaData.DEST_PARTITION: {
      qb.getMetaData().getDestPartitionForAlias(dest).getTable();
      break;
    }
    default: {
      return input;
    }
    }

    return input;
  }

  private int getReducersBucketing(int totalFiles, int maxReducers) {
    int numFiles = totalFiles/maxReducers;
    while (true) {
      if (totalFiles%numFiles == 0) {
        return totalFiles/numFiles;
      }
      numFiles++;
    }
  }

  private static class SortBucketRSCtx {
    ArrayList<ExprNodeDesc> partnCols;
    boolean multiFileSpray;
    int     numFiles;
    int     totalFiles;

    public SortBucketRSCtx() {
      partnCols = null;
      multiFileSpray = false;
      numFiles = 1;
      totalFiles = 1;
    }

    /**
     * @return the partnCols
     */
    public ArrayList<ExprNodeDesc> getPartnCols() {
      return partnCols;
    }

    /**
     * @param partnCols the partnCols to set
     */
    public void setPartnCols(ArrayList<ExprNodeDesc> partnCols) {
      this.partnCols = partnCols;
    }

    /**
     * @return the multiFileSpray
     */
    public boolean isMultiFileSpray() {
      return multiFileSpray;
    }

    /**
     * @param multiFileSpray the multiFileSpray to set
     */
    public void setMultiFileSpray(boolean multiFileSpray) {
      this.multiFileSpray = multiFileSpray;
    }

    /**
     * @return the numFiles
     */
    public int getNumFiles() {
      return numFiles;
    }

    /**
     * @param numFiles the numFiles to set
     */
    public void setNumFiles(int numFiles) {
      this.numFiles = numFiles;
    }

    /**
     * @return the totalFiles
     */
    public int getTotalFiles() {
      return totalFiles;
    }

    /**
     * @param totalFiles the totalFiles to set
     */
    public void setTotalFiles(int totalFiles) {
      this.totalFiles = totalFiles;
    }
  }

  @SuppressWarnings("nls")
  private Operator genBucketingSortingDest(String dest, Operator input, QB qb, TableDesc table_desc,
                                           Table dest_tab, SortBucketRSCtx ctx)
      throws SemanticException {

    // If the table is bucketed, and bucketing is enforced, do the following:
    // If the number of buckets is smaller than the number of maximum reducers,
    // create those many reducers.
    // If not, create a multiFileSink instead of FileSink - the multiFileSink will
    // spray the data into multiple buckets. That way, we can support a very large
    // number of buckets without needing a very large number of reducers.
    boolean enforceBucketing = false;
    boolean enforceSorting   = false;
    ArrayList<ExprNodeDesc> partnCols = new ArrayList<ExprNodeDesc>();
    ArrayList<ExprNodeDesc> partnColsNoConvert = new ArrayList<ExprNodeDesc>();
    ArrayList<ExprNodeDesc> sortCols  = new ArrayList<ExprNodeDesc>();
    boolean multiFileSpray = false;
    int     numFiles = 1;
    int     totalFiles = 1;

    if ((dest_tab.getNumBuckets() > 0) &&
        (conf.getBoolVar(HiveConf.ConfVars.HIVEENFORCEBUCKETING))) {
      enforceBucketing = true;
      partnCols = getParitionColsFromBucketCols(dest, qb, dest_tab, table_desc, input, true);
      partnColsNoConvert = getParitionColsFromBucketCols(dest, qb, dest_tab, table_desc, input, false);
    }

    if ((dest_tab.getSortCols() != null) &&
        (dest_tab.getSortCols().size() > 0) &&
        (conf.getBoolVar(HiveConf.ConfVars.HIVEENFORCESORTING))) {
      enforceSorting = true;
      sortCols = getSortCols(dest, qb, dest_tab, table_desc, input, true);
      if (!enforceBucketing) {
        partnCols = sortCols;
        partnColsNoConvert = getSortCols(dest, qb, dest_tab, table_desc, input, false);
      }
    }

    if (enforceBucketing || enforceSorting) {
      int maxReducers = conf.getIntVar(HiveConf.ConfVars.MAXREDUCERS);
      int numBuckets  = dest_tab.getNumBuckets();
      if (numBuckets > maxReducers) {
        multiFileSpray = true;
        totalFiles = numBuckets;
        if (totalFiles % maxReducers == 0) {
          numFiles = totalFiles / maxReducers;
        }
        else {
          // find the number of reducers such that it is a divisor of totalFiles
          maxReducers = getReducersBucketing(totalFiles, maxReducers);
          numFiles = totalFiles/maxReducers;
        }
      }
      else {
        maxReducers = numBuckets;
      }

      input = genReduceSinkPlanForSortingBucketing(dest_tab, input, sortCols, partnCols, maxReducers);
      ctx.setMultiFileSpray(multiFileSpray);
      ctx.setNumFiles(numFiles);
      ctx.setPartnCols(partnColsNoConvert);
      ctx.setTotalFiles(totalFiles);
      //disable "merge mapfiles" and "merge mapred files".
      HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVEMERGEMAPFILES, false);
      HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVEMERGEMAPREDFILES, false);
    }
    return input;
  }

  /**
   * Check for HOLD_DDLTIME hint.
   * @param qb
   * @return true if HOLD_DDLTIME is set, false otherwise.
   */
  private boolean checkHoldDDLTime(QB qb) {
    ASTNode hints = qb.getParseInfo().getHints();
    if (hints == null) {
      return false;
    }
    for (int pos = 0; pos < hints.getChildCount(); pos++) {
      ASTNode hint = (ASTNode) hints.getChild(pos);
      if (((ASTNode) hint.getChild(0)).getToken().getType() == HiveParser.TOK_HOLD_DDLTIME) {
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings("nls")
  private Operator genFileSinkPlan(String dest, QB qb, Operator input)
      throws SemanticException {

    RowResolver inputRR = opParseCtx.get(input).getRR();
    QBMetaData qbm = qb.getMetaData();
    Integer dest_type = qbm.getDestTypeForAlias(dest);

    Table dest_tab = null;     // destination table if any
    String queryTmpdir = null; // the intermediate destination directory
    Path dest_path = null; // the final destination directory
    TableDesc table_desc = null;
    int currentTableId = 0;
    boolean isLocal = false;
    SortBucketRSCtx rsCtx = new SortBucketRSCtx();
    DynamicPartitionCtx dpCtx = null;
    LoadTableDesc ltd = null;
    boolean holdDDLTime = checkHoldDDLTime(qb);

    switch (dest_type.intValue()) {
    case QBMetaData.DEST_TABLE: {

      dest_tab = qbm.getDestTableForAlias(dest);
      Map<String, String> partSpec = qbm.getPartSpecForAlias(dest);
      dest_path = dest_tab.getPath();

      // check for partition
      List<FieldSchema> parts = dest_tab.getPartitionKeys();
      if (parts != null && parts.size() > 0) { // table is partitioned
        if (partSpec== null || partSpec.size() == 0) { // user did NOT specify partition
          throw new SemanticException(ErrorMsg.NEED_PARTITION_ERROR.getMsg());
        }
        // the HOLD_DDLTIIME hint should not be used with dynamic partition since the
        // newly generated partitions should always update their DDLTIME
        if (holdDDLTime) {
          throw new SemanticException(ErrorMsg.HOLD_DDLTIME_ON_NONEXIST_PARTITIONS.getMsg());
        }
        dpCtx = qbm.getDPCtx(dest);
        if (dpCtx == null) {
          validatePartSpec(dest_tab, partSpec);
          dpCtx = new DynamicPartitionCtx(dest_tab, partSpec,
              conf.getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME),
              conf.getIntVar(HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTSPERNODE));
          qbm.setDPCtx(dest, dpCtx);
        }

        if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.DYNAMICPARTITIONING)) { // allow DP
          // TODO: we should support merge files for dynamically generated partitions later
          if (dpCtx.getNumDPCols() > 0 &&
              (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEMERGEMAPFILES) ||
               HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEMERGEMAPREDFILES))) {
            HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVEMERGEMAPFILES, false);
            HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVEMERGEMAPREDFILES, false);
          }
          // turn on hive.task.progress to update # of partitions created to the JT
          HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVEJOBPROGRESS, true);

        } else { // QBMetaData.DEST_PARTITION capture the all-SP case
          throw new SemanticException(ErrorMsg.DYNAMIC_PARTITION_DISABLED.getMsg());
        }
        if (dpCtx.getSPPath() != null) {
          dest_path = new Path(dest_tab.getPath(), dpCtx.getSPPath());
        }
        if ((dest_tab.getNumBuckets() > 0) &&
            (conf.getBoolVar(HiveConf.ConfVars.HIVEENFORCEBUCKETING))) {
          dpCtx.setNumBuckets(dest_tab.getNumBuckets());
        }
      }

      boolean isNonNativeTable = dest_tab.isNonNative();
      if (isNonNativeTable) {
        queryTmpdir = dest_path.toUri().getPath();
      } else {
        queryTmpdir = ctx.getExternalTmpFileURI(dest_path.toUri());
      }
      if (dpCtx != null) {
        // set the root of the temporay path where dynamic partition columns will populate
        dpCtx.setRootPath(queryTmpdir);
      }
      // this table_desc does not contain the partitioning columns
      table_desc = Utilities.getTableDesc(dest_tab);

      // Add sorting/bucketing if needed
      input = genBucketingSortingDest(dest, input, qb, table_desc, dest_tab, rsCtx);

      idToTableNameMap.put(String.valueOf(destTableId), dest_tab.getTableName());
      currentTableId = destTableId;
      destTableId++;

      // Create the work for moving the table
      // NOTE: specify Dynamic partitions in dest_tab for WriteEntity
      if (!isNonNativeTable) {
        ltd = new LoadTableDesc(queryTmpdir, ctx.getExternalTmpFileURI(dest_path.toUri()),
            table_desc, dpCtx);
        if (holdDDLTime) {
          LOG.info("this query will not update transient_lastDdlTime!");
          ltd.setHoldDDLTime(true);
        }
        loadTableWork.add(ltd);
      }

      // Here only register the whole table for post-exec hook if no DP present
      // in the case of DP, we will register WriteEntity in MoveTask when the
      // list of dynamically created partitions are known.
      if ((dpCtx == null || dpCtx.getNumDPCols() == 0) &&
          !outputs.add(new WriteEntity(dest_tab))) {
        throw new SemanticException(ErrorMsg.OUTPUT_SPECIFIED_MULTIPLE_TIMES
            .getMsg(dest_tab.getTableName()));
      }
      break;
    }
    case QBMetaData.DEST_PARTITION: {

      Partition dest_part = qbm.getDestPartitionForAlias(dest);
      dest_tab = dest_part.getTable();

      dest_path = dest_part.getPath()[0];
      if ("har".equalsIgnoreCase(dest_path.toUri().getScheme())) {
        throw new SemanticException(ErrorMsg.OVERWRITE_ARCHIVED_PART
            .getMsg());
      }
      queryTmpdir = ctx.getExternalTmpFileURI(dest_path.toUri());
      table_desc = Utilities.getTableDesc(dest_tab);

      // Add sorting/bucketing if needed
      input = genBucketingSortingDest(dest, input, qb, table_desc, dest_tab, rsCtx);

      idToTableNameMap.put(String.valueOf(destTableId), dest_tab.getTableName());
      currentTableId = destTableId;
      destTableId++;

      ltd = new LoadTableDesc(queryTmpdir, ctx.getExternalTmpFileURI(dest_path.toUri()),
          table_desc, dest_part.getSpec());
      if (holdDDLTime) {
        try {
          Partition part = db.getPartition(dest_tab, dest_part.getSpec(), false);
          if (part == null) {
            throw new SemanticException(ErrorMsg.HOLD_DDLTIME_ON_NONEXIST_PARTITIONS.getMsg());
          }
        } catch (HiveException e) {
          throw new SemanticException(e);
        }
        LOG.info("this query will not update transient_lastDdlTime!");
        ltd.setHoldDDLTime(true);
      }
      loadTableWork.add(ltd);
      if (!outputs.add(new WriteEntity(dest_part))) {
        throw new SemanticException(ErrorMsg.OUTPUT_SPECIFIED_MULTIPLE_TIMES
            .getMsg(dest_tab.getTableName() + "@" + dest_part.getName()));
      }
      break;
    }
    case QBMetaData.DEST_LOCAL_FILE:
      isLocal = true;
      // fall through
    case QBMetaData.DEST_DFS_FILE: {
      dest_path = new Path(qbm.getDestFileForAlias(dest));
      String destStr = dest_path.toString();

      if (isLocal) {
        // for local directory - we always write to map-red intermediate
        // store and then copy to local fs
        queryTmpdir = ctx.getMRTmpFileURI();
      } else {
        // otherwise write to the file system implied by the directory
        // no copy is required. we may want to revisit this policy in future

        try {
          Path qPath = FileUtils.makeQualified(dest_path, conf);
          queryTmpdir = ctx.getExternalTmpFileURI(qPath.toUri());
        } catch (Exception e) {
          throw new SemanticException("Error creating temporary folder on: "
              + dest_path, e);
        }
      }
      String cols = new String();
      String colTypes = new String();
      ArrayList<ColumnInfo> colInfos = inputRR.getColumnInfos();

      // CTAS case: the file output format and serde are defined by the create
      // table command
      // rather than taking the default value
      List<FieldSchema> field_schemas = null;
      CreateTableDesc tblDesc = qb.getTableDesc();
      if (tblDesc != null) {
        field_schemas = new ArrayList<FieldSchema>();
      }

      boolean first = true;
      for (ColumnInfo colInfo : colInfos) {
        String[] nm = inputRR.reverseLookup(colInfo.getInternalName());

        if (nm[1] != null) { // non-null column alias
          colInfo.setAlias(nm[1]);
        }

        if (field_schemas != null) {
          FieldSchema col = new FieldSchema();
          if (nm[1] != null) {
            col.setName(colInfo.getAlias());
          } else {
            col.setName(colInfo.getInternalName());
          }
          col.setType(colInfo.getType().getTypeName());
          field_schemas.add(col);
        }

        if (!first) {
          cols = cols.concat(",");
          colTypes = colTypes.concat(":");
        }

        first = false;
        cols = cols.concat(colInfo.getInternalName());

        // Replace VOID type with string when the output is a temp table or
        // local files.
        // A VOID type can be generated under the query:
        //
        // select NULL from tt;
        // or
        // insert overwrite local directory "abc" select NULL from tt;
        //
        // where there is no column type to which the NULL value should be
        // converted.
        //
        String tName = colInfo.getType().getTypeName();
        if (tName.equals(Constants.VOID_TYPE_NAME)) {
          colTypes = colTypes.concat(Constants.STRING_TYPE_NAME);
        } else {
          colTypes = colTypes.concat(tName);
        }
      }

      // update the create table descriptor with the resulting schema.
      if (tblDesc != null) {
        tblDesc.setCols(new ArrayList<FieldSchema>(field_schemas));
      }

      if (!ctx.isMRTmpFileURI(destStr)) {
        idToTableNameMap.put(String.valueOf(destTableId), destStr);
        currentTableId = destTableId;
        destTableId++;
      }

      boolean isDfsDir = (dest_type.intValue() == QBMetaData.DEST_DFS_FILE);
      loadFileWork.add(new LoadFileDesc(queryTmpdir, destStr, isDfsDir, cols,
          colTypes));

      if (tblDesc == null) {
        table_desc = PlanUtils.getDefaultTableDesc(Integer
            .toString(Utilities.ctrlaCode), cols, colTypes, false);
      } else {
        table_desc = PlanUtils.getTableDesc(tblDesc, cols, colTypes);
      }

      if (!outputs.add(new WriteEntity(destStr, !isDfsDir))) {
        throw new SemanticException(ErrorMsg.OUTPUT_SPECIFIED_MULTIPLE_TIMES
            .getMsg(destStr));
      }
      break;
    }
    default:
      throw new SemanticException("Unknown destination type: " + dest_type);
    }

    input = genConversionSelectOperator(dest, qb, input, table_desc, dpCtx);
    inputRR = opParseCtx.get(input).getRR();

    ArrayList<ColumnInfo> vecCol = new ArrayList<ColumnInfo>();

    try {
      StructObjectInspector rowObjectInspector = (StructObjectInspector) table_desc
          .getDeserializer().getObjectInspector();
      List<? extends StructField> fields = rowObjectInspector
          .getAllStructFieldRefs();
      for (int i = 0; i < fields.size(); i++) {
        vecCol.add(new ColumnInfo(fields.get(i).getFieldName(), TypeInfoUtils
            .getTypeInfoFromObjectInspector(fields.get(i)
            .getFieldObjectInspector()), "", false));
      }
    } catch (Exception e) {
      throw new SemanticException(e.getMessage());
    }

    RowSchema fsRS = new RowSchema(vecCol);

    Operator output = putOpInsertMap(
        OperatorFactory.getAndMakeChild(
            new FileSinkDesc(
                queryTmpdir,
                table_desc,
                conf.getBoolVar(HiveConf.ConfVars.COMPRESSRESULT),
                currentTableId,
                rsCtx.isMultiFileSpray(),
                rsCtx.getNumFiles(),
                rsCtx.getTotalFiles(),
                rsCtx.getPartnCols(),
                dpCtx),
            fsRS, input), inputRR);


    if (ltd != null && SessionState.get() != null) {
      SessionState.get().getLineageState()
        .mapDirToFop(ltd.getSourceDir(), (FileSinkOperator)output);
    }

    LOG.debug("Created FileSink Plan for clause: " + dest + "dest_path: "
        + dest_path + " row schema: " + inputRR.toString());

    return output;
  }

  private void validatePartSpec(Table tbl, Map<String, String> partSpec)
      throws SemanticException {
    List<FieldSchema> parts = tbl.getPartitionKeys();
    Set<String> partCols = new HashSet<String>(parts.size());
    for (FieldSchema col: parts) {
      partCols.add(col.getName());
    }
    for (String col: partSpec.keySet()) {
      if (!partCols.contains(col)) {
        throw new SemanticException(ErrorMsg.NONEXISTPARTCOL.getMsg());
      }
    }
  }

  /**
   * Generate the conversion SelectOperator that converts the columns into the
   * types that are expected by the table_desc.
   */
  Operator genConversionSelectOperator(String dest, QB qb, Operator input,
      TableDesc table_desc, DynamicPartitionCtx dpCtx) throws SemanticException {
    StructObjectInspector oi = null;
    try {
      Deserializer deserializer = table_desc.getDeserializerClass()
          .newInstance();
      deserializer.initialize(conf, table_desc.getProperties());
      oi = (StructObjectInspector) deserializer.getObjectInspector();
    } catch (Exception e) {
      throw new SemanticException(e);
    }

    // Check column number
    List<? extends StructField> tableFields = oi.getAllStructFieldRefs();
    boolean dynPart = HiveConf.getBoolVar(conf, HiveConf.ConfVars.DYNAMICPARTITIONING);
    ArrayList<ColumnInfo> rowFields = opParseCtx.get(input).getRR()
        .getColumnInfos();
    int inColumnCnt = rowFields.size();
    int outColumnCnt = tableFields.size();
    if (dynPart && dpCtx != null) {
        outColumnCnt += dpCtx.getNumDPCols();
    }

    if (inColumnCnt != outColumnCnt) {
      String reason = "Table " + dest + " has " + outColumnCnt
          + " columns, but query has " + inColumnCnt + " columns.";
      throw new SemanticException(ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH.getMsg(
          qb.getParseInfo().getDestForClause(dest), reason));
    } else if (dynPart && dpCtx != null){
      // create the mapping from input ExprNode to dest table DP column
      dpCtx.mapInputToDP(rowFields.subList(tableFields.size(), rowFields.size()));
    }

    // Check column types
    boolean converted = false;
    int columnNumber = tableFields.size();
    ArrayList<ExprNodeDesc> expressions = new ArrayList<ExprNodeDesc>(
        columnNumber);
    // MetadataTypedColumnsetSerDe does not need type conversions because it
    // does the conversion to String by itself.
    boolean isMetaDataSerDe = table_desc.getDeserializerClass().equals(
        MetadataTypedColumnsetSerDe.class);
    boolean isLazySimpleSerDe = table_desc.getDeserializerClass().equals(
        LazySimpleSerDe.class);
    if (!isMetaDataSerDe) {

      // here only deals with non-partition columns. We deal with partition columns next
      for (int i = 0; i < columnNumber; i++) {
        ObjectInspector tableFieldOI = tableFields.get(i)
            .getFieldObjectInspector();
        TypeInfo tableFieldTypeInfo = TypeInfoUtils
            .getTypeInfoFromObjectInspector(tableFieldOI);
        TypeInfo rowFieldTypeInfo = rowFields.get(i).getType();
        ExprNodeDesc column = new ExprNodeColumnDesc(rowFieldTypeInfo,
            rowFields.get(i).getInternalName(), "", false);
        // LazySimpleSerDe can convert any types to String type using
        // JSON-format.
        if (!tableFieldTypeInfo.equals(rowFieldTypeInfo)
            && !(isLazySimpleSerDe
            && tableFieldTypeInfo.getCategory().equals(Category.PRIMITIVE) && tableFieldTypeInfo
            .equals(TypeInfoFactory.stringTypeInfo))) {
          // need to do some conversions here
          converted = true;
          if (tableFieldTypeInfo.getCategory() != Category.PRIMITIVE) {
            // cannot convert to complex types
            column = null;
          } else {
            column = TypeCheckProcFactory.DefaultExprProcessor
                .getFuncExprNodeDesc(tableFieldTypeInfo.getTypeName(),
                    column);
          }
          if (column == null) {
            String reason = "Cannot convert column " + i + " from "
                + rowFieldTypeInfo + " to " + tableFieldTypeInfo + ".";
            throw new SemanticException(ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH
                .getMsg(qb.getParseInfo().getDestForClause(dest), reason));
          }
        }
        expressions.add(column);
      }
    }

    // deal with dynamic partition columns: convert ExprNodeDesc type to String??
    if (dynPart && dpCtx != null && dpCtx.getNumDPCols() > 0) {
      // DP columns starts with tableFields.size()
      for (int i = tableFields.size(); i < rowFields.size(); ++i ) {
        TypeInfo rowFieldTypeInfo = rowFields.get(i).getType();
        ExprNodeDesc column = new ExprNodeColumnDesc(
            rowFieldTypeInfo, rowFields.get(i).getInternalName(), "", false);
        expressions.add(column);
      }
      // converted = true; // [TODO]: should we check & convert type to String and set it to true?
    }

    if (converted) {
      // add the select operator
      RowResolver rowResolver = new RowResolver();
      ArrayList<String> colName = new ArrayList<String>();
      for (int i = 0; i < expressions.size(); i++) {
        String name = getColumnInternalName(i);
        rowResolver.put("", name, new ColumnInfo(name, expressions.get(i)
            .getTypeInfo(), "", false));
        colName.add(name);
      }
      Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
          new SelectDesc(expressions, colName), new RowSchema(rowResolver
          .getColumnInfos()), input), rowResolver);

      return output;
    } else {
      // not converted
      return input;
    }
  }

  @SuppressWarnings("nls")
  private Operator genLimitPlan(String dest, QB qb, Operator input, int limit)
      throws SemanticException {
    // A map-only job can be optimized - instead of converting it to a
    // map-reduce job, we can have another map
    // job to do the same to avoid the cost of sorting in the map-reduce phase.
    // A better approach would be to
    // write into a local file and then have a map-only job.
    // Add the limit operator to get the value fields

    RowResolver inputRR = opParseCtx.get(input).getRR();
    Operator limitMap = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new LimitDesc(limit), new RowSchema(inputRR.getColumnInfos()), input),
        inputRR);

    LOG.debug("Created LimitOperator Plan for clause: " + dest
        + " row schema: " + inputRR.toString());

    return limitMap;
  }

  private Operator genUDTFPlan(GenericUDTF genericUDTF,
      String outputTableAlias, ArrayList<String> colAliases, QB qb,
      Operator input) throws SemanticException {

    // No GROUP BY / DISTRIBUTE BY / SORT BY / CLUSTER BY
    QBParseInfo qbp = qb.getParseInfo();
    if (!qbp.getDestToGroupBy().isEmpty()) {
      throw new SemanticException(ErrorMsg.UDTF_NO_GROUP_BY.getMsg());
    }
    if (!qbp.getDestToDistributeBy().isEmpty()) {
      throw new SemanticException(ErrorMsg.UDTF_NO_DISTRIBUTE_BY.getMsg());
    }
    if (!qbp.getDestToSortBy().isEmpty()) {
      throw new SemanticException(ErrorMsg.UDTF_NO_SORT_BY.getMsg());
    }
    if (!qbp.getDestToClusterBy().isEmpty()) {
      throw new SemanticException(ErrorMsg.UDTF_NO_CLUSTER_BY.getMsg());
    }
    if (!qbp.getAliasToLateralViews().isEmpty()) {
      throw new SemanticException(ErrorMsg.UDTF_LATERAL_VIEW.getMsg());
    }

    LOG.debug("Table alias: " + outputTableAlias + " Col aliases: "
        + colAliases);

    // Use the RowResolver from the input operator to generate a input
    // ObjectInspector that can be used to initialize the UDTF. Then, the

    // resulting output object inspector can be used to make the RowResolver
    // for the UDTF operator
    RowResolver selectRR = opParseCtx.get(input).getRR();
    ArrayList<ColumnInfo> inputCols = selectRR.getColumnInfos();

    // Create the object inspector for the input columns and initialize the UDTF
    ArrayList<String> colNames = new ArrayList<String>();
    ObjectInspector[] colOIs = new ObjectInspector[inputCols.size()];
    for (int i = 0; i < inputCols.size(); i++) {
      colNames.add(inputCols.get(i).getInternalName());
      colOIs[i] = TypeInfoUtils
          .getStandardWritableObjectInspectorFromTypeInfo(inputCols.get(i)
          .getType());
    }
    StructObjectInspector outputOI = genericUDTF.initialize(colOIs);

    // Make sure that the number of column aliases in the AS clause matches
    // the number of columns output by the UDTF
    int numUdtfCols = outputOI.getAllStructFieldRefs().size();
    int numSuppliedAliases = colAliases.size();
    if (numUdtfCols != numSuppliedAliases) {
      throw new SemanticException(ErrorMsg.UDTF_ALIAS_MISMATCH
          .getMsg("expected " + numUdtfCols + " aliases " + "but got "
          + numSuppliedAliases));
    }

    // Generate the output column info's / row resolver using internal names.
    ArrayList<ColumnInfo> udtfCols = new ArrayList<ColumnInfo>();

    Iterator<String> colAliasesIter = colAliases.iterator();
    for (StructField sf : outputOI.getAllStructFieldRefs()) {

      String colAlias = colAliasesIter.next();
      assert (colAlias != null);

      // Since the UDTF operator feeds into a LVJ operator that will rename
      // all the internal names, we can just use field name from the UDTF's OI
      // as the internal name
      ColumnInfo col = new ColumnInfo(sf.getFieldName(), TypeInfoUtils
          .getTypeInfoFromObjectInspector(sf.getFieldObjectInspector()),
          outputTableAlias, false);
      udtfCols.add(col);
    }

    // Create the row resolver for this operator from the output columns
    RowResolver out_rwsch = new RowResolver();
    for (int i = 0; i < udtfCols.size(); i++) {
      out_rwsch.put(outputTableAlias, colAliases.get(i), udtfCols.get(i));
    }

    // Add the UDTFOperator to the operator DAG
    Operator<?> udtf = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new UDTFDesc(genericUDTF), new RowSchema(out_rwsch.getColumnInfos()),
        input), out_rwsch);
    return udtf;
  }

  @SuppressWarnings("nls")
  private Operator genLimitMapRedPlan(String dest, QB qb, Operator input,
      int limit, boolean extraMRStep) throws SemanticException {
    // A map-only job can be optimized - instead of converting it to a
    // map-reduce job, we can have another map
    // job to do the same to avoid the cost of sorting in the map-reduce phase.
    // A better approach would be to
    // write into a local file and then have a map-only job.
    // Add the limit operator to get the value fields
    Operator curr = genLimitPlan(dest, qb, input, limit);

    // the client requested that an extra map-reduce step be performed
    if (!extraMRStep) {
      return curr;
    }

    // Create a reduceSink operator followed by another limit
    curr = genReduceSinkPlan(dest, qb, curr, 1);
    return genLimitPlan(dest, qb, curr, limit);
  }

  private ArrayList<ExprNodeDesc> getParitionColsFromBucketCols(String dest, QB qb, Table tab,
                                                                TableDesc table_desc, Operator input, boolean convert)
    throws SemanticException {
    RowResolver inputRR = opParseCtx.get(input).getRR();
    List<String> tabBucketCols = tab.getBucketCols();
    List<FieldSchema> tabCols  = tab.getCols();

    // Partition by the bucketing column
    List<Integer> posns = new ArrayList<Integer>();

    for (String bucketCol : tabBucketCols) {
      int pos = 0;
      for (FieldSchema tabCol : tabCols) {
        if (bucketCol.equals(tabCol.getName())) {
          posns.add(pos);
          break;
        }
        pos++;
      }
    }

    return genConvertCol(dest, qb, tab, table_desc, input, posns, convert);
  }

  private ArrayList<ExprNodeDesc> genConvertCol(String dest, QB qb, Table tab, TableDesc table_desc, Operator input,
                                                List<Integer> posns, boolean convert) throws SemanticException {
    StructObjectInspector oi = null;
    try {
      Deserializer deserializer = table_desc.getDeserializerClass()
          .newInstance();
      deserializer.initialize(conf, table_desc.getProperties());
      oi = (StructObjectInspector) deserializer.getObjectInspector();
    } catch (Exception e) {
      throw new SemanticException(e);
    }

    List<? extends StructField> tableFields = oi.getAllStructFieldRefs();
    ArrayList<ColumnInfo> rowFields = opParseCtx.get(input).getRR()
        .getColumnInfos();

    // Check column type
    int columnNumber = posns.size();
    ArrayList<ExprNodeDesc> expressions = new ArrayList<ExprNodeDesc>(columnNumber);
    for (Integer posn: posns) {
      ObjectInspector tableFieldOI = tableFields.get(posn).getFieldObjectInspector();
      TypeInfo tableFieldTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(tableFieldOI);
      TypeInfo rowFieldTypeInfo = rowFields.get(posn).getType();
      ExprNodeDesc column = new ExprNodeColumnDesc(rowFieldTypeInfo, rowFields.get(posn).getInternalName(),
                                                   rowFields.get(posn).getTabAlias(), rowFields.get(posn).getIsPartitionCol());

      if (convert && !tableFieldTypeInfo.equals(rowFieldTypeInfo)) {
        // need to do some conversions here
        if (tableFieldTypeInfo.getCategory() != Category.PRIMITIVE) {
          // cannot convert to complex types
          column = null;
        } else {
          column = TypeCheckProcFactory.DefaultExprProcessor
            .getFuncExprNodeDesc(tableFieldTypeInfo.getTypeName(),
                column);
        }
        if (column == null) {
          String reason = "Cannot convert column " + posn + " from "
            + rowFieldTypeInfo + " to " + tableFieldTypeInfo + ".";
          throw new SemanticException(ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH
                                      .getMsg(qb.getParseInfo().getDestForClause(dest), reason));
        }
      }
      expressions.add(column);
    }

    return expressions;
  }

  private ArrayList<ExprNodeDesc> getSortCols(String dest, QB qb, Table tab, TableDesc table_desc, Operator input, boolean convert)
    throws SemanticException {
    RowResolver inputRR = opParseCtx.get(input).getRR();
    List<Order> tabSortCols = tab.getSortCols();
    List<FieldSchema> tabCols  = tab.getCols();

    // Partition by the bucketing column
    List<Integer> posns = new ArrayList<Integer>();
    for (Order sortCol : tabSortCols) {
      int pos = 0;
      for (FieldSchema tabCol : tabCols) {
        if (sortCol.getCol().equals(tabCol.getName())) {
          ColumnInfo colInfo = inputRR.getColumnInfos().get(pos);
          posns.add(pos);
          break;
        }
        pos++;
      }
    }

    return genConvertCol(dest, qb, tab, table_desc, input, posns, convert);
  }

  @SuppressWarnings("nls")
  private Operator genReduceSinkPlanForSortingBucketing(Table tab, Operator input,
                                                        ArrayList<ExprNodeDesc> sortCols,
                                                        ArrayList<ExprNodeDesc> partitionCols,
                                                        int numReducers)
    throws SemanticException {
    RowResolver inputRR = opParseCtx.get(input).getRR();

    // For the generation of the values expression just get the inputs
    // signature and generate field expressions for those
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    ArrayList<ExprNodeDesc> valueCols = new ArrayList<ExprNodeDesc>();
    for (ColumnInfo colInfo : inputRR.getColumnInfos()) {
      valueCols.add(new ExprNodeColumnDesc(colInfo.getType(), colInfo
          .getInternalName(), colInfo.getTabAlias(), colInfo
          .getIsPartitionCol()));
      colExprMap.put(colInfo.getInternalName(), valueCols
          .get(valueCols.size() - 1));
    }

    ArrayList<String> outputColumns = new ArrayList<String>();
    for (int i = 0; i < valueCols.size(); i++) {
      outputColumns.add(getColumnInternalName(i));
    }

    StringBuilder order = new StringBuilder();
    for (int i = 0; i < sortCols.size(); i++) {
      order.append("+");
    }

    Operator interim = putOpInsertMap(OperatorFactory.getAndMakeChild(PlanUtils
        .getReduceSinkDesc(sortCols, valueCols, outputColumns, false, -1,
                           partitionCols, order.toString(), numReducers),
        new RowSchema(inputRR.getColumnInfos()), input), inputRR);
    interim.setColumnExprMap(colExprMap);

    // Add the extract operator to get the value fields
    RowResolver out_rwsch = new RowResolver();
    RowResolver interim_rwsch = inputRR;
    Integer pos = Integer.valueOf(0);
    for (ColumnInfo colInfo : interim_rwsch.getColumnInfos()) {
      String[] info = interim_rwsch.reverseLookup(colInfo.getInternalName());
      out_rwsch.put(info[0], info[1], new ColumnInfo(
          getColumnInternalName(pos), colInfo.getType(), info[0], false));
      pos = Integer.valueOf(pos.intValue() + 1);
    }

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new ExtractDesc(new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo,
        Utilities.ReduceField.VALUE.toString(), "", false)), new RowSchema(
        out_rwsch.getColumnInfos()), interim), out_rwsch);

    LOG.debug("Created ReduceSink Plan for table: " + tab.getTableName() + " row schema: "
        + out_rwsch.toString());
    return output;

  }

  @SuppressWarnings("nls")
  private Operator genReduceSinkPlan(String dest, QB qb, Operator input,
      int numReducers) throws SemanticException {

    RowResolver inputRR = opParseCtx.get(input).getRR();

    // First generate the expression for the partition and sort keys
    // The cluster by clause / distribute by clause has the aliases for
    // partition function
    ASTNode partitionExprs = qb.getParseInfo().getClusterByForClause(dest);
    if (partitionExprs == null) {
      partitionExprs = qb.getParseInfo().getDistributeByForClause(dest);
    }
    ArrayList<ExprNodeDesc> partitionCols = new ArrayList<ExprNodeDesc>();
    if (partitionExprs != null) {
      int ccount = partitionExprs.getChildCount();
      for (int i = 0; i < ccount; ++i) {
        ASTNode cl = (ASTNode) partitionExprs.getChild(i);
        partitionCols.add(genExprNodeDesc(cl, inputRR));
      }
    }

    ASTNode sortExprs = qb.getParseInfo().getClusterByForClause(dest);
    if (sortExprs == null) {
      sortExprs = qb.getParseInfo().getSortByForClause(dest);
    }

    if (sortExprs == null) {
      sortExprs = qb.getParseInfo().getOrderByForClause(dest);
      if (sortExprs != null) {
        assert numReducers == 1;
        // in strict mode, in the presence of order by, limit must be specified
        Integer limit = qb.getParseInfo().getDestLimit(dest);
        if (conf.getVar(HiveConf.ConfVars.HIVEMAPREDMODE).equalsIgnoreCase(
            "strict")
            && limit == null) {
          throw new SemanticException(ErrorMsg.NO_LIMIT_WITH_ORDERBY
              .getMsg(sortExprs));
        }
      }
    }

    ArrayList<ExprNodeDesc> sortCols = new ArrayList<ExprNodeDesc>();
    StringBuilder order = new StringBuilder();
    if (sortExprs != null) {
      int ccount = sortExprs.getChildCount();
      for (int i = 0; i < ccount; ++i) {
        ASTNode cl = (ASTNode) sortExprs.getChild(i);

        if (cl.getType() == HiveParser.TOK_TABSORTCOLNAMEASC) {
          // SortBy ASC
          order.append("+");
          cl = (ASTNode) cl.getChild(0);
        } else if (cl.getType() == HiveParser.TOK_TABSORTCOLNAMEDESC) {
          // SortBy DESC
          order.append("-");
          cl = (ASTNode) cl.getChild(0);
        } else {
          // ClusterBy
          order.append("+");
        }
        ExprNodeDesc exprNode = genExprNodeDesc(cl, inputRR);
        sortCols.add(exprNode);
      }
    }

    // For the generation of the values expression just get the inputs
    // signature and generate field expressions for those
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    ArrayList<ExprNodeDesc> valueCols = new ArrayList<ExprNodeDesc>();
    for (ColumnInfo colInfo : inputRR.getColumnInfos()) {
      valueCols.add(new ExprNodeColumnDesc(colInfo.getType(), colInfo
          .getInternalName(), colInfo.getTabAlias(), colInfo
          .getIsPartitionCol()));
      colExprMap.put(colInfo.getInternalName(), valueCols
          .get(valueCols.size() - 1));
    }

    ArrayList<String> outputColumns = new ArrayList<String>();
    for (int i = 0; i < valueCols.size(); i++) {
      outputColumns.add(getColumnInternalName(i));
    }
    Operator interim = putOpInsertMap(OperatorFactory.getAndMakeChild(PlanUtils
        .getReduceSinkDesc(sortCols, valueCols, outputColumns, false, -1,
        partitionCols, order.toString(), numReducers),
        new RowSchema(inputRR.getColumnInfos()), input), inputRR);
    interim.setColumnExprMap(colExprMap);

    // Add the extract operator to get the value fields
    RowResolver out_rwsch = new RowResolver();
    RowResolver interim_rwsch = inputRR;
    Integer pos = Integer.valueOf(0);
    for (ColumnInfo colInfo : interim_rwsch.getColumnInfos()) {
      String[] info = interim_rwsch.reverseLookup(colInfo.getInternalName());
      out_rwsch.put(info[0], info[1], new ColumnInfo(
          getColumnInternalName(pos), colInfo.getType(), info[0], false));
      pos = Integer.valueOf(pos.intValue() + 1);
    }

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new ExtractDesc(new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo,
        Utilities.ReduceField.VALUE.toString(), "", false)), new RowSchema(
        out_rwsch.getColumnInfos()), interim), out_rwsch);

    LOG.debug("Created ReduceSink Plan for clause: " + dest + " row schema: "
        + out_rwsch.toString());
    return output;
  }

  private Operator genJoinOperatorChildren(QBJoinTree join, Operator left,
      Operator[] right, HashSet<Integer> omitOpts) throws SemanticException {

    RowResolver outputRS = new RowResolver();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    // all children are base classes
    Operator<?>[] rightOps = new Operator[right.length];
    int outputPos = 0;

    Map<String, Byte> reversedExprs = new HashMap<String, Byte>();
    HashMap<Byte, List<ExprNodeDesc>> exprMap = new HashMap<Byte, List<ExprNodeDesc>>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    HashMap<Integer, Set<String>> posToAliasMap = new HashMap<Integer, Set<String>>();

    for (int pos = 0; pos < right.length; ++pos) {

      Operator input = right[pos];
      if (input == null) {
        input = left;
      }

      ArrayList<ExprNodeDesc> keyDesc = new ArrayList<ExprNodeDesc>();
      Byte tag = Byte.valueOf((byte) (((ReduceSinkDesc) (input.getConf()))
          .getTag()));

      // check whether this input operator produces output
      if (omitOpts == null || !omitOpts.contains(pos)) {
        // prepare output descriptors for the input opt
        RowResolver inputRS = opParseCtx.get(input).getRR();
        Iterator<String> keysIter = inputRS.getTableNames().iterator();
        Set<String> aliases = posToAliasMap.get(pos);
        if (aliases == null) {
          aliases = new HashSet<String>();
          posToAliasMap.put(pos, aliases);
        }
        while (keysIter.hasNext()) {
          String key = keysIter.next();
          aliases.add(key);
          HashMap<String, ColumnInfo> map = inputRS.getFieldMap(key);
          Iterator<String> fNamesIter = map.keySet().iterator();
          while (fNamesIter.hasNext()) {
            String field = fNamesIter.next();
            ColumnInfo valueInfo = inputRS.get(key, field);
            keyDesc.add(new ExprNodeColumnDesc(valueInfo.getType(), valueInfo
                .getInternalName(), valueInfo.getTabAlias(), valueInfo
                .getIsPartitionCol()));

            if (outputRS.get(key, field) == null) {
              String colName = getColumnInternalName(outputPos);
              outputPos++;
              outputColumnNames.add(colName);
              colExprMap.put(colName, keyDesc.get(keyDesc.size() - 1));
              outputRS.put(key, field, new ColumnInfo(colName, valueInfo
                  .getType(), key, false));
              reversedExprs.put(colName, tag);
            }
          }
        }
      }
      exprMap.put(tag, keyDesc);
      rightOps[pos] = input;
    }

    JoinCondDesc[] joinCondns = new JoinCondDesc[join.getJoinCond().length];
    for (int i = 0; i < join.getJoinCond().length; i++) {
      JoinCond condn = join.getJoinCond()[i];
      joinCondns[i] = new JoinCondDesc(condn);
    }

    JoinDesc desc = new JoinDesc(exprMap, outputColumnNames, joinCondns);
    desc.setReversedExprs(reversedExprs);
    JoinOperator joinOp = (JoinOperator) OperatorFactory.getAndMakeChild(desc,
        new RowSchema(outputRS.getColumnInfos()), rightOps);
    joinOp.setColumnExprMap(colExprMap);
    joinOp.setPosToAliasMap(posToAliasMap);
    return putOpInsertMap(joinOp, outputRS);
  }

  @SuppressWarnings("nls")
  private Operator genJoinReduceSinkChild(QB qb, QBJoinTree joinTree,
      Operator child, String srcName, int pos) throws SemanticException {
    RowResolver inputRS = opParseCtx.get(child).getRR();
    RowResolver outputRS = new RowResolver();
    ArrayList<String> outputColumns = new ArrayList<String>();
    ArrayList<ExprNodeDesc> reduceKeys = new ArrayList<ExprNodeDesc>();

    // Compute join keys and store in reduceKeys
    ArrayList<ASTNode> exprs = joinTree.getExpressions().get(pos);
    for (int i = 0; i < exprs.size(); i++) {
      ASTNode expr = exprs.get(i);
      reduceKeys.add(genExprNodeDesc(expr, inputRS));
    }

    // Walk over the input row resolver and copy in the output
    ArrayList<ExprNodeDesc> reduceValues = new ArrayList<ExprNodeDesc>();
    Iterator<String> tblNamesIter = inputRS.getTableNames().iterator();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    while (tblNamesIter.hasNext()) {
      String src = tblNamesIter.next();
      HashMap<String, ColumnInfo> fMap = inputRS.getFieldMap(src);
      for (Map.Entry<String, ColumnInfo> entry : fMap.entrySet()) {
        String field = entry.getKey();
        ColumnInfo valueInfo = entry.getValue();
        ExprNodeColumnDesc inputExpr = new ExprNodeColumnDesc(valueInfo
            .getType(), valueInfo.getInternalName(), valueInfo.getTabAlias(),
            valueInfo.getIsPartitionCol());
        reduceValues.add(inputExpr);
        if (outputRS.get(src, field) == null) {
          String col = getColumnInternalName(reduceValues.size() - 1);
          outputColumns.add(col);
          ColumnInfo newColInfo = new ColumnInfo(Utilities.ReduceField.VALUE
              .toString()
              + "." + col, valueInfo.getType(), src, false);
          colExprMap.put(newColInfo.getInternalName(), inputExpr);
          outputRS.put(src, field, newColInfo);
        }
      }
    }

    int numReds = -1;

    // Use only 1 reducer in case of cartesian product
    if (reduceKeys.size() == 0) {
      numReds = 1;

      // Cartesian product is not supported in strict mode
      if (conf.getVar(HiveConf.ConfVars.HIVEMAPREDMODE).equalsIgnoreCase(
          "strict")) {
        throw new SemanticException(ErrorMsg.NO_CARTESIAN_PRODUCT.getMsg());
      }
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(reduceKeys,
        reduceValues, outputColumns, false, joinTree.getNextTag(),
        reduceKeys.size(), numReds), new RowSchema(outputRS
        .getColumnInfos()), child), outputRS);
    rsOp.setColumnExprMap(colExprMap);
    return rsOp;
  }

  private Operator genJoinOperator(QB qb, QBJoinTree joinTree,
      HashMap<String, Operator> map) throws SemanticException {
    QBJoinTree leftChild = joinTree.getJoinSrc();
    Operator joinSrcOp = null;
    if (leftChild != null) {
      Operator joinOp = genJoinOperator(qb, leftChild, map);
      ArrayList<ASTNode> filter = joinTree.getFilters().get(0);
      for (ASTNode cond : filter) {
        joinOp = genFilterPlan(qb, cond, joinOp);
      }

      joinSrcOp = genJoinReduceSinkChild(qb, joinTree, joinOp, null, 0);
    }

    Operator[] srcOps = new Operator[joinTree.getBaseSrc().length];

    HashSet<Integer> omitOpts = null; // set of input to the join that should be
    // omitted by the output
    int pos = 0;
    for (String src : joinTree.getBaseSrc()) {
      if (src != null) {
        Operator srcOp = map.get(src);

        // for left-semi join, generate an additional selection & group-by
        // operator before ReduceSink
        ArrayList<ASTNode> fields = joinTree.getRHSSemijoinColumns(src);
        if (fields != null) {
          // the RHS table columns should be not be output from the join
          if (omitOpts == null) {
            omitOpts = new HashSet<Integer>();
          }
          omitOpts.add(pos);

          // generate a selection operator for group-by keys only
          srcOp = insertSelectForSemijoin(fields, srcOp);

          // generate a groupby operator (HASH mode) for a map-side partial
          // aggregation for semijoin
          srcOp = genMapGroupByForSemijoin(qb, fields, srcOp,
              GroupByDesc.Mode.HASH);
        }

        // generate a ReduceSink operator for the join
        srcOps[pos] = genJoinReduceSinkChild(qb, joinTree, srcOp, src, pos);
        pos++;
      } else {
        assert pos == 0;
        srcOps[pos++] = null;
      }
    }

    // Type checking and implicit type conversion for join keys
    genJoinOperatorTypeCheck(joinSrcOp, srcOps);

    JoinOperator joinOp = (JoinOperator) genJoinOperatorChildren(joinTree,
        joinSrcOp, srcOps, omitOpts);
    joinContext.put(joinOp, joinTree);
    return joinOp;
  }

  /**
   * Construct a selection operator for semijoin that filter out all fields
   * other than the group by keys.
   *
   * @param fields
   *          list of fields need to be output
   * @param input
   *          input operator
   * @return the selection operator.
   * @throws SemanticException
   */
  private Operator insertSelectForSemijoin(ArrayList<ASTNode> fields,
      Operator input) throws SemanticException {

    RowResolver inputRR = opParseCtx.get(input).getRR();
    ArrayList<ExprNodeDesc> colList = new ArrayList<ExprNodeDesc>();
    ArrayList<String> columnNames = new ArrayList<String>();

    // construct the list of columns that need to be projected
    for (ASTNode field : fields) {
      ExprNodeColumnDesc exprNode = (ExprNodeColumnDesc) genExprNodeDesc(field,
          inputRR);
      colList.add(exprNode);
      columnNames.add(exprNode.getColumn());
    }

    // create selection operator
    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new SelectDesc(colList, columnNames, false), new RowSchema(inputRR
        .getColumnInfos()), input), inputRR);

    output.setColumnExprMap(input.getColumnExprMap());
    return output;
  }

  private Operator genMapGroupByForSemijoin(QB qb, ArrayList<ASTNode> fields, // the
      // ASTNode
      // of
      // the
      // join
      // key
      // "tab.col"
      Operator inputOperatorInfo, GroupByDesc.Mode mode)
      throws SemanticException {

    RowResolver groupByInputRowResolver = opParseCtx.get(inputOperatorInfo)
        .getRR();
    RowResolver groupByOutputRowResolver = new RowResolver();
    ArrayList<ExprNodeDesc> groupByKeys = new ArrayList<ExprNodeDesc>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    ArrayList<AggregationDesc> aggregations = new ArrayList<AggregationDesc>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    qb.getParseInfo();

    groupByOutputRowResolver.setIsExprResolver(true); // join keys should only
    // be columns but not be
    // expressions

    for (int i = 0; i < fields.size(); ++i) {
      // get the group by keys to ColumnInfo
      ASTNode colName = fields.get(i);
      ExprNodeDesc grpByExprNode = genExprNodeDesc(colName,
          groupByInputRowResolver);
      groupByKeys.add(grpByExprNode);

      // generate output column names
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      ColumnInfo colInfo2 = new ColumnInfo(field, grpByExprNode.getTypeInfo(),
          "", false);
      groupByOutputRowResolver.putExpression(colName, colInfo2);

      // establish mapping from the output column to the input column
      colExprMap.put(field, grpByExprNode);
    }

    // Generate group-by operator
    Operator op = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new GroupByDesc(mode, outputColumnNames, groupByKeys, aggregations,
        false), new RowSchema(groupByOutputRowResolver.getColumnInfos()),
        inputOperatorInfo), groupByOutputRowResolver);

    op.setColumnExprMap(colExprMap);
    return op;
  }

  private void genJoinOperatorTypeCheck(Operator left, Operator[] right)
      throws SemanticException {
    // keys[i] -> ArrayList<exprNodeDesc> for the i-th join operator key list
    ArrayList<ArrayList<ExprNodeDesc>> keys = new ArrayList<ArrayList<ExprNodeDesc>>();
    int keyLength = 0;
    for (int i = 0; i < right.length; i++) {
      Operator oi = (i == 0 && right[i] == null ? left : right[i]);
      ReduceSinkDesc now = ((ReduceSinkOperator) (oi)).getConf();
      if (i == 0) {
        keyLength = now.getKeyCols().size();
      } else {
        assert (keyLength == now.getKeyCols().size());
      }
      keys.add(now.getKeyCols());
    }
    // implicit type conversion hierarchy
    for (int k = 0; k < keyLength; k++) {
      // Find the common class for type conversion
      TypeInfo commonType = keys.get(0).get(k).getTypeInfo();
      for (int i = 1; i < right.length; i++) {
        TypeInfo a = commonType;
        TypeInfo b = keys.get(i).get(k).getTypeInfo();
        commonType = FunctionRegistry.getCommonClassForComparison(a, b);
        if (commonType == null) {
          throw new SemanticException(
              "Cannot do equality join on different types: " + a.getTypeName()
              + " and " + b.getTypeName());
        }
      }
      // Add implicit type conversion if necessary
      for (int i = 0; i < right.length; i++) {
        if (!commonType.equals(keys.get(i).get(k).getTypeInfo())) {
          keys.get(i).set(
              k,
              TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(
              commonType.getTypeName(), keys.get(i).get(k)));
        }
      }
    }
    // regenerate keySerializationInfo because the ReduceSinkOperator's
    // output key types might have changed.
    for (int i = 0; i < right.length; i++) {
      Operator oi = (i == 0 && right[i] == null ? left : right[i]);
      ReduceSinkDesc now = ((ReduceSinkOperator) (oi)).getConf();

      now.setKeySerializeInfo(PlanUtils.getReduceKeyTableDesc(PlanUtils
          .getFieldSchemasFromColumnList(now.getKeyCols(), "joinkey"), now
          .getOrder()));
    }
  }

  private Operator genJoinPlan(QB qb, HashMap<String, Operator> map)
      throws SemanticException {
    QBJoinTree joinTree = qb.getQbJoinTree();
    Operator joinOp = genJoinOperator(qb, joinTree, map);
    return joinOp;
  }

  /**
   * Extract the filters from the join condition and push them on top of the
   * source operators. This procedure traverses the query tree recursively,
   */
  private void pushJoinFilters(QB qb, QBJoinTree joinTree,
      HashMap<String, Operator> map) throws SemanticException {
    ArrayList<ArrayList<ASTNode>> filters = joinTree.getFilters();
    if (joinTree.getJoinSrc() != null) {
      pushJoinFilters(qb, joinTree.getJoinSrc(), map);
    }

    int pos = 0;
    for (String src : joinTree.getBaseSrc()) {
      if (src != null) {
        Operator srcOp = map.get(src);
        ArrayList<ASTNode> filter = filters.get(pos);
        for (ASTNode cond : filter) {
          srcOp = genFilterPlan(qb, cond, srcOp);
        }
        map.put(src, srcOp);
      }
      pos++;
    }
  }

  private List<String> getMapSideJoinTables(QB qb) {
    List<String> cols = new ArrayList<String>();
    ASTNode hints = qb.getParseInfo().getHints();
    for (int pos = 0; pos < hints.getChildCount(); pos++) {
      ASTNode hint = (ASTNode) hints.getChild(pos);
      if (((ASTNode) hint.getChild(0)).getToken().getType() == HiveParser.TOK_MAPJOIN) {
        ASTNode hintTblNames = (ASTNode) hint.getChild(1);
        int numCh = hintTblNames.getChildCount();
        for (int tblPos = 0; tblPos < numCh; tblPos++) {
          String tblName = ((ASTNode) hintTblNames.getChild(tblPos)).getText()
              .toLowerCase();
          if (!cols.contains(tblName)) {
            cols.add(tblName);
          }
        }
      }
    }

    return cols;
  }

  private QBJoinTree genUniqueJoinTree(QB qb, ASTNode joinParseTree)
      throws SemanticException {
    QBJoinTree joinTree = new QBJoinTree();
    joinTree.setNoOuterJoin(false);

    joinTree.setExpressions(new ArrayList<ArrayList<ASTNode>>());
    joinTree.setFilters(new ArrayList<ArrayList<ASTNode>>());

    // Create joinTree structures to fill them up later
    ArrayList<String> rightAliases = new ArrayList<String>();
    ArrayList<String> leftAliases = new ArrayList<String>();
    ArrayList<String> baseSrc = new ArrayList<String>();
    ArrayList<Boolean> preserved = new ArrayList<Boolean>();

    boolean lastPreserved = false;
    int cols = -1;

    for (int i = 0; i < joinParseTree.getChildCount(); i++) {
      ASTNode child = (ASTNode) joinParseTree.getChild(i);

      switch (child.getToken().getType()) {
      case HiveParser.TOK_TABREF:
        // Handle a table - populate aliases appropriately:
        // leftAliases should contain the first table, rightAliases should
        // contain all other tables and baseSrc should contain all tables

        String tableName = unescapeIdentifier(child.getChild(0).getText());
        String alias = child.getChildCount() == 1 ? tableName
            : unescapeIdentifier(child.getChild(child.getChildCount() - 1)
            .getText().toLowerCase());

        if (i == 0) {
          leftAliases.add(alias);
          joinTree.setLeftAlias(alias);
        } else {
          rightAliases.add(alias);
        }
        baseSrc.add(alias);

        preserved.add(lastPreserved);
        lastPreserved = false;
        break;

      case HiveParser.TOK_EXPLIST:
        if (cols == -1 && child.getChildCount() != 0) {
          cols = child.getChildCount();
        } else if (child.getChildCount() != cols) {
          throw new SemanticException("Tables with different or invalid "
              + "number of keys in UNIQUEJOIN");
        }

        ArrayList<ASTNode> expressions = new ArrayList<ASTNode>();
        ArrayList<ASTNode> filt = new ArrayList<ASTNode>();

        for (Node exp : child.getChildren()) {
          expressions.add((ASTNode) exp);
        }

        joinTree.getExpressions().add(expressions);
        joinTree.getFilters().add(filt);
        break;

      case HiveParser.KW_PRESERVE:
        lastPreserved = true;
        break;

      case HiveParser.TOK_SUBQUERY:
        throw new SemanticException(
            "Subqueries are not supported in UNIQUEJOIN");

      default:
        throw new SemanticException("Unexpected UNIQUEJOIN structure");
      }
    }

    joinTree.setBaseSrc(baseSrc.toArray(new String[0]));
    joinTree.setLeftAliases(leftAliases.toArray(new String[0]));
    joinTree.setRightAliases(rightAliases.toArray(new String[0]));

    JoinCond[] condn = new JoinCond[preserved.size()];
    for (int i = 0; i < condn.length; i++) {
      condn[i] = new JoinCond(preserved.get(i));
    }
    joinTree.setJoinCond(condn);

    if (qb.getParseInfo().getHints() != null) {
      parseStreamTables(joinTree, qb);
    }

    return joinTree;
  }

  private QBJoinTree genJoinTree(QB qb, ASTNode joinParseTree)
      throws SemanticException {
    QBJoinTree joinTree = new QBJoinTree();
    JoinCond[] condn = new JoinCond[1];

    switch (joinParseTree.getToken().getType()) {
    case HiveParser.TOK_LEFTOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.LEFTOUTER);
      break;
    case HiveParser.TOK_RIGHTOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.RIGHTOUTER);
      break;
    case HiveParser.TOK_FULLOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.FULLOUTER);
      break;
    case HiveParser.TOK_LEFTSEMIJOIN:
      joinTree.setNoSemiJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.LEFTSEMI);
      break;
    default:
      condn[0] = new JoinCond(0, 1, JoinType.INNER);
      joinTree.setNoOuterJoin(true);
      break;
    }

    joinTree.setJoinCond(condn);

    ASTNode left = (ASTNode) joinParseTree.getChild(0);
    ASTNode right = (ASTNode) joinParseTree.getChild(1);

    if ((left.getToken().getType() == HiveParser.TOK_TABREF)
        || (left.getToken().getType() == HiveParser.TOK_SUBQUERY)) {
      String table_name = unescapeIdentifier(left.getChild(0).getText());
      String alias = left.getChildCount() == 1 ? table_name
          : unescapeIdentifier(left.getChild(left.getChildCount() - 1)
          .getText().toLowerCase());
      joinTree.setLeftAlias(alias);
      String[] leftAliases = new String[1];
      leftAliases[0] = alias;
      joinTree.setLeftAliases(leftAliases);
      String[] children = new String[2];
      children[0] = alias;
      joinTree.setBaseSrc(children);
    } else if (isJoinToken(left)) {
      QBJoinTree leftTree = genJoinTree(qb, left);
      joinTree.setJoinSrc(leftTree);
      String[] leftChildAliases = leftTree.getLeftAliases();
      String leftAliases[] = new String[leftChildAliases.length + 1];
      for (int i = 0; i < leftChildAliases.length; i++) {
        leftAliases[i] = leftChildAliases[i];
      }
      leftAliases[leftChildAliases.length] = leftTree.getRightAliases()[0];
      joinTree.setLeftAliases(leftAliases);
    } else {
      assert (false);
    }

    if ((right.getToken().getType() == HiveParser.TOK_TABREF)
        || (right.getToken().getType() == HiveParser.TOK_SUBQUERY)) {
      String tableName = unescapeIdentifier(right.getChild(0).getText());
      String alias = right.getChildCount() == 1 ? tableName
          : unescapeIdentifier(right.getChild(right.getChildCount() - 1)
          .getText().toLowerCase());
      String[] rightAliases = new String[1];
      rightAliases[0] = alias;
      joinTree.setRightAliases(rightAliases);
      String[] children = joinTree.getBaseSrc();
      if (children == null) {
        children = new String[2];
      }
      children[1] = alias;
      joinTree.setBaseSrc(children);
      // remember rhs table for semijoin
      if (joinTree.getNoSemiJoin() == false) {
        joinTree.addRHSSemijoin(alias);
      }
    } else {
      assert false;
    }

    ArrayList<ArrayList<ASTNode>> expressions = new ArrayList<ArrayList<ASTNode>>();
    expressions.add(new ArrayList<ASTNode>());
    expressions.add(new ArrayList<ASTNode>());
    joinTree.setExpressions(expressions);

    ArrayList<ArrayList<ASTNode>> filters = new ArrayList<ArrayList<ASTNode>>();
    filters.add(new ArrayList<ASTNode>());
    filters.add(new ArrayList<ASTNode>());
    joinTree.setFilters(filters);

    ASTNode joinCond = (ASTNode) joinParseTree.getChild(2);
    ArrayList<String> leftSrc = new ArrayList<String>();
    parseJoinCondition(joinTree, joinCond, leftSrc);
    if (leftSrc.size() == 1) {
      joinTree.setLeftAlias(leftSrc.get(0));
    }

    // check the hints to see if the user has specified a map-side join. This
    // will be removed later on, once the cost-based
    // infrastructure is in place
    if (qb.getParseInfo().getHints() != null) {
      List<String> mapSideTables = getMapSideJoinTables(qb);
      List<String> mapAliases = joinTree.getMapAliases();

      for (String mapTbl : mapSideTables) {
        boolean mapTable = false;
        for (String leftAlias : joinTree.getLeftAliases()) {
          if (mapTbl.equalsIgnoreCase(leftAlias)) {
            mapTable = true;
          }
        }
        for (String rightAlias : joinTree.getRightAliases()) {
          if (mapTbl.equalsIgnoreCase(rightAlias)) {
            mapTable = true;
          }
        }

        if (mapTable) {
          if (mapAliases == null) {
            mapAliases = new ArrayList<String>();
          }
          mapAliases.add(mapTbl);
          joinTree.setMapSideJoin(true);
        }
      }

      joinTree.setMapAliases(mapAliases);

      parseStreamTables(joinTree, qb);
    }

    return joinTree;
  }

  private void parseStreamTables(QBJoinTree joinTree, QB qb) {
    List<String> streamAliases = joinTree.getStreamAliases();

    for (Node hintNode : qb.getParseInfo().getHints().getChildren()) {
      ASTNode hint = (ASTNode) hintNode;
      if (hint.getChild(0).getType() == HiveParser.TOK_STREAMTABLE) {
        for (int i = 0; i < hint.getChild(1).getChildCount(); i++) {
          if (streamAliases == null) {
            streamAliases = new ArrayList<String>();
          }
          streamAliases.add(hint.getChild(1).getChild(i).getText());
        }
      }
    }

    joinTree.setStreamAliases(streamAliases);
  }

  private void mergeJoins(QB qb, QBJoinTree parent, QBJoinTree node,
      QBJoinTree target, int pos) {
    String[] nodeRightAliases = node.getRightAliases();
    String[] trgtRightAliases = target.getRightAliases();
    String[] rightAliases = new String[nodeRightAliases.length
        + trgtRightAliases.length];

    for (int i = 0; i < trgtRightAliases.length; i++) {
      rightAliases[i] = trgtRightAliases[i];
    }
    for (int i = 0; i < nodeRightAliases.length; i++) {
      rightAliases[i + trgtRightAliases.length] = nodeRightAliases[i];
    }
    target.setRightAliases(rightAliases);

    String[] nodeBaseSrc = node.getBaseSrc();
    String[] trgtBaseSrc = target.getBaseSrc();
    String[] baseSrc = new String[nodeBaseSrc.length + trgtBaseSrc.length - 1];

    for (int i = 0; i < trgtBaseSrc.length; i++) {
      baseSrc[i] = trgtBaseSrc[i];
    }
    for (int i = 1; i < nodeBaseSrc.length; i++) {
      baseSrc[i + trgtBaseSrc.length - 1] = nodeBaseSrc[i];
    }
    target.setBaseSrc(baseSrc);

    ArrayList<ArrayList<ASTNode>> expr = target.getExpressions();
    for (int i = 0; i < nodeRightAliases.length; i++) {
      expr.add(node.getExpressions().get(i + 1));
    }

    ArrayList<ArrayList<ASTNode>> filter = target.getFilters();
    for (int i = 0; i < nodeRightAliases.length; i++) {
      filter.add(node.getFilters().get(i + 1));
    }

    if (node.getFilters().get(0).size() != 0) {
      ArrayList<ASTNode> filterPos = filter.get(pos);
      filterPos.addAll(node.getFilters().get(0));
    }

    if (qb.getQbJoinTree() == node) {
      qb.setQbJoinTree(node.getJoinSrc());
    } else {
      parent.setJoinSrc(node.getJoinSrc());
    }

    if (node.getNoOuterJoin() && target.getNoOuterJoin()) {
      target.setNoOuterJoin(true);
    } else {
      target.setNoOuterJoin(false);
    }

    if (node.getNoSemiJoin() && target.getNoSemiJoin()) {
      target.setNoSemiJoin(true);
    } else {
      target.setNoSemiJoin(false);
    }

    target.mergeRHSSemijoin(node);

    JoinCond[] nodeCondns = node.getJoinCond();
    int nodeCondnsSize = nodeCondns.length;
    JoinCond[] targetCondns = target.getJoinCond();
    int targetCondnsSize = targetCondns.length;
    JoinCond[] newCondns = new JoinCond[nodeCondnsSize + targetCondnsSize];
    for (int i = 0; i < targetCondnsSize; i++) {
      newCondns[i] = targetCondns[i];
    }

    for (int i = 0; i < nodeCondnsSize; i++) {
      JoinCond nodeCondn = nodeCondns[i];
      if (nodeCondn.getLeft() == 0) {
        nodeCondn.setLeft(pos);
      } else {
        nodeCondn.setLeft(nodeCondn.getLeft() + targetCondnsSize);
      }
      nodeCondn.setRight(nodeCondn.getRight() + targetCondnsSize);
      newCondns[targetCondnsSize + i] = nodeCondn;
    }

    target.setJoinCond(newCondns);
    if (target.isMapSideJoin()) {
      assert node.isMapSideJoin();
      List<String> mapAliases = target.getMapAliases();
      for (String mapTbl : node.getMapAliases()) {
        if (!mapAliases.contains(mapTbl)) {
          mapAliases.add(mapTbl);
        }
      }
      target.setMapAliases(mapAliases);
    }
  }

  private int findMergePos(QBJoinTree node, QBJoinTree target) {
    int res = -1;
    String leftAlias = node.getLeftAlias();
    if (leftAlias == null) {
      return -1;
    }

    ArrayList<ASTNode> nodeCondn = node.getExpressions().get(0);
    ArrayList<ASTNode> targetCondn = null;

    if (leftAlias.equals(target.getLeftAlias())) {
      targetCondn = target.getExpressions().get(0);
      res = 0;
    } else {
      for (int i = 0; i < target.getRightAliases().length; i++) {
        if (leftAlias.equals(target.getRightAliases()[i])) {
          targetCondn = target.getExpressions().get(i + 1);
          res = i + 1;
          break;
        }
      }
    }

    if ((targetCondn == null) || (nodeCondn.size() != targetCondn.size())) {
      return -1;
    }

    for (int i = 0; i < nodeCondn.size(); i++) {
      if (!nodeCondn.get(i).toStringTree().equals(
          targetCondn.get(i).toStringTree())) {
        return -1;
      }
    }

    return res;
  }

  private boolean mergeJoinNodes(QB qb, QBJoinTree parent, QBJoinTree node,
      QBJoinTree target) {
    if (target == null) {
      return false;
    }

    int res = findMergePos(node, target);
    if (res != -1) {
      mergeJoins(qb, parent, node, target, res);
      return true;
    }

    return mergeJoinNodes(qb, parent, node, target.getJoinSrc());
  }

  private void mergeJoinTree(QB qb) {
    QBJoinTree root = qb.getQbJoinTree();
    QBJoinTree parent = null;
    while (root != null) {
      boolean merged = mergeJoinNodes(qb, parent, root, root.getJoinSrc());

      if (parent == null) {
        if (merged) {
          root = qb.getQbJoinTree();
        } else {
          parent = root;
          root = root.getJoinSrc();
        }
      } else {
        parent = parent.getJoinSrc();
        root = parent.getJoinSrc();
      }
    }
  }

  private Operator insertSelectAllPlanForGroupBy(String dest, Operator input)
      throws SemanticException {
    OpParseContext inputCtx = opParseCtx.get(input);
    RowResolver inputRR = inputCtx.getRR();
    ArrayList<ColumnInfo> columns = inputRR.getColumnInfos();
    ArrayList<ExprNodeDesc> colList = new ArrayList<ExprNodeDesc>();
    ArrayList<String> columnNames = new ArrayList<String>();
    for (int i = 0; i < columns.size(); i++) {
      ColumnInfo col = columns.get(i);
      colList.add(new ExprNodeColumnDesc(col.getType(), col.getInternalName(),
          col.getTabAlias(), col.getIsPartitionCol()));
      columnNames.add(col.getInternalName());
    }
    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new SelectDesc(colList, columnNames, true), new RowSchema(inputRR
        .getColumnInfos()), input), inputRR);
    output.setColumnExprMap(input.getColumnExprMap());
    return output;
  }

  // Return the common distinct expression
  // There should be more than 1 destination, with group bys in all of them.
  private List<ASTNode> getCommonDistinctExprs(QB qb, Operator input) {
    RowResolver inputRR = opParseCtx.get(input).getRR();
    QBParseInfo qbp = qb.getParseInfo();

    TreeSet<String> ks = new TreeSet<String>();
    ks.addAll(qbp.getClauseNames());

    // Go over all the destination tables
    if (ks.size() <= 1) {
      return null;
    }

    List<ExprNodeDesc> oldList = null;
    List<ASTNode> oldASTList = null;

    for (String dest : ks) {
      // If a filter is present, common processing is not possible
      if (qbp.getWhrForClause(dest) != null) {
        return null;
      }

      if (qbp.getAggregationExprsForClause(dest).size() == 0
          && getGroupByForClause(qbp, dest).size() == 0) {
        return null;
      }

      // All distinct expressions must be the same
      ASTNode value = qbp.getDistinctFuncExprForClause(dest);
      if (value == null) {
        return null;
      }

      List<ExprNodeDesc> currDestList = new ArrayList<ExprNodeDesc>();
      List<ASTNode> currASTList = new ArrayList<ASTNode>();
      try {
        // 0 is function name
        for (int i = 1; i < value.getChildCount(); i++) {
          ASTNode parameter = (ASTNode) value.getChild(i);
          currDestList.add(genExprNodeDesc(parameter, inputRR));
          currASTList.add(parameter);
        }
      } catch (SemanticException e) {
        return null;
      }

      if (oldList == null) {
        oldList = currDestList;
        oldASTList = currASTList;
      } else {
        if (oldList.size() != currDestList.size()) {
          return null;
        }
        for (int pos = 0; pos < oldList.size(); pos++) {
          if (!oldList.get(pos).isSame(currDestList.get(pos))) {
            return null;
          }
        }
      }
    }

    return oldASTList;
  }

  private Operator createCommonReduceSink(QB qb, Operator input)
      throws SemanticException {
    // Go over all the tables and extract the common distinct key
    List<ASTNode> distExprs = getCommonDistinctExprs(qb, input);

    QBParseInfo qbp = qb.getParseInfo();
    TreeSet<String> ks = new TreeSet<String>();
    ks.addAll(qbp.getClauseNames());

    // Pass the entire row
    RowResolver inputRR = opParseCtx.get(input).getRR();
    RowResolver reduceSinkOutputRowResolver = new RowResolver();
    reduceSinkOutputRowResolver.setIsExprResolver(true);
    ArrayList<ExprNodeDesc> reduceKeys = new ArrayList<ExprNodeDesc>();
    ArrayList<ExprNodeDesc> reduceValues = new ArrayList<ExprNodeDesc>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();

    // Pre-compute distinct group-by keys and store in reduceKeys

    List<String> outputColumnNames = new ArrayList<String>();
    for (ASTNode distn : distExprs) {
      ExprNodeDesc distExpr = genExprNodeDesc(distn, inputRR);
      reduceKeys.add(distExpr);
      if (reduceSinkOutputRowResolver.getExpression(distn) == null) {
        outputColumnNames.add(getColumnInternalName(reduceKeys.size() - 1));
        String field = Utilities.ReduceField.KEY.toString() + "."
            + getColumnInternalName(reduceKeys.size() - 1);
        ColumnInfo colInfo = new ColumnInfo(field, reduceKeys.get(
            reduceKeys.size() - 1).getTypeInfo(), "", false);
        reduceSinkOutputRowResolver.putExpression(distn, colInfo);
        colExprMap.put(colInfo.getInternalName(), distExpr);
      }
    }

    // Go over all the grouping keys and aggregations
    for (String dest : ks) {

      List<ASTNode> grpByExprs = getGroupByForClause(qbp, dest);
      for (int i = 0; i < grpByExprs.size(); ++i) {
        ASTNode grpbyExpr = grpByExprs.get(i);

        if (reduceSinkOutputRowResolver.getExpression(grpbyExpr) == null) {
          ExprNodeDesc grpByExprNode = genExprNodeDesc(grpbyExpr, inputRR);
          reduceValues.add(grpByExprNode);
          String field = Utilities.ReduceField.VALUE.toString() + "."
              + getColumnInternalName(reduceValues.size() - 1);
          ColumnInfo colInfo = new ColumnInfo(field, reduceValues.get(
              reduceValues.size() - 1).getTypeInfo(), "", false);
          reduceSinkOutputRowResolver.putExpression(grpbyExpr, colInfo);
          outputColumnNames.add(getColumnInternalName(reduceValues.size() - 1));
        }
      }

      // For each aggregation
      HashMap<String, ASTNode> aggregationTrees = qbp
          .getAggregationExprsForClause(dest);
      assert (aggregationTrees != null);

      for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
        ASTNode value = entry.getValue();
        value.getChild(0).getText();

        // 0 is the function name
        for (int i = 1; i < value.getChildCount(); i++) {
          ASTNode paraExpr = (ASTNode) value.getChild(i);

          if (reduceSinkOutputRowResolver.getExpression(paraExpr) == null) {
            ExprNodeDesc paraExprNode = genExprNodeDesc(paraExpr, inputRR);
            reduceValues.add(paraExprNode);
            String field = Utilities.ReduceField.VALUE.toString() + "."
                + getColumnInternalName(reduceValues.size() - 1);
            ColumnInfo colInfo = new ColumnInfo(field, reduceValues.get(
                reduceValues.size() - 1).getTypeInfo(), "", false);
            reduceSinkOutputRowResolver.putExpression(paraExpr, colInfo);
            outputColumnNames
                .add(getColumnInternalName(reduceValues.size() - 1));
          }
        }
      }
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(reduceKeys,
        reduceValues, outputColumnNames, true, -1, reduceKeys.size(), -1),
        new RowSchema(reduceSinkOutputRowResolver.getColumnInfos()), input),
        reduceSinkOutputRowResolver);

    rsOp.setColumnExprMap(colExprMap);
    return rsOp;
  }

  @SuppressWarnings("nls")
  private Operator genBodyPlan(QB qb, Operator input) throws SemanticException {

    QBParseInfo qbp = qb.getParseInfo();

    TreeSet<String> ks = new TreeSet<String>(qbp.getClauseNames());

    // For multi-group by with the same distinct, we ignore all user hints
    // currently. It doesnt matter whether he has asked to do
    // map-side aggregation or not. Map side aggregation is turned off
    boolean optimizeMultiGroupBy = (getCommonDistinctExprs(qb, input) != null);
    Operator curr = null;

    // If there are multiple group-bys, map-side aggregation is turned off,
    // there are no filters
    // and there is a single distinct, optimize that. Spray initially by the
    // distinct key,
    // no computation at the mapper. Have multiple group by operators at the
    // reducer - and then
    // proceed
    if (optimizeMultiGroupBy) {
      curr = createCommonReduceSink(qb, input);

      RowResolver currRR = opParseCtx.get(curr).getRR();
      // create a forward operator
      input = putOpInsertMap(OperatorFactory.getAndMakeChild(new ForwardDesc(),
          new RowSchema(currRR.getColumnInfos()), curr), currRR);

      for (String dest : ks) {
        curr = input;
        curr = genGroupByPlan2MRMultiGroupBy(dest, qb, curr);
        curr = genSelectPlan(dest, qb, curr);
        Integer limit = qbp.getDestLimit(dest);
        if (limit != null) {
          curr = genLimitMapRedPlan(dest, qb, curr, limit.intValue(), true);
          qb.getParseInfo().setOuterQueryLimit(limit.intValue());
        }
        curr = genFileSinkPlan(dest, qb, curr);
      }
    } else {
      // Go over all the destination tables
      for (String dest : ks) {
        curr = input;

        if (qbp.getWhrForClause(dest) != null) {
          curr = genFilterPlan(dest, qb, curr);
        }

        if (qbp.getAggregationExprsForClause(dest).size() != 0
            || getGroupByForClause(qbp, dest).size() > 0) {
          // insert a select operator here used by the ColumnPruner to reduce
          // the data to shuffle
          curr = insertSelectAllPlanForGroupBy(dest, curr);
          if (conf.getVar(HiveConf.ConfVars.HIVEMAPSIDEAGGREGATE)
              .equalsIgnoreCase("true")) {
            if (conf.getVar(HiveConf.ConfVars.HIVEGROUPBYSKEW)
                .equalsIgnoreCase("false")) {
              curr = genGroupByPlanMapAggr1MR(dest, qb, curr);
            } else {
              curr = genGroupByPlanMapAggr2MR(dest, qb, curr);
            }
          } else if (conf.getVar(HiveConf.ConfVars.HIVEGROUPBYSKEW)
              .equalsIgnoreCase("true")) {
            curr = genGroupByPlan2MR(dest, qb, curr);
          } else {
            curr = genGroupByPlan1MR(dest, qb, curr);
          }
        }

        curr = genSelectPlan(dest, qb, curr);
        Integer limit = qbp.getDestLimit(dest);

        if (qbp.getClusterByForClause(dest) != null
            || qbp.getDistributeByForClause(dest) != null
            || qbp.getOrderByForClause(dest) != null
            || qbp.getSortByForClause(dest) != null) {

          int numReducers = -1;

          // Use only 1 reducer if order by is present
          if (qbp.getOrderByForClause(dest) != null) {
            numReducers = 1;
          }

          curr = genReduceSinkPlan(dest, qb, curr, numReducers);
        }

        if (qbp.getIsSubQ()) {
          if (limit != null) {
            // In case of order by, only 1 reducer is used, so no need of
            // another shuffle
            curr = genLimitMapRedPlan(dest, qb, curr, limit.intValue(), qbp
                .getOrderByForClause(dest) != null ? false : true);
          }
        } else {
          curr = genConversionOps(dest, qb, curr);
          // exact limit can be taken care of by the fetch operator
          if (limit != null) {
            boolean extraMRStep = true;

            if (qb.getIsQuery() && qbp.getClusterByForClause(dest) == null
                && qbp.getSortByForClause(dest) == null) {
              extraMRStep = false;
            }

            curr = genLimitMapRedPlan(dest, qb, curr, limit.intValue(),
                extraMRStep);
            qb.getParseInfo().setOuterQueryLimit(limit.intValue());
          }
          curr = genFileSinkPlan(dest, qb, curr);
        }

        // change curr ops row resolver's tab aliases to query alias if it
        // exists
        if (qb.getParseInfo().getAlias() != null) {
          RowResolver rr = opParseCtx.get(curr).getRR();
          RowResolver newRR = new RowResolver();
          String alias = qb.getParseInfo().getAlias();
          for (ColumnInfo colInfo : rr.getColumnInfos()) {
            String name = colInfo.getInternalName();
            String[] tmp = rr.reverseLookup(name);
            newRR.put(alias, tmp[1], colInfo);
          }
          opParseCtx.get(curr).setRR(newRR);
        }
      }
    }

    LOG.debug("Created Body Plan for Query Block " + qb.getId());
    return curr;
  }

  @SuppressWarnings("nls")
  private Operator genUnionPlan(String unionalias, String leftalias,
      Operator leftOp, String rightalias, Operator rightOp)
      throws SemanticException {

    // Currently, the unions are not merged - each union has only 2 parents. So,
    // a n-way union will lead to (n-1) union operators.
    // This can be easily merged into 1 union
    RowResolver leftRR = opParseCtx.get(leftOp).getRR();
    RowResolver rightRR = opParseCtx.get(rightOp).getRR();
    HashMap<String, ColumnInfo> leftmap = leftRR.getFieldMap(leftalias);
    HashMap<String, ColumnInfo> rightmap = rightRR.getFieldMap(rightalias);
    // make sure the schemas of both sides are the same
    if (leftmap.size() != rightmap.size()) {
      throw new SemanticException("Schema of both sides of union should match.");
    }
    for (Map.Entry<String, ColumnInfo> lEntry : leftmap.entrySet()) {
      String field = lEntry.getKey();
      ColumnInfo lInfo = lEntry.getValue();
      ColumnInfo rInfo = rightmap.get(field);
      if (rInfo == null) {
        throw new SemanticException(
            "Schema of both sides of union should match. " + rightalias
            + " does not have the field " + field);
      }
      if (lInfo == null) {
        throw new SemanticException(
            "Schema of both sides of union should match. " + leftalias
            + " does not have the field " + field);
      }
      if (!lInfo.getInternalName().equals(rInfo.getInternalName())) {
        throw new SemanticException(
            "Schema of both sides of union should match: " + field + ":"
            + lInfo.getInternalName() + " " + rInfo.getInternalName());
      }
      if (!lInfo.getType().getTypeName().equals(rInfo.getType().getTypeName())) {
        throw new SemanticException(
            "Schema of both sides of union should match: Column " + field
            + " is of type " + lInfo.getType().getTypeName()
            + " on first table and type " + rInfo.getType().getTypeName()
            + " on second table");
      }
    }

    // construct the forward operator
    RowResolver unionoutRR = new RowResolver();
    for (Map.Entry<String, ColumnInfo> lEntry : leftmap.entrySet()) {
      String field = lEntry.getKey();
      ColumnInfo lInfo = lEntry.getValue();
      unionoutRR.put(unionalias, field, lInfo);
    }

    // If one of the children is a union, merge with it
    // else create a new one
    if ((leftOp instanceof UnionOperator) || (rightOp instanceof UnionOperator)) {
      if (leftOp instanceof UnionOperator) {
        // make left a child of right
        List<Operator<? extends Serializable>> child =
          new ArrayList<Operator<? extends Serializable>>();
        child.add(leftOp);
        rightOp.setChildOperators(child);

        List<Operator<? extends Serializable>> parent = leftOp
            .getParentOperators();
        parent.add(rightOp);

        UnionDesc uDesc = ((UnionOperator) leftOp).getConf();
        uDesc.setNumInputs(uDesc.getNumInputs() + 1);
        return putOpInsertMap(leftOp, unionoutRR);
      } else {
        // make right a child of left
        List<Operator<? extends Serializable>> child =
          new ArrayList<Operator<? extends Serializable>>();
        child.add(rightOp);
        leftOp.setChildOperators(child);

        List<Operator<? extends Serializable>> parent = rightOp
            .getParentOperators();
        parent.add(leftOp);
        UnionDesc uDesc = ((UnionOperator) rightOp).getConf();
        uDesc.setNumInputs(uDesc.getNumInputs() + 1);

        return putOpInsertMap(rightOp, unionoutRR);
      }
    }

    // Create a new union operator
    Operator<? extends Serializable> unionforward = OperatorFactory
        .getAndMakeChild(new UnionDesc(), new RowSchema(unionoutRR
        .getColumnInfos()));

    // set union operator as child of each of leftOp and rightOp
    List<Operator<? extends Serializable>> child =
      new ArrayList<Operator<? extends Serializable>>();
    child.add(unionforward);
    rightOp.setChildOperators(child);

    child = new ArrayList<Operator<? extends Serializable>>();
    child.add(unionforward);
    leftOp.setChildOperators(child);

    List<Operator<? extends Serializable>> parent =
      new ArrayList<Operator<? extends Serializable>>();
    parent.add(leftOp);
    parent.add(rightOp);
    unionforward.setParentOperators(parent);

    // create operator info list to return
    return putOpInsertMap(unionforward, unionoutRR);
  }

  /**
   * Generates the sampling predicate from the TABLESAMPLE clause information.
   * This function uses the bucket column list to decide the expression inputs
   * to the predicate hash function in case useBucketCols is set to true,
   * otherwise the expression list stored in the TableSample is used. The bucket
   * columns of the table are used to generate this predicate in case no
   * expressions are provided on the TABLESAMPLE clause and the table has
   * clustering columns defined in it's metadata. The predicate created has the
   * following structure:
   *
   * ((hash(expressions) & Integer.MAX_VALUE) % denominator) == numerator
   *
   * @param ts
   *          TABLESAMPLE clause information
   * @param bucketCols
   *          The clustering columns of the table
   * @param useBucketCols
   *          Flag to indicate whether the bucketCols should be used as input to
   *          the hash function
   * @param alias
   *          The alias used for the table in the row resolver
   * @param rwsch
   *          The row resolver used to resolve column references
   * @param qbm
   *          The metadata information for the query block which is used to
   *          resolve unaliased columns
   * @param planExpr
   *          The plan tree for the expression. If the user specified this, the
   *          parse expressions are not used
   * @return exprNodeDesc
   * @exception SemanticException
   */
  private ExprNodeDesc genSamplePredicate(TableSample ts,
      List<String> bucketCols, boolean useBucketCols, String alias,
      RowResolver rwsch, QBMetaData qbm, ExprNodeDesc planExpr)
      throws SemanticException {

    ExprNodeDesc numeratorExpr = new ExprNodeConstantDesc(
        TypeInfoFactory.intTypeInfo, Integer.valueOf(ts.getNumerator() - 1));

    ExprNodeDesc denominatorExpr = new ExprNodeConstantDesc(
        TypeInfoFactory.intTypeInfo, Integer.valueOf(ts.getDenominator()));

    ExprNodeDesc intMaxExpr = new ExprNodeConstantDesc(
        TypeInfoFactory.intTypeInfo, Integer.valueOf(Integer.MAX_VALUE));

    ArrayList<ExprNodeDesc> args = new ArrayList<ExprNodeDesc>();
    if (planExpr != null) {
      args.add(planExpr);
    } else if (useBucketCols) {
      for (String col : bucketCols) {
        ColumnInfo ci = rwsch.get(alias, col);
        // TODO: change type to the one in the table schema
        args.add(new ExprNodeColumnDesc(ci.getType(), ci.getInternalName(), ci
            .getTabAlias(), ci.getIsPartitionCol()));
      }
    } else {
      for (ASTNode expr : ts.getExprs()) {
        args.add(genExprNodeDesc(expr, rwsch));
      }
    }

    ExprNodeDesc equalsExpr = null;
    {
      ExprNodeDesc hashfnExpr = new ExprNodeGenericFuncDesc(
          TypeInfoFactory.intTypeInfo, new GenericUDFHash(), args);
      assert (hashfnExpr != null);
      LOG.info("hashfnExpr = " + hashfnExpr);
      ExprNodeDesc andExpr = TypeCheckProcFactory.DefaultExprProcessor
          .getFuncExprNodeDesc("&", hashfnExpr, intMaxExpr);
      assert (andExpr != null);
      LOG.info("andExpr = " + andExpr);
      ExprNodeDesc modExpr = TypeCheckProcFactory.DefaultExprProcessor
          .getFuncExprNodeDesc("%", andExpr, denominatorExpr);
      assert (modExpr != null);
      LOG.info("modExpr = " + modExpr);
      LOG.info("numeratorExpr = " + numeratorExpr);
      equalsExpr = TypeCheckProcFactory.DefaultExprProcessor
          .getFuncExprNodeDesc("==", modExpr, numeratorExpr);
      LOG.info("equalsExpr = " + equalsExpr);
      assert (equalsExpr != null);
    }
    return equalsExpr;
  }

  @SuppressWarnings("nls")
  private Operator genTablePlan(String alias, QB qb) throws SemanticException {

    String alias_id = (qb.getId() == null ? alias : qb.getId() + ":" + alias);
    Table tab = qb.getMetaData().getSrcForAlias(alias);
    RowResolver rwsch;

    // is the table already present
    Operator<? extends Serializable> top = topOps.get(alias_id);
    Operator<? extends Serializable> dummySel = topSelOps.get(alias_id);
    if (dummySel != null) {
      top = dummySel;
    }

    if (top == null) {
      rwsch = new RowResolver();
      try {
        StructObjectInspector rowObjectInspector = (StructObjectInspector) tab
            .getDeserializer().getObjectInspector();
        List<? extends StructField> fields = rowObjectInspector
            .getAllStructFieldRefs();
        for (int i = 0; i < fields.size(); i++) {
          rwsch.put(alias, fields.get(i).getFieldName(), new ColumnInfo(fields
              .get(i).getFieldName(), TypeInfoUtils
              .getTypeInfoFromObjectInspector(fields.get(i)
              .getFieldObjectInspector()), alias, false));
        }
      } catch (SerDeException e) {
        throw new RuntimeException(e);
      }
      // Hack!! - refactor once the metadata APIs with types are ready
      // Finally add the partitioning columns
      for (FieldSchema part_col : tab.getPartCols()) {
        LOG.trace("Adding partition col: " + part_col);
        // TODO: use the right type by calling part_col.getType() instead of
        // String.class
        rwsch.put(alias, part_col.getName(), new ColumnInfo(part_col.getName(),
            TypeInfoFactory.stringTypeInfo, alias, true));
      }

      // Create the root of the operator tree
      top = putOpInsertMap(OperatorFactory.get(new TableScanDesc(alias),
          new RowSchema(rwsch.getColumnInfos())), rwsch);

      // Add this to the list of top operators - we always start from a table
      // scan
      topOps.put(alias_id, top);

      // Add a mapping from the table scan operator to Table
      topToTable.put((TableScanOperator) top, tab);
    } else {
      rwsch = opParseCtx.get(top).getRR();
      top.setChildOperators(null);
    }

    // check if this table is sampled and needs more than input pruning
    Operator<? extends Serializable> tableOp = top;
    TableSample ts = qb.getParseInfo().getTabSample(alias);
    if (ts != null) {
      int num = ts.getNumerator();
      int den = ts.getDenominator();
      ArrayList<ASTNode> sampleExprs = ts.getExprs();

      // TODO: Do the type checking of the expressions
      List<String> tabBucketCols = tab.getBucketCols();
      int numBuckets = tab.getNumBuckets();

      // If there are no sample cols and no bucket cols then throw an error
      if (tabBucketCols.size() == 0 && sampleExprs.size() == 0) {
        throw new SemanticException(ErrorMsg.NON_BUCKETED_TABLE.getMsg() + " "
            + tab.getTableName());
      }

      if (num > den) {
        throw new SemanticException(
            ErrorMsg.BUCKETED_NUMBERATOR_BIGGER_DENOMINATOR.getMsg() + " "
            + tab.getTableName());
      }

      // check if a predicate is needed
      // predicate is needed if either input pruning is not enough
      // or if input pruning is not possible

      // check if the sample columns are the same as the table bucket columns
      boolean colsEqual = true;
      if ((sampleExprs.size() != tabBucketCols.size())
          && (sampleExprs.size() != 0)) {
        colsEqual = false;
      }

      for (int i = 0; i < sampleExprs.size() && colsEqual; i++) {
        boolean colFound = false;
        for (int j = 0; j < tabBucketCols.size() && !colFound; j++) {
          if (sampleExprs.get(i).getToken().getType() != HiveParser.TOK_TABLE_OR_COL) {
            break;
          }

          if (((ASTNode) sampleExprs.get(i).getChild(0)).getText()
              .equalsIgnoreCase(tabBucketCols.get(j))) {
            colFound = true;
          }
        }
        colsEqual = (colsEqual && colFound);
      }

      // Check if input can be pruned
      ts
          .setInputPruning((sampleExprs == null || sampleExprs.size() == 0 || colsEqual));

      // check if input pruning is enough
      if ((sampleExprs == null || sampleExprs.size() == 0 || colsEqual)
          && (num == den || (den % numBuckets == 0 || numBuckets % den == 0))) {

        // input pruning is enough; add the filter for the optimizer to use it
        // later
        LOG.info("No need for sample filter");
        ExprNodeDesc samplePredicate = genSamplePredicate(ts, tabBucketCols,
            colsEqual, alias, rwsch, qb.getMetaData(), null);
        tableOp = OperatorFactory.getAndMakeChild(new FilterDesc(
            samplePredicate, true, new sampleDesc(ts.getNumerator(), ts
            .getDenominator(), tabBucketCols, true)),
            new RowSchema(rwsch.getColumnInfos()), top);
      } else {
        // need to add filter
        // create tableOp to be filterDesc and set as child to 'top'
        LOG.info("Need sample filter");
        ExprNodeDesc samplePredicate = genSamplePredicate(ts, tabBucketCols,
            colsEqual, alias, rwsch, qb.getMetaData(), null);
        tableOp = OperatorFactory.getAndMakeChild(new FilterDesc(
            samplePredicate, true),
            new RowSchema(rwsch.getColumnInfos()), top);
      }
    } else {
      boolean testMode = conf.getBoolVar(HiveConf.ConfVars.HIVETESTMODE);
      if (testMode) {
        String tabName = tab.getTableName();

        // has the user explicitly asked not to sample this table
        String unSampleTblList = conf
            .getVar(HiveConf.ConfVars.HIVETESTMODENOSAMPLE);
        String[] unSampleTbls = unSampleTblList.split(",");
        boolean unsample = false;
        for (String unSampleTbl : unSampleTbls) {
          if (tabName.equalsIgnoreCase(unSampleTbl)) {
            unsample = true;
          }
        }

        if (!unsample) {
          int numBuckets = tab.getNumBuckets();

          // If the input table is bucketed, choose the first bucket
          if (numBuckets > 0) {
            TableSample tsSample = new TableSample(1, numBuckets);
            tsSample.setInputPruning(true);
            qb.getParseInfo().setTabSample(alias, tsSample);
            ExprNodeDesc samplePred = genSamplePredicate(tsSample, tab
                .getBucketCols(), true, alias, rwsch, qb.getMetaData(), null);
            tableOp = OperatorFactory
                .getAndMakeChild(new FilterDesc(samplePred, true,
                new sampleDesc(tsSample.getNumerator(), tsSample
                .getDenominator(), tab.getBucketCols(), true)),
                new RowSchema(rwsch.getColumnInfos()), top);
            LOG.info("No need for sample filter");
          } else {
            // The table is not bucketed, add a dummy filter :: rand()
            int freq = conf.getIntVar(HiveConf.ConfVars.HIVETESTMODESAMPLEFREQ);
            TableSample tsSample = new TableSample(1, freq);
            tsSample.setInputPruning(false);
            qb.getParseInfo().setTabSample(alias, tsSample);
            LOG.info("Need sample filter");
            ExprNodeDesc randFunc = TypeCheckProcFactory.DefaultExprProcessor
                .getFuncExprNodeDesc("rand", new ExprNodeConstantDesc(Integer
                .valueOf(460476415)));
            ExprNodeDesc samplePred = genSamplePredicate(tsSample, null, false,
                alias, rwsch, qb.getMetaData(), randFunc);
            tableOp = OperatorFactory.getAndMakeChild(new FilterDesc(
                samplePred, true),
                new RowSchema(rwsch.getColumnInfos()), top);
          }
        }
      }
    }

    Operator output = putOpInsertMap(tableOp, rwsch);
    LOG.debug("Created Table Plan for " + alias + " " + tableOp.toString());

    return output;
  }

  private Operator genPlan(QBExpr qbexpr) throws SemanticException {
    if (qbexpr.getOpcode() == QBExpr.Opcode.NULLOP) {
      return genPlan(qbexpr.getQB());
    }
    if (qbexpr.getOpcode() == QBExpr.Opcode.UNION) {
      Operator qbexpr1Ops = genPlan(qbexpr.getQBExpr1());
      Operator qbexpr2Ops = genPlan(qbexpr.getQBExpr2());

      return genUnionPlan(qbexpr.getAlias(), qbexpr.getQBExpr1().getAlias(),
          qbexpr1Ops, qbexpr.getQBExpr2().getAlias(), qbexpr2Ops);
    }
    return null;
  }

  @SuppressWarnings("nls")
  public Operator genPlan(QB qb) throws SemanticException {

    // First generate all the opInfos for the elements in the from clause
    HashMap<String, Operator> aliasToOpInfo = new HashMap<String, Operator>();

    // Recurse over the subqueries to fill the subquery part of the plan
    for (String alias : qb.getSubqAliases()) {
      QBExpr qbexpr = qb.getSubqForAlias(alias);
      aliasToOpInfo.put(alias, genPlan(qbexpr));
      qbexpr.setAlias(alias);
    }

    // Recurse over all the source tables
    for (String alias : qb.getTabAliases()) {
      aliasToOpInfo.put(alias, genTablePlan(alias, qb));
    }

    // For all the source tables that have a lateral view, attach the
    // appropriate operators to the TS
    genLateralViewPlans(aliasToOpInfo, qb);

    Operator srcOpInfo = null;

    // process join
    if (qb.getParseInfo().getJoinExpr() != null) {
      ASTNode joinExpr = qb.getParseInfo().getJoinExpr();

      if (joinExpr.getToken().getType() == HiveParser.TOK_UNIQUEJOIN) {
        QBJoinTree joinTree = genUniqueJoinTree(qb, joinExpr);
        qb.setQbJoinTree(joinTree);
      } else {
        QBJoinTree joinTree = genJoinTree(qb, joinExpr);
        qb.setQbJoinTree(joinTree);
        mergeJoinTree(qb);
      }

      // if any filters are present in the join tree, push them on top of the
      // table
      pushJoinFilters(qb, qb.getQbJoinTree(), aliasToOpInfo);
      srcOpInfo = genJoinPlan(qb, aliasToOpInfo);
    } else {
      // Now if there are more than 1 sources then we have a join case
      // later we can extend this to the union all case as well
      srcOpInfo = aliasToOpInfo.values().iterator().next();
    }

    Operator bodyOpInfo = genBodyPlan(qb, srcOpInfo);
    LOG.debug("Created Plan for Query Block " + qb.getId());

    this.qb = qb;
    return bodyOpInfo;
  }

  /**
   * Generates the operator DAG needed to implement lateral views and attaches
   * it to the TS operator.
   *
   * @param aliasToOpInfo
   *          A mapping from a table alias to the TS operator. This function
   *          replaces the operator mapping as necessary
   * @param qb
   * @throws SemanticException
   */

  void genLateralViewPlans(HashMap<String, Operator> aliasToOpInfo, QB qb)
      throws SemanticException {
    Map<String, ArrayList<ASTNode>> aliasToLateralViews = qb.getParseInfo()
        .getAliasToLateralViews();
    for (Entry<String, Operator> e : aliasToOpInfo.entrySet()) {
      String alias = e.getKey();
      // See if the alias has a lateral view. If so, chain the lateral view
      // operator on
      ArrayList<ASTNode> lateralViews = aliasToLateralViews.get(alias);
      if (lateralViews != null) {
        Operator op = e.getValue();

        for (ASTNode lateralViewTree : aliasToLateralViews.get(alias)) {
          // There are 2 paths from the TS operator (or a previous LVJ operator)
          // to the same LateralViewJoinOperator.
          // TS -> SelectOperator(*) -> LateralViewJoinOperator
          // TS -> SelectOperator (gets cols for UDTF) -> UDTFOperator0
          // -> LateralViewJoinOperator

          // The order in which the two paths are added is important. The
          // lateral view join operator depends on having the select operator
          // give it the row first.

          // Get the all path by making a select(*)
          RowResolver allPathRR = opParseCtx.get(op).getRR();
          Operator allPath = putOpInsertMap(OperatorFactory.getAndMakeChild(
              new SelectDesc(true), new RowSchema(allPathRR.getColumnInfos()),
              op), allPathRR);

          // Get the UDTF Path
          QB blankQb = new QB(null, null, false);
          Operator udtfPath = genSelectPlan((ASTNode) lateralViewTree
              .getChild(0), blankQb, op);
          RowResolver udtfPathRR = opParseCtx.get(udtfPath).getRR();

          // Merge the two into the lateral view join
          // The cols of the merged result will be the combination of both the
          // cols of the UDTF path and the cols of the all path. The internal
          // names have to be changed to avoid conflicts

          RowResolver lateralViewRR = new RowResolver();
          ArrayList<String> outputInternalColNames = new ArrayList<String>();

          LVmergeRowResolvers(allPathRR, lateralViewRR, outputInternalColNames);
          LVmergeRowResolvers(udtfPathRR, lateralViewRR, outputInternalColNames);

          Operator lateralViewJoin = putOpInsertMap(OperatorFactory
              .getAndMakeChild(new LateralViewJoinDesc(outputInternalColNames),
              new RowSchema(lateralViewRR.getColumnInfos()), allPath,
              udtfPath), lateralViewRR);
          op = lateralViewJoin;
        }
        e.setValue(op);
      }
    }
  }

  /**
   * A helper function that gets all the columns and respective aliases in the
   * source and puts them into dest. It renames the internal names of the
   * columns based on getColumnInternalName(position).
   *
   * Note that this helper method relies on RowResolver.getColumnInfos()
   * returning the columns in the same order as they will be passed in the
   * operator DAG.
   *
   * @param source
   * @param dest
   * @param outputColNames
   *          - a list to which the new internal column names will be added, in
   *          the same order as in the dest row resolver
   */
  private void LVmergeRowResolvers(RowResolver source, RowResolver dest,
      ArrayList<String> outputInternalColNames) {
    for (ColumnInfo c : source.getColumnInfos()) {
      String internalName = getColumnInternalName(outputInternalColNames.size());
      outputInternalColNames.add(internalName);
      ColumnInfo newCol = new ColumnInfo(internalName, c.getType(), c
          .getTabAlias(), c.getIsPartitionCol());
      String[] tableCol = source.reverseLookup(c.getInternalName());
      String tableAlias = tableCol[0];
      String colAlias = tableCol[1];
      dest.put(tableAlias, colAlias, newCol);
    }
  }

  @SuppressWarnings("nls")
  private void genMapRedTasks(QB qb) throws SemanticException {
    FetchWork fetch = null;
    List<Task<? extends Serializable>> mvTask = new ArrayList<Task<? extends Serializable>>();
    FetchTask fetchTask = null;

    QBParseInfo qbParseInfo = qb.getParseInfo();

    // Does this query need reduce job
    if (qb.isSelectStarQuery() && qbParseInfo.getDestToClusterBy().isEmpty()
        && qbParseInfo.getDestToDistributeBy().isEmpty()
        && qbParseInfo.getDestToOrderBy().isEmpty()
        && qbParseInfo.getDestToSortBy().isEmpty()) {
      boolean noMapRed = false;

      Iterator<Map.Entry<String, Table>> iter = qb.getMetaData()
          .getAliasToTable().entrySet().iterator();
      Table tab = (iter.next()).getValue();
      if (!tab.isPartitioned()) {
        if (qbParseInfo.getDestToWhereExpr().isEmpty()) {
          fetch = new FetchWork(tab.getPath().toString(), Utilities
              .getTableDesc(tab), qb.getParseInfo().getOuterQueryLimit());
          noMapRed = true;
          inputs.add(new ReadEntity(tab));
        }
      } else {

        if (topOps.size() == 1) {
          TableScanOperator ts = (TableScanOperator) topOps.values().toArray()[0];

          // check if the pruner only contains partition columns
          if (PartitionPruner.onlyContainsPartnCols(topToTable.get(ts),
              opToPartPruner.get(ts))) {

            PrunedPartitionList partsList = null;
            try {
              partsList = PartitionPruner.prune(topToTable.get(ts),
                  opToPartPruner.get(ts), conf, (String) topOps.keySet()
                  .toArray()[0], prunedPartitions);
            } catch (HiveException e) {
              // Has to use full name to make sure it does not conflict with
              // org.apache.commons.lang.StringUtils
              LOG.error(stringifyException(e));
              throw new SemanticException(e.getMessage(), e);
            }

            // If there is any unknown partition, create a map-reduce job for
            // the filter to prune correctly
            if ((partsList.getUnknownPartns().size() == 0)) {
              List<String> listP = new ArrayList<String>();
              List<PartitionDesc> partP = new ArrayList<PartitionDesc>();

              Set<Partition> parts = partsList.getConfirmedPartns();
              Iterator<Partition> iterParts = parts.iterator();
              while (iterParts.hasNext()) {
                Partition part = iterParts.next();
                listP.add(part.getPartitionPath().toString());
                try {
                  partP.add(Utilities.getPartitionDesc(part));
                } catch (HiveException e) {
                  throw new SemanticException(e.getMessage(), e);
                }
                inputs.add(new ReadEntity(part));
              }

              fetch = new FetchWork(listP, partP, qb.getParseInfo()
                  .getOuterQueryLimit());
              noMapRed = true;
            }
          }
        }
      }

      if (noMapRed) {
        if (fetch.getTblDesc() != null) {
          PlanUtils.configureTableJobPropertiesForStorageHandler(
            fetch.getTblDesc());
        }
        fetchTask = (FetchTask) TaskFactory.get(fetch, conf);
        setFetchTask(fetchTask);

        // remove root tasks if any
        rootTasks.clear();
        return;
      }
    }

    // In case of a select, use a fetch task instead of a move task
    if (qb.getIsQuery()) {
      if ((!loadTableWork.isEmpty()) || (loadFileWork.size() != 1)) {
        throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg());
      }
      String cols = loadFileWork.get(0).getColumns();
      String colTypes = loadFileWork.get(0).getColumnTypes();

      fetch = new FetchWork(new Path(loadFileWork.get(0).getSourceDir()).toString(),
          new TableDesc(LazySimpleSerDe.class,
          TextInputFormat.class, IgnoreKeyTextOutputFormat.class, Utilities
          .makeProperties(SERIALIZATION_FORMAT, "" + Utilities.ctrlaCode,
          LIST_COLUMNS, cols,
          LIST_COLUMN_TYPES, colTypes)),
          qb.getParseInfo().getOuterQueryLimit());

      fetchTask = (FetchTask) TaskFactory.get(fetch, conf);
      setFetchTask(fetchTask);
    } else {
      new ArrayList<MoveWork>();
      for (LoadTableDesc ltd : loadTableWork) {
        Task<MoveWork> tsk = TaskFactory.get(new MoveWork(null, null, ltd, null, false),
            conf);
        mvTask.add(tsk);
      }

      boolean oneLoadFile = true;
      for (LoadFileDesc lfd : loadFileWork) {
        if (qb.isCTAS()) {
          assert (oneLoadFile); // should not have more than 1 load file for
          // CTAS
          // make the movetask's destination directory the table's destination.
          String location = qb.getTableDesc().getLocation();
          if (location == null) {
            // get the table's default location
            location = conf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
            assert (location.length() > 0);
            if (location.charAt(location.length() - 1) != '/') {
              location += '/';
            }
            location += qb.getTableDesc().getTableName().toLowerCase();
          }
          lfd.setTargetDir(location);
          oneLoadFile = false;
        }
        mvTask.add(TaskFactory.get(new MoveWork(null, null, null, lfd, false),
            conf));
      }
    }

    // generate map reduce plans
    GenMRProcContext procCtx = new GenMRProcContext(
        conf,
        new HashMap<Operator<? extends Serializable>, Task<? extends Serializable>>(),
        new ArrayList<Operator<? extends Serializable>>(), getParseContext(),
        mvTask, rootTasks,
        new LinkedHashMap<Operator<? extends Serializable>, GenMapRedCtx>(),
        inputs, outputs);

    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack.
    // The dispatcher generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp(new String("R1"), "TS%"), new GenMRTableScan1());
    opRules.put(new RuleRegExp(new String("R2"), "TS%.*RS%"),
        new GenMRRedSink1());
    opRules.put(new RuleRegExp(new String("R3"), "RS%.*RS%"),
        new GenMRRedSink2());
    opRules.put(new RuleRegExp(new String("R4"), "FS%"), new GenMRFileSink1());
    opRules.put(new RuleRegExp(new String("R5"), "UNION%"), new GenMRUnion1());
    opRules.put(new RuleRegExp(new String("R6"), "UNION%.*RS%"),
        new GenMRRedSink3());
    opRules.put(new RuleRegExp(new String("R6"), "MAPJOIN%.*RS%"),
        new GenMRRedSink4());
    opRules.put(new RuleRegExp(new String("R7"), "TS%.*MAPJOIN%"),
        MapJoinFactory.getTableScanMapJoin());
    opRules.put(new RuleRegExp(new String("R8"), "RS%.*MAPJOIN%"),
        MapJoinFactory.getReduceSinkMapJoin());
    opRules.put(new RuleRegExp(new String("R9"), "UNION%.*MAPJOIN%"),
        MapJoinFactory.getUnionMapJoin());
    opRules.put(new RuleRegExp(new String("R10"), "MAPJOIN%.*MAPJOIN%"),
        MapJoinFactory.getMapJoinMapJoin());
    opRules.put(new RuleRegExp(new String("R11"), "MAPJOIN%SEL%"),
        MapJoinFactory.getMapJoin());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(new GenMROperator(), opRules,
        procCtx);

    GraphWalker ogw = new GenMapRedWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(topOps.values());
    ogw.startWalking(topNodes, null);

    // reduce sink does not have any kids - since the plan by now has been
    // broken up into multiple
    // tasks, iterate over all tasks.
    // For each task, go over all operators recursively
    for (Task<? extends Serializable> rootTask : rootTasks) {
      breakTaskTree(rootTask);
    }

    // For each task, set the key descriptor for the reducer
    for (Task<? extends Serializable> rootTask : rootTasks) {
      setKeyDescTaskTree(rootTask);
    }

    PhysicalContext physicalContext = new PhysicalContext(conf,
        getParseContext(), ctx, rootTasks, fetchTask);
    PhysicalOptimizer physicalOptimizer = new PhysicalOptimizer(
        physicalContext, conf);
    physicalOptimizer.optimize();

    // For each operator, generate the counters if needed
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEJOBPROGRESS)) {
      for (Task<? extends Serializable> rootTask : rootTasks) {
        generateCountersTask(rootTask);
      }
    }

    if (qb.isCTAS()) {
      // generate a DDL task and make it a dependent task of the leaf
      CreateTableDesc crtTblDesc = qb.getTableDesc();

      validateCreateTable(crtTblDesc);

      // Clear the output for CTAS since we don't need the output from the
      // mapredWork, the
      // DDLWork at the tail of the chain will have the output
      getOutputs().clear();

      Task<? extends Serializable> crtTblTask = TaskFactory.get(new DDLWork(
          getInputs(), getOutputs(), crtTblDesc), conf);

      // find all leaf tasks and make the DDLTask as a dependent task of all of
      // them
      HashSet<Task<? extends Serializable>> leaves = new HashSet<Task<? extends Serializable>>();
      getLeafTasks(rootTasks, leaves);
      assert (leaves.size() > 0);
      for (Task<? extends Serializable> task : leaves) {
        task.addDependentTask(crtTblTask);
      }
    }
  }

  /**
   * Find all leaf tasks of the list of root tasks.
   */
  private void getLeafTasks(List<Task<? extends Serializable>> rootTasks,
      HashSet<Task<? extends Serializable>> leaves) {

    for (Task<? extends Serializable> root : rootTasks) {
      getLeafTasks(root, leaves);
    }
  }

  private void getLeafTasks(Task<? extends Serializable> task,
      HashSet<Task<? extends Serializable>> leaves) {
    if (task.getChildTasks() == null) {
      if (!leaves.contains(task)) {
        leaves.add(task);
      }
    } else {
      getLeafTasks(task.getChildTasks(), leaves);
    }
  }

  // loop over all the tasks recursviely
  private void generateCountersTask(Task<? extends Serializable> task) {
    if ((task instanceof MapRedTask) || (task instanceof ExecDriver)) {
      HashMap<String, Operator<? extends Serializable>> opMap = ((MapredWork) task
          .getWork()).getAliasToWork();
      if (!opMap.isEmpty()) {
        for (Operator<? extends Serializable> op : opMap.values()) {
          generateCountersOperator(op);
        }
      }

      Operator<? extends Serializable> reducer = ((MapredWork) task.getWork())
          .getReducer();
      if (reducer != null) {
        LOG.info("Generating counters for operator " + reducer);
        generateCountersOperator(reducer);
      }
    } else if (task instanceof ConditionalTask) {
      List<Task<? extends Serializable>> listTasks = ((ConditionalTask) task)
          .getListTasks();
      for (Task<? extends Serializable> tsk : listTasks) {
        generateCountersTask(tsk);
      }
    }

    // Start the counters from scratch - a hack for hadoop 17.
    Operator.resetLastEnumUsed();

    if (task.getChildTasks() == null) {
      return;
    }

    for (Task<? extends Serializable> childTask : task.getChildTasks()) {
      generateCountersTask(childTask);
    }
  }

  private void generateCountersOperator(Operator<? extends Serializable> op) {
    op.assignCounterNameToEnum();

    if (op.getChildOperators() == null) {
      return;
    }

    for (Operator<? extends Serializable> child : op.getChildOperators()) {
      generateCountersOperator(child);
    }
  }

  // loop over all the tasks recursviely
  private void breakTaskTree(Task<? extends Serializable> task) {

    if ((task instanceof MapRedTask) || (task instanceof ExecDriver)) {
      HashMap<String, Operator<? extends Serializable>> opMap = ((MapredWork) task
          .getWork()).getAliasToWork();
      if (!opMap.isEmpty()) {
        for (Operator<? extends Serializable> op : opMap.values()) {
          breakOperatorTree(op);
        }
      }
    } else if (task instanceof ConditionalTask) {
      List<Task<? extends Serializable>> listTasks = ((ConditionalTask) task)
          .getListTasks();
      for (Task<? extends Serializable> tsk : listTasks) {
        breakTaskTree(tsk);
      }
    }

    if (task.getChildTasks() == null) {
      return;
    }

    for (Task<? extends Serializable> childTask : task.getChildTasks()) {
      breakTaskTree(childTask);
    }
  }

  // loop over all the operators recursviely
  private void breakOperatorTree(Operator<? extends Serializable> topOp) {
    if (topOp instanceof ReduceSinkOperator) {
      topOp.setChildOperators(null);
    }

    if (topOp.getChildOperators() == null) {
      return;
    }

    for (Operator<? extends Serializable> op : topOp.getChildOperators()) {
      breakOperatorTree(op);
    }
  }

  // loop over all the tasks recursviely
  private void setKeyDescTaskTree(Task<? extends Serializable> task) {

    if ((task instanceof MapRedTask) || (task instanceof ExecDriver)) {
      MapredWork work = (MapredWork) task.getWork();
      work.deriveExplainAttributes();
      HashMap<String, Operator<? extends Serializable>> opMap = work
          .getAliasToWork();
      if (!opMap.isEmpty()) {
        for (Operator<? extends Serializable> op : opMap.values()) {
          GenMapRedUtils.setKeyAndValueDesc(work, op);
        }
      }
    } else if (task instanceof ConditionalTask) {
      List<Task<? extends Serializable>> listTasks = ((ConditionalTask) task)
          .getListTasks();
      for (Task<? extends Serializable> tsk : listTasks) {
        setKeyDescTaskTree(tsk);
      }
    }

    if (task.getChildTasks() == null) {
      return;
    }

    for (Task<? extends Serializable> childTask : task.getChildTasks()) {
      setKeyDescTaskTree(childTask);
    }
  }

  @SuppressWarnings("nls")
  public Phase1Ctx initPhase1Ctx() {

    Phase1Ctx ctx_1 = new Phase1Ctx();
    ctx_1.nextNum = 0;
    ctx_1.dest = "reduce";

    return ctx_1;
  }

  @Override
  @SuppressWarnings("nls")
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    reset();

    QB qb = new QB(null, null, false);
    this.qb = qb;
    this.ast = ast;
    ASTNode child = ast;

    LOG.info("Starting Semantic Analysis");

    // analyze create table command
    if (ast.getToken().getType() == HiveParser.TOK_CREATETABLE) {
      // if it is not CTAS, we don't need to go further and just return
      if ((child = analyzeCreateTable(ast, qb)) == null) {
        return;
      }
    }

    // analyze create view command
    if (ast.getToken().getType() == HiveParser.TOK_CREATEVIEW) {
      child = analyzeCreateView(ast, qb);
      if (child == null) {
        return;
      }
      viewSelect = child;
    }

    // continue analyzing from the child ASTNode.
    doPhase1(child, qb, initPhase1Ctx());
    LOG.info("Completed phase 1 of Semantic Analysis");

    getMetaData(qb);
    LOG.info("Completed getting MetaData in Semantic Analysis");

    // Save the result schema derived from the sink operator produced
    // by genPlan.  This has the correct column names, which clients
    // such as JDBC would prefer instead of the c0, c1 we'll end
    // up with later.
    Operator sinkOp = genPlan(qb);
    resultSchema =
        convertRowSchemaToViewSchema(opParseCtx.get(sinkOp).getRR());

    if (createVwDesc != null) {
      saveViewDefinition();
      // Since we're only creating a view (not executing it), we
      // don't need to optimize or translate the plan (and in fact, those
      // procedures can interfere with the view creation). So
      // skip the rest of this method.
      ctx.setResDir(null);
      ctx.setResFile(null);
      return;
    }

    ParseContext pCtx = new ParseContext(conf, qb, child, opToPartPruner,
        topOps, topSelOps, opParseCtx, joinContext, topToTable,
        loadTableWork, loadFileWork, ctx, idToTableNameMap, destTableId, uCtx,
        listMapJoinOpsNoReducer, groupOpToInputTables, prunedPartitions,
        opToSamplePruner);

    Optimizer optm = new Optimizer();
    optm.setPctx(pCtx);
    optm.initialize(conf);
    pCtx = optm.optimize();
    init(pCtx);
    qb = pCtx.getQB();

    // At this point we have the complete operator tree
    // from which we want to find the reduce operator
    genMapRedTasks(qb);

    LOG.info("Completed plan generation");

    return;
  }

  @Override
  public List<FieldSchema> getResultSchema() {
    return resultSchema;
  }

  private void saveViewDefinition() throws SemanticException {

    // Make a copy of the statement's result schema, since we may
    // modify it below as part of imposing view column names.
    List<FieldSchema> derivedSchema =
        new ArrayList<FieldSchema>(resultSchema);
    validateColumnNameUniqueness(derivedSchema);

    List<FieldSchema> imposedSchema = createVwDesc.getSchema();
    if (imposedSchema != null) {
      int explicitColCount = imposedSchema.size();
      int derivedColCount = derivedSchema.size();
      if (explicitColCount != derivedColCount) {
        throw new SemanticException(ErrorMsg.VIEW_COL_MISMATCH
            .getMsg(viewSelect));
      }
    }

    // Preserve the original view definition as specified by the user.
    String originalText = ctx.getTokenRewriteStream().toString(
        viewSelect.getTokenStartIndex(), viewSelect.getTokenStopIndex());
    createVwDesc.setViewOriginalText(originalText);

    // Now expand the view definition with extras such as explicit column
    // references; this expanded form is what we'll re-parse when the view is
    // referenced later.
    unparseTranslator.applyTranslations(ctx.getTokenRewriteStream());
    String expandedText = ctx.getTokenRewriteStream().toString(
        viewSelect.getTokenStartIndex(), viewSelect.getTokenStopIndex());

    if (imposedSchema != null) {
      // Merge the names from the imposed schema into the types
      // from the derived schema.
      StringBuilder sb = new StringBuilder();
      sb.append("SELECT ");
      int n = derivedSchema.size();
      for (int i = 0; i < n; ++i) {
        if (i > 0) {
          sb.append(", ");
        }
        FieldSchema fieldSchema = derivedSchema.get(i);
        // Modify a copy, not the original
        fieldSchema = new FieldSchema(fieldSchema);
        derivedSchema.set(i, fieldSchema);
        sb.append(HiveUtils.unparseIdentifier(fieldSchema.getName()));
        sb.append(" AS ");
        String imposedName = imposedSchema.get(i).getName();
        sb.append(HiveUtils.unparseIdentifier(imposedName));
        fieldSchema.setName(imposedName);
        // We don't currently allow imposition of a type
        fieldSchema.setComment(imposedSchema.get(i).getComment());
      }
      sb.append(" FROM (");
      sb.append(expandedText);
      sb.append(") ");
      sb.append(HiveUtils.unparseIdentifier(createVwDesc.getViewName()));
      expandedText = sb.toString();
    }

    createVwDesc.setSchema(derivedSchema);
    createVwDesc.setViewExpandedText(expandedText);
  }

  private List<FieldSchema> convertRowSchemaToViewSchema(RowResolver rr) {
    List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
    for (ColumnInfo colInfo : rr.getColumnInfos()) {
      String colName = rr.reverseLookup(colInfo.getInternalName())[1];
      fieldSchemas.add(new FieldSchema(colName,
          colInfo.getType().getTypeName(), null));
    }
    return fieldSchemas;
  }

  /**
   * Generates an expression node descriptor for the expression passed in the
   * arguments. This function uses the row resolver and the metadata information
   * that are passed as arguments to resolve the column names to internal names.
   *
   * @param expr
   *          The expression
   * @param input
   *          The row resolver
   * @return exprNodeDesc
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  public ExprNodeDesc genExprNodeDesc(ASTNode expr, RowResolver input)
      throws SemanticException {
    // We recursively create the exprNodeDesc. Base cases: when we encounter
    // a column ref, we convert that into an exprNodeColumnDesc; when we
    // encounter
    // a constant, we convert that into an exprNodeConstantDesc. For others we
    // just
    // build the exprNodeFuncDesc with recursively built children.

    // If the current subExpression is pre-calculated, as in Group-By etc.
    ColumnInfo colInfo = input.getExpression(expr);
    if (colInfo != null) {
      ASTNode source = input.getExpressionSource(expr);
      if (source != null) {
        unparseTranslator.addCopyTranslation(expr, source);
      }
      return new ExprNodeColumnDesc(colInfo.getType(), colInfo
          .getInternalName(), colInfo.getTabAlias(), colInfo
          .getIsPartitionCol());
    }

    // Create the walker, the rules dispatcher and the context.
    TypeCheckCtx tcCtx = new TypeCheckCtx(input);
    tcCtx.setUnparseTranslator(unparseTranslator);

    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack. The dispatcher
    // generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    opRules.put(new RuleRegExp("R1", HiveParser.TOK_NULL + "%"),
        TypeCheckProcFactory.getNullExprProcessor());
    opRules.put(new RuleRegExp("R2", HiveParser.Number + "%"),
        TypeCheckProcFactory.getNumExprProcessor());
    opRules
        .put(new RuleRegExp("R3", HiveParser.Identifier + "%|"
        + HiveParser.StringLiteral + "%|" + HiveParser.TOK_CHARSETLITERAL
        + "%|" + HiveParser.KW_IF + "%|" + HiveParser.KW_CASE + "%|"
        + HiveParser.KW_WHEN + "%|" + HiveParser.KW_IN + "%|"
        + HiveParser.KW_ARRAY + "%|" + HiveParser.KW_MAP + "%|"
        + HiveParser.KW_STRUCT + "%"), TypeCheckProcFactory
        .getStrExprProcessor());
    opRules.put(new RuleRegExp("R4", HiveParser.KW_TRUE + "%|"
        + HiveParser.KW_FALSE + "%"), TypeCheckProcFactory
        .getBoolExprProcessor());
    opRules.put(new RuleRegExp("R5", HiveParser.TOK_TABLE_OR_COL + "%"),
        TypeCheckProcFactory.getColumnExprProcessor());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(TypeCheckProcFactory
        .getDefaultExprProcessor(), opRules, tcCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(expr);
    HashMap<Node, Object> nodeOutputs = new HashMap<Node, Object>();
    ogw.startWalking(topNodes, nodeOutputs);
    ExprNodeDesc desc = (ExprNodeDesc) nodeOutputs.get(expr);
    if (desc == null) {
      throw new SemanticException(tcCtx.getError());
    }

    if (!unparseTranslator.isEnabled()) {
      // Not creating a view, so no need to track view expansions.
      return desc;
    }

    for (Map.Entry<Node, Object> entry : nodeOutputs.entrySet()) {
      if (!(entry.getKey() instanceof ASTNode)) {
        continue;
      }
      if (!(entry.getValue() instanceof ExprNodeColumnDesc)) {
        continue;
      }
      ASTNode node = (ASTNode) entry.getKey();
      ExprNodeColumnDesc columnDesc = (ExprNodeColumnDesc) entry.getValue();
      if ((columnDesc.getTabAlias() == null)
          || (columnDesc.getTabAlias().length() == 0)) {
        // These aren't real column refs; instead, they are special
        // internal expressions used in the representation of aggregation.
        continue;
      }
      String[] tmp = input.reverseLookup(columnDesc.getColumn());
      StringBuilder replacementText = new StringBuilder();
      replacementText.append(HiveUtils.unparseIdentifier(tmp[0]));
      replacementText.append(".");
      replacementText.append(HiveUtils.unparseIdentifier(tmp[1]));
      unparseTranslator.addTranslation(node, replacementText.toString());
    }

    return desc;
  }

  /**
   * Gets the table Alias for the column from the column name. This function
   * throws and exception in case the same column name is present in multiple
   * table. The exception message indicates that the ambiguity could not be
   * resolved.
   *
   * @param qbm
   *          The metadata where the function looks for the table alias
   * @param colName
   *          The name of the non aliased column
   * @param pt
   *          The parse tree corresponding to the column(this is used for error
   *          reporting)
   * @return String
   * @throws SemanticException
   */
  static String getTabAliasForCol(QBMetaData qbm, String colName, ASTNode pt)
      throws SemanticException {
    String tabAlias = null;
    boolean found = false;

    for (Map.Entry<String, Table> ent : qbm.getAliasToTable().entrySet()) {
      for (FieldSchema field : ent.getValue().getAllCols()) {
        if (colName.equalsIgnoreCase(field.getName())) {
          if (found) {
            throw new SemanticException(ErrorMsg.AMBIGUOUS_COLUMN.getMsg(pt));
          }

          found = true;
          tabAlias = ent.getKey();
        }
      }
    }
    return tabAlias;
  }

  @Override
  public void validate() throws SemanticException {
    // Check if the plan contains atleast one path.

    // validate all tasks
    for (Task<? extends Serializable> rootTask : rootTasks) {
      validate(rootTask);
    }
  }

  private void validate(Task<? extends Serializable> task)
      throws SemanticException {
    if ((task instanceof MapRedTask) || (task instanceof ExecDriver)) {
      task.getWork();

      // If the plan does not contain any path, an empty file
      // will be added by ExecDriver at execute time
    }

    if (task.getChildTasks() == null) {
      return;
    }

    for (Task<? extends Serializable> childTask : task.getChildTasks()) {
      validate(childTask);
    }
  }

  /**
   * Get the row resolver given an operator.
   */
  public RowResolver getRowResolver(Operator opt) {
    return opParseCtx.get(opt).getRR();
  }

  /**
   * Analyze the create table command. If it is a regular create-table or
   * create-table-like statements, we create a DDLWork and return true. If it is
   * a create-table-as-select, we get the necessary info such as the SerDe and
   * Storage Format and put it in QB, and return false, indicating the rest of
   * the semantic analyzer need to deal with the select statement with respect
   * to the SerDe and Storage Format.
   */
  private ASTNode analyzeCreateTable(ASTNode ast, QB qb)
      throws SemanticException {
    String tableName = unescapeIdentifier(ast.getChild(0).getText());
    String likeTableName = null;
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> bucketCols = new ArrayList<String>();
    List<Order> sortCols = new ArrayList<Order>();
    int numBuckets = -1;
    String fieldDelim = null;
    String fieldEscape = null;
    String collItemDelim = null;
    String mapKeyDelim = null;
    String lineDelim = null;
    String comment = null;
    String inputFormat = null;
    String outputFormat = null;
    String location = null;
    String serde = null;
    String storageHandler = null;
    Map<String, String> serdeProps = new HashMap<String, String>();
    Map<String, String> tblProps = null;
    boolean ifNotExists = false;
    boolean isExt = false;
    ASTNode selectStmt = null;
    final int CREATE_TABLE = 0; // regular CREATE TABLE
    final int CTLT = 1; // CREATE TABLE LIKE ... (CTLT)
    final int CTAS = 2; // CREATE TABLE AS SELECT ... (CTAS)
    int command_type = CREATE_TABLE;

    LOG.info("Creating table " + tableName + " position="
        + ast.getCharPositionInLine());
    int numCh = ast.getChildCount();

    /*
     * Check the 1st-level children and do simple semantic checks: 1) CTLT and
     * CTAS should not coexists. 2) CTLT or CTAS should not coexists with column
     * list (target table schema). 3) CTAS does not support partitioning (for
     * now).
     */
    for (int num = 1; num < numCh; num++) {
      ASTNode child = (ASTNode) ast.getChild(num);
      switch (child.getToken().getType()) {
      case HiveParser.TOK_IFNOTEXISTS:
        ifNotExists = true;
        break;
      case HiveParser.KW_EXTERNAL:
        isExt = true;
        break;
      case HiveParser.TOK_LIKETABLE:
        if (child.getChildCount() > 0) {
          likeTableName = unescapeIdentifier(child.getChild(0).getText());
          if (likeTableName != null) {
            if (command_type == CTAS) {
              throw new SemanticException(ErrorMsg.CTAS_CTLT_COEXISTENCE
                  .getMsg());
            }
            if (cols.size() != 0) {
              throw new SemanticException(ErrorMsg.CTLT_COLLST_COEXISTENCE
                  .getMsg());
            }
          }
          command_type = CTLT;
        }
        break;
      case HiveParser.TOK_QUERY: // CTAS
        if (command_type == CTLT) {
          throw new SemanticException(ErrorMsg.CTAS_CTLT_COEXISTENCE.getMsg());
        }
        if (cols.size() != 0) {
          throw new SemanticException(ErrorMsg.CTAS_COLLST_COEXISTENCE.getMsg());
        }
        if (partCols.size() != 0 || bucketCols.size() != 0) {
          boolean dynPart = HiveConf.getBoolVar(conf, HiveConf.ConfVars.DYNAMICPARTITIONING);
          if (dynPart == false) {
            throw new SemanticException(ErrorMsg.CTAS_PARCOL_COEXISTENCE.getMsg());
          } else {
            // TODO: support dynamic partition for CTAS
            throw new SemanticException(ErrorMsg.CTAS_PARCOL_COEXISTENCE.getMsg());
          }
        }
        if (isExt) {
          throw new SemanticException(ErrorMsg.CTAS_EXTTBL_COEXISTENCE.getMsg());
        }
        command_type = CTAS;
        selectStmt = child;
        break;
      case HiveParser.TOK_TABCOLLIST:
        cols = getColumns(child);
        break;
      case HiveParser.TOK_TABLECOMMENT:
        comment = unescapeSQLString(child.getChild(0).getText());
        break;
      case HiveParser.TOK_TABLEPARTCOLS:
        partCols = getColumns((ASTNode) child.getChild(0), false);
        break;
      case HiveParser.TOK_TABLEBUCKETS:
        bucketCols = getColumnNames((ASTNode) child.getChild(0));
        if (child.getChildCount() == 2) {
          numBuckets = (Integer.valueOf(child.getChild(1).getText()))
              .intValue();
        } else {
          sortCols = getColumnNamesOrder((ASTNode) child.getChild(1));
          numBuckets = (Integer.valueOf(child.getChild(2).getText()))
              .intValue();
        }
        break;
      case HiveParser.TOK_TABLEROWFORMAT:

        child = (ASTNode) child.getChild(0);
        int numChildRowFormat = child.getChildCount();
        for (int numC = 0; numC < numChildRowFormat; numC++) {
          ASTNode rowChild = (ASTNode) child.getChild(numC);
          switch (rowChild.getToken().getType()) {
          case HiveParser.TOK_TABLEROWFORMATFIELD:
            fieldDelim = unescapeSQLString(rowChild.getChild(0).getText());
            if (rowChild.getChildCount() >= 2) {
              fieldEscape = unescapeSQLString(rowChild.getChild(1).getText());
            }
            break;
          case HiveParser.TOK_TABLEROWFORMATCOLLITEMS:
            collItemDelim = unescapeSQLString(rowChild.getChild(0).getText());
            break;
          case HiveParser.TOK_TABLEROWFORMATMAPKEYS:
            mapKeyDelim = unescapeSQLString(rowChild.getChild(0).getText());
            break;
          case HiveParser.TOK_TABLEROWFORMATLINES:
            lineDelim = unescapeSQLString(rowChild.getChild(0).getText());
            if (!lineDelim.equals("\n") && !lineDelim.equals("10")) {
              throw new SemanticException(
                  ErrorMsg.LINES_TERMINATED_BY_NON_NEWLINE.getMsg());
            }
            break;
          default:
            assert false;
          }
        }
        break;
      case HiveParser.TOK_TABLESERIALIZER:
        child = (ASTNode) child.getChild(0);
        serde = unescapeSQLString(child.getChild(0).getText());
        if (child.getChildCount() == 2) {
          readProps(
            (ASTNode) (child.getChild(1).getChild(0)),
            serdeProps);
        }
        break;
      case HiveParser.TOK_TBLSEQUENCEFILE:
        inputFormat = SEQUENCEFILE_INPUT;
        outputFormat = SEQUENCEFILE_OUTPUT;
        break;
      case HiveParser.TOK_TBLTEXTFILE:
        inputFormat = TEXTFILE_INPUT;
        outputFormat = TEXTFILE_OUTPUT;
        break;
      case HiveParser.TOK_TBLRCFILE:
        inputFormat = RCFILE_INPUT;
        outputFormat = RCFILE_OUTPUT;
        serde = COLUMNAR_SERDE;
        break;
      case HiveParser.TOK_TABLEFILEFORMAT:
        inputFormat = unescapeSQLString(child.getChild(0).getText());
        outputFormat = unescapeSQLString(child.getChild(1).getText());
        break;
      case HiveParser.TOK_TABLELOCATION:
        location = unescapeSQLString(child.getChild(0).getText());
        break;
      case HiveParser.TOK_TABLEPROPERTIES:
        tblProps = DDLSemanticAnalyzer.getProps((ASTNode) child.getChild(0));
        break;
      case HiveParser.TOK_STORAGEHANDLER:
        storageHandler = unescapeSQLString(child.getChild(0).getText());
        if (child.getChildCount() == 2) {
          readProps(
            (ASTNode) (child.getChild(1).getChild(0)),
            serdeProps);
        }
        break;
      default:
        assert false;
      }
    }

    if ((command_type == CTAS) && (storageHandler != null)) {
      throw new SemanticException(ErrorMsg.CREATE_NON_NATIVE_AS.getMsg());
    }

    if ((inputFormat == null) && (storageHandler == null)) {
      assert outputFormat == null;
      if ("SequenceFile".equalsIgnoreCase(conf.getVar(HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT))) {
        inputFormat = SEQUENCEFILE_INPUT;
        outputFormat = SEQUENCEFILE_OUTPUT;
      } else if ("RCFile".equalsIgnoreCase(conf.getVar(HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT))) {
        inputFormat = RCFILE_INPUT;
        outputFormat = RCFILE_OUTPUT;
        serde = COLUMNAR_SERDE;
      } else {
        inputFormat = TEXTFILE_INPUT;
        outputFormat = TEXTFILE_OUTPUT;
      }
    }

    // check for existence of table
    if (ifNotExists) {
      try {
        List<String> tables = db.getTablesByPattern(tableName);
        if (tables != null && tables.size() > 0) { // table exists
          return null;
        }
      } catch (HiveException e) {
        e.printStackTrace();
      }
    }

    // Handle different types of CREATE TABLE command
    CreateTableDesc crtTblDesc = null;
    switch (command_type) {

    case CREATE_TABLE: // REGULAR CREATE TABLE DDL
      crtTblDesc = new CreateTableDesc(tableName, isExt, cols, partCols,
          bucketCols, sortCols, numBuckets, fieldDelim, fieldEscape,
          collItemDelim, mapKeyDelim, lineDelim, comment, inputFormat,
          outputFormat, location, serde, storageHandler, serdeProps,
          tblProps, ifNotExists);

      validateCreateTable(crtTblDesc);
      rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
          crtTblDesc), conf));
      break;

    case CTLT: // create table like <tbl_name>
      CreateTableLikeDesc crtTblLikeDesc = new CreateTableLikeDesc(tableName,
          isExt, location, ifNotExists, likeTableName);
      rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
          crtTblLikeDesc), conf));
      break;

    case CTAS: // create table as select

      // check for existence of table. Throw an exception if it exists.
      try {
        Table tab = db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME,
            tableName, false); // do not throw exception if table does not exist

        if (tab != null) {
          throw new SemanticException(ErrorMsg.TABLE_ALREADY_EXISTS
              .getMsg(tableName));
        }
      } catch (HiveException e) { // may be unable to get meta data
        throw new SemanticException(e);
      }

      crtTblDesc = new CreateTableDesc(tableName, isExt, cols, partCols,
          bucketCols, sortCols, numBuckets, fieldDelim, fieldEscape,
          collItemDelim, mapKeyDelim, lineDelim, comment, inputFormat,
          outputFormat, location, serde, storageHandler, serdeProps,
          tblProps, ifNotExists);
      qb.setTableDesc(crtTblDesc);

      return selectStmt;
    default:
      assert false; // should never be unknown command type
    }
    return null;
  }

  private ASTNode analyzeCreateView(ASTNode ast, QB qb)
      throws SemanticException {
    String tableName = unescapeIdentifier(ast.getChild(0).getText());
    List<FieldSchema> cols = null;
    boolean ifNotExists = false;
    String comment = null;
    ASTNode selectStmt = null;
    Map<String, String> tblProps = null;

    LOG.info("Creating view " + tableName + " position="
        + ast.getCharPositionInLine());
    int numCh = ast.getChildCount();
    for (int num = 1; num < numCh; num++) {
      ASTNode child = (ASTNode) ast.getChild(num);
      switch (child.getToken().getType()) {
      case HiveParser.TOK_IFNOTEXISTS:
        ifNotExists = true;
        break;
      case HiveParser.TOK_QUERY:
        selectStmt = child;
        break;
      case HiveParser.TOK_TABCOLNAME:
        cols = getColumns(child);
        break;
      case HiveParser.TOK_TABLECOMMENT:
        comment = unescapeSQLString(child.getChild(0).getText());
        break;
      case HiveParser.TOK_TABLEPROPERTIES:
        tblProps = DDLSemanticAnalyzer.getProps((ASTNode) child.getChild(0));
        break;
      default:
        assert false;
      }
    }

    createVwDesc = new CreateViewDesc(
      tableName, cols, comment, tblProps, ifNotExists);
    unparseTranslator.enable();
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        createVwDesc), conf));
    return selectStmt;
  }

  private List<String> validateColumnNameUniqueness(
      List<FieldSchema> fieldSchemas) throws SemanticException {

    // no duplicate column names
    // currently, it is a simple n*n algorithm - this can be optimized later if
    // need be
    // but it should not be a major bottleneck as the number of columns are
    // anyway not so big
    Iterator<FieldSchema> iterCols = fieldSchemas.iterator();
    List<String> colNames = new ArrayList<String>();
    while (iterCols.hasNext()) {
      String colName = iterCols.next().getName();
      Iterator<String> iter = colNames.iterator();
      while (iter.hasNext()) {
        String oldColName = iter.next();
        if (colName.equalsIgnoreCase(oldColName)) {
          throw new SemanticException(ErrorMsg.DUPLICATE_COLUMN_NAMES
              .getMsg(oldColName));
        }
      }
      colNames.add(colName);
    }
    return colNames;
  }

  private void validateCreateTable(CreateTableDesc crtTblDesc)
      throws SemanticException {

    if ((crtTblDesc.getCols() == null) || (crtTblDesc.getCols().size() == 0)) {
      // for now make sure that serde exists
      if (StringUtils.isEmpty(crtTblDesc.getSerName())
          || !SerDeUtils.shouldGetColsFromSerDe(crtTblDesc.getSerName())) {
        throw new SemanticException(ErrorMsg.INVALID_TBL_DDL_SERDE.getMsg());
      }
      return;
    }

    if (crtTblDesc.getStorageHandler() == null) {
      try {
        Class<?> origin = Class.forName(crtTblDesc.getOutputFormat(), true,
          JavaUtils.getClassLoader());
        Class<? extends HiveOutputFormat> replaced = HiveFileFormatUtils
          .getOutputFormatSubstitute(origin);
        if (replaced == null) {
          throw new SemanticException(ErrorMsg.INVALID_OUTPUT_FORMAT_TYPE
            .getMsg());
        }
      } catch (ClassNotFoundException e) {
        throw new SemanticException(ErrorMsg.INVALID_OUTPUT_FORMAT_TYPE.getMsg());
      }
    }

    List<String> colNames = validateColumnNameUniqueness(crtTblDesc.getCols());

    if (crtTblDesc.getBucketCols() != null) {
      // all columns in cluster and sort are valid columns
      Iterator<String> bucketCols = crtTblDesc.getBucketCols().iterator();
      while (bucketCols.hasNext()) {
        String bucketCol = bucketCols.next();
        boolean found = false;
        Iterator<String> colNamesIter = colNames.iterator();
        while (colNamesIter.hasNext()) {
          String colName = colNamesIter.next();
          if (bucketCol.equalsIgnoreCase(colName)) {
            found = true;
            break;
          }
        }
        if (!found) {
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg());
        }
      }
    }

    if (crtTblDesc.getSortCols() != null) {
      // all columns in cluster and sort are valid columns
      Iterator<Order> sortCols = crtTblDesc.getSortCols().iterator();
      while (sortCols.hasNext()) {
        String sortCol = sortCols.next().getCol();
        boolean found = false;
        Iterator<String> colNamesIter = colNames.iterator();
        while (colNamesIter.hasNext()) {
          String colName = colNamesIter.next();
          if (sortCol.equalsIgnoreCase(colName)) {
            found = true;
            break;
          }
        }
        if (!found) {
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg());
        }
      }
    }

    if (crtTblDesc.getPartCols() != null) {
      // there is no overlap between columns and partitioning columns
      Iterator<FieldSchema> partColsIter = crtTblDesc.getPartCols().iterator();
      while (partColsIter.hasNext()) {
        String partCol = partColsIter.next().getName();
        Iterator<String> colNamesIter = colNames.iterator();
        while (colNamesIter.hasNext()) {
          String colName = unescapeIdentifier(colNamesIter.next());
          if (partCol.equalsIgnoreCase(colName)) {
            throw new SemanticException(
                ErrorMsg.COLUMN_REPEATED_IN_PARTITIONING_COLS.getMsg());
          }
        }
      }
    }
  }
}
