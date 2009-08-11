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

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Utilities;
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
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.MapJoinFactory;
import org.apache.hadoop.hive.ql.optimizer.GenMRFileSink1;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.optimizer.GenMROperator;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext;
import org.apache.hadoop.hive.ql.optimizer.GenMRRedSink1;
import org.apache.hadoop.hive.ql.optimizer.GenMRRedSink2;
import org.apache.hadoop.hive.ql.optimizer.GenMRTableScan1;
import org.apache.hadoop.hive.ql.optimizer.GenMRUnion1;
import org.apache.hadoop.hive.ql.optimizer.Optimizer;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMapRedCtx;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext;
import org.apache.hadoop.hive.ql.optimizer.GenMRRedSink3;
import org.apache.hadoop.hive.ql.optimizer.GenMRRedSink4;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.aggregationDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeFuncDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeNullDesc;
import org.apache.hadoop.hive.ql.plan.extractDesc;
import org.apache.hadoop.hive.ql.plan.fetchWork;
import org.apache.hadoop.hive.ql.plan.fileSinkDesc;
import org.apache.hadoop.hive.ql.plan.filterDesc;
import org.apache.hadoop.hive.ql.plan.groupByDesc;
import org.apache.hadoop.hive.ql.plan.joinDesc;
import org.apache.hadoop.hive.ql.plan.limitDesc;
import org.apache.hadoop.hive.ql.plan.loadFileDesc;
import org.apache.hadoop.hive.ql.plan.loadTableDesc;
import org.apache.hadoop.hive.ql.plan.moveWork;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.hive.ql.plan.reduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.scriptDesc;
import org.apache.hadoop.hive.ql.plan.selectDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.plan.tableScanDesc;
import org.apache.hadoop.hive.ql.plan.unionDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFHash;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;

import org.apache.hadoop.hive.serde.Constants;

/**
 * Implementation of the semantic analyzer
 */

public class SemanticAnalyzer extends BaseSemanticAnalyzer {
  private HashMap<String, org.apache.hadoop.hive.ql.parse.ASTPartitionPruner> aliasToPruner;
  private HashMap<TableScanOperator, exprNodeDesc> opToPartPruner;
  private HashMap<String, SamplePruner> aliasToSamplePruner;
  private HashMap<String, Operator<? extends Serializable>> topOps;
  private HashMap<String, Operator<? extends Serializable>> topSelOps;
  private LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx;
  private List<loadTableDesc> loadTableWork;
  private List<loadFileDesc> loadFileWork;
  private Map<JoinOperator, QBJoinTree> joinContext;
  private HashMap<TableScanOperator, Table> topToTable;
  private QB qb;
  private ASTNode ast;
  private int destTableId;
  private UnionProcContext uCtx;
  List<MapJoinOperator> listMapJoinOpsNoReducer;

  /**
   * ReadEntitites that are passed to the hooks.
   */
  private Set<ReadEntity> inputs;
  /**
   * List of WriteEntities that are passed to the hooks.
   */
  private Set<WriteEntity> outputs;
  
  private static class Phase1Ctx {
    String dest;
    int nextNum;
  }

  public SemanticAnalyzer(HiveConf conf) throws SemanticException {

    super(conf);

    this.aliasToPruner = new HashMap<String, org.apache.hadoop.hive.ql.parse.ASTPartitionPruner>();
    this.opToPartPruner = new HashMap<TableScanOperator, exprNodeDesc>();
    this.aliasToSamplePruner = new HashMap<String, SamplePruner>();
    this.topOps = new HashMap<String, Operator<? extends Serializable>>();
    this.topSelOps = new HashMap<String, Operator<? extends Serializable>>();
    this.loadTableWork = new ArrayList<loadTableDesc>();
    this.loadFileWork = new ArrayList<loadFileDesc>();
    opParseCtx = new LinkedHashMap<Operator<? extends Serializable>, OpParseContext>();
    joinContext = new HashMap<JoinOperator, QBJoinTree>();
    topToTable = new HashMap<TableScanOperator, Table>();
    this.destTableId = 1;
    this.uCtx = null;
    this.listMapJoinOpsNoReducer = new ArrayList<MapJoinOperator>();
    
    inputs = new LinkedHashSet<ReadEntity>();
    outputs = new LinkedHashSet<WriteEntity>();
  }
  

  @Override
  protected void reset() {
    super.reset();
    this.aliasToPruner.clear();
    this.loadTableWork.clear();
    this.loadFileWork.clear();
    this.topOps.clear();
    this.topSelOps.clear();
    this.destTableId = 1;
    this.idToTableNameMap.clear();
    qb = null;
    ast = null;
    uCtx = null;
    this.aliasToSamplePruner.clear();
    this.joinContext.clear();
    this.opParseCtx.clear();
  }

  public void init(ParseContext pctx) {
    aliasToPruner = pctx.getAliasToPruner();
    opToPartPruner = pctx.getOpToPartPruner();
    aliasToSamplePruner = pctx.getAliasToSamplePruner();
    topOps = pctx.getTopOps();
    topSelOps = pctx.getTopSelOps();
    opParseCtx = pctx.getOpParseCtx();
    loadTableWork = pctx.getLoadTableWork();
    loadFileWork = pctx.getLoadFileWork();
    joinContext = pctx.getJoinContext();
    ctx = pctx.getContext();
    destTableId = pctx.getDestTableId();
    idToTableNameMap = pctx.getIdToTableNameMap();
    this.uCtx = pctx.getUCtx();
    this.listMapJoinOpsNoReducer = pctx.getListMapJoinOpsNoReducer();
    qb = pctx.getQB();
  }

  public ParseContext getParseContext() {
    return new ParseContext(conf, qb, ast, aliasToPruner, opToPartPruner, aliasToSamplePruner, topOps, 
                            topSelOps, opParseCtx, joinContext, topToTable, loadTableWork, loadFileWork, ctx, idToTableNameMap, destTableId, uCtx,
                            listMapJoinOpsNoReducer);
  }
  
  @SuppressWarnings("nls")
  public void doPhase1QBExpr(ASTNode ast, QBExpr qbexpr, String id,
      String alias) throws SemanticException {

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
    if (expressionTree.getToken().getType() == HiveParser.TOK_FUNCTION
        || expressionTree.getToken().getType() == HiveParser.TOK_FUNCTIONDI) {
      assert (expressionTree.getChildCount() != 0);
      if (expressionTree.getChild(0).getType() == HiveParser.Identifier) {
        String functionName = unescapeIdentifier(expressionTree.getChild(0).getText());
        if (FunctionRegistry.getGenericUDAFResolver(functionName) != null) {
          aggregations.put(expressionTree.toStringTree(), expressionTree);
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
          throw new SemanticException(ErrorMsg.UNSUPPORTED_MULTIPLE_DISTINCTS.getMsg(expr));
        }
      }
    }
    return expr;
  }

  private void processTable(QB qb, ASTNode tabref) throws SemanticException {
    // For each table reference get the table name
    // and the alias (if alias is not present, the table name
    // is used as an alias)
    boolean tableSamplePresent = false;
    int aliasIndex = 0;
    if (tabref.getChildCount() == 2) {
      // tablename tablesample
      // OR
      // tablename alias
      ASTNode ct = (ASTNode)tabref.getChild(1);
      if (ct.getToken().getType() == HiveParser.TOK_TABLESAMPLE) {
        tableSamplePresent = true;
      }
      else {
        aliasIndex = 1;
      }
    }
    else if (tabref.getChildCount() == 3) {
      // table name table sample alias
      aliasIndex = 2;
      tableSamplePresent = true;
    }
    ASTNode tableTree = (ASTNode)(tabref.getChild(0));
    String alias = unescapeIdentifier(tabref.getChild(aliasIndex).getText());
    // If the alias is already there then we have a conflict
    if (qb.exists(alias)) {
      throw new SemanticException(ErrorMsg.AMBIGUOUS_TABLE_ALIAS.getMsg(tabref.getChild(aliasIndex)));
    }
    if (tableSamplePresent) {
      ASTNode sampleClause = (ASTNode)tabref.getChild(1);
      ArrayList<ASTNode> sampleCols = new ArrayList<ASTNode>();
      if (sampleClause.getChildCount() > 2) {
        for (int i = 2; i < sampleClause.getChildCount(); i++) {
          sampleCols.add((ASTNode)sampleClause.getChild(i));
        }
      }
      // TODO: For now only support sampling on up to two columns
      // Need to change it to list of columns
      if (sampleCols.size() > 2) {
        throw new SemanticException(ErrorMsg.SAMPLE_RESTRICTION.getMsg(tabref.getChild(0)));
      }
      qb.getParseInfo().setTabSample(alias, new TableSample(
          unescapeIdentifier(sampleClause.getChild(0).getText()), 
          unescapeIdentifier(sampleClause.getChild(1).getText()),
          sampleCols)
      );
    }
    // Insert this map into the stats
    String table_name = unescapeIdentifier(tabref.getChild(0).getText());
    qb.setTabAlias(alias, table_name);

    qb.getParseInfo().setSrcForAlias(alias, tableTree);
  }

  private void processSubQuery(QB qb, ASTNode subq) throws SemanticException {

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
      throw new SemanticException(ErrorMsg.AMBIGUOUS_TABLE_ALIAS.getMsg(subq.getChild(1)));
    }
    // Insert this map into the stats
    qb.setSubqAlias(alias, qbexpr);
  }

  private boolean isJoinToken(ASTNode node)
  {
    if ((node.getToken().getType() == HiveParser.TOK_JOIN) ||
        (node.getToken().getType() == HiveParser.TOK_LEFTOUTERJOIN) ||
        (node.getToken().getType() == HiveParser.TOK_RIGHTOUTERJOIN) ||
        (node.getToken().getType() == HiveParser.TOK_FULLOUTERJOIN))
      return true;

    return false;
  }

  @SuppressWarnings("nls")
  private void processJoin(QB qb, ASTNode join) throws SemanticException {
    int numChildren = join.getChildCount();
    if ((numChildren != 2) && (numChildren != 3))
      throw new SemanticException("Join with multiple children");

    for (int num = 0; num < numChildren; num++) {
      ASTNode child = (ASTNode) join.getChild(num);
      if (child.getToken().getType() == HiveParser.TOK_TABREF)
        processTable(qb, child);
      else if (child.getToken().getType() == HiveParser.TOK_SUBQUERY)
        processSubQuery(qb, child);
      else if (isJoinToken(child))
        processJoin(qb, child);
    }
  }

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

        if (((ASTNode)ast.getChild(0)).getToken().getType() == HiveParser.TOK_HINTLIST)
          qbp.setHints((ASTNode)ast.getChild(0));

        LinkedHashMap<String, ASTNode> aggregations = doPhase1GetAggregationsFromSelect(ast);
        qbp.setAggregationExprsForClause(ctx_1.dest, aggregations);
        qbp.setDistinctFuncExprForClause(ctx_1.dest,
            doPhase1GetDistinctFuncExpr(aggregations));
        break;

      case HiveParser.TOK_WHERE: {
        qbp.setWhrExprForClause(ctx_1.dest, ast);
      }
        break;

      case HiveParser.TOK_DESTINATION: {
        ctx_1.dest = "insclause-" + ctx_1.nextNum;
        ctx_1.nextNum++;

        // is there a insert in the subquery
        if (qbp.getIsSubQ()) {
          ASTNode ch = (ASTNode)ast.getChild(0);
          if ((ch.getToken().getType() != HiveParser.TOK_DIR) ||
              (((ASTNode)ch.getChild(0)).getToken().getType() != HiveParser.TOK_TMP_FILE))
            throw new SemanticException(ErrorMsg.NO_INSERT_INSUBQUERY.getMsg(ast));
        }

        qbp.setDestForClause(ctx_1.dest, (ASTNode) ast.getChild(0));
      }
        break;

      case HiveParser.TOK_FROM: {
        int child_count = ast.getChildCount();
        if (child_count != 1)
          throw new SemanticException("Multiple Children " + child_count);

        // Check if this is a subquery
        ASTNode frm = (ASTNode) ast.getChild(0);
        if (frm.getToken().getType() == HiveParser.TOK_TABREF)
          processTable(qb, frm);
        else if (frm.getToken().getType() == HiveParser.TOK_SUBQUERY)
          processSubQuery(qb, frm);
        else if (isJoinToken(frm))
        {
          processJoin(qb, frm);
          qbp.setJoinExpr(frm);
        }
      }
        break;

      case HiveParser.TOK_CLUSTERBY: {
        // Get the clusterby aliases - these are aliased to the entries in the
        // select list
        qbp.setClusterByExprForClause(ctx_1.dest, ast);
      }
        break;

      case HiveParser.TOK_DISTRIBUTEBY: {
        // Get the distribute by  aliases - these are aliased to the entries in the
        // select list
        qbp.setDistributeByExprForClause(ctx_1.dest, ast);
        if (qbp.getClusterByForClause(ctx_1.dest) != null) {
          throw new SemanticException(ErrorMsg.CLUSTERBY_DISTRIBUTEBY_CONFLICT.getMsg(ast));
        }
        else if (qbp.getOrderByForClause(ctx_1.dest) != null) {
          throw new SemanticException(ErrorMsg.ORDERBY_DISTRIBUTEBY_CONFLICT.getMsg(ast));
        }
      }
        break;

      case HiveParser.TOK_SORTBY: {
        // Get the sort by aliases - these are aliased to the entries in the
        // select list
        qbp.setSortByExprForClause(ctx_1.dest, ast);
        if (qbp.getClusterByForClause(ctx_1.dest) != null) {
          throw new SemanticException(ErrorMsg.CLUSTERBY_SORTBY_CONFLICT.getMsg(ast));
        }
        else if (qbp.getOrderByForClause(ctx_1.dest) != null) {
          throw new SemanticException(ErrorMsg.ORDERBY_SORTBY_CONFLICT.getMsg(ast));
        }
        
      }
        break;

      case HiveParser.TOK_ORDERBY: {
        // Get the order by aliases - these are aliased to the entries in the
        // select list
        qbp.setOrderByExprForClause(ctx_1.dest, ast);
        if (qbp.getClusterByForClause(ctx_1.dest) != null) {
          throw new SemanticException(ErrorMsg.CLUSTERBY_ORDERBY_CONFLICT.getMsg(ast));
        }
      }
        break;

      case HiveParser.TOK_GROUPBY: {
        // Get the groupby aliases - these are aliased to the entries in the
        // select list
        if (qbp.getSelForClause(ctx_1.dest).getToken().getType() == HiveParser.TOK_SELECTDI) {
          throw new SemanticException(ErrorMsg.SELECT_DISTINCT_WITH_GROUPBY.getMsg(ast));
        }
        qbp.setGroupByExprForClause(ctx_1.dest, ast);
        skipRecursion = true;
      }
        break;
        
      case HiveParser.TOK_LIMIT: 
        {
          qbp.setDestLimit(ctx_1.dest, new Integer(ast.getChild(0).getText()));
        }
        break;

      case HiveParser.TOK_UNION:
        // currently, we dont support subq1 union subq2 - the user has to explicitly say: 
        // select * from (subq1 union subq2) subqalias
        if (!qbp.getIsSubQ())
          throw new SemanticException(ErrorMsg.UNION_NOTIN_SUBQ.getMsg());

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

  private void genPartitionPruners(QBExpr qbexpr) throws SemanticException {
    if (qbexpr.getOpcode() == QBExpr.Opcode.NULLOP) {
      genPartitionPruners(qbexpr.getQB());
    } else {
      genPartitionPruners(qbexpr.getQBExpr1());
      genPartitionPruners(qbexpr.getQBExpr2());
    }
  }

  /** 
   * Generate partition pruners. The filters can occur in the where clause and in the JOIN conditions. First, walk over the 
   * filters in the join condition and AND them, since all of them are needed. Then for each where clause, traverse the 
   * filter. 
   * Note that, currently we do not propagate filters over subqueries. For eg: if the query is of the type:
   * select ... FROM t1 JOIN (select ... t2) x where x.partition
   * we will not recognize that x.partition condition introduces a parition pruner on t2
   * 
   */
  @SuppressWarnings("nls")
  private void genPartitionPruners(QB qb) throws SemanticException {
    Map<String, Boolean> joinPartnPruner = new HashMap<String, Boolean>();
    QBParseInfo qbp = qb.getParseInfo();

    // Recursively prune subqueries
    for (String alias : qb.getSubqAliases()) {
      QBExpr qbexpr = qb.getSubqForAlias(alias);
      genPartitionPruners(qbexpr);
    }

    for (String alias : qb.getTabAliases()) {
      String alias_id = (qb.getId() == null ? alias : qb.getId() + ":" + alias);

      org.apache.hadoop.hive.ql.parse.ASTPartitionPruner pruner = 
        new org.apache.hadoop.hive.ql.parse.ASTPartitionPruner(alias, qb.getMetaData());
      
      // Pass each where clause to the pruner
      for(String clause: qbp.getClauseNames()) {

        ASTNode whexp = (ASTNode)qbp.getWhrForClause(clause);
        if (whexp != null) {
          pruner.addExpression((ASTNode)whexp.getChild(0));
        }
      }

      // Add the pruner to the list
      this.aliasToPruner.put(alias_id, pruner);
    }

    if (!qb.getTabAliases().isEmpty() && qb.getQbJoinTree() != null) {
      int pos = 0;
      for (String alias : qb.getQbJoinTree().getBaseSrc()) {
        if (alias != null) {
          String alias_id = (qb.getId() == null ? alias : qb.getId() + ":" + alias);
          org.apache.hadoop.hive.ql.parse.ASTPartitionPruner pruner = 
            this.aliasToPruner.get(alias_id);
          if(pruner == null) {
            // this means that the alias is a subquery
            pos++;
            continue;
          }
          Vector<ASTNode> filters = qb.getQbJoinTree().getFilters().get(pos);
          for (ASTNode cond : filters) {
            pruner.addJoinOnExpression(cond);
            if (pruner.hasPartitionPredicate(cond))
              joinPartnPruner.put(alias_id, new Boolean(true));
          }
          if (qb.getQbJoinTree().getJoinSrc() != null) {
            filters = qb.getQbJoinTree().getFilters().get(0);
            for (ASTNode cond : filters) {
              pruner.addJoinOnExpression(cond);
              if (pruner.hasPartitionPredicate(cond))
                joinPartnPruner.put(alias_id, new Boolean(true));
            }
          }
        }
        pos++;
      }
    }

    for (String alias : qb.getTabAliases()) {
      String alias_id = (qb.getId() == null ? alias : qb.getId() + ":" + alias);
      org.apache.hadoop.hive.ql.parse.ASTPartitionPruner pruner = this.aliasToPruner.get(alias_id);
      if (joinPartnPruner.get(alias_id) == null) {
        // Pass each where clause to the pruner
         for(String clause: qbp.getClauseNames()) {
          
           ASTNode whexp = (ASTNode)qbp.getWhrForClause(clause);
           if (pruner.getTable().isPartitioned() &&
               conf.getVar(HiveConf.ConfVars.HIVEMAPREDMODE).equalsIgnoreCase("strict") &&
               (whexp == null || !pruner.hasPartitionPredicate((ASTNode)whexp.getChild(0)))) {
             throw new SemanticException(ErrorMsg.NO_PARTITION_PREDICATE.getMsg(whexp != null ? whexp : qbp.getSelForClause(clause), 
                                                                                " for Alias " + alias + " Table " + pruner.getTable().getName()));
           }
         }
      }
    }
  }

  private void genSamplePruners(QBExpr qbexpr) throws SemanticException {
    if (qbexpr.getOpcode() == QBExpr.Opcode.NULLOP) {
      genSamplePruners(qbexpr.getQB());
    } else {
      genSamplePruners(qbexpr.getQBExpr1());
      genSamplePruners(qbexpr.getQBExpr2());
    }
  }
  
  @SuppressWarnings("nls")
  private void genSamplePruners(QB qb) throws SemanticException {
    // Recursively prune subqueries
    for (String alias : qb.getSubqAliases()) {
      QBExpr qbexpr = qb.getSubqForAlias(alias);
      genSamplePruners(qbexpr);
    }
    for (String alias : qb.getTabAliases()) {
      String alias_id = (qb.getId() == null ? alias : qb.getId() + ":" + alias);
      QBParseInfo qbp = qb.getParseInfo();
      TableSample tableSample = qbp.getTabSample(alias_id);
      if (tableSample != null) {
        SamplePruner pruner = new SamplePruner(alias, tableSample);
        this.aliasToSamplePruner.put(alias_id, pruner);
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

      // Go over the tables and populate the related structures
      for (String alias : qb.getTabAliases()) {
        String tab_name = qb.getTabNameForAlias(alias);
        Table tab = null;
        try {
          tab = this.db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tab_name);
        }
        catch (InvalidTableException ite) {
          throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(qb.getParseInfo().getSrcForAlias(alias)));
        }

        if (!InputFormat.class.isAssignableFrom(tab.getInputFormatClass()))
          throw new SemanticException(ErrorMsg.INVALID_INPUT_FORMAT_TYPE.getMsg(qb.getParseInfo().getSrcForAlias(alias)));

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
          tableSpec ts = new tableSpec(this.db, conf, ast);

          if (!HiveOutputFormat.class.isAssignableFrom(ts.tableHandle.getOutputFormatClass()))
            throw new SemanticException(ErrorMsg.INVALID_OUTPUT_FORMAT_TYPE.getMsg(ast));

          if(ts.partSpec == null) {
            // This is a table
            qb.getMetaData().setDestForAlias(name, ts.tableHandle);
          } else {
            // This is a partition
            qb.getMetaData().setDestForAlias(name, ts.partHandle);
          }
          break;
        }
        case HiveParser.TOK_LOCAL_DIR:
        case HiveParser.TOK_DIR:
          {
            // This is a dfs file
            String fname = stripQuotes(ast.getChild(0).getText());
            if ((!qb.getParseInfo().getIsSubQ()) &&
                (((ASTNode)ast.getChild(0)).getToken().getType() == HiveParser.TOK_TMP_FILE))
            {
              fname = ctx.getMRTmpFileURI();
              ctx.setResDir(new Path(fname));
              qb.setIsQuery(true);
            }
            qb.getMetaData().setDestForAlias(name, fname,
                                             (ast.getToken().getType() == HiveParser.TOK_DIR));
            break;
          }
        default:
          throw new SemanticException("Unknown Token Type " + ast.getToken().getType());
        }
      }
    } catch (HiveException e) {
      // Has to use full name to make sure it does not conflict with org.apache.commons.lang.StringUtils
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      throw new SemanticException(e.getMessage(), e);
    }
  }

  private boolean isPresent(String[] list, String elem) {
    for (String s : list)
      if (s.equals(elem))
        return true;

    return false;
  }

  @SuppressWarnings("nls")
  private void parseJoinCondPopulateAlias(QBJoinTree joinTree,
      ASTNode condn, Vector<String> leftAliases, Vector<String> rightAliases)
      throws SemanticException {
    // String[] allAliases = joinTree.getAllAliases();
    switch (condn.getToken().getType()) {
    case HiveParser.TOK_TABLE_OR_COL:
      String tableOrCol = unescapeIdentifier(condn.getChild(0).getText().toLowerCase());
      if (isPresent(joinTree.getLeftAliases(), tableOrCol)) {
        if (!leftAliases.contains(tableOrCol))
          leftAliases.add(tableOrCol);
      } else if (isPresent(joinTree.getRightAliases(), tableOrCol)) {
        if (!rightAliases.contains(tableOrCol))
          rightAliases.add(tableOrCol);
      } else {
        // We don't support columns without table prefix in JOIN condition right now.
        // We need to pass Metadata here to know which table the column belongs to. 
        throw new SemanticException(ErrorMsg.INVALID_TABLE_ALIAS.getMsg(condn.getChild(0)));
      }
      break;

    case HiveParser.Number:
    case HiveParser.StringLiteral:
    case HiveParser.Identifier:
    case HiveParser.TOK_CHARSETLITERAL:
    case HiveParser.KW_TRUE:
    case HiveParser.KW_FALSE:
      break;

    case HiveParser.TOK_FUNCTION:
      // check all the arguments
      for (int i = 1; i < condn.getChildCount(); i++)
        parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(i),
            leftAliases, rightAliases);
      break;

    default:
      // This is an operator - so check whether it is unary or binary operator
      if (condn.getChildCount() == 1)
        parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(0),
            leftAliases, rightAliases);
      else if (condn.getChildCount() == 2) {
        parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(0),
            leftAliases, rightAliases);
        parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(1),
            leftAliases, rightAliases);
      } else
        throw new SemanticException(condn.toStringTree() + " encountered with "
            + condn.getChildCount() + " children");
      break;
    }
  }

  private void populateAliases(Vector<String> leftAliases,
      Vector<String> rightAliases, ASTNode condn, QBJoinTree joinTree,
      Vector<String> leftSrc) throws SemanticException {
    if ((leftAliases.size() != 0) && (rightAliases.size() != 0))
      throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_1.getMsg(condn));

    if (rightAliases.size() != 0) {
      assert rightAliases.size() == 1;
      joinTree.getExpressions().get(1).add(condn);
    } else if (leftAliases.size() != 0) {
      joinTree.getExpressions().get(0).add(condn);
      for (String s : leftAliases)
        if (!leftSrc.contains(s))
          leftSrc.add(s);
    } else
      throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_2.getMsg(condn));
  }

  /**
   * Parse the join condition. 
   * If the condition is a join condition, throw an error if it is not an equality. Otherwise, break it into left and 
   * right expressions and store in the join tree.
   * If the condition is a join filter, add it to the filter list of join tree.  The join condition can contains conditions
   * on both the left and tree trees and filters on either. Currently, we only support equi-joins, so we throw an error
   * if the condition involves both subtrees and is not a equality. Also, we only support AND i.e ORs are not supported 
   * currently as their semantics are not very clear, may lead to data explosion and there is no usecase.
   * @param joinTree  jointree to be populated
   * @param joinCond  join condition
   * @param leftSrc   left sources
   * @throws SemanticException
   */
  private void parseJoinCondition(QBJoinTree joinTree, ASTNode joinCond, Vector<String> leftSrc)
      throws SemanticException {
    if (joinCond == null) 
      return;

    switch (joinCond.getToken().getType()) {
    case HiveParser.KW_OR:
      throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_3.getMsg(joinCond));
      
    case HiveParser.KW_AND:
      parseJoinCondition(joinTree, (ASTNode) joinCond
          .getChild(0), leftSrc);
      parseJoinCondition(joinTree, (ASTNode) joinCond
          .getChild(1), leftSrc);
      break;

    case HiveParser.EQUAL:
      ASTNode leftCondn = (ASTNode) joinCond.getChild(0);
      Vector<String> leftCondAl1 = new Vector<String>();
      Vector<String> leftCondAl2 = new Vector<String>();
      parseJoinCondPopulateAlias(joinTree, leftCondn, leftCondAl1, leftCondAl2);

      ASTNode rightCondn = (ASTNode) joinCond.getChild(1);
      Vector<String> rightCondAl1 = new Vector<String>();
      Vector<String> rightCondAl2 = new Vector<String>();
      parseJoinCondPopulateAlias(joinTree, rightCondn, rightCondAl1, rightCondAl2);

      // is it a filter or a join condition
      if (((leftCondAl1.size() != 0) && (leftCondAl2.size() != 0)) ||
          ((rightCondAl1.size() != 0) && (rightCondAl2.size() != 0)))
        throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_1.getMsg(joinCond));

      if (leftCondAl1.size() != 0) {
        if ((rightCondAl1.size() != 0) || ((rightCondAl1.size() == 0) && (rightCondAl2.size() == 0)))
          joinTree.getFilters().get(0).add(joinCond);
        else if (rightCondAl2.size() != 0) {
          populateAliases(leftCondAl1, leftCondAl2, leftCondn, joinTree, leftSrc);
          populateAliases(rightCondAl1, rightCondAl2, rightCondn, joinTree, leftSrc);
        }
      }
      else if (leftCondAl2.size() != 0) {
        if ((rightCondAl2.size() != 0) || ((rightCondAl1.size() == 0) && (rightCondAl2.size() == 0)))
          joinTree.getFilters().get(1).add(joinCond);
        else if (rightCondAl1.size() != 0) {
          populateAliases(leftCondAl1, leftCondAl2, leftCondn, joinTree, leftSrc);
          populateAliases(rightCondAl1, rightCondAl2, rightCondn, joinTree, leftSrc);
        }
      }
      else if (rightCondAl1.size() != 0)
        joinTree.getFilters().get(0).add(joinCond);
      else
        joinTree.getFilters().get(1).add(joinCond);

      break;

    default:
      boolean isFunction = (joinCond.getType() == HiveParser.TOK_FUNCTION);
        
      // Create all children
      int childrenBegin = (isFunction ? 1 : 0);
      ArrayList<Vector<String>> leftAlias = new ArrayList<Vector<String>>(joinCond.getChildCount() - childrenBegin);
      ArrayList<Vector<String>> rightAlias = new ArrayList<Vector<String>>(joinCond.getChildCount() - childrenBegin);
      for (int ci = 0; ci < joinCond.getChildCount() - childrenBegin; ci++) {
        Vector<String> left  = new Vector<String>();
        Vector<String> right = new Vector<String>();
        leftAlias.add(left);
        rightAlias.add(right);
      }
        
      for (int ci=childrenBegin; ci<joinCond.getChildCount(); ci++)
        parseJoinCondPopulateAlias(joinTree, (ASTNode)joinCond.getChild(ci), leftAlias.get(ci-childrenBegin), rightAlias.get(ci-childrenBegin));

      boolean leftAliasNull = true;
      for (Vector<String> left : leftAlias) {
        if (left.size() != 0) {
          leftAliasNull = false;
          break;
        }
      }

      boolean rightAliasNull = true;
      for (Vector<String> right : rightAlias) {
        if (right.size() != 0) {
          rightAliasNull = false;
          break;
        }
      }

      if (!leftAliasNull && !rightAliasNull)
        throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_1.getMsg(joinCond));

      if (!leftAliasNull)
        joinTree.getFilters().get(0).add(joinCond);
      else
        joinTree.getFilters().get(1).add(joinCond);
        
      break;
    }
  }

  @SuppressWarnings("nls")
  public <T extends Serializable> Operator<T> putOpInsertMap(Operator<T> op, RowResolver rr) 
  {
    OpParseContext ctx = new OpParseContext(rr);
    opParseCtx.put(op, ctx);
    return op;
  }
  
  @SuppressWarnings("nls")
  private Operator genFilterPlan(String dest, QB qb,
      Operator input) throws SemanticException {

    ASTNode whereExpr = qb.getParseInfo().getWhrForClause(dest);
    return genFilterPlan(qb, (ASTNode)whereExpr.getChild(0), input);
  }

  /**
   * create a filter plan. The condition and the inputs are specified.
   * @param qb current query block
   * @param condn The condition to be resolved
   * @param input the input operator
   */
  @SuppressWarnings("nls")
  private Operator genFilterPlan(QB qb, ASTNode condn, Operator input) throws SemanticException {

    OpParseContext inputCtx = opParseCtx.get(input);
    RowResolver inputRR = inputCtx.getRR();
    Operator output = putOpInsertMap(
      OperatorFactory.getAndMakeChild(
        new filterDesc(genExprNodeDesc(condn, inputRR), false),
          new RowSchema(inputRR.getColumnInfos()), input), inputRR);
 
    LOG.debug("Created Filter Plan for " + qb.getId() + " row schema: " + inputRR.toString());
    return output;
  }

  @SuppressWarnings("nls")
  private Integer genColListRegex(String colRegex, String tabAlias, String alias, ASTNode sel,
    ArrayList<exprNodeDesc> col_list, RowResolver input, Integer pos,
    RowResolver output) throws SemanticException {
    
    // The table alias should exist
    if (tabAlias != null && !input.hasTableAlias(tabAlias))
      throw new SemanticException(ErrorMsg.INVALID_TABLE_ALIAS.getMsg(sel));
    
    // TODO: Have to put in the support for AS clause
    Pattern regex = null;
    try {
      regex = Pattern.compile(colRegex, Pattern.CASE_INSENSITIVE);
    } catch (PatternSyntaxException e) {
      throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(sel, e.getMessage()));
    }
    
    int matched = 0;
    // This is the tab.* case
    // In this case add all the columns to the fieldList
    // from the input schema
    for(ColumnInfo colInfo: input.getColumnInfos()) {
      String name = colInfo.getInternalName();
      String [] tmp = input.reverseLookup(name);

      // Skip the colinfos which are not for this particular alias 
      if (tabAlias != null && !tmp[0].equalsIgnoreCase(tabAlias)) {
        continue;
      }
 
      // Not matching the regex?
      if (!regex.matcher(tmp[1]).matches()) {
        continue;
      }
      
      exprNodeColumnDesc expr = new exprNodeColumnDesc(colInfo.getType(), name,
                                                       colInfo.getTabAlias(),
                                                       colInfo.getIsPartitionCol());
      col_list.add(expr);
      output.put(tmp[0], tmp[1], 
                 new ColumnInfo(getColumnInternalName(pos), colInfo.getType(),
                     colInfo.getTabAlias(), colInfo.getIsPartitionCol()));
      pos = Integer.valueOf(pos.intValue() + 1);
      matched ++;
    }
    if (matched == 0) {
      throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(sel));
    }
    return pos;
  }
  
  public static String getColumnInternalName(int pos) {
    return HiveConf.getColumnInternalName(pos);
  }


  /**
   * If the user script command needs any modifications - do it here
   */
  private String getFixedCmd(String cmd) {
    SessionState ss = SessionState.get();
    if(ss == null)
      return cmd;

    // for local mode - replace any references to packaged files by name with 
    // the reference to the original file path
    if(ss.getConf().get("mapred.job.tracker", "local").equals("local")) {
      Set<String> files = ss.list_resource(SessionState.ResourceType.FILE, null);
      if((files != null) && !files.isEmpty()) {
        int end = cmd.indexOf(" ");
        String prog = (end == -1) ? cmd : cmd.substring(0, end);
        String args = (end == -1) ? "" :  cmd.substring(end, cmd.length());

        for(String oneFile: files) {
          Path p = new Path(oneFile);
          if(p.getName().equals(prog)) {
            cmd = oneFile + args;
            break;
          }
        }
      }
    }

    return cmd;
  }


  @SuppressWarnings("nls")
  private Operator genScriptPlan(ASTNode trfm, QB qb,
      Operator input) throws SemanticException {
    // If there is no "AS" clause, the output schema will be "key,value"
    ArrayList<String> outputColList = new ArrayList<String>();
    boolean defaultOutputColList = (trfm.getChildCount() < 3);
    if (defaultOutputColList) {
      outputColList.add("key");
      outputColList.add("value");
    } else {
      ASTNode collist = (ASTNode) trfm.getChild(2);
      int ccount = collist.getChildCount();
      for (int i=0; i < ccount; ++i) {
        outputColList.add(unescapeIdentifier(((ASTNode)collist.getChild(i)).getText()));
      }
    }
    
    RowResolver out_rwsch = new RowResolver();
    StringBuilder columns = new StringBuilder();
    for (int i = 0; i < outputColList.size(); ++i) {
      if (i != 0) {
        columns.append(",");
      }
      columns.append(outputColList.get(i));
      out_rwsch.put(
        qb.getParseInfo().getAlias(),
        outputColList.get(i),
        new ColumnInfo(outputColList.get(i), TypeInfoFactory.stringTypeInfo, null, false)  // Script output is always a string
      );
    }

    Operator output = putOpInsertMap(OperatorFactory
            .getAndMakeChild(
                new scriptDesc(
                    getFixedCmd(stripQuotes(trfm.getChild(1).getText())),
                    PlanUtils.getDefaultTableDesc(Integer.toString(Utilities.tabCode), columns.toString(), defaultOutputColList),
                    PlanUtils.getDefaultTableDesc(Integer.toString(Utilities.tabCode), "")),
                    new RowSchema(
                        out_rwsch.getColumnInfos()), input), out_rwsch);

    return output;
  }

  /**
   * This function is a wrapper of parseInfo.getGroupByForClause which automatically
   * translates SELECT DISTINCT a,b,c to SELECT a,b,c GROUP BY a,b,c.
   */
  static List<ASTNode> getGroupByForClause(QBParseInfo parseInfo, String dest) {
    if (parseInfo.getSelForClause(dest).getToken().getType() == HiveParser.TOK_SELECTDI) {
      ASTNode selectExprs = parseInfo.getSelForClause(dest);
      List<ASTNode> result = new ArrayList<ASTNode>(selectExprs == null 
          ? 0 : selectExprs.getChildCount());
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
      List<ASTNode> result = new ArrayList<ASTNode>(grpByExprs == null 
          ? 0 : grpByExprs.getChildCount());
      if (grpByExprs != null) {
        for (int i = 0; i < grpByExprs.getChildCount(); ++i) {
          ASTNode grpbyExpr = (ASTNode) grpByExprs.getChild(i);
          result.add(grpbyExpr);
        }
      }
      return result;
    }
  }
  
  private static String[] getColAlias(ASTNode selExpr, String defaultName, RowResolver inputRR) {
    String colAlias = null;
    String tabAlias = null;
    String[] colRef = new String[2];
    
    if (selExpr.getChildCount() == 2) {
      // return zz for "xx + yy AS zz"
      colAlias  = unescapeIdentifier(selExpr.getChild(1).getText());
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
  
    if(colAlias == null) {
      // Return defaultName if selExpr is not a simple xx.yy.zz 
      colAlias = defaultName;
    }
    
    colRef[0] = tabAlias;
    colRef[1] = colAlias;
    return colRef;
  }
  
  /**
   * Returns whether the pattern is a regex expression (instead of a normal string).
   * Normal string is a string with all alphabets/digits and "_".
   */
  private static boolean isRegex(String pattern) {
    for(int i=0; i<pattern.length(); i++) {
      if (!Character.isLetterOrDigit(pattern.charAt(i))
          && pattern.charAt(i) != '_') {
        return true;
      }
    }
    return false;    
  }
  
  @SuppressWarnings("nls")
  private Operator genSelectPlan(String dest, QB qb,
    Operator input) throws SemanticException {

    ASTNode selExprList = qb.getParseInfo().getSelForClause(dest);

    ArrayList<exprNodeDesc> col_list = new ArrayList<exprNodeDesc>();
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

    boolean isInTransform = (selExprList.getChild(posn).getChild(0).getType() 
        == HiveParser.TOK_TRANSFORM);
    if (isInTransform) {
      trfm = (ASTNode) selExprList.getChild(posn).getChild(0);
    }
    
    // The list of expressions after SELECT or SELECT TRANSFORM.
    ASTNode exprList = (isInTransform ? (ASTNode) trfm.getChild(0) : selExprList);

    LOG.debug("genSelectPlan: input = " + inputRR.toString());
    // Iterate over all expression (either after SELECT, or in SELECT TRANSFORM)
    for (int i = posn; i < exprList.getChildCount(); ++i) {

      // child can be EXPR AS ALIAS, or EXPR.
      ASTNode child = (ASTNode) exprList.getChild(i);
      boolean hasAsClause = (!isInTransform) && (child.getChildCount() == 2);
      // The real expression
      ASTNode expr;
      String tabAlias;
      String colAlias;
      
      if (isInTransform) {
        tabAlias = null;
        colAlias = "_C" + i;
        expr = child;
      } else {
        String[] colRef = getColAlias(child, "_C" + i, inputRR);
        tabAlias = colRef[0];
        colAlias = colRef[1];
        // Get rid of TOK_SELEXPR
        expr = (ASTNode)child.getChild(0);
      }
       
      if (expr.getType() == HiveParser.TOK_ALLCOLREF) {
        pos = genColListRegex(".*", 
            expr.getChildCount() == 0 ? null : unescapeIdentifier(expr.getChild(0).getText().toLowerCase()),
            alias, expr, col_list, inputRR, pos, out_rwsch);
        selectStar = true;
      } else if (expr.getType() == HiveParser.TOK_TABLE_OR_COL
          && !hasAsClause
          && !inputRR.getIsExprResolver()
          && isRegex(unescapeIdentifier(expr.getChild(0).getText()))) {
        // In case the expression is a regex COL.
        // This can only happen without AS clause
        // We don't allow this for ExprResolver - the Group By case
        pos = genColListRegex(unescapeIdentifier(expr.getChild(0).getText()),
            null, alias, expr, col_list, inputRR, pos, out_rwsch);
      } else if (expr.getType() == HiveParser.DOT
          && expr.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
          && inputRR.hasTableAlias(unescapeIdentifier(expr.getChild(0).getChild(0).getText().toLowerCase()))
          && !hasAsClause
          && !inputRR.getIsExprResolver()
          && isRegex(unescapeIdentifier(expr.getChild(1).getText()))) {
        // In case the expression is TABLE.COL (col can be regex).
        // This can only happen without AS clause
        // We don't allow this for ExprResolver - the Group By case
        pos = genColListRegex(unescapeIdentifier(expr.getChild(1).getText()), 
            unescapeIdentifier(expr.getChild(0).getChild(0).getText().toLowerCase()),
            alias, expr, col_list, inputRR, pos, out_rwsch);
      } else {
        // Case when this is an expression
        exprNodeDesc exp = genExprNodeDesc(expr, inputRR);
        col_list.add(exp);
        if (!StringUtils.isEmpty(alias) &&
            (out_rwsch.get(null, colAlias) != null)) {
          throw new SemanticException(ErrorMsg.AMBIGUOUS_COLUMN.getMsg(expr.getChild(1)));
        }
        out_rwsch.put(tabAlias, colAlias,
                      new ColumnInfo(getColumnInternalName(pos),
                                     exp.getTypeInfo(), tabAlias, false));
        pos = Integer.valueOf(pos.intValue() + 1);
      }
    }
    selectStar = selectStar && exprList.getChildCount() == posn + 1;

    ArrayList<String> columnNames = new ArrayList<String>();
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    for (int i=0; i<col_list.size(); i++) {
      // Replace NULL with CAST(NULL AS STRING)
      if (col_list.get(i) instanceof exprNodeNullDesc) {
        col_list.set(i, new exprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, null));
      }
      String outputCol = getColumnInternalName(i);
      colExprMap.put(outputCol, col_list.get(i));
      columnNames.add(outputCol);
    }
    
    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new selectDesc(col_list, columnNames, selectStar), new RowSchema(out_rwsch.getColumnInfos()),
        input), out_rwsch);

    output.setColumnExprMap(colExprMap);
    if (isInTransform) {
      output = genScriptPlan(trfm, qb, output);
    }

    LOG.debug("Created Select Plan for clause: " + dest + " row schema: " + out_rwsch.toString());

    return output;
  }

  /**
   * Class to store GenericUDAF related information.
   */
  static class GenericUDAFInfo {
    ArrayList<exprNodeDesc> convertedParameters;
    GenericUDAFEvaluator genericUDAFEvaluator;
    TypeInfo returnType;
  }

  /**
   * Convert exprNodeDesc array to Typeinfo array. 
   */
  static ArrayList<TypeInfo> getTypeInfo(ArrayList<exprNodeDesc> exprs) {
    ArrayList<TypeInfo> result = new ArrayList<TypeInfo>();
    for(exprNodeDesc expr: exprs) {
      result.add(expr.getTypeInfo());
    }
    return result;
  }
  
  /**
   * Convert exprNodeDesc array to Typeinfo array. 
   */
  static ObjectInspector[] getStandardObjectInspector(ArrayList<TypeInfo> exprs) {
    ObjectInspector[] result = new ObjectInspector[exprs.size()];
    for (int i=0; i<exprs.size(); i++) {
      result[i] = TypeInfoUtils
          .getStandardWritableObjectInspectorFromTypeInfo(exprs.get(i));
    }
    return result;
  }
  
  /**
   * Returns the GenericUDAFEvaluator for the aggregation.
   * This is called once for each GroupBy aggregation.
   */
  static GenericUDAFEvaluator getGenericUDAFEvaluator(String aggName, 
      ArrayList<exprNodeDesc> aggParameters, 
      ASTNode aggTree) throws SemanticException {
    ArrayList<TypeInfo> originalParameterTypeInfos = getTypeInfo(aggParameters);
    GenericUDAFEvaluator result = FunctionRegistry.getGenericUDAFEvaluator(
        aggName, originalParameterTypeInfos);
    if (null == result) {
      String reason = "Looking for UDAF Evaluator\"" + aggName + "\" with parameters " 
          + originalParameterTypeInfos;
      throw new SemanticException(ErrorMsg.INVALID_FUNCTION_SIGNATURE.
          getMsg((ASTNode)aggTree.getChild(0), reason));
    }
    return result;
  }
  
  /**
   * Returns the GenericUDAFInfo struct for the aggregation.
   * @param aggName  The name of the UDAF.
   * @param aggParameters  The exprNodeDesc of the original parameters 
   * @param aggTree   The ASTNode node of the UDAF in the query.
   * @return GenericUDAFInfo 
   * @throws SemanticException when the UDAF is not found or has problems.
   */
  static GenericUDAFInfo getGenericUDAFInfo(GenericUDAFEvaluator evaluator, 
      GenericUDAFEvaluator.Mode emode, ArrayList<exprNodeDesc> aggParameters) 
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

  private static GenericUDAFEvaluator.Mode groupByDescModeToUDAFMode(groupByDesc.Mode mode, boolean isDistinct) {
    switch (mode) {
      case COMPLETE: return GenericUDAFEvaluator.Mode.COMPLETE;
      case PARTIAL1: return GenericUDAFEvaluator.Mode.PARTIAL1;
      case PARTIAL2: return GenericUDAFEvaluator.Mode.PARTIAL2;
      case PARTIALS: return isDistinct ? GenericUDAFEvaluator.Mode.PARTIAL1 : GenericUDAFEvaluator.Mode.PARTIAL2;
      case FINAL: return GenericUDAFEvaluator.Mode.FINAL;
      case HASH: return GenericUDAFEvaluator.Mode.PARTIAL1;
      case MERGEPARTIAL: return isDistinct ? GenericUDAFEvaluator.Mode.COMPLETE : GenericUDAFEvaluator.Mode.FINAL;
      default:
        throw new RuntimeException("internal error in groupByDescModeToUDAFMode");
    }
  }
  /**
   * Generate the GroupByOperator for the Query Block (parseInfo.getXXX(dest)).
   * The new GroupByOperator will be a child of the reduceSinkOperatorInfo.
   * 
   * @param mode The mode of the aggregation (PARTIAL1 or COMPLETE)
   * @param genericUDAFEvaluators  If not null, this function will store the mapping
   *            from Aggregation StringTree to the genericUDAFEvaluator in this parameter,
   *            so it can be used in the next-stage GroupBy aggregations. 
   * @return the new GroupByOperator
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanGroupByOperator(
        QBParseInfo parseInfo, String dest, Operator reduceSinkOperatorInfo,
        groupByDesc.Mode mode, Map<String, GenericUDAFEvaluator> genericUDAFEvaluators)
    throws SemanticException {
    RowResolver groupByInputRowResolver = opParseCtx.get(reduceSinkOperatorInfo).getRR();
    RowResolver groupByOutputRowResolver = new RowResolver();
    groupByOutputRowResolver.setIsExprResolver(true);
    ArrayList<exprNodeDesc> groupByKeys = new ArrayList<exprNodeDesc>();
    ArrayList<aggregationDesc> aggregations = new ArrayList<aggregationDesc>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      String text = grpbyExpr.toStringTree();
      ColumnInfo exprInfo = groupByInputRowResolver.get("",text);

      if (exprInfo == null) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(grpbyExpr));
      }

      groupByKeys.add(new exprNodeColumnDesc(exprInfo.getType(), 
                                             exprInfo.getInternalName(), "", false));
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      groupByOutputRowResolver.put("",grpbyExpr.toStringTree(),
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
      ArrayList<exprNodeDesc> aggParameters = new ArrayList<exprNodeDesc>();
      // 0 is the function name
      for (int i = 1; i < value.getChildCount(); i++) {
        String text = value.getChild(i).toStringTree();
        ASTNode paraExpr = (ASTNode)value.getChild(i);
        ColumnInfo paraExprInfo = groupByInputRowResolver.get("",text);
        if (paraExprInfo == null) {
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(paraExpr));
        }

        String paraExpression = paraExprInfo.getInternalName();
        assert(paraExpression != null);
        aggParameters.add(new exprNodeColumnDesc(paraExprInfo.getType(), 
                                                 paraExprInfo.getInternalName(),
                                                 paraExprInfo.getTabAlias(),
                                                 paraExprInfo.getIsPartitionCol()));
      }
      
      boolean isDistinct = value.getType() == HiveParser.TOK_FUNCTIONDI;
      Mode amode = groupByDescModeToUDAFMode(mode, isDistinct);
      GenericUDAFEvaluator genericUDAFEvaluator = getGenericUDAFEvaluator(aggName, aggParameters, value);
      assert(genericUDAFEvaluator != null);
      GenericUDAFInfo udaf = getGenericUDAFInfo(genericUDAFEvaluator, amode, aggParameters);
      aggregations.add(new aggregationDesc(aggName.toLowerCase(), udaf.genericUDAFEvaluator, udaf.convertedParameters,
          isDistinct, amode));
      String field = getColumnInternalName(groupByKeys.size() + aggregations.size() -1);
      outputColumnNames.add(field);
      groupByOutputRowResolver.put("",value.toStringTree(),
                                   new ColumnInfo(field,
                                       udaf.returnType, "", false));
      // Save the evaluator so that it can be used by the next-stage GroupByOperators 
      if (genericUDAFEvaluators != null) {
        genericUDAFEvaluators.put(entry.getKey(), genericUDAFEvaluator);
      }
    }

    Operator op =  
      putOpInsertMap(OperatorFactory.getAndMakeChild(new groupByDesc(mode, outputColumnNames, groupByKeys, aggregations, false),
                                                     new RowSchema(groupByOutputRowResolver.getColumnInfos()),
                                                     reduceSinkOperatorInfo),
        groupByOutputRowResolver
    );
    op.setColumnExprMap(colExprMap);
    return op;
  }

  /**
   * Generate the GroupByOperator for the Query Block (parseInfo.getXXX(dest)).
   * The new GroupByOperator will be a child of the reduceSinkOperatorInfo.
   * 
   * @param mode The mode of the aggregation (MERGEPARTIAL, PARTIAL2)
   * @param genericUDAFEvaluators  The mapping from Aggregation StringTree to the 
   *            genericUDAFEvaluator. 
   * @param distPartAggr partial aggregation for distincts
   * @return the new GroupByOperator
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanGroupByOperator1(
        QBParseInfo parseInfo, String dest, Operator reduceSinkOperatorInfo,
        groupByDesc.Mode mode, Map<String, GenericUDAFEvaluator> genericUDAFEvaluators, boolean distPartAgg)
    throws SemanticException {
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    RowResolver groupByInputRowResolver = opParseCtx.get(reduceSinkOperatorInfo).getRR();
    RowResolver groupByOutputRowResolver = new RowResolver();
    groupByOutputRowResolver.setIsExprResolver(true);
    ArrayList<exprNodeDesc> groupByKeys = new ArrayList<exprNodeDesc>();
    ArrayList<aggregationDesc> aggregations = new ArrayList<aggregationDesc>();
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      String text = grpbyExpr.toStringTree();
      ColumnInfo exprInfo = groupByInputRowResolver.get("",text);

      if (exprInfo == null) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(grpbyExpr));
      }

      groupByKeys.add(new exprNodeColumnDesc(exprInfo.getType(), 
                                             exprInfo.getInternalName(),
                                             exprInfo.getTabAlias(),
                                             exprInfo.getIsPartitionCol()));
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      groupByOutputRowResolver.put("",grpbyExpr.toStringTree(),
                                   new ColumnInfo(field, exprInfo.getType(), "", false));
      colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
    }

    HashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();
      String aggName = value.getChild(0).getText();
      ArrayList<exprNodeDesc> aggParameters = new ArrayList<exprNodeDesc>();

      // If the function is distinct, partial aggregartion has not been done on the client side. 
      // If distPartAgg is set, the client is letting us know that partial aggregation has not been done.
      // For eg: select a, count(b+c), count(distinct d+e) group by a
      // For count(b+c), if partial aggregation has been performed, then we directly look for count(b+c).
      // Otherwise, we look for b+c.
      // For distincts, partial aggregation is never performed on the client side, so always look for the parameters: d+e
      boolean partialAggDone = !(distPartAgg || (value.getToken().getType() == HiveParser.TOK_FUNCTIONDI));
      if (!partialAggDone) {
        // 0 is the function name
        for (int i = 1; i < value.getChildCount(); i++) {
          String text = value.getChild(i).toStringTree();
          ASTNode paraExpr = (ASTNode)value.getChild(i);
          ColumnInfo paraExprInfo = groupByInputRowResolver.get("",text);
          if (paraExprInfo == null) {
            throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(paraExpr));
          }

          String paraExpression = paraExprInfo.getInternalName();
          assert(paraExpression != null);
          aggParameters.add(new exprNodeColumnDesc(paraExprInfo.getType(), 
                                                   paraExprInfo.getInternalName(),
                                                   paraExprInfo.getTabAlias(),
                                                   paraExprInfo.getIsPartitionCol()));
        }
      }
      else {
        String text = entry.getKey();
        ColumnInfo paraExprInfo = groupByInputRowResolver.get("",text);
        if (paraExprInfo == null) {
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(value));
        }
        String paraExpression = paraExprInfo.getInternalName();
        assert(paraExpression != null);
        aggParameters.add(new exprNodeColumnDesc(paraExprInfo.getType(), paraExpression,
                                                 paraExprInfo.getTabAlias(),
                                                 paraExprInfo.getIsPartitionCol()));
      }
      boolean isDistinct = (value.getType() == HiveParser.TOK_FUNCTIONDI);
      Mode amode = groupByDescModeToUDAFMode(mode, isDistinct); 
      GenericUDAFEvaluator genericUDAFEvaluator = null;
      // For distincts, partial aggregations have not been done
      if (distPartAgg) {
        genericUDAFEvaluator = getGenericUDAFEvaluator(aggName, aggParameters, value);
        assert(genericUDAFEvaluator != null);
        genericUDAFEvaluators.put(entry.getKey(), genericUDAFEvaluator);
      }
      else {
        genericUDAFEvaluator = genericUDAFEvaluators.get(entry.getKey());
        assert(genericUDAFEvaluator != null);
      }
      
      GenericUDAFInfo udaf = getGenericUDAFInfo(genericUDAFEvaluator, amode, aggParameters);
      aggregations.add(new aggregationDesc(aggName.toLowerCase(), udaf.genericUDAFEvaluator, udaf.convertedParameters, 
          (mode != groupByDesc.Mode.FINAL && isDistinct), amode));
      String field = getColumnInternalName(groupByKeys.size() + aggregations.size() - 1);
      outputColumnNames.add(field);
      groupByOutputRowResolver.put("", value.toStringTree(),
                                    new ColumnInfo(field,
                                        udaf.returnType, "", false));
    }

    Operator op = putOpInsertMap(
                                  OperatorFactory.getAndMakeChild(new groupByDesc(mode, outputColumnNames, groupByKeys, aggregations, distPartAgg),
                                  new RowSchema(groupByOutputRowResolver.getColumnInfos()),
                                  reduceSinkOperatorInfo),
        groupByOutputRowResolver);
    op.setColumnExprMap(colExprMap);
    return op;
  }

  /**
   * Generate the map-side GroupByOperator for the Query Block (qb.getParseInfo().getXXX(dest)).
   * The new GroupByOperator will be a child of the inputOperatorInfo.
   * 
   * @param mode The mode of the aggregation (HASH)
   * @param genericUDAFEvaluators  If not null, this function will store the mapping
   *            from Aggregation StringTree to the genericUDAFEvaluator in this parameter,
   *            so it can be used in the next-stage GroupBy aggregations. 
   * @return the new GroupByOperator
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanMapGroupByOperator(QB qb, String dest, Operator inputOperatorInfo, 
        groupByDesc.Mode mode, Map<String, GenericUDAFEvaluator> genericUDAFEvaluators) throws SemanticException {

    RowResolver groupByInputRowResolver = opParseCtx.get(inputOperatorInfo).getRR();
    QBParseInfo parseInfo = qb.getParseInfo();
    RowResolver groupByOutputRowResolver = new RowResolver();
    groupByOutputRowResolver.setIsExprResolver(true);
    ArrayList<exprNodeDesc> groupByKeys = new ArrayList<exprNodeDesc>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    ArrayList<aggregationDesc> aggregations = new ArrayList<aggregationDesc>();
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      exprNodeDesc grpByExprNode = genExprNodeDesc(grpbyExpr, groupByInputRowResolver);

      groupByKeys.add(grpByExprNode);
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      groupByOutputRowResolver.put("",grpbyExpr.toStringTree(),
                                   new ColumnInfo(field, grpByExprNode.getTypeInfo(), "", false));
      colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
    }

    // If there is a distinctFuncExp, add all parameters to the reduceKeys.
    if (parseInfo.getDistinctFuncExprForClause(dest) != null) {
      ASTNode value = parseInfo.getDistinctFuncExprForClause(dest);
      int numDistn=0;
      // 0 is function name
      for (int i = 1; i < value.getChildCount(); i++) {
        ASTNode parameter = (ASTNode) value.getChild(i);
        String text = parameter.toStringTree();
        if (groupByOutputRowResolver.get("",text) == null) {
          exprNodeDesc distExprNode = genExprNodeDesc(parameter, groupByInputRowResolver);
          groupByKeys.add(distExprNode);
          numDistn++;
          String field = getColumnInternalName(grpByExprs.size() + numDistn -1);
          outputColumnNames.add(field);
          groupByOutputRowResolver.put("", text, new ColumnInfo(field, distExprNode.getTypeInfo(), "", false));
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
      String aggName = value.getChild(0).getText();
      ArrayList<exprNodeDesc> aggParameters = new ArrayList<exprNodeDesc>();
      ArrayList<Class<?>> aggClasses = new ArrayList<Class<?>>();
      // 0 is the function name
      for (int i = 1; i < value.getChildCount(); i++) {
        ASTNode paraExpr = (ASTNode)value.getChild(i);
        exprNodeDesc paraExprNode = genExprNodeDesc(paraExpr, groupByInputRowResolver);

        aggParameters.add(paraExprNode);
      }

      boolean isDistinct = value.getType() == HiveParser.TOK_FUNCTIONDI;
      Mode amode = groupByDescModeToUDAFMode(mode, isDistinct);

      GenericUDAFEvaluator genericUDAFEvaluator = getGenericUDAFEvaluator(aggName, aggParameters, value);
      assert(genericUDAFEvaluator != null);
      GenericUDAFInfo udaf = getGenericUDAFInfo(genericUDAFEvaluator, amode, aggParameters);
      aggregations.add(new aggregationDesc(aggName.toLowerCase(), udaf.genericUDAFEvaluator, udaf.convertedParameters,
          isDistinct, amode));
      String field = getColumnInternalName(groupByKeys.size() + aggregations.size() -1);
      outputColumnNames.add(field);
      groupByOutputRowResolver.put("",value.toStringTree(),
                                   new ColumnInfo(field,
                                       udaf.returnType, "", false));
      // Save the evaluator so that it can be used by the next-stage GroupByOperators 
      if (genericUDAFEvaluators != null) {
        genericUDAFEvaluators.put(entry.getKey(), genericUDAFEvaluator);
      }
    }

    Operator op = putOpInsertMap(
                                 OperatorFactory.getAndMakeChild(new groupByDesc(mode, outputColumnNames, groupByKeys, aggregations, false),
                                 new RowSchema(groupByOutputRowResolver.getColumnInfos()),
                                 inputOperatorInfo),
      groupByOutputRowResolver);
    op.setColumnExprMap(colExprMap);
    return op;
  }


  /**
   * Generate the ReduceSinkOperator for the Group By Query Block (qb.getPartInfo().getXXX(dest)).
   * The new ReduceSinkOperator will be a child of inputOperatorInfo.
   * 
   * It will put all Group By keys and the distinct field (if any) in the map-reduce sort key,
   * and all other fields in the map-reduce value.
   * 
   * @param numPartitionFields  the number of fields for map-reduce partitioning.
   *      This is usually the number of fields in the Group By keys.
   * @return the new ReduceSinkOperator.
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanReduceSinkOperator(QB qb,
    String dest, Operator inputOperatorInfo, int numPartitionFields, int numReducers, boolean mapAggrDone) throws SemanticException {

    RowResolver reduceSinkInputRowResolver = opParseCtx.get(inputOperatorInfo).getRR();
    QBParseInfo parseInfo = qb.getParseInfo();
    RowResolver reduceSinkOutputRowResolver = new RowResolver();
    reduceSinkOutputRowResolver.setIsExprResolver(true);
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    ArrayList<exprNodeDesc> reduceKeys = new ArrayList<exprNodeDesc>();
    // Pre-compute group-by keys and store in reduceKeys

    List<String> outputColumnNames = new ArrayList<String>();
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      exprNodeDesc inputExpr = genExprNodeDesc(grpbyExpr, reduceSinkInputRowResolver);
      reduceKeys.add(inputExpr);
      String text = grpbyExpr.toStringTree();
      if (reduceSinkOutputRowResolver.get("", text) == null) {
        outputColumnNames.add(getColumnInternalName(reduceKeys.size() - 1));
        String field = Utilities.ReduceField.KEY.toString() + "." + getColumnInternalName(reduceKeys.size() - 1);
        ColumnInfo colInfo = new ColumnInfo(field,
            reduceKeys.get(reduceKeys.size()-1).getTypeInfo(), null, false);
        reduceSinkOutputRowResolver.put("", text, colInfo);
        colExprMap.put(colInfo.getInternalName(), inputExpr);
      } else {
        throw new SemanticException(ErrorMsg.DUPLICATE_GROUPBY_KEY.getMsg(grpbyExpr));
      }
    }

    // If there is a distinctFuncExp, add all parameters to the reduceKeys.
    if (parseInfo.getDistinctFuncExprForClause(dest) != null) {
      ASTNode value = parseInfo.getDistinctFuncExprForClause(dest);
      // 0 is function name
      for (int i = 1; i < value.getChildCount(); i++) {
        ASTNode parameter = (ASTNode) value.getChild(i);
        String text = parameter.toStringTree();
        if (reduceSinkOutputRowResolver.get("",text) == null) {
          reduceKeys.add(genExprNodeDesc(parameter, reduceSinkInputRowResolver));
          outputColumnNames.add(getColumnInternalName(reduceKeys.size() - 1));
          String field = Utilities.ReduceField.KEY.toString() + "." + getColumnInternalName(reduceKeys.size() - 1);
          ColumnInfo colInfo = new ColumnInfo(field,
              reduceKeys.get(reduceKeys.size()-1).getTypeInfo(), null, false);
          reduceSinkOutputRowResolver.put("", text, colInfo);
          colExprMap.put(colInfo.getInternalName(), reduceKeys.get(reduceKeys.size()-1));
        }
      }
    }

    ArrayList<exprNodeDesc> reduceValues = new ArrayList<exprNodeDesc>();
    HashMap<String, ASTNode> aggregationTrees = parseInfo.getAggregationExprsForClause(dest);

    if (!mapAggrDone) {
      // Put parameters to aggregations in reduceValues
      for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
        ASTNode value = entry.getValue();
        // 0 is function name
        for (int i = 1; i < value.getChildCount(); i++) {
          ASTNode parameter = (ASTNode) value.getChild(i);
          String text = parameter.toStringTree();
          if (reduceSinkOutputRowResolver.get("",text) == null) {
            reduceValues.add(genExprNodeDesc(parameter, reduceSinkInputRowResolver));
            outputColumnNames.add(getColumnInternalName(reduceValues.size() - 1));
            String field = Utilities.ReduceField.VALUE.toString() + "." + getColumnInternalName(reduceValues.size() - 1);
            reduceSinkOutputRowResolver.put("", text,
                                            new ColumnInfo(field,
                                                           reduceValues.get(reduceValues.size()-1).getTypeInfo(),
                                                           null, false));
          }
        }
      }
    }
    else
    {
      // Put partial aggregation results in reduceValues
      int inputField = reduceKeys.size();
      
      for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
        
        TypeInfo type = reduceSinkInputRowResolver.getColumnInfos().get(inputField).getType(); 
        reduceValues.add(new exprNodeColumnDesc(type, getColumnInternalName(inputField),
                                                "", false));
        inputField++;
        outputColumnNames.add(getColumnInternalName(reduceValues.size() - 1));
        String field = Utilities.ReduceField.VALUE.toString() + "." + getColumnInternalName(reduceValues.size() - 1);
        reduceSinkOutputRowResolver.put("", ((ASTNode)entry.getValue()).toStringTree(),
                                        new ColumnInfo(field,
                                                       type, null, false));
      }
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator)  putOpInsertMap(
      OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(reduceKeys, reduceValues, outputColumnNames, true, -1, numPartitionFields,
                                                                  numReducers),
                                        new RowSchema(reduceSinkOutputRowResolver.getColumnInfos()),
                                        inputOperatorInfo),
        reduceSinkOutputRowResolver
    );
    rsOp.setColumnExprMap(colExprMap);
    return rsOp;
  }

  /**
   * Generate the second ReduceSinkOperator for the Group By Plan (parseInfo.getXXX(dest)).
   * The new ReduceSinkOperator will be a child of groupByOperatorInfo.
   * 
   * The second ReduceSinkOperator will put the group by keys in the map-reduce sort
   * key, and put the partial aggregation results in the map-reduce value. 
   *  
   * @param numPartitionFields the number of fields in the map-reduce partition key.
   *     This should always be the same as the number of Group By keys.  We should be 
   *     able to remove this parameter since in this phase there is no distinct any more.  
   * @return the new ReduceSinkOperator.
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanReduceSinkOperator2MR(
    QBParseInfo parseInfo, String dest, Operator groupByOperatorInfo, int numPartitionFields, int numReducers)
    throws SemanticException {
    RowResolver reduceSinkInputRowResolver2 = opParseCtx.get(groupByOperatorInfo).getRR();
    RowResolver reduceSinkOutputRowResolver2 = new RowResolver();
    reduceSinkOutputRowResolver2.setIsExprResolver(true);
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    ArrayList<exprNodeDesc> reduceKeys = new ArrayList<exprNodeDesc>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    // Get group-by keys and store in reduceKeys
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      TypeInfo typeInfo = reduceSinkInputRowResolver2.get("", grpbyExpr.toStringTree()).getType();
      exprNodeColumnDesc inputExpr = new exprNodeColumnDesc(typeInfo, field, "", false);
      reduceKeys.add(inputExpr);
      ColumnInfo colInfo = new ColumnInfo(Utilities.ReduceField.KEY.toString() + "." + field,
           typeInfo, "", false);
      reduceSinkOutputRowResolver2.put("", grpbyExpr.toStringTree(),
                                       colInfo);
      colExprMap.put(colInfo.getInternalName(), inputExpr);
    }
    // Get partial aggregation results and store in reduceValues
    ArrayList<exprNodeDesc> reduceValues = new ArrayList<exprNodeDesc>();
    int inputField = reduceKeys.size();
    HashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      String field = getColumnInternalName(inputField);
      ASTNode t = entry.getValue();
      TypeInfo typeInfo = reduceSinkInputRowResolver2.get("", t.toStringTree()).getType();
      reduceValues.add(new exprNodeColumnDesc(typeInfo, field, "", false));
      inputField++;
      String col = getColumnInternalName(reduceValues.size()-1);
      outputColumnNames.add(col);
      reduceSinkOutputRowResolver2.put("", t.toStringTree(),
                                       new ColumnInfo(Utilities.ReduceField.VALUE.toString() + "." + col,
                                           typeInfo, "", false));
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
      OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(reduceKeys, reduceValues, outputColumnNames, true, -1, 
                                                                  numPartitionFields, numReducers),
                                        new RowSchema(reduceSinkOutputRowResolver2.getColumnInfos()),
                                        groupByOperatorInfo),
        reduceSinkOutputRowResolver2
    );
    
    rsOp.setColumnExprMap(colExprMap);
    return rsOp;
  }

  /**
   * Generate the second GroupByOperator for the Group By Plan (parseInfo.getXXX(dest)).
   * The new GroupByOperator will do the second aggregation based on the partial aggregation 
   * results.
   * 
   * @param mode the mode of aggregation (FINAL)  
   * @param genericUDAFEvaluators  The mapping from Aggregation StringTree to the 
   *            genericUDAFEvaluator. 
   * @return the new GroupByOperator
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanGroupByOperator2MR(
        QBParseInfo parseInfo, String dest, Operator reduceSinkOperatorInfo2, 
        groupByDesc.Mode mode, Map<String, GenericUDAFEvaluator> genericUDAFEvaluators)
        throws SemanticException {
    RowResolver groupByInputRowResolver2 = opParseCtx.get(reduceSinkOperatorInfo2).getRR();
    RowResolver groupByOutputRowResolver2 = new RowResolver();
    groupByOutputRowResolver2.setIsExprResolver(true);
    ArrayList<exprNodeDesc> groupByKeys = new ArrayList<exprNodeDesc>();
    ArrayList<aggregationDesc> aggregations = new ArrayList<aggregationDesc>();
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    ArrayList<String> outputColumnNames = new ArrayList<String>(); 
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      String text = grpbyExpr.toStringTree();
      ColumnInfo exprInfo = groupByInputRowResolver2.get("",text);
      if (exprInfo == null) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(grpbyExpr));
      }

      String expression = exprInfo.getInternalName();
      groupByKeys.add(new exprNodeColumnDesc(exprInfo.getType(), expression,
                                             exprInfo.getTabAlias(),
                                             exprInfo.getIsPartitionCol()));
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      groupByOutputRowResolver2.put("",grpbyExpr.toStringTree(),
                                    new ColumnInfo(field, exprInfo.getType(), "", false));
      colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
    }
    HashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ArrayList<exprNodeDesc> aggParameters = new ArrayList<exprNodeDesc>();
      ASTNode value = entry.getValue();
      String text = entry.getKey();
      ColumnInfo paraExprInfo = groupByInputRowResolver2.get("",text);
      if (paraExprInfo == null) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(value));
      }
      String paraExpression = paraExprInfo.getInternalName();
      assert(paraExpression != null);
      aggParameters.add(new exprNodeColumnDesc(paraExprInfo.getType(), paraExpression,
                                               paraExprInfo.getTabAlias(),
                                               paraExprInfo.getIsPartitionCol()));

      String aggName = value.getChild(0).getText();

      boolean isDistinct = value.getType() == HiveParser.TOK_FUNCTIONDI;
      Mode amode = groupByDescModeToUDAFMode(mode, isDistinct); 
      GenericUDAFEvaluator genericUDAFEvaluator = genericUDAFEvaluators.get(entry.getKey());
      assert(genericUDAFEvaluator != null);
      GenericUDAFInfo udaf = getGenericUDAFInfo(genericUDAFEvaluator, amode, aggParameters);
      aggregations.add(new aggregationDesc(aggName.toLowerCase(), udaf.genericUDAFEvaluator, udaf.convertedParameters, 
                                           (mode != groupByDesc.Mode.FINAL && value.getToken().getType() == HiveParser.TOK_FUNCTIONDI),
                                           amode));
      String field = getColumnInternalName(groupByKeys.size() + aggregations.size() - 1);
      outputColumnNames.add(field);
      groupByOutputRowResolver2.put("", value.toStringTree(),
                                    new ColumnInfo(field,
                                        udaf.returnType, "", false));
    }

    Operator op = putOpInsertMap(
      OperatorFactory.getAndMakeChild(new groupByDesc(mode, outputColumnNames, groupByKeys, aggregations, false),
                                      new RowSchema(groupByOutputRowResolver2.getColumnInfos()),
                                      reduceSinkOperatorInfo2),
        groupByOutputRowResolver2
    );
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
   * selects)
   *
   * @param dest
   * @param qb
   * @param input
   * @return
   * @throws SemanticException
   *
   * Generate a Group-By plan using 1 map-reduce job. 
   * Spray by the group by key, and sort by the distinct key (if any), and 
   * compute aggregates   * 
   * The agggregation evaluation functions are as follows:
   *   Partitioning Key: 
   *      grouping key
   *
   *   Sorting Key: 
   *      grouping key if no DISTINCT
   *      grouping + distinct key if DISTINCT
   * 
   *   Reducer: iterate/merge
   *   (mode = COMPLETE)
   **/
  @SuppressWarnings({ "unused", "nls" })
  private Operator genGroupByPlan1MR(String dest, QB qb,
      Operator input) throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    int numReducers = -1;
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    if (grpByExprs.isEmpty())
      numReducers = 1;

    // ////// 1. Generate ReduceSinkOperator
    Operator reduceSinkOperatorInfo = genGroupByPlanReduceSinkOperator(
      qb, dest, input, grpByExprs.size(), numReducers, false);

    // ////// 2. Generate GroupbyOperator
    Operator groupByOperatorInfo = genGroupByPlanGroupByOperator(parseInfo,
        dest, reduceSinkOperatorInfo, groupByDesc.Mode.COMPLETE, null);

    return groupByOperatorInfo;
  }

  static ArrayList<GenericUDAFEvaluator> getUDAFEvaluators(ArrayList<aggregationDesc> aggs) {
    ArrayList<GenericUDAFEvaluator> result = new ArrayList<GenericUDAFEvaluator>();
    for (int i=0; i<aggs.size(); i++) {
      result.add(aggs.get(i).createGenericUDAFEvaluator());
    }
    return result;
  }
  
  /**
   * Generate a Multi Group-By plan using a 2 map-reduce jobs.
   * @param dest
   * @param qb
   * @param input
   * @return
   * @throws SemanticException
   *
   * Generate a Group-By plan using a 2 map-reduce jobs. 
   * Spray by the distinct key in hope of getting a uniform distribution, and compute partial aggregates 
   * by the grouping key.
   * Evaluate partial aggregates first, and spray by the grouping key to compute actual
   * aggregates in the second phase.
   * The agggregation evaluation functions are as follows:
   *   Partitioning Key: 
   *     distinct key
   *
   *   Sorting Key: 
   *     distinct key
   * 
   *   Reducer: iterate/terminatePartial
   *   (mode = PARTIAL1)
   * 
   *   STAGE 2
   *
   *   Partitioning Key: 
   *      grouping key 
   *
   *   Sorting Key: 
   *      grouping key 
   *
   *   Reducer: merge/terminate
   *   (mode = FINAL)
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlan2MRMultiGroupBy(String dest, QB qb,
      Operator input) throws SemanticException {

    // ////// Generate GroupbyOperator for a map-side partial aggregation
    Map<String, GenericUDAFEvaluator> genericUDAFEvaluators = 
      new LinkedHashMap<String, GenericUDAFEvaluator>();

    QBParseInfo parseInfo = qb.getParseInfo();

    // ////// 2. Generate GroupbyOperator
    Operator groupByOperatorInfo = 
      genGroupByPlanGroupByOperator1(parseInfo, dest, input, groupByDesc.Mode.HASH, genericUDAFEvaluators, true);

    int numReducers = -1;
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);

    // ////// 3. Generate ReduceSinkOperator2
    Operator reduceSinkOperatorInfo2 = genGroupByPlanReduceSinkOperator2MR(
      parseInfo, dest, groupByOperatorInfo, grpByExprs.size(), numReducers);

    // ////// 4. Generate GroupbyOperator2
    Operator groupByOperatorInfo2 = 
      genGroupByPlanGroupByOperator2MR(parseInfo, dest, reduceSinkOperatorInfo2, groupByDesc.Mode.FINAL, genericUDAFEvaluators);

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
   * sum(VALUE.2))) Select (final selects)
   *
   * @param dest
   * @param qb
   * @param input
   * @return
   * @throws SemanticException
   *
   * Generate a Group-By plan using a 2 map-reduce jobs. 
   * Spray by the grouping key and distinct key (or a random number, if no distinct is 
   * present) in hope of getting a uniform distribution, and compute partial aggregates 
   * grouped by the reduction key (grouping key + distinct key).
   * Evaluate partial aggregates first, and spray by the grouping key to compute actual
   * aggregates in the second phase.
   * The agggregation evaluation functions are as follows:
   *   Partitioning Key: 
   *      random() if no DISTINCT
   *      grouping + distinct key if DISTINCT
   *
   *   Sorting Key: 
   *      grouping key if no DISTINCT
   *      grouping + distinct key if DISTINCT
   * 
   *   Reducer: iterate/terminatePartial
   *   (mode = PARTIAL1)
   * 
   *   STAGE 2
   *
   *   Partitioning Key: 
   *      grouping key 
   *
   *   Sorting Key: 
   *      grouping key if no DISTINCT
   *      grouping + distinct key if DISTINCT
   *
   *   Reducer: merge/terminate
   *   (mode = FINAL)
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlan2MR(String dest, QB qb,
      Operator input) throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    // ////// 1. Generate ReduceSinkOperator
    // There is a special case when we want the rows to be randomly distributed to  
    // reducers for load balancing problem.  That happens when there is no DISTINCT
    // operator.  We set the numPartitionColumns to -1 for this purpose.  This is 
    // captured by WritableComparableHiveObject.hashCode() function. 
    Operator reduceSinkOperatorInfo = genGroupByPlanReduceSinkOperator(
      qb, dest, input, (parseInfo.getDistinctFuncExprForClause(dest) == null ? -1
                        : Integer.MAX_VALUE), -1, false);

    // ////// 2. Generate GroupbyOperator
    Map<String, GenericUDAFEvaluator> genericUDAFEvaluators = 
      new LinkedHashMap<String, GenericUDAFEvaluator>();
    GroupByOperator groupByOperatorInfo = (GroupByOperator)genGroupByPlanGroupByOperator(parseInfo,
        dest, reduceSinkOperatorInfo, groupByDesc.Mode.PARTIAL1, genericUDAFEvaluators);
    
    int numReducers = -1;
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    if (grpByExprs.isEmpty())
      numReducers = 1;

    // ////// 3. Generate ReduceSinkOperator2
    Operator reduceSinkOperatorInfo2 = genGroupByPlanReduceSinkOperator2MR(
      parseInfo, dest, groupByOperatorInfo, grpByExprs.size(), numReducers);

    // ////// 4. Generate GroupbyOperator2
    Operator groupByOperatorInfo2 = 
      genGroupByPlanGroupByOperator2MR(parseInfo, dest, reduceSinkOperatorInfo2, 
          groupByDesc.Mode.FINAL, genericUDAFEvaluators);

    return groupByOperatorInfo2;
  }

  private boolean optimizeMapAggrGroupBy(String dest, QB qb) {
    List<ASTNode> grpByExprs = getGroupByForClause(qb.getParseInfo(), dest);
    if ((grpByExprs != null) && !grpByExprs.isEmpty())
      return false;

    if (qb.getParseInfo().getDistinctFuncExprForClause(dest) != null)
      return false;

    return true;
  }

  /**
   * Generate a Group-By plan using 1 map-reduce job. 
   * First perform a map-side partial aggregation (to reduce the amount of data), at this
   * point of time, we may turn off map-side partial aggregation based on its performance. 
   * Then spray by the group by key, and sort by the distinct key (if any), and 
   * compute aggregates based on actual aggregates
   * 
   * The agggregation evaluation functions are as follows:
   *   Mapper: iterate/terminatePartial      
   *   (mode = HASH)
   *
   *   Partitioning Key: 
   *      grouping key
   *
   *   Sorting Key: 
   *      grouping key if no DISTINCT
   *      grouping + distinct key if DISTINCT
   * 
   *   Reducer: iterate/terminate if DISTINCT
   *            merge/terminate if NO DISTINCT
   *   (mode = MERGEPARTIAL)
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanMapAggr1MR(String dest, QB qb, 
    Operator inputOperatorInfo) throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    // ////// Generate GroupbyOperator for a map-side partial aggregation
    Map<String, GenericUDAFEvaluator> genericUDAFEvaluators = 
      new LinkedHashMap<String, GenericUDAFEvaluator>();
    GroupByOperator groupByOperatorInfo = (GroupByOperator)genGroupByPlanMapGroupByOperator(qb,
      dest, inputOperatorInfo, groupByDesc.Mode.HASH, genericUDAFEvaluators);

    int numReducers = -1;

    // Optimize the scenario when there are no grouping keys - only 1 reducer is needed
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    if (grpByExprs.isEmpty())
      numReducers = 1;
    
    // ////// Generate ReduceSink Operator
    Operator reduceSinkOperatorInfo = 
      genGroupByPlanReduceSinkOperator(qb, dest, groupByOperatorInfo, 
          grpByExprs.size(), numReducers, true);
    
    // This is a 1-stage map-reduce processing of the groupby. Tha map-side aggregates was just used to
    // reduce output data. In case of distincts, partial results are not used, and so iterate is again
    // invoked on the reducer. In case of non-distincts, partial results are used, and merge is invoked
    // on the reducer.
    return genGroupByPlanGroupByOperator1(parseInfo, dest, 
        reduceSinkOperatorInfo, groupByDesc.Mode.MERGEPARTIAL,
                                          genericUDAFEvaluators, false);
  }

  /**
   * Generate a Group-By plan using a 2 map-reduce jobs. 
   * However, only 1 group-by plan is generated if the query involves no grouping key and
   * no distincts. In that case, the plan is same as generated by genGroupByPlanMapAggr1MR.
   * Otherwise, the following plan is generated:
   * First perform a map side partial aggregation (to reduce the amount of data). Then 
   * spray by the grouping key and distinct key (or a random number, if no distinct is 
   * present) in hope of getting a uniform distribution, and compute partial aggregates 
   * grouped by the reduction key (grouping key + distinct key).
   * Evaluate partial aggregates first, and spray by the grouping key to compute actual
   * aggregates in the second phase.
   * The agggregation evaluation functions are as follows:
   *   Mapper: iterate/terminatePartial      
   *   (mode = HASH)
   * 
   *   Partitioning Key: 
   *      random() if no DISTINCT
   *      grouping + distinct key if DISTINCT
   *
   *   Sorting Key: 
   *      grouping key if no DISTINCT
   *      grouping + distinct key if DISTINCT
   * 
   *   Reducer: iterate/terminatePartial if DISTINCT
   *            merge/terminatePartial if NO DISTINCT
   *   (mode = MERGEPARTIAL)
   * 
   *   STAGE 2
   *
   *   Partitioining Key: 
   *      grouping key 
   *
   *   Sorting Key: 
   *      grouping key if no DISTINCT
   *      grouping + distinct key if DISTINCT
   *
   *   Reducer: merge/terminate
   *   (mode = FINAL)
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanMapAggr2MR(String dest, QB qb, 
    Operator inputOperatorInfo) throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    // ////// Generate GroupbyOperator for a map-side partial aggregation
    Map<String, GenericUDAFEvaluator> genericUDAFEvaluators = 
      new LinkedHashMap<String, GenericUDAFEvaluator>();
    GroupByOperator groupByOperatorInfo = (GroupByOperator)genGroupByPlanMapGroupByOperator(qb,
      dest, inputOperatorInfo, groupByDesc.Mode.HASH, genericUDAFEvaluators);
    
    // Optimize the scenario when there are no grouping keys and no distinct - 2 map-reduce jobs are not needed
    // For eg: select count(1) from T where t.ds = ....
    if (!optimizeMapAggrGroupBy(dest, qb)) {

      // ////// Generate ReduceSink Operator
      Operator reduceSinkOperatorInfo = 
        genGroupByPlanReduceSinkOperator(qb, dest, groupByOperatorInfo, 
                                         (parseInfo.getDistinctFuncExprForClause(dest) == null ? -1
                                          : Integer.MAX_VALUE), -1, true);
      
      // ////// Generate GroupbyOperator for a partial aggregation
      Operator groupByOperatorInfo2 = genGroupByPlanGroupByOperator1(parseInfo,
          dest, reduceSinkOperatorInfo, groupByDesc.Mode.PARTIALS,
                                                                     genericUDAFEvaluators, false);

      int numReducers = -1;
      List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
      if (grpByExprs.isEmpty())
        numReducers = 1;
      
      // //////  Generate ReduceSinkOperator2
      Operator reduceSinkOperatorInfo2 = genGroupByPlanReduceSinkOperator2MR(parseInfo, dest, groupByOperatorInfo2,
                                                                             grpByExprs.size(), numReducers);

      // ////// Generate GroupbyOperator3
      return genGroupByPlanGroupByOperator2MR(parseInfo, dest, reduceSinkOperatorInfo2, groupByDesc.Mode.FINAL, genericUDAFEvaluators);
    }
    else {
      // ////// Generate ReduceSink Operator
      Operator reduceSinkOperatorInfo = 
        genGroupByPlanReduceSinkOperator(qb, dest, groupByOperatorInfo, getGroupByForClause(parseInfo, dest).size(), 1, true);
      
      return genGroupByPlanGroupByOperator2MR(parseInfo, dest, reduceSinkOperatorInfo, groupByDesc.Mode.FINAL, genericUDAFEvaluators);
    }
  }

  @SuppressWarnings("nls")
  private Operator genConversionOps(String dest, QB qb,
      Operator input) throws SemanticException {

    Integer dest_type = qb.getMetaData().getDestTypeForAlias(dest);
    Table dest_tab = null;
    switch (dest_type.intValue()) {
    case QBMetaData.DEST_TABLE:
      {
        dest_tab = qb.getMetaData().getDestTableForAlias(dest);
        break;
      }
    case QBMetaData.DEST_PARTITION:
      {
        dest_tab = qb.getMetaData().getDestPartitionForAlias(dest).getTable();
        break;
      }
    default:
      {
        return input;
      }
    }

    return input;
  }

  @SuppressWarnings("nls")
  private Operator genFileSinkPlan(String dest, QB qb,
      Operator input) throws SemanticException {

    RowResolver inputRR = opParseCtx.get(input).getRR();
    QBMetaData qbm = qb.getMetaData();
    Integer dest_type = qbm.getDestTypeForAlias(dest);
    
    Table dest_tab;     // destination table if any
    String queryTmpdir; // the intermediate destination directory
    Path dest_path;     // the final destination directory
    tableDesc table_desc = null;
    int currentTableId = 0;
    boolean isLocal = false;

    switch (dest_type.intValue()) {
    case QBMetaData.DEST_TABLE: {

        dest_tab = qbm.getDestTableForAlias(dest);
        //check for partition
        List<FieldSchema> parts = dest_tab.getTTable().getPartitionKeys();
        if(parts != null && parts.size() > 0) {
          throw new SemanticException(ErrorMsg.NEED_PARTITION_ERROR.getMsg());
        }
        dest_path = dest_tab.getPath();
        queryTmpdir = ctx.getExternalTmpFileURI(dest_path.toUri());
        table_desc = Utilities.getTableDesc(dest_tab);
        
        this.idToTableNameMap.put( String.valueOf(this.destTableId), dest_tab.getName());
        currentTableId = this.destTableId;
        this.destTableId ++;

        // Create the work for moving the table
        this.loadTableWork.add
          (new loadTableDesc(queryTmpdir,
                             ctx.getExternalTmpFileURI(dest_path.toUri()),
                             table_desc,
                             new HashMap<String, String>()));
        outputs.add(new WriteEntity(dest_tab));
        break;
      }
    case QBMetaData.DEST_PARTITION: {

        Partition dest_part = qbm.getDestPartitionForAlias(dest);
        dest_tab = dest_part.getTable();
        dest_path = dest_part.getPath()[0];
        queryTmpdir = ctx.getExternalTmpFileURI(dest_path.toUri());
        table_desc = Utilities.getTableDesc(dest_tab);

        this.idToTableNameMap.put(String.valueOf(this.destTableId), dest_tab.getName());
        currentTableId = this.destTableId;
        this.destTableId ++;
        
        this.loadTableWork.add
          (new loadTableDesc(queryTmpdir,
                             ctx.getExternalTmpFileURI(dest_path.toUri()),
                             table_desc, dest_part.getSpec()));
        outputs.add(new WriteEntity(dest_part));
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
        Vector<ColumnInfo> colInfos = inputRR.getColumnInfos();
    
        boolean first = true;
        for (ColumnInfo colInfo:colInfos) {
          String[] nm = inputRR.reverseLookup(colInfo.getInternalName());
          if (!first) {
            cols = cols.concat(",");
            colTypes = colTypes.concat(":");
          }
          
          first = false;
          cols = cols.concat(colInfo.getInternalName());
          
          // Replace VOID type with string when the output is a temp table or local files.
          // A VOID type can be generated under the query:
          // 
          //     select NULL from tt; 
          // or
          //     insert overwrite local directory "abc" select NULL from tt;
          // 
          // where there is no column type to which the NULL value should be converted.
          // 
          String tName = colInfo.getType().getTypeName();
          if ( tName.equals(Constants.VOID_TYPE_NAME) )
            colTypes = colTypes.concat(Constants.STRING_TYPE_NAME);
          else
            colTypes = colTypes.concat(tName);
        }

        if (!ctx.isMRTmpFileURI(destStr)) {
          this.idToTableNameMap.put( String.valueOf(this.destTableId), destStr);
          currentTableId = this.destTableId;
          this.destTableId ++;
        }

        boolean isDfsDir = (dest_type.intValue() == QBMetaData.DEST_DFS_FILE);
        this.loadFileWork.add(new loadFileDesc(queryTmpdir, destStr,
                                               isDfsDir, cols, colTypes));

        table_desc = PlanUtils.getDefaultTableDesc(Integer.toString(Utilities.ctrlaCode),
                                                   cols, colTypes, false);
        outputs.add(new WriteEntity(destStr, !isDfsDir));
        break;
    }
    default:
      throw new SemanticException("Unknown destination type: " + dest_type);
    }

    input = genConversionSelectOperator(dest, qb, input, table_desc);
    inputRR = opParseCtx.get(input).getRR();

    Vector<ColumnInfo> vecCol = new Vector<ColumnInfo>();

    try {
      StructObjectInspector rowObjectInspector = (StructObjectInspector)table_desc.getDeserializer().getObjectInspector();
      List<? extends StructField> fields = rowObjectInspector.getAllStructFieldRefs();
      for (int i=0; i<fields.size(); i++)
        vecCol.add(new ColumnInfo(fields.get(i).getFieldName(), 
                                 TypeInfoUtils.getTypeInfoFromObjectInspector(fields.get(i).getFieldObjectInspector()),
                                 "", false));
    } catch (Exception e)
    {
      throw new SemanticException(e.getMessage());
    }
    
    RowSchema fsRS = new RowSchema(vecCol);
    
    Operator output = putOpInsertMap(
      OperatorFactory.getAndMakeChild(
        new fileSinkDesc(queryTmpdir, table_desc,
                         conf.getBoolVar(HiveConf.ConfVars.COMPRESSRESULT), currentTableId),
        fsRS, input), inputRR);

    LOG.debug("Created FileSink Plan for clause: " + dest + "dest_path: "
        + dest_path + " row schema: "
        + inputRR.toString());
    
    return output;
  }

  /**
   * Generate the conversion SelectOperator that converts the columns into 
   * the types that are expected by the table_desc.
   */
  Operator genConversionSelectOperator(String dest, QB qb,
      Operator input, tableDesc table_desc) throws SemanticException {
    StructObjectInspector oi = null;
    try {
      Deserializer deserializer = table_desc.getDeserializerClass().newInstance();
      deserializer.initialize(null, table_desc.getProperties());
      oi = (StructObjectInspector) deserializer.getObjectInspector();
    } catch (Exception e) {
      throw new SemanticException(e);
    }
    
    // Check column number
    List<? extends StructField> tableFields = oi.getAllStructFieldRefs();
    Vector<ColumnInfo> rowFields = opParseCtx.get(input).getRR().getColumnInfos();
    if (tableFields.size() != rowFields.size()) {
      String reason = "Table " + dest + " has " + tableFields.size() + " columns but query has "
          + rowFields.size() + " columns.";
      throw new SemanticException(ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH.getMsg(
          qb.getParseInfo().getDestForClause(dest), reason));
    }
    
    // Check column types
    boolean converted = false;
    int columnNumber = tableFields.size();
    ArrayList<exprNodeDesc> expressions = new ArrayList<exprNodeDesc>(columnNumber);
    // MetadataTypedColumnsetSerDe does not need type conversions because it does
    // the conversion to String by itself.
    boolean isMetaDataSerDe = table_desc.getDeserializerClass().equals(MetadataTypedColumnsetSerDe.class);
    boolean isLazySimpleSerDe = table_desc.getDeserializerClass().equals(LazySimpleSerDe.class);
    if (!isMetaDataSerDe) {
      for (int i=0; i<columnNumber; i++) {
        ObjectInspector tableFieldOI = tableFields.get(i).getFieldObjectInspector();
        TypeInfo tableFieldTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(tableFieldOI);
        TypeInfo rowFieldTypeInfo = rowFields.get(i).getType();
        exprNodeDesc column = new exprNodeColumnDesc(rowFieldTypeInfo, 
                                                     rowFields.get(i).getInternalName(), "", false);
        // LazySimpleSerDe can convert any types to String type using JSON-format.
        if (!tableFieldTypeInfo.equals(rowFieldTypeInfo)
            && !(isLazySimpleSerDe && tableFieldTypeInfo.getCategory().equals(Category.PRIMITIVE)
              && tableFieldTypeInfo.equals(TypeInfoFactory.stringTypeInfo))) { 
          // need to do some conversions here
          converted = true;
          if (tableFieldTypeInfo.getCategory() != Category.PRIMITIVE) {
            // cannot convert to complex types
            column = null; 
          } else {
            column = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(tableFieldTypeInfo.getTypeName(), column);
          }
          if (column == null) {
            String reason = "Cannot convert column " + i + " from " + rowFieldTypeInfo + " to " 
                + tableFieldTypeInfo + ".";
            throw new SemanticException(ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH.getMsg(
                qb.getParseInfo().getDestForClause(dest), reason));
          }
        }
        expressions.add(column);
      }
    }
    
    if (converted) {
      // add the select operator
      RowResolver rowResolver = new RowResolver();
      ArrayList<String> colName = new ArrayList<String>();
      for (int i=0; i<expressions.size(); i++) {
        String name = getColumnInternalName(i);
        rowResolver.put("", name, new ColumnInfo(name, expressions.get(i).getTypeInfo(), "", false));
        colName.add(name);
      }
      Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new selectDesc(expressions, colName), new RowSchema(rowResolver.getColumnInfos()), input), rowResolver);

      return output;
    } else {
      // not converted
      return input;
    }
  }

  @SuppressWarnings("nls")
  private Operator genLimitPlan(String dest, QB qb, Operator input, int limit) throws SemanticException {
    // A map-only job can be optimized - instead of converting it to a map-reduce job, we can have another map
    // job to do the same to avoid the cost of sorting in the map-reduce phase. A better approach would be to
    // write into a local file and then have a map-only job.
    // Add the limit operator to get the value fields

    RowResolver inputRR = opParseCtx.get(input).getRR();
    Operator limitMap = 
      putOpInsertMap(OperatorFactory.getAndMakeChild(
                       new limitDesc(limit), new RowSchema(inputRR.getColumnInfos()), input), 
                       inputRR);
      

    LOG.debug("Created LimitOperator Plan for clause: " + dest + " row schema: "
        + inputRR.toString());

    return limitMap;
  }

  @SuppressWarnings("nls")
  private Operator genLimitMapRedPlan(String dest, QB qb, Operator input, int limit, boolean extraMRStep) 
    throws SemanticException {
    // A map-only job can be optimized - instead of converting it to a map-reduce job, we can have another map
    // job to do the same to avoid the cost of sorting in the map-reduce phase. A better approach would be to
    // write into a local file and then have a map-only job.
    // Add the limit operator to get the value fields
    Operator curr = genLimitPlan(dest, qb, input, limit);

    // the client requested that an extra map-reduce step be performed
    if (!extraMRStep)
      return curr;

    // Create a reduceSink operator followed by another limit
    curr = genReduceSinkPlan(dest, qb, curr, 1);
    return genLimitPlan(dest, qb, curr, limit);
  }

  @SuppressWarnings("nls")
  private Operator genReduceSinkPlan(String dest, QB qb,
                                     Operator input, int numReducers) throws SemanticException {

    RowResolver inputRR = opParseCtx.get(input).getRR();

    // First generate the expression for the partition and sort keys
    // The cluster by clause / distribute by clause has the aliases for partition function 
    ASTNode partitionExprs = qb.getParseInfo().getClusterByForClause(dest);
    if (partitionExprs == null) {
      partitionExprs = qb.getParseInfo().getDistributeByForClause(dest);
    }
    ArrayList<exprNodeDesc> partitionCols = new ArrayList<exprNodeDesc>();
    if (partitionExprs != null) {
      int ccount = partitionExprs.getChildCount();
      for(int i=0; i<ccount; ++i) {
        ASTNode cl = (ASTNode)partitionExprs.getChild(i);
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
        if (conf.getVar(HiveConf.ConfVars.HIVEMAPREDMODE).equalsIgnoreCase("strict") && limit == null)
          throw new SemanticException(ErrorMsg.NO_LIMIT_WITH_ORDERBY.getMsg(sortExprs));
      }
    }

    ArrayList<exprNodeDesc> sortCols = new ArrayList<exprNodeDesc>();
    StringBuilder order = new StringBuilder();
    if (sortExprs != null) {
      int ccount = sortExprs.getChildCount();
      for(int i=0; i<ccount; ++i) {
        ASTNode cl = (ASTNode)sortExprs.getChild(i);
        
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
        exprNodeDesc exprNode = genExprNodeDesc(cl, inputRR);
        sortCols.add(exprNode);
      }
    }

    // For the generation of the values expression just get the inputs
    // signature and generate field expressions for those
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    ArrayList<exprNodeDesc> valueCols = new ArrayList<exprNodeDesc>();
    for(ColumnInfo colInfo: inputRR.getColumnInfos()) {
      valueCols.add(new exprNodeColumnDesc(colInfo.getType(), colInfo.getInternalName(),
                                           colInfo.getTabAlias(), colInfo.getIsPartitionCol()));
      colExprMap.put(colInfo.getInternalName(), valueCols.get(valueCols.size() - 1));
    }
    
    ArrayList<String> outputColumns = new ArrayList<String>();
    for (int i = 0; i < valueCols.size(); i++)
      outputColumns.add(getColumnInternalName(i));
    Operator interim = putOpInsertMap(
      OperatorFactory.getAndMakeChild(
        PlanUtils.getReduceSinkDesc(sortCols, valueCols, outputColumns, false, -1, partitionCols, order.toString(),
            numReducers),
        new RowSchema(inputRR.getColumnInfos()),
        input), inputRR);
    interim.setColumnExprMap(colExprMap);

    // Add the extract operator to get the value fields
    RowResolver out_rwsch = new RowResolver();
    RowResolver interim_rwsch = inputRR;
    Integer pos = Integer.valueOf(0);
    for(ColumnInfo colInfo: interim_rwsch.getColumnInfos()) {
      String [] info = interim_rwsch.reverseLookup(colInfo.getInternalName());
      out_rwsch.put(info[0], info[1],
                    new ColumnInfo(getColumnInternalName(pos), colInfo.getType(), info[0], false));
      pos = Integer.valueOf(pos.intValue() + 1);
    }

    Operator output = putOpInsertMap(
      OperatorFactory.getAndMakeChild(
        new extractDesc(new exprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, 
                                               Utilities.ReduceField.VALUE.toString(),
                                               "", false)),
        new RowSchema(out_rwsch.getColumnInfos()),
        interim), out_rwsch);

    LOG.debug("Created ReduceSink Plan for clause: " + dest + " row schema: "
        + out_rwsch.toString());
    return output;
  }

  private Operator genJoinOperatorChildren(QBJoinTree join, Operator left, Operator[] right) 
    throws SemanticException {
    RowResolver outputRS = new RowResolver();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    // all children are base classes
    Operator<?>[] rightOps = new Operator[right.length];
    int pos = 0;
    int outputPos = 0;

    Map<String, Byte> reversedExprs = new HashMap<String, Byte>(); 
    HashMap<Byte, List<exprNodeDesc>> exprMap = new HashMap<Byte, List<exprNodeDesc>>();
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    HashMap<Integer, Set<String>> posToAliasMap = new HashMap<Integer, Set<String>>();
    for (Operator input : right)
    {
      ArrayList<exprNodeDesc> keyDesc = new ArrayList<exprNodeDesc>();
      if (input == null)
        input = left;
      Byte tag = Byte.valueOf((byte)(((reduceSinkDesc)(input.getConf())).getTag()));
      RowResolver inputRS = opParseCtx.get(input).getRR();
      Iterator<String> keysIter = inputRS.getTableNames().iterator();
      Set<String> aliases = posToAliasMap.get(pos);
      if(aliases == null) {
        aliases = new HashSet<String>();
        posToAliasMap.put(pos, aliases);
      }
      
      while (keysIter.hasNext())
      {
        String key = keysIter.next();
        aliases.add(key);
        HashMap<String, ColumnInfo> map = inputRS.getFieldMap(key);
        Iterator<String> fNamesIter = map.keySet().iterator();
        while (fNamesIter.hasNext())
        {
          String field = fNamesIter.next();
          ColumnInfo valueInfo = inputRS.get(key, field);
          keyDesc.add(new exprNodeColumnDesc(valueInfo.getType(), 
                                             valueInfo.getInternalName(),
                                             valueInfo.getTabAlias(),
                                             valueInfo.getIsPartitionCol()));
          if (outputRS.get(key, field) == null) {
            String colName = getColumnInternalName(outputPos);
            outputPos++;
            outputColumnNames.add(colName);
            colExprMap.put(colName, keyDesc.get(keyDesc.size() - 1));
            outputRS.put(key, field, new ColumnInfo(colName, 
                                                    valueInfo.getType(), key, false));
            reversedExprs.put(colName, tag);
          }
        }
      }

      exprMap.put(tag, keyDesc);
      rightOps[pos++] = input;
    }

    org.apache.hadoop.hive.ql.plan.joinCond[] joinCondns = new org.apache.hadoop.hive.ql.plan.joinCond[join.getJoinCond().length];
    for (int i = 0; i < join.getJoinCond().length; i++) {
      joinCond condn = join.getJoinCond()[i];
      joinCondns[i] = new org.apache.hadoop.hive.ql.plan.joinCond(condn);
    }

    joinDesc desc = new joinDesc(exprMap, outputColumnNames, joinCondns);
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
    ArrayList<exprNodeDesc> reduceKeys = new ArrayList<exprNodeDesc>();

    // Compute join keys and store in reduceKeys
    Vector<ASTNode> exprs = joinTree.getExpressions().get(pos);
    for (int i = 0; i < exprs.size(); i++) {
      ASTNode expr = exprs.get(i);
      reduceKeys.add(genExprNodeDesc(expr, inputRS));
    }

    // Walk over the input row resolver and copy in the output
    ArrayList<exprNodeDesc> reduceValues = new ArrayList<exprNodeDesc>();
    Iterator<String> tblNamesIter = inputRS.getTableNames().iterator();
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();
    while (tblNamesIter.hasNext())
    {
      String src = tblNamesIter.next();
      HashMap<String, ColumnInfo> fMap = inputRS.getFieldMap(src);
      for (Map.Entry<String, ColumnInfo> entry : fMap.entrySet()) {
        String field = entry.getKey();
        ColumnInfo valueInfo = entry.getValue();
        exprNodeColumnDesc inputExpr = new exprNodeColumnDesc(valueInfo.getType(), 
                                                              valueInfo.getInternalName(),
                                                              valueInfo.getTabAlias(),
                                                              valueInfo.getIsPartitionCol());
        reduceValues.add(inputExpr);
        if (outputRS.get(src, field) == null) {
          String col = getColumnInternalName(reduceValues.size() - 1);
          outputColumns.add(col);
          ColumnInfo newColInfo = new ColumnInfo(Utilities.ReduceField.VALUE.toString() + "." +
                          col,
                          valueInfo.getType(), src, false);
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
      if (conf.getVar(HiveConf.ConfVars.HIVEMAPREDMODE).equalsIgnoreCase("strict"))
        throw new SemanticException(ErrorMsg.NO_CARTESIAN_PRODUCT.getMsg());
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator)putOpInsertMap(
      OperatorFactory.getAndMakeChild(
        PlanUtils.getReduceSinkDesc(reduceKeys, reduceValues, outputColumns, false, joinTree.getNextTag(), reduceKeys.size(), numReds), 
        new RowSchema(outputRS.getColumnInfos()),
        child), outputRS);
    rsOp.setColumnExprMap(colExprMap);
    return rsOp;
  }

  private Operator genJoinOperator(QB qb, QBJoinTree joinTree,
      HashMap<String, Operator> map) throws SemanticException {
    QBJoinTree leftChild = joinTree.getJoinSrc();
    Operator joinSrcOp = null;
    if (leftChild != null)
    {
      Operator joinOp = genJoinOperator(qb, leftChild, map);
      Vector<ASTNode> filter = joinTree.getFilters().get(0);
      for (ASTNode cond: filter) 
        joinOp = genFilterPlan(qb, cond, joinOp);
      
      joinSrcOp = genJoinReduceSinkChild(qb, joinTree, joinOp, null, 0);
    }

    Operator[] srcOps = new Operator[joinTree.getBaseSrc().length];
    int pos = 0;
    for (String src : joinTree.getBaseSrc()) {
      if (src != null) {
        Operator srcOp = map.get(src);
        srcOps[pos] = genJoinReduceSinkChild(qb, joinTree, srcOp, src, pos);
        pos++;
      } else {
        assert pos == 0;
        srcOps[pos++] = null;
      }
    }

    // Type checking and implicit type conversion for join keys
    genJoinOperatorTypeCheck(joinSrcOp, srcOps);
    
    JoinOperator joinOp = (JoinOperator)genJoinOperatorChildren(joinTree, joinSrcOp, srcOps);
    joinContext.put(joinOp, joinTree);
    return joinOp;
  }

  private void genJoinOperatorTypeCheck(Operator left, Operator[] right) throws SemanticException {
    // keys[i] -> ArrayList<exprNodeDesc> for the i-th join operator key list 
    ArrayList<ArrayList<exprNodeDesc>> keys = new ArrayList<ArrayList<exprNodeDesc>>();
    int keyLength = 0;
    for (int i=0; i<right.length; i++) {
      Operator oi = (i==0 && right[i] == null ? left : right[i]);
      reduceSinkDesc now = ((ReduceSinkOperator)(oi)).getConf();
      if (i == 0) {
        keyLength = now.getKeyCols().size();
      } else {
        assert(keyLength == now.getKeyCols().size());
      }
      keys.add(now.getKeyCols());
    }
    // implicit type conversion hierarchy
    for (int k = 0; k < keyLength; k++) {
      // Find the common class for type conversion
      TypeInfo commonType = keys.get(0).get(k).getTypeInfo();
      for(int i=1; i<right.length; i++) {
        TypeInfo a = commonType;
        TypeInfo b = keys.get(i).get(k).getTypeInfo(); 
        commonType = FunctionRegistry.getCommonClassForComparison(a, b);
        if (commonType == null) {
          throw new SemanticException("Cannot do equality join on different types: " + a.getTypeName() + " and " + b.getTypeName());
        }
      }
      // Add implicit type conversion if necessary
      for(int i=0; i<right.length; i++) {
        if (!commonType.equals(keys.get(i).get(k).getTypeInfo())) {
          keys.get(i).set(k, TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc(commonType.getTypeName(), keys.get(i).get(k)));
        }
      }
    }
    // regenerate keySerializationInfo because the ReduceSinkOperator's 
    // output key types might have changed.
    for (int i=0; i<right.length; i++) {
      Operator oi = (i==0 && right[i] == null ? left : right[i]);
      reduceSinkDesc now = ((ReduceSinkOperator)(oi)).getConf();

      now.setKeySerializeInfo(
          PlanUtils.getReduceKeyTableDesc(
              PlanUtils.getFieldSchemasFromColumnList(now.getKeyCols(), "joinkey"),
              now.getOrder()
          )
      );
    }
  }
  
  private Operator genJoinPlan(QB qb, HashMap<String, Operator> map)
      throws SemanticException {
    QBJoinTree joinTree = qb.getQbJoinTree();
    Operator joinOp = genJoinOperator(qb, joinTree, map);
    return joinOp;
  }

  /**
   * Extract the filters from the join condition and push them on top of the source operators. This procedure 
   * traverses the query tree recursively,
   */
  private void pushJoinFilters(QB qb, QBJoinTree joinTree, HashMap<String, Operator> map) throws SemanticException {
    Vector<Vector<ASTNode>> filters = joinTree.getFilters();
    if (joinTree.getJoinSrc() != null)
      pushJoinFilters(qb, joinTree.getJoinSrc(), map);

    int pos = 0;
    for (String src : joinTree.getBaseSrc()) {
      if (src != null) {
        Operator srcOp = map.get(src);
        Vector<ASTNode> filter = filters.get(pos);
        for (ASTNode cond: filter) 
          srcOp = genFilterPlan(qb, cond, srcOp);
        map.put(src, srcOp);
      }
      pos++;
    }
  }

  private List<String> getMapSideJoinTables(QB qb) {
    List<String> cols = null;
    ASTNode hints = qb.getParseInfo().getHints();
    for (int pos = 0; pos < hints.getChildCount(); pos++) {
      ASTNode hint = (ASTNode)hints.getChild(pos);
      if (((ASTNode)hint.getChild(0)).getToken().getType() == HiveParser.TOK_MAPJOIN) {
        ASTNode hintTblNames = (ASTNode)hint.getChild(1);
        int numCh = hintTblNames.getChildCount();
        for (int tblPos = 0; tblPos < numCh; tblPos++) {
          String tblName = ((ASTNode)hintTblNames.getChild(tblPos)).getText();
          if (cols == null)
            cols = new ArrayList<String>();
          if (!cols.contains(tblName))
            cols.add(tblName);
        }
      }
    }
    
    return cols;
  }
  
  private QBJoinTree genJoinTree(QB qb, ASTNode joinParseTree)
      throws SemanticException {
    QBJoinTree joinTree = new QBJoinTree();
    joinCond[] condn = new joinCond[1];

    if (joinParseTree.getToken().getType() == HiveParser.TOK_LEFTOUTERJOIN)
    {
      joinTree.setNoOuterJoin(false);
      condn[0] = new joinCond(0, 1, joinType.LEFTOUTER);
    }
    else if (joinParseTree.getToken().getType() == HiveParser.TOK_RIGHTOUTERJOIN)
    {
      joinTree.setNoOuterJoin(false);
      condn[0] = new joinCond(0, 1, joinType.RIGHTOUTER);
    }
    else if (joinParseTree.getToken().getType() == HiveParser.TOK_FULLOUTERJOIN)
    {
      joinTree.setNoOuterJoin(false);
      condn[0] = new joinCond(0, 1, joinType.FULLOUTER);
    }
    else
    {
      condn[0] = new joinCond(0, 1, joinType.INNER);
      joinTree.setNoOuterJoin(true);
    }

    joinTree.setJoinCond(condn);

    ASTNode left = (ASTNode) joinParseTree.getChild(0);
    ASTNode right = (ASTNode) joinParseTree.getChild(1);

    if ((left.getToken().getType() == HiveParser.TOK_TABREF)
        || (left.getToken().getType() == HiveParser.TOK_SUBQUERY)) {
      String table_name = unescapeIdentifier(left.getChild(0).getText());
      String alias = left.getChildCount() == 1 ? table_name : 
        unescapeIdentifier(left.getChild(left.getChildCount()-1).getText().toLowerCase());
      joinTree.setLeftAlias(alias);
      String[] leftAliases = new String[1];
      leftAliases[0] = alias;
      joinTree.setLeftAliases(leftAliases);
      String[] children = new String[2];
      children[0] = alias;
      joinTree.setBaseSrc(children);
    }
    else if (isJoinToken(left)) {
      QBJoinTree leftTree = genJoinTree(qb, left);
      joinTree.setJoinSrc(leftTree);
      String[] leftChildAliases = leftTree.getLeftAliases();
      String leftAliases[] = new String[leftChildAliases.length + 1];
      for (int i = 0; i < leftChildAliases.length; i++)
        leftAliases[i] = leftChildAliases[i];
      leftAliases[leftChildAliases.length] = leftTree.getRightAliases()[0];
      joinTree.setLeftAliases(leftAliases);
    } else
      assert (false);

    if ((right.getToken().getType() == HiveParser.TOK_TABREF)
        || (right.getToken().getType() == HiveParser.TOK_SUBQUERY)) {
      String table_name = unescapeIdentifier(right.getChild(0).getText());
      String alias = right.getChildCount() == 1 ? table_name : 
        unescapeIdentifier(right.getChild(right.getChildCount()-1).getText().toLowerCase());
      String[] rightAliases = new String[1];
      rightAliases[0] = alias;
      joinTree.setRightAliases(rightAliases);
      String[] children = joinTree.getBaseSrc();
      if (children == null)
        children = new String[2];
      children[1] = alias;
      joinTree.setBaseSrc(children);
    } else
      assert false;

    Vector<Vector<ASTNode>> expressions = new Vector<Vector<ASTNode>>();
    expressions.add(new Vector<ASTNode>());
    expressions.add(new Vector<ASTNode>());
    joinTree.setExpressions(expressions);

    Vector<Vector<ASTNode>> filters = new Vector<Vector<ASTNode>>();
    filters.add(new Vector<ASTNode>());
    filters.add(new Vector<ASTNode>());
    joinTree.setFilters(filters);

    ASTNode joinCond = (ASTNode) joinParseTree.getChild(2);
    Vector<String> leftSrc = new Vector<String>();
    parseJoinCondition(joinTree, joinCond, leftSrc);
    if (leftSrc.size() == 1)
      joinTree.setLeftAlias(leftSrc.get(0));

    // check the hints to see if the user has specified a map-side join. This will be removed later on, once the cost-based 
    // infrastructure is in place
    if (qb.getParseInfo().getHints() != null) {
      List<String> mapSideTables = getMapSideJoinTables(qb);
      List<String> mapAliases    = joinTree.getMapAliases();

      for (String mapTbl : mapSideTables) {
        boolean mapTable = false;
        for (String leftAlias : joinTree.getLeftAliases()) {
          if (mapTbl.equals(leftAlias))
            mapTable = true;
        }
        for (String rightAlias : joinTree.getRightAliases()) {
          if (mapTbl.equals(rightAlias))
            mapTable = true;
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
    }

    return joinTree;
  }

  private void mergeJoins(QB qb, QBJoinTree parent, QBJoinTree node,
      QBJoinTree target, int pos) {
    String[] nodeRightAliases = node.getRightAliases();
    String[] trgtRightAliases = target.getRightAliases();
    String[] rightAliases = new String[nodeRightAliases.length
        + trgtRightAliases.length];

    for (int i = 0; i < trgtRightAliases.length; i++)
      rightAliases[i] = trgtRightAliases[i];
    for (int i = 0; i < nodeRightAliases.length; i++)
      rightAliases[i + trgtRightAliases.length] = nodeRightAliases[i];
    target.setRightAliases(rightAliases);

    String[] nodeBaseSrc = node.getBaseSrc();
    String[] trgtBaseSrc = target.getBaseSrc();
    String[] baseSrc = new String[nodeBaseSrc.length + trgtBaseSrc.length - 1];

    for (int i = 0; i < trgtBaseSrc.length; i++)
      baseSrc[i] = trgtBaseSrc[i];
    for (int i = 1; i < nodeBaseSrc.length; i++)
      baseSrc[i + trgtBaseSrc.length - 1] = nodeBaseSrc[i];
    target.setBaseSrc(baseSrc);

    Vector<Vector<ASTNode>> expr = target.getExpressions();
    for (int i = 0; i < nodeRightAliases.length; i++)
      expr.add(node.getExpressions().get(i + 1));

    Vector<Vector<ASTNode>> filter = target.getFilters();
    for (int i = 0; i < nodeRightAliases.length; i++)
      filter.add(node.getFilters().get(i + 1));

    if (node.getFilters().get(0).size() != 0) {
      Vector<ASTNode> filterPos = filter.get(pos);
      filterPos.addAll(node.getFilters().get(0));
    }

    if (qb.getQbJoinTree() == node)
      qb.setQbJoinTree(node.getJoinSrc());
    else
      parent.setJoinSrc(node.getJoinSrc());

    if (node.getNoOuterJoin() && target.getNoOuterJoin())
      target.setNoOuterJoin(true);
    else
      target.setNoOuterJoin(false);

    joinCond[] nodeCondns = node.getJoinCond();
    int nodeCondnsSize = nodeCondns.length;
    joinCond[] targetCondns = target.getJoinCond();
    int targetCondnsSize = targetCondns.length;
    joinCond[] newCondns = new joinCond[nodeCondnsSize + targetCondnsSize];
    for (int i = 0; i < targetCondnsSize; i++)
      newCondns[i] = targetCondns[i];

    for (int i = 0; i < nodeCondnsSize; i++)
    {
      joinCond nodeCondn = nodeCondns[i];
      if (nodeCondn.getLeft() == 0)
        nodeCondn.setLeft(pos);
      else
        nodeCondn.setLeft(nodeCondn.getLeft() + targetCondnsSize);
      nodeCondn.setRight(nodeCondn.getRight() + targetCondnsSize);
      newCondns[targetCondnsSize + i] = nodeCondn;
    }

    target.setJoinCond(newCondns);
    if (target.isMapSideJoin()) {
      assert node.isMapSideJoin();
      List<String> mapAliases = target.getMapAliases();
      for (String mapTbl : node.getMapAliases())
        if (!mapAliases.contains(mapTbl))
          mapAliases.add(mapTbl);
      target.setMapAliases(mapAliases);
    }
  }

  private int findMergePos(QBJoinTree node, QBJoinTree target) {
    int res = -1;
    String leftAlias = node.getLeftAlias();
    if (leftAlias == null)
      return -1;

    Vector<ASTNode> nodeCondn = node.getExpressions().get(0);
    Vector<ASTNode> targetCondn = null;

    if (leftAlias.equals(target.getLeftAlias()))
    {
      targetCondn = target.getExpressions().get(0);
      res = 0;
    }
    else
      for (int i = 0; i < target.getRightAliases().length; i++) {
        if (leftAlias.equals(target.getRightAliases()[i])) {
          targetCondn = target.getExpressions().get(i + 1);
          res = i + 1;
          break;
        }
      }

    if ((targetCondn == null) || (nodeCondn.size() != targetCondn.size()))
      return -1;

    for (int i = 0; i < nodeCondn.size(); i++)
      if (!nodeCondn.get(i).toStringTree().equals(
          targetCondn.get(i).toStringTree()))
        return -1;

    return res;
  }

  private boolean mergeJoinNodes(QB qb, QBJoinTree parent, QBJoinTree node,
      QBJoinTree target) {
    if (target == null)
      return false;

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
        if (merged)
          root = qb.getQbJoinTree();
        else {
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
    Vector<ColumnInfo> columns = inputRR.getColumnInfos();
    ArrayList<exprNodeDesc> colList = new ArrayList<exprNodeDesc>();
    ArrayList<String> columnNames = new ArrayList<String>();
    for (int i = 0; i < columns.size(); i++) {
      ColumnInfo col = columns.get(i);
      colList.add(new exprNodeColumnDesc(col.getType(), col.getInternalName(),
                                         col.getTabAlias(), col.getIsPartitionCol()));
      columnNames.add(col.getInternalName());
    }
    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new selectDesc(colList, columnNames, true), new RowSchema(inputRR.getColumnInfos()), input), inputRR);
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
    if (ks.size() <= 1)
      return null;
    
    List<exprNodeDesc> oldList = null;
    List<ASTNode>      oldASTList = null;
    
    for (String dest : ks) {
      Operator curr = input;      

      // If a filter is present, common processing is not possible
      if (qbp.getWhrForClause(dest) != null) 
        return null;

      if (qbp.getAggregationExprsForClause(dest).size() == 0
          && getGroupByForClause(qbp, dest).size() == 0)
        return null;
      
      // All distinct expressions must be the same
      ASTNode value = qbp.getDistinctFuncExprForClause(dest);
      if (value == null)
        return null;
      
      List<exprNodeDesc> currDestList = new ArrayList<exprNodeDesc>();
      List<ASTNode>      currASTList  = new ArrayList<ASTNode>();
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
        oldList    = currDestList;
        oldASTList = currASTList;
      }
      else {
        if (oldList.size() != currDestList.size())
          return null;
        for (int pos = 0; pos < oldList.size(); pos++)
        {
          if (!oldList.get(pos).isSame(currDestList.get(pos)))
            return null;
        }
      }
    }
    
    return oldASTList;
  }
  
  private Operator createCommonReduceSink(QB qb, Operator input) throws SemanticException {
    // Go over all the tables and extract the common distinct key
    List<ASTNode> distExprs = getCommonDistinctExprs(qb, input);
    
    QBParseInfo qbp = qb.getParseInfo();
    TreeSet<String> ks = new TreeSet<String>();
    ks.addAll(qbp.getClauseNames());

    // Pass the entire row 
    RowResolver inputRR = opParseCtx.get(input).getRR();
    RowResolver reduceSinkOutputRowResolver = new RowResolver();
    reduceSinkOutputRowResolver.setIsExprResolver(true);
    ArrayList<exprNodeDesc> reduceKeys   = new ArrayList<exprNodeDesc>();
    ArrayList<exprNodeDesc> reduceValues = new ArrayList<exprNodeDesc>();
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();

    // Pre-compute distinct group-by keys and store in reduceKeys

    List<String> outputColumnNames = new ArrayList<String>();
    for (ASTNode distn : distExprs) {
      exprNodeDesc distExpr = genExprNodeDesc(distn, inputRR);
      reduceKeys.add(distExpr);
      String text = distn.toStringTree();
      if (reduceSinkOutputRowResolver.get("", text) == null) {
        outputColumnNames.add(getColumnInternalName(reduceKeys.size() - 1));
        String field = Utilities.ReduceField.KEY.toString() + "." + getColumnInternalName(reduceKeys.size() - 1);
        ColumnInfo colInfo = new ColumnInfo(field,
                                            reduceKeys.get(reduceKeys.size()-1).getTypeInfo(), "", false);
        reduceSinkOutputRowResolver.put("", text, colInfo);
        colExprMap.put(colInfo.getInternalName(), distExpr);
      }
    }

    // Go over all the grouping keys and aggregations
    for (String dest : ks) {

      List<ASTNode> grpByExprs = getGroupByForClause(qbp, dest);
      for (int i = 0; i < grpByExprs.size(); ++i) {
        ASTNode grpbyExpr = grpByExprs.get(i);
        String text       = grpbyExpr.toStringTree();

        if (reduceSinkOutputRowResolver.get("", text) == null) {
          exprNodeDesc grpByExprNode = genExprNodeDesc(grpbyExpr, inputRR);
          reduceValues.add(grpByExprNode);
          String field = Utilities.ReduceField.VALUE.toString() + "." + getColumnInternalName(reduceValues.size() - 1);
          ColumnInfo colInfo = new ColumnInfo(field, reduceValues.get(reduceValues.size()-1).getTypeInfo(), "", false);
          reduceSinkOutputRowResolver.put("", text, colInfo);
          outputColumnNames.add(getColumnInternalName(reduceValues.size() - 1));
        }
      }

      // For each aggregation
      HashMap<String, ASTNode> aggregationTrees = qbp.getAggregationExprsForClause(dest);
      assert (aggregationTrees != null);
      
      for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
        ASTNode value = entry.getValue();
        String aggName = value.getChild(0).getText();

        // 0 is the function name
        for (int i = 1; i < value.getChildCount(); i++) {
          ASTNode paraExpr = (ASTNode)value.getChild(i);
          String  text     = paraExpr.toStringTree();
          
          if (reduceSinkOutputRowResolver.get("", text) == null) {
            exprNodeDesc paraExprNode = genExprNodeDesc(paraExpr, inputRR);
            reduceValues.add(paraExprNode);
            String field = Utilities.ReduceField.VALUE.toString() + "." + getColumnInternalName(reduceValues.size() - 1);
            ColumnInfo colInfo = new ColumnInfo(field, reduceValues.get(reduceValues.size()-1).getTypeInfo(), "", false);
            reduceSinkOutputRowResolver.put("", text, colInfo);
            outputColumnNames.add(getColumnInternalName(reduceValues.size() - 1));
          }
        }
      }        
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator)  putOpInsertMap(
      OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(reduceKeys, reduceValues, outputColumnNames, true, -1, reduceKeys.size(), -1),
                                      new RowSchema(reduceSinkOutputRowResolver.getColumnInfos()), input),
      reduceSinkOutputRowResolver);
    
    rsOp.setColumnExprMap(colExprMap);
    return rsOp;
  }

  @SuppressWarnings("nls")
  private Operator genBodyPlan(QB qb, Operator input)
      throws SemanticException {

    QBParseInfo qbp = qb.getParseInfo();

    TreeSet<String> ks = new TreeSet<String>();
    ks.addAll(qbp.getClauseNames());

    // For multi-group by with the same distinct, we ignore all user hints currently. It doesnt matter whether he has asked to do
    // map-side aggregation or not. Map side aggregation is turned off
    boolean optimizeMultiGroupBy = (getCommonDistinctExprs(qb, input) != null);
    Operator curr = null;
    
    // If there are multiple group-bys, map-side aggregation is turned off, there are no filters
    // and there is a single distinct, optimize that. Spray initially by the distinct key,
    // no computation at the mapper. Have multiple group by operators at the reducer - and then 
    // proceed
    if (optimizeMultiGroupBy) {
      curr = createCommonReduceSink(qb, input);
      
      RowResolver currRR = opParseCtx.get(curr).getRR();
      // create a forward operator
      input = putOpInsertMap(OperatorFactory.getAndMakeChild(new forwardDesc(), 
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
    }    
    else {
    // Go over all the destination tables
    for (String dest : ks) {
      curr = input;      

      if (qbp.getWhrForClause(dest) != null) {
        curr = genFilterPlan(dest, qb, curr);
      }

      if (qbp.getAggregationExprsForClause(dest).size() != 0
          || getGroupByForClause(qbp, dest).size() > 0)
      {
        // insert a select operator here used by the ColumnPruner to reduce the data to shuffle
        curr = insertSelectAllPlanForGroupBy(dest, curr);
        if (conf.getVar(HiveConf.ConfVars.HIVEMAPSIDEAGGREGATE).equalsIgnoreCase("true")) {
          if (conf.getVar(HiveConf.ConfVars.HIVEGROUPBYSKEW).equalsIgnoreCase("false"))
            curr = genGroupByPlanMapAggr1MR(dest, qb, curr);
          else
            curr = genGroupByPlanMapAggr2MR(dest, qb, curr);
        }
        else if (conf.getVar(HiveConf.ConfVars.HIVEGROUPBYSKEW).equalsIgnoreCase("true"))
          curr = genGroupByPlan2MR(dest, qb, curr);
        else
          curr = genGroupByPlan1MR(dest, qb, curr);
      }

      curr = genSelectPlan(dest, qb, curr);
      Integer limit = qbp.getDestLimit(dest);

      if (qbp.getClusterByForClause(dest) != null
          || qbp.getDistributeByForClause(dest) != null
          || qbp.getOrderByForClause(dest) != null
          || qbp.getSortByForClause(dest) != null) {

        int numReducers = -1;

        // Use only 1 reducer if order by is present
        if (qbp.getOrderByForClause(dest) != null)
          numReducers = 1;

        curr = genReduceSinkPlan(dest, qb, curr, numReducers);
      }

      if (qbp.getIsSubQ()) {
        if (limit != null) {
          // In case of order by, only 1 reducer is used, so no need of another shuffle
          curr = genLimitMapRedPlan(dest, qb, curr, limit.intValue(), qbp.getOrderByForClause(dest) != null ? false : true);
        }
      } else {
        curr = genConversionOps(dest, qb, curr);
        // exact limit can be taken care of by the fetch operator
        if (limit != null) {
          boolean extraMRStep = true;

          if (qb.getIsQuery() &&
              qbp.getClusterByForClause(dest) == null &&
              qbp.getSortByForClause(dest) == null)
            extraMRStep = false;

          curr = genLimitMapRedPlan(dest, qb, curr, limit.intValue(), extraMRStep);
          qb.getParseInfo().setOuterQueryLimit(limit.intValue());
        }
        curr = genFileSinkPlan(dest, qb, curr);
      }
      
      // change curr ops row resolver's tab aliases to query alias if it exists
      if(qb.getParseInfo().getAlias() != null) {
        RowResolver rr = opParseCtx.get(curr).getRR();
        RowResolver newRR = new RowResolver();
        String alias = qb.getParseInfo().getAlias();
        for(ColumnInfo colInfo: rr.getColumnInfos()) {
          String name = colInfo.getInternalName();
          String [] tmp = rr.reverseLookup(name);
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

    // Currently, the unions are not merged - each union has only 2 parents. So, a n-way union will lead to (n-1) union operators.
    // This can be easily merged into 1 union
    RowResolver leftRR = opParseCtx.get(leftOp).getRR();
    RowResolver rightRR = opParseCtx.get(rightOp).getRR();
    HashMap<String, ColumnInfo> leftmap = leftRR.getFieldMap(leftalias);
    HashMap<String, ColumnInfo> rightmap = rightRR.getFieldMap(rightalias);
    // make sure the schemas of both sides are the same
    for (Map.Entry<String, ColumnInfo> lEntry: leftmap.entrySet()) {
      String field = lEntry.getKey();
      ColumnInfo lInfo = lEntry.getValue();
      ColumnInfo rInfo = rightmap.get(field);
      if (rInfo == null) {
        throw new SemanticException("Schema of both sides of union should match. "
            + rightalias + " does not have the field " + field);
      }
      if (lInfo == null) {
        throw new SemanticException("Schema of both sides of union should match. " 
            + leftalias + " does not have the field " + field);
      }
      if (!lInfo.getInternalName().equals(rInfo.getInternalName())) {
        throw new SemanticException("Schema of both sides of union should match: "
            + field + ":" + lInfo.getInternalName() + " " + rInfo.getInternalName());
      }
      if (!lInfo.getType().getTypeName().equals(rInfo.getType().getTypeName())) {
        throw new SemanticException("Schema of both sides of union should match: Column "
            + field + " is of type " + lInfo.getType().getTypeName() + 
            " on first table and type " + rInfo.getType().getTypeName() + " on second table");
      }
    }

    // construct the forward operator
    RowResolver unionoutRR = new RowResolver();
    for (Map.Entry<String, ColumnInfo> lEntry: leftmap.entrySet()) {
      String field = lEntry.getKey();
      ColumnInfo lInfo = lEntry.getValue();
      unionoutRR.put(unionalias, field, lInfo);
    }

    // If one of the children is a union, merge with it
    // else create a new one
    if ((leftOp instanceof UnionOperator) || (rightOp instanceof UnionOperator))
    {
      if (leftOp instanceof UnionOperator) {
        // make left a child of right
        List<Operator<? extends Serializable>> child = new ArrayList<Operator<? extends Serializable>>();
        child.add(leftOp);
        rightOp.setChildOperators(child);

        List<Operator<? extends Serializable>> parent = leftOp.getParentOperators();
        parent.add(rightOp);
        
        unionDesc uDesc = ((UnionOperator)leftOp).getConf();
        uDesc.setNumInputs(uDesc.getNumInputs()+1);
        return putOpInsertMap(leftOp, unionoutRR);
      }
      else {
        // make right a child of left
        List<Operator<? extends Serializable>> child = new ArrayList<Operator<? extends Serializable>>();
        child.add(rightOp);
        leftOp.setChildOperators(child);

        List<Operator<? extends Serializable>> parent = rightOp.getParentOperators();
        parent.add(leftOp);
        unionDesc uDesc = ((UnionOperator)rightOp).getConf();
        uDesc.setNumInputs(uDesc.getNumInputs()+1);
        
        return putOpInsertMap(rightOp, unionoutRR);
      }
    }

    // Create a new union operator
    Operator<? extends Serializable> unionforward = 
      OperatorFactory.getAndMakeChild(new unionDesc(), new RowSchema(unionoutRR.getColumnInfos()));
    
    // set union operator as child of each of leftOp and rightOp
    List<Operator<? extends Serializable>> child = new ArrayList<Operator<? extends Serializable>>();
    child.add(unionforward);
    rightOp.setChildOperators(child);
    
    child = new ArrayList<Operator<? extends Serializable>>();
    child.add(unionforward);
    leftOp.setChildOperators(child);
    
    List<Operator<? extends Serializable>> parent = new ArrayList<Operator<? extends Serializable>>();
    parent.add(leftOp);
    parent.add(rightOp);
    unionforward.setParentOperators(parent);
    
    // create operator info list to return
    return putOpInsertMap(unionforward, unionoutRR);
  }

  /**
   * Generates the sampling predicate from the TABLESAMPLE clause information. This function uses the 
   * bucket column list to decide the expression inputs to the predicate hash function in case useBucketCols
   * is set to true, otherwise the expression list stored in the TableSample is used. The bucket columns of 
   * the table are used to generate this predicate in case no expressions are provided on the TABLESAMPLE
   * clause and the table has clustering columns defined in it's metadata.
   * The predicate created has the following structure:
   * 
   *     ((hash(expressions) & Integer.MAX_VALUE) % denominator) == numerator
   * 
   * @param ts TABLESAMPLE clause information
   * @param bucketCols The clustering columns of the table
   * @param useBucketCols Flag to indicate whether the bucketCols should be used as input to the hash
   *                      function
   * @param alias The alias used for the table in the row resolver
   * @param rwsch The row resolver used to resolve column references
   * @param qbm The metadata information for the query block which is used to resolve unaliased columns
   * @param planExpr The plan tree for the expression. If the user specified this, the parse expressions are not used
   * @return exprNodeDesc
   * @exception SemanticException
   */
  private exprNodeDesc genSamplePredicate(TableSample ts, List<String> bucketCols,
                                      boolean useBucketCols, String alias,
                                      RowResolver rwsch, QBMetaData qbm, exprNodeDesc planExpr) 
    throws SemanticException {
    
    exprNodeDesc numeratorExpr = new exprNodeConstantDesc(
        TypeInfoFactory.intTypeInfo, 
        Integer.valueOf(ts.getNumerator() - 1));
      
    exprNodeDesc denominatorExpr = new exprNodeConstantDesc(
        TypeInfoFactory.intTypeInfo, 
        Integer.valueOf(ts.getDenominator()));

    exprNodeDesc intMaxExpr = new exprNodeConstantDesc(
        TypeInfoFactory.intTypeInfo, 
        Integer.valueOf(Integer.MAX_VALUE));
    
    ArrayList<exprNodeDesc> args = new ArrayList<exprNodeDesc>();
    if (planExpr != null)
      args.add(planExpr);
    else if (useBucketCols) {
      for (String col : bucketCols) {
        ColumnInfo ci = rwsch.get(alias, col);
        // TODO: change type to the one in the table schema
        args.add(new exprNodeColumnDesc(ci.getType(), ci.getInternalName(),
                                        ci.getTabAlias(), ci.getIsPartitionCol()));
      }
    }
    else {
      for(ASTNode expr: ts.getExprs()) {
        args.add(genExprNodeDesc(expr, rwsch));
      }
    }

    exprNodeDesc equalsExpr = null;
    {
      exprNodeDesc hashfnExpr = new exprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
          GenericUDFHash.class, args);
      assert(hashfnExpr != null);
      LOG.info("hashfnExpr = " + hashfnExpr);
      exprNodeDesc andExpr = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("&", hashfnExpr, intMaxExpr);
      assert(andExpr != null);
      LOG.info("andExpr = " + andExpr);
      exprNodeDesc modExpr = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("%", andExpr, denominatorExpr);
      assert(modExpr != null);
      LOG.info("modExpr = " + modExpr);
      LOG.info("numeratorExpr = " + numeratorExpr);
      equalsExpr = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("==", modExpr, numeratorExpr);
      LOG.info("equalsExpr = " + equalsExpr);
      assert(equalsExpr != null);
    }
    return equalsExpr;
  }
  
  @SuppressWarnings("nls")
  private Operator genTablePlan(String alias, QB qb) throws SemanticException {

    String alias_id = (qb.getId() == null ? alias : qb.getId() + ":" + alias);
    Table tab = qb.getMetaData().getSrcForAlias(alias);
    RowResolver rwsch;
    
    // is the table already present
    Operator<? extends Serializable> top = this.topOps.get(alias_id);
    Operator<? extends Serializable> dummySel = this.topSelOps.get(alias_id);
    if (dummySel != null)
      top = dummySel;
   
    if (top == null) {    
      rwsch = new RowResolver();
      try {
        StructObjectInspector rowObjectInspector = (StructObjectInspector)tab.getDeserializer().getObjectInspector();
        List<? extends StructField> fields = rowObjectInspector.getAllStructFieldRefs();
        for (int i=0; i<fields.size(); i++) {
          rwsch.put(alias, fields.get(i).getFieldName(),
                    new ColumnInfo(fields.get(i).getFieldName(), 
                                   TypeInfoUtils.getTypeInfoFromObjectInspector(fields.get(i).getFieldObjectInspector()),
                                   alias, false));
        }
      } catch (SerDeException e) {
        throw new RuntimeException(e);
      }
      // Hack!! - refactor once the metadata APIs with types are ready
      // Finally add the partitioning columns
      for(FieldSchema part_col: tab.getPartCols()) {
        LOG.trace("Adding partition col: " + part_col);
        // TODO: use the right type by calling part_col.getType() instead of String.class
        rwsch.put(alias, part_col.getName(), 
                  new ColumnInfo(part_col.getName(), TypeInfoFactory.stringTypeInfo, alias, true));
      }
      
      // Create the root of the operator tree
      top = putOpInsertMap(OperatorFactory.get(new tableScanDesc(alias), new RowSchema(rwsch.getColumnInfos())), rwsch);

      // Add this to the list of top operators - we always start from a table scan
      this.topOps.put(alias_id, top);
      
      // Add a mapping from the table scan operator to Table
      this.topToTable.put((TableScanOperator)top, tab);
    }
    else {
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
          throw new SemanticException(ErrorMsg.NON_BUCKETED_TABLE.getMsg() + " " + tab.getName());
      }

      // check if a predicate is needed
      // predicate is needed if either input pruning is not enough
      // or if input pruning is not possible
      
      // check if the sample columns are the same as the table bucket columns
      boolean colsEqual = true;
      if ( (sampleExprs.size() != tabBucketCols.size()) && (sampleExprs.size() != 0) ) {
        colsEqual = false;
      }
      
      for (int i = 0; i < sampleExprs.size() && colsEqual; i++) {
        boolean colFound = false;
        for (int j = 0; j < tabBucketCols.size() && !colFound; j++) {
          if (sampleExprs.get(i).getToken().getType() != HiveParser.TOK_TABLE_OR_COL) {
            break;
          }
          
          if (((ASTNode)sampleExprs.get(i).getChild(0)).getText().equalsIgnoreCase(tabBucketCols.get(j))) {
            colFound = true;
          }
        }
        colsEqual = (colsEqual && colFound);
      }
      
      // Check if input can be pruned
      ts.setInputPruning((sampleExprs == null || sampleExprs.size() == 0 || colsEqual));
      
      // check if input pruning is enough     
      if ((sampleExprs == null || sampleExprs.size() == 0 || colsEqual)
          && (num == den || den <= numBuckets && numBuckets % den == 0)) { 
        // input pruning is enough; no need for filter
        LOG.info("No need for sample filter");
        // TODO sample predicate is not needed, but we are adding it anyway since
        // input pruning is broken for subqueries. will remove this once we move
        // compilation of sampling to use the operator tree
        exprNodeDesc samplePredicate = genSamplePredicate(ts, tabBucketCols, colsEqual, alias, rwsch, qb.getMetaData(), null);
        tableOp = OperatorFactory.getAndMakeChild(
            new filterDesc(samplePredicate, true), 
            top);
      }
      else {
        // need to add filter
        // create tableOp to be filterDesc and set as child to 'top'
        LOG.info("Need sample filter");
        exprNodeDesc samplePredicate = genSamplePredicate(ts, tabBucketCols, colsEqual, alias, rwsch, qb.getMetaData(), null);
        tableOp = OperatorFactory.getAndMakeChild(
            new filterDesc(samplePredicate, true), 
            top);
      }
    } 
    else {
      boolean testMode = conf.getBoolVar(HiveConf.ConfVars.HIVETESTMODE);
      if (testMode) {
        String tabName = tab.getName();
        
        // has the user explicitly asked not to sample this table
        String   unSampleTblList = conf.getVar(HiveConf.ConfVars.HIVETESTMODENOSAMPLE);
        String[] unSampleTbls    = unSampleTblList.split(",");
        boolean unsample = false;
        for (String unSampleTbl : unSampleTbls) 
          if (tabName.equalsIgnoreCase(unSampleTbl))
            unsample = true;
        
        if (!unsample) {
          int numBuckets = tab.getNumBuckets();
        
          // If the input table is bucketed, choose the first bucket
          if (numBuckets > 0) {
            TableSample tsSample = new TableSample(1, numBuckets);
            tsSample.setInputPruning(true);
            qb.getParseInfo().setTabSample(alias, tsSample);
            LOG.info("No need for sample filter");
          }
          // The table is not bucketed, add a dummy filter :: rand()
          else {
            int freq = conf.getIntVar(HiveConf.ConfVars.HIVETESTMODESAMPLEFREQ);
            TableSample tsSample = new TableSample(1, freq);
            tsSample.setInputPruning(false);
            qb.getParseInfo().setTabSample(alias, tsSample);
            LOG.info("Need sample filter");
            exprNodeDesc randFunc = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("rand", new exprNodeConstantDesc(Integer.valueOf(460476415)));
            exprNodeDesc samplePred = genSamplePredicate(tsSample, null, false, alias, rwsch, qb.getMetaData(), randFunc);
            tableOp = OperatorFactory.getAndMakeChild(new filterDesc(samplePred, true), top);
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

    Operator srcOpInfo = null;

    // process join
    if (qb.getParseInfo().getJoinExpr() != null) {
      ASTNode joinExpr = qb.getParseInfo().getJoinExpr();
      QBJoinTree joinTree = genJoinTree(qb, joinExpr);
      qb.setQbJoinTree(joinTree);
      mergeJoinTree(qb);

      // if any filters are present in the join tree, push them on top of the table
      pushJoinFilters(qb, qb.getQbJoinTree(), aliasToOpInfo);
      srcOpInfo = genJoinPlan(qb, aliasToOpInfo);
    }
    else
      // Now if there are more than 1 sources then we have a join case
      // later we can extend this to the union all case as well
      srcOpInfo = aliasToOpInfo.values().iterator().next();

    Operator bodyOpInfo = genBodyPlan(qb, srcOpInfo);
    LOG.debug("Created Plan for Query Block " + qb.getId());
    
    this.qb = qb;
    return bodyOpInfo;
  }

  private Operator<? extends Serializable> getReduceSink(Operator<? extends Serializable> top) {
    if (top.getClass() == ReduceSinkOperator.class) {
      // Get the operator following the reduce sink
      assert (top.getChildOperators().size() == 1);

      return top;
    }

    List<Operator<? extends Serializable>> childOps = top.getChildOperators();
    if (childOps == null) {
      return null;
    }

    for (int i = 0; i < childOps.size(); ++i) {
      Operator<? extends Serializable> reducer = getReduceSink(childOps.get(i));
      if (reducer != null) {
        return reducer;
      }
    }

    return null;
  }

  @SuppressWarnings("nls")
  private void genMapRedTasks(QB qb) throws SemanticException {
    fetchWork fetch = null;
    List<Task<? extends Serializable>> mvTask = new ArrayList<Task<? extends Serializable>>();
    Task<? extends Serializable> fetchTask = null;

    QBParseInfo qbParseInfo = qb.getParseInfo();
    if (qb.isSelectStarQuery()
        && qbParseInfo.getDestToClusterBy().isEmpty()
        && qbParseInfo.getDestToDistributeBy().isEmpty()
        && qbParseInfo.getDestToOrderBy().isEmpty()
        && qbParseInfo.getDestToSortBy().isEmpty()) {
      Iterator<Map.Entry<String, Table>> iter = qb.getMetaData().getAliasToTable().entrySet().iterator();
      Table tab = ((Map.Entry<String, Table>)iter.next()).getValue();
      if (!tab.isPartitioned()) {
        if (qbParseInfo.getDestToWhereExpr().isEmpty())
          fetch = new fetchWork(tab.getPath().toString(), Utilities.getTableDesc(tab), qb.getParseInfo().getOuterQueryLimit()); 
        inputs.add(new ReadEntity(tab));
      }
      else {
        if (aliasToPruner.size() == 1) {
          Iterator<Map.Entry<String, org.apache.hadoop.hive.ql.parse.ASTPartitionPruner>> iterP = 
            aliasToPruner.entrySet().iterator();
          org.apache.hadoop.hive.ql.parse.ASTPartitionPruner pr = 
            ((Map.Entry<String, org.apache.hadoop.hive.ql.parse.ASTPartitionPruner>)iterP.next()).getValue();
          if (pr.onlyContainsPartitionCols()) {
            List<String> listP = new ArrayList<String>();
            List<partitionDesc> partP = new ArrayList<partitionDesc>();
            PrunedPartitionList partsList = null;
            Set<Partition> parts = null;
            try {
              partsList = pr.prune();
              // If there is any unknown partition, create a map-reduce job for the filter to prune correctly
              if (partsList.getUnknownPartns().size() == 0) {
                parts = partsList.getConfirmedPartns();
                Iterator<Partition> iterParts = parts.iterator();
                while (iterParts.hasNext()) {
                  Partition part = iterParts.next();
                  listP.add(part.getPartitionPath().toString());
                  partP.add(Utilities.getPartitionDesc(part));
                  inputs.add(new ReadEntity(part));
                }
                fetch = new fetchWork(listP, partP, qb.getParseInfo().getOuterQueryLimit());
              }
            } catch (HiveException e) {
              // Has to use full name to make sure it does not conflict with org.apache.commons.lang.StringUtils
              LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
              throw new SemanticException(e.getMessage(), e);
            }
          }
        }
      }
      if (fetch != null) {
        fetchTask = TaskFactory.get(fetch, this.conf);
        setFetchTask(fetchTask);
        return;
      }
    }

    // In case of a select, use a fetch task instead of a move task
    if (qb.getIsQuery()) {
      if ((!loadTableWork.isEmpty()) || (loadFileWork.size() != 1))
        throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg());
      String cols = loadFileWork.get(0).getColumns();
      String colTypes = loadFileWork.get(0).getColumnTypes();
    
      fetch = new fetchWork(new Path(loadFileWork.get(0).getSourceDir()).toString(),
                            new tableDesc(LazySimpleSerDe.class, TextInputFormat.class,
                                           IgnoreKeyTextOutputFormat.class,
                                           Utilities.makeProperties(
                                            org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "" + Utilities.ctrlaCode,
                                            org.apache.hadoop.hive.serde.Constants.LIST_COLUMNS, cols,
                                            org.apache.hadoop.hive.serde.Constants.LIST_COLUMN_TYPES, colTypes)),    
                            qb.getParseInfo().getOuterQueryLimit());    

      fetchTask = TaskFactory.get(fetch, this.conf);
      setFetchTask(fetchTask);
    }
    else {
      // First we generate the move work as this needs to be made dependent on all
      // the tasks that have a file sink operation
      List<moveWork>  mv = new ArrayList<moveWork>();
      for (loadTableDesc ltd : loadTableWork)
        mvTask.add(TaskFactory.get(new moveWork(ltd, null, false), this.conf));
      for (loadFileDesc lfd : loadFileWork)
        mvTask.add(TaskFactory.get(new moveWork(null, lfd, false), this.conf));
    }

    // generate map reduce plans
    GenMRProcContext procCtx = 
      new GenMRProcContext(
        conf, new HashMap<Operator<? extends Serializable>, Task<? extends Serializable>>(),
        new ArrayList<Operator<? extends Serializable>>(),
        getParseContext(), mvTask, this.rootTasks,
        new LinkedHashMap<Operator<? extends Serializable>, GenMapRedCtx>(),
        inputs, outputs);

    // create a walker which walks the tree in a DFS manner while maintaining the operator stack. 
    // The dispatcher generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp(new String("R1"), "TS%"), new GenMRTableScan1());
    opRules.put(new RuleRegExp(new String("R2"), "TS%.*RS%"), new GenMRRedSink1());
    opRules.put(new RuleRegExp(new String("R3"), "RS%.*RS%"), new GenMRRedSink2());
    opRules.put(new RuleRegExp(new String("R4"), "FS%"), new GenMRFileSink1());
    opRules.put(new RuleRegExp(new String("R5"), "UNION%"), new GenMRUnion1());
    opRules.put(new RuleRegExp(new String("R6"), "UNION%.*RS%"), new GenMRRedSink3());
    opRules.put(new RuleRegExp(new String("R6"), "MAPJOIN%.*RS%"), new GenMRRedSink4());
    opRules.put(new RuleRegExp(new String("R7"), "TS%.*MAPJOIN%"), MapJoinFactory.getTableScanMapJoin());
    opRules.put(new RuleRegExp(new String("R8"), "RS%.*MAPJOIN%"), MapJoinFactory.getReduceSinkMapJoin());
    opRules.put(new RuleRegExp(new String("R9"), "UNION%.*MAPJOIN%"), MapJoinFactory.getUnionMapJoin());
    opRules.put(new RuleRegExp(new String("R10"), "MAPJOIN%.*MAPJOIN%"), MapJoinFactory.getMapJoinMapJoin());
    opRules.put(new RuleRegExp(new String("R11"), "MAPJOIN%SEL%"), MapJoinFactory.getMapJoin());

    // The dispatcher fires the processor corresponding to the closest matching rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(new GenMROperator(), opRules, procCtx);

    GraphWalker ogw = new GenMapRedWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(this.topOps.values());
    ogw.startWalking(topNodes, null);

    // reduce sink does not have any kids - since the plan by now has been broken up into multiple
    // tasks, iterate over all tasks.
    // For each task, go over all operators recursively
    for(Task<? extends Serializable> rootTask: rootTasks)
      breakTaskTree(rootTask);

    // For each task, set the key descriptor for the reducer
    for(Task<? extends Serializable> rootTask: rootTasks)
      setKeyDescTaskTree(rootTask);
  }

  // loop over all the tasks recursviely
  private void breakTaskTree(Task<? extends Serializable> task) { 
    
    if ((task instanceof MapRedTask) || (task instanceof ExecDriver)) {
      HashMap<String, Operator<? extends Serializable>> opMap = ((mapredWork)task.getWork()).getAliasToWork();
      if (!opMap.isEmpty())
        for (Operator<? extends Serializable> op: opMap.values()) {
          breakOperatorTree(op);
        }
    }

    if (task.getChildTasks() == null)
      return;
    
    for (Task<? extends Serializable> childTask :  task.getChildTasks())
      breakTaskTree(childTask);
  }
    
  // loop over all the operators recursviely
  private void breakOperatorTree(Operator<? extends Serializable> topOp) {
    if (topOp instanceof ReduceSinkOperator)
      topOp.setChildOperators(null);

    if (topOp.getChildOperators() == null)
      return;
    
    for (Operator<? extends Serializable> op: topOp.getChildOperators())
      breakOperatorTree(op);
  }

  // loop over all the tasks recursviely
  private void setKeyDescTaskTree(Task<? extends Serializable> task) { 
    
    if ((task instanceof MapRedTask) || (task instanceof ExecDriver)) {
      mapredWork work = (mapredWork)task.getWork();
      HashMap<String, Operator<? extends Serializable>> opMap = work.getAliasToWork();
      if (!opMap.isEmpty())
        for (Operator<? extends Serializable> op: opMap.values())
          GenMapRedUtils.setKeyAndValueDesc(work, op);
    }
    else if (task instanceof ConditionalTask) {
      List<Task<? extends Serializable>> listTasks = ((ConditionalTask)task).getListTasks();
      for (Task<? extends Serializable> tsk : listTasks)
        setKeyDescTaskTree(tsk);
    }

    if (task.getChildTasks() == null)
      return;
    
    for (Task<? extends Serializable> childTask :  task.getChildTasks())
      setKeyDescTaskTree(childTask);
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

    LOG.info("Starting Semantic Analysis");
    doPhase1(ast, qb, initPhase1Ctx());
    LOG.info("Completed phase 1 of Semantic Analysis");

    getMetaData(qb);
    LOG.info("Completed getting MetaData in Semantic Analysis");

    genPlan(qb);
    

    ParseContext pCtx = new ParseContext(conf, qb, ast, aliasToPruner, opToPartPruner, aliasToSamplePruner, topOps, 
    		                                 topSelOps, opParseCtx, joinContext, topToTable, loadTableWork, loadFileWork, 
    		                                 ctx, idToTableNameMap, destTableId, uCtx, listMapJoinOpsNoReducer);
  
    Optimizer optm = new Optimizer();
    optm.setPctx(pCtx);
    optm.initialize(conf);
    pCtx = optm.optimize();
    init(pCtx);
    qb = pCtx.getQB();
    
    // Do any partition pruning
    genPartitionPruners(qb);
    LOG.info("Completed partition pruning");
    
    // Do any sample pruning
    genSamplePruners(qb);
    LOG.info("Completed sample pruning");
    
    // At this point we have the complete operator tree
    // from which we want to find the reduce operator
    genMapRedTasks(qb);

    LOG.info("Completed plan generation");

    return;
  }

  /**
   * Generates and expression node descriptor for the expression passed in the arguments. This
   * function uses the row resolver and the metadata informatinon that are passed as arguments
   * to resolve the column names to internal names.
   * @param expr The expression
   * @param input The row resolver
   * @return exprNodeDesc
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  public static exprNodeDesc genExprNodeDesc(ASTNode expr, RowResolver input)
  throws SemanticException {
    //  We recursively create the exprNodeDesc.  Base cases:  when we encounter 
    //  a column ref, we convert that into an exprNodeColumnDesc;  when we encounter 
    //  a constant, we convert that into an exprNodeConstantDesc.  For others we just 
    //  build the exprNodeFuncDesc with recursively built children.
    
    //  If the current subExpression is pre-calculated, as in Group-By etc.
    ColumnInfo colInfo = input.get("", expr.toStringTree());
    if (colInfo != null) {
      return new exprNodeColumnDesc(colInfo.getType(), colInfo.getInternalName(),
                                    colInfo.getTabAlias(), colInfo.getIsPartitionCol());
    }

    // Create the walker, the rules dispatcher and the context.
    TypeCheckCtx tcCtx = new TypeCheckCtx(input);
    
    // create a walker which walks the tree in a DFS manner while maintaining the operator stack. The dispatcher
    // generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    StringBuilder sb = new StringBuilder();
    Formatter fm = new Formatter(sb);
    opRules.put(new RuleRegExp("R1", HiveParser.TOK_NULL + "%"), TypeCheckProcFactory.getNullExprProcessor());
    opRules.put(new RuleRegExp("R2", HiveParser.Number + "%"), TypeCheckProcFactory.getNumExprProcessor());
    opRules.put(new RuleRegExp("R3", HiveParser.Identifier + "%|" + 
                                     HiveParser.StringLiteral + "%|" + 
                                     HiveParser.TOK_CHARSETLITERAL + "%|" +
                                     HiveParser.KW_IF + "%|" + 
                                     HiveParser.KW_CASE + "%|" +
                                     HiveParser.KW_WHEN + "%"),
                               TypeCheckProcFactory.getStrExprProcessor());
    opRules.put(new RuleRegExp("R4", HiveParser.KW_TRUE + "%|" + HiveParser.KW_FALSE + "%"), 
                               TypeCheckProcFactory.getBoolExprProcessor());
    opRules.put(new RuleRegExp("R5", HiveParser.TOK_TABLE_OR_COL + "%"), TypeCheckProcFactory.getColumnExprProcessor());

    // The dispatcher fires the processor corresponding to the closest matching rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(TypeCheckProcFactory.getDefaultExprProcessor(), opRules, tcCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);
   
    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(expr);
    HashMap<Node, Object> nodeOutputs = new HashMap<Node, Object>();
    ogw.startWalking(topNodes, nodeOutputs);
    exprNodeDesc desc = (exprNodeDesc)nodeOutputs.get(expr);
    if (desc == null) {
      throw new SemanticException(tcCtx.getError());
    }

    return desc;
  }
  
  /**
   * Gets the table Alias for the column from the column name. This function throws
   * and exception in case the same column name is present in multiple table. The exception 
   * message indicates that the ambiguity could not be resolved.
   * 
   * @param qbm The metadata where the function looks for the table alias
   * @param colName The name of the non aliased column
   * @param pt The parse tree corresponding to the column(this is used for error reporting)
   * @return String
   * @throws SemanticException
   */
  static String getTabAliasForCol(QBMetaData qbm, String colName, ASTNode pt) 
  throws SemanticException {
    String tabAlias = null;
    boolean found = false;
    
    for(Map.Entry<String, Table> ent: qbm.getAliasToTable().entrySet()) {
      for(FieldSchema field: ent.getValue().getAllCols()) {
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
  
  
  public static ArrayList<exprNodeDesc> convertParameters(Method m, List<exprNodeDesc> parametersPassed) {
    
    ArrayList<exprNodeDesc> newParameters = new ArrayList<exprNodeDesc>();
    List<TypeInfo> parametersAccepted = TypeInfoUtils.getParameterTypeInfos(m);

    // 0 is the function name
    for (int i = 0; i < parametersPassed.size(); i++) {
      exprNodeDesc descPassed = parametersPassed.get(i);
      TypeInfo typeInfoPassed = descPassed.getTypeInfo();
      TypeInfo typeInfoAccepted = parametersAccepted.get(i);
      if (descPassed instanceof exprNodeNullDesc) {
        exprNodeConstantDesc newCh = new exprNodeConstantDesc(typeInfoAccepted, null);
        newParameters.add(newCh);
      } else if (typeInfoAccepted.equals(typeInfoPassed)
          || typeInfoAccepted.equals(TypeInfoFactory.unknownTypeInfo)
          || (typeInfoAccepted.equals(TypeInfoFactory.unknownMapTypeInfo) 
              && typeInfoPassed.getCategory().equals(Category.MAP))
          || (typeInfoAccepted.equals(TypeInfoFactory.unknownListTypeInfo)
              && typeInfoPassed.getCategory().equals(Category.LIST))
          ) {
        // no type conversion needed
        newParameters.add(descPassed);
      } else {
        // must be implicit type conversion
        TypeInfo to = typeInfoAccepted;
        if (!FunctionRegistry.implicitConvertable(typeInfoPassed, to)) {
          throw new RuntimeException("Internal exception: cannot convert from " + typeInfoPassed + " to " + to);
        }
        Method conv = FunctionRegistry.getUDFMethod(to.getTypeName(), typeInfoPassed);
        assert(conv != null);
        Class<? extends UDF> c = FunctionRegistry.getUDFClass(to.getTypeName());
        assert(c != null);
        
        // get the conversion method
        ArrayList<exprNodeDesc> conversionArg = new ArrayList<exprNodeDesc>(1);
        conversionArg.add(descPassed);
        newParameters.add(new exprNodeFuncDesc(to.getTypeName(), typeInfoAccepted, c, conv, conversionArg));
      }
    }

    return newParameters;
  }
  
  public void validate() throws SemanticException {
    // Check if the plan contains atleast one path.

    // validate all tasks
    for(Task<? extends Serializable> rootTask: rootTasks)
      validate(rootTask);
  }

  private void validate(Task<? extends Serializable> task) throws SemanticException {
    if ((task instanceof MapRedTask) || (task instanceof ExecDriver)) {
      mapredWork work = (mapredWork)task.getWork();

      if (conf.getVar(HiveConf.ConfVars.HIVEMAPREDMODE).equalsIgnoreCase("strict")) {
        if ((work.getPathToAliases() == null) || (work.getPathToAliases().isEmpty()))
          throw new SemanticException(ErrorMsg.NO_VALID_PARTN.getMsg());
      }
    }

    if (task.getChildTasks() == null)
      return;

    for (Task<? extends Serializable> childTask :  task.getChildTasks())
      validate(childTask);
  }

  @Override
  public Set<ReadEntity> getInputs() {
    return inputs;
  }
  
  public Set<WriteEntity> getOutputs() {
    return outputs;
  }
}
