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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.antlr.runtime.tree.TreeVisitor;
import org.antlr.runtime.tree.TreeVisitorAction;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptQuery;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.SemiJoinFilterTransposeRule;
import org.apache.calcite.rel.rules.SemiJoinJoinTransposeRule;
import org.apache.calcite.rel.rules.SemiJoinProjectTransposeRule;
import org.apache.calcite.rel.rules.UnionMergeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.CompositeList;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException.UnsupportedFeature;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveConfigContext;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveDefaultRelMetadataProvider;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveAlgorithmsConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveVolcanoPlanner;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveGroupingID;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSort;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveExpandDistinctAggregatesRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveFilterJoinRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveFilterProjectTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveFilterSetOpTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveInsertExchange4JoinRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveJoinAddNotNullRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveJoinCommuteRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveJoinPushTransitivePredicatesRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveJoinToMultiJoinRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HivePartitionPruneRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HivePreFilteringRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveRelFieldTrimmer;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveWindowingFixRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ASTConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.HiveOpConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.JoinCondTypeCheckProcFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.JoinTypeCheckCtx;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.PlanModifierForReturnPath;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.RexNodeConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.SqlFunctionConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.OrderExpression;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.OrderSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionExpression;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionSpec;
import org.apache.hadoop.hive.ql.parse.QBSubQuery.SubQueryType;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.RangeBoundarySpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowExpressionSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowFunctionSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowSpec;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class CalcitePlanner extends SemanticAnalyzer {

  private final AtomicInteger noColsMissingStats = new AtomicInteger(0);
  private List<FieldSchema> topLevelFieldSchema;
  private SemanticException semanticException;
  private boolean           runCBO             = true;

  public CalcitePlanner(HiveConf conf) throws SemanticException {
    super(conf);
    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CBO_ENABLED)) {
      runCBO = false;
    }
  }

  @Override
  @SuppressWarnings("nls")
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    if (runCBO) {
      PreCboCtx cboCtx = new PreCboCtx();
      super.analyzeInternal(ast, cboCtx);
    } else {
      super.analyzeInternal(ast);
    }
  }

  @Override
  @SuppressWarnings("rawtypes")
  Operator genOPTree(ASTNode ast, PlannerContext plannerCtx) throws SemanticException {
    Operator sinkOp = null;
    boolean skipCalcitePlan = false;

    if (!runCBO) {
      skipCalcitePlan = true;
    } else {
      PreCboCtx cboCtx = (PreCboCtx) plannerCtx;

      // Note: for now, we don't actually pass the queryForCbo to CBO, because
      // it accepts qb, not AST, and can also access all the private stuff in
      // SA. We rely on the fact that CBO ignores the unknown tokens (create
      // table, destination), so if the query is otherwise ok, it is as if we
      // did remove those and gave CBO the proper AST. That is kinda hacky.
      ASTNode queryForCbo = ast;
      if (cboCtx.type == PreCboCtx.Type.CTAS) {
        queryForCbo = cboCtx.nodeOfInterest; // nodeOfInterest is the query
      }
      runCBO = canCBOHandleAst(queryForCbo, getQB(), cboCtx);

      if (runCBO) {
        disableJoinMerge = true;
        boolean reAnalyzeAST = false;

        try {
          if (this.conf.getBoolVar(HiveConf.ConfVars.HIVE_CBO_RETPATH_HIVEOP)) {
            sinkOp = getOptimizedHiveOPDag();
          } else {
            // 1. Gen Optimized AST
            ASTNode newAST = getOptimizedAST();

            // 1.1. Fix up the query for insert/ctas
            newAST = fixUpCtasAndInsertAfterCbo(ast, newAST, cboCtx);

            // 2. Regen OP plan from optimized AST
            init(false);
            if (cboCtx.type == PreCboCtx.Type.CTAS) {
              // Redo create-table analysis, because it's not part of doPhase1.
              setAST(newAST);
              newAST = reAnalyzeCtasAfterCbo(newAST);
            }
          Phase1Ctx ctx_1 = initPhase1Ctx();
          if (!doPhase1(newAST, getQB(), ctx_1, null)) {
            throw new RuntimeException("Couldn't do phase1 on CBO optimized query plan");
          }
          // unfortunately making prunedPartitions immutable is not possible
          // here with SemiJoins not all tables are costed in CBO, so their
          // PartitionList is not evaluated until the run phase.
          getMetaData(getQB());

          disableJoinMerge = false;
          sinkOp = genPlan(getQB());
          LOG.info("CBO Succeeded; optimized logical plan.");
          this.ctx.setCboInfo("Plan optimized by CBO.");
          this.ctx.setCboSucceeded(true);
          LOG.debug(newAST.dump());
          }
        } catch (Exception e) {
          boolean isMissingStats = noColsMissingStats.get() > 0;
          if (isMissingStats) {
            LOG.error("CBO failed due to missing column stats (see previous errors), skipping CBO");
            this.ctx
                .setCboInfo("Plan not optimized by CBO due to missing statistics. Please check log for more details.");
          } else {
            LOG.error("CBO failed, skipping CBO. ", e);
            if (e instanceof CalciteSemanticException) {
              CalciteSemanticException calciteSemanticException = (CalciteSemanticException) e;
              UnsupportedFeature unsupportedFeature = calciteSemanticException
                  .getUnsupportedFeature();
              if (unsupportedFeature != null) {
                this.ctx.setCboInfo("Plan not optimized by CBO due to missing feature ["
                    + unsupportedFeature + "].");
              } else {
                this.ctx.setCboInfo("Plan not optimized by CBO.");
              }
            } else {
              this.ctx.setCboInfo("Plan not optimized by CBO.");
            }
          }
          if (!conf.getBoolVar(ConfVars.HIVE_IN_TEST) || isMissingStats
              || e instanceof CalciteSemanticException) {
            reAnalyzeAST = true;
          } else if (e instanceof SemanticException) {
            throw (SemanticException) e;
          } else if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
          } else {
            throw new SemanticException(e);
          }
        } finally {
          runCBO = false;
          disableJoinMerge = false;
          if (reAnalyzeAST) {
            init(true);
            prunedPartitions.clear();
            // Assumption: At this point Parse Tree gen & resolution will always
            // be true (since we started out that way).
            super.genResolvedParseTree(ast, new PlannerContext());
            skipCalcitePlan = true;
          }
        }
      } else {
        this.ctx.setCboInfo("Plan not optimized by CBO.");
        skipCalcitePlan = true;
      }
    }

    if (skipCalcitePlan) {
      sinkOp = super.genOPTree(ast, plannerCtx);
    }

    return sinkOp;
  }

  /**
   * Can CBO handle the given AST?
   *
   * @param ast
   *          Top level AST
   * @param qb
   *          top level QB corresponding to the AST
   * @param cboCtx
   * @param semAnalyzer
   * @return boolean
   *
   *         Assumption:<br>
   *         If top level QB is query then everything below it must also be
   *         Query.
   */
  boolean canCBOHandleAst(ASTNode ast, QB qb, PreCboCtx cboCtx) {
    int root = ast.getToken().getType();
    boolean needToLogMessage = STATIC_LOG.isInfoEnabled();
    boolean isSupportedRoot = root == HiveParser.TOK_QUERY || root == HiveParser.TOK_EXPLAIN
        || qb.isCTAS();
    boolean isSupportedType = qb.getIsQuery() || qb.isCTAS()
        || cboCtx.type == PreCboCtx.Type.INSERT;
    boolean noBadTokens = HiveCalciteUtil.validateASTForUnsupportedTokens(ast);
    boolean result = isSupportedRoot && isSupportedType && getCreateViewDesc() == null
        && noBadTokens;

    if (!result) {
      if (needToLogMessage) {
        String msg = "";
        if (!isSupportedRoot) {
          msg += "doesn't have QUERY or EXPLAIN as root and not a CTAS; ";
        }
        if (!isSupportedType) {
          msg += "is not a query, CTAS, or insert; ";
        }
        if (getCreateViewDesc() != null) {
          msg += "has create view; ";
        }
        if (!noBadTokens) {
          msg += "has unsupported tokens; ";
        }

        if (msg.isEmpty()) {
          msg += "has some unspecified limitations; ";
        }
        STATIC_LOG.info("Not invoking CBO because the statement "
            + msg.substring(0, msg.length() - 2));
      }
      return false;
    }
    // Now check QB in more detail. canHandleQbForCbo returns null if query can
    // be handled.
    String msg = CalcitePlanner.canHandleQbForCbo(queryProperties, conf, true, needToLogMessage, qb);
    if (msg == null) {
      return true;
    }
    if (needToLogMessage) {
      STATIC_LOG.info("Not invoking CBO because the statement "
          + msg.substring(0, msg.length() - 2));
    }
    return false;
  }

  /**
   * Checks whether Calcite can handle the query.
   *
   * @param queryProperties
   * @param conf
   * @param topLevelQB
   *          Does QB corresponds to top most query block?
   * @param verbose
   *          Whether return value should be verbose in case of failure.
   * @return null if the query can be handled; non-null reason string if it
   *         cannot be.
   *
   *         Assumption:<br>
   *         1. If top level QB is query then everything below it must also be
   *         Query<br>
   *         2. Nested Subquery will return false for qbToChk.getIsQuery()
   */
  static String canHandleQbForCbo(QueryProperties queryProperties, HiveConf conf,
      boolean topLevelQB, boolean verbose, QB qb) {
    boolean isInTest = conf.getBoolVar(ConfVars.HIVE_IN_TEST);
    boolean isStrictTest = isInTest
        && !conf.getVar(ConfVars.HIVEMAPREDMODE).equalsIgnoreCase("nonstrict");
    boolean hasEnoughJoins = !topLevelQB || (queryProperties.getJoinCount() > 1) || isInTest || distinctExprsExists(qb);

    if (!isStrictTest && hasEnoughJoins && !queryProperties.hasClusterBy()
        && !queryProperties.hasDistributeBy() && !queryProperties.hasSortBy()
        && !queryProperties.hasPTF() && !queryProperties.usesScript()
        && !queryProperties.hasMultiDestQuery() && !queryProperties.hasLateralViews()) {
      // Ok to run CBO.
      return null;
    }

    // Not ok to run CBO, build error message.
    String msg = "";
    if (verbose) {
      if (isStrictTest)
        msg += "is in test running in mode other than nonstrict; ";
      if (!hasEnoughJoins)
        msg += "has too few joins; ";
      if (queryProperties.hasClusterBy())
        msg += "has cluster by; ";
      if (queryProperties.hasDistributeBy())
        msg += "has distribute by; ";
      if (queryProperties.hasSortBy())
        msg += "has sort by; ";
      if (queryProperties.hasPTF())
        msg += "has PTF; ";
      if (queryProperties.usesScript())
        msg += "uses scripts; ";
      if (queryProperties.hasMultiDestQuery())
        msg += "is a multi-destination query; ";
      if (queryProperties.hasLateralViews())
        msg += "has lateral views; ";

      if (msg.isEmpty())
        msg += "has some unspecified limitations; ";
    }
    return msg;
  }

  @Override
  boolean continueJoinMerge() {
    return !runCBO;
  }

  @Override
  String fixCtasColumnName(String colName) {
    if (runCBO) {
      int lastDot = colName.lastIndexOf('.');
      if (lastDot < 0)
        return colName; // alias is not fully qualified
      String nqColumnName = colName.substring(lastDot + 1);
      STATIC_LOG.debug("Replacing " + colName + " (produced by CBO) by " + nqColumnName);
      return nqColumnName;
    }

    return super.fixCtasColumnName(colName);
  }

  /**
   * The context that doPhase1 uses to populate information pertaining to CBO
   * (currently, this is used for CTAS and insert-as-select).
   */
  static class PreCboCtx extends PlannerContext {
    enum Type {
      NONE, INSERT, CTAS, UNEXPECTED
    }

    private ASTNode nodeOfInterest;
    private Type    type = Type.NONE;

    private void set(Type type, ASTNode ast) {
      if (this.type != Type.NONE) {
        STATIC_LOG.warn("Setting " + type + " when already " + this.type + "; node " + ast.dump()
            + " vs old node " + nodeOfInterest.dump());
        this.type = Type.UNEXPECTED;
        return;
      }
      this.type = type;
      this.nodeOfInterest = ast;
    }

    @Override
    void setCTASToken(ASTNode child) {
      set(PreCboCtx.Type.CTAS, child);
    }

    @Override
    void setInsertToken(ASTNode ast, boolean isTmpFileDest) {
      if (!isTmpFileDest) {
        set(PreCboCtx.Type.INSERT, ast);
      }
    }
  }

  ASTNode fixUpCtasAndInsertAfterCbo(ASTNode originalAst, ASTNode newAst, PreCboCtx cboCtx)
      throws SemanticException {
    switch (cboCtx.type) {

    case NONE:
      // nothing to do
      return newAst;

    case CTAS: {
      // Patch the optimized query back into original CTAS AST, replacing the
      // original query.
      replaceASTChild(cboCtx.nodeOfInterest, newAst);
      return originalAst;
    }

    case INSERT: {
      // We need to patch the dest back to original into new query.
      // This makes assumptions about the structure of the AST.
      ASTNode newDest = new ASTSearcher().simpleBreadthFirstSearch(newAst, HiveParser.TOK_QUERY,
          HiveParser.TOK_INSERT, HiveParser.TOK_DESTINATION);
      if (newDest == null) {
        LOG.error("Cannot find destination after CBO; new ast is " + newAst.dump());
        throw new SemanticException("Cannot find destination after CBO");
      }
      replaceASTChild(newDest, cboCtx.nodeOfInterest);
      return newAst;
    }

    default:
      throw new AssertionError("Unexpected type " + cboCtx.type);
    }
  }

  ASTNode reAnalyzeCtasAfterCbo(ASTNode newAst) throws SemanticException {
    // analyzeCreateTable uses this.ast, but doPhase1 doesn't, so only reset it
    // here.
    newAst = analyzeCreateTable(newAst, getQB(), null);
    if (newAst == null) {
      LOG.error("analyzeCreateTable failed to initialize CTAS after CBO;" + " new ast is "
          + getAST().dump());
      throw new SemanticException("analyzeCreateTable failed to initialize CTAS after CBO");
    }
    return newAst;
  }

  /**
   * Performs breadth-first search of the AST for a nested set of tokens. Tokens
   * don't have to be each others' direct children, they can be separated by
   * layers of other tokens. For each token in the list, the first one found is
   * matched and there's no backtracking; thus, if AST has multiple instances of
   * some token, of which only one matches, it is not guaranteed to be found. We
   * use this for simple things. Not thread-safe - reuses searchQueue.
   */
  static class ASTSearcher {
    private final LinkedList<ASTNode> searchQueue = new LinkedList<ASTNode>();

    public ASTNode simpleBreadthFirstSearch(ASTNode ast, int... tokens) {
      searchQueue.clear();
      searchQueue.add(ast);
      for (int i = 0; i < tokens.length; ++i) {
        boolean found = false;
        int token = tokens[i];
        while (!searchQueue.isEmpty() && !found) {
          ASTNode next = searchQueue.poll();
          found = next.getType() == token;
          if (found) {
            if (i == tokens.length - 1)
              return next;
            searchQueue.clear();
          }
          for (int j = 0; j < next.getChildCount(); ++j) {
            searchQueue.add((ASTNode) next.getChild(j));
          }
        }
        if (!found)
          return null;
      }
      return null;
    }
  }

  private static void replaceASTChild(ASTNode child, ASTNode newChild) {
    ASTNode parent = (ASTNode) child.parent;
    int childIndex = child.childIndex;
    parent.deleteChild(childIndex);
    parent.insertChild(childIndex, newChild);
  }

  /**
   * Get Optimized AST for the given QB tree in the semAnalyzer.
   *
   * @return Optimized operator tree translated in to Hive AST
   * @throws SemanticException
   */
  ASTNode getOptimizedAST() throws SemanticException {
    ASTNode optiqOptimizedAST = null;
    RelNode optimizedOptiqPlan = null;
    CalcitePlannerAction calcitePlannerAction = new CalcitePlannerAction(prunedPartitions);

    try {
      optimizedOptiqPlan = Frameworks.withPlanner(calcitePlannerAction, Frameworks
          .newConfigBuilder().typeSystem(new HiveTypeSystemImpl()).build());
    } catch (Exception e) {
      rethrowCalciteException(e);
      throw new AssertionError("rethrowCalciteException didn't throw for " + e.getMessage());
    }
    optiqOptimizedAST = ASTConverter.convert(optimizedOptiqPlan, topLevelFieldSchema);

    return optiqOptimizedAST;
  }

  /**
   * Get Optimized Hive Operator DAG for the given QB tree in the semAnalyzer.
   *
   * @return Optimized Hive operator tree
   * @throws SemanticException
   */
  Operator getOptimizedHiveOPDag() throws SemanticException {
    RelNode optimizedOptiqPlan = null;
    CalcitePlannerAction calcitePlannerAction = new CalcitePlannerAction(prunedPartitions);

    try {
      optimizedOptiqPlan = Frameworks.withPlanner(calcitePlannerAction, Frameworks
          .newConfigBuilder().typeSystem(new HiveTypeSystemImpl()).build());
    } catch (Exception e) {
      rethrowCalciteException(e);
      throw new AssertionError("rethrowCalciteException didn't throw for " + e.getMessage());
    }

    RelNode modifiedOptimizedOptiqPlan = PlanModifierForReturnPath.convertOpTree(
            introduceProjectIfNeeded(optimizedOptiqPlan), topLevelFieldSchema);

    LOG.debug("Translating the following plan:\n" + RelOptUtil.toString(modifiedOptimizedOptiqPlan));
    Operator<?> hiveRoot = new HiveOpConverter(this, conf, unparseTranslator, topOps,
        conf.getVar(HiveConf.ConfVars.HIVEMAPREDMODE).equalsIgnoreCase("strict")).convert(modifiedOptimizedOptiqPlan);
    RowResolver hiveRootRR = genRowResolver(hiveRoot, getQB());
    opParseCtx.put(hiveRoot, new OpParseContext(hiveRootRR));
    return genFileSinkPlan(getQB().getParseInfo().getClauseNames().iterator().next(), getQB(), hiveRoot);
  }

  private RelNode introduceProjectIfNeeded(RelNode optimizedOptiqPlan)
      throws CalciteSemanticException {
    RelNode parent = null;
    RelNode input = optimizedOptiqPlan;
    RelNode newRoot = optimizedOptiqPlan;

    while (!(input instanceof Project) && (input instanceof Sort)) {
      parent = input;
      input = input.getInput(0);
    }

    if (!(input instanceof Project)) {
      HiveProject hpRel = HiveProject.create(input,
          HiveCalciteUtil.getProjsFromBelowAsInputRef(input), input.getRowType().getFieldNames());
      if (input == optimizedOptiqPlan) {
        newRoot = hpRel;
      } else {
        parent.replaceInput(0, hpRel);
      }
    }

    return newRoot;
  }

  /***
   * Unwraps Calcite Invocation exceptions coming meta data provider chain and
   * obtains the real cause.
   *
   * @param Exception
   */
  private void rethrowCalciteException(Exception e) throws SemanticException {
    Throwable first = (semanticException != null) ? semanticException : e, current = first, cause = current
        .getCause();
    while (cause != null) {
      Throwable causeOfCause = cause.getCause();
      if (current == first && causeOfCause == null && isUselessCause(first)) {
        // "cause" is a root cause, and "e"/"first" is a useless
        // exception it's wrapped in.
        first = cause;
        break;
      } else if (causeOfCause != null && isUselessCause(cause)
          && ExceptionHelper.resetCause(current, causeOfCause)) {
        // "cause" was a useless intermediate cause and was replace it
        // with its own cause.
        cause = causeOfCause;
        continue; // do loop once again with the new cause of "current"
      }
      current = cause;
      cause = current.getCause();
    }

    if (first instanceof RuntimeException) {
      throw (RuntimeException) first;
    } else if (first instanceof SemanticException) {
      throw (SemanticException) first;
    }
    throw new RuntimeException(first);
  }

  private static class ExceptionHelper {
    private static final Field CAUSE_FIELD = getField(Throwable.class, "cause"),
        TARGET_FIELD = getField(InvocationTargetException.class, "target"),
        MESSAGE_FIELD = getField(Throwable.class, "detailMessage");

    private static Field getField(Class<?> clazz, String name) {
      try {
        Field f = clazz.getDeclaredField(name);
        f.setAccessible(true);
        return f;
      } catch (Throwable t) {
        return null;
      }
    }

    public static boolean resetCause(Throwable target, Throwable newCause) {
      try {
        if (MESSAGE_FIELD == null)
          return false;
        Field field = (target instanceof InvocationTargetException) ? TARGET_FIELD : CAUSE_FIELD;
        if (field == null)
          return false;

        Throwable oldCause = target.getCause();
        String oldMsg = target.getMessage();
        field.set(target, newCause);
        if (oldMsg != null && oldMsg.equals(oldCause.toString())) {
          MESSAGE_FIELD.set(target, newCause == null ? null : newCause.toString());
        }
      } catch (Throwable se) {
        return false;
      }
      return true;
    }
  }

  private boolean isUselessCause(Throwable t) {
    return t instanceof RuntimeException || t instanceof InvocationTargetException
        || t instanceof UndeclaredThrowableException;
  }

  private RowResolver genRowResolver(Operator op, QB qb) {
    RowResolver rr = new RowResolver();
    String subqAlias = (qb.getAliases().size() == 1 && qb.getSubqAliases().size() == 1) ? qb
        .getAliases().get(0) : null;

    for (ColumnInfo ci : op.getSchema().getSignature()) {
      try {
        rr.putWithCheck((subqAlias != null) ? subqAlias : ci.getTabAlias(),
            ci.getAlias() != null ? ci.getAlias() : ci.getInternalName(), ci.getInternalName(),
            new ColumnInfo(ci));
      } catch (SemanticException e) {
        throw new RuntimeException(e);
      }
    }

    return rr;
  }

  /**
   * Code responsible for Calcite plan generation and optimization.
   */
  private class CalcitePlannerAction implements Frameworks.PlannerAction<RelNode> {
    private RelOptCluster                                 cluster;
    private RelOptSchema                                  relOptSchema;
    private final Map<String, PrunedPartitionList>              partitionCache;

    // TODO: Do we need to keep track of RR, ColNameToPosMap for every op or
    // just last one.
    LinkedHashMap<RelNode, RowResolver>                   relToHiveRR                   = new LinkedHashMap<RelNode, RowResolver>();
    LinkedHashMap<RelNode, ImmutableMap<String, Integer>> relToHiveColNameCalcitePosMap = new LinkedHashMap<RelNode, ImmutableMap<String, Integer>>();

    CalcitePlannerAction(Map<String, PrunedPartitionList> partitionCache) {
      this.partitionCache = partitionCache;
    }

    @Override
    public RelNode apply(RelOptCluster cluster, RelOptSchema relOptSchema, SchemaPlus rootSchema) {
      RelNode calciteGenPlan = null;
      RelNode calcitePreCboPlan = null;
      RelNode calciteOptimizedPlan = null;

      /*
       * recreate cluster, so that it picks up the additional traitDef
       */
      final Double maxSplitSize = (double) HiveConf.getLongVar(
              conf, HiveConf.ConfVars.MAPREDMAXSPLITSIZE);
      final Double maxMemory = (double) HiveConf.getLongVar(
              conf, HiveConf.ConfVars.HIVECONVERTJOINNOCONDITIONALTASKTHRESHOLD);
      HiveAlgorithmsConf algorithmsConf = new HiveAlgorithmsConf(maxSplitSize, maxMemory);
      HiveConfigContext confContext = new HiveConfigContext(algorithmsConf);
      RelOptPlanner planner = HiveVolcanoPlanner.createPlanner(confContext);
      final RelOptQuery query = new RelOptQuery(planner);
      final RexBuilder rexBuilder = cluster.getRexBuilder();
      cluster = query.createCluster(rexBuilder.getTypeFactory(), rexBuilder);

      this.cluster = cluster;
      this.relOptSchema = relOptSchema;

      // 1. Gen Calcite Plan
      try {
        calciteGenPlan = genLogicalPlan(getQB(), true);
        topLevelFieldSchema = SemanticAnalyzer.convertRowSchemaToResultSetSchema(
            relToHiveRR.get(calciteGenPlan),
            HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_RESULTSET_USE_UNIQUE_COLUMN_NAMES));
      } catch (SemanticException e) {
        semanticException = e;
        throw new RuntimeException(e);
      }

      // Create MD provider
      HiveDefaultRelMetadataProvider mdProvider = new HiveDefaultRelMetadataProvider(conf);

      // 2. Apply Pre Join Order optimizations
      calcitePreCboPlan = applyPreJoinOrderingTransforms(calciteGenPlan,
              mdProvider.getMetadataProvider());

      // 3. Appy Join Order Optimizations using Hep Planner (MST Algorithm)
      List<RelMetadataProvider> list = Lists.newArrayList();
      list.add(mdProvider.getMetadataProvider());
      RelTraitSet desiredTraits = cluster
          .traitSetOf(HiveRelNode.CONVENTION, RelCollations.EMPTY);

      HepProgram hepPgm = null;
      HepProgramBuilder hepPgmBldr = new HepProgramBuilder().addMatchOrder(HepMatchOrder.BOTTOM_UP)
          .addRuleInstance(new JoinToMultiJoinRule(HiveJoin.class));
      hepPgmBldr.addRuleInstance(new LoptOptimizeJoinRule(HiveJoin.HIVE_JOIN_FACTORY,
          HiveProject.DEFAULT_PROJECT_FACTORY, HiveFilter.DEFAULT_FILTER_FACTORY));

      hepPgmBldr.addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE);
      hepPgmBldr.addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE);
      hepPgmBldr.addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE);
      hepPgmBldr.addRuleInstance(ProjectRemoveRule.INSTANCE);
      hepPgmBldr.addRuleInstance(UnionMergeRule.INSTANCE);
      hepPgmBldr.addRuleInstance(new ProjectMergeRule(false, HiveProject.DEFAULT_PROJECT_FACTORY));

      hepPgm = hepPgmBldr.build();
      HepPlanner hepPlanner = new HepPlanner(hepPgm);

      hepPlanner.registerMetadataProviders(list);
      RelMetadataProvider chainedProvider = ChainedRelMetadataProvider.of(list);
      cluster.setMetadataProvider(new CachingRelMetadataProvider(chainedProvider, hepPlanner));

      RelNode rootRel = calcitePreCboPlan;
      hepPlanner.setRoot(rootRel);
      if (!calcitePreCboPlan.getTraitSet().equals(desiredTraits)) {
        rootRel = hepPlanner.changeTraits(calcitePreCboPlan, desiredTraits);
      }
      hepPlanner.setRoot(rootRel);

      calciteOptimizedPlan = hepPlanner.findBestExp();

      // 4. Run rule to try to remove projects on top of join operators
      hepPgmBldr = new HepProgramBuilder().addMatchOrder(HepMatchOrder.BOTTOM_UP);
      hepPgmBldr.addRuleInstance(HiveJoinCommuteRule.INSTANCE);
      hepPlanner = new HepPlanner(hepPgmBldr.build());
      hepPlanner.registerMetadataProviders(list);
      cluster.setMetadataProvider(new CachingRelMetadataProvider(chainedProvider, hepPlanner));
      hepPlanner.setRoot(calciteOptimizedPlan);
      calciteOptimizedPlan = hepPlanner.findBestExp();

      // 5. Run rule to fix windowing issue when it is done over
      // aggregation columns (HIVE-10627)
      hepPgmBldr = new HepProgramBuilder().addMatchOrder(HepMatchOrder.BOTTOM_UP);
      hepPgmBldr.addRuleInstance(HiveWindowingFixRule.INSTANCE);
      hepPlanner = new HepPlanner(hepPgmBldr.build());
      hepPlanner.registerMetadataProviders(list);
      cluster.setMetadataProvider(new CachingRelMetadataProvider(chainedProvider, hepPlanner));
      hepPlanner.setRoot(calciteOptimizedPlan);
      calciteOptimizedPlan = hepPlanner.findBestExp();

      // 6. Run rules to aid in translation from Calcite tree to Hive tree
      if (HiveConf.getBoolVar(conf, ConfVars.HIVE_CBO_RETPATH_HIVEOP)) {
        // 6.1. Merge join into multijoin operators (if possible)
        hepPgmBldr = new HepProgramBuilder().addMatchOrder(HepMatchOrder.BOTTOM_UP);
        hepPgmBldr.addRuleInstance(HiveJoinToMultiJoinRule.INSTANCE);
        hepPlanner = new HepPlanner(hepPgmBldr.build());
        hepPlanner.registerMetadataProviders(list);
        cluster.setMetadataProvider(new CachingRelMetadataProvider(chainedProvider, hepPlanner));
        hepPlanner.setRoot(calciteOptimizedPlan);
        calciteOptimizedPlan = hepPlanner.findBestExp();

        // 6.2.  Introduce exchange operators below join/multijoin operators
        hepPgmBldr = new HepProgramBuilder().addMatchOrder(HepMatchOrder.BOTTOM_UP);
        hepPgmBldr.addRuleInstance(HiveInsertExchange4JoinRule.EXCHANGE_BELOW_JOIN);
        hepPgmBldr.addRuleInstance(HiveInsertExchange4JoinRule.EXCHANGE_BELOW_MULTIJOIN);
        hepPlanner = new HepPlanner(hepPgmBldr.build());
        hepPlanner.registerMetadataProviders(list);
        cluster.setMetadataProvider(new CachingRelMetadataProvider(chainedProvider, hepPlanner));
        hepPlanner.setRoot(calciteOptimizedPlan);
        calciteOptimizedPlan = hepPlanner.findBestExp();
      }

      if (LOG.isDebugEnabled() && !conf.getBoolVar(ConfVars.HIVE_IN_TEST)) {
        LOG.debug("CBO Planning details:\n");
        LOG.debug("Original Plan:\n" + RelOptUtil.toString(calciteGenPlan));
        LOG.debug("Plan After PPD, PartPruning, ColumnPruning:\n"
            + RelOptUtil.toString(calcitePreCboPlan));
        LOG.debug("Plan After Join Reordering:\n"
            + RelOptUtil.toString(calciteOptimizedPlan, SqlExplainLevel.ALL_ATTRIBUTES));
      }

      return calciteOptimizedPlan;
    }

    /**
     * Perform all optimizations before Join Ordering.
     *
     * @param basePlan
     *          original plan
     * @param mdProvider
     *          meta data provider
     * @return
     */
    private RelNode applyPreJoinOrderingTransforms(RelNode basePlan, RelMetadataProvider mdProvider) {

      // TODO: Decorelation of subquery should be done before attempting
      // Partition Pruning; otherwise Expression evaluation may try to execute
      // corelated sub query.

      //0. Distinct aggregate rewrite
      // Run this optimization early, since it is expanding the operator pipeline.
      if (conf.getVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez") &&
          conf.getBoolVar(HiveConf.ConfVars.HIVEOPTIMIZEDISTINCTREWRITE)) {
        // Its not clear, if this rewrite is always performant on MR, since extra map phase
        // introduced for 2nd MR job may offset gains of this multi-stage aggregation.
        // We need a cost model for MR to enable this on MR.
        basePlan = hepPlan(basePlan, true, mdProvider, HiveExpandDistinctAggregatesRule.INSTANCE);
      }

      // 1. Push Down Semi Joins
      basePlan = hepPlan(basePlan, true, mdProvider, SemiJoinJoinTransposeRule.INSTANCE,
          SemiJoinFilterTransposeRule.INSTANCE, SemiJoinProjectTransposeRule.INSTANCE);

      // 2. Add not null filters
      if (conf.getBoolVar(HiveConf.ConfVars.HIVE_CBO_RETPATH_HIVEOP)) {
        basePlan = hepPlan(basePlan, true, mdProvider, HiveJoinAddNotNullRule.INSTANCE);
      }

      // 3. Constant propagation, common filter extraction, and PPD
      basePlan = hepPlan(basePlan, true, mdProvider,
          ReduceExpressionsRule.PROJECT_INSTANCE,
          ReduceExpressionsRule.FILTER_INSTANCE,
          ReduceExpressionsRule.JOIN_INSTANCE,
          HivePreFilteringRule.INSTANCE,
          new HiveFilterProjectTransposeRule(Filter.class, HiveFilter.DEFAULT_FILTER_FACTORY,
                  HiveProject.class, HiveProject.DEFAULT_PROJECT_FACTORY),
          new HiveFilterSetOpTransposeRule(HiveFilter.DEFAULT_FILTER_FACTORY),
          HiveFilterJoinRule.JOIN,
          HiveFilterJoinRule.FILTER_ON_JOIN,
          new FilterAggregateTransposeRule(Filter.class,
              HiveFilter.DEFAULT_FILTER_FACTORY, Aggregate.class));

      // 4. Transitive inference & Partition Pruning
      basePlan = hepPlan(basePlan, false, mdProvider, new HiveJoinPushTransitivePredicatesRule(
          Join.class, HiveFilter.DEFAULT_FILTER_FACTORY),
          new HivePartitionPruneRule(conf));

      // 5. Projection Pruning
      HiveRelFieldTrimmer fieldTrimmer = new HiveRelFieldTrimmer(null, HiveProject.DEFAULT_PROJECT_FACTORY,
          HiveFilter.DEFAULT_FILTER_FACTORY, HiveJoin.HIVE_JOIN_FACTORY,
          RelFactories.DEFAULT_SEMI_JOIN_FACTORY, HiveSort.HIVE_SORT_REL_FACTORY,
          HiveAggregate.HIVE_AGGR_REL_FACTORY, HiveUnion.UNION_REL_FACTORY);
      basePlan = fieldTrimmer.trim(basePlan);

      // 6. Rerun PPD through Project as column pruning would have introduced DT
      // above scans
      basePlan = hepPlan(basePlan, true, mdProvider,
          new FilterProjectTransposeRule(Filter.class, HiveFilter.DEFAULT_FILTER_FACTORY,
              HiveProject.class, HiveProject.DEFAULT_PROJECT_FACTORY));

      return basePlan;
    }

    /**
     * Run the HEP Planner with the given rule set.
     *
     * @param basePlan
     * @param followPlanChanges
     * @param mdProvider
     * @param rules
     * @return optimized RelNode
     */
    private RelNode hepPlan(RelNode basePlan, boolean followPlanChanges,
        RelMetadataProvider mdProvider, RelOptRule... rules) {

      RelNode optimizedRelNode = basePlan;
      HepProgramBuilder programBuilder = new HepProgramBuilder();
      if (followPlanChanges) {
        programBuilder.addMatchOrder(HepMatchOrder.TOP_DOWN);
        programBuilder = programBuilder.addRuleCollection(ImmutableList.copyOf(rules));
      } else {
        // TODO: Should this be also TOP_DOWN?
        for (RelOptRule r : rules)
          programBuilder.addRuleInstance(r);
      }

      HepPlanner planner = new HepPlanner(programBuilder.build());
      List<RelMetadataProvider> list = Lists.newArrayList();
      list.add(mdProvider);
      planner.registerMetadataProviders(list);
      RelMetadataProvider chainedProvider = ChainedRelMetadataProvider.of(list);
      basePlan.getCluster().setMetadataProvider(
          new CachingRelMetadataProvider(chainedProvider, planner));

      // Executor is required for constant-reduction rules; see [CALCITE-566]
      final RexExecutorImpl executor =
          new RexExecutorImpl(Schemas.createDataContext(null));
      basePlan.getCluster().getPlanner().setExecutor(executor);

      planner.setRoot(basePlan);
      optimizedRelNode = planner.findBestExp();

      return optimizedRelNode;
    }

    @SuppressWarnings("nls")
    private RelNode genUnionLogicalPlan(String unionalias, String leftalias, RelNode leftRel,
        String rightalias, RelNode rightRel) throws SemanticException {
      HiveUnion unionRel = null;

      // 1. Get Row Resolvers, Column map for original left and right input of
      // Union Rel
      RowResolver leftRR = this.relToHiveRR.get(leftRel);
      RowResolver rightRR = this.relToHiveRR.get(rightRel);
      HashMap<String, ColumnInfo> leftmap = leftRR.getFieldMap(leftalias);
      HashMap<String, ColumnInfo> rightmap = rightRR.getFieldMap(rightalias);

      // 2. Validate that Union is feasible according to Hive (by using type
      // info from RR)
      if (leftmap.size() != rightmap.size()) {
        throw new SemanticException("Schema of both sides of union should match.");
      }

      ASTNode tabref = getQB().getAliases().isEmpty() ? null : getQB().getParseInfo()
          .getSrcForAlias(getQB().getAliases().get(0));

      // 3. construct Union Output RR using original left & right Input
      RowResolver unionoutRR = new RowResolver();

      Iterator<Map.Entry<String, ColumnInfo>> lIter = leftmap.entrySet().iterator();
      Iterator<Map.Entry<String, ColumnInfo>> rIter = rightmap.entrySet().iterator();
      while (lIter.hasNext()) {
        Map.Entry<String, ColumnInfo> lEntry = lIter.next();
        Map.Entry<String, ColumnInfo> rEntry = rIter.next();
        ColumnInfo lInfo = lEntry.getValue();
        ColumnInfo rInfo = rEntry.getValue();

        String field = lEntry.getKey();
        // try widening conversion, otherwise fail union
        TypeInfo commonTypeInfo = FunctionRegistry.getCommonClassForUnionAll(lInfo.getType(),
            rInfo.getType());
        if (commonTypeInfo == null) {
          throw new SemanticException(generateErrorMessage(tabref,
              "Schema of both sides of union should match: Column " + field
                  + " is of type " + lInfo.getType().getTypeName()
                  + " on first table and type " + rInfo.getType().getTypeName()
                  + " on second table"));
        }
        ColumnInfo unionColInfo = new ColumnInfo(lInfo);
        unionColInfo.setType(FunctionRegistry.getCommonClassForUnionAll(lInfo.getType(),
            rInfo.getType()));
        unionoutRR.put(unionalias, field, unionColInfo);
      }

      // 4. Determine which columns requires cast on left/right input (Calcite
      // requires exact types on both sides of union)
      boolean leftNeedsTypeCast = false;
      boolean rightNeedsTypeCast = false;
      List<RexNode> leftProjs = new ArrayList<RexNode>();
      List<RexNode> rightProjs = new ArrayList<RexNode>();
      List<RelDataTypeField> leftRowDT = leftRel.getRowType().getFieldList();
      List<RelDataTypeField> rightRowDT = rightRel.getRowType().getFieldList();

      RelDataType leftFieldDT;
      RelDataType rightFieldDT;
      RelDataType unionFieldDT;
      for (int i = 0; i < leftRowDT.size(); i++) {
        leftFieldDT = leftRowDT.get(i).getType();
        rightFieldDT = rightRowDT.get(i).getType();
        if (!leftFieldDT.equals(rightFieldDT)) {
          unionFieldDT = TypeConverter.convert(unionoutRR.getColumnInfos().get(i).getType(),
              cluster.getTypeFactory());
          if (!unionFieldDT.equals(leftFieldDT)) {
            leftNeedsTypeCast = true;
          }
          leftProjs.add(cluster.getRexBuilder().ensureType(unionFieldDT,
              cluster.getRexBuilder().makeInputRef(leftFieldDT, i), true));

          if (!unionFieldDT.equals(rightFieldDT)) {
            rightNeedsTypeCast = true;
          }
          rightProjs.add(cluster.getRexBuilder().ensureType(unionFieldDT,
              cluster.getRexBuilder().makeInputRef(rightFieldDT, i), true));
        } else {
          leftProjs.add(cluster.getRexBuilder().ensureType(leftFieldDT,
              cluster.getRexBuilder().makeInputRef(leftFieldDT, i), true));
          rightProjs.add(cluster.getRexBuilder().ensureType(rightFieldDT,
              cluster.getRexBuilder().makeInputRef(rightFieldDT, i), true));
        }
      }

      // 5. Introduce Project Rel above original left/right inputs if cast is
      // needed for type parity
      RelNode unionLeftInput = leftRel;
      RelNode unionRightInput = rightRel;
      if (leftNeedsTypeCast) {
        unionLeftInput = HiveProject.create(leftRel, leftProjs, leftRel.getRowType()
            .getFieldNames());
      }
      if (rightNeedsTypeCast) {
        unionRightInput = HiveProject.create(rightRel, rightProjs, rightRel.getRowType()
            .getFieldNames());
      }

      // 6. Construct Union Rel
      Builder<RelNode> bldr = new ImmutableList.Builder<RelNode>();
      bldr.add(unionLeftInput);
      bldr.add(unionRightInput);
      unionRel = new HiveUnion(cluster, TraitsUtil.getDefaultTraitSet(cluster), bldr.build());

      relToHiveRR.put(unionRel, unionoutRR);
      relToHiveColNameCalcitePosMap.put(unionRel,
          this.buildHiveToCalciteColumnMap(unionoutRR, unionRel));

      return unionRel;
    }

    private RelNode genJoinRelNode(RelNode leftRel, RelNode rightRel, JoinType hiveJoinType,
        ASTNode joinCond) throws SemanticException {
      RelNode joinRel = null;

      // 1. construct the RowResolver for the new Join Node by combining row
      // resolvers from left, right
      RowResolver leftRR = this.relToHiveRR.get(leftRel);
      RowResolver rightRR = this.relToHiveRR.get(rightRel);
      RowResolver joinRR = null;

      if (hiveJoinType != JoinType.LEFTSEMI) {
        joinRR = RowResolver.getCombinedRR(leftRR, rightRR);
      } else {
        joinRR = new RowResolver();
        if (!RowResolver.add(joinRR, leftRR)) {
          LOG.warn("Duplicates detected when adding columns to RR: see previous message");
        }
      }

      // 2. Construct ExpressionNodeDesc representing Join Condition
      RexNode calciteJoinCond = null;
      if (joinCond != null) {
        JoinTypeCheckCtx jCtx = new JoinTypeCheckCtx(leftRR, rightRR, hiveJoinType);
        Map<ASTNode, ExprNodeDesc> exprNodes = JoinCondTypeCheckProcFactory.genExprNode(joinCond,
            jCtx);
        if (jCtx.getError() != null)
          throw new SemanticException(SemanticAnalyzer.generateErrorMessage(jCtx.getErrorSrcNode(),
              jCtx.getError()));

        ExprNodeDesc joinCondnExprNode = exprNodes.get(joinCond);

        List<RelNode> inputRels = new ArrayList<RelNode>();
        inputRels.add(leftRel);
        inputRels.add(rightRel);
        calciteJoinCond = RexNodeConverter.convert(cluster, joinCondnExprNode, inputRels,
            relToHiveRR, relToHiveColNameCalcitePosMap, false);
      } else {
        calciteJoinCond = cluster.getRexBuilder().makeLiteral(true);
      }

      // 3. Validate that join condition is legal (i.e no function refering to
      // both sides of join, only equi join)
      // TODO: Join filter handling (only supported for OJ by runtime or is it
      // supported for IJ as well)

      // 4. Construct Join Rel Node
      boolean leftSemiJoin = false;
      JoinRelType calciteJoinType;
      switch (hiveJoinType) {
      case LEFTOUTER:
        calciteJoinType = JoinRelType.LEFT;
        break;
      case RIGHTOUTER:
        calciteJoinType = JoinRelType.RIGHT;
        break;
      case FULLOUTER:
        calciteJoinType = JoinRelType.FULL;
        break;
      case LEFTSEMI:
        calciteJoinType = JoinRelType.INNER;
        leftSemiJoin = true;
        break;
      case INNER:
      default:
        calciteJoinType = JoinRelType.INNER;
        break;
      }

      if (leftSemiJoin) {
        List<RelDataTypeField> sysFieldList = new ArrayList<RelDataTypeField>();
        List<RexNode> leftJoinKeys = new ArrayList<RexNode>();
        List<RexNode> rightJoinKeys = new ArrayList<RexNode>();

        RexNode nonEquiConds = HiveRelOptUtil.splitJoinCondition(sysFieldList, leftRel, rightRel,
            calciteJoinCond, leftJoinKeys, rightJoinKeys, null, null);

        if (!nonEquiConds.isAlwaysTrue()) {
          throw new SemanticException("Non equality condition not supported in Semi-Join"
              + nonEquiConds);
        }

        RelNode[] inputRels = new RelNode[] { leftRel, rightRel };
        final List<Integer> leftKeys = new ArrayList<Integer>();
        final List<Integer> rightKeys = new ArrayList<Integer>();
        calciteJoinCond = HiveCalciteUtil.projectNonColumnEquiConditions(
            HiveProject.DEFAULT_PROJECT_FACTORY, inputRels, leftJoinKeys, rightJoinKeys, 0,
            leftKeys, rightKeys);

        joinRel = new SemiJoin(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION), inputRels[0],
            inputRels[1], calciteJoinCond, ImmutableIntList.copyOf(leftKeys),
            ImmutableIntList.copyOf(rightKeys));
      } else {
        joinRel = HiveJoin.getJoin(cluster, leftRel, rightRel, calciteJoinCond, calciteJoinType,
            leftSemiJoin);
      }
      // 5. Add new JoinRel & its RR to the maps
      relToHiveColNameCalcitePosMap.put(joinRel, this.buildHiveToCalciteColumnMap(joinRR, joinRel));
      relToHiveRR.put(joinRel, joinRR);

      return joinRel;
    }

    /**
     * Generate Join Logical Plan Relnode by walking through the join AST.
     *
     * @param qb
     * @param aliasToRel
     *          Alias(Table/Relation alias) to RelNode; only read and not
     *          written in to by this method
     * @return
     * @throws SemanticException
     */
    private RelNode genJoinLogicalPlan(ASTNode joinParseTree, Map<String, RelNode> aliasToRel)
        throws SemanticException {
      RelNode leftRel = null;
      RelNode rightRel = null;
      JoinType hiveJoinType = null;

      if (joinParseTree.getToken().getType() == HiveParser.TOK_UNIQUEJOIN) {
        String msg = String.format("UNIQUE JOIN is currently not supported in CBO,"
            + " turn off cbo to use UNIQUE JOIN.");
        LOG.debug(msg);
        throw new CalciteSemanticException(msg, UnsupportedFeature.Unique_join);
      }

      // 1. Determine Join Type
      // TODO: What about TOK_CROSSJOIN, TOK_MAPJOIN
      switch (joinParseTree.getToken().getType()) {
      case HiveParser.TOK_LEFTOUTERJOIN:
        hiveJoinType = JoinType.LEFTOUTER;
        break;
      case HiveParser.TOK_RIGHTOUTERJOIN:
        hiveJoinType = JoinType.RIGHTOUTER;
        break;
      case HiveParser.TOK_FULLOUTERJOIN:
        hiveJoinType = JoinType.FULLOUTER;
        break;
      case HiveParser.TOK_LEFTSEMIJOIN:
        hiveJoinType = JoinType.LEFTSEMI;
        break;
      default:
        hiveJoinType = JoinType.INNER;
        break;
      }

      // 2. Get Left Table Alias
      ASTNode left = (ASTNode) joinParseTree.getChild(0);
      if ((left.getToken().getType() == HiveParser.TOK_TABREF)
          || (left.getToken().getType() == HiveParser.TOK_SUBQUERY)
          || (left.getToken().getType() == HiveParser.TOK_PTBLFUNCTION)) {
        String tableName = SemanticAnalyzer.getUnescapedUnqualifiedTableName(
            (ASTNode) left.getChild(0)).toLowerCase();
        String leftTableAlias = left.getChildCount() == 1 ? tableName : SemanticAnalyzer
            .unescapeIdentifier(left.getChild(left.getChildCount() - 1).getText().toLowerCase());
        // ptf node form is: ^(TOK_PTBLFUNCTION $name $alias?
        // partitionTableFunctionSource partitioningSpec? expression*)
        // guranteed to have an lias here: check done in processJoin
        leftTableAlias = (left.getToken().getType() == HiveParser.TOK_PTBLFUNCTION) ? SemanticAnalyzer
            .unescapeIdentifier(left.getChild(1).getText().toLowerCase()) : leftTableAlias;
        leftRel = aliasToRel.get(leftTableAlias);
      } else if (SemanticAnalyzer.isJoinToken(left)) {
        leftRel = genJoinLogicalPlan(left, aliasToRel);
      } else {
        assert (false);
      }

      // 3. Get Right Table Alias
      ASTNode right = (ASTNode) joinParseTree.getChild(1);
      if ((right.getToken().getType() == HiveParser.TOK_TABREF)
          || (right.getToken().getType() == HiveParser.TOK_SUBQUERY)
          || (right.getToken().getType() == HiveParser.TOK_PTBLFUNCTION)) {
        String tableName = SemanticAnalyzer.getUnescapedUnqualifiedTableName(
            (ASTNode) right.getChild(0)).toLowerCase();
        String rightTableAlias = right.getChildCount() == 1 ? tableName : SemanticAnalyzer
            .unescapeIdentifier(right.getChild(right.getChildCount() - 1).getText().toLowerCase());
        // ptf node form is: ^(TOK_PTBLFUNCTION $name $alias?
        // partitionTableFunctionSource partitioningSpec? expression*)
        // guranteed to have an lias here: check done in processJoin
        rightTableAlias = (right.getToken().getType() == HiveParser.TOK_PTBLFUNCTION) ? SemanticAnalyzer
            .unescapeIdentifier(right.getChild(1).getText().toLowerCase()) : rightTableAlias;
        rightRel = aliasToRel.get(rightTableAlias);
      } else {
        assert (false);
      }

      // 4. Get Join Condn
      ASTNode joinCond = (ASTNode) joinParseTree.getChild(2);

      // 5. Create Join rel
      return genJoinRelNode(leftRel, rightRel, hiveJoinType, joinCond);
    }

    private RelNode genTableLogicalPlan(String tableAlias, QB qb) throws SemanticException {
      RowResolver rr = new RowResolver();
      HiveTableScan tableRel = null;

      try {

        // 1. If the table has a Sample specified, bail from Calcite path.
        if (qb.getParseInfo().getTabSample(tableAlias) != null
            || getNameToSplitSampleMap().containsKey(tableAlias)) {
          String msg = String.format("Table Sample specified for %s."
              + " Currently we don't support Table Sample clauses in CBO,"
              + " turn off cbo for queries on tableSamples.", tableAlias);
          LOG.debug(msg);
          throw new CalciteSemanticException(msg, UnsupportedFeature.Table_sample_clauses);
        }

        // 2. Get Table Metadata
        Table tabMetaData = qb.getMetaData().getSrcForAlias(tableAlias);

        // 3. Get Table Logical Schema (Row Type)
        // NOTE: Table logical schema = Non Partition Cols + Partition Cols +
        // Virtual Cols

        // 3.1 Add Column info for non partion cols (Object Inspector fields)
        @SuppressWarnings("deprecation")
        StructObjectInspector rowObjectInspector = (StructObjectInspector) tabMetaData.getDeserializer()
            .getObjectInspector();
        List<? extends StructField> fields = rowObjectInspector.getAllStructFieldRefs();
        ColumnInfo colInfo;
        String colName;
        ArrayList<ColumnInfo> cInfoLst = new ArrayList<ColumnInfo>();
        for (int i = 0; i < fields.size(); i++) {
          colName = fields.get(i).getFieldName();
          colInfo = new ColumnInfo(
              fields.get(i).getFieldName(),
              TypeInfoUtils.getTypeInfoFromObjectInspector(fields.get(i).getFieldObjectInspector()),
              tableAlias, false);
          colInfo.setSkewedCol((SemanticAnalyzer.isSkewedCol(tableAlias, qb, colName)) ? true
              : false);
          rr.put(tableAlias, colName, colInfo);
          cInfoLst.add(colInfo);
        }
        // TODO: Fix this
        ArrayList<ColumnInfo> nonPartitionColumns = new ArrayList<ColumnInfo>(cInfoLst);
        ArrayList<ColumnInfo> partitionColumns = new ArrayList<ColumnInfo>();

        // 3.2 Add column info corresponding to partition columns
        for (FieldSchema part_col : tabMetaData.getPartCols()) {
          colName = part_col.getName();
          colInfo = new ColumnInfo(colName,
              TypeInfoFactory.getPrimitiveTypeInfo(part_col.getType()), tableAlias, true);
          rr.put(tableAlias, colName, colInfo);
          cInfoLst.add(colInfo);
          partitionColumns.add(colInfo);
        }

        // 3.3 Add column info corresponding to virtual columns
        List<VirtualColumn> virtualCols = new ArrayList<VirtualColumn>();
        Iterator<VirtualColumn> vcs = VirtualColumn.getRegistry(conf).iterator();
        while (vcs.hasNext()) {
          VirtualColumn vc = vcs.next();
          colInfo = new ColumnInfo(vc.getName(), vc.getTypeInfo(), tableAlias, true,
              vc.getIsHidden());
          rr.put(tableAlias, vc.getName(), colInfo);
          cInfoLst.add(colInfo);
          virtualCols.add(vc);
        }

        // 3.4 Build row type from field <type, name>
        RelDataType rowType = TypeConverter.getType(cluster, rr, null);

        // 4. Build RelOptAbstractTable
        String fullyQualifiedTabName = tabMetaData.getDbName();
        if (fullyQualifiedTabName != null && !fullyQualifiedTabName.isEmpty()) {
          fullyQualifiedTabName = fullyQualifiedTabName + "." + tabMetaData.getTableName();
        }
        else {
          fullyQualifiedTabName = tabMetaData.getTableName();
        }
        RelOptHiveTable optTable = new RelOptHiveTable(relOptSchema, fullyQualifiedTabName,
            rowType, tabMetaData, nonPartitionColumns, partitionColumns, virtualCols, conf,
            partitionCache, noColsMissingStats);

        // 5. Build Hive Table Scan Rel
        tableRel = new HiveTableScan(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION), optTable,
            null == tableAlias ? tabMetaData.getTableName() : tableAlias,
            getAliasId(tableAlias, qb), HiveConf.getBoolVar(conf,
                HiveConf.ConfVars.HIVE_CBO_RETPATH_HIVEOP));

        // 6. Add Schema(RR) to RelNode-Schema map
        ImmutableMap<String, Integer> hiveToCalciteColMap = buildHiveToCalciteColumnMap(rr,
            tableRel);
        relToHiveRR.put(tableRel, rr);
        relToHiveColNameCalcitePosMap.put(tableRel, hiveToCalciteColMap);
      } catch (Exception e) {
        if (e instanceof SemanticException) {
          throw (SemanticException) e;
        } else {
          throw (new RuntimeException(e));
        }
      }

      return tableRel;
    }

    private RelNode genFilterRelNode(ASTNode filterExpr, RelNode srcRel) throws SemanticException {
      ExprNodeDesc filterCondn = genExprNodeDesc(filterExpr, relToHiveRR.get(srcRel));
      if (filterCondn instanceof ExprNodeConstantDesc
          && !filterCondn.getTypeString().equals(serdeConstants.BOOLEAN_TYPE_NAME)) {
        // queries like select * from t1 where 'foo';
        // Calcite's rule PushFilterThroughProject chokes on it. Arguably, we
        // can insert a cast to
        // boolean in such cases, but since Postgres, Oracle and MS SQL server
        // fail on compile time
        // for such queries, its an arcane corner case, not worth of adding that
        // complexity.
        throw new CalciteSemanticException("Filter expression with non-boolean return type.",
            UnsupportedFeature.Filter_expression_with_non_boolean_return_type);
      }
      ImmutableMap<String, Integer> hiveColNameCalcitePosMap = this.relToHiveColNameCalcitePosMap
          .get(srcRel);
      RexNode convertedFilterExpr = new RexNodeConverter(cluster, srcRel.getRowType(),
          hiveColNameCalcitePosMap, 0, true).convert(filterCondn);
      RexNode factoredFilterExpr = RexUtil
          .pullFactors(cluster.getRexBuilder(), convertedFilterExpr);
      RelNode filterRel = new HiveFilter(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
          srcRel, factoredFilterExpr);
      this.relToHiveColNameCalcitePosMap.put(filterRel, hiveColNameCalcitePosMap);
      relToHiveRR.put(filterRel, relToHiveRR.get(srcRel));
      relToHiveColNameCalcitePosMap.put(filterRel, hiveColNameCalcitePosMap);

      return filterRel;
    }

    private RelNode genFilterRelNode(QB qb, ASTNode searchCond, RelNode srcRel,
        Map<String, RelNode> aliasToRel, boolean forHavingClause) throws SemanticException {
      /*
       * Handle Subquery predicates.
       *
       * Notes (8/22/14 hb): Why is this a copy of the code from {@link
       * #genFilterPlan} - for now we will support the same behavior as non CBO
       * route. - but plan to allow nested SubQueries(Restriction.9.m) and
       * multiple SubQuery expressions(Restriction.8.m). This requires use to
       * utilize Calcite's Decorrelation mechanics, and for Calcite to fix/flush
       * out Null semantics(CALCITE-373) - besides only the driving code has
       * been copied. Most of the code which is SubQueryUtils and QBSubQuery is
       * reused.
       */
      int numSrcColumns = srcRel.getRowType().getFieldCount();
      List<ASTNode> subQueriesInOriginalTree = SubQueryUtils.findSubQueries(searchCond);
      if (subQueriesInOriginalTree.size() > 0) {

        /*
         * Restriction.9.m :: disallow nested SubQuery expressions.
         */
        if (qb.getSubQueryPredicateDef() != null) {
          throw new SemanticException(ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(
              subQueriesInOriginalTree.get(0), "Nested SubQuery expressions are not supported."));
        }

        /*
         * Restriction.8.m :: We allow only 1 SubQuery expression per Query.
         */
        if (subQueriesInOriginalTree.size() > 1) {

          throw new SemanticException(ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(
              subQueriesInOriginalTree.get(1), "Only 1 SubQuery expression is supported."));
        }

        /*
         * Clone the Search AST; apply all rewrites on the clone.
         */
        ASTNode clonedSearchCond = (ASTNode) SubQueryUtils.adaptor.dupTree(searchCond);
        List<ASTNode> subQueries = SubQueryUtils.findSubQueries(clonedSearchCond);

        RowResolver inputRR = relToHiveRR.get(srcRel);
        RowResolver outerQBRR = inputRR;
        ImmutableMap<String, Integer> outerQBPosMap = relToHiveColNameCalcitePosMap.get(srcRel);

        for (int i = 0; i < subQueries.size(); i++) {
          ASTNode subQueryAST = subQueries.get(i);
          ASTNode originalSubQueryAST = subQueriesInOriginalTree.get(i);

          int sqIdx = qb.incrNumSubQueryPredicates();
          clonedSearchCond = SubQueryUtils.rewriteParentQueryWhere(clonedSearchCond, subQueryAST);

          QBSubQuery subQuery = SubQueryUtils.buildSubQuery(qb.getId(), sqIdx, subQueryAST,
              originalSubQueryAST, ctx);

          if (!forHavingClause) {
            qb.setWhereClauseSubQueryPredicate(subQuery);
          } else {
            qb.setHavingClauseSubQueryPredicate(subQuery);
          }
          String havingInputAlias = null;

          if (forHavingClause) {
            havingInputAlias = "gby_sq" + sqIdx;
            aliasToRel.put(havingInputAlias, srcRel);
          }

          subQuery.validateAndRewriteAST(inputRR, forHavingClause, havingInputAlias,
              aliasToRel.keySet());

          QB qbSQ = new QB(subQuery.getOuterQueryId(), subQuery.getAlias(), true);
          qbSQ.setSubQueryDef(subQuery.getSubQuery());
          Phase1Ctx ctx_1 = initPhase1Ctx();
          doPhase1(subQuery.getSubQueryAST(), qbSQ, ctx_1, null);
          getMetaData(qbSQ);
          RelNode subQueryRelNode = genLogicalPlan(qbSQ, false);
          aliasToRel.put(subQuery.getAlias(), subQueryRelNode);
          RowResolver sqRR = relToHiveRR.get(subQueryRelNode);

          /*
           * Check.5.h :: For In and Not In the SubQuery must implicitly or
           * explicitly only contain one select item.
           */
          if (subQuery.getOperator().getType() != SubQueryType.EXISTS
              && subQuery.getOperator().getType() != SubQueryType.NOT_EXISTS
              && sqRR.getColumnInfos().size() - subQuery.getNumOfCorrelationExprsAddedToSQSelect() > 1) {
            throw new SemanticException(ErrorMsg.INVALID_SUBQUERY_EXPRESSION.getMsg(subQueryAST,
                "SubQuery can contain only 1 item in Select List."));
          }

          /*
           * If this is a Not In SubQuery Predicate then Join in the Null Check
           * SubQuery. See QBSubQuery.NotInCheck for details on why and how this
           * is constructed.
           */
          if (subQuery.getNotInCheck() != null) {
            QBSubQuery.NotInCheck notInCheck = subQuery.getNotInCheck();
            notInCheck.setSQRR(sqRR);
            QB qbSQ_nic = new QB(subQuery.getOuterQueryId(), notInCheck.getAlias(), true);
            qbSQ_nic.setSubQueryDef(notInCheck.getSubQuery());
            ctx_1 = initPhase1Ctx();
            doPhase1(notInCheck.getSubQueryAST(), qbSQ_nic, ctx_1, null);
            getMetaData(qbSQ_nic);
            RelNode subQueryNICRelNode = genLogicalPlan(qbSQ_nic, false);
            aliasToRel.put(notInCheck.getAlias(), subQueryNICRelNode);
            srcRel = genJoinRelNode(srcRel, subQueryNICRelNode,
            // set explicitly to inner until we figure out SemiJoin use
            // notInCheck.getJoinType(),
                JoinType.INNER, notInCheck.getJoinConditionAST());
            inputRR = relToHiveRR.get(srcRel);
            if (forHavingClause) {
              aliasToRel.put(havingInputAlias, srcRel);
            }
          }

          /*
           * Gen Join between outer Operator and SQ op
           */
          subQuery.buildJoinCondition(inputRR, sqRR, forHavingClause, havingInputAlias);
          srcRel = genJoinRelNode(srcRel, subQueryRelNode, subQuery.getJoinType(),
              subQuery.getJoinConditionAST());
          searchCond = subQuery.updateOuterQueryFilter(clonedSearchCond);

          srcRel = genFilterRelNode(searchCond, srcRel);

          /*
           * For Not Exists and Not In, add a projection on top of the Left
           * Outer Join.
           */
          if (subQuery.getOperator().getType() != SubQueryType.NOT_EXISTS
              || subQuery.getOperator().getType() != SubQueryType.NOT_IN) {
            srcRel = projectLeftOuterSide(srcRel, numSrcColumns);
          }
        }
        relToHiveRR.put(srcRel, outerQBRR);
        relToHiveColNameCalcitePosMap.put(srcRel, outerQBPosMap);
        return srcRel;
      }

      return genFilterRelNode(searchCond, srcRel);
    }

    private RelNode projectLeftOuterSide(RelNode srcRel, int numColumns) throws SemanticException {
      RowResolver iRR = relToHiveRR.get(srcRel);
      RowResolver oRR = new RowResolver();
      RowResolver.add(oRR, iRR, numColumns);

      List<RexNode> calciteColLst = new ArrayList<RexNode>();
      List<String> oFieldNames = new ArrayList<String>();
      RelDataType iType = srcRel.getRowType();

      for (int i = 0; i < iType.getFieldCount(); i++) {
        RelDataTypeField fType = iType.getFieldList().get(i);
        String fName = iType.getFieldNames().get(i);
        calciteColLst.add(cluster.getRexBuilder().makeInputRef(fType.getType(), i));
        oFieldNames.add(fName);
      }

      HiveRelNode selRel = HiveProject.create(srcRel, calciteColLst, oFieldNames);

      this.relToHiveColNameCalcitePosMap.put(selRel, buildHiveToCalciteColumnMap(oRR, selRel));
      this.relToHiveRR.put(selRel, oRR);
      return selRel;
    }

    private RelNode genFilterLogicalPlan(QB qb, RelNode srcRel, Map<String, RelNode> aliasToRel,
        boolean forHavingClause) throws SemanticException {
      RelNode filterRel = null;

      Iterator<ASTNode> whereClauseIterator = getQBParseInfo(qb).getDestToWhereExpr().values()
          .iterator();
      if (whereClauseIterator.hasNext()) {
        filterRel = genFilterRelNode(qb, (ASTNode) whereClauseIterator.next().getChild(0), srcRel,
            aliasToRel, forHavingClause);
      }

      return filterRel;
    }

    /**
     * Class to store GenericUDAF related information.
     */
    private class AggInfo {
      private final List<ExprNodeDesc> m_aggParams;
      private final TypeInfo           m_returnType;
      private final String             m_udfName;
      private final boolean            m_distinct;

      private AggInfo(List<ExprNodeDesc> aggParams, TypeInfo returnType, String udfName,
          boolean isDistinct) {
        m_aggParams = aggParams;
        m_returnType = returnType;
        m_udfName = udfName;
        m_distinct = isDistinct;
      }
    }

    private AggregateCall convertGBAgg(AggInfo agg, RelNode input, List<RexNode> gbChildProjLst,
        RexNodeConverter converter, HashMap<String, Integer> rexNodeToPosMap,
        Integer childProjLstIndx) throws SemanticException {

      // 1. Get agg fn ret type in Calcite
      RelDataType aggFnRetType = TypeConverter.convert(agg.m_returnType,
          this.cluster.getTypeFactory());

      // 2. Convert Agg Fn args and type of args to Calcite
      // TODO: Does HQL allows expressions as aggregate args or can it only be
      // projections from child?
      Integer inputIndx;
      List<Integer> argList = new ArrayList<Integer>();
      RexNode rexNd = null;
      RelDataTypeFactory dtFactory = this.cluster.getTypeFactory();
      ImmutableList.Builder<RelDataType> aggArgRelDTBldr = new ImmutableList.Builder<RelDataType>();
      for (ExprNodeDesc expr : agg.m_aggParams) {
        rexNd = converter.convert(expr);
        inputIndx = rexNodeToPosMap.get(rexNd.toString());
        if (inputIndx == null) {
          gbChildProjLst.add(rexNd);
          rexNodeToPosMap.put(rexNd.toString(), childProjLstIndx);
          inputIndx = childProjLstIndx;
          childProjLstIndx++;
        }
        argList.add(inputIndx);

        // TODO: does arg need type cast?
        aggArgRelDTBldr.add(TypeConverter.convert(expr.getTypeInfo(), dtFactory));
      }

      // 3. Get Aggregation FN from Calcite given name, ret type and input arg
      // type
      final SqlAggFunction aggregation = SqlFunctionConverter.getCalciteAggFn(agg.m_udfName,
          aggArgRelDTBldr.build(), aggFnRetType);

      return new AggregateCall(aggregation, agg.m_distinct, argList, aggFnRetType, null);
    }

    private RelNode genGBRelNode(List<ExprNodeDesc> gbExprs, List<AggInfo> aggInfoLst,
        List<Integer> groupSets, RelNode srcRel) throws SemanticException {
      ImmutableMap<String, Integer> posMap = this.relToHiveColNameCalcitePosMap.get(srcRel);
      RexNodeConverter converter = new RexNodeConverter(this.cluster, srcRel.getRowType(), posMap,
          0, false);

      final boolean hasGroupSets = groupSets != null && !groupSets.isEmpty();
      final List<RexNode> gbChildProjLst = Lists.newArrayList();
      final HashMap<String, Integer> rexNodeToPosMap = new HashMap<String, Integer>();
      final List<Integer> groupSetPositions = Lists.newArrayList();
      Integer gbIndx = 0;
      RexNode rnd;
      for (ExprNodeDesc key : gbExprs) {
        rnd = converter.convert(key);
        gbChildProjLst.add(rnd);
        groupSetPositions.add(gbIndx);
        rexNodeToPosMap.put(rnd.toString(), gbIndx);
        gbIndx++;
      }
      final ImmutableBitSet groupSet = ImmutableBitSet.of(groupSetPositions);

      // Grouping sets: we need to transform them into ImmutableBitSet
      // objects for Calcite
      List<ImmutableBitSet> transformedGroupSets = null;
      if(hasGroupSets) {
        Set<ImmutableBitSet> setTransformedGroupSets =
                new HashSet<ImmutableBitSet>(groupSets.size());
        for(int val: groupSets) {
          setTransformedGroupSets.add(convert(val));
        }
        // Calcite expects the grouping sets sorted and without duplicates
        transformedGroupSets = new ArrayList<ImmutableBitSet>(setTransformedGroupSets);
        Collections.sort(transformedGroupSets, ImmutableBitSet.COMPARATOR);
      }

      List<AggregateCall> aggregateCalls = Lists.newArrayList();
      for (AggInfo agg : aggInfoLst) {
        aggregateCalls.add(convertGBAgg(agg, srcRel, gbChildProjLst, converter, rexNodeToPosMap,
            gbChildProjLst.size()));
      }
      if (hasGroupSets) {
        // Create GroupingID column
        AggregateCall aggCall = new AggregateCall(HiveGroupingID.INSTANCE,
                false, new ImmutableList.Builder<Integer>().build(),
                this.cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER),
                HiveGroupingID.INSTANCE.getName());
        aggregateCalls.add(aggCall);
      }

      if (gbChildProjLst.isEmpty()) {
        // This will happen for count(*), in such cases we arbitarily pick
        // first element from srcRel
        gbChildProjLst.add(this.cluster.getRexBuilder().makeInputRef(srcRel, 0));
      }
      RelNode gbInputRel = HiveProject.create(srcRel, gbChildProjLst, null);

      HiveRelNode aggregateRel = null;
      try {
        aggregateRel = new HiveAggregate(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
            gbInputRel, (transformedGroupSets!=null ? true:false), groupSet,
            transformedGroupSets, aggregateCalls);
      } catch (InvalidRelException e) {
        throw new SemanticException(e);
      }

      return aggregateRel;
    }

    private ImmutableBitSet convert(int value) {
      BitSet bits = new BitSet();
      int index = 0;
      while (value != 0L) {
        if (value % 2 != 0) {
          bits.set(index);
        }
        ++index;
        value = value >>> 1;
      }
      return ImmutableBitSet.FROM_BIT_SET.apply(bits);
    }

    private void addAlternateGByKeyMappings(ASTNode gByExpr, ColumnInfo colInfo,
        RowResolver gByInputRR, RowResolver gByRR) {
      if (gByExpr.getType() == HiveParser.DOT
          && gByExpr.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL) {
        String tab_alias = BaseSemanticAnalyzer.unescapeIdentifier(gByExpr.getChild(0).getChild(0)
            .getText());
        String col_alias = BaseSemanticAnalyzer.unescapeIdentifier(gByExpr.getChild(1).getText());
        gByRR.put(tab_alias, col_alias, colInfo);
      } else if (gByExpr.getType() == HiveParser.TOK_TABLE_OR_COL) {
        String col_alias = BaseSemanticAnalyzer.unescapeIdentifier(gByExpr.getChild(0).getText());
        String tab_alias = null;
        /*
         * If the input to the GBy has a tab alias for the column, then add an
         * entry based on that tab_alias. For e.g. this query: select b.x,
         * count(*) from t1 b group by x needs (tab_alias=b, col_alias=x) in the
         * GBy RR. tab_alias=b comes from looking at the RowResolver that is the
         * ancestor before any GBy/ReduceSinks added for the GBY operation.
         */
        try {
          ColumnInfo pColInfo = gByInputRR.get(tab_alias, col_alias);
          tab_alias = pColInfo == null ? null : pColInfo.getTabAlias();
        } catch (SemanticException se) {
        }
        gByRR.put(tab_alias, col_alias, colInfo);
      }
    }

    private void addToGBExpr(RowResolver groupByOutputRowResolver,
        RowResolver groupByInputRowResolver, ASTNode grpbyExpr, ExprNodeDesc grpbyExprNDesc,
        List<ExprNodeDesc> gbExprNDescLst, List<String> outputColumnNames) {
      // TODO: Should we use grpbyExprNDesc.getTypeInfo()? what if expr is
      // UDF
      int i = gbExprNDescLst.size();
      String field = SemanticAnalyzer.getColumnInternalName(i);
      outputColumnNames.add(field);
      gbExprNDescLst.add(grpbyExprNDesc);

      ColumnInfo oColInfo = new ColumnInfo(field, grpbyExprNDesc.getTypeInfo(), null, false);
      groupByOutputRowResolver.putExpression(grpbyExpr, oColInfo);

      addAlternateGByKeyMappings(grpbyExpr, oColInfo, groupByInputRowResolver,
          groupByOutputRowResolver);
    }

    private AggInfo getHiveAggInfo(ASTNode aggAst, int aggFnLstArgIndx, RowResolver inputRR)
        throws SemanticException {
      AggInfo aInfo = null;

      // 1 Convert UDAF Params to ExprNodeDesc
      ArrayList<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>();
      for (int i = 1; i <= aggFnLstArgIndx; i++) {
        ASTNode paraExpr = (ASTNode) aggAst.getChild(i);
        ExprNodeDesc paraExprNode = genExprNodeDesc(paraExpr, inputRR);
        aggParameters.add(paraExprNode);
      }

      // 2. Is this distinct UDAF
      boolean isDistinct = aggAst.getType() == HiveParser.TOK_FUNCTIONDI;

      // 3. Determine type of UDAF
      TypeInfo udafRetType = null;

      // 3.1 Obtain UDAF name
      String aggName = SemanticAnalyzer.unescapeIdentifier(aggAst.getChild(0).getText());

      // 3.2 Rank functions type is 'int'/'double'
      if (FunctionRegistry.isRankingFunction(aggName)) {
        if (aggName.equalsIgnoreCase("percent_rank"))
          udafRetType = TypeInfoFactory.doubleTypeInfo;
        else
          udafRetType = TypeInfoFactory.intTypeInfo;
      } else {
        // 3.3 Try obtaining UDAF evaluators to determine the ret type
        try {
          boolean isAllColumns = aggAst.getType() == HiveParser.TOK_FUNCTIONSTAR;

          // 3.3.1 Get UDAF Evaluator
          Mode amode = SemanticAnalyzer.groupByDescModeToUDAFMode(GroupByDesc.Mode.COMPLETE,
              isDistinct);

          GenericUDAFEvaluator genericUDAFEvaluator = null;
          if (aggName.toLowerCase().equals(FunctionRegistry.LEAD_FUNC_NAME)
              || aggName.toLowerCase().equals(FunctionRegistry.LAG_FUNC_NAME)) {
            ArrayList<ObjectInspector> originalParameterTypeInfos = SemanticAnalyzer
                .getWritableObjectInspector(aggParameters);
            genericUDAFEvaluator = FunctionRegistry.getGenericWindowingEvaluator(aggName,
                originalParameterTypeInfos, isDistinct, isAllColumns);
            GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(genericUDAFEvaluator, amode,
                aggParameters);
            udafRetType = ((ListTypeInfo) udaf.returnType).getListElementTypeInfo();
          } else {
            genericUDAFEvaluator = SemanticAnalyzer.getGenericUDAFEvaluator(aggName, aggParameters,
                aggAst, isDistinct, isAllColumns);
            assert (genericUDAFEvaluator != null);

            // 3.3.2 Get UDAF Info using UDAF Evaluator
            GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(genericUDAFEvaluator, amode,
                aggParameters);
            if (FunctionRegistry.pivotResult(aggName)) {
              udafRetType = ((ListTypeInfo)udaf.returnType).getListElementTypeInfo();
            } else {
              udafRetType = udaf.returnType;
            }
          }
        } catch (Exception e) {
          LOG.debug("CBO: Couldn't Obtain UDAF evaluators for " + aggName
              + ", trying to translate to GenericUDF");
        }

        // 3.4 Try GenericUDF translation
        if (udafRetType == null) {
          TypeCheckCtx tcCtx = new TypeCheckCtx(inputRR);
          // We allow stateful functions in the SELECT list (but nowhere else)
          tcCtx.setAllowStatefulFunctions(true);
          tcCtx.setAllowDistinctFunctions(false);
          ExprNodeDesc exp = genExprNodeDesc((ASTNode) aggAst.getChild(0), inputRR, tcCtx);
          udafRetType = exp.getTypeInfo();
        }
      }

      // 4. Construct AggInfo
      aInfo = new AggInfo(aggParameters, udafRetType, aggName, isDistinct);

      return aInfo;
    }

    /**
     * Generate GB plan.
     *
     * @param qb
     * @param srcRel
     * @return TODO: 1. Grouping Sets (roll up..)
     * @throws SemanticException
     */
    private RelNode genGBLogicalPlan(QB qb, RelNode srcRel) throws SemanticException {
      RelNode gbRel = null;
      QBParseInfo qbp = getQBParseInfo(qb);

      // 1. Gather GB Expressions (AST) (GB + Aggregations)
      // NOTE: Multi Insert is not supported
      String detsClauseName = qbp.getClauseNames().iterator().next();
      // Check and transform group by *. This will only happen for select distinct *.
      // Here the "genSelectPlan" is being leveraged.
      // The main benefits are (1) remove virtual columns that should
      // not be included in the group by; (2) add the fully qualified column names to unParseTranslator
      // so that view is supported. The drawback is that an additional SEL op is added. If it is
      // not necessary, it will be removed by NonBlockingOpDeDupProc Optimizer because it will match
      // SEL%SEL% rule.
      ASTNode selExprList = qb.getParseInfo().getSelForClause(detsClauseName);
      if (selExprList.getToken().getType() == HiveParser.TOK_SELECTDI
          && selExprList.getChildCount() == 1 && selExprList.getChild(0).getChildCount() == 1) {
        ASTNode node = (ASTNode) selExprList.getChild(0).getChild(0);
        if (node.getToken().getType() == HiveParser.TOK_ALLCOLREF) {
          srcRel = genSelectLogicalPlan(qb, srcRel, srcRel);
          RowResolver rr = this.relToHiveRR.get(srcRel);
          qbp.setSelExprForClause(detsClauseName, SemanticAnalyzer.genSelectDIAST(rr));
        }
      }

      List<ASTNode> grpByAstExprs = SemanticAnalyzer.getGroupByForClause(qbp, detsClauseName);
      HashMap<String, ASTNode> aggregationTrees = qbp.getAggregationExprsForClause(detsClauseName);
      boolean hasGrpByAstExprs = (grpByAstExprs != null && !grpByAstExprs.isEmpty()) ? true : false;
      boolean hasAggregationTrees = (aggregationTrees != null && !aggregationTrees.isEmpty()) ? true
          : false;

      final boolean cubeRollupGrpSetPresent = (!qbp.getDestRollups().isEmpty()
          || !qbp.getDestGroupingSets().isEmpty() || !qbp.getDestCubes().isEmpty());

      // 2. Sanity check
      if (conf.getBoolVar(HiveConf.ConfVars.HIVEGROUPBYSKEW)
          && qbp.getDistinctFuncExprsForClause(detsClauseName).size() > 1) {
        throw new SemanticException(ErrorMsg.UNSUPPORTED_MULTIPLE_DISTINCTS.getMsg());
      }
      if (cubeRollupGrpSetPresent) {
        if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEMAPSIDEAGGREGATE)) {
          throw new SemanticException(ErrorMsg.HIVE_GROUPING_SETS_AGGR_NOMAPAGGR.getMsg());
        }

        if (conf.getBoolVar(HiveConf.ConfVars.HIVEGROUPBYSKEW)) {
          checkExpressionsForGroupingSet(grpByAstExprs, qb.getParseInfo()
              .getDistinctFuncExprsForClause(detsClauseName), aggregationTrees,
              this.relToHiveRR.get(srcRel));

          if (qbp.getDestGroupingSets().size() > conf
              .getIntVar(HiveConf.ConfVars.HIVE_NEW_JOB_GROUPING_SET_CARDINALITY)) {
            String errorMsg = "The number of rows per input row due to grouping sets is "
                + qbp.getDestGroupingSets().size();
            throw new SemanticException(
                ErrorMsg.HIVE_GROUPING_SETS_THRESHOLD_NOT_ALLOWED_WITH_SKEW.getMsg(errorMsg));
          }
        }
      }


      if (hasGrpByAstExprs || hasAggregationTrees) {
        ArrayList<ExprNodeDesc> gbExprNDescLst = new ArrayList<ExprNodeDesc>();
        ArrayList<String> outputColumnNames = new ArrayList<String>();

        // 3. Input, Output Row Resolvers
        RowResolver groupByInputRowResolver = this.relToHiveRR.get(srcRel);
        RowResolver groupByOutputRowResolver = new RowResolver();
        groupByOutputRowResolver.setIsExprResolver(true);

        if (hasGrpByAstExprs) {
          // 4. Construct GB Keys (ExprNode)
          for (int i = 0; i < grpByAstExprs.size(); ++i) {
            ASTNode grpbyExpr = grpByAstExprs.get(i);
            Map<ASTNode, ExprNodeDesc> astToExprNDescMap = TypeCheckProcFactory.genExprNode(
                grpbyExpr, new TypeCheckCtx(groupByInputRowResolver));
            ExprNodeDesc grpbyExprNDesc = astToExprNDescMap.get(grpbyExpr);
            if (grpbyExprNDesc == null)
              throw new CalciteSemanticException("Invalid Column Reference: " + grpbyExpr.dump(),
                  UnsupportedFeature.Invalid_column_reference);

            addToGBExpr(groupByOutputRowResolver, groupByInputRowResolver, grpbyExpr,
                grpbyExprNDesc, gbExprNDescLst, outputColumnNames);
          }
        }

        // 5. GroupingSets, Cube, Rollup
        int groupingColsSize = gbExprNDescLst.size();
        List<Integer> groupingSets = null;
        if (cubeRollupGrpSetPresent) {
          if (qbp.getDestRollups().contains(detsClauseName)) {
            groupingSets = getGroupingSetsForRollup(grpByAstExprs.size());
          } else if (qbp.getDestCubes().contains(detsClauseName)) {
            groupingSets = getGroupingSetsForCube(grpByAstExprs.size());
          } else if (qbp.getDestGroupingSets().contains(detsClauseName)) {
            groupingSets = getGroupingSets(grpByAstExprs, qbp, detsClauseName);
          }

          final int limit = groupingColsSize * 2;
          while (groupingColsSize < limit) {
            String field = getColumnInternalName(groupingColsSize);
            outputColumnNames.add(field);
            groupByOutputRowResolver.put(null, field,
                    new ColumnInfo(
                            field,
                            TypeInfoFactory.booleanTypeInfo,
                            null,
                            false));
            groupingColsSize++;
          }
        }

        // 6. Construct aggregation function Info
        ArrayList<AggInfo> aggregations = new ArrayList<AggInfo>();
        if (hasAggregationTrees) {
          assert (aggregationTrees != null);
          for (ASTNode value : aggregationTrees.values()) {
            // 6.1 Determine type of UDAF
            // This is the GenericUDAF name
            String aggName = SemanticAnalyzer.unescapeIdentifier(value.getChild(0).getText());
            boolean isDistinct = value.getType() == HiveParser.TOK_FUNCTIONDI;
            boolean isAllColumns = value.getType() == HiveParser.TOK_FUNCTIONSTAR;

            // 6.2 Convert UDAF Params to ExprNodeDesc
            ArrayList<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>();
            for (int i = 1; i < value.getChildCount(); i++) {
              ASTNode paraExpr = (ASTNode) value.getChild(i);
              ExprNodeDesc paraExprNode = genExprNodeDesc(paraExpr, groupByInputRowResolver);
              aggParameters.add(paraExprNode);
            }

            Mode amode = SemanticAnalyzer.groupByDescModeToUDAFMode(GroupByDesc.Mode.COMPLETE,
                isDistinct);
            GenericUDAFEvaluator genericUDAFEvaluator = SemanticAnalyzer.getGenericUDAFEvaluator(
                aggName, aggParameters, value, isDistinct, isAllColumns);
            assert (genericUDAFEvaluator != null);
            GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(genericUDAFEvaluator, amode,
                aggParameters);
            AggInfo aInfo = new AggInfo(aggParameters, udaf.returnType, aggName, isDistinct);
            aggregations.add(aInfo);
            String field = getColumnInternalName(groupingColsSize + aggregations.size() - 1);
            outputColumnNames.add(field);
            groupByOutputRowResolver.putExpression(value, new ColumnInfo(field, aInfo.m_returnType,
                "", false));
          }
        }

        // 7. If GroupingSets, Cube, Rollup were used, we account grouping__id
        if(groupingSets != null && !groupingSets.isEmpty()) {
          String field = getColumnInternalName(groupingColsSize + aggregations.size());
          outputColumnNames.add(field);
          groupByOutputRowResolver.put(null, VirtualColumn.GROUPINGID.getName(),
                  new ColumnInfo(
                          field,
                          TypeInfoFactory.intTypeInfo,
                          null,
                          true));
        }

        // 8. We create the group_by operator
        gbRel = genGBRelNode(gbExprNDescLst, aggregations, groupingSets, srcRel);
        relToHiveColNameCalcitePosMap.put(gbRel,
            buildHiveToCalciteColumnMap(groupByOutputRowResolver, gbRel));
        this.relToHiveRR.put(gbRel, groupByOutputRowResolver);
      }

      return gbRel;
    }

    /**
     * Generate OB RelNode and input Select RelNode that should be used to
     * introduce top constraining Project. If Input select RelNode is not
     * present then don't introduce top constraining select.
     *
     * @param qb
     * @param srcRel
     * @param outermostOB
     * @return Pair<RelNode, RelNode> Key- OB RelNode, Value - Input Select for
     *         top constraining Select
     * @throws SemanticException
     */
    private Pair<RelNode, RelNode> genOBLogicalPlan(QB qb, RelNode srcRel, boolean outermostOB)
        throws SemanticException {
      RelNode sortRel = null;
      RelNode originalOBChild = null;

      QBParseInfo qbp = getQBParseInfo(qb);
      String dest = qbp.getClauseNames().iterator().next();
      ASTNode obAST = qbp.getOrderByForClause(dest);

      if (obAST != null) {
        // 1. OB Expr sanity test
        // in strict mode, in the presence of order by, limit must be specified
        Integer limit = qb.getParseInfo().getDestLimit(dest);
        if (conf.getVar(HiveConf.ConfVars.HIVEMAPREDMODE).equalsIgnoreCase("strict")
            && limit == null) {
          throw new SemanticException(SemanticAnalyzer.generateErrorMessage(obAST,
              ErrorMsg.NO_LIMIT_WITH_ORDERBY.getMsg()));
        }

        // 2. Walk through OB exprs and extract field collations and additional
        // virtual columns needed
        final List<RexNode> newVCLst = new ArrayList<RexNode>();
        final List<RelFieldCollation> fieldCollations = Lists.newArrayList();
        int fieldIndex = 0;

        List<Node> obASTExprLst = obAST.getChildren();
        ASTNode obASTExpr;
        List<Pair<ASTNode, TypeInfo>> vcASTTypePairs = new ArrayList<Pair<ASTNode, TypeInfo>>();
        RowResolver inputRR = relToHiveRR.get(srcRel);
        RowResolver outputRR = new RowResolver();

        RexNode rnd;
        RexNodeConverter converter = new RexNodeConverter(cluster, srcRel.getRowType(),
            relToHiveColNameCalcitePosMap.get(srcRel), 0, false);
        int srcRelRecordSz = srcRel.getRowType().getFieldCount();

        for (int i = 0; i < obASTExprLst.size(); i++) {
          // 2.1 Convert AST Expr to ExprNode
          obASTExpr = (ASTNode) obASTExprLst.get(i);
          Map<ASTNode, ExprNodeDesc> astToExprNDescMap = TypeCheckProcFactory.genExprNode(
              obASTExpr, new TypeCheckCtx(inputRR));
          ExprNodeDesc obExprNDesc = astToExprNDescMap.get(obASTExpr.getChild(0));
          if (obExprNDesc == null)
            throw new SemanticException("Invalid order by expression: " + obASTExpr.toString());

          // 2.2 Convert ExprNode to RexNode
          rnd = converter.convert(obExprNDesc);

          // 2.3 Determine the index of ob expr in child schema
          // NOTE: Calcite can not take compound exprs in OB without it being
          // present in the child (& hence we add a child Project Rel)
          if (rnd instanceof RexInputRef) {
            fieldIndex = ((RexInputRef) rnd).getIndex();
          } else {
            fieldIndex = srcRelRecordSz + newVCLst.size();
            newVCLst.add(rnd);
            vcASTTypePairs.add(new Pair<ASTNode, TypeInfo>((ASTNode) obASTExpr.getChild(0),
                obExprNDesc.getTypeInfo()));
          }

          // 2.4 Determine the Direction of order by
          org.apache.calcite.rel.RelFieldCollation.Direction order = RelFieldCollation.Direction.DESCENDING;
          if (obASTExpr.getType() == HiveParser.TOK_TABSORTCOLNAMEASC) {
            order = RelFieldCollation.Direction.ASCENDING;
          }

          // 2.5 Add to field collations
          fieldCollations.add(new RelFieldCollation(fieldIndex, order));
        }

        // 3. Add Child Project Rel if needed, Generate Output RR, input Sel Rel
        // for top constraining Sel
        RelNode obInputRel = srcRel;
        if (!newVCLst.isEmpty()) {
          List<RexNode> originalInputRefs = Lists.transform(srcRel.getRowType().getFieldList(),
              new Function<RelDataTypeField, RexNode>() {
                @Override
                public RexNode apply(RelDataTypeField input) {
                  return new RexInputRef(input.getIndex(), input.getType());
                }
              });
          RowResolver obSyntheticProjectRR = new RowResolver();
          if (!RowResolver.add(obSyntheticProjectRR, inputRR)) {
            throw new CalciteSemanticException(
                "Duplicates detected when adding columns to RR: see previous message",
                UnsupportedFeature.Duplicates_in_RR);
          }
          int vcolPos = inputRR.getRowSchema().getSignature().size();
          for (Pair<ASTNode, TypeInfo> astTypePair : vcASTTypePairs) {
            obSyntheticProjectRR.putExpression(astTypePair.getKey(), new ColumnInfo(
                SemanticAnalyzer.getColumnInternalName(vcolPos), astTypePair.getValue(), null,
                false));
            vcolPos++;
          }
          obInputRel = genSelectRelNode(CompositeList.of(originalInputRefs, newVCLst),
              obSyntheticProjectRR, srcRel);

          if (outermostOB) {
            if (!RowResolver.add(outputRR, inputRR)) {
              throw new CalciteSemanticException(
                  "Duplicates detected when adding columns to RR: see previous message",
                  UnsupportedFeature.Duplicates_in_RR);
            }

          } else {
            if (!RowResolver.add(outputRR, obSyntheticProjectRR)) {
              throw new CalciteSemanticException(
                  "Duplicates detected when adding columns to RR: see previous message",
                  UnsupportedFeature.Duplicates_in_RR);
            }
            originalOBChild = srcRel;
          }
        } else {
          if (!RowResolver.add(outputRR, inputRR)) {
            throw new CalciteSemanticException(
                "Duplicates detected when adding columns to RR: see previous message",
                UnsupportedFeature.Duplicates_in_RR);
          }
        }

        // 4. Construct SortRel
        RelTraitSet traitSet = cluster.traitSetOf(HiveRelNode.CONVENTION);
        RelCollation canonizedCollation = traitSet.canonize(RelCollationImpl.of(fieldCollations));
        sortRel = new HiveSort(cluster, traitSet, obInputRel, canonizedCollation, null, null);

        // 5. Update the maps
        // NOTE: Output RR for SortRel is considered same as its input; we may
        // end up not using VC that is present in sort rel. Also note that
        // rowtype of sortrel is the type of it child; if child happens to be
        // synthetic project that we introduced then that projectrel would
        // contain the vc.
        ImmutableMap<String, Integer> hiveColNameCalcitePosMap = buildHiveToCalciteColumnMap(
            outputRR, sortRel);
        relToHiveRR.put(sortRel, outputRR);
        relToHiveColNameCalcitePosMap.put(sortRel, hiveColNameCalcitePosMap);
      }

      return (new Pair<RelNode, RelNode>(sortRel, originalOBChild));
    }

    private RelNode genLimitLogicalPlan(QB qb, RelNode srcRel) throws SemanticException {
      HiveRelNode sortRel = null;
      QBParseInfo qbp = getQBParseInfo(qb);
      Integer limit = qbp.getDestToLimit().get(qbp.getClauseNames().iterator().next());

      if (limit != null) {
        RexNode fetch = cluster.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(limit));
        RelTraitSet traitSet = cluster.traitSetOf(HiveRelNode.CONVENTION);
        RelCollation canonizedCollation = traitSet.canonize(RelCollations.EMPTY);
        sortRel = new HiveSort(cluster, traitSet, srcRel, canonizedCollation, null, fetch);

        RowResolver outputRR = new RowResolver();
        if (!RowResolver.add(outputRR, relToHiveRR.get(srcRel))) {
          throw new CalciteSemanticException(
              "Duplicates detected when adding columns to RR: see previous message",
              UnsupportedFeature.Duplicates_in_RR);
        }
        ImmutableMap<String, Integer> hiveColNameCalcitePosMap = buildHiveToCalciteColumnMap(
            outputRR, sortRel);
        relToHiveRR.put(sortRel, outputRR);
        relToHiveColNameCalcitePosMap.put(sortRel, hiveColNameCalcitePosMap);
      }

      return sortRel;
    }

    private List<RexNode> getPartitionKeys(PartitionSpec ps, RexNodeConverter converter,
        RowResolver inputRR) throws SemanticException {
      List<RexNode> pKeys = new ArrayList<RexNode>();
      if (ps != null) {
        List<PartitionExpression> pExprs = ps.getExpressions();
        for (PartitionExpression pExpr : pExprs) {
          TypeCheckCtx tcCtx = new TypeCheckCtx(inputRR);
          tcCtx.setAllowStatefulFunctions(true);
          ExprNodeDesc exp = genExprNodeDesc(pExpr.getExpression(), inputRR, tcCtx);
          pKeys.add(converter.convert(exp));
        }
      }

      return pKeys;
    }

    private List<RexFieldCollation> getOrderKeys(OrderSpec os, RexNodeConverter converter,
        RowResolver inputRR) throws SemanticException {
      List<RexFieldCollation> oKeys = new ArrayList<RexFieldCollation>();
      if (os != null) {
        List<OrderExpression> oExprs = os.getExpressions();
        for (OrderExpression oExpr : oExprs) {
          TypeCheckCtx tcCtx = new TypeCheckCtx(inputRR);
          tcCtx.setAllowStatefulFunctions(true);
          ExprNodeDesc exp = genExprNodeDesc(oExpr.getExpression(), inputRR, tcCtx);
          RexNode ordExp = converter.convert(exp);
          Set<SqlKind> flags = new HashSet<SqlKind>();
          if (oExpr.getOrder() == org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.Order.DESC)
            flags.add(SqlKind.DESCENDING);
          oKeys.add(new RexFieldCollation(ordExp, flags));
        }
      }

      return oKeys;
    }

    private RexWindowBound getBound(BoundarySpec bs, RexNodeConverter converter) {
      RexWindowBound rwb = null;

      if (bs != null) {
        SqlParserPos pos = new SqlParserPos(1, 1);
        SqlNode amt = bs.getAmt() == 0 ? null : SqlLiteral.createExactNumeric(
            String.valueOf(bs.getAmt()), new SqlParserPos(2, 2));
        RexNode amtLiteral = null;
        SqlCall sc = null;

        if (amt != null)
          amtLiteral = cluster.getRexBuilder().makeLiteral(new Integer(bs.getAmt()),
              cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER), true);

        switch (bs.getDirection()) {
        case PRECEDING:
          if (amt == null) {
            rwb = RexWindowBound.create(SqlWindow.createUnboundedPreceding(pos), null);
          } else {
            sc = (SqlCall) SqlWindow.createPreceding(amt, pos);
            rwb = RexWindowBound.create(sc,
                cluster.getRexBuilder().makeCall(sc.getOperator(), amtLiteral));
          }
          break;

        case CURRENT:
          rwb = RexWindowBound.create(SqlWindow.createCurrentRow(new SqlParserPos(1, 1)), null);
          break;

        case FOLLOWING:
          if (amt == null) {
            rwb = RexWindowBound.create(SqlWindow.createUnboundedFollowing(new SqlParserPos(1, 1)),
                null);
          } else {
            sc = (SqlCall) SqlWindow.createFollowing(amt, pos);
            rwb = RexWindowBound.create(sc,
                cluster.getRexBuilder().makeCall(sc.getOperator(), amtLiteral));
          }
          break;
        }
      }

      return rwb;
    }

    private int getWindowSpecIndx(ASTNode wndAST) {
      int wi = wndAST.getChildCount() - 1;
      if (wi <= 0 || (wndAST.getChild(wi).getType() != HiveParser.TOK_WINDOWSPEC)) {
        wi = -1;
      }

      return wi;
    }

    private Pair<RexNode, TypeInfo> genWindowingProj(QB qb, WindowExpressionSpec wExpSpec,
        RelNode srcRel) throws SemanticException {
      RexNode w = null;
      TypeInfo wHiveRetType = null;

      if (wExpSpec instanceof WindowFunctionSpec) {
        WindowFunctionSpec wFnSpec = (WindowFunctionSpec) wExpSpec;
        ASTNode windowProjAst = wFnSpec.getExpression();
        // TODO: do we need to get to child?
        int wndSpecASTIndx = getWindowSpecIndx(windowProjAst);
        // 2. Get Hive Aggregate Info
        AggInfo hiveAggInfo = getHiveAggInfo(windowProjAst, wndSpecASTIndx - 1,
            this.relToHiveRR.get(srcRel));

        // 3. Get Calcite Return type for Agg Fn
        wHiveRetType = hiveAggInfo.m_returnType;
        RelDataType calciteAggFnRetType = TypeConverter.convert(hiveAggInfo.m_returnType,
            this.cluster.getTypeFactory());

        // 4. Convert Agg Fn args to Calcite
        ImmutableMap<String, Integer> posMap = this.relToHiveColNameCalcitePosMap.get(srcRel);
        RexNodeConverter converter = new RexNodeConverter(this.cluster, srcRel.getRowType(),
            posMap, 0, false);
        Builder<RexNode> calciteAggFnArgsBldr = ImmutableList.<RexNode> builder();
        Builder<RelDataType> calciteAggFnArgsTypeBldr = ImmutableList.<RelDataType> builder();
        for (int i = 0; i < hiveAggInfo.m_aggParams.size(); i++) {
          calciteAggFnArgsBldr.add(converter.convert(hiveAggInfo.m_aggParams.get(i)));
          calciteAggFnArgsTypeBldr.add(TypeConverter.convert(hiveAggInfo.m_aggParams.get(i)
              .getTypeInfo(), this.cluster.getTypeFactory()));
        }
        ImmutableList<RexNode> calciteAggFnArgs = calciteAggFnArgsBldr.build();
        ImmutableList<RelDataType> calciteAggFnArgsType = calciteAggFnArgsTypeBldr.build();

        // 5. Get Calcite Agg Fn
        final SqlAggFunction calciteAggFn = SqlFunctionConverter.getCalciteAggFn(
            hiveAggInfo.m_udfName, calciteAggFnArgsType, calciteAggFnRetType);

        // 6. Translate Window spec
        RowResolver inputRR = relToHiveRR.get(srcRel);
        WindowSpec wndSpec = ((WindowFunctionSpec) wExpSpec).getWindowSpec();
        List<RexNode> partitionKeys = getPartitionKeys(wndSpec.getPartition(), converter, inputRR);
        List<RexFieldCollation> orderKeys = getOrderKeys(wndSpec.getOrder(), converter, inputRR);
        RexWindowBound upperBound = getBound(wndSpec.windowFrame.start, converter);
        RexWindowBound lowerBound = getBound(wndSpec.windowFrame.end, converter);
        boolean isRows = ((wndSpec.windowFrame.start instanceof RangeBoundarySpec) || (wndSpec.windowFrame.end instanceof RangeBoundarySpec)) ? true
            : false;

        w = cluster.getRexBuilder().makeOver(calciteAggFnRetType, calciteAggFn, calciteAggFnArgs,
            partitionKeys, ImmutableList.<RexFieldCollation> copyOf(orderKeys), lowerBound,
            upperBound, isRows, true, false);
      } else {
        // TODO: Convert to Semantic Exception
        throw new RuntimeException("Unsupported window Spec");
      }

      return new Pair<RexNode, TypeInfo>(w, wHiveRetType);
    }

    private RelNode genSelectForWindowing(QB qb, RelNode srcRel, HashSet<ColumnInfo> newColumns)
        throws SemanticException {
      getQBParseInfo(qb);
      WindowingSpec wSpec = (!qb.getAllWindowingSpecs().isEmpty()) ? qb.getAllWindowingSpecs()
          .values().iterator().next() : null;
      if (wSpec == null)
        return null;
      // 1. Get valid Window Function Spec
      wSpec.validateAndMakeEffective();
      List<WindowExpressionSpec> windowExpressions = wSpec.getWindowExpressions();
      if (windowExpressions == null || windowExpressions.isEmpty())
        return null;

      RowResolver inputRR = this.relToHiveRR.get(srcRel);
      // 2. Get RexNodes for original Projections from below
      List<RexNode> projsForWindowSelOp = new ArrayList<RexNode>(
          HiveCalciteUtil.getProjsFromBelowAsInputRef(srcRel));

      // 3. Construct new Row Resolver with everything from below.
      RowResolver out_rwsch = new RowResolver();
      if (!RowResolver.add(out_rwsch, inputRR)) {
        LOG.warn("Duplicates detected when adding columns to RR: see previous message");
      }

      // 4. Walk through Window Expressions & Construct RexNodes for those,
      // Update out_rwsch
      for (WindowExpressionSpec wExprSpec : windowExpressions) {
        if (out_rwsch.getExpression(wExprSpec.getExpression()) == null) {
          Pair<RexNode, TypeInfo> wtp = genWindowingProj(qb, wExprSpec, srcRel);
          projsForWindowSelOp.add(wtp.getKey());

          // 6.2.2 Update Output Row Schema
          ColumnInfo oColInfo = new ColumnInfo(
              SemanticAnalyzer.getColumnInternalName(projsForWindowSelOp.size()), wtp.getValue(),
              null, false);
          out_rwsch.putExpression(wExprSpec.getExpression(), oColInfo);
          newColumns.add(oColInfo);
        }
      }

      return genSelectRelNode(projsForWindowSelOp, out_rwsch, srcRel, windowExpressions);
    }

    private RelNode genSelectRelNode(List<RexNode> calciteColLst, RowResolver out_rwsch,
            RelNode srcRel) throws CalciteSemanticException {
      return genSelectRelNode(calciteColLst, out_rwsch, srcRel, null);
    }

    private RelNode genSelectRelNode(List<RexNode> calciteColLst, RowResolver out_rwsch,
        RelNode srcRel, List<WindowExpressionSpec> windowExpressions) throws CalciteSemanticException {
      // 1. Build Column Names
      Set<String> colNamesSet = new HashSet<String>();
      List<ColumnInfo> cInfoLst = out_rwsch.getRowSchema().getSignature();
      ArrayList<String> columnNames = new ArrayList<String>();
      Map<String,String> windowToAlias = null;
      if (windowExpressions != null ) {
        windowToAlias = new HashMap<String,String>();
        for (WindowExpressionSpec wes : windowExpressions) {
          windowToAlias.put(wes.getExpression().toStringTree().toLowerCase(), wes.getAlias());
        }
      }
      String[] qualifiedColNames;
      String tmpColAlias;
      for (int i = 0; i < calciteColLst.size(); i++) {
        ColumnInfo cInfo = cInfoLst.get(i);
        qualifiedColNames = out_rwsch.reverseLookup(cInfo.getInternalName());
        /*
         * if (qualifiedColNames[0] != null && !qualifiedColNames[0].isEmpty())
         * tmpColAlias = qualifiedColNames[0] + "." + qualifiedColNames[1]; else
         */
        tmpColAlias = qualifiedColNames[1];

        if (tmpColAlias.contains(".") || tmpColAlias.contains(":")) {
          tmpColAlias = cInfo.getInternalName();
        }
        // Prepend column names with '_o_' if it starts with '_c'
        /*
         * Hive treats names that start with '_c' as internalNames; so change
         * the names so we don't run into this issue when converting back to
         * Hive AST.
         */
        if (tmpColAlias.startsWith("_c")) {
          tmpColAlias = "_o_" + tmpColAlias;
        } else if (windowToAlias != null && windowToAlias.containsKey(tmpColAlias)) {
          tmpColAlias = windowToAlias.get(tmpColAlias);
        }
        int suffix = 1;
        while (colNamesSet.contains(tmpColAlias)) {
          tmpColAlias = qualifiedColNames[1] + suffix;
          suffix++;
        }

        colNamesSet.add(tmpColAlias);
        columnNames.add(tmpColAlias);
      }

      // 3 Build Calcite Rel Node for project using converted projections & col
      // names
      HiveRelNode selRel = HiveProject.create(srcRel, calciteColLst, columnNames);

      // 4. Keep track of colname-to-posmap && RR for new select
      this.relToHiveColNameCalcitePosMap
          .put(selRel, buildHiveToCalciteColumnMap(out_rwsch, selRel));
      this.relToHiveRR.put(selRel, out_rwsch);

      return selRel;
    }

    /**
     * NOTE: there can only be one select caluse since we don't handle multi
     * destination insert.
     *
     * @throws SemanticException
     */
    private RelNode genSelectLogicalPlan(QB qb, RelNode srcRel, RelNode starSrcRel)
        throws SemanticException {
      // 0. Generate a Select Node for Windowing
      // Exclude the newly-generated select columns from */etc. resolution.
      HashSet<ColumnInfo> excludedColumns = new HashSet<ColumnInfo>();
      RelNode selForWindow = genSelectForWindowing(qb, srcRel, excludedColumns);
      srcRel = (selForWindow == null) ? srcRel : selForWindow;

      ArrayList<ExprNodeDesc> col_list = new ArrayList<ExprNodeDesc>();

      // 1. Get Select Expression List
      QBParseInfo qbp = getQBParseInfo(qb);
      String selClauseName = qbp.getClauseNames().iterator().next();
      ASTNode selExprList = qbp.getSelForClause(selClauseName);

      // 2.Row resolvers for input, output
      RowResolver out_rwsch = new RowResolver();
      Integer pos = Integer.valueOf(0);
      // TODO: will this also fix windowing? try
      RowResolver inputRR = this.relToHiveRR.get(srcRel), starRR = inputRR;
      if (starSrcRel != null) {
        starRR = this.relToHiveRR.get(starSrcRel);
      }

      // 3. Query Hints
      // TODO: Handle Query Hints; currently we ignore them
      boolean selectStar = false;
      int posn = 0;
      boolean hintPresent = (selExprList.getChild(0).getType() == HiveParser.TOK_HINTLIST);
      if (hintPresent) {
        String hint = ctx.getTokenRewriteStream().toString(
            selExprList.getChild(0).getTokenStartIndex(),
            selExprList.getChild(0).getTokenStopIndex());
        String msg = String.format("Hint specified for %s."
            + " Currently we don't support hints in CBO, turn off cbo to use hints.", hint);
        LOG.debug(msg);
        throw new CalciteSemanticException(msg, UnsupportedFeature.Hint);
      }

      // 4. Bailout if select involves Transform
      boolean isInTransform = (selExprList.getChild(posn).getChild(0).getType() == HiveParser.TOK_TRANSFORM);
      if (isInTransform) {
        String msg = String.format("SELECT TRANSFORM is currently not supported in CBO,"
            + " turn off cbo to use TRANSFORM.");
        LOG.debug(msg);
        throw new CalciteSemanticException(msg, UnsupportedFeature.Select_transform);
      }

      // 5. Bailout if select involves UDTF
      ASTNode expr = (ASTNode) selExprList.getChild(posn).getChild(0);
      int exprType = expr.getType();
      if (exprType == HiveParser.TOK_FUNCTION || exprType == HiveParser.TOK_FUNCTIONSTAR) {
        String funcName = TypeCheckProcFactory.DefaultExprProcessor.getFunctionText(expr, true);
        FunctionInfo fi = FunctionRegistry.getFunctionInfo(funcName);
        if (fi != null && fi.getGenericUDTF() != null) {
          String msg = String.format("UDTF " + funcName + " is currently not supported in CBO,"
              + " turn off cbo to use UDTF " + funcName);
          LOG.debug(msg);
          throw new CalciteSemanticException(msg, UnsupportedFeature.UDTF);
        }
      }

      // 6. Iterate over all expression (after SELECT)
      ASTNode exprList = selExprList;
      int startPosn = posn;
      List<String> tabAliasesForAllProjs = getTabAliases(starRR);
      for (int i = startPosn; i < exprList.getChildCount(); ++i) {

        // 6.1 child can be EXPR AS ALIAS, or EXPR.
        ASTNode child = (ASTNode) exprList.getChild(i);
        boolean hasAsClause = (!isInTransform) && (child.getChildCount() == 2);

        // 6.2 EXPR AS (ALIAS,...) parses, but is only allowed for UDTF's
        // This check is not needed and invalid when there is a transform b/c
        // the
        // AST's are slightly different.
        if (child.getChildCount() > 2) {
          throw new SemanticException(SemanticAnalyzer.generateErrorMessage(
              (ASTNode) child.getChild(2), ErrorMsg.INVALID_AS.getMsg()));
        }

        String tabAlias;
        String colAlias;

        // 6.3 Get rid of TOK_SELEXPR
        expr = (ASTNode) child.getChild(0);
        String[] colRef = SemanticAnalyzer.getColAlias(child, getAutogenColAliasPrfxLbl(), inputRR,
            autogenColAliasPrfxIncludeFuncName(), i);
        tabAlias = colRef[0];
        colAlias = colRef[1];

        // 6.4 Build ExprNode corresponding to colums
        if (expr.getType() == HiveParser.TOK_ALLCOLREF) {
          pos = genColListRegex(".*", expr.getChildCount() == 0 ? null : SemanticAnalyzer
              .getUnescapedName((ASTNode) expr.getChild(0)).toLowerCase(), expr, col_list,
              excludedColumns, inputRR, starRR, pos, out_rwsch, tabAliasesForAllProjs, true);
          selectStar = true;
        } else if (expr.getType() == HiveParser.TOK_TABLE_OR_COL
            && !hasAsClause
            && !inputRR.getIsExprResolver()
            && SemanticAnalyzer.isRegex(
                SemanticAnalyzer.unescapeIdentifier(expr.getChild(0).getText()), conf)) {
          // In case the expression is a regex COL.
          // This can only happen without AS clause
          // We don't allow this for ExprResolver - the Group By case
          pos = genColListRegex(SemanticAnalyzer.unescapeIdentifier(expr.getChild(0).getText()),
              null, expr, col_list, excludedColumns, inputRR, starRR, pos, out_rwsch,
              tabAliasesForAllProjs, true);
        } else if (expr.getType() == HiveParser.DOT
            && expr.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
            && inputRR.hasTableAlias(SemanticAnalyzer.unescapeIdentifier(expr.getChild(0)
                .getChild(0).getText().toLowerCase()))
            && !hasAsClause
            && !inputRR.getIsExprResolver()
            && SemanticAnalyzer.isRegex(
                SemanticAnalyzer.unescapeIdentifier(expr.getChild(1).getText()), conf)) {
          // In case the expression is TABLE.COL (col can be regex).
          // This can only happen without AS clause
          // We don't allow this for ExprResolver - the Group By case
          pos = genColListRegex(
              SemanticAnalyzer.unescapeIdentifier(expr.getChild(1).getText()),
              SemanticAnalyzer.unescapeIdentifier(expr.getChild(0).getChild(0).getText()
                  .toLowerCase()), expr, col_list, excludedColumns, inputRR, starRR, pos,
              out_rwsch, tabAliasesForAllProjs, true);
        } else if (expr.toStringTree().contains("TOK_FUNCTIONDI")
            && !(srcRel instanceof HiveAggregate)) {
          // Likely a malformed query eg, select hash(distinct c1) from t1;
          throw new CalciteSemanticException("Distinct without an aggreggation.",
              UnsupportedFeature.Distinct_without_an_aggreggation);
        } else {
          // Case when this is an expression
          TypeCheckCtx tcCtx = new TypeCheckCtx(inputRR);
          // We allow stateful functions in the SELECT list (but nowhere else)
          tcCtx.setAllowStatefulFunctions(true);
          ExprNodeDesc exp = genExprNodeDesc(expr, inputRR, tcCtx);
          String recommended = recommendName(exp, colAlias);
          if (recommended != null && out_rwsch.get(null, recommended) == null) {
            colAlias = recommended;
          }
          col_list.add(exp);

          ColumnInfo colInfo = new ColumnInfo(SemanticAnalyzer.getColumnInternalName(pos),
              exp.getWritableObjectInspector(), tabAlias, false);
          colInfo.setSkewedCol((exp instanceof ExprNodeColumnDesc) ? ((ExprNodeColumnDesc) exp)
              .isSkewedCol() : false);
          if (!out_rwsch.putWithCheck(tabAlias, colAlias, null, colInfo)) {
            throw new CalciteSemanticException("Cannot add column to RR: " + tabAlias + "."
                + colAlias + " => " + colInfo + " due to duplication, see previous warnings",
                UnsupportedFeature.Duplicates_in_RR);
          }

          if (exp instanceof ExprNodeColumnDesc) {
            ExprNodeColumnDesc colExp = (ExprNodeColumnDesc) exp;
            String[] altMapping = inputRR.getAlternateMappings(colExp.getColumn());
            if (altMapping != null) {
              out_rwsch.put(altMapping[0], altMapping[1], colInfo);
            }
          }

          pos = Integer.valueOf(pos.intValue() + 1);
        }
      }
      selectStar = selectStar && exprList.getChildCount() == posn + 1;

      // 7. Convert Hive projections to Calcite
      List<RexNode> calciteColLst = new ArrayList<RexNode>();
      RexNodeConverter rexNodeConv = new RexNodeConverter(cluster, srcRel.getRowType(),
          buildHiveColNameToInputPosMap(col_list, inputRR), 0, false);
      for (ExprNodeDesc colExpr : col_list) {
        calciteColLst.add(rexNodeConv.convert(colExpr));
      }

      // 8. Build Calcite Rel
      RelNode selRel = genSelectRelNode(calciteColLst, out_rwsch, srcRel);

      return selRel;
    }

    private RelNode genLogicalPlan(QBExpr qbexpr) throws SemanticException {
      if (qbexpr.getOpcode() == QBExpr.Opcode.NULLOP) {
        return genLogicalPlan(qbexpr.getQB(), false);
      }
      if (qbexpr.getOpcode() == QBExpr.Opcode.UNION) {
        RelNode qbexpr1Ops = genLogicalPlan(qbexpr.getQBExpr1());
        RelNode qbexpr2Ops = genLogicalPlan(qbexpr.getQBExpr2());

        return genUnionLogicalPlan(qbexpr.getAlias(), qbexpr.getQBExpr1().getAlias(), qbexpr1Ops,
            qbexpr.getQBExpr2().getAlias(), qbexpr2Ops);
      }
      return null;
    }

    private RelNode genLogicalPlan(QB qb, boolean outerMostQB) throws SemanticException {
      RelNode srcRel = null;
      RelNode filterRel = null;
      RelNode gbRel = null;
      RelNode gbHavingRel = null;
      RelNode selectRel = null;
      RelNode obRel = null;
      RelNode limitRel = null;

      // First generate all the opInfos for the elements in the from clause
      Map<String, RelNode> aliasToRel = new HashMap<String, RelNode>();

      // 0. Check if we can handle the SubQuery;
      // canHandleQbForCbo returns null if the query can be handled.
      String reason = canHandleQbForCbo(queryProperties, conf, false, LOG.isDebugEnabled(), qb);
      if (reason != null) {
        String msg = "CBO can not handle Sub Query";
        if (LOG.isDebugEnabled()) {
          LOG.debug(msg + " because it: " + reason);
        }
        throw new CalciteSemanticException(msg, UnsupportedFeature.Subquery);
      }

      // 1. Build Rel For Src (SubQuery, TS, Join)
      // 1.1. Recurse over the subqueries to fill the subquery part of the plan
      for (String subqAlias : qb.getSubqAliases()) {
        QBExpr qbexpr = qb.getSubqForAlias(subqAlias);
        aliasToRel.put(subqAlias, genLogicalPlan(qbexpr));
      }

      // 1.2 Recurse over all the source tables
      for (String tableAlias : qb.getTabAliases()) {
        RelNode op = genTableLogicalPlan(tableAlias, qb);
        aliasToRel.put(tableAlias, op);
      }

      if (aliasToRel.isEmpty()) {
        // // This may happen for queries like select 1; (no source table)
        // We can do following which is same, as what Hive does.
        // With this, we will be able to generate Calcite plan.
        // qb.getMetaData().setSrcForAlias(DUMMY_TABLE, getDummyTable());
        // RelNode op = genTableLogicalPlan(DUMMY_TABLE, qb);
        // qb.addAlias(DUMMY_TABLE);
        // qb.setTabAlias(DUMMY_TABLE, DUMMY_TABLE);
        // aliasToRel.put(DUMMY_TABLE, op);
        // However, Hive trips later while trying to get Metadata for this dummy
        // table
        // So, for now lets just disable this. Anyway there is nothing much to
        // optimize in such cases.
        throw new CalciteSemanticException("Unsupported", UnsupportedFeature.Others);

      }
      // 1.3 process join
      if (qb.getParseInfo().getJoinExpr() != null) {
        srcRel = genJoinLogicalPlan(qb.getParseInfo().getJoinExpr(), aliasToRel);
      } else {
        // If no join then there should only be either 1 TS or 1 SubQuery
        srcRel = aliasToRel.values().iterator().next();
      }

      // 2. Build Rel for where Clause
      filterRel = genFilterLogicalPlan(qb, srcRel, aliasToRel, false);
      srcRel = (filterRel == null) ? srcRel : filterRel;
      RelNode starSrcRel = srcRel;

      // 3. Build Rel for GB Clause
      gbRel = genGBLogicalPlan(qb, srcRel);
      srcRel = (gbRel == null) ? srcRel : gbRel;

      // 4. Build Rel for GB Having Clause
      gbHavingRel = genGBHavingLogicalPlan(qb, srcRel, aliasToRel);
      srcRel = (gbHavingRel == null) ? srcRel : gbHavingRel;

      // 5. Build Rel for Select Clause
      selectRel = genSelectLogicalPlan(qb, srcRel, starSrcRel);
      srcRel = (selectRel == null) ? srcRel : selectRel;

      // 6. Build Rel for OB Clause
      Pair<RelNode, RelNode> obTopProjPair = genOBLogicalPlan(qb, srcRel, outerMostQB);
      obRel = obTopProjPair.getKey();
      RelNode topConstrainingProjArgsRel = obTopProjPair.getValue();
      srcRel = (obRel == null) ? srcRel : obRel;

      // 7. Build Rel for Limit Clause
      limitRel = genLimitLogicalPlan(qb, srcRel);
      srcRel = (limitRel == null) ? srcRel : limitRel;

      // 8. Introduce top constraining select if needed.
      // NOTES:
      // 1. Calcite can not take an expr in OB; hence it needs to be added as VC
      // in the input select; In such cases we need to introduce a select on top
      // to ensure VC is not visible beyond Limit, OB.
      // 2. Hive can not preserve order across select. In subqueries OB is used
      // to get a deterministic set of tuples from following limit. Hence we
      // introduce the constraining select above Limit (if present) instead of
      // OB.
      // 3. The top level OB will not introduce constraining select due to Hive
      // limitation(#2) stated above. The RR for OB will not include VC. Thus
      // Result Schema will not include exprs used by top OB. During AST Conv,
      // in the PlanModifierForASTConv we would modify the top level OB to
      // migrate exprs from input sel to SortRel (Note that Calcite doesn't
      // support this; but since we are done with Calcite at this point its OK).
      if (topConstrainingProjArgsRel != null) {
        List<RexNode> originalInputRefs = Lists.transform(topConstrainingProjArgsRel.getRowType()
            .getFieldList(), new Function<RelDataTypeField, RexNode>() {
          @Override
          public RexNode apply(RelDataTypeField input) {
            return new RexInputRef(input.getIndex(), input.getType());
          }
        });
        RowResolver topConstrainingProjRR = new RowResolver();
        if (!RowResolver.add(topConstrainingProjRR,
            this.relToHiveRR.get(topConstrainingProjArgsRel))) {
          LOG.warn("Duplicates detected when adding columns to RR: see previous message");
        }
        srcRel = genSelectRelNode(originalInputRefs, topConstrainingProjRR, srcRel);
      }

      // 9. Incase this QB corresponds to subquery then modify its RR to point
      // to subquery alias
      // TODO: cleanup this
      if (qb.getParseInfo().getAlias() != null) {
        RowResolver rr = this.relToHiveRR.get(srcRel);
        RowResolver newRR = new RowResolver();
        String alias = qb.getParseInfo().getAlias();
        for (ColumnInfo colInfo : rr.getColumnInfos()) {
          String name = colInfo.getInternalName();
          String[] tmp = rr.reverseLookup(name);
          if ("".equals(tmp[0]) || tmp[1] == null) {
            // ast expression is not a valid column name for table
            tmp[1] = colInfo.getInternalName();
          }
          ColumnInfo newCi = new ColumnInfo(colInfo);
          newCi.setTabAlias(alias);
          newRR.put(alias, tmp[1], newCi);
        }
        relToHiveRR.put(srcRel, newRR);
        relToHiveColNameCalcitePosMap.put(srcRel, buildHiveToCalciteColumnMap(newRR, srcRel));
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Created Plan for Query Block " + qb.getId());
      }

      setQB(qb);
      return srcRel;
    }

    private RelNode genGBHavingLogicalPlan(QB qb, RelNode srcRel, Map<String, RelNode> aliasToRel)
        throws SemanticException {
      RelNode gbFilter = null;
      QBParseInfo qbp = getQBParseInfo(qb);
      ASTNode havingClause = qbp.getHavingForClause(qbp.getClauseNames().iterator().next());

      if (havingClause != null) {
        if (!(srcRel instanceof HiveAggregate)) {
          // ill-formed query like select * from t1 having c1 > 0;
          throw new CalciteSemanticException("Having clause without any group-by.",
              UnsupportedFeature.Having_clause_without_any_groupby);
        }
        validateNoHavingReferenceToAlias(qb, (ASTNode) havingClause.getChild(0));
        gbFilter = genFilterRelNode(qb, (ASTNode) havingClause.getChild(0), srcRel, aliasToRel,
            true);
      }

      return gbFilter;
    }

    /*
     * Bail if having clause uses Select Expression aliases for Aggregation
     * expressions. We could do what Hive does. But this is non standard
     * behavior. Making sure this doesn't cause issues when translating through
     * Calcite is not worth it.
     */
    private void validateNoHavingReferenceToAlias(QB qb, ASTNode havingExpr)
        throws CalciteSemanticException {

      QBParseInfo qbPI = qb.getParseInfo();
      Map<ASTNode, String> exprToAlias = qbPI.getAllExprToColumnAlias();
      /*
       * a mouthful, but safe: - a QB is guaranteed to have atleast 1
       * destination - we don't support multi insert, so picking the first dest.
       */
      Set<String> aggExprs = qbPI.getDestToAggregationExprs().values().iterator().next().keySet();

      for (Map.Entry<ASTNode, String> selExpr : exprToAlias.entrySet()) {
        ASTNode selAST = selExpr.getKey();
        if (!aggExprs.contains(selAST.toStringTree().toLowerCase())) {
          continue;
        }
        final String aliasToCheck = selExpr.getValue();
        final Set<Object> aliasReferences = new HashSet<Object>();
        TreeVisitorAction action = new TreeVisitorAction() {

          @Override
          public Object pre(Object t) {
            if (ParseDriver.adaptor.getType(t) == HiveParser.TOK_TABLE_OR_COL) {
              Object c = ParseDriver.adaptor.getChild(t, 0);
              if (c != null && ParseDriver.adaptor.getType(c) == HiveParser.Identifier
                  && ParseDriver.adaptor.getText(c).equals(aliasToCheck)) {
                aliasReferences.add(t);
              }
            }
            return t;
          }

          @Override
          public Object post(Object t) {
            return t;
          }
        };
        new TreeVisitor(ParseDriver.adaptor).visit(havingExpr, action);

        if (aliasReferences.size() > 0) {
          String havingClause = ctx.getTokenRewriteStream().toString(
              havingExpr.getTokenStartIndex(), havingExpr.getTokenStopIndex());
          String msg = String.format("Encountered Select alias '%s' in having clause '%s'"
              + " This non standard behavior is not supported with cbo on."
              + " Turn off cbo for these queries.", aliasToCheck, havingClause);
          LOG.debug(msg);
          throw new CalciteSemanticException(msg, UnsupportedFeature.Select_alias_in_having_clause);
        }
      }

    }

    private ImmutableMap<String, Integer> buildHiveToCalciteColumnMap(RowResolver rr, RelNode rNode) {
      ImmutableMap.Builder<String, Integer> b = new ImmutableMap.Builder<String, Integer>();
      for (ColumnInfo ci : rr.getRowSchema().getSignature()) {
        b.put(ci.getInternalName(), rr.getPosition(ci.getInternalName()));
      }
      return b.build();
    }

    private ImmutableMap<String, Integer> buildHiveColNameToInputPosMap(
        List<ExprNodeDesc> col_list, RowResolver inputRR) {
      // Build a map of Hive column Names (ExprNodeColumnDesc Name)
      // to the positions of those projections in the input
      Map<Integer, ExprNodeDesc> hashCodeTocolumnDescMap = new HashMap<Integer, ExprNodeDesc>();
      ExprNodeDescUtils.getExprNodeColumnDesc(col_list, hashCodeTocolumnDescMap);
      ImmutableMap.Builder<String, Integer> hiveColNameToInputPosMapBuilder = new ImmutableMap.Builder<String, Integer>();
      String exprNodecolName;
      for (ExprNodeDesc exprDesc : hashCodeTocolumnDescMap.values()) {
        exprNodecolName = ((ExprNodeColumnDesc) exprDesc).getColumn();
        hiveColNameToInputPosMapBuilder.put(exprNodecolName, inputRR.getPosition(exprNodecolName));
      }

      return hiveColNameToInputPosMapBuilder.build();
    }

    private QBParseInfo getQBParseInfo(QB qb) throws CalciteSemanticException {
      QBParseInfo qbp = qb.getParseInfo();
      if (qbp.getClauseNames().size() > 1) {
        String msg = String.format("Multi Insert is currently not supported in CBO,"
            + " turn off cbo to use Multi Insert.");
        LOG.debug(msg);
        throw new CalciteSemanticException(msg, UnsupportedFeature.Multi_insert);
      }
      return qbp;
    }

    private List<String> getTabAliases(RowResolver inputRR) {
      List<String> tabAliases = new ArrayList<String>(); // TODO: this should be
                                                         // unique
      for (ColumnInfo ci : inputRR.getColumnInfos()) {
        tabAliases.add(ci.getTabAlias());
      }

      return tabAliases;
    }
  }

}
