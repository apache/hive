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

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.math.BigDecimal;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.antlr.runtime.ClassicToken;
import org.antlr.runtime.CommonToken;
import org.antlr.runtime.tree.TreeVisitor;
import org.antlr.runtime.tree.TreeVisitorAction;
import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.adapter.druid.DruidRules;
import org.apache.calcite.adapter.druid.DruidSchema;
import org.apache.calcite.adapter.druid.DruidTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptPlanner.Executor;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.SemiJoinFilterTransposeRule;
import org.apache.calcite.rel.rules.SemiJoinJoinTransposeRule;
import org.apache.calcite.rel.rules.SemiJoinProjectTransposeRule;
import org.apache.calcite.rel.rules.UnionMergeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.CompositeList;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveConf.StrictChecks;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSubquerySemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException.UnsupportedFeature;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteViewSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveDefaultRelMetadataProvider;
import org.apache.hadoop.hive.ql.optimizer.calcite.HivePlannerContext;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRexExecutorImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveAlgorithmsConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveVolcanoPlanner;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveExcept;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveGroupingID;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveIntersect;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableFunctionScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveAggregateJoinTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveAggregateProjectMergeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveAggregatePullUpConstantsRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveExceptRewriteRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveExpandDistinctAggregatesRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveFilterAggregateTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveFilterJoinRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveFilterProjectTSTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveFilterProjectTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveFilterSetOpTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveFilterSortTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveInsertExchange4JoinRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveIntersectMergeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveIntersectRewriteRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveJoinAddNotNullRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveJoinCommuteRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveJoinProjectTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveJoinPushTransitivePredicatesRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveJoinToMultiJoinRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HivePartitionPruneRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HivePointLookupOptimizerRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HivePreFilteringRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveProjectFilterPullUpConstantsRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveProjectMergeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveProjectOverIntersectRemoveRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveProjectSortTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveReduceExpressionsRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveReduceExpressionsWithStatsRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveRelDecorrelator;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveRelFieldTrimmer;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveRulesRegistry;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveSemiJoinRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveSortJoinReduceRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveSortLimitPullUpConstantsRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveSortMergeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveSortProjectTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveSortRemoveRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveSortUnionReduceRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveSubQueryRemoveRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveUnionPullUpConstantsRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveWindowingFixRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewFilterScanRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ASTBuilder;
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
import org.apache.hadoop.hive.ql.parse.QBExpr.Opcode;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowExpressionSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowFunctionSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowType;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.joda.time.Interval;

import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

public class CalcitePlanner extends SemanticAnalyzer {

  private final AtomicInteger noColsMissingStats = new AtomicInteger(0);
  private SemanticException semanticException;
  private boolean runCBO = true;
  private boolean disableSemJoinReordering = true;
  private EnumSet<ExtendedCBOProfile> profilesCBO;

  public CalcitePlanner(QueryState queryState) throws SemanticException {
    super(queryState);
    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CBO_ENABLED)) {
      runCBO = false;
      disableSemJoinReordering = false;
    }
  }

  public void resetCalciteConfiguration() {
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CBO_ENABLED)) {
      runCBO = true;
      disableSemJoinReordering = true;
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

  /**
   * This method is useful if we want to obtain the logical plan after being parsed and
   * optimized by Calcite.
   *
   * @return the Calcite plan for the query, null if it could not be generated
   */
  public RelNode genLogicalPlan(ASTNode ast) throws SemanticException {
    LOG.info("Starting generating logical plan");
    PreCboCtx cboCtx = new PreCboCtx();
    //change the location of position alias process here
    processPositionAlias(ast);
    if (!genResolvedParseTree(ast, cboCtx)) {
      return null;
    }
    ASTNode queryForCbo = ast;
    if (cboCtx.type == PreCboCtx.Type.CTAS || cboCtx.type == PreCboCtx.Type.VIEW) {
      queryForCbo = cboCtx.nodeOfInterest; // nodeOfInterest is the query
    }
    runCBO = canCBOHandleAst(queryForCbo, getQB(), cboCtx);
    if (!runCBO) {
      return null;
    }
    profilesCBO = obtainCBOProfiles(queryProperties);
    disableJoinMerge = true;
    final RelNode resPlan = logicalPlan();
    LOG.info("Finished generating logical plan");
    return resPlan;
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
      if (cboCtx.type == PreCboCtx.Type.CTAS || cboCtx.type == PreCboCtx.Type.VIEW) {
        queryForCbo = cboCtx.nodeOfInterest; // nodeOfInterest is the query
      }
      runCBO = canCBOHandleAst(queryForCbo, getQB(), cboCtx);
      if (queryProperties.hasMultiDestQuery()) {
        handleMultiDestQuery(ast, cboCtx);
      }

      if (runCBO) {
        profilesCBO = obtainCBOProfiles(queryProperties);

        disableJoinMerge = true;
        boolean reAnalyzeAST = false;
        final boolean materializedView = getQB().isMaterializedView();

        try {
          if (this.conf.getBoolVar(HiveConf.ConfVars.HIVE_CBO_RETPATH_HIVEOP)) {
            if (cboCtx.type == PreCboCtx.Type.VIEW && !materializedView) {
              throw new SemanticException("Create view is not supported in cbo return path.");
            }
            sinkOp = getOptimizedHiveOPDag();
            LOG.info("CBO Succeeded; optimized logical plan.");
            this.ctx.setCboInfo("Plan optimized by CBO.");
            this.ctx.setCboSucceeded(true);
          } else {
            // 1. Gen Optimized AST
            ASTNode newAST = getOptimizedAST();

            // 1.1. Fix up the query for insert/ctas/materialized views
            newAST = fixUpAfterCbo(ast, newAST, cboCtx);

            // 2. Regen OP plan from optimized AST
            if (cboCtx.type == PreCboCtx.Type.VIEW && !materializedView) {
              try {
                handleCreateViewDDL(newAST);
              } catch (SemanticException e) {
                throw new CalciteViewSemanticException(e.getMessage());
              }
            } else {
              init(false);
              if (cboCtx.type == PreCboCtx.Type.VIEW && materializedView) {
                // Redo create-table/view analysis, because it's not part of
                // doPhase1.
                // Use the REWRITTEN AST
                setAST(newAST);
                newAST = reAnalyzeViewAfterCbo(newAST);
                // Store text of the ORIGINAL QUERY
                String originalText = ctx.getTokenRewriteStream().toString(
                    cboCtx.nodeOfInterest.getTokenStartIndex(),
                    cboCtx.nodeOfInterest.getTokenStopIndex());
                createVwDesc.setViewOriginalText(originalText);
                viewSelect = newAST;
                viewsExpanded = new ArrayList<>();
                viewsExpanded.add(createVwDesc.getViewName());
              } else if (cboCtx.type == PreCboCtx.Type.CTAS) {
                // CTAS
                setAST(newAST);
                newAST = reAnalyzeCTASAfterCbo(newAST);
              }
            }
            Phase1Ctx ctx_1 = initPhase1Ctx();
            if (!doPhase1(newAST, getQB(), ctx_1, null)) {
              throw new RuntimeException("Couldn't do phase1 on CBO optimized query plan");
            }
            // unfortunately making prunedPartitions immutable is not possible
            // here with SemiJoins not all tables are costed in CBO, so their
            // PartitionList is not evaluated until the run phase.
            getMetaData(getQB());

            disableJoinMerge = defaultJoinMerge;
            sinkOp = genPlan(getQB());
            LOG.info("CBO Succeeded; optimized logical plan.");
            this.ctx.setCboInfo("Plan optimized by CBO.");
            this.ctx.setCboSucceeded(true);
            if (LOG.isTraceEnabled()) {
              LOG.trace(newAST.dump());
            }
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
          if( e instanceof CalciteSubquerySemanticException) {
            // non-cbo path retries to execute subqueries and throws completely different exception/error
            // to eclipse the original error message
            // so avoid executing subqueries on non-cbo
            throw new SemanticException(e);
          }
          else if( e instanceof CalciteViewSemanticException) {
            // non-cbo path retries to execute create view and
            // we believe it will throw the same error message
            throw new SemanticException(e);
          }
          else if (!conf.getBoolVar(ConfVars.HIVE_IN_TEST) || isMissingStats
              || e instanceof CalciteSemanticException ) {
              reAnalyzeAST = true;
          } else if (e instanceof SemanticException) {
            // although, its likely to be a valid exception, we will retry
            // with cbo off anyway.
              reAnalyzeAST = true;
          } else if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
          } else {
            throw new SemanticException(e);
          }
        } finally {
          runCBO = false;
          disableJoinMerge = defaultJoinMerge;
          disableSemJoinReordering = false;
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

  private void handleCreateViewDDL(ASTNode newAST) throws SemanticException {
    saveViewDefinition();
    String originalText = createVwDesc.getViewOriginalText();
    String expandedText = createVwDesc.getViewExpandedText();
    List<FieldSchema> schema = createVwDesc.getSchema();
    List<FieldSchema> partitionColumns = createVwDesc.getPartCols();
    init(false);
    setAST(newAST);
    newAST = reAnalyzeViewAfterCbo(newAST);
    createVwDesc.setViewOriginalText(originalText);
    createVwDesc.setViewExpandedText(expandedText);
    createVwDesc.setSchema(schema);
    createVwDesc.setPartCols(partitionColumns);
  }

  /*
   * Tries to optimize FROM clause of multi-insert. No attempt to optimize insert clauses of the query.
   * Returns true if rewriting is successful, false otherwise.
   */
  private void handleMultiDestQuery(ASTNode ast, PreCboCtx cboCtx) throws SemanticException {
    // Not supported by CBO
    if (!runCBO) {
      return;
    }
    // Currently, we only optimized the query the content of the FROM clause
    // for multi-insert queries. Thus, nodeOfInterest is the FROM clause
    if (isJoinToken(cboCtx.nodeOfInterest)) {
      // Join clause: rewriting is needed
      ASTNode subq = rewriteASTForMultiInsert(ast, cboCtx.nodeOfInterest);
      if (subq != null) {
        // We could rewrite into a subquery
        cboCtx.nodeOfInterest = (ASTNode) subq.getChild(0);
        QB newQB = new QB(null, "", false);
        Phase1Ctx ctx_1 = initPhase1Ctx();
        doPhase1(cboCtx.nodeOfInterest, newQB, ctx_1, null);
        setQB(newQB);
        getMetaData(getQB());
      } else {
        runCBO = false;
      }
    } else if (cboCtx.nodeOfInterest.getToken().getType() == HiveParser.TOK_SUBQUERY) {
      // Subquery: no rewriting needed
      ASTNode subq = cboCtx.nodeOfInterest;
      // First child is subquery, second child is alias
      // We set the node of interest and QB to the subquery
      // We do not need to generate the QB again, but rather we use it directly
      cboCtx.nodeOfInterest = (ASTNode) subq.getChild(0);
      String subQAlias = unescapeIdentifier(subq.getChild(1).getText());
      final QB newQB = getQB().getSubqForAlias(subQAlias).getQB();
      newQB.getParseInfo().setAlias("");
      newQB.getParseInfo().setIsSubQ(false);
      setQB(newQB);
    } else {
      // No need to run CBO (table ref or virtual table) or not supported
      runCBO = false;
    }
  }

  private ASTNode rewriteASTForMultiInsert(ASTNode query, ASTNode nodeOfInterest) {
    // 1. gather references from original query
    // This is a map from aliases to references.
    // We keep all references as we will need to modify them after creating
    // the subquery
    final Multimap<String, Object> aliasNodes = ArrayListMultimap.create();
    // To know if we need to bail out
    final AtomicBoolean notSupported = new AtomicBoolean(false);
    TreeVisitorAction action = new TreeVisitorAction() {
      @Override
      public Object pre(Object t) {
        if (!notSupported.get()) {
          if (ParseDriver.adaptor.getType(t) == HiveParser.TOK_ALLCOLREF) {
            // TODO: this is a limitation of the AST rewriting approach that we will
            // not be able to overcome till proper integration of full multi-insert
            // queries with Calcite is implemented.
            // The current rewriting gather references from insert clauses and then
            // updates them with the new subquery references. However, if insert
            // clauses use * or tab.*, we cannot resolve the columns that we are
            // referring to. Thus, we just bail out and those queries will not be
            // currently optimized by Calcite.
            // An example of such query is:
            // FROM T_A a LEFT JOIN T_B b ON a.id = b.id
            // INSERT OVERWRITE TABLE join_result_1
            // SELECT a.*, b.*
            // INSERT OVERWRITE TABLE join_result_3
            // SELECT a.*, b.*;
            notSupported.set(true);
          } else if (ParseDriver.adaptor.getType(t) == HiveParser.DOT) {
            Object c = ParseDriver.adaptor.getChild(t, 0);
            if (c != null && ParseDriver.adaptor.getType(c) == HiveParser.TOK_TABLE_OR_COL) {
              aliasNodes.put(((ASTNode) t).toStringTree(), t);
            }
          } else if (ParseDriver.adaptor.getType(t) == HiveParser.TOK_TABLE_OR_COL) {
            Object p = ParseDriver.adaptor.getParent(t);
            if (p == null || ParseDriver.adaptor.getType(p) != HiveParser.DOT) {
              aliasNodes.put(((ASTNode) t).toStringTree(), t);
            }
          }
        }
        return t;
      }
      @Override
      public Object post(Object t) {
        return t;
      }
    };
    TreeVisitor tv = new TreeVisitor(ParseDriver.adaptor);
    // We will iterate through the children: if it is an INSERT, we will traverse
    // the subtree to gather the references
    for (int i = 0; i < query.getChildCount(); i++) {
      ASTNode child = (ASTNode) query.getChild(i);
      if (ParseDriver.adaptor.getType(child) != HiveParser.TOK_INSERT) {
        // If it is not an INSERT, we do not need to anything
        continue;
      }
      tv.visit(child, action);
    }
    if (notSupported.get()) {
      // Bail out
      return null;
    }
    // 2. rewrite into query
    //  TOK_QUERY
    //     TOK_FROM
    //        join
    //     TOK_INSERT
    //        TOK_DESTINATION
    //           TOK_DIR
    //              TOK_TMP_FILE
    //        TOK_SELECT
    //           refs
    ASTNode from = new ASTNode(new CommonToken(HiveParser.TOK_FROM, "TOK_FROM"));
    from.addChild((ASTNode) ParseDriver.adaptor.dupTree(nodeOfInterest));
    ASTNode destination = new ASTNode(new CommonToken(HiveParser.TOK_DESTINATION, "TOK_DESTINATION"));
    ASTNode dir = new ASTNode(new CommonToken(HiveParser.TOK_DIR, "TOK_DIR"));
    ASTNode tmpFile = new ASTNode(new CommonToken(HiveParser.TOK_TMP_FILE, "TOK_TMP_FILE"));
    dir.addChild(tmpFile);
    destination.addChild(dir);
    ASTNode select = new ASTNode(new CommonToken(HiveParser.TOK_SELECT, "TOK_SELECT"));
    int num = 0;
    for (Collection<Object> selectIdentifier : aliasNodes.asMap().values()) {
      Iterator<Object> it = selectIdentifier.iterator();
      ASTNode node = (ASTNode) it.next();
      // Add select expression
      ASTNode selectExpr = new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR, "TOK_SELEXPR"));
      selectExpr.addChild((ASTNode) ParseDriver.adaptor.dupTree(node)); // Identifier
      String colAlias = "col" + num;
      selectExpr.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, colAlias))); // Alias
      select.addChild(selectExpr);
      // Rewrite all INSERT references (all the node values for this key)
      ASTNode colExpr = new ASTNode(new CommonToken(HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL"));
      colExpr.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, colAlias)));
      replaceASTChild(node, colExpr);
      while (it.hasNext()) {
        // Loop to rewrite rest of INSERT references
        node = (ASTNode) it.next();
        colExpr = new ASTNode(new CommonToken(HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL"));
        colExpr.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, colAlias)));
        replaceASTChild(node, colExpr);
      }
      num++;
    }
    ASTNode insert = new ASTNode(new CommonToken(HiveParser.TOK_INSERT, "TOK_INSERT"));
    insert.addChild(destination);
    insert.addChild(select);
    ASTNode newQuery = new ASTNode(new CommonToken(HiveParser.TOK_QUERY, "TOK_QUERY"));
    newQuery.addChild(from);
    newQuery.addChild(insert);
    // 3. create subquery
    ASTNode subq = new ASTNode(new CommonToken(HiveParser.TOK_SUBQUERY, "TOK_SUBQUERY"));
    subq.addChild(newQuery);
    subq.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, "subq")));
    replaceASTChild(nodeOfInterest, subq);
    // 4. return subquery
    return subq;
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
        || qb.isCTAS() || qb.isMaterializedView();
    // Queries without a source table currently are not supported by CBO
    boolean isSupportedType = (qb.getIsQuery() && !qb.containsQueryWithoutSourceTable())
        || qb.isCTAS() || qb.isMaterializedView() || cboCtx.type == PreCboCtx.Type.INSERT
        || cboCtx.type == PreCboCtx.Type.MULTI_INSERT;
    boolean noBadTokens = HiveCalciteUtil.validateASTForUnsupportedTokens(ast);
    boolean result = isSupportedRoot && isSupportedType && noBadTokens;

    if (!result) {
      if (needToLogMessage) {
        String msg = "";
        if (!isSupportedRoot) {
          msg += "doesn't have QUERY or EXPLAIN as root and not a CTAS; ";
        }
        if (!isSupportedType) {
          msg += "is not a query with at least one source table "
                  + " or there is a subquery without a source table, or CTAS, or insert; ";
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

    if (!queryProperties.hasClusterBy() && !queryProperties.hasDistributeBy()
        && !queryProperties.hasSortBy() && !queryProperties.hasPTF() && !queryProperties.usesScript()
        && !queryProperties.hasLateralViews()) {
      // Ok to run CBO.
      return null;
    }

    // Not ok to run CBO, build error message.
    String msg = "";
    if (verbose) {
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
      if (queryProperties.hasLateralViews())
        msg += "has lateral views; ";

      if (msg.isEmpty())
        msg += "has some unspecified limitations; ";
    }
    return msg;
  }

  /* This method inserts the right profiles into profiles CBO depending
   * on the query characteristics. */
  private static EnumSet<ExtendedCBOProfile> obtainCBOProfiles(QueryProperties queryProperties) {
    EnumSet<ExtendedCBOProfile> profilesCBO = EnumSet.noneOf(ExtendedCBOProfile.class);
    // If the query contains more than one join
    if (queryProperties.getJoinCount() > 1) {
      profilesCBO.add(ExtendedCBOProfile.JOIN_REORDERING);
    }
    // If the query contains windowing processing
    if (queryProperties.hasWindowing()) {
      profilesCBO.add(ExtendedCBOProfile.WINDOWING_POSTPROCESSING);
    }
    return profilesCBO;
  }

  @Override
  boolean isCBOExecuted() {
    return runCBO;
  }

  @Override
  boolean continueJoinMerge() {
    return !(runCBO && disableSemJoinReordering);
  }

  @Override
  Table materializeCTE(String cteName, CTEClause cte) throws HiveException {

    ASTNode createTable = new ASTNode(new ClassicToken(HiveParser.TOK_CREATETABLE));

    ASTNode tableName = new ASTNode(new ClassicToken(HiveParser.TOK_TABNAME));
    tableName.addChild(new ASTNode(new ClassicToken(HiveParser.Identifier, cteName)));

    ASTNode temporary = new ASTNode(new ClassicToken(HiveParser.KW_TEMPORARY, MATERIALIZATION_MARKER));

    createTable.addChild(tableName);
    createTable.addChild(temporary);
    createTable.addChild(cte.cteNode);

    CalcitePlanner analyzer = new CalcitePlanner(queryState);
    analyzer.initCtx(ctx);
    analyzer.init(false);

    // should share cte contexts
    analyzer.aliasToCTEs.putAll(aliasToCTEs);

    HiveOperation operation = queryState.getHiveOperation();
    try {
      analyzer.analyzeInternal(createTable);
    } finally {
      queryState.setCommandType(operation);
    }

    Table table = analyzer.tableDesc.toTable(conf);
    Path location = table.getDataLocation();
    try {
      location.getFileSystem(conf).mkdirs(location);
    } catch (IOException e) {
      throw new HiveException(e);
    }
    table.setMaterializedTable(true);

    LOG.info(cteName + " will be materialized into " + location);
    cte.table = table;
    cte.source = analyzer;

    ctx.addMaterializedTable(cteName, table);
    // For CalcitePlanner, store qualified name too
    ctx.addMaterializedTable(table.getDbName() + "." + table.getTableName(), table);

    return table;
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
      NONE, INSERT, MULTI_INSERT, CTAS, VIEW, UNEXPECTED
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
    void setViewToken(ASTNode child) {
      set(PreCboCtx.Type.VIEW, child);
    }

    @Override
    void setInsertToken(ASTNode ast, boolean isTmpFileDest) {
      if (!isTmpFileDest) {
        set(PreCboCtx.Type.INSERT, ast);
      }
    }

    @Override
    void setMultiInsertToken(ASTNode child) {
      set(PreCboCtx.Type.MULTI_INSERT, child);
    }

    @Override
    void resetToken() {
      this.type = Type.NONE;
      this.nodeOfInterest = null;
    }
  }

  ASTNode fixUpAfterCbo(ASTNode originalAst, ASTNode newAst, PreCboCtx cboCtx)
      throws SemanticException {
    switch (cboCtx.type) {

    case NONE:
      // nothing to do
      return newAst;

    case CTAS:
    case VIEW: {
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

    case MULTI_INSERT: {
      // Patch the optimized query back into original FROM clause.
      replaceASTChild(cboCtx.nodeOfInterest, newAst);
      return originalAst;
    }

    default:
      throw new AssertionError("Unexpected type " + cboCtx.type);
    }
  }

  ASTNode reAnalyzeCTASAfterCbo(ASTNode newAst) throws SemanticException {
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

  ASTNode reAnalyzeViewAfterCbo(ASTNode newAst) throws SemanticException {
    // analyzeCreateView uses this.ast, but doPhase1 doesn't, so only reset it
    // here.
    newAst = analyzeCreateView(newAst, getQB(), null);
    if (newAst == null) {
      LOG.error("analyzeCreateTable failed to initialize materialized view after CBO;" + " new ast is "
          + getAST().dump());
      throw new SemanticException("analyzeCreateTable failed to initialize materialized view after CBO");
    }
    return newAst;
  }


  public static class ASTSearcher {
    private final LinkedList<ASTNode> searchQueue = new LinkedList<ASTNode>();

    /**
     * Performs breadth-first search of the AST for a nested set of tokens. Tokens
     * don't have to be each others' direct children, they can be separated by
     * layers of other tokens. For each token in the list, the first one found is
     * matched and there's no backtracking; thus, if AST has multiple instances of
     * some token, of which only one matches, it is not guaranteed to be found. We
     * use this for simple things. Not thread-safe - reuses searchQueue.
     */
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

    public ASTNode depthFirstSearch(ASTNode ast, int token) {
      searchQueue.clear();
      searchQueue.add(ast);
      while (!searchQueue.isEmpty()) {
        ASTNode next = searchQueue.poll();
        if (next.getType() == token) return next;
        for (int j = 0; j < next.getChildCount(); ++j) {
          searchQueue.add((ASTNode) next.getChild(j));
        }
      }
      return null;
    }

    public ASTNode simpleBreadthFirstSearchAny(ASTNode ast, int... tokens) {
      searchQueue.clear();
      searchQueue.add(ast);
      while (!searchQueue.isEmpty()) {
        ASTNode next = searchQueue.poll();
        for (int i = 0; i < tokens.length; ++i) {
          if (next.getType() == tokens[i]) return next;
        }
        for (int i = 0; i < next.getChildCount(); ++i) {
          searchQueue.add((ASTNode) next.getChild(i));
        }
      }
      return null;
    }

    public void reset() {
      searchQueue.clear();
    }
  }

  private static void replaceASTChild(ASTNode child, ASTNode newChild) {
    ASTNode parent = (ASTNode) child.parent;
    int childIndex = child.childIndex;
    parent.deleteChild(childIndex);
    parent.insertChild(childIndex, newChild);
  }

  /**
   * Get optimized logical plan for the given QB tree in the semAnalyzer.
   *
   * @return
   * @throws SemanticException
   */
  RelNode logicalPlan() throws SemanticException {
    RelNode optimizedOptiqPlan = null;

    CalcitePlannerAction calcitePlannerAction = null;
    if (this.columnAccessInfo == null) {
      this.columnAccessInfo = new ColumnAccessInfo();
    }
    calcitePlannerAction = new CalcitePlannerAction(prunedPartitions, this.columnAccessInfo);

    try {
      optimizedOptiqPlan = Frameworks.withPlanner(calcitePlannerAction, Frameworks
          .newConfigBuilder().typeSystem(new HiveTypeSystemImpl()).build());
    } catch (Exception e) {
      rethrowCalciteException(e);
      throw new AssertionError("rethrowCalciteException didn't throw for " + e.getMessage());
    }
    return optimizedOptiqPlan;
  }

  /**
   * Get Optimized AST for the given QB tree in the semAnalyzer.
   *
   * @return Optimized operator tree translated in to Hive AST
   * @throws SemanticException
   */
  ASTNode getOptimizedAST() throws SemanticException {
    RelNode optimizedOptiqPlan = logicalPlan();
    ASTNode optiqOptimizedAST = ASTConverter.convert(optimizedOptiqPlan, resultSchema,
            HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_COLUMN_ALIGNMENT));
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
    CalcitePlannerAction calcitePlannerAction = null;
    if (this.columnAccessInfo == null) {
      this.columnAccessInfo = new ColumnAccessInfo();
    }
    calcitePlannerAction = new CalcitePlannerAction(prunedPartitions, this.columnAccessInfo);

    try {
      optimizedOptiqPlan = Frameworks.withPlanner(calcitePlannerAction, Frameworks
          .newConfigBuilder().typeSystem(new HiveTypeSystemImpl()).build());
    } catch (Exception e) {
      rethrowCalciteException(e);
      throw new AssertionError("rethrowCalciteException didn't throw for " + e.getMessage());
    }

    RelNode modifiedOptimizedOptiqPlan = PlanModifierForReturnPath.convertOpTree(
        optimizedOptiqPlan, resultSchema, this.getQB().getTableDesc() != null);

    LOG.debug("Translating the following plan:\n" + RelOptUtil.toString(modifiedOptimizedOptiqPlan));
    Operator<?> hiveRoot = new HiveOpConverter(this, conf, unparseTranslator, topOps)
                                  .convert(modifiedOptimizedOptiqPlan);
    RowResolver hiveRootRR = genRowResolver(hiveRoot, getQB());
    opParseCtx.put(hiveRoot, new OpParseContext(hiveRootRR));
    String dest = getQB().getParseInfo().getClauseNames().iterator().next();
    if (getQB().getParseInfo().getDestSchemaForClause(dest) != null
        && this.getQB().getTableDesc() == null) {
      Operator<?> selOp = handleInsertStatement(dest, hiveRoot, hiveRootRR, getQB());
      return genFileSinkPlan(dest, getQB(), selOp);
    } else {
      return genFileSinkPlan(dest, getQB(), hiveRoot);
    }
  }

  // This function serves as the wrapper of handleInsertStatementSpec in
  // SemanticAnalyzer
  Operator<?> handleInsertStatement(String dest, Operator<?> input, RowResolver inputRR, QB qb)
      throws SemanticException {
    ArrayList<ExprNodeDesc> colList = new ArrayList<ExprNodeDesc>();
    ArrayList<ColumnInfo> columns = inputRR.getColumnInfos();
    for (int i = 0; i < columns.size(); i++) {
      ColumnInfo col = columns.get(i);
      colList.add(new ExprNodeColumnDesc(col));
    }
    ASTNode selExprList = qb.getParseInfo().getSelForClause(dest);

    RowResolver out_rwsch = handleInsertStatementSpec(colList, dest, inputRR, inputRR, qb,
        selExprList);

    ArrayList<String> columnNames = new ArrayList<String>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    for (int i = 0; i < colList.size(); i++) {
      String outputCol = getColumnInternalName(i);
      colExprMap.put(outputCol, colList.get(i));
      columnNames.add(outputCol);
    }
    Operator<?> output = putOpInsertMap(OperatorFactory.getAndMakeChild(new SelectDesc(colList,
        columnNames), new RowSchema(out_rwsch.getColumnInfos()), input), out_rwsch);
    output.setColumnExprMap(colExprMap);
    return output;
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

  private enum ExtendedCBOProfile {
    JOIN_REORDERING,
    WINDOWING_POSTPROCESSING;
  }

  /**
   * Code responsible for Calcite plan generation and optimization.
   */
  private class CalcitePlannerAction implements Frameworks.PlannerAction<RelNode> {
    private RelOptCluster                                 cluster;
    private RelOptSchema                                  relOptSchema;
    private final Map<String, PrunedPartitionList>        partitionCache;
    private final ColumnAccessInfo columnAccessInfo;
    private Map<HiveProject, Table> viewProjectToTableSchema;

    //correlated vars across subqueries within same query needs to have different ID
    // this will be used in RexNodeConverter to create cor var
    private int subqueryId;

    // this is to keep track if a subquery is correlated and contains aggregate
    // since this is special cased when it is rewritten in SubqueryRemoveRule
    Set<RelNode> corrScalarRexSQWithAgg = new HashSet<RelNode>();

    // TODO: Do we need to keep track of RR, ColNameToPosMap for every op or
    // just last one.
    LinkedHashMap<RelNode, RowResolver>                   relToHiveRR                   = new LinkedHashMap<RelNode, RowResolver>();
    LinkedHashMap<RelNode, ImmutableMap<String, Integer>> relToHiveColNameCalcitePosMap = new LinkedHashMap<RelNode, ImmutableMap<String, Integer>>();

    CalcitePlannerAction(Map<String, PrunedPartitionList> partitionCache, ColumnAccessInfo columnAccessInfo) {
      this.partitionCache = partitionCache;
      this.columnAccessInfo = columnAccessInfo;
    }

    @Override
    public RelNode apply(RelOptCluster cluster, RelOptSchema relOptSchema, SchemaPlus rootSchema) {
      RelNode calciteGenPlan = null;
      RelNode calcitePreCboPlan = null;
      RelNode calciteOptimizedPlan = null;
      subqueryId = 0;

      /*
       * recreate cluster, so that it picks up the additional traitDef
       */
      final Double maxSplitSize = (double) HiveConf.getLongVar(
              conf, HiveConf.ConfVars.MAPREDMAXSPLITSIZE);
      final Double maxMemory = (double) HiveConf.getLongVar(
              conf, HiveConf.ConfVars.HIVECONVERTJOINNOCONDITIONALTASKTHRESHOLD);
      HiveAlgorithmsConf algorithmsConf = new HiveAlgorithmsConf(maxSplitSize, maxMemory);
      HiveRulesRegistry registry = new HiveRulesRegistry();
      HivePlannerContext confContext = new HivePlannerContext(algorithmsConf, registry, corrScalarRexSQWithAgg);
      RelOptPlanner planner = HiveVolcanoPlanner.createPlanner(confContext);
      final RexBuilder rexBuilder = cluster.getRexBuilder();
      final RelOptCluster optCluster = RelOptCluster.create(planner, rexBuilder);

      this.cluster = optCluster;
      this.relOptSchema = relOptSchema;

      PerfLogger perfLogger = SessionState.getPerfLogger();

      // 1. Gen Calcite Plan
      perfLogger.PerfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
      try {
        calciteGenPlan = genLogicalPlan(getQB(), true, null, null);
        // if it is to create view, we do not use table alias
        resultSchema = SemanticAnalyzer.convertRowSchemaToResultSetSchema(
            relToHiveRR.get(calciteGenPlan),
            getQB().isView() ? false : HiveConf.getBoolVar(conf,
                HiveConf.ConfVars.HIVE_RESULTSET_USE_UNIQUE_COLUMN_NAMES));
      } catch (SemanticException e) {
        semanticException = e;
        throw new RuntimeException(e);
      }
      perfLogger.PerfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER, "Calcite: Plan generation");

      // We need to get the ColumnAccessInfo and viewToTableSchema for views.
      HiveRelFieldTrimmer fieldTrimmer = new HiveRelFieldTrimmer(null,
          HiveRelFactories.HIVE_BUILDER.create(optCluster, null), this.columnAccessInfo,
          this.viewProjectToTableSchema);
      fieldTrimmer.trim(calciteGenPlan);

      // Create and set MD provider
      HiveDefaultRelMetadataProvider mdProvider = new HiveDefaultRelMetadataProvider(conf);
      RelMetadataQuery.THREAD_PROVIDERS.set(
              JaninoRelMetadataProvider.of(mdProvider.getMetadataProvider()));

      // Create executor
      Executor executorProvider = new HiveRexExecutorImpl(optCluster);

      //Remove subquery
      LOG.debug("Plan before removing subquery:\n" + RelOptUtil.toString(calciteGenPlan));
      calciteGenPlan = hepPlan(calciteGenPlan, false, mdProvider.getMetadataProvider(), null,
              HiveSubQueryRemoveRule.FILTER, HiveSubQueryRemoveRule.PROJECT);
      LOG.debug("Plan just after removing subquery:\n" + RelOptUtil.toString(calciteGenPlan));

      calciteGenPlan = HiveRelDecorrelator.decorrelateQuery(calciteGenPlan);
      LOG.debug("Plan after decorrelation:\n" + RelOptUtil.toString(calciteGenPlan));

      // 2. Apply pre-join order optimizations
      calcitePreCboPlan = applyPreJoinOrderingTransforms(calciteGenPlan,
              mdProvider.getMetadataProvider(), executorProvider);

      // 3. Apply join order optimizations: reordering MST algorithm
      //    If join optimizations failed because of missing stats, we continue with
      //    the rest of optimizations
      if (profilesCBO.contains(ExtendedCBOProfile.JOIN_REORDERING)) {
        perfLogger.PerfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
        try {
          List<RelMetadataProvider> list = Lists.newArrayList();
          list.add(mdProvider.getMetadataProvider());
          RelTraitSet desiredTraits = optCluster
              .traitSetOf(HiveRelNode.CONVENTION, RelCollations.EMPTY);

          HepProgramBuilder hepPgmBldr = new HepProgramBuilder().addMatchOrder(HepMatchOrder.BOTTOM_UP);
          hepPgmBldr.addRuleInstance(new JoinToMultiJoinRule(HiveJoin.class));
          hepPgmBldr.addRuleInstance(new LoptOptimizeJoinRule(HiveRelFactories.HIVE_BUILDER));

          HepProgram hepPgm = hepPgmBldr.build();
          HepPlanner hepPlanner = new HepPlanner(hepPgm);

          hepPlanner.registerMetadataProviders(list);
          RelMetadataProvider chainedProvider = ChainedRelMetadataProvider.of(list);
          optCluster.setMetadataProvider(new CachingRelMetadataProvider(chainedProvider, hepPlanner));

          RelNode rootRel = calcitePreCboPlan;
          hepPlanner.setRoot(rootRel);
          if (!calcitePreCboPlan.getTraitSet().equals(desiredTraits)) {
            rootRel = hepPlanner.changeTraits(calcitePreCboPlan, desiredTraits);
          }
          hepPlanner.setRoot(rootRel);

          calciteOptimizedPlan = hepPlanner.findBestExp();
        } catch (Exception e) {
          boolean isMissingStats = noColsMissingStats.get() > 0;
          if (isMissingStats) {
            LOG.warn("Missing column stats (see previous messages), skipping join reordering in CBO");
            noColsMissingStats.set(0);
            calciteOptimizedPlan = calcitePreCboPlan;
            disableSemJoinReordering = false;
          } else {
            throw e;
          }
        }
        perfLogger.PerfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER, "Calcite: Join Reordering");
      } else {
        calciteOptimizedPlan = calcitePreCboPlan;
        disableSemJoinReordering = false;
      }

      // 4. Run other optimizations that do not need stats
      perfLogger.PerfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
      calciteOptimizedPlan = hepPlan(calciteOptimizedPlan, false, mdProvider.getMetadataProvider(), null,
              HepMatchOrder.BOTTOM_UP, ProjectRemoveRule.INSTANCE, UnionMergeRule.INSTANCE,
              HiveProjectMergeRule.INSTANCE_NO_FORCE, HiveAggregateProjectMergeRule.INSTANCE,
              HiveJoinCommuteRule.INSTANCE);
      perfLogger.PerfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER, "Calcite: Optimizations without stats");

      // 5. Materialized view based rewriting
      // We disable it for CTAS and MV creation queries (trying to avoid any problem
      // due to data freshness)
      if (conf.getBoolVar(ConfVars.HIVE_MATERIALIZED_VIEW_ENABLE_AUTO_REWRITING) &&
              !getQB().isMaterializedView() && !getQB().isCTAS()) {
        perfLogger.PerfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
        // Use Calcite cost model for view rewriting
        RelMetadataProvider calciteMdProvider = DefaultRelMetadataProvider.INSTANCE;
        RelMetadataQuery.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(calciteMdProvider));
        planner.registerMetadataProviders(Lists.newArrayList(calciteMdProvider));
        // Add views to planner
        List<RelOptMaterialization> materializations = new ArrayList<>();
        try {
          materializations = Hive.get().getRewritingMaterializedViews();
          // We need to use the current cluster for the scan operator on views,
          // otherwise the planner will throw an Exception (different planners)
          materializations = Lists.transform(materializations,
              new Function<RelOptMaterialization, RelOptMaterialization>() {
                @Override
                public RelOptMaterialization apply(RelOptMaterialization materialization) {
                  final RelNode viewScan = materialization.tableRel;
                  final RelNode newViewScan;
                  if (viewScan instanceof DruidQuery) {
                    final DruidQuery dq = (DruidQuery) viewScan;
                    newViewScan = DruidQuery.create(optCluster, optCluster.traitSetOf(HiveRelNode.CONVENTION),
                        viewScan.getTable(), dq.getDruidTable(),
                        ImmutableList.<RelNode>of(dq.getTableScan()));
                  } else {
                    newViewScan = new HiveTableScan(optCluster, optCluster.traitSetOf(HiveRelNode.CONVENTION),
                        (RelOptHiveTable) viewScan.getTable(), viewScan.getTable().getQualifiedName().get(0),
                        null, false, false);
                  }
                  return new RelOptMaterialization(newViewScan, materialization.queryRel, null);
                }
              }
          );
        } catch (HiveException e) {
          LOG.warn("Exception loading materialized views", e);
        }
        if (!materializations.isEmpty()) {
          for (RelOptMaterialization materialization : materializations) {
            planner.addMaterialization(materialization);
          }
          // Add view-based rewriting rules to planner
          planner.addRule(HiveMaterializedViewFilterScanRule.INSTANCE);
          // Optimize plan
          planner.setRoot(calciteOptimizedPlan);
          calciteOptimizedPlan = planner.findBestExp();
          // Remove view-based rewriting rules from planner
          planner.clear();
        }
        // Restore default cost model
        RelMetadataQuery.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(mdProvider.getMetadataProvider()));
        perfLogger.PerfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER, "Calcite: View-based rewriting");
      }

      // 6. Run aggregate-join transpose (cost based)
      //    If it failed because of missing stats, we continue with
      //    the rest of optimizations
      if (conf.getBoolVar(ConfVars.AGGR_JOIN_TRANSPOSE)) {
        perfLogger.PerfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
        try {
          calciteOptimizedPlan = hepPlan(calciteOptimizedPlan, false, mdProvider.getMetadataProvider(), null,
                  HepMatchOrder.BOTTOM_UP, HiveAggregateJoinTransposeRule.INSTANCE);
        } catch (Exception e) {
          boolean isMissingStats = noColsMissingStats.get() > 0;
          if (isMissingStats) {
            LOG.warn("Missing column stats (see previous messages), skipping aggregate-join transpose in CBO");
            noColsMissingStats.set(0);
          } else {
            throw e;
          }
        }
        perfLogger.PerfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER, "Calcite: Aggregate join transpose");
      }

      // 7.convert Join + GBy to semijoin
      // run this rule at later stages, since many calcite rules cant deal with semijoin
      if (conf.getBoolVar(ConfVars.SEMIJOIN_CONVERSION)) {
        perfLogger.PerfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
        calciteOptimizedPlan = hepPlan(calciteOptimizedPlan, false, mdProvider.getMetadataProvider(), null, HiveSemiJoinRule.INSTANCE);
        perfLogger.PerfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER, "Calcite: Semijoin conversion");
      }


      // 8. Run rule to fix windowing issue when it is done over
      // aggregation columns (HIVE-10627)
      if (profilesCBO.contains(ExtendedCBOProfile.WINDOWING_POSTPROCESSING)) {
        perfLogger.PerfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
        calciteOptimizedPlan = hepPlan(calciteOptimizedPlan, false, mdProvider.getMetadataProvider(), null,
                HepMatchOrder.BOTTOM_UP, HiveWindowingFixRule.INSTANCE);
        perfLogger.PerfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER, "Calcite: Window fixing rule");
      }

      // 9. Apply Druid transformation rules
      perfLogger.PerfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
      calciteOptimizedPlan = hepPlan(calciteOptimizedPlan, false, mdProvider.getMetadataProvider(), null,
              HepMatchOrder.BOTTOM_UP, DruidRules.FILTER, DruidRules.PROJECT_AGGREGATE,
              DruidRules.PROJECT, DruidRules.AGGREGATE, DruidRules.PROJECT_SORT,
              DruidRules.SORT, DruidRules.SORT_PROJECT);
      perfLogger.PerfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER, "Calcite: Druid transformation rules");

      // 10. Run rules to aid in translation from Calcite tree to Hive tree
      if (HiveConf.getBoolVar(conf, ConfVars.HIVE_CBO_RETPATH_HIVEOP)) {
        perfLogger.PerfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
        // 10.1. Merge join into multijoin operators (if possible)
        calciteOptimizedPlan = hepPlan(calciteOptimizedPlan, true, mdProvider.getMetadataProvider(), null,
                HepMatchOrder.BOTTOM_UP, HiveJoinProjectTransposeRule.BOTH_PROJECT_INCLUDE_OUTER,
                HiveJoinProjectTransposeRule.LEFT_PROJECT_INCLUDE_OUTER,
                HiveJoinProjectTransposeRule.RIGHT_PROJECT_INCLUDE_OUTER,
                HiveJoinToMultiJoinRule.INSTANCE, HiveProjectMergeRule.INSTANCE);
        // The previous rules can pull up projections through join operators,
        // thus we run the field trimmer again to push them back down
        fieldTrimmer = new HiveRelFieldTrimmer(null,
            HiveRelFactories.HIVE_BUILDER.create(optCluster, null));
        calciteOptimizedPlan = fieldTrimmer.trim(calciteOptimizedPlan);
        calciteOptimizedPlan = hepPlan(calciteOptimizedPlan, false, mdProvider.getMetadataProvider(), null,
                HepMatchOrder.BOTTOM_UP, ProjectRemoveRule.INSTANCE,
                new ProjectMergeRule(false, HiveRelFactories.HIVE_BUILDER));
        calciteOptimizedPlan = hepPlan(calciteOptimizedPlan, true, mdProvider.getMetadataProvider(), null,
                HiveFilterProjectTSTransposeRule.INSTANCE, HiveFilterProjectTSTransposeRule.INSTANCE_DRUID,
                HiveProjectFilterPullUpConstantsRule.INSTANCE);

        // 10.2.  Introduce exchange operators below join/multijoin operators
        calciteOptimizedPlan = hepPlan(calciteOptimizedPlan, false, mdProvider.getMetadataProvider(), null,
                HepMatchOrder.BOTTOM_UP, HiveInsertExchange4JoinRule.EXCHANGE_BELOW_JOIN,
                HiveInsertExchange4JoinRule.EXCHANGE_BELOW_MULTIJOIN);
        perfLogger.PerfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER, "Calcite: Translation from Calcite tree to Hive tree");
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
     * @param executorProvider
     *          executor
     * @return
     */
    private RelNode applyPreJoinOrderingTransforms(RelNode basePlan, RelMetadataProvider mdProvider, Executor executorProvider) {
      // TODO: Decorelation of subquery should be done before attempting
      // Partition Pruning; otherwise Expression evaluation may try to execute
      // corelated sub query.

      PerfLogger perfLogger = SessionState.getPerfLogger();

      final int maxCNFNodeCount = conf.getIntVar(HiveConf.ConfVars.HIVE_CBO_CNF_NODES_LIMIT);
      final int minNumORClauses = conf.getIntVar(HiveConf.ConfVars.HIVEPOINTLOOKUPOPTIMIZERMIN);

      //0. SetOp rewrite
      perfLogger.PerfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
      basePlan = hepPlan(basePlan, true, mdProvider, null, HepMatchOrder.BOTTOM_UP,
          HiveProjectOverIntersectRemoveRule.INSTANCE, HiveIntersectMergeRule.INSTANCE);
      perfLogger.PerfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER,
          "Calcite: HiveProjectOverIntersectRemoveRule and HiveIntersectMerge rules");

      perfLogger.PerfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
      basePlan = hepPlan(basePlan, false, mdProvider, null, HepMatchOrder.BOTTOM_UP,
          HiveIntersectRewriteRule.INSTANCE);
      perfLogger.PerfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER,
          "Calcite: HiveIntersectRewrite rule");

      perfLogger.PerfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
      basePlan = hepPlan(basePlan, false, mdProvider, null, HepMatchOrder.BOTTOM_UP,
          HiveExceptRewriteRule.INSTANCE);
      perfLogger.PerfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER,
          "Calcite: HiveExceptRewrite rule");

      //1. Distinct aggregate rewrite
      // Run this optimization early, since it is expanding the operator pipeline.
      if (!conf.getVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("mr") &&
          conf.getBoolVar(HiveConf.ConfVars.HIVEOPTIMIZEDISTINCTREWRITE)) {
        perfLogger.PerfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
        // Its not clear, if this rewrite is always performant on MR, since extra map phase
        // introduced for 2nd MR job may offset gains of this multi-stage aggregation.
        // We need a cost model for MR to enable this on MR.
        basePlan = hepPlan(basePlan, true, mdProvider, null, HiveExpandDistinctAggregatesRule.INSTANCE);
        perfLogger.PerfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER,
         "Calcite: Prejoin ordering transformation, Distinct aggregate rewrite");
      }

      // 2. Try factoring out common filter elements & separating deterministic
      // vs non-deterministic UDF. This needs to run before PPD so that PPD can
      // add on-clauses for old style Join Syntax
      // Ex: select * from R1 join R2 where ((R1.x=R2.x) and R1.y<10) or
      // ((R1.x=R2.x) and R1.z=10)) and rand(1) < 0.1
      perfLogger.PerfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
      basePlan = hepPlan(basePlan, false, mdProvider, null, HepMatchOrder.ARBITRARY,
          new HivePreFilteringRule(maxCNFNodeCount));
      perfLogger.PerfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER,
        "Calcite: Prejoin ordering transformation, factor out common filter elements and separating deterministic vs non-deterministic UDF");

      // 3. Run exhaustive PPD, add not null filters, transitive inference,
      // constant propagation, constant folding
      List<RelOptRule> rules = Lists.newArrayList();
      if (conf.getBoolVar(HiveConf.ConfVars.HIVEOPTPPD_WINDOWING)) {
        rules.add(HiveFilterProjectTransposeRule.INSTANCE_DETERMINISTIC_WINDOWING);
      } else {
        rules.add(HiveFilterProjectTransposeRule.INSTANCE_DETERMINISTIC);
      }
      rules.add(HiveFilterSetOpTransposeRule.INSTANCE);
      rules.add(HiveFilterSortTransposeRule.INSTANCE);
      rules.add(HiveFilterJoinRule.JOIN);
      rules.add(HiveFilterJoinRule.FILTER_ON_JOIN);
      rules.add(new HiveFilterAggregateTransposeRule(Filter.class, HiveRelFactories.HIVE_FILTER_FACTORY, Aggregate.class));
      rules.add(new FilterMergeRule(HiveRelFactories.HIVE_BUILDER));
      if (conf.getBoolVar(HiveConf.ConfVars.HIVE_OPTIMIZE_REDUCE_WITH_STATS)) {
        rules.add(HiveReduceExpressionsWithStatsRule.INSTANCE);
      }
      rules.add(HiveProjectFilterPullUpConstantsRule.INSTANCE);
      rules.add(HiveReduceExpressionsRule.PROJECT_INSTANCE);
      rules.add(HiveReduceExpressionsRule.FILTER_INSTANCE);
      rules.add(HiveReduceExpressionsRule.JOIN_INSTANCE);
      if (conf.getBoolVar(HiveConf.ConfVars.HIVEPOINTLOOKUPOPTIMIZER)) {
        rules.add(new HivePointLookupOptimizerRule(minNumORClauses));
      }
      rules.add(HiveJoinAddNotNullRule.INSTANCE_JOIN);
      rules.add(HiveJoinAddNotNullRule.INSTANCE_SEMIJOIN);
      rules.add(HiveJoinPushTransitivePredicatesRule.INSTANCE_JOIN);
      rules.add(HiveJoinPushTransitivePredicatesRule.INSTANCE_SEMIJOIN);
      rules.add(HiveSortMergeRule.INSTANCE);
      rules.add(HiveSortLimitPullUpConstantsRule.INSTANCE);
      rules.add(HiveUnionPullUpConstantsRule.INSTANCE);
      rules.add(HiveAggregatePullUpConstantsRule.INSTANCE);
      perfLogger.PerfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
      basePlan = hepPlan(basePlan, true, mdProvider, executorProvider, HepMatchOrder.BOTTOM_UP,
              rules.toArray(new RelOptRule[rules.size()]));
      perfLogger.PerfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER,
        "Calcite: Prejoin ordering transformation, PPD, not null predicates, transitive inference, constant folding");

      // 4. Push down limit through outer join
      // NOTE: We run this after PPD to support old style join syntax.
      // Ex: select * from R1 left outer join R2 where ((R1.x=R2.x) and R1.y<10) or
      // ((R1.x=R2.x) and R1.z=10)) and rand(1) < 0.1 order by R1.x limit 10
      if (conf.getBoolVar(HiveConf.ConfVars.HIVE_OPTIMIZE_LIMIT_TRANSPOSE)) {
        perfLogger.PerfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
        // This should be a cost based decision, but till we enable the extended cost
        // model, we will use the given value for the variable
        final float reductionProportion = HiveConf.getFloatVar(conf,
            HiveConf.ConfVars.HIVE_OPTIMIZE_LIMIT_TRANSPOSE_REDUCTION_PERCENTAGE);
        final long reductionTuples = HiveConf.getLongVar(conf,
            HiveConf.ConfVars.HIVE_OPTIMIZE_LIMIT_TRANSPOSE_REDUCTION_TUPLES);
        basePlan = hepPlan(basePlan, true, mdProvider, null, HiveSortMergeRule.INSTANCE,
            HiveSortProjectTransposeRule.INSTANCE, HiveSortJoinReduceRule.INSTANCE,
            HiveSortUnionReduceRule.INSTANCE);
        basePlan = hepPlan(basePlan, true, mdProvider, null, HepMatchOrder.BOTTOM_UP,
            new HiveSortRemoveRule(reductionProportion, reductionTuples),
            HiveProjectSortTransposeRule.INSTANCE);
        perfLogger.PerfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER,
          "Calcite: Prejoin ordering transformation, Push down limit through outer join");
      }

      // 5. Push Down Semi Joins
      perfLogger.PerfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
      basePlan = hepPlan(basePlan, true, mdProvider, null, SemiJoinJoinTransposeRule.INSTANCE,
          SemiJoinFilterTransposeRule.INSTANCE, SemiJoinProjectTransposeRule.INSTANCE);
      perfLogger.PerfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER,
        "Calcite: Prejoin ordering transformation, Push Down Semi Joins");

      // 6. Apply Partition Pruning
      perfLogger.PerfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
      basePlan = hepPlan(basePlan, false, mdProvider, null, new HivePartitionPruneRule(conf));
      perfLogger.PerfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER,
        "Calcite: Prejoin ordering transformation, Partition Pruning");

      // 7. Projection Pruning (this introduces select above TS & hence needs to be run last due to PP)
      perfLogger.PerfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
      HiveRelFieldTrimmer fieldTrimmer = new HiveRelFieldTrimmer(null,
          HiveRelFactories.HIVE_BUILDER.create(cluster, null),
          profilesCBO.contains(ExtendedCBOProfile.JOIN_REORDERING));
      basePlan = fieldTrimmer.trim(basePlan);
      perfLogger.PerfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER,
        "Calcite: Prejoin ordering transformation, Projection Pruning");

      // 8. Merge, remove and reduce Project if possible
      perfLogger.PerfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
      basePlan = hepPlan(basePlan, false, mdProvider, null,
           HiveProjectMergeRule.INSTANCE, ProjectRemoveRule.INSTANCE);
      perfLogger.PerfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER,
        "Calcite: Prejoin ordering transformation, Merge Project-Project");

      // 9. Rerun PPD through Project as column pruning would have introduced
      // DT above scans; By pushing filter just above TS, Hive can push it into
      // storage (incase there are filters on non partition cols). This only
      // matches FIL-PROJ-TS
      perfLogger.PerfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
      basePlan = hepPlan(basePlan, true, mdProvider, null,
          HiveFilterProjectTSTransposeRule.INSTANCE, HiveFilterProjectTSTransposeRule.INSTANCE_DRUID,
          HiveProjectFilterPullUpConstantsRule.INSTANCE);
      perfLogger.PerfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER,
        "Calcite: Prejoin ordering transformation, Rerun PPD");

      return basePlan;
    }

    /**
     * Run the HEP Planner with the given rule set.
     *
     * @param basePlan
     * @param followPlanChanges
     * @param mdProvider
     * @param executorProvider
     * @param rules
     * @return optimized RelNode
     */
    private RelNode hepPlan(RelNode basePlan, boolean followPlanChanges,
        RelMetadataProvider mdProvider, Executor executorProvider, RelOptRule... rules) {
      return hepPlan(basePlan, followPlanChanges, mdProvider, executorProvider,
              HepMatchOrder.TOP_DOWN, rules);
    }

    /**
     * Run the HEP Planner with the given rule set.
     *
     * @param basePlan
     * @param followPlanChanges
     * @param mdProvider
     * @param executorProvider
     * @param order
     * @param rules
     * @return optimized RelNode
     */
    private RelNode hepPlan(RelNode basePlan, boolean followPlanChanges,
        RelMetadataProvider mdProvider, Executor executorProvider, HepMatchOrder order,
        RelOptRule... rules) {

      RelNode optimizedRelNode = basePlan;
      HepProgramBuilder programBuilder = new HepProgramBuilder();
      if (followPlanChanges) {
        programBuilder.addMatchOrder(order);
        programBuilder = programBuilder.addRuleCollection(ImmutableList.copyOf(rules));
      } else {
        // TODO: Should this be also TOP_DOWN?
        for (RelOptRule r : rules)
          programBuilder.addRuleInstance(r);
      }

      // Create planner and copy context
      HepPlanner planner = new HepPlanner(programBuilder.build(),
              basePlan.getCluster().getPlanner().getContext());

      List<RelMetadataProvider> list = Lists.newArrayList();
      list.add(mdProvider);
      planner.registerMetadataProviders(list);
      RelMetadataProvider chainedProvider = ChainedRelMetadataProvider.of(list);
      basePlan.getCluster().setMetadataProvider(
          new CachingRelMetadataProvider(chainedProvider, planner));

      if (executorProvider != null) {
        basePlan.getCluster().getPlanner().setExecutor(executorProvider);
      }

      planner.setRoot(basePlan);
      optimizedRelNode = planner.findBestExp();

      return optimizedRelNode;
    }

    @SuppressWarnings("nls")
    private RelNode genSetOpLogicalPlan(Opcode opcode, String alias, String leftalias, RelNode leftRel,
        String rightalias, RelNode rightRel) throws SemanticException {
      // 1. Get Row Resolvers, Column map for original left and right input of
      // SetOp Rel
      RowResolver leftRR = this.relToHiveRR.get(leftRel);
      RowResolver rightRR = this.relToHiveRR.get(rightRel);
      HashMap<String, ColumnInfo> leftmap = leftRR.getFieldMap(leftalias);
      HashMap<String, ColumnInfo> rightmap = rightRR.getFieldMap(rightalias);

      // 2. Validate that SetOp is feasible according to Hive (by using type
      // info from RR)
      if (leftmap.size() != rightmap.size()) {
        throw new SemanticException("Schema of both sides of union should match.");
      }

      ASTNode tabref = getQB().getAliases().isEmpty() ? null : getQB().getParseInfo()
          .getSrcForAlias(getQB().getAliases().get(0));

      // 3. construct SetOp Output RR using original left & right Input
      RowResolver setOpOutRR = new RowResolver();

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
              "Schema of both sides of setop should match: Column " + field
                  + " is of type " + lInfo.getType().getTypeName()
                  + " on first table and type " + rInfo.getType().getTypeName()
                  + " on second table"));
        }
        ColumnInfo setOpColInfo = new ColumnInfo(lInfo);
        setOpColInfo.setType(commonTypeInfo);
        setOpOutRR.put(alias, field, setOpColInfo);
      }

      // 4. Determine which columns requires cast on left/right input (Calcite
      // requires exact types on both sides of SetOp)
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
          unionFieldDT = TypeConverter.convert(setOpOutRR.getColumnInfos().get(i).getType(),
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
      RelNode setOpLeftInput = leftRel;
      RelNode setOpRightInput = rightRel;
      if (leftNeedsTypeCast) {
        setOpLeftInput = HiveProject.create(leftRel, leftProjs, leftRel.getRowType()
            .getFieldNames());
      }
      if (rightNeedsTypeCast) {
        setOpRightInput = HiveProject.create(rightRel, rightProjs, rightRel.getRowType()
            .getFieldNames());
      }

      // 6. Construct SetOp Rel
      Builder<RelNode> bldr = new ImmutableList.Builder<RelNode>();
      bldr.add(setOpLeftInput);
      bldr.add(setOpRightInput);
      SetOp setOpRel = null;
      switch (opcode) {
      case UNION:
        setOpRel = new HiveUnion(cluster, TraitsUtil.getDefaultTraitSet(cluster), bldr.build());
        break;
      case INTERSECT:
        setOpRel = new HiveIntersect(cluster, TraitsUtil.getDefaultTraitSet(cluster), bldr.build(),
            false);
        break;
      case INTERSECTALL:
        setOpRel = new HiveIntersect(cluster, TraitsUtil.getDefaultTraitSet(cluster), bldr.build(),
            true);
        break;
      case EXCEPT:
        setOpRel = new HiveExcept(cluster, TraitsUtil.getDefaultTraitSet(cluster), bldr.build(),
            false);
        break;
      case EXCEPTALL:
        setOpRel = new HiveExcept(cluster, TraitsUtil.getDefaultTraitSet(cluster), bldr.build(),
            true);
        break;
      default:
        throw new SemanticException(ErrorMsg.UNSUPPORTED_SET_OPERATOR.getMsg(opcode.toString()));
      }
      relToHiveRR.put(setOpRel, setOpOutRR);
      relToHiveColNameCalcitePosMap.put(setOpRel,
          this.buildHiveToCalciteColumnMap(setOpOutRR, setOpRel));
      return setOpRel;
    }

    private RelNode genJoinRelNode(RelNode leftRel, String leftTableAlias, RelNode rightRel, String rightTableAlias, JoinType hiveJoinType,
        ASTNode joinCond) throws SemanticException {

      RowResolver leftRR = this.relToHiveRR.get(leftRel);
      RowResolver rightRR = this.relToHiveRR.get(rightRel);

      // 1. Construct ExpressionNodeDesc representing Join Condition
      RexNode calciteJoinCond = null;
      List<String> namedColumns = null;
      if (joinCond != null) {
        JoinTypeCheckCtx jCtx = new JoinTypeCheckCtx(leftRR, rightRR, hiveJoinType);
        RowResolver input = RowResolver.getCombinedRR(leftRR, rightRR);
        // named columns join
        // TODO: we can also do the same for semi join but it seems that other
        // DBMS does not support it yet.
        if (joinCond.getType() == HiveParser.TOK_TABCOLNAME
            && !hiveJoinType.equals(JoinType.LEFTSEMI)) {
          namedColumns = new ArrayList<>();
          // We will transform using clause and make it look like an on-clause.
          // So, lets generate a valid on-clause AST from using.
          ASTNode and = (ASTNode) ParseDriver.adaptor.create(HiveParser.KW_AND, "and");
          ASTNode equal = null;
          int count = 0;
          for (Node child : joinCond.getChildren()) {
            String columnName = ((ASTNode) child).getText();
            // dealing with views
            if (unparseTranslator != null && unparseTranslator.isEnabled()) {
              unparseTranslator.addIdentifierTranslation((ASTNode) child);
            }
            namedColumns.add(columnName);
            ASTNode left = ASTBuilder.qualifiedName(leftTableAlias, columnName);
            ASTNode right = ASTBuilder.qualifiedName(rightTableAlias, columnName);
            equal = (ASTNode) ParseDriver.adaptor.create(HiveParser.EQUAL, "=");
            ParseDriver.adaptor.addChild(equal, left);
            ParseDriver.adaptor.addChild(equal, right);
            ParseDriver.adaptor.addChild(and, equal);
            count++;
          }
          joinCond = count > 1 ? and : equal;
        } else if (unparseTranslator != null && unparseTranslator.isEnabled()) {
          genAllExprNodeDesc(joinCond, input, jCtx);
        }
        Map<ASTNode, ExprNodeDesc> exprNodes = JoinCondTypeCheckProcFactory.genExprNode(joinCond,
            jCtx);
        if (jCtx.getError() != null) {
          throw new SemanticException(SemanticAnalyzer.generateErrorMessage(jCtx.getErrorSrcNode(),
              jCtx.getError()));
        }
        ExprNodeDesc joinCondnExprNode = exprNodes.get(joinCond);
        List<RelNode> inputRels = new ArrayList<RelNode>();
        inputRels.add(leftRel);
        inputRels.add(rightRel);
        calciteJoinCond = RexNodeConverter.convert(cluster, joinCondnExprNode, inputRels,
            relToHiveRR, relToHiveColNameCalcitePosMap, false);
      } else {
        calciteJoinCond = cluster.getRexBuilder().makeLiteral(true);
      }

      // 2. Validate that join condition is legal (i.e no function refering to
      // both sides of join, only equi join)
      // TODO: Join filter handling (only supported for OJ by runtime or is it
      // supported for IJ as well)

      // 3. Construct Join Rel Node and RowResolver for the new Join Node
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

      RelNode topRel = null;
      RowResolver topRR = null;
      if (leftSemiJoin) {
        List<RelDataTypeField> sysFieldList = new ArrayList<RelDataTypeField>();
        List<RexNode> leftJoinKeys = new ArrayList<RexNode>();
        List<RexNode> rightJoinKeys = new ArrayList<RexNode>();

        RexNode nonEquiConds = RelOptUtil.splitJoinCondition(sysFieldList, leftRel, rightRel,
            calciteJoinCond, leftJoinKeys, rightJoinKeys, null, null);

        if (!nonEquiConds.isAlwaysTrue()) {
          throw new SemanticException("Non equality condition not supported in Semi-Join"
              + nonEquiConds);
        }

        RelNode[] inputRels = new RelNode[] { leftRel, rightRel };
        final List<Integer> leftKeys = new ArrayList<Integer>();
        final List<Integer> rightKeys = new ArrayList<Integer>();
        calciteJoinCond = HiveCalciteUtil.projectNonColumnEquiConditions(
            HiveRelFactories.HIVE_PROJECT_FACTORY, inputRels, leftJoinKeys, rightJoinKeys, 0,
            leftKeys, rightKeys);
        topRel = HiveSemiJoin.getSemiJoin(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
            inputRels[0], inputRels[1], calciteJoinCond, ImmutableIntList.copyOf(leftKeys),
            ImmutableIntList.copyOf(rightKeys));

        // Create join RR: we need to check whether we need to update left RR in case
        // previous call to projectNonColumnEquiConditions updated it
        if (inputRels[0] != leftRel) {
          RowResolver newLeftRR = new RowResolver();
          if (!RowResolver.add(newLeftRR, leftRR)) {
            LOG.warn("Duplicates detected when adding columns to RR: see previous message");
          }
          for (int i = leftRel.getRowType().getFieldCount();
                  i < inputRels[0].getRowType().getFieldCount(); i++) {
            ColumnInfo oColInfo = new ColumnInfo(
                SemanticAnalyzer.getColumnInternalName(i),
                TypeConverter.convert(inputRels[0].getRowType().getFieldList().get(i).getType()),
                null, false);
            newLeftRR.put(oColInfo.getTabAlias(), oColInfo.getInternalName(), oColInfo);
          }

          RowResolver joinRR = new RowResolver();
          if (!RowResolver.add(joinRR, newLeftRR)) {
            LOG.warn("Duplicates detected when adding columns to RR: see previous message");
          }
          relToHiveColNameCalcitePosMap.put(topRel, this.buildHiveToCalciteColumnMap(joinRR, topRel));
          relToHiveRR.put(topRel, joinRR);

          // Introduce top project operator to remove additional column(s) that have
          // been introduced
          List<RexNode> topFields = new ArrayList<RexNode>();
          List<String> topFieldNames = new ArrayList<String>();
          for (int i = 0; i < leftRel.getRowType().getFieldCount(); i++) {
            final RelDataTypeField field = leftRel.getRowType().getFieldList().get(i);
            topFields.add(leftRel.getCluster().getRexBuilder().makeInputRef(field.getType(), i));
            topFieldNames.add(field.getName());
          }
          topRel = HiveRelFactories.HIVE_PROJECT_FACTORY.createProject(topRel, topFields, topFieldNames);
        }

        topRR = new RowResolver();
        if (!RowResolver.add(topRR, leftRR)) {
          LOG.warn("Duplicates detected when adding columns to RR: see previous message");
        }
      } else {
        topRel = HiveJoin.getJoin(cluster, leftRel, rightRel, calciteJoinCond, calciteJoinType);
        topRR = RowResolver.getCombinedRR(leftRR, rightRR);
        if (namedColumns != null) {
          List<String> tableAliases = new ArrayList<>();
          tableAliases.add(leftTableAlias);
          tableAliases.add(rightTableAlias);
          topRR.setNamedJoinInfo(new NamedJoinInfo(tableAliases, namedColumns, hiveJoinType));
        }
      }

      // 4. Add new rel & its RR to the maps
      relToHiveColNameCalcitePosMap.put(topRel, this.buildHiveToCalciteColumnMap(topRR, topRel));
      relToHiveRR.put(topRel, topRR);
      return topRel;
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
      String leftTableAlias = null;
      if ((left.getToken().getType() == HiveParser.TOK_TABREF)
          || (left.getToken().getType() == HiveParser.TOK_SUBQUERY)
          || (left.getToken().getType() == HiveParser.TOK_PTBLFUNCTION)) {
        String tableName = SemanticAnalyzer.getUnescapedUnqualifiedTableName(
            (ASTNode) left.getChild(0)).toLowerCase();
        leftTableAlias = left.getChildCount() == 1 ? tableName : SemanticAnalyzer
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
      String rightTableAlias = null;
      if ((right.getToken().getType() == HiveParser.TOK_TABREF)
          || (right.getToken().getType() == HiveParser.TOK_SUBQUERY)
          || (right.getToken().getType() == HiveParser.TOK_PTBLFUNCTION)) {
        String tableName = SemanticAnalyzer.getUnescapedUnqualifiedTableName(
            (ASTNode) right.getChild(0)).toLowerCase();
        rightTableAlias = right.getChildCount() == 1 ? tableName : SemanticAnalyzer
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
      return genJoinRelNode(leftRel, leftTableAlias, rightRel, rightTableAlias, hiveJoinType, joinCond);
    }

    private RelNode genTableLogicalPlan(String tableAlias, QB qb) throws SemanticException {
      RowResolver rr = new RowResolver();
      RelNode tableRel = null;

      try {

        // 1. If the table has a Sample specified, bail from Calcite path.
        // 2. if returnpath is on and hivetestmode is on bail
        if (qb.getParseInfo().getTabSample(tableAlias) != null
            || getNameToSplitSampleMap().containsKey(tableAlias)
            || (conf.getBoolVar(HiveConf.ConfVars.HIVE_CBO_RETPATH_HIVEOP)) && (conf.getBoolVar(HiveConf.ConfVars.HIVETESTMODE)) ) {
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

        final TableType tableType = obtainTableType(tabMetaData);

        // 3.3 Add column info corresponding to virtual columns
        List<VirtualColumn> virtualCols = new ArrayList<VirtualColumn>();
        if (tableType == TableType.NATIVE) {
          Iterator<VirtualColumn> vcs = VirtualColumn.getRegistry(conf).iterator();
          while (vcs.hasNext()) {
            VirtualColumn vc = vcs.next();
            colInfo = new ColumnInfo(vc.getName(), vc.getTypeInfo(), tableAlias, true,
                vc.getIsHidden());
            rr.put(tableAlias, vc.getName().toLowerCase(), colInfo);
            cInfoLst.add(colInfo);
            virtualCols.add(vc);
          }
        }

        // 4. Build operator
        if (tableType == TableType.DRUID) {
          // Create case sensitive columns list
          List<String> originalColumnNames =
                  ((StandardStructObjectInspector)rowObjectInspector).getOriginalColumnNames();
          List<ColumnInfo> cIList = new ArrayList<ColumnInfo>(originalColumnNames.size());
          for (int i = 0; i < rr.getColumnInfos().size(); i++) {
            cIList.add(new ColumnInfo(originalColumnNames.get(i), rr.getColumnInfos().get(i).getType(),
                    tableAlias, false));
          }
          // Build row type from field <type, name>
          RelDataType rowType = TypeConverter.getType(cluster, cIList);
          // Build RelOptAbstractTable
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
          // Build Druid query
          String address = HiveConf.getVar(conf,
                  HiveConf.ConfVars.HIVE_DRUID_BROKER_DEFAULT_ADDRESS);
          String dataSource = tabMetaData.getParameters().get(Constants.DRUID_DATA_SOURCE);
          Set<String> metrics = new HashSet<>();
          List<RelDataType> druidColTypes = new ArrayList<>();
          List<String> druidColNames = new ArrayList<>();
          for (RelDataTypeField field : rowType.getFieldList()) {
            druidColTypes.add(field.getType());
            druidColNames.add(field.getName());
            if (field.getName().equals(DruidTable.DEFAULT_TIMESTAMP_COLUMN)) {
              // timestamp
              continue;
            }
            if (field.getType().getSqlTypeName() == SqlTypeName.VARCHAR) {
              // dimension
              continue;
            }
            metrics.add(field.getName());
          }
          List<Interval> intervals = Arrays.asList(DruidTable.DEFAULT_INTERVAL);

          DruidTable druidTable = new DruidTable(new DruidSchema(address, address, false),
                  dataSource, RelDataTypeImpl.proto(rowType), metrics, DruidTable.DEFAULT_TIMESTAMP_COLUMN, intervals);
          final TableScan scan = new HiveTableScan(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
                  optTable, null == tableAlias ? tabMetaData.getTableName() : tableAlias,
                  getAliasId(tableAlias, qb), HiveConf.getBoolVar(conf,
                      HiveConf.ConfVars.HIVE_CBO_RETPATH_HIVEOP), qb.isInsideView()
                      || qb.getAliasInsideView().contains(tableAlias.toLowerCase()));
          tableRel = DruidQuery.create(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
              optTable, druidTable, ImmutableList.<RelNode>of(scan));
        } else {
          // Build row type from field <type, name>
          RelDataType rowType = TypeConverter.getType(cluster, rr, null);
          // Build RelOptAbstractTable
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
          // Build Hive Table Scan Rel
          tableRel = new HiveTableScan(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION), optTable,
              null == tableAlias ? tabMetaData.getTableName() : tableAlias,
              getAliasId(tableAlias, qb), HiveConf.getBoolVar(conf,
                  HiveConf.ConfVars.HIVE_CBO_RETPATH_HIVEOP), qb.isInsideView()
                  || qb.getAliasInsideView().contains(tableAlias.toLowerCase()));
        }

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

    private TableType obtainTableType(Table tabMetaData) {
      if (tabMetaData.getStorageHandler() != null &&
              tabMetaData.getStorageHandler().toString().equals(
                      Constants.DRUID_HIVE_STORAGE_HANDLER_ID)) {
        return TableType.DRUID;
      }
      return TableType.NATIVE;
    }

    private RelNode genFilterRelNode(ASTNode filterExpr, RelNode srcRel,
            ImmutableMap<String, Integer> outerNameToPosMap, RowResolver outerRR,
            boolean useCaching) throws SemanticException {
      ExprNodeDesc filterCondn = genExprNodeDesc(filterExpr, relToHiveRR.get(srcRel),
              outerRR, null, useCaching);
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
          outerNameToPosMap, hiveColNameCalcitePosMap, relToHiveRR.get(srcRel), outerRR,
              0, true, subqueryId).convert(filterCondn);
      RexNode factoredFilterExpr = RexUtil
          .pullFactors(cluster.getRexBuilder(), convertedFilterExpr);
      RelNode filterRel = new HiveFilter(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
          srcRel, factoredFilterExpr);
      this.relToHiveColNameCalcitePosMap.put(filterRel, hiveColNameCalcitePosMap);
      relToHiveRR.put(filterRel, relToHiveRR.get(srcRel));
      relToHiveColNameCalcitePosMap.put(filterRel, hiveColNameCalcitePosMap);

      return filterRel;
    }

    private boolean topLevelConjunctCheck(ASTNode searchCond, ObjectPair<Boolean, Integer> subqInfo) {
      if( searchCond.getType() == HiveParser.KW_OR) {
        subqInfo.setFirst(Boolean.TRUE);
        if(subqInfo.getSecond() > 1) {
          return false;
        }
      }
      if( searchCond.getType() == HiveParser.TOK_SUBQUERY_EXPR) {
        subqInfo.setSecond(subqInfo.getSecond() + 1);
        if(subqInfo.getSecond()> 1 && subqInfo.getFirst()) {
          return false;
        }
        return true;
      }
      for(int i=0; i<searchCond.getChildCount(); i++){
          boolean validSubQuery = topLevelConjunctCheck((ASTNode)searchCond.getChild(i), subqInfo);
          if(!validSubQuery) {
            return false;
          }
      }
      return true;
    }

    private void subqueryRestrictionCheck(QB qb, ASTNode searchCond, RelNode srcRel,
                                         boolean forHavingClause,
                                          Set<ASTNode> corrScalarQueries) throws SemanticException {
        List<ASTNode> subQueriesInOriginalTree = SubQueryUtils.findSubQueries(searchCond);

        ASTNode clonedSearchCond = (ASTNode) SubQueryUtils.adaptor.dupTree(searchCond);
        List<ASTNode> subQueries = SubQueryUtils.findSubQueries(clonedSearchCond);
        for(int i=0; i<subQueriesInOriginalTree.size(); i++){
          //we do not care about the transformation or rewriting of AST
          // which following statement does
          // we only care about the restriction checks they perform.
          // We plan to get rid of these restrictions later
          int sqIdx = qb.incrNumSubQueryPredicates();
          ASTNode originalSubQueryAST = subQueriesInOriginalTree.get(i);

          ASTNode subQueryAST = subQueries.get(i);
          //SubQueryUtils.rewriteParentQueryWhere(clonedSearchCond, subQueryAST);
          Boolean orInSubquery = new Boolean(false);
          Integer subqueryCount = new Integer(0);
          ObjectPair<Boolean, Integer> subqInfo = new ObjectPair<Boolean, Integer>(false, 0);
          if(!topLevelConjunctCheck(clonedSearchCond, subqInfo)){
          /*
           *  Restriction.7.h :: SubQuery predicates can appear only as top level conjuncts.
           */

            throw new CalciteSubquerySemanticException(ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(
                    subQueryAST, "Only SubQuery expressions that are top level conjuncts are allowed"));

          }
          ASTNode outerQueryExpr = (ASTNode) subQueryAST.getChild(2);

          if (outerQueryExpr != null && outerQueryExpr.getType() == HiveParser.TOK_SUBQUERY_EXPR ) {

            throw new CalciteSubquerySemanticException(ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(
                    outerQueryExpr, "IN/NOT IN subqueries are not allowed in LHS"));
          }


          QBSubQuery subQuery = SubQueryUtils.buildSubQuery(qb.getId(), sqIdx, subQueryAST,
                  originalSubQueryAST, ctx);

          RowResolver inputRR = relToHiveRR.get(srcRel);

          String havingInputAlias = null;

          boolean isCorrScalarWithAgg = subQuery.subqueryRestrictionsCheck(inputRR, forHavingClause, havingInputAlias);
          if(isCorrScalarWithAgg) {
            corrScalarQueries.add(originalSubQueryAST);
          }
      }
    }
    private boolean genSubQueryRelNode(QB qb, ASTNode node, RelNode srcRel, boolean forHavingClause,
                                       Map<ASTNode, RelNode> subQueryToRelNode) throws SemanticException {

        Set<ASTNode> corrScalarQueriesWithAgg = new HashSet<ASTNode>();
        //disallow subqueries which HIVE doesn't currently support
        subqueryRestrictionCheck(qb, node, srcRel, forHavingClause, corrScalarQueriesWithAgg);
        Deque<ASTNode> stack = new ArrayDeque<ASTNode>();
        stack.push(node);

        boolean isSubQuery = false;

        while (!stack.isEmpty()) {
          ASTNode next = stack.pop();

          switch(next.getType()) {
            case HiveParser.TOK_SUBQUERY_EXPR:
              /*
               * Restriction 2.h Subquery isnot allowed in LHS
               */
              if(next.getChildren().size() == 3
                      && next.getChild(2).getType() == HiveParser.TOK_SUBQUERY_EXPR){
                throw new CalciteSemanticException(ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(
                         next.getChild(2),
                        "SubQuery in LHS expressions are not supported."));
              }
              String sbQueryAlias = "sq_" + qb.incrNumSubQueryPredicates();
              QB qbSQ = new QB(qb.getId(), sbQueryAlias, true);
              Phase1Ctx ctx1 = initPhase1Ctx();
              doPhase1((ASTNode)next.getChild(1), qbSQ, ctx1, null);
              getMetaData(qbSQ);
              RelNode subQueryRelNode = genLogicalPlan(qbSQ, false,  relToHiveColNameCalcitePosMap.get(srcRel),
                      relToHiveRR.get(srcRel));
              subQueryToRelNode.put(next, subQueryRelNode);
              //keep track of subqueries which are scalar, correlated and contains aggregate
              // subquery expression. This will later be special cased in Subquery remove rule
              if(corrScalarQueriesWithAgg.contains(next)) {
                  corrScalarRexSQWithAgg.add(subQueryRelNode);
              }
              isSubQuery = true;
              break;
            default:
              int childCount = next.getChildCount();
              for(int i = childCount - 1; i >= 0; i--) {
                stack.push((ASTNode) next.getChild(i));
              }
          }
      }
      return isSubQuery;
    }
    private RelNode genFilterRelNode(QB qb, ASTNode searchCond, RelNode srcRel,
        Map<String, RelNode> aliasToRel, ImmutableMap<String, Integer> outerNameToPosMap,
        RowResolver outerRR, boolean forHavingClause) throws SemanticException {

      Map<ASTNode, RelNode> subQueryToRelNode = new HashMap<>();
      boolean isSubQuery = genSubQueryRelNode(qb, searchCond, srcRel, forHavingClause,
                                                subQueryToRelNode);
      if(isSubQuery) {
        ExprNodeDesc subQueryExpr = genExprNodeDesc(searchCond, relToHiveRR.get(srcRel),
                outerRR, subQueryToRelNode, forHavingClause);

        ImmutableMap<String, Integer> hiveColNameCalcitePosMap = this.relToHiveColNameCalcitePosMap
                .get(srcRel);
        RexNode convertedFilterLHS = new RexNodeConverter(cluster, srcRel.getRowType(),
                outerNameToPosMap, hiveColNameCalcitePosMap, relToHiveRR.get(srcRel),
                outerRR, 0, true, subqueryId).convert(subQueryExpr);

        RelNode filterRel = new HiveFilter(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
                srcRel, convertedFilterLHS);

        this.relToHiveColNameCalcitePosMap.put(filterRel, this.relToHiveColNameCalcitePosMap
                .get(srcRel));
        relToHiveRR.put(filterRel, relToHiveRR.get(srcRel));
        this.subqueryId++;
        return filterRel;
      } else {
        return genFilterRelNode(searchCond, srcRel, outerNameToPosMap, outerRR, forHavingClause);
      }
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
              ImmutableMap<String, Integer> outerNameToPosMap, RowResolver outerRR,
                                         boolean forHavingClause) throws SemanticException {
      RelNode filterRel = null;

      Iterator<ASTNode> whereClauseIterator = getQBParseInfo(qb).getDestToWhereExpr().values()
          .iterator();
      if (whereClauseIterator.hasNext()) {
        filterRel = genFilterRelNode(qb, (ASTNode) whereClauseIterator.next().getChild(0), srcRel,
            aliasToRel, outerNameToPosMap, outerRR, forHavingClause);
      }

      return filterRel;
    }

    /**
     * Class to store GenericUDAF related information.
     */
    private class AggInfo {
      private final List<ExprNodeDesc> m_aggParams;
      private final TypeInfo m_returnType;
      private final String m_udfName;
      private final boolean m_distinct;

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
      final SqlAggFunction aggregation = SqlFunctionConverter.getCalciteAggFn(agg.m_udfName, agg.m_distinct,
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
          setTransformedGroupSets.add(convert(val, groupSet.cardinality()));
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

      HiveRelNode aggregateRel = new HiveAggregate(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
            gbInputRel, (transformedGroupSets!=null ? true:false), groupSet,
            transformedGroupSets, aggregateCalls);

      return aggregateRel;
    }

    /* This method returns the flip big-endian representation of value */
    private ImmutableBitSet convert(int value, int length) {
      BitSet bits = new BitSet();
      for (int index = length - 1; index >= 0; index--) {
        if (value % 2 != 0) {
          bits.set(index);
        }
        value = value >>> 1;
      }
      // We flip the bits because Calcite considers that '1'
      // means that the column participates in the GroupBy
      // and '0' does not, as opposed to grouping_id.
      bits.flip(0, length);
      return ImmutableBitSet.FROM_BIT_SET.apply(bits);
    }

    private void addAlternateGByKeyMappings(ASTNode gByExpr, ColumnInfo colInfo,
        RowResolver gByInputRR, RowResolver gByRR) {
      if (gByExpr.getType() == HiveParser.DOT
          && gByExpr.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL) {
        String tab_alias = BaseSemanticAnalyzer.unescapeIdentifier(gByExpr.getChild(0).getChild(0)
            .getText().toLowerCase());
        String col_alias = BaseSemanticAnalyzer.unescapeIdentifier(gByExpr.getChild(1).getText().toLowerCase());
        gByRR.put(tab_alias, col_alias, colInfo);
      } else if (gByExpr.getType() == HiveParser.TOK_TABLE_OR_COL) {
        String col_alias = BaseSemanticAnalyzer.unescapeIdentifier(gByExpr.getChild(0).getText().toLowerCase());
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
      SubQueryUtils.checkForTopLevelSubqueries(selExprList);
      if (selExprList.getToken().getType() == HiveParser.TOK_SELECTDI
          && selExprList.getChildCount() == 1 && selExprList.getChild(0).getChildCount() == 1) {
        ASTNode node = (ASTNode) selExprList.getChild(0).getChild(0);
        if (node.getToken().getType() == HiveParser.TOK_ALLCOLREF) {
          srcRel = genSelectLogicalPlan(qb, srcRel, srcRel, null,null);
          RowResolver rr = this.relToHiveRR.get(srcRel);
          qbp.setSelExprForClause(detsClauseName, SemanticAnalyzer.genSelectDIAST(rr));
        }
      }

      // Select DISTINCT + windowing; GBy handled by genSelectForWindowing
      if (selExprList.getToken().getType() == HiveParser.TOK_SELECTDI &&
              !qb.getAllWindowingSpecs().isEmpty()) {
        return null;
      }

      List<ASTNode> grpByAstExprs = getGroupByForClause(qbp, detsClauseName);
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
            Map<ASTNode, ExprNodeDesc> astToExprNDescMap = genAllExprNodeDesc(grpbyExpr, groupByInputRowResolver);
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
        if (limit == null) {
          String error = StrictChecks.checkNoLimit(conf);
          if (error != null) {
            throw new SemanticException(SemanticAnalyzer.generateErrorMessage(obAST, error));
          }
        }

        // 2. Walk through OB exprs and extract field collations and additional
        // virtual columns needed
        final List<RexNode> newVCLst = new ArrayList<RexNode>();
        final List<RelFieldCollation> fieldCollations = Lists.newArrayList();
        int fieldIndex = 0;

        List<Node> obASTExprLst = obAST.getChildren();
        ASTNode obASTExpr;
        ASTNode nullObASTExpr;
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
          nullObASTExpr = (ASTNode) obASTExpr.getChild(0);
          ASTNode ref = (ASTNode) nullObASTExpr.getChild(0);
          Map<ASTNode, ExprNodeDesc> astToExprNDescMap = genAllExprNodeDesc(ref, inputRR);
          ExprNodeDesc obExprNDesc = astToExprNDescMap.get(ref);
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
            vcASTTypePairs.add(new Pair<ASTNode, TypeInfo>(ref, obExprNDesc.getTypeInfo()));
          }

          // 2.4 Determine the Direction of order by
          RelFieldCollation.Direction order = RelFieldCollation.Direction.DESCENDING;
          if (obASTExpr.getType() == HiveParser.TOK_TABSORTCOLNAMEASC) {
            order = RelFieldCollation.Direction.ASCENDING;
          }
          RelFieldCollation.NullDirection nullOrder;
          if (nullObASTExpr.getType() == HiveParser.TOK_NULLS_FIRST) {
            nullOrder = RelFieldCollation.NullDirection.FIRST;
          } else if (nullObASTExpr.getType() == HiveParser.TOK_NULLS_LAST) {
            nullOrder = RelFieldCollation.NullDirection.LAST;
          } else {
            throw new SemanticException(
                    "Unexpected null ordering option: " + nullObASTExpr.getType());
          }

          // 2.5 Add to field collations
          fieldCollations.add(new RelFieldCollation(fieldIndex, order, nullOrder));
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
        sortRel = new HiveSortLimit(cluster, traitSet, obInputRel, canonizedCollation, null, null);

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
      SimpleEntry<Integer,Integer> entry =
          qbp.getDestToLimit().get(qbp.getClauseNames().iterator().next());
      Integer offset = (entry == null) ? 0 : entry.getKey();
      Integer fetch = (entry == null) ? null : entry.getValue();

      if (fetch != null) {
        RexNode offsetRN = cluster.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(offset));
        RexNode fetchRN = cluster.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(fetch));
        RelTraitSet traitSet = cluster.traitSetOf(HiveRelNode.CONVENTION);
        RelCollation canonizedCollation = traitSet.canonize(RelCollations.EMPTY);
        sortRel = new HiveSortLimit(cluster, traitSet, srcRel, canonizedCollation, offsetRN, fetchRN);

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
          if (oExpr.getOrder() == org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.Order.DESC) {
            flags.add(SqlKind.DESCENDING);
          }
          if (oExpr.getNullOrder() == org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.NullOrder.NULLS_FIRST) {
            flags.add(SqlKind.NULLS_FIRST);
          } else if (oExpr.getNullOrder() == org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.NullOrder.NULLS_LAST) {
            flags.add(SqlKind.NULLS_LAST);
          } else {
            throw new SemanticException(
                    "Unexpected null ordering option: " + oExpr.getNullOrder());
          }
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
            hiveAggInfo.m_udfName, hiveAggInfo.m_distinct, calciteAggFnArgsType, calciteAggFnRetType);

        // 6. Translate Window spec
        RowResolver inputRR = relToHiveRR.get(srcRel);
        WindowSpec wndSpec = ((WindowFunctionSpec) wExpSpec).getWindowSpec();
        List<RexNode> partitionKeys = getPartitionKeys(wndSpec.getPartition(), converter, inputRR);
        List<RexFieldCollation> orderKeys = getOrderKeys(wndSpec.getOrder(), converter, inputRR);
        RexWindowBound upperBound = getBound(wndSpec.getWindowFrame().getStart(), converter);
        RexWindowBound lowerBound = getBound(wndSpec.getWindowFrame().getEnd(), converter);
        boolean isRows = wndSpec.getWindowFrame().getWindowType() == WindowType.ROWS;

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
      final QBParseInfo qbp = getQBParseInfo(qb);
      final String selClauseName = qbp.getClauseNames().iterator().next();
      final boolean cubeRollupGrpSetPresent = (!qbp.getDestRollups().isEmpty()
              || !qbp.getDestGroupingSets().isEmpty() || !qbp.getDestCubes().isEmpty());
      for (WindowExpressionSpec wExprSpec : windowExpressions) {
        if (!qbp.getDestToGroupBy().isEmpty()) {
          // Special handling of grouping function
          wExprSpec.setExpression(rewriteGroupingFunctionAST(
                  getGroupByForClause(qbp, selClauseName), wExprSpec.getExpression(),
                  !cubeRollupGrpSetPresent));
        }
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
    private RelNode genSelectLogicalPlan(QB qb, RelNode srcRel, RelNode starSrcRel,
                                         ImmutableMap<String, Integer> outerNameToPosMap, RowResolver outerRR)
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

      // make sure if there is subquery it is top level expression
      SubQueryUtils.checkForTopLevelSubqueries(selExprList);

      final boolean cubeRollupGrpSetPresent = (!qbp.getDestRollups().isEmpty()
              || !qbp.getDestGroupingSets().isEmpty() || !qbp.getDestCubes().isEmpty());

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
      boolean hintPresent = (selExprList.getChild(0).getType() == HiveParser.QUERY_HINT);
      if (hintPresent) {
        posn++;
      }

      // 4. Bailout if select involves Transform
      boolean isInTransform = (selExprList.getChild(posn).getChild(0).getType() == HiveParser.TOK_TRANSFORM);
      if (isInTransform) {
        String msg = String.format("SELECT TRANSFORM is currently not supported in CBO,"
            + " turn off cbo to use TRANSFORM.");
        LOG.debug(msg);
        throw new CalciteSemanticException(msg, UnsupportedFeature.Select_transform);
      }

      // 5. Check if select involves UDTF
      String udtfTableAlias = null;
      GenericUDTF genericUDTF = null;
      String genericUDTFName = null;
      ArrayList<String> udtfColAliases = new ArrayList<String>();
      ASTNode expr = (ASTNode) selExprList.getChild(posn).getChild(0);
      int exprType = expr.getType();
      if (exprType == HiveParser.TOK_FUNCTION || exprType == HiveParser.TOK_FUNCTIONSTAR) {
        String funcName = TypeCheckProcFactory.DefaultExprProcessor.getFunctionText(expr, true);
        FunctionInfo fi = FunctionRegistry.getFunctionInfo(funcName);
        if (fi != null && fi.getGenericUDTF() != null) {
          LOG.debug("Find UDTF " + funcName);
          genericUDTF = fi.getGenericUDTF();
          genericUDTFName = funcName;
          if (!fi.isNative()) {
            unparseTranslator.addIdentifierTranslation((ASTNode) expr.getChild(0));
          }
          if (genericUDTF != null && (selectStar = exprType == HiveParser.TOK_FUNCTIONSTAR)) {
            genColListRegex(".*", null, (ASTNode) expr.getChild(0),
                col_list, null, inputRR, starRR, pos, out_rwsch, qb.getAliases(), false);
          }
        }
      }

      if (genericUDTF != null) {
        // Only support a single expression when it's a UDTF
        if (selExprList.getChildCount() > 1) {
          throw new SemanticException(generateErrorMessage(
              (ASTNode) selExprList.getChild(1),
              ErrorMsg.UDTF_MULTIPLE_EXPR.getMsg()));
        }

        ASTNode selExpr = (ASTNode) selExprList.getChild(posn);

        // Get the column / table aliases from the expression. Start from 1 as
        // 0 is the TOK_FUNCTION
        // column names also can be inferred from result of UDTF
        for (int i = 1; i < selExpr.getChildCount(); i++) {
          ASTNode selExprChild = (ASTNode) selExpr.getChild(i);
          switch (selExprChild.getType()) {
          case HiveParser.Identifier:
            udtfColAliases.add(unescapeIdentifier(selExprChild.getText().toLowerCase()));
            unparseTranslator.addIdentifierTranslation(selExprChild);
            break;
          case HiveParser.TOK_TABALIAS:
            assert (selExprChild.getChildCount() == 1);
            udtfTableAlias = unescapeIdentifier(selExprChild.getChild(0)
                .getText());
            qb.addAlias(udtfTableAlias);
            unparseTranslator.addIdentifierTranslation((ASTNode) selExprChild
                .getChild(0));
            break;
          default:
            throw new SemanticException("Find invalid token type " + selExprChild.getType()
                + " in UDTF.");
          }
        }
        LOG.debug("UDTF table alias is " + udtfTableAlias);
        LOG.debug("UDTF col aliases are " + udtfColAliases);
      }

      // 6. Iterate over all expression (after SELECT)
      ASTNode exprList;
      if (genericUDTF != null) {
        exprList = expr;
      } else {
        exprList = selExprList;
      }
      // For UDTF's, skip the function name to get the expressions
      int startPosn = genericUDTF != null ? posn + 1 : posn;
      for (int i = startPosn; i < exprList.getChildCount(); ++i) {

        // 6.1 child can be EXPR AS ALIAS, or EXPR.
        ASTNode child = (ASTNode) exprList.getChild(i);
        boolean hasAsClause = (!isInTransform) && (child.getChildCount() == 2);

        // 6.2 EXPR AS (ALIAS,...) parses, but is only allowed for UDTF's
        // This check is not needed and invalid when there is a transform b/c
        // the
        // AST's are slightly different.
        if (genericUDTF == null && child.getChildCount() > 2) {
          throw new SemanticException(SemanticAnalyzer.generateErrorMessage(
              (ASTNode) child.getChild(2), ErrorMsg.INVALID_AS.getMsg()));
        }

        String tabAlias;
        String colAlias;

        if (genericUDTF != null) {
          tabAlias = null;
          colAlias = getAutogenColAliasPrfxLbl() + i;
          expr = child;
        } else {
          // 6.3 Get rid of TOK_SELEXPR
          expr = (ASTNode) child.getChild(0);
          String[] colRef = SemanticAnalyzer.getColAlias(child, getAutogenColAliasPrfxLbl(),
                  inputRR, autogenColAliasPrfxIncludeFuncName(), i);
          tabAlias = colRef[0];
          colAlias = colRef[1];
          if (hasAsClause) {
            unparseTranslator.addIdentifierTranslation((ASTNode) child
                    .getChild(1));
          }
        }

          Map<ASTNode, RelNode> subQueryToRelNode = new HashMap<>();
          boolean isSubQuery = genSubQueryRelNode(qb, expr, srcRel, false,
                  subQueryToRelNode);
          if(isSubQuery) {
            ExprNodeDesc subQueryExpr = genExprNodeDesc(expr, relToHiveRR.get(srcRel),
                    outerRR, subQueryToRelNode, false);
            col_list.add(subQueryExpr);

            ColumnInfo colInfo = new ColumnInfo(SemanticAnalyzer.getColumnInternalName(pos),
                    subQueryExpr.getWritableObjectInspector(), tabAlias, false);
            if (!out_rwsch.putWithCheck(tabAlias, colAlias, null, colInfo)) {
              throw new CalciteSemanticException("Cannot add column to RR: " + tabAlias + "."
                      + colAlias + " => " + colInfo + " due to duplication, see previous warnings",
                      UnsupportedFeature.Duplicates_in_RR);
            }
          } else {

            // 6.4 Build ExprNode corresponding to colums
            if (expr.getType() == HiveParser.TOK_ALLCOLREF) {
              pos = genColListRegex(".*", expr.getChildCount() == 0 ? null : SemanticAnalyzer
                              .getUnescapedName((ASTNode) expr.getChild(0)).toLowerCase(), expr, col_list,
                      excludedColumns, inputRR, starRR, pos, out_rwsch, qb.getAliases(), true);
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
                      qb.getAliases(), true);
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
                      out_rwsch, qb.getAliases(), true);
            } else if (ParseUtils.containsTokenOfType(expr, HiveParser.TOK_FUNCTIONDI)
                    && !(srcRel instanceof HiveAggregate)) {
              // Likely a malformed query eg, select hash(distinct c1) from t1;
              throw new CalciteSemanticException("Distinct without an aggregation.",
                      UnsupportedFeature.Distinct_without_an_aggreggation);
            }
              else {
              // Case when this is an expression
              TypeCheckCtx tcCtx = new TypeCheckCtx(inputRR);
              // We allow stateful functions in the SELECT list (but nowhere else)
              tcCtx.setAllowStatefulFunctions(true);
              if (!qbp.getDestToGroupBy().isEmpty()) {
                // Special handling of grouping function
                expr = rewriteGroupingFunctionAST(getGroupByForClause(qbp, selClauseName), expr,
                        !cubeRollupGrpSetPresent);
              }
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
                  // TODO: this can overwrite the mapping. Should this be allowed?
                  out_rwsch.put(altMapping[0], altMapping[1], colInfo);
                }
              }

              pos = Integer.valueOf(pos.intValue() + 1);
            }
          }
      }
      selectStar = selectStar && exprList.getChildCount() == posn + 1;

      // 7. Convert Hive projections to Calcite
      List<RexNode> calciteColLst = new ArrayList<RexNode>();

      RexNodeConverter rexNodeConv = new RexNodeConverter(cluster, srcRel.getRowType(),
              outerNameToPosMap, buildHiveColNameToInputPosMap(col_list, inputRR), relToHiveRR.get(srcRel),
              outerRR, 0, false, subqueryId);
      for (ExprNodeDesc colExpr : col_list) {
        calciteColLst.add(rexNodeConv.convert(colExpr));
      }

      // 8. Build Calcite Rel
      RelNode outputRel = null;
      if (genericUDTF != null) {
        // The basic idea for CBO support of UDTF is to treat UDTF as a special project.
        // In AST return path, as we just need to generate a SEL_EXPR, we just need to remember the expressions and the alias.
        // In OP return path, we need to generate a SEL and then a UDTF following old semantic analyzer.
        outputRel = genUDTFPlan(genericUDTF, genericUDTFName, udtfTableAlias, udtfColAliases, qb, calciteColLst, out_rwsch, srcRel);
      }
      else{
        outputRel = genSelectRelNode(calciteColLst, out_rwsch, srcRel);
      }

      // 9. Handle select distinct as GBY if there exist windowing functions
      if (selForWindow != null && selExprList.getToken().getType() == HiveParser.TOK_SELECTDI) {
        ImmutableBitSet groupSet = ImmutableBitSet.range(outputRel.getRowType().getFieldList().size());
        outputRel = new HiveAggregate(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
              outputRel, false, groupSet, null, new ArrayList<AggregateCall>());
        RowResolver groupByOutputRowResolver = new RowResolver();
        for (int i = 0; i < out_rwsch.getColumnInfos().size(); i++) {
          ColumnInfo colInfo = out_rwsch.getColumnInfos().get(i);
          ColumnInfo newColInfo = new ColumnInfo(colInfo.getInternalName(),
              colInfo.getType(), colInfo.getTabAlias(), colInfo.getIsVirtualCol());
          groupByOutputRowResolver.put(colInfo.getTabAlias(), colInfo.getAlias(), newColInfo);
        }
        relToHiveColNameCalcitePosMap.put(outputRel,
            buildHiveToCalciteColumnMap(groupByOutputRowResolver, outputRel));
        this.relToHiveRR.put(outputRel, groupByOutputRowResolver);
      }

      return outputRel;
    }

    private RelNode genUDTFPlan(GenericUDTF genericUDTF, String genericUDTFName, String outputTableAlias,
        ArrayList<String> colAliases, QB qb, List<RexNode> selectColLst, RowResolver selectRR, RelNode input) throws SemanticException {

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

      LOG.debug("Table alias: " + outputTableAlias + " Col aliases: " + colAliases);

      // Use the RowResolver from the input operator to generate a input
      // ObjectInspector that can be used to initialize the UDTF. Then, the
      // resulting output object inspector can be used to make the RowResolver
      // for the UDTF operator
      ArrayList<ColumnInfo> inputCols = selectRR.getColumnInfos();

      // Create the object inspector for the input columns and initialize the
      // UDTF
      ArrayList<String> colNames = new ArrayList<String>();
      ObjectInspector[] colOIs = new ObjectInspector[inputCols.size()];
      for (int i = 0; i < inputCols.size(); i++) {
        colNames.add(inputCols.get(i).getInternalName());
        colOIs[i] = inputCols.get(i).getObjectInspector();
      }
      StandardStructObjectInspector rowOI = ObjectInspectorFactory
          .getStandardStructObjectInspector(colNames, Arrays.asList(colOIs));
      StructObjectInspector outputOI = genericUDTF.initialize(rowOI);

      int numUdtfCols = outputOI.getAllStructFieldRefs().size();
      if (colAliases.isEmpty()) {
        // user did not specfied alias names, infer names from outputOI
        for (StructField field : outputOI.getAllStructFieldRefs()) {
          colAliases.add(field.getFieldName());
        }
      }
      // Make sure that the number of column aliases in the AS clause matches
      // the number of columns output by the UDTF
      int numSuppliedAliases = colAliases.size();
      if (numUdtfCols != numSuppliedAliases) {
        throw new SemanticException(ErrorMsg.UDTF_ALIAS_MISMATCH.getMsg("expected " + numUdtfCols
            + " aliases " + "but got " + numSuppliedAliases));
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
        ColumnInfo col = new ColumnInfo(sf.getFieldName(),
            TypeInfoUtils.getTypeInfoFromObjectInspector(sf.getFieldObjectInspector()),
            outputTableAlias, false);
        udtfCols.add(col);
      }

      // Create the row resolver for this operator from the output columns
      RowResolver out_rwsch = new RowResolver();
      for (int i = 0; i < udtfCols.size(); i++) {
        out_rwsch.put(outputTableAlias, colAliases.get(i), udtfCols.get(i));
      }

      // Add the UDTFOperator to the operator DAG
      RelTraitSet traitSet = TraitsUtil.getDefaultTraitSet(cluster);

      // Build row type from field <type, name>
      RelDataType retType = TypeConverter.getType(cluster, out_rwsch, null);

      Builder<RelDataType> argTypeBldr = ImmutableList.<RelDataType> builder();

      RexBuilder rexBuilder = cluster.getRexBuilder();
      RelDataTypeFactory dtFactory = rexBuilder.getTypeFactory();
      RowSchema rs = selectRR.getRowSchema();
      for (ColumnInfo ci : rs.getSignature()) {
        argTypeBldr.add(TypeConverter.convert(ci.getType(), dtFactory));
      }

      SqlOperator calciteOp = SqlFunctionConverter.getCalciteOperator(genericUDTFName, genericUDTF,
             argTypeBldr.build(), retType);

      // Hive UDTF only has a single input
      List<RelNode> list = new ArrayList<>();
      list.add(input);

      RexNode rexNode = cluster.getRexBuilder().makeCall(calciteOp, selectColLst);

      RelNode udtf = HiveTableFunctionScan.create(cluster, traitSet, list, rexNode, null, retType,
          null);
      // Add new rel & its RR to the maps
      relToHiveColNameCalcitePosMap.put(udtf, this.buildHiveToCalciteColumnMap(out_rwsch, udtf));
      relToHiveRR.put(udtf, out_rwsch);

      return udtf;
    }

    private RelNode genLogicalPlan(QBExpr qbexpr) throws SemanticException {
      switch (qbexpr.getOpcode()) {
      case NULLOP:
        return genLogicalPlan(qbexpr.getQB(), false, null, null);
      case UNION:
      case INTERSECT:
      case INTERSECTALL:
      case EXCEPT:
      case EXCEPTALL:
        RelNode qbexpr1Ops = genLogicalPlan(qbexpr.getQBExpr1());
        RelNode qbexpr2Ops = genLogicalPlan(qbexpr.getQBExpr2());
        return genSetOpLogicalPlan(qbexpr.getOpcode(), qbexpr.getAlias(), qbexpr.getQBExpr1()
            .getAlias(), qbexpr1Ops, qbexpr.getQBExpr2().getAlias(), qbexpr2Ops);
      default:
        return null;
      }
    }

    private RelNode genLogicalPlan(QB qb, boolean outerMostQB,
                                   ImmutableMap<String, Integer> outerNameToPosMap,
                                   RowResolver outerRR) throws SemanticException {
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
        RelNode relNode = genLogicalPlan(qbexpr);
        aliasToRel.put(subqAlias, relNode);
        if (qb.getViewToTabSchema().containsKey(subqAlias)) {
          if (relNode instanceof HiveProject) {
            if (this.viewProjectToTableSchema == null) {
              this.viewProjectToTableSchema = new LinkedHashMap<>();
            }
            viewProjectToTableSchema.put((HiveProject) relNode, qb.getViewToTabSchema().get(subqAlias));
          } else {
            throw new SemanticException("View " + subqAlias + " is corresponding to "
                + relNode.toString() + ", rather than a HiveProject.");
          }
        }
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
      filterRel = genFilterLogicalPlan(qb, srcRel, aliasToRel, outerNameToPosMap, outerRR, false);
      srcRel = (filterRel == null) ? srcRel : filterRel;
      RelNode starSrcRel = srcRel;

      // 3. Build Rel for GB Clause
      gbRel = genGBLogicalPlan(qb, srcRel);
      srcRel = (gbRel == null) ? srcRel : gbRel;

      // 4. Build Rel for GB Having Clause
      gbHavingRel = genGBHavingLogicalPlan(qb, srcRel, aliasToRel);
      srcRel = (gbHavingRel == null) ? srcRel : gbHavingRel;

      // 5. Build Rel for Select Clause
      selectRel = genSelectLogicalPlan(qb, srcRel, starSrcRel, outerNameToPosMap, outerRR);
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
      String destClauseName = qbp.getClauseNames().iterator().next();
      ASTNode havingClause = qbp.getHavingForClause(qbp.getClauseNames().iterator().next());

      if (havingClause != null) {
        if (!(srcRel instanceof HiveAggregate)) {
          // ill-formed query like select * from t1 having c1 > 0;
          throw new CalciteSemanticException("Having clause without any group-by.",
              UnsupportedFeature.Having_clause_without_any_groupby);
        }
        ASTNode targetNode = (ASTNode) havingClause.getChild(0);
        validateNoHavingReferenceToAlias(qb, targetNode);
        if (!qbp.getDestToGroupBy().isEmpty()) {
          final boolean cubeRollupGrpSetPresent = (!qbp.getDestRollups().isEmpty()
                  || !qbp.getDestGroupingSets().isEmpty() || !qbp.getDestCubes().isEmpty());
          // Special handling of grouping function
          targetNode = rewriteGroupingFunctionAST(getGroupByForClause(qbp, destClauseName), targetNode,
              !cubeRollupGrpSetPresent);
        }
        gbFilter = genFilterRelNode(qb, targetNode, srcRel, aliasToRel, null, null, true);
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
      return qb.getParseInfo();
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

  private enum TableType {
    DRUID,
    NATIVE
  }

}
