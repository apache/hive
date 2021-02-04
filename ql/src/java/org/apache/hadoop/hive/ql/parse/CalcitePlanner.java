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

import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.util.Map.Entry;
import java.util.Optional;
import java.util.regex.Pattern;
import org.antlr.runtime.ClassicToken;
import org.antlr.runtime.CommonToken;
import org.antlr.runtime.tree.Tree;
import org.antlr.runtime.tree.TreeVisitor;
import org.antlr.runtime.tree.TreeVisitorAction;
import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.adapter.druid.DruidSchema;
import org.apache.calcite.adapter.druid.DruidTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcAggregate;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcFilter;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcJoin;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcProject;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcSort;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcUnion;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactoryImpl;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.CompositeList;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveConf.StrictChecks;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveRelOptMaterialization;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.NotNullConstraint;
import org.apache.hadoop.hive.ql.metadata.PrimaryKeyInfo;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSubquerySemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteViewSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveConfPlannerContext;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveDefaultRelMetadataProvider;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTezModelRelMetadataProvider;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveJoinSwapConstraintsRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveSemiJoinProjectTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializationRelMetadataProvider;
import org.apache.hadoop.hive.ql.optimizer.calcite.HivePlannerContext;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelDistribution;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptMaterializationValidator;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRexExecutorImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException.UnsupportedFeature;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveAlgorithmsConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveVolcanoPlanner;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAntiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveExcept;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveGroupingID;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveIntersect;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveMultiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRexExprList;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortExchange;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableFunctionScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc.HiveJdbcConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc.JdbcHiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveAggregateJoinTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveAggregateProjectMergeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveAggregatePullUpConstantsRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveAggregateReduceFunctionsRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveAggregateReduceRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveAggregateSplitRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveCardinalityPreservingJoinRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveDruidRules;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveExceptRewriteRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveExpandDistinctAggregatesRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveFieldTrimmerRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveFilterAggregateTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveFilterJoinRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveFilterMergeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveFilterProjectTSTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveFilterProjectTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveFilterSetOpTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveFilterSortPredicates;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveFilterSortTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveInBetweenExpandRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveInsertExchange4JoinRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveIntersectMergeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveIntersectRewriteRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveJoinAddNotNullRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveJoinCommuteRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveJoinConstraintsRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveJoinProjectTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveJoinPushTransitivePredicatesRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveJoinToMultiJoinRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveAntiSemiJoinRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HivePartitionPruneRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HivePointLookupOptimizerRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HivePreFilteringRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveProjectFilterPullUpConstantsRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveProjectJoinTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveProjectMergeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveProjectOverIntersectRemoveRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveProjectSortExchangeTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveProjectSortTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveReduceExpressionsRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveReduceExpressionsWithStatsRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveRelDecorrelator;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveRelFieldTrimmer;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveRemoveGBYSemiJoinRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveRemoveSqCountCheck;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveRewriteToDataSketchesRules;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveRulesRegistry;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveSemiJoinRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveSortJoinReduceRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveSortLimitRemoveRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveSortMergeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveSortProjectTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveSortPullUpConstantsRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveSortRemoveRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveSortUnionReduceRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveSubQueryRemoveRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveUnionMergeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveUnionPullUpConstantsRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveWindowingFixRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveWindowingLastValueRewrite;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.jdbc.JDBCAbstractSplitFilterRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.jdbc.JDBCAggregationPushDownRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.jdbc.JDBCExpandExpressionsRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.jdbc.JDBCExtractJoinFilterRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.jdbc.JDBCFilterJoinRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.jdbc.JDBCFilterPushDownRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.jdbc.JDBCJoinPushDownRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.jdbc.JDBCProjectPushDownRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.jdbc.JDBCSortPushDownRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.jdbc.JDBCUnionPushDownRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveAggregateIncrementalRewritingRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewBoxing;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewUtils;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveNoAggregateIncrementalRewritingRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.MaterializedViewRewritingRelVisitor;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ASTBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ASTConverter;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper.AggregateInfo;
import org.apache.hadoop.hive.ql.parse.type.HiveFunctionHelper;
import org.apache.hadoop.hive.ql.parse.type.JoinTypeCheckCtx;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.PlanModifierForReturnPath;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.SqlFunctionConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.opconventer.HiveOpConverter;
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
import org.apache.hadoop.hive.ql.parse.type.RexNodeTypeCheck;
import org.apache.hadoop.hive.ql.parse.type.TypeCheckCtx;
import org.apache.hadoop.hive.ql.parse.type.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.mapper.EmptyStatsSource;
import org.apache.hadoop.hive.ql.plan.mapper.StatsSource;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFArray;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFInline;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.joda.time.Interval;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import static java.util.Collections.singletonList;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewUtils.copyMaterializationToNewCluster;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewUtils.extractTable;


public class CalcitePlanner extends SemanticAnalyzer {

  /**
   * {@link org.antlr.runtime.TokenRewriteStream} offers the opportunity of multiple rewrites of the same
   * input text (in our case the sql query text). These rewrites are called programs and identified by a string.
   * EXPANDED_QUERY_TOKEN_REWRITE_PROGRAM is for identifying the program which replaces all identifiers in the
   * query with fully qualified identifiers.
   */
  private static final String EXPANDED_QUERY_TOKEN_REWRITE_PROGRAM = "EXPANDED_QUERY_PROGRAM";
  private final AtomicInteger noColsMissingStats = new AtomicInteger(0);
  private SemanticException semanticException;
  private boolean runCBO = true;
  private boolean disableSemJoinReordering = true;
  private final CBOFallbackStrategy fallbackStrategy;

  private EnumSet<ExtendedCBOProfile> profilesCBO;

  private static final CommonToken FROM_TOKEN =
      new ImmutableCommonToken(HiveParser.TOK_FROM, "TOK_FROM");
  private static final CommonToken DEST_TOKEN =
      new ImmutableCommonToken(HiveParser.TOK_DESTINATION, "TOK_DESTINATION");
  private static final CommonToken DIR_TOKEN =
      new ImmutableCommonToken(HiveParser.TOK_DIR, "TOK_DIR");
  private static final CommonToken TMPFILE_TOKEN =
      new ImmutableCommonToken(HiveParser.TOK_TMP_FILE, "TOK_TMP_FILE");
  private static final CommonToken SELECT_TOKEN =
      new ImmutableCommonToken(HiveParser.TOK_SELECT, "TOK_SELECT");
  private static final CommonToken SELEXPR_TOKEN =
      new ImmutableCommonToken(HiveParser.TOK_SELEXPR, "TOK_SELEXPR");
  private static final CommonToken TABLEORCOL_TOKEN =
      new ImmutableCommonToken(HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL");
  private static final CommonToken INSERT_TOKEN =
      new ImmutableCommonToken(HiveParser.TOK_INSERT, "TOK_INSERT");
  private static final CommonToken QUERY_TOKEN =
      new ImmutableCommonToken(HiveParser.TOK_QUERY, "TOK_QUERY");
  private static final CommonToken SUBQUERY_TOKEN =
      new ImmutableCommonToken(HiveParser.TOK_SUBQUERY, "TOK_SUBQUERY");

  private static final Pattern PATTERN_VARCHAR =
      Pattern.compile("VARCHAR\\(2147483647\\)");
  private static final Pattern PATTERN_TIMESTAMP =
      Pattern.compile("TIMESTAMP\\(9\\)");

  /**
   * This is the list of operators that are specifically used in Hive.
   */
  private static final List<Class<? extends RelNode>> HIVE_REL_NODE_CLASSES =
      ImmutableList.of(
          RelNode.class,
          AbstractRelNode.class,
          RelSubset.class,
          HepRelVertex.class,
          ConverterImpl.class,
          AbstractConverter.class,

          HiveTableScan.class,
          HiveAggregate.class,
          HiveAntiJoin.class,
          HiveExcept.class,
          HiveFilter.class,
          HiveIntersect.class,
          HiveJoin.class,
          HiveMultiJoin.class,
          HiveProject.class,
          HiveRelNode.class,
          HiveSemiJoin.class,
          HiveSortExchange.class,
          HiveSortLimit.class,
          HiveTableFunctionScan.class,
          HiveUnion.class,

          DruidQuery.class,

          HiveJdbcConverter.class,
          JdbcHiveTableScan.class,
          JdbcAggregate.class,
          JdbcFilter.class,
          JdbcJoin.class,
          JdbcProject.class,
          JdbcSort.class,
          JdbcUnion.class);


  public CalcitePlanner(QueryState queryState) throws SemanticException {
    super(queryState);
    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CBO_ENABLED)) {
      runCBO = false;
      disableSemJoinReordering = false;
    }
    fallbackStrategy = CBOFallbackStrategy.valueOf(conf.getVar(ConfVars.HIVE_CBO_FALLBACK_STRATEGY));
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
      super.analyzeInternal(ast, PreCboCtx::new);
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
    Pair<Boolean, String> pairCanCBOHandleReason = canCBOHandleAst(queryForCbo, getQB(), cboCtx);
    runCBO = pairCanCBOHandleReason.left;
    if (!runCBO) {
      ctx.setCboInfo("Plan not optimized by CBO because the statement " + pairCanCBOHandleReason.right);
      return null;
    }
    profilesCBO = obtainCBOProfiles(queryProperties);
    disableJoinMerge = true;
    final RelNode resPlan = logicalPlan();
    LOG.info("Finished generating logical plan");
    return resPlan;
  }

  public static RelOptPlanner createPlanner(HiveConf conf) {
    return createPlanner(conf, new HashSet<RelNode>(), EmptyStatsSource.INSTANCE);
  }

  private static RelOptPlanner createPlanner(
      HiveConf conf, Set<RelNode> corrScalarRexSQWithAgg,
      StatsSource statsSource) {
    final Double maxSplitSize = (double) HiveConf.getLongVar(
            conf, HiveConf.ConfVars.MAPREDMAXSPLITSIZE);
    final Double maxMemory = (double) HiveConf.getLongVar(
            conf, HiveConf.ConfVars.HIVECONVERTJOINNOCONDITIONALTASKTHRESHOLD);
    HiveAlgorithmsConf algorithmsConf = new HiveAlgorithmsConf(maxSplitSize, maxMemory);
    HiveRulesRegistry registry = new HiveRulesRegistry();
    Properties calciteConfigProperties = new Properties();
    calciteConfigProperties.setProperty(
        CalciteConnectionProperty.TIME_ZONE.camelName(),
        conf.getLocalTimeZone().getId());
    calciteConfigProperties.setProperty(
        CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(),
        Boolean.FALSE.toString());
    CalciteConnectionConfig calciteConfig = new CalciteConnectionConfigImpl(calciteConfigProperties);
    boolean isCorrelatedColumns = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CBO_STATS_CORRELATED_MULTI_KEY_JOINS);
    boolean heuristicMaterializationStrategy = HiveConf.getVar(conf,
        HiveConf.ConfVars.HIVE_MATERIALIZED_VIEW_REWRITING_SELECTION_STRATEGY).equals("heuristic");
    HivePlannerContext confContext = new HivePlannerContext(algorithmsConf, registry, calciteConfig,
        corrScalarRexSQWithAgg,
        new HiveConfPlannerContext(isCorrelatedColumns, heuristicMaterializationStrategy), statsSource);
    return HiveVolcanoPlanner.createPlanner(confContext);
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
      List<ASTNode> oldHints = new ArrayList<>();
      // Cache the hints before CBO runs and removes them.
      // Use the hints later in top level QB.
        getHintsFromQB(getQB(), oldHints);

      // Note: for now, we don't actually pass the queryForCbo to CBO, because
      // it accepts qb, not AST, and can also access all the private stuff in
      // SA. We rely on the fact that CBO ignores the unknown tokens (create
      // table, destination), so if the query is otherwise ok, it is as if we
      // did remove those and gave CBO the proper AST. That is kinda hacky.
      ASTNode queryForCbo = ast;
      if (cboCtx.type == PreCboCtx.Type.CTAS || cboCtx.type == PreCboCtx.Type.VIEW) {
        queryForCbo = cboCtx.nodeOfInterest; // nodeOfInterest is the query
      }
      Pair<Boolean, String> canCBOHandleReason = canCBOHandleAst(queryForCbo, getQB(), cboCtx);
      runCBO = canCBOHandleReason.left;
      if (queryProperties.hasMultiDestQuery()) {
        handleMultiDestQuery(ast, cboCtx);
      }

      if (runCBO) {
        profilesCBO = obtainCBOProfiles(queryProperties);

        disableJoinMerge = true;
        boolean reAnalyzeAST = false;
        final boolean materializedView = getQB().isMaterializedView();

        try {
          // 0. Gen Optimized Plan
          RelNode newPlan = logicalPlan();

          if (this.conf.getBoolVar(HiveConf.ConfVars.HIVE_CBO_RETPATH_HIVEOP)) {
            if (cboCtx.type == PreCboCtx.Type.VIEW && !materializedView) {
              throw new SemanticException("Create view is not supported in cbo return path.");
            }
            sinkOp = getOptimizedHiveOPDag(newPlan);
            if (oldHints.size() > 0) {
              LOG.debug("Propagating hints to QB: " + oldHints);
              getQB().getParseInfo().setHintList(oldHints);
            }
            LOG.info("CBO Succeeded; optimized logical plan.");
            this.ctx.setCboInfo("Plan optimized by CBO.");
            this.ctx.setCboSucceeded(true);
          } else {
            // 1. Convert Plan to AST
            ASTNode newAST = getOptimizedAST(newPlan);

            // 1.1. Fix up the query for insert/ctas/materialized views
            newAST = fixUpAfterCbo(ast, newAST, cboCtx);

            // 1.2. Fix up the query for materialization rebuild
            if (mvRebuildMode == MaterializationRebuildMode.AGGREGATE_REBUILD) {
              fixUpASTAggregateIncrementalRebuild(newAST);
            } else if (mvRebuildMode == MaterializationRebuildMode.NO_AGGREGATE_REBUILD) {
              fixUpASTNoAggregateIncrementalRebuild(newAST);
            }

            // 2. Regen OP plan from optimized AST
            if (forViewCreation) {
              // the reset would remove the translations
              executeUnparseTranlations();
              // save the resultSchema before rewriting it
              originalResultSchema = resultSchema;
            }
            if (cboCtx.type == PreCboCtx.Type.VIEW) {
              try {
                viewSelect = handleCreateViewDDL(newAST);
              } catch (SemanticException e) {
                throw new CalciteViewSemanticException(e.getMessage());
              }
            } else if (cboCtx.type == PreCboCtx.Type.CTAS) {
              // CTAS
              init(false);
              setAST(newAST);
              newAST = reAnalyzeCTASAfterCbo(newAST);
            } else {
              // All others
              init(false);
            }
            if (oldHints.size() > 0) {
              if (getQB().getParseInfo().getHints() != null) {
                LOG.warn("Hints are not null in the optimized tree; "
                    + "after CBO " + getQB().getParseInfo().getHints().dump());
              } else {
                LOG.debug("Propagating hints to QB: " + oldHints);
                getQB().getParseInfo().setHintList(oldHints);
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
            if (this.ctx.isExplainPlan()) {
              // Enrich explain with information derived from CBO
              ExplainConfiguration explainConfig = this.ctx.getExplainConfig();
              if (explainConfig.isCbo()) {
                if (!explainConfig.isCboJoinCost()) {
                  // Include cost as provided by Calcite
                  newPlan.getCluster().invalidateMetadataQuery();
                  RelMetadataQuery.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.DEFAULT);
                }
                if (explainConfig.isFormatted()) {
                  this.ctx.setCalcitePlan(HiveRelOptUtil.toJsonString(newPlan));
                } else if (explainConfig.isCboCost() || explainConfig.isCboJoinCost()) {
                  this.ctx.setCalcitePlan(RelOptUtil.toString(newPlan, SqlExplainLevel.ALL_ATTRIBUTES));
                } else {
                  // Do not include join cost
                  this.ctx.setCalcitePlan(RelOptUtil.toString(newPlan));
                }
              } else if (explainConfig.isFormatted()) {
                this.ctx.setCalcitePlan(HiveRelOptUtil.toJsonString(newPlan));
                this.ctx.setOptimizedSql(getOptimizedSql(newPlan));
              } else if (explainConfig.isExtended()) {
                this.ctx.setOptimizedSql(getOptimizedSql(newPlan));
              }
            }
            if (LOG.isTraceEnabled()) {
              LOG.trace(getOptimizedSql(newPlan));
              LOG.trace(newAST.dump());
            }
          }
        } catch (Exception e) {
          LOG.error("CBO failed, skipping CBO. ", e);

          String cboMsg = "Plan not optimized by CBO.";
          boolean isMissingStats = noColsMissingStats.get() > 0;
          if (isMissingStats) {
            LOG.error("CBO failed due to missing column stats (see previous errors), skipping CBO");
            cboMsg = "Plan not optimized by CBO due to missing statistics. Please check log for more details.";
          } else if (e instanceof CalciteSemanticException) {
            CalciteSemanticException cse = (CalciteSemanticException) e;
            UnsupportedFeature unsupportedFeature = cse.getUnsupportedFeature();
            if (unsupportedFeature != null) {
              cboMsg = "Plan not optimized by CBO due to missing feature [" + unsupportedFeature + "].";
            }
          }
          this.ctx.setCboInfo(cboMsg);

          // Determine if we should re-throw the exception OR if we try to mark plan as reAnayzeAST to retry
          // planning as non-CBO.
          if (fallbackStrategy.isFatal(e)) {
            if (e instanceof RuntimeException || e instanceof SemanticException) {
              // These types of exceptions do not need wrapped
              throw e;
            }
            // Wrap all other errors (Should only hit in tests)
            throw new SemanticException(e);
          } else {
            reAnalyzeAST = true;
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
        String msg;
        if (canCBOHandleReason.right != null) {
          msg = "Plan not optimized by CBO because the statement " + canCBOHandleReason.right;
        } else {
          msg = "Plan not optimized by CBO.";
        }
        this.ctx.setCboInfo(msg);
        skipCalcitePlan = true;
      }
    }

    if (skipCalcitePlan) {
      sinkOp = super.genOPTree(ast, plannerCtx);
    }

    return sinkOp;
  }

  private ASTNode handleCreateViewDDL(ASTNode ast) throws SemanticException {
    saveViewDefinition();
    String originalText = createVwDesc.getViewOriginalText();
    String expandedText = createVwDesc.getViewExpandedText();
    List<FieldSchema> schema = createVwDesc.getSchema();
    List<FieldSchema> partitionColumns = createVwDesc.getPartCols();
    init(false);
    setAST(ast);
    ASTNode newAST = reAnalyzeViewAfterCbo(ast);
    createVwDesc.setViewOriginalText(originalText);
    createVwDesc.setViewExpandedText(expandedText);
    createVwDesc.setSchema(schema);
    createVwDesc.setPartCols(partitionColumns);
    return newAST;
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
    ASTNode from = new ASTNode(FROM_TOKEN);
    from.addChild((ASTNode) ParseDriver.adaptor.dupTree(nodeOfInterest));
    ASTNode destination = new ASTNode(DEST_TOKEN);
    ASTNode dir = new ASTNode(DIR_TOKEN);
    ASTNode tmpFile = new ASTNode(TMPFILE_TOKEN);
    dir.addChild(tmpFile);
    destination.addChild(dir);
    ASTNode select = new ASTNode(SELECT_TOKEN);
    int num = 0;
    for (Collection<Object> selectIdentifier : aliasNodes.asMap().values()) {
      Iterator<Object> it = selectIdentifier.iterator();
      ASTNode node = (ASTNode) it.next();
      // Add select expression
      ASTNode selectExpr = new ASTNode(SELEXPR_TOKEN);
      selectExpr.addChild((ASTNode) ParseDriver.adaptor.dupTree(node)); // Identifier
      String colAlias = "col" + num;
      selectExpr.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, colAlias))); // Alias
      select.addChild(selectExpr);
      // Rewrite all INSERT references (all the node values for this key)
      ASTNode colExpr = new ASTNode(TABLEORCOL_TOKEN);
      colExpr.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, colAlias)));
      replaceASTChild(node, colExpr);
      while (it.hasNext()) {
        // Loop to rewrite rest of INSERT references
        node = (ASTNode) it.next();
        colExpr = new ASTNode(TABLEORCOL_TOKEN);
        colExpr.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, colAlias)));
        replaceASTChild(node, colExpr);
      }
      num++;
    }
    ASTNode insert = new ASTNode(INSERT_TOKEN);
    insert.addChild(destination);
    insert.addChild(select);
    ASTNode newQuery = new ASTNode(QUERY_TOKEN);
    newQuery.addChild(from);
    newQuery.addChild(insert);
    // 3. create subquery
    ASTNode subq = new ASTNode(SUBQUERY_TOKEN);
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
   * @return boolean
   *
   *         Assumption:<br>
   *         If top level QB is query then everything below it must also be
   *         Query.
   */
  Pair<Boolean, String> canCBOHandleAst(ASTNode ast, QB qb, PreCboCtx cboCtx) {
    int root = ast.getToken().getType();
    boolean needToLogMessage = STATIC_LOG.isInfoEnabled();
    boolean isSupportedRoot = root == HiveParser.TOK_QUERY || root == HiveParser.TOK_EXPLAIN
        || qb.isCTAS() || qb.isMaterializedView();
    // Queries without a source table currently are not supported by CBO
    boolean isSupportedType = (qb.getIsQuery())
        || qb.isCTAS() || qb.isMaterializedView() || cboCtx.type == PreCboCtx.Type.INSERT
        || cboCtx.type == PreCboCtx.Type.MULTI_INSERT;
    boolean noBadTokens = HiveCalciteUtil.validateASTForUnsupportedTokens(ast);
    boolean result = isSupportedRoot && isSupportedType && noBadTokens;

    String msg = "";
    if (!result) {
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
      msg = msg.substring(0, msg.length() - 2);
      if (needToLogMessage) {
        STATIC_LOG.info("Not invoking CBO because the statement " + msg);
      }
      return Pair.of(false, msg);
    }

    // Now check QB in more detail. canHandleQbForCbo returns null if query can
    // be handled.
    msg = CalcitePlanner.canHandleQbForCbo(queryProperties, conf, true, needToLogMessage);
    if (msg == null) {
      return Pair.of(true, msg);
    }
    msg = msg.substring(0, msg.length() - 2);
    if (needToLogMessage) {
      STATIC_LOG.info("Not invoking CBO because the statement " + msg);
    }
    return Pair.of(false, msg);
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
  private static String canHandleQbForCbo(QueryProperties queryProperties, HiveConf conf,
      boolean topLevelQB, boolean verbose) {

    if (!queryProperties.hasClusterBy() && !queryProperties.hasDistributeBy()
        && !(queryProperties.hasSortBy() && queryProperties.hasLimit())
        && !queryProperties.hasPTF() && !queryProperties.usesScript()
        && queryProperties.isCBOSupportedLateralViews()) {
      // Ok to run CBO.
      return null;
    }

    // Not ok to run CBO, build error message.
    String msg = "";
    if (verbose) {
      if (queryProperties.hasClusterBy()) {
        msg += "has cluster by; ";
      }
      if (queryProperties.hasDistributeBy()) {
        msg += "has distribute by; ";
      }
      if (queryProperties.hasSortBy() && queryProperties.hasLimit()) {
        msg += "has sort by with limit; ";
      }
      if (queryProperties.hasPTF()) {
        msg += "has PTF; ";
      }
      if (queryProperties.usesScript()) {
        msg += "uses scripts; ";
      }
      if (queryProperties.hasLateralViews()) {
        msg += "has lateral views; ";
      }

      if (msg.isEmpty()) {
        msg += "has some unspecified limitations; ";
      }
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
  boolean isCBOSupportedLateralView(ASTNode lateralView) {
    // Lateral view AST has the following shape:
    // ^(TOK_LATERAL_VIEW
    //   ^(TOK_SELECT ^(TOK_SELEXPR ^(TOK_FUNCTION Identifier params) identifier* tableAlias)))
    if (lateralView.getToken().getType() == HiveParser.TOK_LATERAL_VIEW_OUTER) {
      // LATERAL VIEW OUTER not supported in CBO
      return false;
    }
    // Only INLINE followed by ARRAY supported in CBO
    ASTNode lvFunc = (ASTNode) lateralView.getChild(0).getChild(0).getChild(0);
    String lvFuncName = lvFunc.getChild(0).getText();
    if (lvFuncName.compareToIgnoreCase(
            GenericUDTFInline.class.getAnnotation(Description.class).name()) != 0) {
      return false;
    }
    if (lvFunc.getChildCount() != 2) {
      return false;
    }
    ASTNode innerFunc = (ASTNode) lvFunc.getChild(1);
    if (innerFunc.getToken().getType() != HiveParser.TOK_FUNCTION ||
            innerFunc.getChild(0).getText().compareToIgnoreCase(
                GenericUDFArray.class.getAnnotation(Description.class).name()) != 0) {
      return false;
    }
    return true;
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
    cte.source = analyzer;

    ctx.addMaterializedTable(cteName, table);
    // For CalcitePlanner, store qualified name too
    ctx.addMaterializedTable(table.getFullyQualifiedName(), table);

    return table;
  }

  private void fixUpASTAggregateIncrementalRebuild(ASTNode newAST) throws SemanticException {
    // Replace INSERT OVERWRITE by MERGE equivalent rewriting.
    // Here we need to do this complex AST rewriting that generates the same plan
    // that a MERGE clause would generate because CBO does not support MERGE yet.
    // TODO: Support MERGE as first class member in CBO to simplify this logic.
    // 1) Replace INSERT OVERWRITE by INSERT
    ASTNode updateNode = new ASTSearcher().simpleBreadthFirstSearch(
        newAST, HiveParser.TOK_QUERY, HiveParser.TOK_INSERT);
    ASTNode destinationNode = (ASTNode) updateNode.getChild(0);
    ASTNode newInsertInto = (ASTNode) ParseDriver.adaptor.create(
        HiveParser.TOK_INSERT_INTO, "TOK_INSERT_INTO");
    newInsertInto.addChildren(destinationNode.getChildren());
    ASTNode destinationParentNode = (ASTNode) destinationNode.getParent();
    int childIndex = destinationNode.childIndex;
    destinationParentNode.deleteChild(childIndex);
    destinationParentNode.insertChild(childIndex, newInsertInto);
    // 1.1) Extract name as we will need it afterwards:
    // TOK_DESTINATION TOK_TAB TOK_TABNAME <materialization_name>
    ASTNode materializationNode = new ASTSearcher().simpleBreadthFirstSearch(
        newInsertInto, HiveParser.TOK_INSERT_INTO, HiveParser.TOK_TAB, HiveParser.TOK_TABNAME);
    // 2) Copy INSERT branch and duplicate it, the first branch will be the UPDATE
    // for the MERGE statement while the new branch will be the INSERT for the
    // MERGE statement
    ASTNode updateParent = (ASTNode) updateNode.getParent();
    ASTNode insertNode = (ASTNode) ParseDriver.adaptor.dupTree(updateNode);
    insertNode.setParent(updateParent);
    updateParent.addChild(insertNode);
    // 3) Create ROW_ID column in select clause from left input for the RIGHT OUTER JOIN.
    // This is needed for the UPDATE clause. Hence, we find the following node:
    // TOK_QUERY
    //   TOK_FROM
    //      TOK_RIGHTOUTERJOIN
    //         TOK_SUBQUERY
    //            TOK_QUERY
    //               ...
    //               TOK_INSERT
    //                  ...
    //                  TOK_SELECT
    // And then we create the following child node:
    // TOK_SELEXPR
    //    .
    //       TOK_TABLE_OR_COL
    //          cmv_mat_view
    //       ROW__ID
    ASTNode subqueryNodeInputROJ = new ASTSearcher().simpleBreadthFirstSearch(
        newAST, HiveParser.TOK_QUERY, HiveParser.TOK_FROM, HiveParser.TOK_RIGHTOUTERJOIN,
        HiveParser.TOK_SUBQUERY);
    ASTNode selectNodeInputROJ = new ASTSearcher().simpleBreadthFirstSearch(
        subqueryNodeInputROJ, HiveParser.TOK_SUBQUERY, HiveParser.TOK_QUERY,
        HiveParser.TOK_INSERT, HiveParser.TOK_SELECT);
    ASTNode selectExprNodeInputROJ = (ASTNode) ParseDriver.adaptor.create(
        HiveParser.TOK_SELEXPR, "TOK_SELEXPR");
    ASTNode dotNodeInputROJ = (ASTNode) ParseDriver.adaptor.create(
        HiveParser.DOT, ".");
    ASTNode columnTokNodeInputROJ = (ASTNode) ParseDriver.adaptor.create(
        HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL");
    ASTNode tableNameNodeInputROJ = (ASTNode) ParseDriver.adaptor.create(
        HiveParser.Identifier, Warehouse.getQualifiedName(
            materializationNode.getChild(0).getText(),
            materializationNode.getChild(1).getText()));
    ASTNode rowIdNodeInputROJ = (ASTNode) ParseDriver.adaptor.create(
        HiveParser.Identifier, VirtualColumn.ROWID.getName());
    ParseDriver.adaptor.addChild(selectNodeInputROJ, selectExprNodeInputROJ);
    ParseDriver.adaptor.addChild(selectExprNodeInputROJ, dotNodeInputROJ);
    ParseDriver.adaptor.addChild(dotNodeInputROJ, columnTokNodeInputROJ);
    ParseDriver.adaptor.addChild(dotNodeInputROJ, rowIdNodeInputROJ);
    ParseDriver.adaptor.addChild(columnTokNodeInputROJ, tableNameNodeInputROJ);
    // 4) Transform first INSERT branch into an UPDATE
    // 4.1) Adding ROW__ID field
    ASTNode selectNodeInUpdate = (ASTNode) updateNode.getChild(1);
    if (selectNodeInUpdate.getType() != HiveParser.TOK_SELECT) {
      throw new SemanticException("TOK_SELECT expected in incremental rewriting");
    }
    ASTNode selectExprNodeInUpdate = (ASTNode) ParseDriver.adaptor.dupNode(selectExprNodeInputROJ);
    ASTNode dotNodeInUpdate = (ASTNode) ParseDriver.adaptor.dupNode(dotNodeInputROJ);
    ASTNode columnTokNodeInUpdate = (ASTNode) ParseDriver.adaptor.dupNode(columnTokNodeInputROJ);
    ASTNode tableNameNodeInUpdate = (ASTNode) ParseDriver.adaptor.dupNode(subqueryNodeInputROJ.getChild(1));
    ASTNode rowIdNodeInUpdate = (ASTNode) ParseDriver.adaptor.dupNode(rowIdNodeInputROJ);
    ParseDriver.adaptor.addChild(selectExprNodeInUpdate, dotNodeInUpdate);
    ParseDriver.adaptor.addChild(dotNodeInUpdate, columnTokNodeInUpdate);
    ParseDriver.adaptor.addChild(dotNodeInUpdate, rowIdNodeInUpdate);
    ParseDriver.adaptor.addChild(columnTokNodeInUpdate, tableNameNodeInUpdate);
    selectNodeInUpdate.insertChild(0, ParseDriver.adaptor.dupTree(selectExprNodeInUpdate));
    // 4.2) Modifying filter condition. The incremental rewriting rule generated an OR
    // clause where first disjunct contains the condition for the UPDATE branch.
    // TOK_WHERE
    //   or
    //      and <- DISJUNCT FOR <UPDATE>
    //         =
    //            .
    //               TOK_TABLE_OR_COL
    //                  $hdt$_0
    //               a
    //            .
    //               TOK_TABLE_OR_COL
    //                  $hdt$_1
    //               a
    //         =
    //            .
    //               TOK_TABLE_OR_COL
    //                  $hdt$_0
    //               c
    //            .
    //               TOK_TABLE_OR_COL
    //                  $hdt$_1
    //               c
    //      and <- DISJUNCT FOR <INSERT>
    //         TOK_FUNCTION
    //            isnull
    //            .
    //               TOK_TABLE_OR_COL
    //                  $hdt$_0
    //               a
    //         TOK_FUNCTION
    //            isnull
    //            .
    //               TOK_TABLE_OR_COL
    //                  $hdt$_0
    //               c
    ASTNode whereClauseInUpdate = null;
    for (int i = 0; i < updateNode.getChildren().size(); i++) {
      if (updateNode.getChild(i).getType() == HiveParser.TOK_WHERE) {
        whereClauseInUpdate = (ASTNode) updateNode.getChild(i);
        break;
      }
    }
    if (whereClauseInUpdate == null) {
      throw new SemanticException("TOK_WHERE expected in incremental rewriting");
    }
    if (whereClauseInUpdate.getChild(0).getType() != HiveParser.KW_OR) {
      throw new SemanticException("OR clause expected below TOK_WHERE in incremental rewriting");
    }
    // We bypass the OR clause and select the first disjunct
    int indexUpdate;
    int indexInsert;
    if (whereClauseInUpdate.getChild(0).getChild(0).getType() == HiveParser.EQUAL ||
        (whereClauseInUpdate.getChild(0).getChild(0).getType() == HiveParser.KW_AND &&
            whereClauseInUpdate.getChild(0).getChild(0).getChild(0).getType() == HiveParser.EQUAL)) {
      indexUpdate = 0;
      indexInsert = 1;
    } else if (whereClauseInUpdate.getChild(0).getChild(1).getType() == HiveParser.EQUAL ||
        (whereClauseInUpdate.getChild(0).getChild(1).getType() == HiveParser.KW_AND &&
            whereClauseInUpdate.getChild(0).getChild(1).getChild(0).getType() == HiveParser.EQUAL)) {
      indexUpdate = 1;
      indexInsert = 0;
    } else {
      throw new SemanticException("Unexpected condition in incremental rewriting");
    }
    ASTNode newCondInUpdate = (ASTNode) whereClauseInUpdate.getChild(0).getChild(indexUpdate);
    ParseDriver.adaptor.setChild(whereClauseInUpdate, 0, newCondInUpdate);
    // 4.3) Finally, we add SORT clause, this is needed for the UPDATE.
    //       TOK_SORTBY
    //         TOK_TABSORTCOLNAMEASC
    //            TOK_NULLS_FIRST
    //               .
    //                  TOK_TABLE_OR_COL
    //                     cmv_basetable_2
    //                  ROW__ID
    ASTNode sortExprNode = (ASTNode) ParseDriver.adaptor.create(
        HiveParser.TOK_SORTBY, "TOK_SORTBY");
    ASTNode orderExprNode = (ASTNode) ParseDriver.adaptor.create(
        HiveParser.TOK_TABSORTCOLNAMEASC, "TOK_TABSORTCOLNAMEASC");
    ASTNode nullsOrderExprNode = (ASTNode) ParseDriver.adaptor.create(
        HiveParser.TOK_NULLS_FIRST, "TOK_NULLS_FIRST");
    ASTNode dotNodeInSort = (ASTNode) ParseDriver.adaptor.dupTree(dotNodeInUpdate);
    ParseDriver.adaptor.addChild(updateNode, sortExprNode);
    ParseDriver.adaptor.addChild(sortExprNode, orderExprNode);
    ParseDriver.adaptor.addChild(orderExprNode, nullsOrderExprNode);
    ParseDriver.adaptor.addChild(nullsOrderExprNode, dotNodeInSort);
    // 5) Modify INSERT branch condition. In particular, we need to modify the
    // WHERE clause and pick up the second disjunct from the OR operation.
    ASTNode whereClauseInInsert = null;
    for (int i = 0; i < insertNode.getChildren().size(); i++) {
      if (insertNode.getChild(i).getType() == HiveParser.TOK_WHERE) {
        whereClauseInInsert = (ASTNode) insertNode.getChild(i);
        break;
      }
    }
    if (whereClauseInInsert == null) {
      throw new SemanticException("TOK_WHERE expected in incremental rewriting");
    }
    if (whereClauseInInsert.getChild(0).getType() != HiveParser.KW_OR) {
      throw new SemanticException("OR clause expected below TOK_WHERE in incremental rewriting");
    }
    // We bypass the OR clause and select the second disjunct
    ASTNode newCondInInsert = (ASTNode) whereClauseInInsert.getChild(0).getChild(indexInsert);
    ParseDriver.adaptor.setChild(whereClauseInInsert, 0, newCondInInsert);
    // 6) Now we set some tree properties related to multi-insert
    // operation with INSERT/UPDATE
    ctx.setOperation(Context.Operation.MERGE);
    ctx.addDestNamePrefix(1, Context.DestClausePrefix.UPDATE);
    ctx.addDestNamePrefix(2, Context.DestClausePrefix.INSERT);
  }

  private void fixUpASTNoAggregateIncrementalRebuild(ASTNode newAST) throws SemanticException {
    // Replace INSERT OVERWRITE by INSERT INTO
    // AST tree will have this shape:
    // TOK_QUERY
    //   TOK_FROM
    //      ...
    //   TOK_INSERT
    //      TOK_DESTINATION <- THIS TOKEN IS REPLACED BY 'TOK_INSERT_INTO'
    //         TOK_TAB
    //            TOK_TABNAME
    //               default.cmv_mat_view
    //      TOK_SELECT
    //         ...
    ASTNode dest = new ASTSearcher().simpleBreadthFirstSearch(newAST, HiveParser.TOK_QUERY,
        HiveParser.TOK_INSERT, HiveParser.TOK_DESTINATION);
    ASTNode newChild = (ASTNode) ParseDriver.adaptor.create(
        HiveParser.TOK_INSERT_INTO, "TOK_INSERT_INTO");
    newChild.addChildren(dest.getChildren());
    ASTNode destParent = (ASTNode) dest.getParent();
    int childIndex = dest.childIndex;
    destParent.deleteChild(childIndex);
    destParent.insertChild(childIndex, newChild);
  }

  @Override
  String fixCtasColumnName(String colName) {
    if (runCBO) {
      int lastDot = colName.lastIndexOf('.');
      if (lastDot < 0)
       {
        return colName; // alias is not fully qualified
      }
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
            if (i == tokens.length - 1) {
              return next;
            }
            searchQueue.clear();
          }
          for (int j = 0; j < next.getChildCount(); ++j) {
            searchQueue.add((ASTNode) next.getChild(j));
          }
        }
        if (!found) {
          return null;
        }
      }
      return null;
    }

    public ASTNode depthFirstSearch(ASTNode ast, int token) {
      searchQueue.clear();
      searchQueue.add(ast);
      while (!searchQueue.isEmpty()) {
        ASTNode next = searchQueue.poll();
        if (next.getType() == token) {
          return next;
        }
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
          if (next.getType() == tokens[i]) {
            return next;
          }
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
    calcitePlannerAction = new CalcitePlannerAction(
        prunedPartitions,
        ctx.getStatsSource(),
        this.columnAccessInfo);

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
   * Get SQL rewrite for a Calcite logical plan
   *
   * @return Optimized SQL text (or null, if failed)
   */
  public String getOptimizedSql(RelNode optimizedOptiqPlan) {
    boolean nullsLast = HiveConf.getBoolVar(conf, ConfVars.HIVE_DEFAULT_NULLS_LAST);
    NullCollation nullCollation = nullsLast ? NullCollation.LAST : NullCollation.LOW;
    SqlDialect dialect = new HiveSqlDialect(SqlDialect.EMPTY_CONTEXT
        .withDatabaseProduct(SqlDialect.DatabaseProduct.HIVE)
        .withDatabaseMajorVersion(4) // TODO: should not be hardcoded
        .withDatabaseMinorVersion(0)
        .withIdentifierQuoteString("`")
        .withDataTypeSystem(new HiveTypeSystemImpl())
        .withNullCollation(nullCollation)) {
      @Override
      protected boolean allowsAs() {
        return true;
      }

      @Override
      public boolean supportsCharSet() {
        return false;
      }
    };
    try {
      final JdbcImplementor jdbcImplementor =
          new JdbcImplementor(dialect, (JavaTypeFactory) optimizedOptiqPlan.getCluster()
              .getTypeFactory());
      final JdbcImplementor.Result result = jdbcImplementor.visitChild(0, optimizedOptiqPlan);
      String sql = result.asStatement().toSqlString(dialect).getSql();
      sql = PATTERN_VARCHAR.matcher(sql).replaceAll("STRING"); // VARCHAR(INTEGER.MAX) -> STRING
      sql = PATTERN_TIMESTAMP.matcher(sql).replaceAll("TIMESTAMP"); // TIMESTAMP(9) -> TIMESTAMP
      return sql;
    } catch (Exception ex) {
      LOG.warn("Rel2SQL Rewrite threw error", ex);
    }
    return null;
  }

  /**
   * Get Optimized AST for the given QB tree in the semAnalyzer.
   *
   * @return Optimized operator tree translated in to Hive AST
   * @throws SemanticException
   */
  ASTNode getOptimizedAST() throws SemanticException {
    return getOptimizedAST(logicalPlan());
  }

  /**
   * Get Optimized AST for the given QB tree in the semAnalyzer.
   *
   * @return Optimized operator tree translated in to Hive AST
   * @throws SemanticException
   */
  ASTNode getOptimizedAST(RelNode optimizedOptiqPlan) throws SemanticException {
    ASTNode optiqOptimizedAST = ASTConverter.convert(optimizedOptiqPlan, resultSchema,
            HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_COLUMN_ALIGNMENT),ctx.getPlanMapper());
    return optiqOptimizedAST;
  }

  /**
   * Get Optimized Hive Operator DAG for the given QB tree in the semAnalyzer.
   *
   * @return Optimized Hive operator tree
   * @throws SemanticException
   */
  Operator getOptimizedHiveOPDag(RelNode optimizedOptiqPlan) throws SemanticException {
    RelNode modifiedOptimizedOptiqPlan = PlanModifierForReturnPath.convertOpTree(
        optimizedOptiqPlan, resultSchema, this.getQB().getTableDesc() != null);

    LOG.debug("Translating the following plan:\n" + RelOptUtil.toString(modifiedOptimizedOptiqPlan));
    Operator<?> hiveRoot = new HiveOpConverter(this, conf, unparseTranslator, topOps)
                                  .convert(modifiedOptimizedOptiqPlan);
    RowResolver hiveRootRR = genRowResolver(hiveRoot, getQB());
    opParseCtx.put(hiveRoot, new OpParseContext(hiveRootRR));
    String dest = getQB().getParseInfo().getClauseNames().iterator().next();
    if (isInsertInto(getQB().getParseInfo(), dest)) {
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
    List<ExprNodeDesc> colList = new ArrayList<ExprNodeDesc>();
    List<ColumnInfo> columns = inputRR.getColumnInfos();
    for (int i = 0; i < columns.size(); i++) {
      ColumnInfo col = columns.get(i);
      colList.add(new ExprNodeColumnDesc(col));
    }
    ASTNode selExprList = qb.getParseInfo().getSelForClause(dest);

    RowResolver rowResolver = createRowResolver(columns);
    rowResolver = handleInsertStatementSpec(colList, dest, rowResolver, qb, selExprList);

    List<String> columnNames = new ArrayList<String>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    for (int i = 0; i < colList.size(); i++) {
      String outputCol = getColumnInternalName(i);
      colExprMap.put(outputCol, colList.get(i));
      columnNames.add(outputCol);
    }
    Operator<?> output = putOpInsertMap(OperatorFactory.getAndMakeChild(new SelectDesc(colList,
        columnNames), new RowSchema(rowResolver.getColumnInfos()), input), rowResolver);
    output.setColumnExprMap(colExprMap);
    return output;
  }

  private RowResolver createRowResolver(List<ColumnInfo> columnInfos) {
    RowResolver rowResolver = new RowResolver();
    int pos = 0;
    for (ColumnInfo columnInfo : columnInfos) {
      ColumnInfo newColumnInfo = new ColumnInfo(columnInfo);
      newColumnInfo.setInternalName(HiveConf.getColumnInternalName(pos++));
      rowResolver.put(newColumnInfo.getTabAlias(), newColumnInfo.getAlias(), newColumnInfo);
    }

    return rowResolver;
  }

  /***
   * Unwraps Calcite Invocation exceptions coming meta data provider chain and
   * obtains the real cause.
   *
   * @param e
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
        if (MESSAGE_FIELD == null) {
          return false;
        }
        Field field = (target instanceof InvocationTargetException) ? TARGET_FIELD : CAUSE_FIELD;
        if (field == null) {
          return false;
        }

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
    return t instanceof RuntimeException || t instanceof InvocationTargetException;
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
    WINDOWING_POSTPROCESSING,
    REFERENTIAL_CONSTRAINTS;
  }

  /**
   * Code responsible for Calcite plan generation and optimization.
   */
  private class CalcitePlannerAction implements Frameworks.PlannerAction<RelNode> {
    private RelOptCluster                                 cluster;
    private RelOptSchema                                  relOptSchema;
    private FunctionHelper                                functionHelper;
    private final Map<String, PrunedPartitionList>        partitionCache;
    private final Map<String, ColumnStatsList>            colStatsCache;
    private final ColumnAccessInfo columnAccessInfo;
    private Map<HiveProject, Table> viewProjectToTableSchema;

    // correlated vars across subqueries within same query needs to have different ID
    private int subqueryId;

    // this is to keep track if a subquery is correlated and contains aggregate
    // since this is special cased when it is rewritten in SubqueryRemoveRule
    Set<RelNode> corrScalarRexSQWithAgg = new HashSet<RelNode>();

    // TODO: Do we need to keep track of RR, ColNameToPosMap for every op or
    // just last one.
    LinkedHashMap<RelNode, RowResolver>                   relToHiveRR                   = new LinkedHashMap<RelNode, RowResolver>();
    LinkedHashMap<RelNode, ImmutableMap<String, Integer>> relToHiveColNameCalcitePosMap = new LinkedHashMap<RelNode, ImmutableMap<String, Integer>>();
    private final StatsSource statsSource;

    CalcitePlannerAction(
        Map<String, PrunedPartitionList> partitionCache,
        StatsSource statsSource,
        ColumnAccessInfo columnAccessInfo) {
      this.partitionCache = partitionCache;
      this.statsSource = statsSource;
      this.colStatsCache = ctx.getOpContext().getColStatsCache();
      this.columnAccessInfo = columnAccessInfo;
    }

    @Override
    public RelNode apply(RelOptCluster cluster, RelOptSchema relOptSchema, SchemaPlus rootSchema) {
      RelNode calciteGenPlan = null;
      RelNode calcitePreCboPlan = null;
      RelNode calciteOptimizedPlan = null;
      subqueryId = -1;

      /*
       * recreate cluster, so that it picks up the additional traitDef
       */
      RelOptPlanner planner = createPlanner(conf, corrScalarRexSQWithAgg, statsSource);
      final RexBuilder rexBuilder = cluster.getRexBuilder();
      final RelOptCluster optCluster = RelOptCluster.create(planner, rexBuilder);

      this.cluster = optCluster;
      this.relOptSchema = relOptSchema;
      this.functionHelper = new HiveFunctionHelper(rexBuilder);

      PerfLogger perfLogger = SessionState.getPerfLogger();
      // 1. Gen Calcite Plan
      perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
      try {
        calciteGenPlan = genLogicalPlan(getQB(), true, null, null);
        // if it is to create view, we do not use table alias
        resultSchema = convertRowSchemaToResultSetSchema(relToHiveRR.get(calciteGenPlan),
            (forViewCreation || getQB().isMaterializedView()) ? false : HiveConf.getBoolVar(conf,
                HiveConf.ConfVars.HIVE_RESULTSET_USE_UNIQUE_COLUMN_NAMES));
      } catch (SemanticException e) {
        semanticException = e;
        throw new RuntimeException(e);
      }
      perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER, "Calcite: Plan generation");

      if (isMaterializedViewRewritingByTextEnabled()) {
        RelNode rewrittenPlan = applyMaterializedViewRewritingByText(calciteGenPlan, optCluster);
        if (rewrittenPlan != null) {
          return rewrittenPlan;
        }
      }

      // Create executor
      RexExecutor executorProvider = new HiveRexExecutorImpl();
      calciteGenPlan.getCluster().getPlanner().setExecutor(executorProvider);

      // We need to get the ColumnAccessInfo and viewToTableSchema for views.
      HiveRelFieldTrimmer.get()
          .trim(HiveRelFactories.HIVE_BUILDER.create(optCluster, null),
              calciteGenPlan, this.columnAccessInfo, this.viewProjectToTableSchema);

      // Create and set MD provider
      HiveDefaultRelMetadataProvider mdProvider = new HiveDefaultRelMetadataProvider(conf, HIVE_REL_NODE_CLASSES);
      RelMetadataQuery.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(mdProvider.getMetadataProvider()));

      //Remove subquery
      if (LOG.isDebugEnabled()) {
        LOG.debug("Plan before removing subquery:\n" + RelOptUtil.toString(calciteGenPlan));
      }
      calciteGenPlan = hepPlan(calciteGenPlan, false, mdProvider.getMetadataProvider(), null,
          HepMatchOrder.DEPTH_FIRST, new HiveSubQueryRemoveRule(conf));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Plan just after removing subquery:\n" + RelOptUtil.toString(calciteGenPlan));
      }
      calciteGenPlan = HiveRelDecorrelator.decorrelateQuery(calciteGenPlan);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Plan after decorrelation:\n" + RelOptUtil.toString(calciteGenPlan));
      }

      // Validate query materialization for query results caching. This check needs
      // to occur before constant folding, which may remove some function calls
      // from the query plan.
      // In addition, if it is a materialized view creation and we are enabling it
      // for rewriting, it should pass all checks done for query results caching
      // and on top of that we should check that it only contains operators that
      // are supported by the rewriting algorithm.
      HiveRelOptMaterializationValidator materializationValidator = new HiveRelOptMaterializationValidator();
      materializationValidator.validate(calciteGenPlan);
      setInvalidResultCacheReason(
          materializationValidator.getResultCacheInvalidReason());
      setInvalidAutomaticRewritingMaterializationReason(
          materializationValidator.getAutomaticRewritingInvalidReason());

      // 2. Apply pre-join order optimizations
      calcitePreCboPlan = applyPreJoinOrderingTransforms(calciteGenPlan,
          mdProvider.getMetadataProvider(), executorProvider);

      // 3. Materialized view based rewriting
      // We disable it for CTAS and MV creation queries (trying to avoid any problem
      // due to data freshness)
      if (conf.getBoolVar(ConfVars.HIVE_MATERIALIZED_VIEW_ENABLE_AUTO_REWRITING) &&
              !getQB().isMaterializedView() && !ctx.isLoadingMaterializedView() && !getQB().isCTAS() &&
               getQB().hasTableDefined() &&
              !forViewCreation) {
        calcitePreCboPlan = applyMaterializedViewRewriting(planner,
            calcitePreCboPlan, mdProvider.getMetadataProvider(), executorProvider);
      }

      // 4. Apply join order optimizations: reordering MST algorithm
      //    If join optimizations failed because of missing stats, we continue with
      //    the rest of optimizations
      if (profilesCBO.contains(ExtendedCBOProfile.JOIN_REORDERING)) {
        calciteOptimizedPlan = applyJoinOrderingTransform(calcitePreCboPlan,
            mdProvider.getMetadataProvider(), executorProvider);
      } else {
        calciteOptimizedPlan = calcitePreCboPlan;
        disableSemJoinReordering = false;
      }

      // 5. Apply post-join order optimizations
      calciteOptimizedPlan = applyPostJoinOrderingTransform(calciteOptimizedPlan,
          mdProvider.getMetadataProvider(), executorProvider);

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
    private RelNode applyPreJoinOrderingTransforms(RelNode basePlan, RelMetadataProvider mdProvider, RexExecutor executorProvider) {
      // TODO: Decorelation of subquery should be done before attempting
      // Partition Pruning; otherwise Expression evaluation may try to execute
      // corelated sub query.

      PerfLogger perfLogger = SessionState.getPerfLogger();

      final int maxCNFNodeCount = conf.getIntVar(HiveConf.ConfVars.HIVE_CBO_CNF_NODES_LIMIT);
      final int minNumORClauses = conf.getIntVar(HiveConf.ConfVars.HIVEPOINTLOOKUPOPTIMIZERMIN);

      final HepProgramBuilder program = new HepProgramBuilder();

      //0. SetOp rewrite
      generatePartialProgram(program, true, HepMatchOrder.BOTTOM_UP,
          HiveProjectOverIntersectRemoveRule.INSTANCE, HiveIntersectMergeRule.INSTANCE,
          HiveUnionMergeRule.INSTANCE);
      generatePartialProgram(program, false, HepMatchOrder.DEPTH_FIRST,
          HiveIntersectRewriteRule.INSTANCE);
      generatePartialProgram(program, false, HepMatchOrder.DEPTH_FIRST,
          HiveExceptRewriteRule.INSTANCE);

      //1. Distinct aggregate rewrite
      if (!isMaterializedViewMaintenance() && conf.getBoolVar(ConfVars.HIVE_OPTIMIZE_BI_ENABLED)) {
        // Rewrite to datasketches if enabled
        if (conf.getBoolVar(ConfVars.HIVE_OPTIMIZE_BI_REWRITE_COUNTDISTINCT_ENABLED)) {
          String sketchType = conf.getVar(ConfVars.HIVE_OPTIMIZE_BI_REWRITE_COUNT_DISTINCT_SKETCH);
          RelOptRule rule = new HiveRewriteToDataSketchesRules.CountDistinctRewrite(sketchType);
          generatePartialProgram(program, true, HepMatchOrder.TOP_DOWN, rule);
        }
        if (conf.getBoolVar(ConfVars.HIVE_OPTIMIZE_BI_REWRITE_PERCENTILE_DISC_ENABLED)) {
          String sketchType = conf.getVar(ConfVars.HIVE_OPTIMIZE_BI_REWRITE_PERCENTILE_DISC_SKETCH);
          RelOptRule rule = new HiveRewriteToDataSketchesRules.PercentileDiscRewrite(sketchType);
          generatePartialProgram(program, true, HepMatchOrder.TOP_DOWN, rule);
        }
        if (conf.getBoolVar(ConfVars.HIVE_OPTIMIZE_BI_REWRITE_CUME_DIST_ENABLED)) {
          String sketchType = conf.getVar(ConfVars.HIVE_OPTIMIZE_BI_REWRITE_CUME_DIST_SKETCH);
          RelOptRule rule = new HiveRewriteToDataSketchesRules.CumeDistRewriteRule(sketchType);
          generatePartialProgram(program, true, HepMatchOrder.TOP_DOWN, rule);
        }
        if (conf.getBoolVar(ConfVars.HIVE_OPTIMIZE_BI_REWRITE_NTILE_ENABLED)) {
          String sketchType = conf.getVar(ConfVars.HIVE_OPTIMIZE_BI_REWRITE_NTILE_SKETCH);
          RelOptRule rule = new HiveRewriteToDataSketchesRules.NTileRewrite(sketchType);
          generatePartialProgram(program, true, HepMatchOrder.TOP_DOWN, rule);
        }
        if (conf.getBoolVar(ConfVars.HIVE_OPTIMIZE_BI_REWRITE_RANK_ENABLED)) {
          String sketchType = conf.getVar(ConfVars.HIVE_OPTIMIZE_BI_REWRITE_RANK_SKETCH);
          RelOptRule rule = new HiveRewriteToDataSketchesRules.RankRewriteRule(sketchType);
          generatePartialProgram(program, true, HepMatchOrder.TOP_DOWN, rule);
        }
      }
      // Run this optimization early, since it is expanding the operator pipeline.
      if (!conf.getVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("mr") &&
          conf.getBoolVar(HiveConf.ConfVars.HIVEOPTIMIZEDISTINCTREWRITE)) {
        // Its not clear, if this rewrite is always performant on MR, since extra map phase
        // introduced for 2nd MR job may offset gains of this multi-stage aggregation.
        // We need a cost model for MR to enable this on MR.
        generatePartialProgram(program, true, HepMatchOrder.TOP_DOWN,
            HiveExpandDistinctAggregatesRule.INSTANCE);
      }

      // 2. Try factoring out common filter elements & separating deterministic
      // vs non-deterministic UDF. This needs to run before PPD so that PPD can
      // add on-clauses for old style Join Syntax
      // Ex: select * from R1 join R2 where ((R1.x=R2.x) and R1.y<10) or
      // ((R1.x=R2.x) and R1.z=10)) and rand(1) < 0.1
      generatePartialProgram(program, false, HepMatchOrder.DEPTH_FIRST,
          new HivePreFilteringRule(maxCNFNodeCount));

      // 3. Run exhaustive PPD, add not null filters, transitive inference,
      // constant propagation, constant folding
      List<RelOptRule> rules = Lists.newArrayList();
      if (conf.getBoolVar(HiveConf.ConfVars.HIVEOPTPPD_WINDOWING)) {
        rules.add(HiveFilterProjectTransposeRule.DETERMINISTIC_WINDOWING);
      } else {
        rules.add(HiveFilterProjectTransposeRule.DETERMINISTIC);
      }
      rules.add(HiveFilterSetOpTransposeRule.INSTANCE);
      rules.add(HiveFilterSortTransposeRule.INSTANCE);
      rules.add(HiveFilterJoinRule.JOIN);
      rules.add(HiveFilterJoinRule.FILTER_ON_JOIN);
      rules.add(new HiveFilterAggregateTransposeRule(Filter.class, HiveRelFactories.HIVE_BUILDER,
          Aggregate.class));
      rules.add(HiveFilterMergeRule.INSTANCE);
      if (conf.getBoolVar(HiveConf.ConfVars.HIVE_OPTIMIZE_REDUCE_WITH_STATS)) {
        rules.add(HiveReduceExpressionsWithStatsRule.INSTANCE);
      }
      rules.add(HiveProjectFilterPullUpConstantsRule.INSTANCE);
      rules.add(HiveReduceExpressionsRule.PROJECT_INSTANCE);
      rules.add(HiveReduceExpressionsRule.FILTER_INSTANCE);
      rules.add(HiveReduceExpressionsRule.JOIN_INSTANCE);
      rules.add(HiveAggregateReduceFunctionsRule.INSTANCE);
      rules.add(HiveAggregateReduceRule.INSTANCE);
      if (conf.getBoolVar(HiveConf.ConfVars.HIVEPOINTLOOKUPOPTIMIZER)) {
        rules.add(new HivePointLookupOptimizerRule.FilterCondition(minNumORClauses));
        rules.add(new HivePointLookupOptimizerRule.JoinCondition(minNumORClauses));
        rules.add(new HivePointLookupOptimizerRule.ProjectionExpressions(minNumORClauses));
      }
      rules.add(HiveProjectJoinTransposeRule.INSTANCE);
      if (conf.getBoolVar(HiveConf.ConfVars.HIVE_OPTIMIZE_CONSTRAINTS_JOIN) &&
          profilesCBO.contains(ExtendedCBOProfile.REFERENTIAL_CONSTRAINTS)) {
        rules.add(HiveJoinConstraintsRule.INSTANCE);
      }
      rules.add(HiveJoinAddNotNullRule.INSTANCE_JOIN);
      rules.add(HiveJoinAddNotNullRule.INSTANCE_SEMIJOIN);
      rules.add(HiveJoinAddNotNullRule.INSTANCE_ANTIJOIN);
      rules.add(HiveJoinPushTransitivePredicatesRule.INSTANCE_JOIN);
      rules.add(HiveJoinPushTransitivePredicatesRule.INSTANCE_SEMIJOIN);
      rules.add(HiveJoinPushTransitivePredicatesRule.INSTANCE_ANTIJOIN);
      rules.add(HiveSortMergeRule.INSTANCE);
      rules.add(HiveSortPullUpConstantsRule.SORT_LIMIT_INSTANCE);
      rules.add(HiveSortPullUpConstantsRule.SORT_EXCHANGE_INSTANCE);
      rules.add(HiveUnionPullUpConstantsRule.INSTANCE);
      rules.add(HiveAggregatePullUpConstantsRule.INSTANCE);
      generatePartialProgram(program, true, HepMatchOrder.BOTTOM_UP,
          rules.toArray(new RelOptRule[0]));

      // 4. Push down limit through outer join
      // NOTE: We run this after PPD to support old style join syntax.
      // Ex: select * from R1 left outer join R2 where ((R1.x=R2.x) and R1.y<10) or
      // ((R1.x=R2.x) and R1.z=10)) and rand(1) < 0.1 order by R1.x limit 10
      if (conf.getBoolVar(HiveConf.ConfVars.HIVE_OPTIMIZE_LIMIT_TRANSPOSE)) {
        // This should be a cost based decision, but till we enable the extended cost
        // model, we will use the given value for the variable
        final float reductionProportion = HiveConf.getFloatVar(conf,
            HiveConf.ConfVars.HIVE_OPTIMIZE_LIMIT_TRANSPOSE_REDUCTION_PERCENTAGE);
        final long reductionTuples = HiveConf.getLongVar(conf,
            HiveConf.ConfVars.HIVE_OPTIMIZE_LIMIT_TRANSPOSE_REDUCTION_TUPLES);
        generatePartialProgram(program, true, HepMatchOrder.TOP_DOWN,
            HiveSortMergeRule.INSTANCE, HiveSortProjectTransposeRule.INSTANCE,
            HiveSortJoinReduceRule.INSTANCE, HiveSortUnionReduceRule.INSTANCE);
        generatePartialProgram(program, true, HepMatchOrder.BOTTOM_UP,
            new HiveSortRemoveRule(reductionProportion, reductionTuples),
            HiveProjectSortTransposeRule.INSTANCE);
      }

      // Push Down Semi Joins
      //TODO: Enable this later
      /*perfLogger.PerfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
      basePlan = hepPlan(basePlan, true, mdProvider, executorProvider, SemiJoinJoinTransposeRule.INSTANCE,
          SemiJoinFilterTransposeRule.INSTANCE, SemiJoinProjectTransposeRule.INSTANCE);
      perfLogger.PerfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER,
        "Calcite: Prejoin ordering transformation, Push Down Semi Joins"); */

      // 5. Try to remove limit and order by
      generatePartialProgram(program, false, HepMatchOrder.DEPTH_FIRST,
          HiveSortLimitRemoveRule.INSTANCE);

      // 6. Apply Partition Pruning
      generatePartialProgram(program, false, HepMatchOrder.DEPTH_FIRST,
          new HivePartitionPruneRule(conf));

      // 7. Projection Pruning (this introduces select above TS & hence needs to be run last due to PP)
      generatePartialProgram(program, false, HepMatchOrder.TOP_DOWN,
          new HiveFieldTrimmerRule(true));

      // 8. Rerun PPD through Project as column pruning would have introduced
      // DT above scans; By pushing filter just above TS, Hive can push it into
      // storage (incase there are filters on non partition cols). This only
      // matches FIL-PROJ-TS
      // Also merge, remove and reduce Project if possible
      generatePartialProgram(program, true, HepMatchOrder.TOP_DOWN,
          HiveFilterProjectTSTransposeRule.INSTANCE, HiveFilterProjectTSTransposeRule.INSTANCE_DRUID,
          HiveProjectFilterPullUpConstantsRule.INSTANCE, HiveProjectMergeRule.INSTANCE,
          ProjectRemoveRule.INSTANCE, HiveSortMergeRule.INSTANCE);

      // 9. Get rid of sq_count_check if group by key is constant
      if (conf.getBoolVar(ConfVars.HIVE_REMOVE_SQ_COUNT_CHECK)) {
        generatePartialProgram(program, false, HepMatchOrder.DEPTH_FIRST,
            HiveRemoveSqCountCheck.INSTANCE);
      }

      // 10. Convert left outer join + null filter on right side table column to anti join. Add this
      // rule after all the optimization for which calcite support for anti join is missing.
      // Needs to be done before ProjectRemoveRule as it expect a project over filter.
      // This is done before join re-ordering as join re-ordering is converting the left outer
      // to right join in some cases before converting back again to left outer.
      if (conf.getBoolVar(ConfVars.HIVE_CONVERT_ANTI_JOIN)) {
        generatePartialProgram(program, false, HepMatchOrder.DEPTH_FIRST,
                HiveAntiSemiJoinRule.INSTANCE);
      }

      // Trigger program
      perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
      basePlan = executeProgram(basePlan, program.build(), mdProvider, executorProvider);
      perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER,
          "Calcite: Prejoin ordering transformation");

      return basePlan;
    }

    /**
     * Returns true if MV is being loaded, constructed or being rebuilt.
     */
    private boolean isMaterializedViewMaintenance() {
      return mvRebuildMode != MaterializationRebuildMode.NONE
          || ctx.isLoadingMaterializedView()
          || getQB().isMaterializedView();
    }

    private RelNode applyMaterializedViewRewriting(RelOptPlanner planner, RelNode basePlan,
        RelMetadataProvider mdProvider, RexExecutor executorProvider) {
      final RelOptCluster optCluster = basePlan.getCluster();
      final PerfLogger perfLogger = SessionState.getPerfLogger();

      final boolean useMaterializedViewsRegistry = !conf.get(HiveConf.ConfVars.HIVE_SERVER2_MATERIALIZED_VIEWS_REGISTRY_IMPL.varname)
          .equals("DUMMY");
      final RelNode calcitePreMVRewritingPlan = basePlan;
      final List<String> tablesUsedQuery = getTablesUsed(basePlan);
      final boolean mvRebuild = mvRebuildMode != MaterializationRebuildMode.NONE;

      // Add views to planner
      List<RelOptMaterialization> materializations = new ArrayList<>();
      try {
        if (mvRebuild) {
          // We only retrieve the materialization corresponding to the rebuild. In turn,
          // we pass 'true' for the forceMVContentsUpToDate parameter, as we cannot allow the
          // materialization contents to be stale for a rebuild if we want to use it.
          HiveRelOptMaterialization materialization = db.getMaterializedViewForRebuild(
              mvRebuildDbName, mvRebuildName, tablesUsedQuery, getTxnMgr());
          if (materialization != null) {
            materializations.add(materialization);
          }
        } else {
          // This is not a rebuild, we retrieve all the materializations.
          // In turn, we do not need to force the materialization contents to be up-to-date,
          // as this is not a rebuild, and we apply the user parameters
          // (HIVE_MATERIALIZED_VIEW_REWRITING_TIME_WINDOW) instead.
          if (useMaterializedViewsRegistry) {
            materializations.addAll(db.getPreprocessedMaterializedViewsFromRegistry(tablesUsedQuery, getTxnMgr()));
          } else {
            materializations.addAll(db.getPreprocessedMaterializedViews(tablesUsedQuery, getTxnMgr()));
          }
        }
        // We need to use the current cluster for the scan operator on views,
        // otherwise the planner will throw an Exception (different planners)
        materializations = materializations.stream().
                map(materialization -> copyMaterializationToNewCluster(optCluster, materialization)).
                collect(Collectors.toList());
      } catch (HiveException e) {
        LOG.warn("Exception loading materialized views", e);
      }

      if (materializations.isEmpty()) {
        // There are no materializations, we can return the original plan
        return calcitePreMVRewritingPlan;
      }

      perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);

      // We need to expand IN/BETWEEN expressions when materialized view rewriting
      // is triggered since otherwise this may prevent some rewritings from happening
      HepProgramBuilder program = new HepProgramBuilder();
      generatePartialProgram(program, false, HepMatchOrder.DEPTH_FIRST,
          HiveInBetweenExpandRule.FILTER_INSTANCE,
          HiveInBetweenExpandRule.JOIN_INSTANCE,
          HiveInBetweenExpandRule.PROJECT_INSTANCE);
      basePlan = executeProgram(basePlan, program.build(), mdProvider, executorProvider);

      if (mvRebuild) {
        // If it is a materialized view rebuild, we use the HepPlanner, since we only have
        // one MV and we would like to use it to create incremental maintenance plans
        program = new HepProgramBuilder();
        generatePartialProgram(program, true, HepMatchOrder.TOP_DOWN,
            HiveMaterializedViewRule.MATERIALIZED_VIEW_REWRITING_RULES);
        // Add materialization for rebuild to planner
        assert materializations.size() == 1;
        // Optimize plan
        basePlan = executeProgram(basePlan, program.build(), mdProvider, executorProvider, materializations);
      } else {
        // Pre-processing to being able to trigger additional rewritings
        basePlan = HiveMaterializedViewBoxing.boxPlan(basePlan);

        // If this is not a rebuild, we use Volcano planner as the decision
        // on whether to use MVs or not and which MVs to use should be cost-based
        optCluster.invalidateMetadataQuery();
        RelMetadataQuery.THREAD_PROVIDERS.set(HiveMaterializationRelMetadataProvider.DEFAULT);

        // Add materializations to planner
        for (RelOptMaterialization materialization : materializations) {
          planner.addMaterialization(materialization);
        }
        // Add rule to split aggregate with grouping sets (if any)
        planner.addRule(HiveAggregateSplitRule.INSTANCE);
        // Add view-based rewriting rules to planner
        for (RelOptRule rule : HiveMaterializedViewRule.MATERIALIZED_VIEW_REWRITING_RULES) {
          planner.addRule(rule);
        }
        // Unboxing rule
        planner.addRule(HiveMaterializedViewBoxing.INSTANCE_UNBOXING);
        // Partition pruner rule
        planner.addRule(HiveFilterProjectTSTransposeRule.INSTANCE);
        planner.addRule(new HivePartitionPruneRule(conf));

        // Optimize plan
        planner.setRoot(basePlan);
        basePlan = planner.findBestExp();
        // Remove view-based rewriting rules from planner
        planner.clear();

        // Restore default cost model
        optCluster.invalidateMetadataQuery();
        RelMetadataQuery.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(mdProvider));
      }

      perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER, "Calcite: View-based rewriting");

      List<Table> materializedViewsUsedOriginalPlan = getMaterializedViewsUsed(calcitePreMVRewritingPlan);
      List<Table> materializedViewsUsedAfterRewrite = getMaterializedViewsUsed(basePlan);
      if (materializedViewsUsedOriginalPlan.size() == materializedViewsUsedAfterRewrite.size()) {
        // Materialized view-based rewriting did not happen, we can return the original plan
        return calcitePreMVRewritingPlan;
      }

      try {
        if (!HiveMaterializedViewUtils.checkPrivilegeForMaterializedViews(materializedViewsUsedAfterRewrite)) {
          // if materialized views do not have appropriate privileges, we shouldn't be using them
          return calcitePreMVRewritingPlan;
        }
      } catch (HiveException e) {
        LOG.warn("Exception checking privileges for materialized views", e);
        return calcitePreMVRewritingPlan;
      }
      // A rewriting was produced, we will check whether it was part of an incremental rebuild
      // to try to replace INSERT OVERWRITE by INSERT or MERGE
      if (mvRebuildMode == MaterializationRebuildMode.INSERT_OVERWRITE_REBUILD) {
        if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_MATERIALIZED_VIEW_REBUILD_INCREMENTAL)) {
          // First we need to check if it is valid to convert to MERGE/INSERT INTO.
          // If we succeed, we modify the plan and afterwards the AST.
          // MV should be an acid table.
          MaterializedViewRewritingRelVisitor visitor = new MaterializedViewRewritingRelVisitor();
          visitor.go(basePlan);
          if (visitor.isRewritingAllowed()) {
            // Trigger rewriting to remove UNION branch with MV
            program = new HepProgramBuilder();
            if (visitor.isContainsAggregate()) {
              generatePartialProgram(program, false, HepMatchOrder.DEPTH_FIRST,
                  HiveAggregateIncrementalRewritingRule.INSTANCE);
              mvRebuildMode = MaterializationRebuildMode.AGGREGATE_REBUILD;
            } else {
              generatePartialProgram(program, false, HepMatchOrder.DEPTH_FIRST,
                  HiveNoAggregateIncrementalRewritingRule.INSTANCE);
              mvRebuildMode = MaterializationRebuildMode.NO_AGGREGATE_REBUILD;
            }
            basePlan = executeProgram(basePlan, program.build(), mdProvider, executorProvider);
          }
        }
      } else if (useMaterializedViewsRegistry) {
        // Before proceeding we need to check whether materialized views used are up-to-date
        // wrt information in metastore
        try {
          if (!db.validateMaterializedViewsFromRegistry(materializedViewsUsedAfterRewrite, tablesUsedQuery, getTxnMgr())) {
            return calcitePreMVRewritingPlan;
          }
        } catch (HiveException e) {
          LOG.warn("Exception validating materialized views", e);
          return calcitePreMVRewritingPlan;
        }
      }
      // Now we trigger some needed optimization rules again
      basePlan = applyPreJoinOrderingTransforms(basePlan, mdProvider, executorProvider);

      if (mvRebuildMode == MaterializationRebuildMode.AGGREGATE_REBUILD) {
        // Make a cost-based decision factoring the configuration property
        optCluster.invalidateMetadataQuery();
        RelMetadataQuery.THREAD_PROVIDERS.set(HiveMaterializationRelMetadataProvider.DEFAULT);
        RelMetadataQuery mq = RelMetadataQuery.instance();
        RelOptCost costOriginalPlan = mq.getCumulativeCost(calcitePreMVRewritingPlan);
        final double factorSelectivity = HiveConf.getFloatVar(
            conf, HiveConf.ConfVars.HIVE_MATERIALIZED_VIEW_REBUILD_INCREMENTAL_FACTOR);
        RelOptCost costRebuildPlan = mq.getCumulativeCost(basePlan).multiplyBy(factorSelectivity);
        if (costOriginalPlan.isLe(costRebuildPlan)) {
          basePlan = calcitePreMVRewritingPlan;
          mvRebuildMode = MaterializationRebuildMode.INSERT_OVERWRITE_REBUILD;
        }
        optCluster.invalidateMetadataQuery();
        RelMetadataQuery.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(mdProvider));
      }

      return basePlan;
    }

    private boolean isMaterializedViewRewritingByTextEnabled() {
      return conf.getBoolVar(ConfVars.HIVE_MATERIALIZED_VIEW_ENABLE_AUTO_REWRITING_SQL) &&
              mvRebuildMode == MaterializationRebuildMode.NONE &&
              !getQB().isMaterializedView() && !ctx.isLoadingMaterializedView() && !getQB().isCTAS() &&
              getQB().getIsQuery() &&
              getQB().hasTableDefined();
    }

    private RelNode applyMaterializedViewRewritingByText(RelNode calciteGenPlan, RelOptCluster optCluster) {
      unparseTranslator.applyTranslations(ctx.getTokenRewriteStream(), EXPANDED_QUERY_TOKEN_REWRITE_PROGRAM);
      String expandedQueryText = ctx.getTokenRewriteStream()
              .toString(EXPANDED_QUERY_TOKEN_REWRITE_PROGRAM, ast.getTokenStartIndex(), ast.getTokenStopIndex());
      try {
        List<HiveRelOptMaterialization> relOptMaterializationList = db.getMaterializedViewsBySql(
                expandedQueryText, getTablesUsed(calciteGenPlan), getTxnMgr());
        for (HiveRelOptMaterialization relOptMaterialization : relOptMaterializationList) {
          try {
            Table hiveTableMD = extractTable(relOptMaterialization);
            if (HiveMaterializedViewUtils.checkPrivilegeForMaterializedViews(singletonList(hiveTableMD))) {
              if (db.validateMaterializedViewsFromRegistry(
                      singletonList(hiveTableMD),
                      singletonList(hiveTableMD.getFullyQualifiedName()),
                      getTxnMgr())) {
                return copyMaterializationToNewCluster(optCluster, relOptMaterialization).tableRel;
              }
            } else {
              LOG.debug("User does not have privilege to use materialized view {}",
                      relOptMaterialization.qualifiedTableName);
            }
          } catch (HiveException e) {
            LOG.warn("Skipping materialized view due to validation failure: " +
                    relOptMaterialization.qualifiedTableName, e);
          }
        }
      } catch (HiveException e) {
        LOG.warn(String.format("Exception while looking up materialized views for query '%s'", expandedQueryText), e);
      }

      return null;
    }

    /**
     * Perform join reordering optimization.
     *
     * @param basePlan
     *          original plan
     * @param mdProvider
     *          meta data provider
     * @param executorProvider
     *          executor
     * @return
     */
    private RelNode applyJoinOrderingTransform(RelNode basePlan, RelMetadataProvider mdProvider, RexExecutor executorProvider) {
      PerfLogger perfLogger = SessionState.getPerfLogger();

      final HepProgramBuilder program = new HepProgramBuilder();
      // Remove Projects between Joins so that JoinToMultiJoinRule can merge them to MultiJoin.
      // If FK-PK are declared, it tries to pull non-filtering column appending join nodes.
      List<RelOptRule> rules = Lists.newArrayList();
      if (profilesCBO.contains(ExtendedCBOProfile.REFERENTIAL_CONSTRAINTS)) {
        rules.add(HiveJoinSwapConstraintsRule.INSTANCE);
      }
      rules.add(HiveSemiJoinProjectTransposeRule.INSTANCE);
      rules.add(HiveJoinProjectTransposeRule.LEFT_PROJECT_BTW_JOIN);
      rules.add(HiveJoinProjectTransposeRule.RIGHT_PROJECT_BTW_JOIN);
      rules.add(HiveProjectMergeRule.INSTANCE);
      if (profilesCBO.contains(ExtendedCBOProfile.REFERENTIAL_CONSTRAINTS)) {
        rules.add(conf.getBoolVar(HiveConf.ConfVars.HIVEOPTPPD_WINDOWING) ?
            HiveFilterProjectTransposeRule.DETERMINISTIC_WINDOWING_ON_NON_FILTERING_JOIN :
            HiveFilterProjectTransposeRule.DETERMINISTIC_ON_NON_FILTERING_JOIN);
        rules.add(HiveFilterJoinRule.FILTER_ON_NON_FILTERING_JOIN);
      }
      generatePartialProgram(program, true, HepMatchOrder.BOTTOM_UP,
          rules.toArray(new RelOptRule[0]));
      // Join reordering
      generatePartialProgram(program, false, HepMatchOrder.BOTTOM_UP,
          new JoinToMultiJoinRule(HiveJoin.class), new LoptOptimizeJoinRule(HiveRelFactories.HIVE_BUILDER));

      perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
      RelNode calciteOptimizedPlan;
      try {
        calciteOptimizedPlan = executeProgram(basePlan, program.build(), mdProvider, executorProvider);
      } catch (Exception e) {
        if (noColsMissingStats.get() > 0) {
          LOG.warn("Missing column stats (see previous messages), skipping join reordering in CBO");
          noColsMissingStats.set(0);
          calciteOptimizedPlan = basePlan;
          disableSemJoinReordering = false;
        } else {
          throw e;
        }
      }
      perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER, "Calcite: Join Reordering");

      return calciteOptimizedPlan;
    }

    /**
     * Perform join reordering post-optimization.
     *
     * @param basePlan
     *          original plan
     * @param mdProvider
     *          meta data provider
     * @param executorProvider
     *          executor
     * @return
     */
    private RelNode applyPostJoinOrderingTransform(RelNode basePlan, RelMetadataProvider mdProvider, RexExecutor executorProvider) {
      PerfLogger perfLogger = SessionState.getPerfLogger();

      final HepProgramBuilder program = new HepProgramBuilder();

      double factor = conf.getFloatVar(ConfVars.HIVE_CARDINALITY_PRESERVING_JOIN_OPTIMIZATION_FACTOR);
      if (factor > 0.0) {
        generatePartialProgram(program, false, HepMatchOrder.TOP_DOWN,
            new HiveCardinalityPreservingJoinRule(factor));
      }

      // 1. Run other optimizations that do not need stats
      generatePartialProgram(program, false, HepMatchOrder.DEPTH_FIRST,
          ProjectRemoveRule.INSTANCE, HiveUnionMergeRule.INSTANCE,
          HiveAggregateProjectMergeRule.INSTANCE, HiveProjectMergeRule.INSTANCE_NO_FORCE,
          HiveJoinCommuteRule.INSTANCE);

      // 2. Run aggregate-join transpose (cost based)
      //    If it failed because of missing stats, we continue with
      //    the rest of optimizations
      if (conf.getBoolVar(ConfVars.AGGR_JOIN_TRANSPOSE) || conf.getBoolVar(ConfVars.AGGR_JOIN_TRANSPOSE_UNIQUE)) {
        generatePartialProgram(program, false, HepMatchOrder.DEPTH_FIRST,
            new HiveAggregateJoinTransposeRule(noColsMissingStats,
                conf.getBoolVar(ConfVars.AGGR_JOIN_TRANSPOSE),
                conf.getBoolVar(ConfVars.AGGR_JOIN_TRANSPOSE_UNIQUE)));
      }

      // 3. Convert Join + GBy to semijoin
      // Run this rule at later stages, since many calcite rules cant deal with semijoin
      if (conf.getBoolVar(ConfVars.SEMIJOIN_CONVERSION)) {
        generatePartialProgram(program, true, HepMatchOrder.DEPTH_FIRST,
            HiveSemiJoinRule.INSTANCE_PROJECT, HiveSemiJoinRule.INSTANCE_PROJECT_SWAPPED,
            HiveSemiJoinRule.INSTANCE_AGGREGATE, HiveSemiJoinRule.INSTANCE_AGGREGATE_SWAPPED);
      }

      // 4. convert SemiJoin + GBy to SemiJoin
      generatePartialProgram(program, false, HepMatchOrder.DEPTH_FIRST,
          HiveRemoveGBYSemiJoinRule.INSTANCE);

      // 5. Run rule to fix windowing issue when it is done over
      // aggregation columns (HIVE-10627)
      if (profilesCBO.contains(ExtendedCBOProfile.WINDOWING_POSTPROCESSING)) {
        generatePartialProgram(program, false, HepMatchOrder.DEPTH_FIRST,
            HiveWindowingFixRule.INSTANCE);
        generatePartialProgram(program, false, HepMatchOrder.DEPTH_FIRST,
            HiveWindowingLastValueRewrite.INSTANCE);
      }
      // 6. Sort predicates in filter expressions
      if (conf.getBoolVar(HiveConf.ConfVars.HIVE_OPTIMIZE_SORT_PREDS_WITH_STATS)) {
        generatePartialProgram(program, false, HepMatchOrder.DEPTH_FIRST,
            new HiveFilterSortPredicates(noColsMissingStats));
      }

      // 7. Apply Druid transformation rules
      generatePartialProgram(program, false, HepMatchOrder.DEPTH_FIRST,
          HiveDruidRules.FILTER_DATE_RANGE_RULE,
          HiveDruidRules.FILTER, HiveDruidRules.PROJECT_FILTER_TRANSPOSE,
          HiveDruidRules.AGGREGATE_FILTER_TRANSPOSE,
          HiveDruidRules.AGGREGATE_PROJECT,
          HiveDruidRules.PROJECT,
          HiveDruidRules.EXPAND_SINGLE_DISTINCT_AGGREGATES_DRUID_RULE,
          HiveDruidRules.AGGREGATE,
          HiveDruidRules.POST_AGGREGATION_PROJECT,
          HiveDruidRules.FILTER_AGGREGATE_TRANSPOSE,
          HiveDruidRules.FILTER_PROJECT_TRANSPOSE,
          HiveDruidRules.HAVING_FILTER_RULE,
          HiveDruidRules.SORT_PROJECT_TRANSPOSE,
          HiveDruidRules.SORT,
          HiveDruidRules.PROJECT_SORT_TRANSPOSE);

      // 8. Apply JDBC transformation rules
      if (conf.getBoolVar(ConfVars.HIVE_ENABLE_JDBC_PUSHDOWN)) {
        List<RelOptRule> rules = Lists.newArrayList();
        rules.add(JDBCExpandExpressionsRule.FILTER_INSTANCE);
        rules.add(JDBCExpandExpressionsRule.JOIN_INSTANCE);
        rules.add(JDBCExpandExpressionsRule.PROJECT_INSTANCE);
        rules.add(JDBCExtractJoinFilterRule.INSTANCE);
        rules.add(JDBCAbstractSplitFilterRule.SPLIT_FILTER_ABOVE_JOIN);
        rules.add(JDBCAbstractSplitFilterRule.SPLIT_FILTER_ABOVE_CONVERTER);
        rules.add(JDBCFilterJoinRule.INSTANCE);
        rules.add(JDBCFilterPushDownRule.INSTANCE);
        rules.add(JDBCProjectPushDownRule.INSTANCE);
        if (!conf.getBoolVar(ConfVars.HIVE_ENABLE_JDBC_SAFE_PUSHDOWN)) {
          rules.add(JDBCJoinPushDownRule.INSTANCE);
          rules.add(JDBCUnionPushDownRule.INSTANCE);
          rules.add(JDBCAggregationPushDownRule.INSTANCE);
          rules.add(JDBCSortPushDownRule.INSTANCE);
        }
        generatePartialProgram(program, true, HepMatchOrder.TOP_DOWN,
            rules.toArray(new RelOptRule[rules.size()]));
      }

      // 9. Run rules to aid in translation from Calcite tree to Hive tree
      if (HiveConf.getBoolVar(conf, ConfVars.HIVE_CBO_RETPATH_HIVEOP)) {
        // 9.1. Merge join into multijoin operators (if possible)
        generatePartialProgram(program, true, HepMatchOrder.BOTTOM_UP,
            HiveJoinProjectTransposeRule.BOTH_PROJECT_INCLUDE_OUTER,
            HiveJoinProjectTransposeRule.LEFT_PROJECT_INCLUDE_OUTER,
            HiveJoinProjectTransposeRule.RIGHT_PROJECT_INCLUDE_OUTER,
            HiveJoinToMultiJoinRule.INSTANCE, HiveProjectMergeRule.INSTANCE);
        // The previous rules can pull up projections through join operators,
        // thus we run the field trimmer again to push them back down
        generatePartialProgram(program, false, HepMatchOrder.TOP_DOWN,
            new HiveFieldTrimmerRule(false));
        generatePartialProgram(program, false, HepMatchOrder.DEPTH_FIRST,
            ProjectRemoveRule.INSTANCE, new ProjectMergeRule(false, HiveRelFactories.HIVE_BUILDER));
        generatePartialProgram(program, true, HepMatchOrder.TOP_DOWN,
            HiveFilterProjectTSTransposeRule.INSTANCE, HiveFilterProjectTSTransposeRule.INSTANCE_DRUID,
            HiveProjectFilterPullUpConstantsRule.INSTANCE);

        // 9.2.  Introduce exchange operators below join/multijoin operators
        generatePartialProgram(program, false, HepMatchOrder.DEPTH_FIRST,
            HiveInsertExchange4JoinRule.EXCHANGE_BELOW_JOIN, HiveInsertExchange4JoinRule.EXCHANGE_BELOW_MULTIJOIN);
      } else {
        generatePartialProgram(program, false, HepMatchOrder.DEPTH_FIRST,
                HiveProjectSortExchangeTransposeRule.INSTANCE, HiveProjectMergeRule.INSTANCE);
      }

      // 10. We need to expand IN/BETWEEN expressions when loading a materialized view
      // since otherwise this may prevent some rewritings from happening
      if (ctx.isLoadingMaterializedView()) {
        generatePartialProgram(program, false, HepMatchOrder.DEPTH_FIRST,
            HiveInBetweenExpandRule.FILTER_INSTANCE,
            HiveInBetweenExpandRule.JOIN_INSTANCE,
            HiveInBetweenExpandRule.PROJECT_INSTANCE);
      }

      // Trigger program
      perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.OPTIMIZER);
      basePlan = executeProgram(basePlan, program.build(), mdProvider, executorProvider);
      perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.OPTIMIZER,
          "Calcite: Postjoin ordering transformation");

      return basePlan;
    }

    private List<String> getTablesUsed(RelNode plan) {
      List<String> tablesUsed = new ArrayList<>();
      new RelVisitor() {
        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {
          if (node instanceof TableScan) {
            TableScan ts = (TableScan) node;
            tablesUsed.add(((RelOptHiveTable) ts.getTable()).getHiveTableMD().getFullyQualifiedName());
          }
          super.visit(node, ordinal, parent);
        }
      }.go(plan);
      return tablesUsed;
    }

    private List<Table> getMaterializedViewsUsed(RelNode plan) {
      List<Table> materializedViewsUsed = new ArrayList<>();
      new RelVisitor() {
        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {
          if (node instanceof TableScan) {
            TableScan ts = (TableScan) node;
            Table table = ((RelOptHiveTable) ts.getTable()).getHiveTableMD();
            if (table.isMaterializedView()) {
              materializedViewsUsed.add(table);
            }
          } else if (node instanceof DruidQuery) {
            DruidQuery dq = (DruidQuery) node;
            Table table = ((RelOptHiveTable) dq.getTable()).getHiveTableMD();
            if (table.isMaterializedView()) {
              materializedViewsUsed.add(table);
            }
          }
          super.visit(node, ordinal, parent);
        }
      }.go(plan);
      return materializedViewsUsed;
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
    @Deprecated
    private RelNode hepPlan(RelNode basePlan, boolean followPlanChanges,
        RelMetadataProvider mdProvider, RexExecutor executorProvider, HepMatchOrder order,
        RelOptRule... rules) {
      final HepProgramBuilder programBuilder = new HepProgramBuilder();
      generatePartialProgram(programBuilder, followPlanChanges, order, rules);
      return executeProgram(basePlan, programBuilder.build(), mdProvider, executorProvider);
    }

    /**
     * Generate a HEP program with the given rule set.
     *
     * @param isCollection
     * @param order
     * @param rules
     * @return HEP program
     */
    private void generatePartialProgram(HepProgramBuilder programBuilder, boolean isCollection, HepMatchOrder order,
        RelOptRule... rules) {
      programBuilder.addMatchOrder(order);
      if (isCollection) {
        programBuilder.addRuleCollection(ImmutableList.copyOf(rules));
      } else {
        for (RelOptRule r : rules) {
          programBuilder.addRuleInstance(r);
        }
      }
    }

    private RelNode executeProgram(RelNode basePlan, HepProgram program,
        RelMetadataProvider mdProvider, RexExecutor executorProvider) {
      return executeProgram(basePlan, program, mdProvider, executorProvider, null);
    }

    private RelNode executeProgram(RelNode basePlan, HepProgram program,
        RelMetadataProvider mdProvider, RexExecutor executorProvider,
        List<RelOptMaterialization> materializations) {

      // Create planner and copy context
      HepPlanner planner = new HepPlanner(program,
          basePlan.getCluster().getPlanner().getContext());

      List<RelMetadataProvider> list = Lists.newArrayList();
      list.add(mdProvider);
      planner.registerMetadataProviders(list);
      RelMetadataProvider chainedProvider = ChainedRelMetadataProvider.of(list);
      cluster.setMetadataProvider(
          new CachingRelMetadataProvider(chainedProvider, planner));

      if (executorProvider != null) {
        // basePlan.getCluster.getPlanner is the VolcanoPlanner from apply()
        // both planners need to use the correct executor
        cluster.getPlanner().setExecutor(executorProvider);
        planner.setExecutor(executorProvider);
      }

      if (materializations != null) {
        // Add materializations to planner
        for (RelOptMaterialization materialization : materializations) {
          planner.addMaterialization(materialization);
        }
      }

      planner.setRoot(basePlan);

      return planner.findBestExp();
    }

    @SuppressWarnings("nls")
    private RelNode genSetOpLogicalPlan(Opcode opcode, String alias, String leftalias, RelNode leftRel,
        String rightalias, RelNode rightRel) throws SemanticException {
      // 1. Get Row Resolvers, Column map for original left and right input of
      // SetOp Rel
      RowResolver leftRR = this.relToHiveRR.get(leftRel);
      RowResolver rightRR = this.relToHiveRR.get(rightRel);
      Map<String, ColumnInfo> leftmap = leftRR.getFieldMap(leftalias);
      Map<String, ColumnInfo> rightmap = rightRR.getFieldMap(rightalias);

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
      relToHiveColNameCalcitePosMap.put(setOpRel, buildHiveToCalciteColumnMap(setOpOutRR));
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
        Map<ASTNode, RexNode> exprNodes = RexNodeTypeCheck.genExprNodeJoinCond(
            joinCond, jCtx, cluster.getRexBuilder());
        if (jCtx.getError() != null) {
          throw new SemanticException(SemanticAnalyzer.generateErrorMessage(jCtx.getErrorSrcNode(),
              jCtx.getError()));
        }
        calciteJoinCond = exprNodes.get(joinCond);
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
        calciteJoinType = JoinRelType.SEMI;
        leftSemiJoin = true;
        break;
      case ANTI:
        calciteJoinType = JoinRelType.ANTI;
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

        RelNode[] inputRels = new RelNode[] { leftRel, rightRel };
        final List<Integer> leftKeys = new ArrayList<Integer>();
        final List<Integer> rightKeys = new ArrayList<Integer>();
        RexNode remainingEquiCond = HiveCalciteUtil.projectNonColumnEquiConditions(HiveRelFactories.HIVE_PROJECT_FACTORY,
            inputRels, leftJoinKeys, rightJoinKeys, 0, leftKeys, rightKeys);
        // Adjust right input fields in nonEquiConds if previous call modified the input
        if (inputRels[0] != leftRel) {
          nonEquiConds = RexUtil.shift(nonEquiConds, leftRel.getRowType().getFieldCount(),
              inputRels[0].getRowType().getFieldCount() - leftRel.getRowType().getFieldCount());
        }
        calciteJoinCond = remainingEquiCond != null ?
            RexUtil.composeConjunction(cluster.getRexBuilder(),
                ImmutableList.of(remainingEquiCond, nonEquiConds), false) :
            nonEquiConds;
        final RelDataType combinedRowType = SqlValidatorUtil.createJoinType(
            cluster.getTypeFactory(), inputRels[0].getRowType(), inputRels[1].getRowType(),
            null, ImmutableList.of());

        if (hiveJoinType == JoinType.LEFTSEMI) {
          topRel = HiveSemiJoin.getSemiJoin(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
                  inputRels[0], inputRels[1],
                  HiveCalciteUtil.fixNullability(cluster.getRexBuilder(),
                          calciteJoinCond, RelOptUtil.getFieldTypeList(combinedRowType)));
        } else {
          topRel = HiveAntiJoin.getAntiJoin(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
                  inputRels[0], inputRels[1],
                  HiveCalciteUtil.fixNullability(cluster.getRexBuilder(),
                          calciteJoinCond, RelOptUtil.getFieldTypeList(combinedRowType)));
        }

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
          relToHiveColNameCalcitePosMap.put(topRel, buildHiveToCalciteColumnMap(joinRR));
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
        final RelDataType combinedRowType = SqlValidatorUtil.createJoinType(
            cluster.getTypeFactory(), leftRel.getRowType(), rightRel.getRowType(),
            null, ImmutableList.of());
        topRel = HiveJoin.getJoin(
            cluster, leftRel, rightRel,
            HiveCalciteUtil.fixNullability(cluster.getRexBuilder(),
                calciteJoinCond, RelOptUtil.getFieldTypeList(combinedRowType)),
            calciteJoinType);
        topRR = RowResolver.getCombinedRR(leftRR, rightRR);
        if (namedColumns != null) {
          List<String> tableAliases = new ArrayList<>();
          tableAliases.add(leftTableAlias);
          tableAliases.add(rightTableAlias);
          topRR.setNamedJoinInfo(new NamedJoinInfo(tableAliases, namedColumns, hiveJoinType));
        }
      }

      // 4. Add new rel & its RR to the maps
      relToHiveColNameCalcitePosMap.put(topRel, buildHiveToCalciteColumnMap(topRR));
      relToHiveRR.put(topRel, topRR);
      return topRel;
    }

    /**
     * Generate Join Logical Plan Relnode by walking through the join AST.
     *
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
      case HiveParser.TOK_LEFTANTISEMIJOIN:
        hiveJoinType = JoinType.ANTI;
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
        leftTableAlias = getTableAlias(left);
        leftRel = aliasToRel.get(leftTableAlias);
      } else if (SemanticAnalyzer.isJoinToken(left)) {
        leftRel = genJoinLogicalPlan(left, aliasToRel);
      } else if (left.getToken().getType() == HiveParser.TOK_LATERAL_VIEW) {
        leftRel = genLateralViewPlans(left, aliasToRel);
      } else {
        assert (false);
      }

      // 3. Get Right Table Alias
      ASTNode right = (ASTNode) joinParseTree.getChild(1);
      String rightTableAlias = null;
      if ((right.getToken().getType() == HiveParser.TOK_TABREF)
          || (right.getToken().getType() == HiveParser.TOK_SUBQUERY)
          || (right.getToken().getType() == HiveParser.TOK_PTBLFUNCTION)) {
        rightTableAlias = getTableAlias(right);
        rightRel = aliasToRel.get(rightTableAlias);
      } else if (right.getToken().getType() == HiveParser.TOK_LATERAL_VIEW) {
        rightRel = genLateralViewPlans(right, aliasToRel);
      }  else {
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
          colInfo.setSkewedCol(isSkewedCol(tableAlias, qb, colName));
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
        RelOptHiveTable optTable;
        if (tableType == TableType.DRUID ||
                (tableType == TableType.JDBC && tabMetaData.getProperty(Constants.JDBC_TABLE) != null)) {
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
          List<String> fullyQualifiedTabName = new ArrayList<>();
          if (tabMetaData.getDbName() != null && !tabMetaData.getDbName().isEmpty()) {
            fullyQualifiedTabName.add(tabMetaData.getDbName());
          }
          fullyQualifiedTabName.add(tabMetaData.getTableName());

          if (tableType == TableType.DRUID) {
            // Build Druid query
            String address = HiveConf.getVar(conf,
                  HiveConf.ConfVars.HIVE_DRUID_BROKER_DEFAULT_ADDRESS);
            String dataSource = tabMetaData.getParameters().get(Constants.DRUID_DATA_SOURCE);
            Set<String> metrics = new HashSet<>();
            RexBuilder rexBuilder = cluster.getRexBuilder();
            RelDataTypeFactory dtFactory = rexBuilder.getTypeFactory();
            List<RelDataType> druidColTypes = new ArrayList<>();
            List<String> druidColNames = new ArrayList<>();
            //@TODO FIX this, we actually do not need this anymore,
            // in addition to that Druid allow numeric dimensions now so this check is not accurate
            for (RelDataTypeField field : rowType.getFieldList()) {
              if (DruidTable.DEFAULT_TIMESTAMP_COLUMN.equals(field.getName())) {
                // Druid's time column is always not null.
                druidColTypes.add(dtFactory.createTypeWithNullability(field.getType(), false));
              } else {
                druidColTypes.add(field.getType());
              }
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
            rowType = dtFactory.createStructType(druidColTypes, druidColNames);
            DruidTable druidTable = new DruidTable(new DruidSchema(address, address, false),
                dataSource, RelDataTypeImpl.proto(rowType), metrics, DruidTable.DEFAULT_TIMESTAMP_COLUMN,
                intervals, null, null);
            optTable = new RelOptHiveTable(relOptSchema, relOptSchema.getTypeFactory(), fullyQualifiedTabName,
                rowType, tabMetaData, nonPartitionColumns, partitionColumns, virtualCols, conf,
                db, tabNameToTabObject, partitionCache, colStatsCache, noColsMissingStats);
            final TableScan scan = new HiveTableScan(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
                optTable, null == tableAlias ? tabMetaData.getTableName() : tableAlias,
                getAliasId(tableAlias, qb), HiveConf.getBoolVar(conf,
                    HiveConf.ConfVars.HIVE_CBO_RETPATH_HIVEOP), qb.isInsideView()
                    || qb.getAliasInsideView().contains(tableAlias.toLowerCase()));
            tableRel = DruidQuery.create(cluster, cluster.traitSetOf(BindableConvention.INSTANCE),
                optTable, druidTable, ImmutableList.of(scan), DruidSqlOperatorConverter.getDefaultMap());
          } else {
            optTable = new RelOptHiveTable(relOptSchema, relOptSchema.getTypeFactory(), fullyQualifiedTabName,
                  rowType, tabMetaData, nonPartitionColumns, partitionColumns, virtualCols, conf,
                  db, tabNameToTabObject, partitionCache, colStatsCache, noColsMissingStats);
            final HiveTableScan hts = new HiveTableScan(cluster,
                  cluster.traitSetOf(HiveRelNode.CONVENTION), optTable,
                  null == tableAlias ? tabMetaData.getTableName() : tableAlias,
                  getAliasId(tableAlias, qb),
                  HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CBO_RETPATH_HIVEOP),
                  qb.isInsideView() || qb.getAliasInsideView().contains(tableAlias.toLowerCase()));

            final String dataBaseType = tabMetaData.getProperty(Constants.JDBC_DATABASE_TYPE);
            final String url = tabMetaData.getProperty(Constants.JDBC_URL);
            final String driver = tabMetaData.getProperty(Constants.JDBC_DRIVER);
            final String user = tabMetaData.getProperty(Constants.JDBC_USERNAME);
            String pswd = tabMetaData.getProperty(Constants.JDBC_PASSWORD);
            if (pswd == null) {
              String keystore = tabMetaData.getProperty(Constants.JDBC_KEYSTORE);
              String key = tabMetaData.getProperty(Constants.JDBC_KEY);
              pswd = Utilities.getPasswdFromKeystore(keystore, key);
            }
            final String catalogName = tabMetaData.getProperty(Constants.JDBC_CATALOG);
            final String schemaName = tabMetaData.getProperty(Constants.JDBC_SCHEMA);
            final String tableName = tabMetaData.getProperty(Constants.JDBC_TABLE);

            DataSource ds = JdbcSchema.dataSource(url, driver, user, pswd);
            SqlDialect jdbcDialect = JdbcSchema.createDialect(SqlDialectFactoryImpl.INSTANCE, ds);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Dialect for table {}: {}", tableName, jdbcDialect.getClass().getName());
            }
            JdbcConvention jc = JdbcConvention.of(jdbcDialect, null, dataBaseType);
            JdbcSchema schema = new JdbcSchema(ds, jc.dialect, jc, catalogName, schemaName);
            JdbcTable jt = (JdbcTable) schema.getTable(tableName);
            if (jt == null) {
              throw new SemanticException("Table " + tableName + " was not found in the database");
            }

            JdbcHiveTableScan jdbcTableRel = new JdbcHiveTableScan(cluster, optTable, jt, jc, hts);
            tableRel = new HiveJdbcConverter(cluster, jdbcTableRel.getTraitSet().replace(HiveRelNode.CONVENTION),
                    jdbcTableRel, jc, url, user);
          }
        } else {
          // Build row type from field <type, name>
          RelDataType rowType = inferNotNullableColumns(tabMetaData, TypeConverter.getType(cluster, rr, null));
          // Build RelOptAbstractTable
          List<String> fullyQualifiedTabName = new ArrayList<>();
          if (tabMetaData.getDbName() != null && !tabMetaData.getDbName().isEmpty()) {
            fullyQualifiedTabName.add(tabMetaData.getDbName());
          }
          fullyQualifiedTabName.add(tabMetaData.getTableName());
          optTable = new RelOptHiveTable(relOptSchema, relOptSchema.getTypeFactory(), fullyQualifiedTabName,
              rowType, tabMetaData, nonPartitionColumns, partitionColumns, virtualCols, conf,
              db, tabNameToTabObject, partitionCache, colStatsCache, noColsMissingStats);
          // Build Hive Table Scan Rel
          tableRel = new HiveTableScan(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION), optTable,
              null == tableAlias ? tabMetaData.getTableName() : tableAlias,
              getAliasId(tableAlias, qb), HiveConf.getBoolVar(conf,
                  HiveConf.ConfVars.HIVE_CBO_RETPATH_HIVEOP), qb.isInsideView()
                  || qb.getAliasInsideView().contains(tableAlias.toLowerCase()));
        }

        if (!optTable.getReferentialConstraints().isEmpty()) {
          profilesCBO.add(ExtendedCBOProfile.REFERENTIAL_CONSTRAINTS);
        }

        // 6. Add Schema(RR) to RelNode-Schema map
        ImmutableMap<String, Integer> hiveToCalciteColMap = buildHiveToCalciteColumnMap(rr);
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

    private RelDataType inferNotNullableColumns(Table tabMetaData, RelDataType rowType)
        throws HiveException {
      final NotNullConstraint nnc = tabMetaData.getNotNullConstraint();
      final PrimaryKeyInfo pkc = tabMetaData.getPrimaryKeyInfo();
      if ((nnc == null || nnc.getNotNullConstraints().isEmpty()) &&
          (pkc == null || pkc.getColNames().isEmpty())) {
        return rowType;
      }

      // Build the bitset with not null columns
      ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
      if (nnc != null) {
        for (String nnCol : nnc.getNotNullConstraints().values()) {
          int nnPos = -1;
          for (int i = 0; i < rowType.getFieldNames().size(); i++) {
            if (rowType.getFieldNames().get(i).equals(nnCol)) {
              nnPos = i;
              break;
            }
          }
          if (nnPos == -1) {
            LOG.error("Column for not null constraint definition " + nnCol + " not found");
            return rowType;
          }
          builder.set(nnPos);
        }
      }
      if (pkc != null) {
        for (String pkCol : pkc.getColNames().values()) {
          int pkPos = -1;
          for (int i = 0; i < rowType.getFieldNames().size(); i++) {
            if (rowType.getFieldNames().get(i).equals(pkCol)) {
              pkPos = i;
              break;
            }
          }
          if (pkPos == -1) {
            LOG.error("Column for not null constraint definition " + pkCol + " not found");
            return rowType;
          }
          builder.set(pkPos);
        }
      }
      ImmutableBitSet bitSet = builder.build();

      RexBuilder rexBuilder = cluster.getRexBuilder();
      RelDataTypeFactory dtFactory = rexBuilder.getTypeFactory();

      List<RelDataType> fieldTypes = new LinkedList<RelDataType>();
      List<String> fieldNames = new LinkedList<String>();
      for (RelDataTypeField rdtf : rowType.getFieldList()) {
        if (bitSet.indexOf(rdtf.getIndex()) != -1) {
          fieldTypes.add(dtFactory.createTypeWithNullability(rdtf.getType(), false));
        } else {
          fieldTypes.add(rdtf.getType());
        }
        fieldNames.add(rdtf.getName());
      }
      return dtFactory.createStructType(fieldTypes, fieldNames);
    }

    private TableType obtainTableType(Table tabMetaData) {
      if (tabMetaData.getStorageHandler() != null) {
        final String storageHandlerStr = tabMetaData.getStorageHandler().toString();
        if (storageHandlerStr
            .equals(Constants.DRUID_HIVE_STORAGE_HANDLER_ID)) {
          return TableType.DRUID;
        }

        if (storageHandlerStr
            .equals(Constants.JDBC_HIVE_STORAGE_HANDLER_ID)) {
          return TableType.JDBC;
        }

      }

      return TableType.NATIVE;
    }

    private RelNode genFilterRelNode(ASTNode filterNode, RelNode srcRel,
            ImmutableMap<String, Integer> outerNameToPosMap, RowResolver outerRR,
            boolean useCaching) throws SemanticException {
      RexNode filterExpression = genRexNode(filterNode, relToHiveRR.get(srcRel),
          outerRR, null, useCaching, cluster.getRexBuilder());

      return genFilterRelNode(filterExpression, srcRel, outerNameToPosMap, outerRR);
    }

    private RelNode genFilterRelNode(RexNode filterExpression, RelNode srcRel,
        ImmutableMap<String, Integer> outerNameToPosMap, RowResolver outerRR) throws SemanticException {
      if (RexUtil.isLiteral(filterExpression, false)
          && filterExpression.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
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
      final ImmutableMap<String, Integer> hiveColNameCalcitePosMap =
          this.relToHiveColNameCalcitePosMap.get(srcRel);
      filterExpression = new CorrelationConverter(
          new InputContext(srcRel.getRowType(), hiveColNameCalcitePosMap, relToHiveRR.get(srcRel)),
          outerNameToPosMap, outerRR, subqueryId).apply(filterExpression);
      RexNode factoredFilterExpression = RexUtil
          .pullFactors(cluster.getRexBuilder(), filterExpression);
      RelNode filterRel = new HiveFilter(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION), srcRel,
          HiveCalciteUtil.fixNullability(cluster.getRexBuilder(),
              factoredFilterExpression, RelOptUtil.getFieldTypeList(srcRel.getRowType())));
      this.relToHiveColNameCalcitePosMap.put(filterRel, hiveColNameCalcitePosMap);
      relToHiveRR.put(filterRel, relToHiveRR.get(srcRel));
      relToHiveColNameCalcitePosMap.put(filterRel, hiveColNameCalcitePosMap);

      return filterRel;
    }

    /**
     * Shuttle that replaces certain references with correlation variables
     * if needed.
     */
    private class CorrelationConverter extends RexShuttle {
      private final InputContext inputContext;
      private final ImmutableMap<Integer, String> outerPositionToColumnName;
      private final RowResolver outerRowResolver;
      private final int correlatedId;

      private CorrelationConverter(InputContext inputContext,
          ImmutableMap<String, Integer> outerColumnNameToPosition, RowResolver outerRowResolver,
          int correlatedId) {
        this.inputContext = inputContext;
        this.outerPositionToColumnName = outerColumnNameToPosition == null ?
            null : ImmutableBiMap.copyOf(outerColumnNameToPosition).inverse();
        this.outerRowResolver = outerRowResolver;
        this.correlatedId = correlatedId;
      }

      @Override
      public RexNode visitInputRef(RexInputRef col) {
        InputContext context = null;
        if (inputContext.inputRowResolver == null) {
          context = inputContext;
        } else {
          int index = col.getIndex();
          String colName = inputContext.positionToColumnName.get(index);
          if (colName != null) {
            context = inputContext;
          }
        }

        if(context == null) {
          // we have correlated column, build data type from outer rr
          RelDataType rowType;
          try {
            rowType = TypeConverter.getType(cluster, outerRowResolver, null);
          } catch (CalciteSemanticException e) {
            throw new RuntimeException("Error converting type", e);
          }
          int index = col.getIndex() - inputContext.inputRowType.getFieldList().size();
          if (outerPositionToColumnName.get(index) == null) {
            throw new RuntimeException(ErrorMsg.INVALID_COLUMN_NAME.getMsg());
          }
          CorrelationId colCorr = new CorrelationId(correlatedId);
          RexNode corExpr = cluster.getRexBuilder().makeCorrel(rowType, colCorr);
          return cluster.getRexBuilder().makeFieldAccess(corExpr, index);
        }
        int pos = col.getIndex();
        return cluster.getRexBuilder().makeInputRef(
            context.inputRowType.getFieldList().get(pos).getType(), pos);
      }
    }

    private RelNode genLateralViewPlans(ASTNode lateralView, Map<String, RelNode> aliasToRel)
            throws SemanticException {
      final RexBuilder rexBuilder = this.cluster.getRexBuilder();
      final RelDataTypeFactory dtFactory = this.cluster.getTypeFactory();
      final String inlineFunctionName =
          GenericUDTFInline.class.getAnnotation(Description.class).name();
      int numChildren = lateralView.getChildCount();
      assert (numChildren == 2);

      // 1) Obtain input and all related data structures
      ASTNode next = (ASTNode) lateralView.getChild(1);
      RelNode inputRel = null;
      switch (next.getToken().getType()) {
        case HiveParser.TOK_TABREF:
        case HiveParser.TOK_SUBQUERY:
        case HiveParser.TOK_PTBLFUNCTION:
          String inputTableAlias = getTableAlias(next);
          inputRel = aliasToRel.get(inputTableAlias);
          break;
        case HiveParser.TOK_LATERAL_VIEW:
          inputRel = genLateralViewPlans(next, aliasToRel);
          break;
        default:
          throw new SemanticException(ASTErrorUtils.getMsg(
              ErrorMsg.LATERAL_VIEW_INVALID_CHILD.getMsg(), lateralView));
      }
      // Input row resolver
      RowResolver inputRR = this.relToHiveRR.get(inputRel);
      // Extract input refs. They will serve as input for the function invocation
      List<RexNode> inputRefs = Lists.transform(inputRel.getRowType().getFieldList(),
          input -> new RexInputRef(input.getIndex(), input.getType()));
      // Extract type for the arguments
      List<RelDataType> inputRefsTypes = new ArrayList<>();
      for (int i = 0; i < inputRefs.size(); i++) {
        inputRefsTypes.add(inputRefs.get(i).getType());
      }
      // Input name to position map
      ImmutableMap<String, Integer> inputPosMap = this.relToHiveColNameCalcitePosMap.get(inputRel);

      // 2) Generate HiveTableFunctionScan RelNode for lateral view
      // TODO: Support different functions (not only INLINE) with LATERAL VIEW JOIN
      // ^(TOK_LATERAL_VIEW ^(TOK_SELECT ^(TOK_SELEXPR ^(TOK_FUNCTION Identifier["inline"] valuesClause) identifier* tableAlias)))
      final ASTNode selExprClause =
          (ASTNode) lateralView.getChild(0).getChild(0);
      final ASTNode functionCall =
          (ASTNode) selExprClause.getChild(0);
      if (functionCall.getChild(0).getText().compareToIgnoreCase(inlineFunctionName) != 0) {
        throw new SemanticException("CBO only supports inline LVJ");
      }
      final ASTNode valuesClause =
          (ASTNode) functionCall.getChild(1);
      // Output types. They will be the concatenation of the input refs types and
      // the types of the expressions for the lateral view generated rows
      // Generate all expressions from lateral view
      RexCall valuesExpr = (RexCall) genRexNode(
          valuesClause, inputRR, false, false, cluster.getRexBuilder());
      RelDataType valuesRowType = valuesExpr.getType().getComponentType();
      List<RexNode> newStructExprs = new ArrayList<>();
      for (RexNode structExpr : valuesExpr.getOperands()) {
        RexCall structCall = (RexCall) structExpr;
        List<RexNode> exprs = new ArrayList<>(inputRefs);
        exprs.addAll(structCall.getOperands());
        newStructExprs.add(rexBuilder.makeCall(structCall.op, exprs));
      }
      RexNode convertedFinalValuesExpr =
          rexBuilder.makeCall(valuesExpr.op, newStructExprs);
      // The return type will be the concatenation of input type and original values type
      RelDataType retType = SqlValidatorUtil.deriveJoinRowType(inputRel.getRowType(),
          valuesRowType, JoinRelType.INNER, dtFactory, null, ImmutableList.of());

      // Create inline SQL operator
      FunctionInfo inlineFunctionInfo = FunctionRegistry.getFunctionInfo(inlineFunctionName);
      SqlOperator calciteOp = SqlFunctionConverter.getCalciteOperator(
          inlineFunctionName, inlineFunctionInfo.getGenericUDTF(),
          ImmutableList.copyOf(inputRefsTypes), retType);

      RelNode htfsRel = HiveTableFunctionScan.create(cluster, TraitsUtil.getDefaultTraitSet(cluster),
          ImmutableList.of(inputRel), rexBuilder.makeCall(calciteOp, convertedFinalValuesExpr),
          null, retType, null);

      // 3) Keep track of colname-to-posmap && RR for new op
      RowResolver outputRR = new RowResolver();
      // Add all input columns
      if (!RowResolver.add(outputRR, inputRR)) {
        LOG.warn("Duplicates detected when adding columns to RR: see previous message");
      }
      // Add all columns from lateral view
      // First we extract the information that the query provides
      String tableAlias = null;
      List<String> columnAliases = new ArrayList<>();
      Set<String> uniqueNames = new HashSet<>();
      for (int i = 1; i < selExprClause.getChildren().size(); i++) {
        ASTNode child = (ASTNode) selExprClause.getChild(i);
        switch (child.getToken().getType()) {
          case HiveParser.TOK_TABALIAS:
            tableAlias = unescapeIdentifier(child.getChild(0).getText());
            break;
          default:
            String colAlias = unescapeIdentifier(child.getText());
            if (uniqueNames.contains(colAlias)) {
              // Column aliases defined by query for lateral view output are duplicated
              throw new SemanticException(ErrorMsg.COLUMN_ALIAS_ALREADY_EXISTS.getMsg(colAlias));
            }
            columnAliases.add(colAlias);
            uniqueNames.add(colAlias);
        }
      }
      if (tableAlias == null) {
        // Parser enforces that table alias is added, but check again
        throw new SemanticException("Alias should be specified LVJ");
      }
      if (!columnAliases.isEmpty() &&
              columnAliases.size() != valuesRowType.getFieldCount()) {
        // Number of columns in the aliases does not match with number of columns
        // generated by the lateral view
        throw new SemanticException(ErrorMsg.UDTF_ALIAS_MISMATCH.getMsg());
      }
      if (columnAliases.isEmpty()) {
        // Auto-generate column aliases
        for (int i = 0; i < valuesRowType.getFieldCount(); i++) {
          columnAliases.add(SemanticAnalyzer.getColumnInternalName(i));
        }
      }
      ListTypeInfo listTypeInfo = (ListTypeInfo) TypeConverter.convert(valuesExpr.getType()); // Array should have ListTypeInfo
      StructTypeInfo typeInfos = (StructTypeInfo) listTypeInfo.getListElementTypeInfo(); // Within the list, we extract types
      for (int i = 0, j = 0; i < columnAliases.size(); i++) {
        String internalColName;
        do {
          internalColName = SemanticAnalyzer.getColumnInternalName(j++);
        } while (inputRR.getPosition(internalColName) != -1);
        outputRR.put(tableAlias, columnAliases.get(i),
            new ColumnInfo(internalColName,  typeInfos.getAllStructFieldTypeInfos().get(i),
                tableAlias, false));
      }
      this.relToHiveColNameCalcitePosMap.put(htfsRel, buildHiveToCalciteColumnMap(outputRR));
      this.relToHiveRR.put(htfsRel, outputRR);

      // 4) Return new operator
      return htfsRel;
    }

    private boolean genSubQueryRelNode(QB qb, ASTNode node, RelNode srcRel, boolean forHavingClause,
                                       Map<ASTNode, RelNode> subQueryToRelNode) throws CalciteSubquerySemanticException {

      Set<ASTNode> corrScalarQueriesWithAgg = new HashSet<ASTNode>();
      boolean isSubQuery = false;
      try {
        Deque<ASTNode> stack = new ArrayDeque<ASTNode>();
        stack.push(node);

        while (!stack.isEmpty()) {
          ASTNode next = stack.pop();

          switch (next.getType()) {
          case HiveParser.TOK_SUBQUERY_EXPR:

            //disallow subqueries which HIVE doesn't currently support
            SubQueryUtils.subqueryRestrictionCheck(qb, next, srcRel, forHavingClause,
                corrScalarQueriesWithAgg, ctx, this.relToHiveRR);

            String sbQueryAlias = "sq_" + qb.incrNumSubQueryPredicates();
            QB qbSQ = new QB(qb.getId(), sbQueryAlias, true);
            qbSQ.setInsideView(qb.isInsideView());
            Phase1Ctx ctx1 = initPhase1Ctx();
            doPhase1((ASTNode) next.getChild(1), qbSQ, ctx1, null);
            getMetaData(qbSQ);
            this.subqueryId++;
            RelNode subQueryRelNode =
                genLogicalPlan(qbSQ, false, relToHiveColNameCalcitePosMap.get(srcRel), relToHiveRR.get(srcRel));
            subQueryToRelNode.put(next, subQueryRelNode);
            //keep track of subqueries which are scalar, correlated and contains aggregate
            // subquery expression. This will later be special cased in Subquery remove rule
            // for correlated scalar queries with aggregate we have take care of the case where
            // inner aggregate happens on empty result
            if (corrScalarQueriesWithAgg.contains(next)) {
              corrScalarRexSQWithAgg.add(subQueryRelNode);
            }
            isSubQuery = true;
            break;
          default:
            int childCount = next.getChildCount();
            for (int i = childCount - 1; i >= 0; i--) {
              stack.push((ASTNode) next.getChild(i));
            }
          }
        }
      } catch (SemanticException e) {
        throw new CalciteSubquerySemanticException(e.getMessage());
      }
      if (isSubQuery) {
        // since subqueries will later be rewritten into JOINs we want join reordering logic to trigger
        profilesCBO.add(ExtendedCBOProfile.JOIN_REORDERING);
      }
      return isSubQuery;
    }

    private RelNode genFilterRelNode(QB qb, ASTNode searchCond, RelNode srcRel,
        ImmutableMap<String, Integer> outerNameToPosMap, RowResolver outerRR, boolean forHavingClause)
        throws SemanticException {
      final Map<ASTNode, RelNode> subQueryToRelNode = new HashMap<>();
      boolean isSubQuery = genSubQueryRelNode(qb, searchCond, srcRel, forHavingClause, subQueryToRelNode);
      if(isSubQuery) {
        RexNode filterExpression = genRexNode(searchCond, relToHiveRR.get(srcRel),
                outerRR, subQueryToRelNode, forHavingClause, cluster.getRexBuilder());

        ImmutableMap<String, Integer> hiveColNameCalcitePosMap = this.relToHiveColNameCalcitePosMap
            .get(srcRel);
        filterExpression = new CorrelationConverter(
            new InputContext(srcRel.getRowType(), hiveColNameCalcitePosMap, relToHiveRR.get(srcRel)),
            outerNameToPosMap, outerRR, subqueryId).apply(filterExpression);

        RelNode filterRel = new HiveFilter(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
                srcRel, filterExpression);
        relToHiveColNameCalcitePosMap.put(filterRel, relToHiveColNameCalcitePosMap.get(srcRel));
        relToHiveRR.put(filterRel, relToHiveRR.get(srcRel));
        return filterRel;
      } else {
        return genFilterRelNode(searchCond, srcRel, outerNameToPosMap, outerRR, forHavingClause);
      }
    }

    private RelNode genFilterLogicalPlan(QB qb, RelNode srcRel, ImmutableMap<String, Integer> outerNameToPosMap,
        RowResolver outerRR, boolean forHavingClause) throws SemanticException {
      RelNode filterRel = null;

      Iterator<ASTNode> whereClauseIterator = getQBParseInfo(qb).getDestToWhereExpr().values()
          .iterator();
      if (whereClauseIterator.hasNext()) {
        filterRel = genFilterRelNode(qb, (ASTNode) whereClauseIterator.next().getChild(0), srcRel,
            outerNameToPosMap, outerRR, forHavingClause);
      }

      return filterRel;
    }

    /**
     * This method creates a HiveFilter containing a filter expression to enforce constraints.
     * Constraints to check: not null, check
     * The return value is the pair of Constraint HiveFilter and the corresponding RowResolver
     * or null if the target has no constraint defined or all of them are disabled.
     */
    private Pair<RelNode, RowResolver> genConstraintFilterLogicalPlan(
        QB qb, RelNode srcRel, ImmutableMap<String, Integer> outerNameToPosMap, RowResolver outerRR)
        throws SemanticException {
      if (qb.getIsQuery()) {
        return null;
      }

      String dest = qb.getParseInfo().getClauseNames().iterator().next();
      if (!updating(dest)) {
        return null;
      }

      RowResolver inputRR = relToHiveRR.get(srcRel);
      RexNode constraintUDF = RexNodeTypeCheck.genConstraintsExpr(
          conf, cluster.getRexBuilder(), getTargetTable(qb, dest), updating(dest), inputRR);
      if (constraintUDF == null) {
        return null;
      }

      RelNode constraintRel = genFilterRelNode(constraintUDF, srcRel, outerNameToPosMap, outerRR);
      return new Pair<>(constraintRel, inputRR);
    }

    private AggregateCall convertGBAgg(AggregateInfo agg, List<RexNode> gbChildProjLst,
        HashMap<String, Integer> rexNodeToPosMap, Integer childProjLstIndx) throws SemanticException {
      // 1. Get agg fn ret type in Calcite
      RelDataType aggFnRetType = TypeConverter.convert(agg.getReturnType(),
          this.cluster.getTypeFactory());

      // 2. Convert Agg Fn args and type of args to Calcite
      List<Integer> argList = new ArrayList<>();
      ImmutableList.Builder<RelDataType> aggArgRelDTBldr = ImmutableList.builder();
      for (RexNode rexNd : agg.getParameters()) {
        Integer inputIndx = rexNodeToPosMap.get(rexNd.toString());
        if (inputIndx == null) {
          gbChildProjLst.add(rexNd);
          rexNodeToPosMap.put(rexNd.toString(), childProjLstIndx);
          inputIndx = childProjLstIndx;
          childProjLstIndx++;
        }
        argList.add(inputIndx);

        aggArgRelDTBldr.add(rexNd.getType());
      }

      // 3. Get Aggregation FN from Calcite given name, ret type and input arg
      // type
      final SqlAggFunction aggregation = SqlFunctionConverter.getCalciteAggFn(agg.getAggregateName(), agg.isDistinct(),
          aggArgRelDTBldr.build(), aggFnRetType);

      return new AggregateCall(aggregation, agg.isDistinct(), argList, aggFnRetType, null);
    }

    private RelNode genGBRelNode(List<RexNode> gbExprs, List<AggregateInfo> aggInfoLst,
        List<Long> groupSets, RelNode srcRel) throws SemanticException {
      final boolean hasGroupSets = groupSets != null && !groupSets.isEmpty();
      final List<RexNode> gbChildProjLst = Lists.newArrayList();
      final HashMap<String, Integer> rexNodeToPosMap = new HashMap<>();
      final List<Integer> groupSetPositions = Lists.newArrayList();
      Integer gbIndx = 0;
      for (RexNode gbExpr : gbExprs) {
        gbChildProjLst.add(gbExpr);
        groupSetPositions.add(gbIndx);
        rexNodeToPosMap.put(gbExpr.toString(), gbIndx);
        gbIndx++;
      }
      final ImmutableBitSet groupSet = ImmutableBitSet.of(groupSetPositions);

      // Grouping sets: we need to transform them into ImmutableBitSet
      // objects for Calcite
      List<ImmutableBitSet> transformedGroupSets = null;
      if(hasGroupSets) {
        Set<ImmutableBitSet> setTransformedGroupSets =
                new HashSet<>(groupSets.size());
        for(long val: groupSets) {
          setTransformedGroupSets.add(convert(val, groupSet.cardinality()));
        }
        // Calcite expects the grouping sets sorted and without duplicates
        transformedGroupSets = new ArrayList<>(setTransformedGroupSets);
        Collections.sort(transformedGroupSets, ImmutableBitSet.COMPARATOR);
      }

      List<AggregateCall> aggregateCalls = Lists.newArrayList();
      for (AggregateInfo agg : aggInfoLst) {
        aggregateCalls.add(
            convertGBAgg(agg, gbChildProjLst, rexNodeToPosMap, gbChildProjLst.size()));
      }
      if (hasGroupSets) {
        // Create GroupingID column
        AggregateCall aggCall = AggregateCall.create(HiveGroupingID.INSTANCE,
                false, new ImmutableList.Builder<Integer>().build(), -1,
                this.cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT),
                HiveGroupingID.INSTANCE.getName());
        aggregateCalls.add(aggCall);
      }

      if (gbChildProjLst.isEmpty()) {
        // This will happen for count(*), in such cases we arbitarily pick
        // first element from srcRel
        gbChildProjLst.add(this.cluster.getRexBuilder().makeInputRef(srcRel, 0));
      }

      // Create input project fixing up nullability of inputs
      RelNode gbInputRel = HiveProject.create(
          srcRel,
          HiveCalciteUtil.fixNullability(cluster.getRexBuilder(), gbChildProjLst, RelOptUtil.getFieldTypeList(srcRel.getRowType())),
          null);

      HiveRelNode aggregateRel = new HiveAggregate(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
            gbInputRel, groupSet, transformedGroupSets, aggregateCalls);

      return aggregateRel;
    }

    /* This method returns the flip big-endian representation of value */
    private ImmutableBitSet convert(long value, int length) {
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
        String tab_alias = unescapeIdentifier(gByExpr.getChild(0).getChild(0)
            .getText().toLowerCase());
        String col_alias = unescapeIdentifier(gByExpr.getChild(1).getText().toLowerCase());
        gByRR.put(tab_alias, col_alias, colInfo);
      } else if (gByExpr.getType() == HiveParser.TOK_TABLE_OR_COL) {
        String col_alias = unescapeIdentifier(gByExpr.getChild(0).getText().toLowerCase());
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
        RowResolver groupByInputRowResolver, ASTNode grpbyExpr, RexNode grpbyExprNDesc,
        List<RexNode> gbExprNDescLst, List<String> outputColumnNames) {
      int i = gbExprNDescLst.size();
      String field = SemanticAnalyzer.getColumnInternalName(i);
      outputColumnNames.add(field);
      gbExprNDescLst.add(grpbyExprNDesc);

      ColumnInfo oColInfo = new ColumnInfo(field, TypeConverter.convert(grpbyExprNDesc.getType()), null, false);
      groupByOutputRowResolver.putExpression(grpbyExpr, oColInfo);

      addAlternateGByKeyMappings(grpbyExpr, oColInfo, groupByInputRowResolver,
          groupByOutputRowResolver);
    }

    private AggregateInfo getHiveAggInfo(ASTNode aggAst, int aggFnLstArgIndx, RowResolver inputRR)
        throws SemanticException {
      List<RexNode> aggParameters = new ArrayList<>();
      for (int i = 1; i <= aggFnLstArgIndx; i++) {
        RexNode parameterExpr = genRexNode(
            (ASTNode) aggAst.getChild(i), inputRR, cluster.getRexBuilder());
        aggParameters.add(parameterExpr);
      }
      boolean isDistinct = aggAst.getType() == HiveParser.TOK_FUNCTIONDI;
      boolean isAllColumns = aggAst.getType() == HiveParser.TOK_FUNCTIONSTAR;
      String aggName = unescapeIdentifier(aggAst.getChild(0).getText());

      AggregateInfo aInfo = functionHelper.getWindowAggregateFunctionInfo(
          isDistinct, isAllColumns, aggName, aggParameters);

      // If that did not work, try GenericUDF translation
      if (aInfo == null) {
        TypeCheckCtx tcCtx = new TypeCheckCtx(inputRR, cluster.getRexBuilder());
        // We allow stateful functions in the SELECT list (but nowhere else)
        tcCtx.setAllowStatefulFunctions(true);
        tcCtx.setAllowDistinctFunctions(false);
        RexNode exp = genRexNode((ASTNode) aggAst.getChild(0), inputRR, tcCtx);
        aInfo = new AggregateInfo(
            aggParameters, TypeConverter.convert(exp.getType()), aggName, isDistinct);
      }

      return aInfo;
    }

    /**
     * Generate a group by plan.
     */
    private RelNode genGBLogicalPlan(QB qb, RelNode srcRel) throws SemanticException {
      RelNode groupByRel = null;
      QBParseInfo qbp = getQBParseInfo(qb);

      // 1. Gather GB Expressions (AST) (GB + Aggregations)
      // NOTE: Multi Insert is not supported
      String destClauseName = qbp.getClauseNames().iterator().next();
      // Check and transform group by *. This will only happen for select distinct *.
      // Here the "genSelectPlan" is being leveraged.
      // The main benefits are (1) remove virtual columns that should
      // not be included in the group by; (2) add the fully qualified column names to unParseTranslator
      // so that view is supported. The drawback is that an additional SEL op is added. If it is
      // not necessary, it will be removed by NonBlockingOpDeDupProc Optimizer because it will match
      // SEL%SEL% rule.
      ASTNode selExprList = qb.getParseInfo().getSelForClause(destClauseName);
      SubQueryUtils.checkForTopLevelSubqueries(selExprList);
      if (selExprList.getToken().getType() == HiveParser.TOK_SELECTDI
          && selExprList.getChildCount() == 1 && selExprList.getChild(0).getChildCount() == 1) {
        ASTNode node = (ASTNode) selExprList.getChild(0).getChild(0);
        if (node.getToken().getType() == HiveParser.TOK_ALLCOLREF) {
          // As we said before, here we use genSelectLogicalPlan to rewrite AllColRef
          srcRel = genSelectLogicalPlan(qb, srcRel, srcRel, null, null, true).getKey();
          RowResolver rr = relToHiveRR.get(srcRel);
          qbp.setSelExprForClause(destClauseName, genSelectDIAST(rr));
        }
      }

      // Select DISTINCT + windowing; GBy handled by genSelectForWindowing
      if (selExprList.getToken().getType() == HiveParser.TOK_SELECTDI &&
              !qb.getAllWindowingSpecs().isEmpty()) {
        return null;
      }

      List<ASTNode> groupByNodes = getGroupByForClause(qbp, destClauseName);
      Map<String, ASTNode> aggregationTrees = qbp.getAggregationExprsForClause(destClauseName);
      boolean hasGrpByAstExprs = groupByNodes != null && !groupByNodes.isEmpty();
      boolean hasAggregationTrees = aggregationTrees != null && !aggregationTrees.isEmpty();

      final boolean cubeRollupGrpSetPresent = (!qbp.getDestRollups().isEmpty()
          || !qbp.getDestGroupingSets().isEmpty() || !qbp.getDestCubes().isEmpty());

      // 2. Sanity check
      if (conf.getBoolVar(HiveConf.ConfVars.HIVEGROUPBYSKEW)
          && qbp.getDistinctFuncExprsForClause(destClauseName).size() > 1) {
        throw new SemanticException(ErrorMsg.UNSUPPORTED_MULTIPLE_DISTINCTS.getMsg());
      }
      if (cubeRollupGrpSetPresent) {
        if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEMAPSIDEAGGREGATE)) {
          throw new SemanticException(ErrorMsg.HIVE_GROUPING_SETS_AGGR_NOMAPAGGR.getMsg());
        }

        if (conf.getBoolVar(HiveConf.ConfVars.HIVEGROUPBYSKEW)) {
          checkExpressionsForGroupingSet(groupByNodes,
              qb.getParseInfo().getDistinctFuncExprsForClause(destClauseName),
              aggregationTrees, relToHiveRR.get(srcRel));

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
        List<RexNode> groupByExpressions = new ArrayList<>();
        List<String> outputColumnNames = new ArrayList<>();

        // 3. Input, Output Row Resolvers
        RowResolver groupByInputRowResolver = this.relToHiveRR.get(srcRel);
        RowResolver groupByOutputRowResolver = new RowResolver();
        groupByOutputRowResolver.setIsExprResolver(true);

        if (hasGrpByAstExprs) {
          // 4. Construct GB Keys (ExprNode)
          for (int i = 0; i < groupByNodes.size(); ++i) {
            ASTNode groupByNode = groupByNodes.get(i);
            Map<ASTNode, RexNode> astToRexNodeMap = genAllRexNode(
                groupByNode, groupByInputRowResolver, cluster.getRexBuilder());
            RexNode groupByExpression = astToRexNodeMap.get(groupByNode);
            if (groupByExpression == null) {
              throw new CalciteSemanticException("Invalid Column Reference: " + groupByNode.dump(),
                  UnsupportedFeature.Invalid_column_reference);
            }

            addToGBExpr(groupByOutputRowResolver, groupByInputRowResolver, groupByNode,
                groupByExpression, groupByExpressions, outputColumnNames);
          }
        }

        // 5. GroupingSets, Cube, Rollup
        int groupingColsSize = groupByExpressions.size();
        List<Long> groupingSets = null;
        if (cubeRollupGrpSetPresent) {
          groupingSets = getGroupByGroupingSetsForClause(qbp, destClauseName).getRight();
        }

        // 6. Construct aggregation function Info
        ArrayList<AggregateInfo> aggregations = new ArrayList<AggregateInfo>();
        if (hasAggregationTrees) {
          assert (aggregationTrees != null);
          for (ASTNode value : aggregationTrees.values()) {
            // 6.1 Determine type of UDAF
            // This is the GenericUDAF name
            String aggName = unescapeIdentifier(value.getChild(0).getText());
            boolean isDistinct = value.getType() == HiveParser.TOK_FUNCTIONDI;
            boolean isAllColumns = value.getType() == HiveParser.TOK_FUNCTIONSTAR;

            // 6.2 Convert UDAF Params to ExprNodeDesc
            List<RexNode> aggParameters = new ArrayList<>();
            for (int i = 1; i < value.getChildCount(); i++) {
              RexNode parameterExpr = genRexNode(
                  (ASTNode) value.getChild(i), groupByInputRowResolver, cluster.getRexBuilder());
              aggParameters.add(parameterExpr);
            }

            AggregateInfo aInfo = functionHelper.getAggregateFunctionInfo(
              isDistinct, isAllColumns, aggName, aggParameters);
            aggregations.add(aInfo);
            String field = getColumnInternalName(groupingColsSize + aggregations.size() - 1);
            outputColumnNames.add(field);
            groupByOutputRowResolver.putExpression(value,
                new ColumnInfo(field, aInfo.getReturnType(), "", false));
          }
        }

        // 7. If GroupingSets, Cube, Rollup were used, we account grouping__id
        if(groupingSets != null && !groupingSets.isEmpty()) {
          String field = getColumnInternalName(groupingColsSize + aggregations.size());
          outputColumnNames.add(field);
          groupByOutputRowResolver.put(null, VirtualColumn.GROUPINGID.getName(),
                  new ColumnInfo(
                          field,
                          VirtualColumn.GROUPINGID.getTypeInfo(),
                          null,
                          true));
        }

        // 8. We create the group_by operator
        groupByRel = genGBRelNode(groupByExpressions, aggregations, groupingSets, srcRel);
        relToHiveColNameCalcitePosMap.put(groupByRel, buildHiveToCalciteColumnMap(groupByOutputRowResolver));
        relToHiveRR.put(groupByRel, groupByOutputRowResolver);
      }

      return groupByRel;
    }

    /**
     * Generate OB RelNode and input Select RelNode that should be used to
     * introduce top constraining Project. If Input select RelNode is not
     * present then don't introduce top constraining select.
     *
     * @param qb
     * @param selPair
     * @param outermostOB
     * @return RelNode OB RelNode
     * @throws SemanticException
     */
    private RelNode genOBLogicalPlan(QB qb, Pair<RelNode, RowResolver> selPair,
        boolean outermostOB) throws SemanticException {
      QBParseInfo qbp = getQBParseInfo(qb);
      String dest = qbp.getClauseNames().iterator().next();
      ASTNode obAST = qbp.getOrderByForClause(dest);

      if (obAST == null) {
        return null;
      }

      // 1. OB Expr sanity test
      // in strict mode, in the presence of order by, limit must be
      // specified
      Integer limit = qb.getParseInfo().getDestLimit(dest);
      if (limit == null) {
        String error = StrictChecks.checkNoLimit(conf);
        if (error != null) {
          throw new SemanticException(SemanticAnalyzer.generateErrorMessage(obAST, error));
        }
      }

      OBLogicalPlanGenState obLogicalPlanGenState = beginGenOBLogicalPlan(obAST, selPair, outermostOB);

      // 4. Construct SortRel
      RelTraitSet traitSet = cluster.traitSetOf(HiveRelNode.CONVENTION);
      RelCollation canonizedCollation = traitSet.canonize(
              RelCollationImpl.of(obLogicalPlanGenState.getFieldCollation()));
      RelNode sortRel = new HiveSortLimit(
              cluster, traitSet, obLogicalPlanGenState.getObInputRel(), canonizedCollation, null, null);

      return endGenOBLogicalPlan(obLogicalPlanGenState, sortRel);
    }

    private RelNode genSBLogicalPlan(QB qb, Pair<RelNode, RowResolver> selPair,
        boolean outermostOB) throws SemanticException {
      QBParseInfo qbp = getQBParseInfo(qb);
      String dest = qbp.getClauseNames().iterator().next();
      ASTNode sbAST = qbp.getSortByForClause(dest);

      if (sbAST == null) {
        return null;
      }

      OBLogicalPlanGenState obLogicalPlanGenState = beginGenOBLogicalPlan(sbAST, selPair, outermostOB);

      // 4. Construct SortRel
      RelTraitSet traitSet = cluster.traitSetOf(HiveRelNode.CONVENTION);
      RelCollation canonizedCollation =
              traitSet.canonize(RelCollationImpl.of(obLogicalPlanGenState.getFieldCollation()));
      List<Integer> joinKeyPositions = new ArrayList<>(canonizedCollation.getFieldCollations().size());
      ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
      for (RelFieldCollation relFieldCollation : canonizedCollation.getFieldCollations()) {
        int index = relFieldCollation.getFieldIndex();
        joinKeyPositions.add(index);
        builder.add(cluster.getRexBuilder().makeInputRef(obLogicalPlanGenState.getObInputRel(), index));
      }

      RelNode sortRel = HiveSortExchange.create(
                  obLogicalPlanGenState.getObInputRel(),
                  // In case of SORT BY we do not need Distribution
                  // but the instance RelDistributions.ANY can not be used here because
                  // org.apache.calcite.rel.core.Exchange has
                  // assert distribution != RelDistributions.ANY;
                  new HiveRelDistribution(RelDistribution.Type.ANY, RelDistributions.ANY.getKeys()),
              canonizedCollation,
              builder.build());

      return endGenOBLogicalPlan(obLogicalPlanGenState, sortRel);
    }

    // - Walk through OB exprs and extract field collations and additional virtual columns needed
    // - Add Child Project Rel if needed,
    // - Generate Output RR, input Sel Rel for top constraining Sel
    private OBLogicalPlanGenState beginGenOBLogicalPlan(
        ASTNode obAST, Pair<RelNode, RowResolver> selPair, boolean outermostOB) throws SemanticException {
      // selPair.getKey() is the operator right before OB
      // selPair.getValue() is RR which only contains columns needed in result
      // set. Extra columns needed by order by will be absent from it.
      RelNode srcRel = selPair.getKey();
      RowResolver selectOutputRR = selPair.getValue();

      // 2. Walk through OB exprs and extract field collations and additional
      // virtual columns needed
      final List<RexNode> newVCLst = new ArrayList<>();
      final List<RelFieldCollation> fieldCollations = Lists.newArrayList();
      int fieldIndex = 0;

      List<Node> obASTExprLst = obAST.getChildren();
      List<Pair<ASTNode, TypeInfo>> vcASTTypePairs = new ArrayList<>();
      RowResolver inputRR = relToHiveRR.get(srcRel);
      RowResolver outputRR = new RowResolver();

      int srcRelRecordSz = srcRel.getRowType().getFieldCount();

      for (int i = 0; i < obASTExprLst.size(); i++) {
        // 2.1 Convert AST Expr to ExprNode
        ASTNode orderByNode = (ASTNode) obASTExprLst.get(i);
        ASTNode nullObASTExpr = (ASTNode) orderByNode.getChild(0);
        ASTNode ref = (ASTNode) nullObASTExpr.getChild(0);

        boolean isBothByPos = HiveConf.getBoolVar(conf, ConfVars.HIVE_GROUPBY_ORDERBY_POSITION_ALIAS);
        boolean isObyByPos = isBothByPos
                || HiveConf.getBoolVar(conf, ConfVars.HIVE_ORDERBY_POSITION_ALIAS);
        // replace each of the position alias in ORDERBY with the actual column
        if (ref != null && ref.getToken().getType() == HiveParser.Number) {
          if (isObyByPos) {
            fieldIndex = getFieldIndexFromColumnNumber(selectOutputRR, ref);
          } else { // if not using position alias and it is a number.
            LOG.warn("Using constant number "
                    + ref.getText()
                    + " in order by. If you try to use position alias when hive.orderby.position.alias is false, " +
                    "the position alias will be ignored.");
          }
        } else {
          // 2.2 Convert ExprNode to RexNode
          RexNode orderByExpression = getOrderByExpression(selectOutputRR, inputRR, orderByNode, ref);

          // 2.3 Determine the index of ob expr in child schema
          // NOTE: Calcite can not take compound exprs in OB without it being
          // present in the child (& hence we add a child Project Rel)
          if (orderByExpression instanceof RexInputRef) {
            fieldIndex = ((RexInputRef) orderByExpression).getIndex();
          } else {
            fieldIndex = srcRelRecordSz + newVCLst.size();
            newVCLst.add(orderByExpression);
            vcASTTypePairs.add(new Pair<>(ref, TypeConverter.convert(orderByExpression.getType())));
          }
        }

        // 2.4 Determine the Direction of order by
        RelFieldCollation.Direction order = RelFieldCollation.Direction.DESCENDING;
        if (orderByNode.getType() == HiveParser.TOK_TABSORTCOLNAMEASC) {
          order = RelFieldCollation.Direction.ASCENDING;
        }
        RelFieldCollation.NullDirection nullOrder;
        if (nullObASTExpr.getType() == HiveParser.TOK_NULLS_FIRST) {
          nullOrder = RelFieldCollation.NullDirection.FIRST;
        } else if (nullObASTExpr.getType() == HiveParser.TOK_NULLS_LAST) {
          nullOrder = RelFieldCollation.NullDirection.LAST;
        } else {
          throw new SemanticException("Unexpected null ordering option: "
                  + nullObASTExpr.getType());
        }

        // 2.5 Add to field collations
        fieldCollations.add(new RelFieldCollation(fieldIndex, order, nullOrder));
      }

      // 3. Add Child Project Rel if needed, Generate Output RR, input Sel Rel
      // for top constraining Sel
      RelNode obInputRel = srcRel;
      if (!newVCLst.isEmpty()) {
        List<RexNode> originalInputRefs = toRexNodeList(srcRel);
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
        }
      } else {
        if (!RowResolver.add(outputRR, inputRR)) {
          throw new CalciteSemanticException(
                  "Duplicates detected when adding columns to RR: see previous message",
                  UnsupportedFeature.Duplicates_in_RR);
        }
      }
      return new OBLogicalPlanGenState(obInputRel, fieldCollations, selectOutputRR, outputRR, srcRel);
    }

    private RexNode getOrderByExpression(
        RowResolver selectOutputRR, RowResolver inputRR, ASTNode orderByNode, ASTNode ref)
        throws SemanticException {
      // first try to get it from select
      // in case of udtf, selectOutputRR may be null.
      RexNode orderByExpression = null;
      if (selectOutputRR != null) {
        try {
          Map<ASTNode, RexNode> astToExprNDescMap = genAllRexNode(ref, selectOutputRR, cluster.getRexBuilder());
          orderByExpression = astToExprNDescMap.get(ref);
        } catch (SemanticException ex) {
          // we can tolerate this as this is the previous behavior
          LOG.debug("Can not find column in " + ref.getText() + ". The error msg is "
                  + ex.getMessage());
        }
      }
      // then try to get it from all
      if (orderByExpression == null) {
        Map<ASTNode, RexNode> astToExprNDescMap = genAllRexNode(ref, inputRR, cluster.getRexBuilder());
        orderByExpression = astToExprNDescMap.get(ref);
      }
      if (orderByExpression == null) {
        throw new SemanticException("Invalid order by expression: " + orderByNode.toString());
      }
      return orderByExpression;
    }

    // SELECT a, b FROM t ORDER BY 1
    private int getFieldIndexFromColumnNumber(RowResolver selectOutputRR, ASTNode ref) throws SemanticException {
      int fieldIndex;
      int pos = Integer.parseInt(ref.getText());
      if (pos > 0 && pos <= selectOutputRR.getColumnInfos().size()) {
        // fieldIndex becomes so simple
        // Note that pos starts from 1 while fieldIndex starts from 0;
        fieldIndex = pos - 1;
      } else {
        throw new SemanticException(
                ErrorMsg.INVALID_POSITION_ALIAS_IN_ORDERBY.getMsg("Position alias: " + pos
                        + " does not exist\n" + "The Select List is indexed from 1 to "
                        + selectOutputRR.getColumnInfos().size()));
      }
      return fieldIndex;
    }

    private List<RexNode> toRexNodeList(RelNode srcRel) {
      return srcRel.getRowType().getFieldList().stream()
              .map(input -> new RexInputRef(input.getIndex(), input.getType()))
              .collect(Collectors.toList());
    }

    // 5. Update RR maps
    // NOTE: Output RR for SortRel is considered same as its input; we may
    // end up not using VC that is present in sort rel. Also note that
    // rowtype of sortrel is the type of it child; if child happens to be
    // synthetic project that we introduced then that projectrel would
    // contain the vc.
    public RelNode endGenOBLogicalPlan(OBLogicalPlanGenState obLogicalPlanGenState, RelNode sortRel)
            throws CalciteSemanticException {

      ImmutableMap<String, Integer> hiveColNameCalcitePosMap =
              buildHiveToCalciteColumnMap(obLogicalPlanGenState.getOutputRR());
      relToHiveRR.put(sortRel, obLogicalPlanGenState.getOutputRR());
      relToHiveColNameCalcitePosMap.put(sortRel, hiveColNameCalcitePosMap);

      if (obLogicalPlanGenState.getSelectOutputRR() != null) {
        List<RexNode> originalInputRefs = toRexNodeList(obLogicalPlanGenState.getSrcRel());
        List<RexNode> selectedRefs = Lists.newArrayList();
        for (int index = 0; index < obLogicalPlanGenState.getSelectOutputRR().getColumnInfos().size(); index++) {
          selectedRefs.add(originalInputRefs.get(index));
        }
        // We need to add select since order by schema may have more columns than result schema.
        return genSelectRelNode(selectedRefs, obLogicalPlanGenState.getSelectOutputRR(), sortRel);
      } else {
        return sortRel;
      }
    }

    private RelNode genLimitLogicalPlan(QB qb, RelNode srcRel) throws SemanticException {
      HiveRelNode sortRel = null;
      QBParseInfo qbp = getQBParseInfo(qb);
      SimpleEntry<Integer,Integer> entry =
          qbp.getDestToLimit().get(qbp.getClauseNames().iterator().next());
      Integer offset = (entry == null) ? null : entry.getKey();
      Integer fetch = (entry == null) ? null : entry.getValue();

      if (fetch != null) {
        RexNode offsetRN = (offset == null || offset == 0) ?
            null : cluster.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(offset));
        RexNode fetchRN = cluster.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(fetch));
        RelTraitSet traitSet = cluster.traitSetOf(HiveRelNode.CONVENTION);
        RelCollation canonizedCollation = traitSet.canonize(RelCollations.EMPTY);
        sortRel = new HiveSortLimit(cluster, traitSet, srcRel, canonizedCollation, offsetRN, fetchRN);

        RowResolver inputRR = relToHiveRR.get(srcRel);
        RowResolver outputRR = inputRR.duplicate();
        ImmutableMap<String, Integer> hiveColNameCalcitePosMap = buildHiveToCalciteColumnMap(outputRR);
        relToHiveRR.put(sortRel, outputRR);
        relToHiveColNameCalcitePosMap.put(sortRel, hiveColNameCalcitePosMap);
      }

      return sortRel;
    }

    private List<RexNode> getPartitionKeys(PartitionSpec ps,
        RowResolver inputRR) throws SemanticException {
      List<RexNode> pKeys = new ArrayList<>();
      if (ps != null) {
        List<PartitionExpression> pExprs = ps.getExpressions();
        for (PartitionExpression pExpr : pExprs) {
          TypeCheckCtx tcCtx = new TypeCheckCtx(inputRR, cluster.getRexBuilder());
          tcCtx.setAllowStatefulFunctions(true);
          RexNode exp = genRexNode(pExpr.getExpression(), inputRR, tcCtx);
          pKeys.add(exp);
        }
      }

      return pKeys;
    }

    private List<RexFieldCollation> getOrderKeys(OrderSpec os,
        RowResolver inputRR) throws SemanticException {
      List<RexFieldCollation> oKeys = new ArrayList<>();
      if (os != null) {
        List<OrderExpression> oExprs = os.getExpressions();
        for (OrderExpression oExpr : oExprs) {
          TypeCheckCtx tcCtx = new TypeCheckCtx(inputRR, cluster.getRexBuilder());
          tcCtx.setAllowStatefulFunctions(true);
          RexNode ordExp = genRexNode(oExpr.getExpression(), inputRR, tcCtx);
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

    private RexWindowBound getBound(BoundarySpec bs) {
      RexWindowBound rwb = null;

      if (bs != null) {
        SqlParserPos pos = new SqlParserPos(1, 1);
        SqlNode amt = bs.getAmt() == 0 || bs.getAmt() == BoundarySpec.UNBOUNDED_AMOUNT
            ? null
            : SqlLiteral.createExactNumeric(String.valueOf(bs.getAmt()), new SqlParserPos(2, 2));
        RexNode amtLiteral = null;
        SqlCall sc = null;

        if (amt != null) {
          amtLiteral = cluster.getRexBuilder().makeLiteral(Integer.valueOf(bs.getAmt()),
              cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER), true);
        }

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

    private Pair<RexNode, TypeInfo> genWindowingProj(WindowExpressionSpec wExpSpec, RelNode srcRel)
        throws SemanticException {
      RexNode w = null;
      TypeInfo wHiveRetType = null;

      if (wExpSpec instanceof WindowFunctionSpec) {
        WindowFunctionSpec wFnSpec = (WindowFunctionSpec) wExpSpec;
        ASTNode windowProjAst = wFnSpec.getExpression();
        // TODO: do we need to get to child?
        int wndSpecASTIndx = getWindowSpecIndx(windowProjAst);
        // 2. Get Hive Aggregate Info
        AggregateInfo hiveAggInfo = getHiveAggInfo(windowProjAst, wndSpecASTIndx - 1,
            this.relToHiveRR.get(srcRel));

        // 3. Get Calcite Return type for Agg Fn
        wHiveRetType = hiveAggInfo.getReturnType();
        RelDataType calciteAggFnRetType = TypeConverter.convert(hiveAggInfo.getReturnType(),
            this.cluster.getTypeFactory());

        // 4. Convert Agg Fn args to Calcite
        List<RexNode> calciteAggFnArgs = hiveAggInfo.getParameters();
        Builder<RelDataType> calciteAggFnArgsTypeBldr = ImmutableList.builder();
        for (int i = 0; i < hiveAggInfo.getParameters().size(); i++) {
          calciteAggFnArgsTypeBldr.add(hiveAggInfo.getParameters().get(i).getType());
        }
        ImmutableList<RelDataType> calciteAggFnArgsType = calciteAggFnArgsTypeBldr.build();

        // 5. Get Calcite Agg Fn
        final SqlAggFunction calciteAggFn = SqlFunctionConverter.getCalciteAggFn(
            hiveAggInfo.getAggregateName(), hiveAggInfo.isDistinct(), calciteAggFnArgsType, calciteAggFnRetType);

        // 6. Translate Window spec
        RowResolver inputRR = relToHiveRR.get(srcRel);
        WindowSpec wndSpec = ((WindowFunctionSpec) wExpSpec).getWindowSpec();
        List<RexNode> partitionKeys = getPartitionKeys(wndSpec.getPartition(), inputRR);
        List<RexFieldCollation> orderKeys = getOrderKeys(wndSpec.getOrder(), inputRR);
        RexWindowBound lowerBound = getBound(wndSpec.getWindowFrame().getStart());
        RexWindowBound upperBound = getBound(wndSpec.getWindowFrame().getEnd());
        boolean isRows = wndSpec.getWindowFrame().getWindowType() == WindowType.ROWS;

        w = cluster.getRexBuilder().makeOver(calciteAggFnRetType, calciteAggFn, calciteAggFnArgs,
            partitionKeys, ImmutableList.<RexFieldCollation> copyOf(orderKeys), lowerBound,
            upperBound, isRows, true, false, hiveAggInfo.isDistinct());
      } else {
        // TODO: Convert to Semantic Exception
        throw new RuntimeException("Unsupported window Spec");
      }

      return new Pair<>(w, wHiveRetType);
    }

    private RelNode genSelectForWindowing(QB qb, RelNode srcRel, HashSet<ColumnInfo> newColumns)
        throws SemanticException {
      getQBParseInfo(qb);
      WindowingSpec wSpec = (!qb.getAllWindowingSpecs().isEmpty()) ? qb.getAllWindowingSpecs()
          .values().iterator().next() : null;
      if (wSpec == null) {
        return null;
      }
      // 1. Get valid Window Function Spec
      wSpec.validateAndMakeEffective();
      List<WindowExpressionSpec> windowExpressions = wSpec.getWindowExpressions();
      if (windowExpressions == null || windowExpressions.isEmpty()) {
        return null;
      }

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
          Pair<RexNode, TypeInfo> wtp = genWindowingProj(wExprSpec, srcRel);
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
      Set<String> colNamesSet = new HashSet<>();
      List<ColumnInfo> cInfoLst = out_rwsch.getRowSchema().getSignature();
      List<String> columnNames = new ArrayList<>();
      Map<String,String> windowToAlias = null;
      if (windowExpressions != null ) {
        windowToAlias = new HashMap<>();
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
      // names. Fix nullability
      HiveRelNode selRel = HiveProject.create(
          srcRel,
          HiveCalciteUtil.fixNullability(cluster.getRexBuilder(), calciteColLst, RelOptUtil.getFieldTypeList(srcRel.getRowType())),
          columnNames);

      // 4. Keep track of colname-to-posmap && RR for new select
      this.relToHiveColNameCalcitePosMap.put(selRel, buildHiveToCalciteColumnMap(out_rwsch));
      this.relToHiveRR.put(selRel, out_rwsch);

      return selRel;
    }

    private void setQueryHints(QB qb) throws SemanticException {
      QBParseInfo qbp = getQBParseInfo(qb);
      String selClauseName = qbp.getClauseNames().iterator().next();
      Tree selExpr0 = qbp.getSelForClause(selClauseName).getChild(0);

      if (selExpr0.getType() != HiveParser.QUERY_HINT) {
        return;
      }
      String hint = ctx.getTokenRewriteStream().toString(
          selExpr0.getTokenStartIndex(), selExpr0.getTokenStopIndex());
      LOG.debug("Handling query hints: " + hint);
      ParseDriver pd = new ParseDriver();
      try {
        ASTNode hintNode = pd.parseHint(hint);
        qbp.setHints(hintNode);
      } catch (ParseException e) {
        throw new SemanticException("failed to parse query hint: "+e.getMessage(), e);
      }
    }

    private Pair<RelNode, RowResolver> genSelectLogicalPlan(QB qb, RelNode srcRel, RelNode starSrcRel,
        ImmutableMap<String, Integer> outerNameToPosMap, RowResolver outerRR, boolean isAllColRefRewrite)
        throws SemanticException {
      Pair<RelNode, RowResolver> retNodeRR = internalGenSelectLogicalPlan(qb, srcRel, starSrcRel, outerNameToPosMap,
          outerRR, isAllColRefRewrite);

      QBParseInfo qbp = getQBParseInfo(qb);
      String selClauseName = qbp.getClauseNames().iterator().next();
      ASTNode selExprList = qbp.getSelForClause(selClauseName);
      if (isSelectDistinct(selExprList) && hasGroupBySibling(selExprList)) {
        retNodeRR = genGBSelectDistinctPlan(retNodeRR);
      }

      return retNodeRR;
    }

    /**
     * NOTE: there can only be one select caluse since we don't handle multi destination insert.
     * @param isAllColRefRewrite
     *          when it is true, it means that it is called from group by *, where we use
     *          genSelectLogicalPlan to rewrite *
     * @return RelNode: the select relnode RowResolver: i.e., originalRR, the RR after select when there is an order by.
     */
    private Pair<RelNode, RowResolver> internalGenSelectLogicalPlan(QB qb, RelNode srcRel, RelNode starSrcRel,
        ImmutableMap<String, Integer> outerNameToPosMap, RowResolver outerRR, boolean isAllColRefRewrite)
        throws SemanticException {
      // 0. Generate a Select Node for Windowing
      // Exclude the newly-generated select columns from */etc. resolution.
      HashSet<ColumnInfo> excludedColumns = new HashSet<ColumnInfo>();
      RelNode selForWindow = genSelectForWindowing(qb, srcRel, excludedColumns);
      srcRel = (selForWindow == null) ? srcRel : selForWindow;

      List<RexNode> columnList = new ArrayList<>();

      // 1. Get Select Expression List
      QBParseInfo qbp = getQBParseInfo(qb);
      String selClauseName = qbp.getClauseNames().iterator().next();
      ASTNode selExprList = qbp.getSelForClause(selClauseName);

      // make sure if there is subquery it is top level expression
      SubQueryUtils.checkForTopLevelSubqueries(selExprList);

      final boolean cubeRollupGrpSetPresent = (!qbp.getDestRollups().isEmpty()
              || !qbp.getDestGroupingSets().isEmpty() || !qbp.getDestCubes().isEmpty());

      // 2.Row resolvers for input, output
      RowResolver outputRR = new RowResolver();
      Integer pos = Integer.valueOf(0);
      // TODO: will this also fix windowing? try
      RowResolver inputRR = this.relToHiveRR.get(srcRel), starRR = inputRR;
      inputRR.setCheckForAmbiguity(true);
      if (starSrcRel != null) {
        starRR = this.relToHiveRR.get(starSrcRel);
      }

      // 3. Query Hints
      // TODO: Handle Query Hints; currently we ignore them
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
        String funcName = TypeCheckProcFactory.getFunctionText(expr, true);
        FunctionInfo fi = FunctionRegistry.getFunctionInfo(funcName);
        if (fi != null && fi.getGenericUDTF() != null) {
          LOG.debug("Find UDTF " + funcName);
          genericUDTF = fi.getGenericUDTF();
          genericUDTFName = funcName;
          if (!fi.isNative()) {
            unparseTranslator.addIdentifierTranslation((ASTNode) expr.getChild(0));
          }
          if (genericUDTF != null && exprType == HiveParser.TOK_FUNCTIONSTAR) {
            genRexNodeRegex(".*", null, (ASTNode) expr.getChild(0),
                columnList, null, inputRR, starRR, pos, outputRR, qb.getAliases(), false);
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
          String[] colRef = getColAlias(child, getAutogenColAliasPrfxLbl(),
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
          RexNode subQueryExpr = genRexNode(expr, relToHiveRR.get(srcRel),
                  outerRR, subQueryToRelNode, true, cluster.getRexBuilder());
          columnList.add(subQueryExpr);
          ColumnInfo colInfo = new ColumnInfo(SemanticAnalyzer.getColumnInternalName(pos),
              TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
                  TypeConverter.convert(subQueryExpr.getType())),
              tabAlias, false);
          if (!outputRR.putWithCheck(tabAlias, colAlias, null, colInfo)) {
            throw new CalciteSemanticException("Cannot add column to RR: " + tabAlias + "."
                    + colAlias + " => " + colInfo + " due to duplication, see previous warnings",
                    UnsupportedFeature.Duplicates_in_RR);
          }
          pos = Integer.valueOf(pos.intValue() + 1);
        } else {

          // 6.4 Build ExprNode corresponding to colums
          if (expr.getType() == HiveParser.TOK_ALLCOLREF) {
            pos = genRexNodeRegex(".*",
                expr.getChildCount() == 0 ? null : getUnescapedName((ASTNode) expr.getChild(0)).toLowerCase(),
                expr, columnList, excludedColumns, inputRR, starRR, pos, outputRR, qb.getAliases(), true);
          } else if (expr.getType() == HiveParser.TOK_TABLE_OR_COL
                  && !hasAsClause
                  && !inputRR.getIsExprResolver()
                  && isRegex(
                  unescapeIdentifier(expr.getChild(0).getText()), conf)) {
            // In case the expression is a regex COL.
            // This can only happen without AS clause
            // We don't allow this for ExprResolver - the Group By case
            pos = genRexNodeRegex(unescapeIdentifier(expr.getChild(0).getText()), null,
                expr, columnList, excludedColumns, inputRR, starRR, pos, outputRR, qb.getAliases(), true);
          } else if (expr.getType() == HiveParser.DOT
                  && expr.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
                  && inputRR.hasTableAlias(unescapeIdentifier(expr.getChild(0)
                  .getChild(0).getText().toLowerCase()))
                  && !hasAsClause
                  && !inputRR.getIsExprResolver()
                  && isRegex(
                  unescapeIdentifier(expr.getChild(1).getText()), conf)) {
            // In case the expression is TABLE.COL (col can be regex).
            // This can only happen without AS clause
            // We don't allow this for ExprResolver - the Group By case
            pos = genRexNodeRegex(
                    unescapeIdentifier(expr.getChild(1).getText()),
                    unescapeIdentifier(expr.getChild(0).getChild(0).getText().toLowerCase()),
                    expr, columnList, excludedColumns, inputRR, starRR, pos,
                    outputRR, qb.getAliases(), true);
          } else if (ParseUtils.containsTokenOfType(expr, HiveParser.TOK_FUNCTIONDI)
                  && !(srcRel instanceof HiveAggregate ||
              (srcRel.getInputs().size() == 1 && srcRel.getInput(0) instanceof HiveAggregate))) {
            // Likely a malformed query eg, select hash(distinct c1) from t1;
            throw new CalciteSemanticException("Distinct without an aggregation.",
                    UnsupportedFeature.Distinct_without_an_aggreggation);
          } else {
            // Case when this is an expression
            TypeCheckCtx tcCtx = new TypeCheckCtx(inputRR, cluster.getRexBuilder());
            // We allow stateful functions in the SELECT list (but nowhere else)
            tcCtx.setAllowStatefulFunctions(true);
            if (!qbp.getDestToGroupBy().isEmpty()) {
              // Special handling of grouping function
              expr = rewriteGroupingFunctionAST(getGroupByForClause(qbp, selClauseName), expr,
                      !cubeRollupGrpSetPresent);
            }
            RexNode expression = genRexNode(expr, inputRR, tcCtx);

            String recommended = recommendName(expression, colAlias, inputRR);
            if (recommended != null && outputRR.get(null, recommended) == null) {
              colAlias = recommended;
            }
            columnList.add(expression);

            TypeInfo typeInfo = expression.isA(SqlKind.LITERAL) ?
                TypeConverter.convertLiteralType((RexLiteral) expression) :
                TypeConverter.convert(expression.getType());
            ColumnInfo colInfo = new ColumnInfo(SemanticAnalyzer.getColumnInternalName(pos),
                TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(typeInfo),
                tabAlias, false);
            outputRR.put(tabAlias, colAlias, colInfo);

            pos = Integer.valueOf(pos.intValue() + 1);
          }
        }
      }

      // 7. For correlated queries
      ImmutableMap<String, Integer> hiveColNameCalcitePosMap =
          buildHiveColNameToInputPosMap(columnList, inputRR);
      CorrelationConverter cc = new CorrelationConverter(
          new InputContext(srcRel.getRowType(), hiveColNameCalcitePosMap, relToHiveRR.get(srcRel)),
          outerNameToPosMap, outerRR, subqueryId);
      columnList = columnList.stream()
          .map(cc::apply)
          .collect(Collectors.toList());

      // 8. Build Calcite Rel
      RelNode outputRel = null;
      if (genericUDTF != null) {
        // The basic idea for CBO support of UDTF is to treat UDTF as a special
        // project.
        // In AST return path, as we just need to generate a SEL_EXPR, we just
        // need to remember the expressions and the alias.
        // In OP return path, we need to generate a SEL and then a UDTF
        // following old semantic analyzer.
        outputRel = genUDTFPlan(genericUDTF, genericUDTFName, udtfTableAlias, udtfColAliases, qb,
            columnList, outputRR, srcRel);
      } else {
        String dest = qbp.getClauseNames().iterator().next();
        ASTNode obAST = qbp.getOrderByForClause(dest);
        ASTNode sbAST = qbp.getSortByForClause(dest);

        RowResolver originalRR = null;
        // We only support limited unselected column following by order by.
        // TODO: support unselected columns in genericUDTF and windowing functions.
        // We examine the order by in this query block and adds in column needed
        // by order by in select list.
        //
        // If DISTINCT is present, it is not possible to ORDER BY unselected
        // columns, and in fact adding all columns would change the behavior of
        // DISTINCT, so we bypass this logic.
        if ((obAST != null || sbAST != null)
            && selExprList.getToken().getType() != HiveParser.TOK_SELECTDI
            && !isAllColRefRewrite) {
          // 1. OB Expr sanity test
          // in strict mode, in the presence of order by, limit must be
          // specified
          Integer limit = qb.getParseInfo().getDestLimit(dest);
          if (limit == null) {
            String error = StrictChecks.checkNoLimit(conf);
            if (error != null) {
              throw new SemanticException(SemanticAnalyzer.generateErrorMessage(obAST, error));
            }
          }
          List<RexNode> originalInputRefs = Lists.transform(srcRel.getRowType().getFieldList(),
              new Function<RelDataTypeField, RexNode>() {
                @Override
                public RexNode apply(RelDataTypeField input) {
                  return new RexInputRef(input.getIndex(), input.getType());
                }
              });
          originalRR = outputRR.duplicate();
          for (int i = 0; i < inputRR.getColumnInfos().size(); i++) {
            ColumnInfo colInfo = new ColumnInfo(inputRR.getColumnInfos().get(i));
            String internalName = SemanticAnalyzer.getColumnInternalName(outputRR.getColumnInfos()
                .size() + i);
            colInfo.setInternalName(internalName);
            // if there is any confict, then we do not generate it in the new select
            // otherwise, we add it into the calciteColLst and generate the new select
            if (!outputRR.putWithCheck(colInfo.getTabAlias(), colInfo.getAlias(), internalName,
                colInfo)) {
              LOG.trace("Column already present in RR. skipping.");
            } else {
              columnList.add(originalInputRefs.get(i));
            }
          }
          outputRel = genSelectRelNode(columnList, outputRR, srcRel);
          // outputRel is the generated augmented select with extra unselected
          // columns, and originalRR is the original generated select
          return new Pair<RelNode, RowResolver>(outputRel, originalRR);
        } else {
          outputRel = genSelectRelNode(columnList, outputRR, srcRel);
        }
      }
      // 9. Handle select distinct as GBY if there exist windowing functions
      if (selForWindow != null && selExprList.getToken().getType() == HiveParser.TOK_SELECTDI) {
        ImmutableBitSet groupSet = ImmutableBitSet.range(outputRel.getRowType().getFieldList().size());
        outputRel = new HiveAggregate(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
              outputRel, groupSet, null, new ArrayList<AggregateCall>());
        RowResolver groupByOutputRowResolver = new RowResolver();
        for (int i = 0; i < outputRR.getColumnInfos().size(); i++) {
          ColumnInfo colInfo = outputRR.getColumnInfos().get(i);
          ColumnInfo newColInfo = new ColumnInfo(colInfo.getInternalName(),
              colInfo.getType(), colInfo.getTabAlias(), colInfo.getIsVirtualCol());
          groupByOutputRowResolver.put(colInfo.getTabAlias(), colInfo.getAlias(), newColInfo);
        }
        relToHiveColNameCalcitePosMap.put(outputRel, buildHiveToCalciteColumnMap(groupByOutputRowResolver));
        this.relToHiveRR.put(outputRel, groupByOutputRowResolver);
      }

      inputRR.setCheckForAmbiguity(false);
      return new Pair<RelNode, RowResolver>(outputRel, null);
    }

    Integer genRexNodeRegex(String colRegex, String tabAlias, ASTNode sel,
        List<RexNode> exprList, Set<ColumnInfo> excludeCols, RowResolver input,
        RowResolver colSrcRR, Integer pos, RowResolver output, List<String> aliases,
        boolean ensureUniqueCols) throws SemanticException {
      List<org.apache.commons.lang3.tuple.Pair<ColumnInfo, RowResolver>> colList = new ArrayList<>();
      Integer i = genColListRegex(colRegex, tabAlias, sel,
          colList, excludeCols, input, colSrcRR, pos, output, aliases, ensureUniqueCols);
      for (org.apache.commons.lang3.tuple.Pair<ColumnInfo, RowResolver> p : colList) {
        exprList.add(RexNodeTypeCheck.toExprNode(p.getLeft(), p.getRight(), 0, cluster.getRexBuilder()));
      }
      return i;
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

      // Create the return type info for the input columns and initialize the
      // UDTF
      StructTypeInfo type = (StructTypeInfo) TypeConverter.convert(
          functionHelper.getReturnType(
              functionHelper.getFunctionInfo(genericUDTFName),
              selectColLst));

      int numUdtfCols = type.getAllStructFieldNames().size();
      if (colAliases.isEmpty()) {
        // user did not specfied alias names, infer names from outputOI
        for (String fieldName : type.getAllStructFieldNames()) {
          colAliases.add(fieldName);
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
      List<ColumnInfo> udtfCols = new ArrayList<ColumnInfo>();

      Iterator<String> colAliasesIter = colAliases.iterator();
      for (int i = 0; i < type.getAllStructFieldTypeInfos().size(); i++) {
        final String fieldName = type.getAllStructFieldNames().get(i);
        final TypeInfo fieldTypeInfo = type.getAllStructFieldTypeInfos().get(i);

        String colAlias = colAliasesIter.next();
        assert (colAlias != null);

        // Since the UDTF operator feeds into a LVJ operator that will rename
        // all the internal names, we can just use field name from the UDTF's OI
        // as the internal name
        ColumnInfo col = new ColumnInfo(fieldName, fieldTypeInfo, outputTableAlias, false);
        udtfCols.add(col);
      }

      // Create the row resolver for this operator from the output columns
      RowResolver outputRR = new RowResolver();
      for (int i = 0; i < udtfCols.size(); i++) {
        outputRR.put(outputTableAlias, colAliases.get(i), udtfCols.get(i));
      }

      // Add the UDTFOperator to the operator DAG
      RelTraitSet traitSet = TraitsUtil.getDefaultTraitSet(cluster);

      // Build row type from field <type, name>
      RelDataType retType = TypeConverter.getType(cluster, outputRR, null);

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
      relToHiveColNameCalcitePosMap.put(udtf, buildHiveToCalciteColumnMap(outputRR));
      relToHiveRR.put(udtf, outputRR);

      return udtf;
    }

    private Pair<RelNode, RowResolver> genGBSelectDistinctPlan(Pair<RelNode, RowResolver> srcNodeRR)
        throws SemanticException {
      RelNode srcRel = srcNodeRR.left;

      RelDataType inputRT = srcRel.getRowType();
      List<Integer> groupSetPositions =
          IntStream.range(0, inputRT.getFieldCount()).boxed().collect(Collectors.toList());

      HiveAggregate distAgg = new HiveAggregate(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION), srcRel,
          ImmutableBitSet.of(groupSetPositions), null, new ArrayList<AggregateCall>());

      // This comes from genSelectLogicalPlan, must be a project assert srcRel instanceof HiveProject;
      RowResolver outputRR = srcNodeRR.right;
      if (outputRR == null) {
        outputRR = relToHiveRR.get(srcRel);
      }

      relToHiveRR.put(distAgg, outputRR);
      relToHiveColNameCalcitePosMap.put(distAgg, relToHiveColNameCalcitePosMap.get(srcRel));
      return new Pair<RelNode, RowResolver>(distAgg, outputRR);
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
      RelNode sbRel = null;
      RelNode limitRel = null;

      // First generate all the opInfos for the elements in the from clause
      Map<String, RelNode> aliasToRel = new HashMap<String, RelNode>();

      // 0. Check if we can handle the SubQuery;
      // canHandleQbForCbo returns null if the query can be handled.
      String reason = canHandleQbForCbo(queryProperties, conf, false, LOG.isDebugEnabled());
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
        qb.getMetaData().setSrcForAlias(DUMMY_TABLE, getDummyTable());
        qb.addAlias(DUMMY_TABLE);
        qb.setTabAlias(DUMMY_TABLE, DUMMY_TABLE);
        RelNode op = genTableLogicalPlan(DUMMY_TABLE, qb);
        aliasToRel.put(DUMMY_TABLE, op);

      }

      // 1.3 process join
      // 1.3.1 process hints
      setQueryHints(qb);

      // 1.3.2 process the actual join
      if (qb.getParseInfo().getJoinExpr() != null) {
        srcRel = genJoinLogicalPlan(qb.getParseInfo().getJoinExpr(), aliasToRel);
      } else {
        // If no join then there should only be either 1 TS or 1 SubQuery
        Map.Entry<String, RelNode> uniqueAliasToRel = aliasToRel.entrySet().iterator().next();
        srcRel = uniqueAliasToRel.getValue();
        // If it contains a LV
        List<ASTNode> lateralViews = getQBParseInfo(qb).getAliasToLateralViews().get(uniqueAliasToRel.getKey());
        if (lateralViews != null) {
          srcRel = genLateralViewPlans(Iterables.getLast(lateralViews), aliasToRel);
        }
      }

      // 2. Build Rel for where Clause
      filterRel = genFilterLogicalPlan(qb, srcRel, outerNameToPosMap, outerRR, false);
      srcRel = (filterRel == null) ? srcRel : filterRel;
      RelNode starSrcRel = srcRel;

      // 3. Build Rel for GB Clause
      gbRel = genGBLogicalPlan(qb, srcRel);
      srcRel = (gbRel == null) ? srcRel : gbRel;

      // 4. Build Rel for GB Having Clause
      gbHavingRel = genGBHavingLogicalPlan(qb, srcRel);
      srcRel = (gbHavingRel == null) ? srcRel : gbHavingRel;

      // 5. Build Rel for Select Clause
      Pair<RelNode, RowResolver> selPair = genSelectLogicalPlan(qb, srcRel, starSrcRel, outerNameToPosMap, outerRR, false);
      selectRel = selPair.getKey();
      srcRel = (selectRel == null) ? srcRel : selectRel;

      // Build Rel for Constraint checks
      Pair<RelNode, RowResolver> constraintPair =
          genConstraintFilterLogicalPlan(qb, srcRel, outerNameToPosMap, outerRR);
      if (constraintPair != null) {
        selPair = constraintPair;
      }

      // 6. Build Rel for OB Clause
      obRel = genOBLogicalPlan(qb, selPair, outerMostQB);
      srcRel = (obRel == null) ? srcRel : obRel;

      // 7. Build Rel for Sort By Clause
      sbRel = genSBLogicalPlan(qb, selPair, outerMostQB);
      srcRel = (sbRel == null) ? srcRel : sbRel;

      // 8. Build Rel for Limit Clause
      limitRel = genLimitLogicalPlan(qb, srcRel);
      srcRel = (limitRel == null) ? srcRel : limitRel;

      // 9. Incase this QB corresponds to subquery then modify its RR to point
      // to subquery alias.
      if (qb.getParseInfo().getAlias() != null) {
        RowResolver rr = this.relToHiveRR.get(srcRel);
        RowResolver newRR = new RowResolver();
        String alias = qb.getParseInfo().getAlias();
        List<String> targetColNames = processTableColumnNames(qb.getParseInfo().getColAliases(), alias);
        if (targetColNames.size() > rr.getColumnInfos().size()) {
          throw new SemanticException(ErrorMsg.WITH_COL_LIST_NUM_OVERFLOW, alias,
                  Integer.toString(rr.getColumnInfos().size()), Integer.toString(targetColNames.size()));
        }

        for (int i = 0; i < rr.getColumnInfos().size(); ++i) {
          ColumnInfo colInfo = rr.getColumnInfos().get(i);
          String name = colInfo.getInternalName();
          String[] tmp = rr.reverseLookup(name);
          ColumnInfo newCi = new ColumnInfo(colInfo);
          newCi.setTabAlias(alias);
          if (i < targetColNames.size()) {
            tmp[1] = targetColNames.get(i);
            newCi.setAlias(tmp[1]);
          } else if ("".equals(tmp[0]) || tmp[1] == null) {
            // ast expression is not a valid column name for table
            tmp[1] = colInfo.getInternalName();
          }
          newRR.putWithCheck(alias, tmp[1], colInfo.getInternalName(), newCi);
        }
        relToHiveRR.put(srcRel, newRR);
        relToHiveColNameCalcitePosMap.put(srcRel, buildHiveToCalciteColumnMap(newRR));
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Created Plan for Query Block " + qb.getId());
      }

      setQB(qb);
      return srcRel;
    }

    private RelNode genGBHavingLogicalPlan(QB qb, RelNode srcRel) throws SemanticException {
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
        gbFilter = genFilterRelNode(qb, targetNode, srcRel, null, null, true);
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

    private ImmutableMap<String, Integer> buildHiveToCalciteColumnMap(RowResolver rr) {
      ImmutableMap.Builder<String, Integer> b = new ImmutableMap.Builder<String, Integer>();
      for (ColumnInfo ci : rr.getRowSchema().getSignature()) {
        b.put(ci.getInternalName(), rr.getPosition(ci.getInternalName()));
      }
      return b.build();
    }

    private ImmutableMap<String, Integer> buildHiveColNameToInputPosMap(
        List<RexNode> columnList, RowResolver inputRR) {
      final ImmutableBitSet refs =
          RelOptUtil.InputFinder.bits(columnList, null);
      ImmutableMap.Builder<String, Integer> hiveColNameToInputPosMapBuilder =
          new ImmutableMap.Builder<>();
      for (int ref : refs) {
        hiveColNameToInputPosMapBuilder.put(
            inputRR.getColumnInfos().get(ref).getInternalName(), ref);
      }
      return hiveColNameToInputPosMapBuilder.build();
    }

    private QBParseInfo getQBParseInfo(QB qb) throws CalciteSemanticException {
      return qb.getParseInfo();
    }
  }

  /**
   * This class stores the partial results of Order/Sort by clause logical plan generation.
   * See {@link CalcitePlannerAction#beginGenOBLogicalPlan}, {@link CalcitePlannerAction#endGenOBLogicalPlan}
   */
  private static class OBLogicalPlanGenState {
    private final RelNode obInputRel;
    private final List<RelFieldCollation> canonizedCollation;
    private final RowResolver selectOutputRR;
    private final RowResolver outputRR;
    private final RelNode srcRel;

    OBLogicalPlanGenState(RelNode obInputRel, List<RelFieldCollation> canonizedCollation,
                                 RowResolver selectOutputRR, RowResolver outputRR, RelNode srcRel) {
      this.obInputRel = obInputRel;
      this.canonizedCollation = canonizedCollation;
      this.selectOutputRR = selectOutputRR;
      this.outputRR = outputRR;
      this.srcRel = srcRel;
    }

    public RelNode getObInputRel() {
      return obInputRel;
    }

    public List<RelFieldCollation> getFieldCollation() {
      return canonizedCollation;
    }

    public RowResolver getSelectOutputRR() {
      return selectOutputRR;
    }

    public RowResolver getOutputRR() {
      return outputRR;
    }

    public RelNode getSrcRel() {
      return srcRel;
    }
  }

  @Override
  protected Table getTableObjectByName(String tabName, boolean throwException) throws HiveException {
    String[] names = Utilities.getDbTableName(tabName);
    final String  tableName = names[1];
    final String  dbName = names[0];
    final String fullyQualName = dbName + "." + tableName;
    if (!tabNameToTabObject.containsKey(fullyQualName)) {
      Table table = db.getTable(dbName, tableName, throwException);
      if (table != null) {
        tabNameToTabObject.put(fullyQualName, table);
      }
      return table;
    }
    return tabNameToTabObject.get(fullyQualName);
  }

  RexNode genRexNode(ASTNode expr, RowResolver input,
      RowResolver outerRR, Map<ASTNode, RelNode> subqueryToRelNode,
      boolean useCaching, RexBuilder rexBuilder) throws SemanticException {
    TypeCheckCtx tcCtx = new TypeCheckCtx(input, rexBuilder, useCaching, false);
    tcCtx.setOuterRR(outerRR);
    tcCtx.setSubqueryToRelNode(subqueryToRelNode);
    return genRexNode(expr, input, tcCtx);
  }

  /**
   * Generates a Calcite {@link RexNode} for the expression with TypeCheckCtx.
   */
  RexNode genRexNode(ASTNode expr, RowResolver input, RexBuilder rexBuilder)
      throws SemanticException {
    // Since the user didn't supply a customized type-checking context,
    // use default settings.
    return genRexNode(expr, input, true, false, rexBuilder);
  }

  RexNode genRexNode(ASTNode expr, RowResolver input, boolean useCaching,
      boolean foldExpr, RexBuilder rexBuilder) throws SemanticException {
    TypeCheckCtx tcCtx = new TypeCheckCtx(input, rexBuilder, useCaching, foldExpr);
    return genRexNode(expr, input, tcCtx);
  }

  /**
   * Generates a Calcite {@link RexNode} for the expression and children of it
   * with default TypeCheckCtx.
   */
  Map<ASTNode, RexNode> genAllRexNode(ASTNode expr, RowResolver input, RexBuilder rexBuilder)
      throws SemanticException {
    TypeCheckCtx tcCtx = new TypeCheckCtx(input, rexBuilder);
    return genAllRexNode(expr, input, tcCtx);
  }

  /**
   * Returns a Calcite {@link RexNode} for the expression.
   * If it is evaluated already in previous operator, it can be retrieved from cache.
   */
  RexNode genRexNode(ASTNode expr, RowResolver input,
      TypeCheckCtx tcCtx) throws SemanticException {
    RexNode cached = null;
    if (tcCtx.isUseCaching()) {
      cached = getRexNodeCached(expr, input, tcCtx);
    }
    if (cached == null) {
      Map<ASTNode, RexNode> allExprs = genAllRexNode(expr, input, tcCtx);
      return allExprs.get(expr);
    }
    return cached;
  }

  /**
   * Find RexNode for the expression cached in the RowResolver. Returns null if not exists.
   */
  private RexNode getRexNodeCached(ASTNode node, RowResolver input,
      TypeCheckCtx tcCtx) throws SemanticException {
    ColumnInfo colInfo = input.getExpression(node);
    if (colInfo != null) {
      ASTNode source = input.getExpressionSource(node);
      if (source != null) {
        unparseTranslator.addCopyTranslation(node, source);
      }
      return RexNodeTypeCheck.toExprNode(colInfo, input, 0, tcCtx.getRexBuilder());
    }
    return null;
  }

  /**
   * Generates all of the Calcite {@link RexNode}s for the expression and children of it
   * passed in the arguments. This function uses the row resolver and the metadata information
   * that are passed as arguments to resolve the column names to internal names.
   *
   * @param expr
   *          The expression
   * @param input
   *          The row resolver
   * @param tcCtx
   *          Customized type-checking context
   * @return expression to exprNodeDesc mapping
   * @throws SemanticException Failed to evaluate expression
   */
  Map<ASTNode, RexNode> genAllRexNode(ASTNode expr, RowResolver input,
      TypeCheckCtx tcCtx) throws SemanticException {
    // Create the walker and  the rules dispatcher.
    tcCtx.setUnparseTranslator(unparseTranslator);

    Map<ASTNode, RexNode> nodeOutputs =
        RexNodeTypeCheck.genExprNode(expr, tcCtx);
    RexNode desc = nodeOutputs.get(expr);
    if (desc == null) {
      String tableOrCol = BaseSemanticAnalyzer.unescapeIdentifier(expr
          .getChild(0).getText());
      ColumnInfo colInfo = input.get(null, tableOrCol);
      String errMsg;
      if (colInfo == null && input.getIsExprResolver()){
        errMsg = ASTErrorUtils.getMsg(
            ErrorMsg.NON_KEY_EXPR_IN_GROUPBY.getMsg(), expr);
      } else {
        errMsg = tcCtx.getError();
      }
      throw new SemanticException(Optional.ofNullable(errMsg).orElse("Error in parsing "));
    }
    if (desc instanceof HiveRexExprList) {
      throw new SemanticException("TOK_ALLCOLREF is not supported in current context");
    }

    if (!unparseTranslator.isEnabled()) {
      // Not creating a view, so no need to track view expansions.
      return nodeOutputs;
    }

    List<ASTNode> fieldDescList = new ArrayList<>();

    for (Map.Entry<ASTNode, RexNode> entry : nodeOutputs.entrySet()) {
      if (!(entry.getValue() instanceof RexInputRef)) {
        // we need to translate the RexFieldAccess too, e.g., identifiers in
        // struct<>.
        if (entry.getValue() instanceof RexFieldAccess) {
          fieldDescList.add(entry.getKey());
        }
        continue;
      }
      ASTNode node = entry.getKey();
      RexInputRef columnDesc = (RexInputRef) entry.getValue();
      int index = columnDesc.getIndex();
      String[] tmp;
      if (index < input.getColumnInfos().size()) {
        ColumnInfo columnInfo = input.getColumnInfos().get(index);
        if (columnInfo.getTabAlias() == null
            || columnInfo.getTabAlias().length() == 0) {
          // These aren't real column refs; instead, they are special
          // internal expressions used in the representation of aggregation.
          continue;
        }
        tmp = input.reverseLookup(columnInfo.getInternalName());
      } else {
        // in subquery case, tmp may be from outside.
        ColumnInfo columnInfo = tcCtx.getOuterRR().getColumnInfos().get(
            index - input.getColumnInfos().size());
        if (columnInfo.getTabAlias() == null
            || columnInfo.getTabAlias().length() == 0) {
          continue;
        }
        tmp = tcCtx.getOuterRR().reverseLookup(columnInfo.getInternalName());
      }
      StringBuilder replacementText = new StringBuilder();
      replacementText.append(HiveUtils.unparseIdentifier(tmp[0], conf));
      replacementText.append(".");
      replacementText.append(HiveUtils.unparseIdentifier(tmp[1], conf));
      unparseTranslator.addTranslation(node, replacementText.toString());
    }

    for (ASTNode node : fieldDescList) {
      Map<ASTNode, String> map = translateFieldDesc(node);
      for (Entry<ASTNode, String> entry : map.entrySet()) {
        unparseTranslator.addTranslation(entry.getKey(), entry.getValue());
      }
    }

    return nodeOutputs;
  }

  private String recommendName(RexNode exp, String colAlias, RowResolver rowResolver) {
    if (!colAlias.startsWith(autogenColAliasPrfxLbl)) {
      return null;
    }
    String column = recommendInputName(exp, rowResolver);
    if (column != null && !column.startsWith(autogenColAliasPrfxLbl)) {
      return column;
    }
    return null;
  }

  /**
   * Recommend name for the expression
   */
  private static String recommendInputName(RexNode desc, RowResolver rowResolver) {
    Integer pos = null;
    if (desc instanceof RexInputRef) {
      pos = ((RexInputRef) desc).getIndex();
    }
    if (desc.isA(SqlKind.CAST)) {
      RexNode input = ((RexCall) desc).operands.get(0);
      if (input instanceof RexInputRef) {
        pos =  ((RexInputRef) input).getIndex();
      }
    }
    return pos != null ?
        rowResolver.getColumnInfos().get(pos).getInternalName() :
        null;
  }


  /**
   * Contains information useful to decorrelate queries.
   */
  protected static class InputContext {
    protected final RelDataType inputRowType;
    protected final ImmutableBiMap<Integer, String> positionToColumnName;
    protected final RowResolver inputRowResolver;

    protected InputContext(RelDataType inputRowType, ImmutableMap<String, Integer> columnNameToPosition,
          RowResolver inputRowResolver) {
      this.inputRowType = inputRowType;
      this.positionToColumnName = ImmutableBiMap.copyOf(columnNameToPosition).inverse();
      this.inputRowResolver = inputRowResolver.duplicate();
    }
  }

  /**
   * This method can be called at startup time to pre-register all the
   * additional Hive classes (compared to Calcite core classes) that may
   * be visited during the planning phase in the metadata providers
   * and the field trimmer.
   */
  public static void warmup() {
    JaninoRelMetadataProvider.DEFAULT.register(HIVE_REL_NODE_CLASSES);
    HiveDefaultRelMetadataProvider.initializeMetadataProviderClass(HIVE_REL_NODE_CLASSES);
    HiveTezModelRelMetadataProvider.DEFAULT.register(HIVE_REL_NODE_CLASSES);
    HiveMaterializationRelMetadataProvider.DEFAULT.register(HIVE_REL_NODE_CLASSES);
    HiveRelFieldTrimmer.initializeFieldTrimmerClass(HIVE_REL_NODE_CLASSES);
  }

  private enum TableType {
    DRUID,
    NATIVE,
    JDBC
  }

}
