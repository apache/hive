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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.tools.Frameworks;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveRelOptMaterialization;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTezModelRelMetadataProvider;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveInBetweenExpandRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveAggregateInsertDeleteIncrementalRewritingRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveAggregateInsertIncrementalRewritingRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveAggregatePartitionIncrementalRewritingRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveAugmentMaterializationRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveAugmentSnapshotMaterializationRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveInsertOnlyScanWriteIdRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveJoinInsertIncrementalRewritingRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewUtils;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HivePushdownSnapshotFilterRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveRowIsDeletedPropagator;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.MaterializedViewRewritingRelVisitor;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveIncrementalRelMdRowCount;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.ColumnAccessInfo;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.mapper.StatsSource;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonList;

/**
 * Semantic analyzer for alter materialized view rebuild commands.
 * This subclass of {@link SemanticAnalyzer} generates a plan which is derived from the materialized view definition
 * query plan.
 * <br>
 * Steps:
 * <ul>
 *    <li>Take the Calcite plan of the materialized view definition query.</li>
 *    <li>Using the snapshot data in materialized view metadata insert A {@link HiveFilter} operator on top of each
 *    {@link HiveTableScan} operator. The condition has a predicate like ROW_ID.writeid &lt;= high_watermark
 *    This step is done by {@link HiveAugmentMaterializationRule} or {@link HiveAugmentSnapshotMaterializationRule}.
 *    The resulting plan should produce the current result of the materialized view, the one which was created at last
 *    rebuild.</li>
 *    <li>Transform the original view definition query plan using
 *    <a href="https://calcite.apache.org/docs/materialized_views.html#union-rewriting">Union rewrite</a> or
 *    <a href="https://calcite.apache.org/docs/materialized_views.html#union-rewriting-with-aggregate">Union rewrite with aggregate</a> and
 *    the augmented plan. The result plan has a {@link HiveUnion} operator on top with two branches
 *    <ul>
 *      <li>Scan the materialize view for existing records</li>
 *      <li>A plan which is derived from the augmented materialized view definition query plan. This produces the
 *      newly inserted records</li>
 *    </ul>
 *    </li>
 *    <li>Transform the plan into incremental rebuild plan if possible:
 *    <ul>
 *      <li>The materialized view definition query has aggregate and base tables has insert operations only.
 *      {@link HiveAggregateInsertIncrementalRewritingRule}</li>
 *      <li>The materialized view definition query hasn't got aggregate and base tables has insert operations only.
 *      {@link HiveJoinInsertIncrementalRewritingRule}</li>
 *      <li>The materialized view definition query has aggregate and any base tables has delete operations.
 *      {@link HiveAggregateInsertDeleteIncrementalRewritingRule}</li>
 *      <li>The materialized view definition query hasn't got aggregate and any base tables has delete operations.
 *      Incremental rebuild is not possible because all records from all source tables need a unique identifier to
 *      join it with the corresponding record exists in the view. ROW__ID can not be used because it's writedId
 *      component is changed at delete and unique and primary key constraints are not enforced in Hive.
 *      </li>
 *    </ul>
 *    When any base tables has delete operations the {@link HiveTableScan} operators are fetching the deleted rows too
 *    and {@link HiveRowIsDeletedPropagator} ensures that extra filter conditions are added to address these.
 *    </li>
 * </ul>
 */

@DDLType(types = HiveParser.TOK_ALTER_MATERIALIZED_VIEW_REBUILD)
public class AlterMaterializedViewRebuildAnalyzer extends CalcitePlanner {
  private static final Logger LOG = LoggerFactory.getLogger(AlterMaterializedViewRebuildAnalyzer.class);

  protected Table mvTable;

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

    try {
      mvTable = db.getTable(tableName.getDb(), tableName.getTable());
      Boolean outdated = db.isOutdatedMaterializedView(getTxnMgr(), mvTable);
      if (outdated != null && !outdated) {
        String msg = String.format("Materialized view %s.%s is up to date. Skipping rebuild.",
                tableName.getDb(), tableName.getTable());
        LOG.info(msg);
        console.printInfo(msg, false);
        return;
      }
    } catch (HiveException e) {
      LOG.warn("Error while checking materialized view " + tableName.getDb() + "." + tableName.getTable(), e);
    }

    ASTNode rewrittenAST = getRewrittenAST(tableName);

    mvRebuildMode = MaterializationRebuildMode.INSERT_OVERWRITE_REBUILD;

    LOG.debug("Rebuilding materialized view " + tableName.getNotEmptyDbTable());
    super.analyzeInternal(rewrittenAST);
    queryState.setCommandType(HiveOperation.ALTER_MATERIALIZED_VIEW_REBUILD);
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

  @Override
  protected Frameworks.PlannerAction<RelNode> createPlannerAction(
          Map<String, PrunedPartitionList> partitionCache,
          StatsSource statsSource,
          ColumnAccessInfo columnAccessInfo) {
    return new MVRebuildCalcitePlannerAction(partitionCache, statsSource, columnAccessInfo);
  }

  protected class MVRebuildCalcitePlannerAction extends CalcitePlannerAction {
    public MVRebuildCalcitePlannerAction(Map<String, PrunedPartitionList> partitionCache,
                                         StatsSource statsSource,
                                         ColumnAccessInfo columnAccessInfo) {
      super(partitionCache, statsSource, columnAccessInfo, getQB());
    }

    @Override
    protected RelNode applyMaterializedViewRewriting(RelOptPlanner planner, RelNode basePlan,
                                                     RelMetadataProvider mdProvider, RexExecutor executorProvider) {
      final RelOptCluster optCluster = basePlan.getCluster();
      final PerfLogger perfLogger = SessionState.getPerfLogger();
      final RelNode calcitePreMVRewritingPlan = basePlan;
      final Set<TableName> tablesUsedQuery = getTablesUsed(basePlan);

      // Add views to planner
      HiveRelOptMaterialization materialization;
      try {
        // We only retrieve the materialization corresponding to the rebuild. In turn,
        // we pass 'true' for the forceMVContentsUpToDate parameter, as we cannot allow the
        // materialization contents to be stale for a rebuild if we want to use it.
        materialization = db.getMaterializedViewForRebuild(
                mvTable.getDbName(), mvTable.getTableName(), tablesUsedQuery, getTxnMgr());
        if (materialization == null) {
          // There is no materialization, we can return the original plan
          return calcitePreMVRewritingPlan;
        }
        // We need to use the current cluster for the scan operator on views,
        // otherwise the planner will throw an Exception (different planners)
        materialization = materialization.copyToNewCluster(optCluster);
      } catch (HiveException e) {
        LOG.warn("Exception loading materialized views", e);
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

      // If it is a materialized view rebuild, we use the HepPlanner, since we only have
      // one MV and we would like to use it to create incremental maintenance plans
      program = new HepProgramBuilder();
      generatePartialProgram(program, true, HepMatchOrder.TOP_DOWN,
              HiveMaterializedViewRule.MATERIALIZED_VIEW_REWRITING_RULES);
      // Add materialization for rebuild to planner
      // Optimize plan
      basePlan = executeProgram(basePlan, program.build(), mdProvider, executorProvider, singletonList(materialization));

      program = new HepProgramBuilder();
      generatePartialProgram(program, false, HepMatchOrder.DEPTH_FIRST, HivePushdownSnapshotFilterRule.INSTANCE);
      basePlan = executeProgram(basePlan, program.build(), mdProvider, executorProvider);

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
      if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_MATERIALIZED_VIEW_REBUILD_INCREMENTAL)) {
        if (materialization.isSourceTablesCompacted()) {
          return calcitePreMVRewritingPlan;
        }

        RelNode incrementalRebuildPlan = applyRecordIncrementalRebuildPlan(
                basePlan,
                mdProvider,
                executorProvider,
                optCluster,
                calcitePreMVRewritingPlan,
                materialization);

        if (mvRebuildMode != MaterializationRebuildMode.INSERT_OVERWRITE_REBUILD) {
          return incrementalRebuildPlan;
        }

        return applyPartitionIncrementalRebuildPlan(
                basePlan, mdProvider, executorProvider, materialization, optCluster, calcitePreMVRewritingPlan);
      }

      // Now we trigger some needed optimization rules again
      return applyPreJoinOrderingTransforms(basePlan, mdProvider, executorProvider);
    }

    private RelNode applyRecordIncrementalRebuildPlan(
        RelNode basePlan,
        RelMetadataProvider mdProvider,
        RexExecutor executorProvider,
        RelOptCluster optCluster,
        RelNode calcitePreMVRewritingPlan,
        HiveRelOptMaterialization materialization) {
      // First we need to check if it is valid to convert to MERGE/INSERT INTO.
      // If we succeed, we modify the plan and afterwards the AST.
      // MV should be an acid table.
      boolean acidView = AcidUtils.isFullAcidTable(mvTable.getTTable())
              || AcidUtils.isNonNativeAcidTable(mvTable);
      MaterializedViewRewritingRelVisitor visitor =
          new MaterializedViewRewritingRelVisitor(acidView);
      visitor.go(basePlan);
      switch (visitor.getIncrementalRebuildMode()) {
        case INSERT_ONLY:
          if (materialization.isSourceTablesUpdateDeleteModified()) {
            return calcitePreMVRewritingPlan;
          }

          if (visitor.isContainsAggregate()) {
            return applyAggregateInsertIncremental(
                basePlan, mdProvider, executorProvider, optCluster, materialization, calcitePreMVRewritingPlan);
          } else {
            return applyJoinInsertIncremental(basePlan, mdProvider, executorProvider);
          }
        case AVAILABLE:
          if (!materialization.isSourceTablesUpdateDeleteModified()) {
            return applyAggregateInsertIncremental(
                basePlan, mdProvider, executorProvider, optCluster, materialization, calcitePreMVRewritingPlan);
          } else {
            return applyAggregateInsertDeleteIncremental(basePlan, mdProvider, executorProvider);
          }
        case NOT_AVAILABLE:
        default:
          if (materialization.isSourceTablesUpdateDeleteModified()) {
            // calcitePreMVRewritingPlan is already got the optimizations by applyPreJoinOrderingTransforms prior to calling
            // applyMaterializedViewRewriting in CalcitePlanner.CalcitePlannerAction.apply
            return calcitePreMVRewritingPlan;
          } else {
            return applyPreJoinOrderingTransforms(basePlan, mdProvider, executorProvider);
          }
      }
    }

    private RelNode applyAggregateInsertDeleteIncremental(
            RelNode basePlan, RelMetadataProvider mdProvider, RexExecutor executorProvider) {
      mvRebuildMode = MaterializationRebuildMode.AGGREGATE_INSERT_DELETE_REBUILD;
      return applyIncrementalRebuild(
              basePlan, mdProvider, executorProvider, HiveAggregateInsertDeleteIncrementalRewritingRule.INSTANCE);
    }

    private RelNode applyAggregateInsertIncremental(
            RelNode basePlan, RelMetadataProvider mdProvider, RexExecutor executorProvider, RelOptCluster optCluster,
            HiveRelOptMaterialization materialization, RelNode calcitePreMVRewritingPlan) {
      mvRebuildMode = MaterializationRebuildMode.AGGREGATE_INSERT_REBUILD;
      RelNode incrementalRebuildPlan = applyIncrementalRebuild(basePlan, mdProvider, executorProvider,
              HiveInsertOnlyScanWriteIdRule.INSTANCE, HiveAggregateInsertIncrementalRewritingRule.INSTANCE);

      // Make a cost-based decision factoring the configuration property
      RelOptCost costOriginalPlan = calculateCost(
          optCluster, mdProvider, HiveTezModelRelMetadataProvider.DEFAULT, calcitePreMVRewritingPlan);

      RelOptCost costIncrementalRebuildPlan = calculateCost(optCluster, mdProvider,
          HiveIncrementalRelMdRowCount.createMetadataProvider(materialization), incrementalRebuildPlan);

      final double factorSelectivity = HiveConf.getFloatVar(
          conf, HiveConf.ConfVars.HIVE_MATERIALIZED_VIEW_REBUILD_INCREMENTAL_FACTOR);
      costIncrementalRebuildPlan = costIncrementalRebuildPlan.multiplyBy(factorSelectivity);

      if (costOriginalPlan.isLe(costIncrementalRebuildPlan)) {
        mvRebuildMode = MaterializationRebuildMode.INSERT_OVERWRITE_REBUILD;
        return calcitePreMVRewritingPlan;
      }

      return incrementalRebuildPlan;
    }

    private RelNode applyJoinInsertIncremental(
            RelNode basePlan, RelMetadataProvider mdProvider, RexExecutor executorProvider) {
      mvRebuildMode = MaterializationRebuildMode.JOIN_INSERT_REBUILD;
      return applyIncrementalRebuild(basePlan, mdProvider, executorProvider,
              HiveInsertOnlyScanWriteIdRule.INSTANCE, HiveJoinInsertIncrementalRewritingRule.INSTANCE);
    }

    private RelNode applyPartitionIncrementalRebuildPlan(
        RelNode basePlan, RelMetadataProvider mdProvider, RexExecutor executorProvider,
        HiveRelOptMaterialization materialization, RelOptCluster optCluster,
        RelNode calcitePreMVRewritingPlan) {

      if (materialization.isSourceTablesUpdateDeleteModified()) {
        // TODO: Create rewrite rule to transform the plan to partition based incremental rebuild
        // addressing deleted records. The rule should enable fetching deleted rows and count deleted records
        // with a negative sign when calculating sum and count functions in top aggregate.
        // This type of rewrite also requires the existence of count(*) function call in view definition.
        return calcitePreMVRewritingPlan;
      }

      RelOptHiveTable hiveTable = (RelOptHiveTable) materialization.tableRel.getTable();
      if (!AcidUtils.isInsertOnlyTable(hiveTable.getHiveTableMD())) {
        // TODO: plan may contains TS on fully ACID table and aggregate functions which are not supported the
        // record level incremental rewriting rules but partition based can be applied.
        return applyPreJoinOrderingTransforms(basePlan, mdProvider, executorProvider);
      }

      RelNode incrementalRebuildPlan = applyIncrementalRebuild(basePlan, mdProvider, executorProvider,
              HiveInsertOnlyScanWriteIdRule.INSTANCE,
              HiveAggregatePartitionIncrementalRewritingRule.INSTANCE);

      // Make a cost-based decision factoring the configuration property
      RelOptCost costOriginalPlan = calculateCost(
              optCluster, mdProvider, HiveTezModelRelMetadataProvider.DEFAULT, calcitePreMVRewritingPlan);

      RelOptCost costIncrementalRebuildPlan = calculateCost(optCluster, mdProvider,
              HiveIncrementalRelMdRowCount.createMetadataProvider(materialization), incrementalRebuildPlan);

      final double factorSelectivity = HiveConf.getFloatVar(
              conf, HiveConf.ConfVars.HIVE_MATERIALIZED_VIEW_REBUILD_INCREMENTAL_FACTOR);
      costIncrementalRebuildPlan = costIncrementalRebuildPlan.multiplyBy(factorSelectivity);

      if (costOriginalPlan.isLe(costIncrementalRebuildPlan)) {
        mvRebuildMode = MaterializationRebuildMode.INSERT_OVERWRITE_REBUILD;
        return calcitePreMVRewritingPlan;
      }

      return incrementalRebuildPlan;
    }

    private RelOptCost calculateCost(
            RelOptCluster optCluster,
            RelMetadataProvider originalMetadataProvider,
            JaninoRelMetadataProvider metadataProvider,
            RelNode plan) {
      optCluster.invalidateMetadataQuery();
      RelMetadataQuery.THREAD_PROVIDERS.set(metadataProvider);
      try {
        return RelMetadataQuery.instance().getCumulativeCost(plan);
      } finally {
        optCluster.invalidateMetadataQuery();
        RelMetadataQuery.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(originalMetadataProvider));
      }
    }

    private RelNode applyIncrementalRebuild(RelNode basePlan, RelMetadataProvider mdProvider,
                                            RexExecutor executorProvider, RelOptRule... rebuildRules) {
      HepProgramBuilder program = new HepProgramBuilder();
      generatePartialProgram(program, false, HepMatchOrder.DEPTH_FIRST, rebuildRules);
      basePlan = executeProgram(basePlan, program.build(), mdProvider, executorProvider);
      return applyPreJoinOrderingTransforms(basePlan, mdProvider, executorProvider);
    }

  }

  @Override
  protected ASTNode fixUpAfterCbo(ASTNode originalAst, ASTNode newAst, CalcitePlanner.PreCboCtx cboCtx)
          throws SemanticException {
    ASTNode fixedAST = super.fixUpAfterCbo(originalAst, newAst, cboCtx);
    switch (mvRebuildMode) {
      case INSERT_OVERWRITE_REBUILD:
        return fixedAST;
      case JOIN_INSERT_REBUILD:
        fixUpASTJoinInsertIncrementalRebuild(fixedAST);
        return fixedAST;
      case AGGREGATE_INSERT_REBUILD:
        fixUpASTAggregateInsertIncrementalRebuild(fixedAST, getMaterializedViewASTBuilder());
        return fixedAST;
      case AGGREGATE_INSERT_DELETE_REBUILD:
        fixUpASTAggregateInsertDeleteIncrementalRebuild(fixedAST, getMaterializedViewASTBuilder());
        return fixedAST;
      default:
        throw new UnsupportedOperationException("No materialized view rebuild exists for mode " + mvRebuildMode);
    }
  }

  @NotNull
  private MaterializedViewASTBuilder getMaterializedViewASTBuilder() {
    if (AcidUtils.isFullAcidTable(mvTable.getTTable())) {
      return new NativeAcidMaterializedViewASTBuilder();
    } else if (AcidUtils.isNonNativeAcidTable(mvTable)) {
      return new NonNativeAcidMaterializedViewASTBuilder(mvTable);
    } else {
      throw new UnsupportedOperationException("Incremental rebuild is supported only for fully ACID materialized " +
              "views or if the Storage handler supports snapshots (Iceberg).");
    }
  }

  private void fixUpASTAggregateInsertIncrementalRebuild(ASTNode newAST, MaterializedViewASTBuilder astBuilder)
          throws SemanticException {
    ASTNode updateNode = new CalcitePlanner.ASTSearcher().simpleBreadthFirstSearch(
            newAST, HiveParser.TOK_QUERY, HiveParser.TOK_INSERT);
    ASTNode subqueryNodeInputROJ = new CalcitePlanner.ASTSearcher().simpleBreadthFirstSearch(
            newAST, HiveParser.TOK_QUERY, HiveParser.TOK_FROM, HiveParser.TOK_RIGHTOUTERJOIN,
            HiveParser.TOK_SUBQUERY);
    ASTNode whereClauseInUpdate = findWhereClause(updateNode);

    // TOK_WHERE
    //    or
    //       .                        <- DISJUNCT FOR <UPDATE>
    //          TOK_TABLE_OR_COL
    //             $hdt$_0
    //          $f3
    //       TOK_FUNCTION             <- DISJUNCT FOR <INSERT>
    //          isnull
    //          .
    //             TOK_TABLE_OR_COL
    //                $hdt$_0
    //             $f3
    Map<Context.DestClausePrefix, ASTNode> disjunctMap = new HashMap<>(Context.DestClausePrefix.values().length);
    if (whereClauseInUpdate.getChild(0).getChild(0).getType() == HiveParser.DOT) {
      disjunctMap.put(Context.DestClausePrefix.UPDATE, (ASTNode) whereClauseInUpdate.getChild(0).getChild(0));
      disjunctMap.put(Context.DestClausePrefix.INSERT, (ASTNode) whereClauseInUpdate.getChild(0).getChild(1));
    } else if (whereClauseInUpdate.getChild(0).getChild(1).getType() == HiveParser.DOT) {
      disjunctMap.put(Context.DestClausePrefix.INSERT, (ASTNode) whereClauseInUpdate.getChild(0).getChild(0));
      disjunctMap.put(Context.DestClausePrefix.UPDATE, (ASTNode) whereClauseInUpdate.getChild(0).getChild(1));
    } else {
      throw new SemanticException("Unexpected condition in incremental rewriting");
    }

    fixUpASTAggregateIncrementalRebuild(subqueryNodeInputROJ, updateNode, disjunctMap, astBuilder);
  }

  private void fixUpASTAggregateIncrementalRebuild(
          ASTNode subqueryNodeInputROJ,
          ASTNode updateInsertNode,
          Map<Context.DestClausePrefix, ASTNode> disjuncts,
          MaterializedViewASTBuilder astBuilder)
          throws SemanticException {
    // Replace INSERT OVERWRITE by MERGE equivalent rewriting. The update branch is
    // split to a delete (updateDeleteNode) and an insert (updateInsertNode) branch
    // Here we need to do this complex AST rewriting that generates the same plan
    // that a MERGE clause would generate because CBO does not support MERGE yet.
    // TODO: Support MERGE as first class member in CBO to simplify this logic.
    // 1) Replace INSERT OVERWRITE by INSERT
    ASTNode destinationNode = (ASTNode) updateInsertNode.getChild(0);
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
    ASTNode updateParent = (ASTNode) updateInsertNode.getParent();
    ASTNode insertNode = (ASTNode) ParseDriver.adaptor.dupTree(updateInsertNode);
    insertNode.setParent(updateParent);
    // 3) Add sort columns (ROW_ID in case of native) to select clause from left input for the RIGHT OUTER JOIN.
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
    ASTNode selectNodeInputROJ = new ASTSearcher().simpleBreadthFirstSearch(
            subqueryNodeInputROJ, HiveParser.TOK_SUBQUERY, HiveParser.TOK_QUERY,
            HiveParser.TOK_INSERT, HiveParser.TOK_SELECT);
    astBuilder.appendDeleteSelectNodes(
        selectNodeInputROJ,
        TableName.getDbTable(
            materializationNode.getChild(0).getText(),
            materializationNode.getChild(1).getText()));
    // 4) Transform first INSERT branch into an UPDATE
    // 4.1) Modifying filter condition.
    ASTNode whereClauseInUpdate = findWhereClause(updateInsertNode);
    if (whereClauseInUpdate.getChild(0).getType() != HiveParser.KW_OR) {
      throw new SemanticException("OR clause expected below TOK_WHERE in incremental rewriting");
    }
    // We bypass the OR clause and select the first disjunct for the Update branch
    ParseDriver.adaptor.setChild(whereClauseInUpdate, 0, disjuncts.get(Context.DestClausePrefix.UPDATE));
    ASTNode updateDeleteNode = (ASTNode) ParseDriver.adaptor.dupTree(updateInsertNode);
    // 4.2) Adding acid sort columns (ROW__ID in case of native acid query StorageHandler otherwise)
    ASTNode selectNodeInUpdateDelete = (ASTNode) updateDeleteNode.getChild(1);
    if (selectNodeInUpdateDelete.getType() != HiveParser.TOK_SELECT) {
      throw new SemanticException("TOK_SELECT expected in incremental rewriting got "
              + selectNodeInUpdateDelete.getType());
    }
    // Remove children
    while (selectNodeInUpdateDelete.getChildCount() > 0) {
      selectNodeInUpdateDelete.deleteChild(0);
    }
    // And add acid sort columns
    List<ASTNode> selectExprNodesInUpdate = astBuilder.createDeleteSelectNodes(
            subqueryNodeInputROJ.getChild(1).getText());
    for (int i = 0; i < selectExprNodesInUpdate.size(); ++i) {
      selectNodeInUpdateDelete.insertChild(i, selectExprNodesInUpdate.get(i));
    }
    // 4.3) Finally, we add SORT clause, this is needed for the UPDATE.
    ASTNode sortExprNode = astBuilder.createSortNodes(
            astBuilder.createAcidSortNodes((ASTNode) subqueryNodeInputROJ.getChild(1)));
    ParseDriver.adaptor.addChild(updateDeleteNode, sortExprNode);
    // 5) Modify INSERT branch condition. In particular, we need to modify the
    // WHERE clause and pick up the disjunct for the Insert branch.
    ASTNode whereClauseInInsert = findWhereClause(insertNode);
    if (whereClauseInInsert.getChild(0).getType() != HiveParser.KW_OR) {
      throw new SemanticException("OR clause expected below TOK_WHERE in incremental rewriting");
    }
    // We bypass the OR clause and select the second disjunct
    ParseDriver.adaptor.setChild(whereClauseInInsert, 0, disjuncts.get(Context.DestClausePrefix.INSERT));

    updateParent.addChild(updateDeleteNode);
    updateParent.addChild(insertNode);

    // 6) Now we set some tree properties related to multi-insert
    // operation with INSERT/UPDATE
    ctx.setOperation(Context.Operation.MERGE);
    ctx.addDestNamePrefix(1, Context.DestClausePrefix.INSERT);
    ctx.addDestNamePrefix(2, Context.DestClausePrefix.DELETE);
    ctx.addDestNamePrefix(3, Context.DestClausePrefix.INSERT);
  }

  private void fixUpASTAggregateInsertDeleteIncrementalRebuild(ASTNode newAST, MaterializedViewASTBuilder astBuilder)
          throws SemanticException {
    ASTNode updateNode = new CalcitePlanner.ASTSearcher().simpleBreadthFirstSearch(
            newAST, HiveParser.TOK_QUERY, HiveParser.TOK_INSERT);
    ASTNode subqueryNodeInputROJ = new CalcitePlanner.ASTSearcher().simpleBreadthFirstSearch(
            newAST, HiveParser.TOK_QUERY, HiveParser.TOK_FROM, HiveParser.TOK_RIGHTOUTERJOIN,
            HiveParser.TOK_SUBQUERY);
    ASTNode whereClauseInUpdate = findWhereClause(updateNode);

    ASTNode[] disjuncts = new ASTNode[] {
            (ASTNode) whereClauseInUpdate.getChild(0).getChild(0),
            (ASTNode) whereClauseInUpdate.getChild(0).getChild(1).getChild(0),
            (ASTNode) whereClauseInUpdate.getChild(0).getChild(1).getChild(1)
    };

    // Find disjuncts for each branches: INSERT, UPDATE, DELETE
    // TOK_WHERE
    //    or
    //       <disjuct>
    //       or
    //          <disjuct>
    //          <disjuct>
    //
    // Note: the order of disjuncts may change by other optimizations,
    // so we have to find the right disjunct for each branch
    Map<Context.DestClausePrefix, ASTNode> disjunctMap = new HashMap<>(Context.DestClausePrefix.values().length);
    for (ASTNode disjunct : disjuncts) {
      if (disjunct.getChild(0).getType() == HiveParser.TOK_FUNCTION &&
              disjunct.getChild(0).getChild(0).getType() == HiveParser.Identifier &&
              "isnull".equals(disjunct.getChild(0).getChild(0).getText())) {
        disjunctMap.put(Context.DestClausePrefix.INSERT, disjunct);
      } else if (disjunct.getChild(0).getType() != HiveParser.TOK_FUNCTION &&
              new ASTSearcher().simpleBreadthFirstSearch(disjunct,
                      HiveParser.KW_AND, HiveParser.KW_OR, HiveParser.KW_AND, HiveParser.EQUAL) != null) {
        disjunctMap.put(Context.DestClausePrefix.DELETE, disjunct);
      } else if (disjunct.getChild(0).getType() != HiveParser.TOK_FUNCTION &&
              new ASTSearcher().simpleBreadthFirstSearch(disjunct,
                      HiveParser.KW_AND, HiveParser.KW_OR, HiveParser.KW_AND, HiveParser.GREATERTHAN) != null) {
        disjunctMap.put(Context.DestClausePrefix.UPDATE, disjunct);
      } else {
        throw new SemanticException("Unexpected condition in incremental rewriting");
      }
    }

    fixUpASTAggregateIncrementalRebuild(subqueryNodeInputROJ, updateNode, disjunctMap, astBuilder);
    addDeleteBranch(updateNode, subqueryNodeInputROJ, disjunctMap.get(Context.DestClausePrefix.DELETE), astBuilder);

    ctx.addDestNamePrefix(4, Context.DestClausePrefix.DELETE);
  }

  private ASTNode findWhereClause(ASTNode updateNode) throws SemanticException {
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

    return whereClauseInUpdate;
  }

  private void addDeleteBranch(ASTNode updateNode, ASTNode subqueryNodeInputROJ, ASTNode predicate,
                               MaterializedViewASTBuilder astBuilder)
          throws SemanticException {
    ASTNode updateParent = (ASTNode) updateNode.getParent();
    ASTNode deleteNode = (ASTNode) ParseDriver.adaptor.dupTree(updateNode);
    deleteNode.setParent(updateParent);
    updateParent.addChild(deleteNode);

    // 1) Transform first INSERT branch into a DELETE
    ASTNode selectNodeInDelete = (ASTNode) deleteNode.getChild(1);
    if (selectNodeInDelete.getType() != HiveParser.TOK_SELECT) {
      throw new SemanticException("TOK_SELECT expected in incremental rewriting");
    }
    // 2) Remove all fields
    while (selectNodeInDelete.getChildCount() > 0) {
      selectNodeInDelete.deleteChild(0);
    }
    // 3) Adding acid sort columns
    astBuilder.createDeleteSelectNodes(subqueryNodeInputROJ.getChild(1).getText())
            .forEach(astNode -> ParseDriver.adaptor.addChild(selectNodeInDelete, astNode));

    // 4) Add filter condition to delete
    ASTNode whereClauseInDelete = findWhereClause(deleteNode);
    ParseDriver.adaptor.setChild(whereClauseInDelete, 0, predicate);
  }

  private void fixUpASTJoinInsertIncrementalRebuild(ASTNode newAST) throws SemanticException {
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
  protected boolean allowOutputMultipleTimes() {
    return true;
  }
}
