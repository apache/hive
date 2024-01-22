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
package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.AppMasterEventOperator;
import org.apache.hadoop.hive.ql.exec.DummyStoreOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.graph.OperatorGraph;
import org.apache.hadoop.hive.ql.optimizer.graph.OperatorGraph.Cluster;
import org.apache.hadoop.hive.ql.optimizer.graph.OperatorGraph.EdgeType;
import org.apache.hadoop.hive.ql.optimizer.graph.OperatorGraph.OpEdge;
import org.apache.hadoop.hive.ql.optimizer.graph.OperatorGraph.OperatorEdgePredicate;
import org.apache.hadoop.hive.ql.parse.GenTezUtils;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.SemiJoinBranchInfo;
import org.apache.hadoop.hive.ql.plan.DynamicPruningEventDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeDynamicListDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDynamicValueDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFInBloomFilter;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultiset;

import static org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits.AUTOPARALLEL;
import static org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits.FIXED;
import static org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits.UNIFORM;
import static org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits.UNSET;

import static org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils.*;
/**
 * Shared computation optimizer.
 *
 * <p>Originally, this rule would find scan operators over the same table
 * in the query plan and merge them if they met some preconditions.
 * <pre>
 *  TS   TS             TS
 *  |    |     -&gt;   /  \
 *  Op   Op           Op  Op
 * </pre>
 * <p>Now the rule has been extended to find opportunities to other operators
 * downstream, not only a single table scan.
 *
 *  TS1   TS2    TS1   TS2            TS1   TS2
 *   |     |      |     |              |     |
 *   |    RS      |    RS              |    RS
 *    \   /        \   /       -&gt;    \   /
 *   MapJoin      MapJoin              MapJoin
 *      |            |                  /   \
 *      Op           Op                Op   Op
 *
 * <p>If the extended version of the optimizer is enabled, it can go beyond
 * a work boundary to find reutilization opportunities.
 *
 * <p>The optimization only works with the Tez execution engine.
 */
public class SharedWorkOptimizer extends Transform {

  private final static Logger LOG = LoggerFactory.getLogger(SharedWorkOptimizer.class);

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    LOG.info("SharedWorkOptimizer start");

    final Map<String, TableScanOperator> topOps = pctx.getTopOps();
    if (topOps.size() < 2) {
      // Nothing to do, bail out
      return pctx;
    }

    // Map of dbName.TblName -> TSOperator
    ArrayListMultimap<String, TableScanOperator> tableNameToOps = splitTableScanOpsByTable(pctx);

    // Check whether all tables in the plan are unique
    boolean tablesReferencedOnlyOnce =
        tableNameToOps.asMap().entrySet().stream().noneMatch(e -> e.getValue().size() > 1);
    if (tablesReferencedOnlyOnce) {
      // Nothing to do, bail out
      return pctx;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Before SharedWorkOptimizer:\n" + Operator.toString(pctx.getTopOps().values()));
    }

    // We enforce a certain order when we do the reutilization.
    // In particular, we use size of table x number of reads to
    // rank the tables.
    List<Entry<String, Long>> sortedTables = rankTablesByAccumulatedSize(pctx);
    LOG.debug("Sorted tables by size: {}", sortedTables);

    // Cache to use during optimization
    SharedWorkOptimizerCache optimizerCache = new SharedWorkOptimizerCache();

    // Gather information about the DPP table scans and store it in the cache
    gatherDPPTableScanOps(pctx, optimizerCache);

    for (Entry<String, Long> tablePair : sortedTables) {
      String tableName = tablePair.getKey();
      List<TableScanOperator> scans = tableNameToOps.get(tableName);

      // Execute shared work optimization
      runSharedWorkOptimization(pctx, optimizerCache, scans, Mode.SubtreeMerge);

      if (LOG.isDebugEnabled()) {
        LOG.debug("After SharedWorkOptimizer:\n" + Operator.toString(pctx.getTopOps().values()));
      }

      if (pctx.getConf().getBoolVar(ConfVars.HIVE_SHARED_WORK_EXTENDED_OPTIMIZATION)) {
        // Execute extended shared work optimization
        sharedWorkExtendedOptimization(pctx, optimizerCache);

        if (LOG.isDebugEnabled()) {
          LOG.debug("After SharedWorkExtendedOptimizer:\n" + Operator.toString(pctx.getTopOps().values()));
        }
      }

      if (pctx.getConf().getBoolVar(ConfVars.HIVE_SHARED_WORK_SEMIJOIN_OPTIMIZATION)) {

        // Execute shared work optimization with semijoin removal

        boolean optimized = runSharedWorkOptimization(pctx, optimizerCache, scans, Mode.RemoveSemijoin);
        if (optimized && pctx.getConf().getBoolVar(ConfVars.HIVE_SHARED_WORK_EXTENDED_OPTIMIZATION)) {
          // If it was further optimized, execute a second round of extended shared work optimizer
          sharedWorkExtendedOptimization(pctx, optimizerCache);
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("After SharedWorkSJOptimizer:\n" + Operator.toString(pctx.getTopOps().values()));
        }
      }

      if (pctx.getConf().getBoolVar(ConfVars.HIVE_SHARED_WORK_DPPUNION_OPTIMIZATION)) {
        boolean optimized = runSharedWorkOptimization(pctx, optimizerCache, scans, Mode.DPPUnion);

        if (optimized && pctx.getConf().getBoolVar(ConfVars.HIVE_SHARED_WORK_EXTENDED_OPTIMIZATION)) {
          // If it was further optimized, do a round of extended shared work optimizer
          sharedWorkExtendedOptimization(pctx, optimizerCache);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("After DPPUnion:\n" + Operator.toString(pctx.getTopOps().values()));
        }
      }

    }

    if (pctx.getConf().getBoolVar(ConfVars.HIVE_SHARED_WORK_REUSE_MAPJOIN_CACHE)) {
      runMapJoinCacheReuseOptimization(pctx, optimizerCache);
    }

    // If we are running tests, we are going to verify that the contents of the cache
    // correspond with the contents of the plan, and otherwise we fail.
    // This check always run when we are running in test mode, independently on whether
    // we use the basic or the extended version of the optimizer.
    if (pctx.getConf().getBoolVar(ConfVars.HIVE_IN_TEST)) {
      Set<Operator<?>> visited = new HashSet<>();
      Iterator<Entry<String, TableScanOperator>> it = topOps.entrySet().iterator();
      while (it.hasNext()) {
        Entry<String, TableScanOperator> e = it.next();
        for (Operator<?> op : OperatorUtils.findOperators(e.getValue(), Operator.class)) {
          if (!visited.contains(op)) {
            Set<Operator<?>> workCachedOps = findWorkOperators(optimizerCache, op);
            Set<Operator<?>> workPlanOps = findWorkOperators(op, new HashSet<>());
            if (!workCachedOps.equals(workPlanOps)) {
              throw new SemanticException("Error in shared work optimizer: operator cache contents "
                  + "and actual plan differ\nIn cache: " + workCachedOps + "\nIn plan: " + workPlanOps);
            }
            visited.add(op);
          }
        }
      }
    }

    LOG.info("SharedWorkOptimizer end");
    return pctx;
  }

  private boolean runSharedWorkOptimization(ParseContext pctx, SharedWorkOptimizerCache optimizerCache, List<TableScanOperator> scans,
      Mode mode) throws SemanticException {
    boolean ret = false;
    ret |= sharedWorkOptimization(pctx, optimizerCache, scans, mode, false);
    if (pctx.getConf().getBoolVar(ConfVars.HIVE_SHARED_WORK_MERGE_TS_SCHEMA)) {
      ret |= sharedWorkOptimization(pctx, optimizerCache, scans, mode, true);
    }
    return ret;
  }

  /** SharedWorkOptimization strategy modes */
  public enum Mode {
    /**
     * Merges two identical subtrees.
     */
    SubtreeMerge,
    /**
     * Merges a filtered scan into a non-filtered scan.
     *
     * In case we are already scanning the whole table - we should not scan it twice.
     */
    RemoveSemijoin,
    /**
     * Fuses two filtered table scans into a single one.
     *
     * Dynamic filter subtree is kept on both sides - but the table is onlt scanned once.
     */
    DPPUnion;
  }

  /**
   * Analyzes the TS and exposes dynamic filters separetly.
   */
  static class DecomposedTs {

    private TableScanOperator ts;
    private ExprNodeDesc normalFilterExpr;
    private List<ExprNodeDesc> semijoinExprNodes = new ArrayList<>();

    public DecomposedTs(TableScanOperator ts) throws UDFArgumentException {
      this.ts = ts;
      TableScanOperator retainableTsOp = ts;
      if (retainableTsOp.getConf().getFilterExpr() != null) {
        // Gather SJ expressions and normal expressions
        List<ExprNodeDesc> allExprNodesExceptSemijoin = new ArrayList<>();
        splitExpressions(retainableTsOp.getConf().getFilterExpr(),
            allExprNodesExceptSemijoin, semijoinExprNodes);
        // Create new expressions
        normalFilterExpr = conjunction(allExprNodesExceptSemijoin);
      }
    }

    public List<ExprNodeDesc> getSemiJoinFilter() {
      return semijoinExprNodes;
    }

    public void replaceTabAlias(String oldAlias, String newAlias) {
      ExprNodeDescUtils.replaceTabAlias(normalFilterExpr, oldAlias, newAlias);
      for (ExprNodeDesc expr : semijoinExprNodes) {
        ExprNodeDescUtils.replaceTabAlias(expr, oldAlias, newAlias);
      }
      List<Operator<? extends OperatorDesc>> children = ts.getChildOperators();
      for (Operator<? extends OperatorDesc> c : children) {
        c.replaceTabAlias(oldAlias, newAlias);
      }
    }

    public ExprNodeDesc getFullFilterExpr() throws UDFArgumentException {
      return conjunction(semijoinExprNodes, normalFilterExpr);
    }

  }

  /**
   * Class wrapping shared work optimizer.
   * This implementation enables merging of TS with different schemas by taking the union of the
   * {@link TableScanDesc#getNeededColumns()} and {@link TableScanDesc#getNeededColumnIDs()}
   * from both {@link TableScanOperator}s.
   */

  public boolean sharedWorkOptimization(ParseContext pctx, SharedWorkOptimizerCache optimizerCache,
      List<TableScanOperator> tableScans, Mode mode, boolean schemaMerge) throws SemanticException {
    // Boolean to keep track of whether this method actually merged any TS operators
    boolean mergedExecuted = false;

    Set<TableScanOperator> retainedScans = new LinkedHashSet<>();
    Set<Operator<?>> removedOps = new HashSet<>();
    for (TableScanOperator discardableTsOp : tableScans) {
      TableName tableName1 = discardableTsOp.getTableName();
        if (discardableTsOp.getNumChild() == 0) {
          removedOps.add(discardableTsOp);
        }
      if (removedOps.contains(discardableTsOp)) {
        LOG.debug("Skip {} as it has already been removed", discardableTsOp);
        continue;
      }
      for (TableScanOperator retainableTsOp : retainedScans) {
        if (optimizerCache.getWorkGroup(discardableTsOp).contains(retainableTsOp)) {
          LOG.trace("No need check further {} and {} are in the same group", discardableTsOp, retainableTsOp);
          continue;
        }
        if (removedOps.contains(retainableTsOp)) {
          LOG.debug("Skip {} as it has already been removed", retainableTsOp);
          continue;
        }
        LOG.debug("Can we merge {} into {} to remove a scan on {}?", discardableTsOp, retainableTsOp, tableName1);

        SharedResult sr;

        // If Iceberg metadata tables are in the query, disable this optimisation.
        String metaTable1 = retainableTsOp.getConf().getTableMetadata().getMetaTable();
        String metaTable2 = discardableTsOp.getConf().getTableMetadata().getMetaTable();
        if (metaTable1 != null || metaTable2 != null) {
          LOG.info("Skip the schema merging as the query contains Iceberg metadata table.");
          continue;
        }

        if (!schemaMerge && !compatibleSchema(retainableTsOp, discardableTsOp)) {
          LOG.debug("incompatible schemas: {} {} for {} (and merge disabled)", discardableTsOp, retainableTsOp,
              tableName1);
          continue;
        }

        if (mode == Mode.RemoveSemijoin) {
          // We check if the two table scan operators can actually be merged modulo SJs.
          // Hence, two conditions should be met:
          // (i) the TS ops should be mergeable excluding any kind of DPP, and
          // (ii) the DPP branches (excluding SJs) should be the same
          boolean mergeable = areMergeable(pctx, retainableTsOp, discardableTsOp);
          if (!mergeable) {
            // Skip
            LOG.debug("{} and {} cannot be merged", retainableTsOp, discardableTsOp);
            continue;
          }
          boolean validMerge =
              areMergeableExcludeSemijoinsExtendedCheck(pctx, optimizerCache, retainableTsOp, discardableTsOp);
          if (!validMerge) {
            // Skip
            LOG.debug("{} and {} do not meet preconditions", retainableTsOp, discardableTsOp);
            continue;
          }

          // If tests pass, we create the shared work optimizer additional information
          // about the part of the tree that can be merged. We need to regenerate the
          // cache because semijoin operators have been removed
          sr = extractSharedOptimizationInfoForRoot(pctx, optimizerCache, retainableTsOp, discardableTsOp, true, true);
        } else if (mode == Mode.DPPUnion) {
          boolean mergeable = areMergeable(pctx, retainableTsOp, discardableTsOp);
          if (!mergeable) {
            LOG.debug("{} and {} cannot be merged", retainableTsOp, discardableTsOp);
            continue;
          }
          boolean validMerge = areMergeableDppUnion(pctx, optimizerCache, retainableTsOp, discardableTsOp);
          if (!validMerge) {
            // Skip
            LOG.debug("{} and {} do not meet preconditions", retainableTsOp, discardableTsOp);
            continue;
          }

          // If tests pass, we create the shared work optimizer additional information
          // about the part of the tree that can be merged. We need to regenerate the
          // cache because semijoin operators have been removed
          sr = extractSharedOptimizationInfoForRoot(pctx, optimizerCache, retainableTsOp, discardableTsOp, false,
              false);
          if (!validPreConditions(pctx, optimizerCache, sr)) {
            continue;
          }
        } else if (mode == Mode.SubtreeMerge) {
          // First we quickly check if the two table scan operators can actually be merged
          if (!areMergeable(pctx, retainableTsOp, discardableTsOp)
              || !areMergeableExtendedCheck(pctx, optimizerCache, retainableTsOp, discardableTsOp)) {
            // Skip
            LOG.debug("{} and {} cannot be merged", retainableTsOp, discardableTsOp);
            continue;
          }

          // Secondly, we extract information about the part of the tree that can be merged
          // as well as some structural information (memory consumption) that needs to be
          // used to determined whether the merge can happen
          sr = extractSharedOptimizationInfoForRoot(pctx, optimizerCache, retainableTsOp, discardableTsOp, true, true);

          // It seems these two operators can be merged.
          // Check that plan meets some preconditions before doing it.
          // In particular, in the presence of map joins in the upstream plan:
          // - we cannot exceed the noconditional task size, and
          // - if we already merged the big table, we cannot merge the broadcast
          // tables.
          if (!validPreConditions(pctx, optimizerCache, sr)) {
            // Skip
            LOG.debug("{} and {} do not meet preconditions", retainableTsOp, discardableTsOp);
            continue;
          }
        } else {
          throw new RuntimeException("unhandled mode: " + mode);
        }

        // We can merge
        mergedExecuted = true;
        if (mode != Mode.DPPUnion && sr.retainableOps.size() > 1) {
          // More than TS operator
          Operator<?> lastRetainableOp = sr.retainableOps.get(sr.retainableOps.size() - 1);
          Operator<?> lastDiscardableOp = sr.discardableOps.get(sr.discardableOps.size() - 1);
          if (lastDiscardableOp.getNumChild() != 0) {
            List<Operator<? extends OperatorDesc>> allChildren =
                Lists.newArrayList(lastDiscardableOp.getChildOperators());
            for (Operator<? extends OperatorDesc> op : allChildren) {
              lastDiscardableOp.getChildOperators().remove(op);
              op.replaceParent(lastDiscardableOp, lastRetainableOp);
              lastRetainableOp.getChildOperators().add(op);
            }
          }

          LOG.debug("Merging subtree starting at {} into subtree starting at {}", discardableTsOp, retainableTsOp);
        } else {

          if (sr.discardableOps.size() > 1) {
            throw new RuntimeException("we can't discard more in this path");
          }

          DecomposedTs modelR = new DecomposedTs(retainableTsOp);
          DecomposedTs modelD = new DecomposedTs(discardableTsOp);

          // Push filter on top of children for retainable
          pushFilterToTopOfTableScan(optimizerCache, modelR);

          if (mode == Mode.RemoveSemijoin || mode == Mode.SubtreeMerge) {
            // For RemoveSemiJoin; this will clear the discardable's semijoin filters
            replaceSemijoinExpressions(discardableTsOp, modelR.getSemiJoinFilter());
          }

          modelD.replaceTabAlias(discardableTsOp.getConf().getAlias(), retainableTsOp.getConf().getAlias());

          // Push filter on top of children for discardable
          pushFilterToTopOfTableScan(optimizerCache, modelD);


          // Obtain filter for shared TS operator
          ExprNodeDesc exprNode = null;
          if (modelR.normalFilterExpr != null && modelD.normalFilterExpr != null) {
            exprNode = disjunction(modelR.normalFilterExpr, modelD.normalFilterExpr);
          }
          List<ExprNodeDesc> semiJoinExpr = null;
          if (mode == Mode.DPPUnion) {
            assert modelR.semijoinExprNodes != null;
            assert modelD.semijoinExprNodes != null;
            ExprNodeDesc disjunction =
                disjunction(conjunction(modelR.semijoinExprNodes), conjunction(modelD.semijoinExprNodes));
            semiJoinExpr = disjunction == null ? null : Lists.newArrayList(disjunction);
          } else {
            semiJoinExpr = modelR.semijoinExprNodes;
          }

          // Create expression node that will be used for the retainable table scan
          exprNode = conjunction(semiJoinExpr, exprNode);
          // Replace filter
          retainableTsOp.getConf().setFilterExpr((ExprNodeGenericFuncDesc) exprNode);
          // Replace table scan operator
          adoptChildren(retainableTsOp, discardableTsOp);

          LOG.debug("Merging {} into {}", discardableTsOp, retainableTsOp);
        }

        // First we remove the input operators of the expression that
        // we are going to eliminate
        if (mode != Mode.DPPUnion) {
          for (Operator<?> op : sr.discardableInputOps) {
            OperatorUtils.removeOperator(op);
            optimizerCache.removeOp(op);
            removedOps.add(op);
            // Remove DPP predicates
            if (op instanceof ReduceSinkOperator) {
              SemiJoinBranchInfo sjbi = pctx.getRsToSemiJoinBranchInfo().get(op);
              if (sjbi != null && !sr.discardableOps.contains(sjbi.getTsOp())
                  && !sr.discardableInputOps.contains(sjbi.getTsOp())) {
                GenTezUtils.removeSemiJoinOperator(pctx, (ReduceSinkOperator) op, sjbi.getTsOp());
                optimizerCache.tableScanToDPPSource.remove(sjbi.getTsOp(), op);
              }
            } else if (op instanceof AppMasterEventOperator) {
              DynamicPruningEventDesc dped = (DynamicPruningEventDesc) op.getConf();
              if (!sr.discardableOps.contains(dped.getTableScan())
                  && !sr.discardableInputOps.contains(dped.getTableScan())) {
                GenTezUtils.removeSemiJoinOperator(pctx, (AppMasterEventOperator) op, dped.getTableScan());
                optimizerCache.tableScanToDPPSource.remove(dped.getTableScan(), op);
              }
            }
            LOG.debug("Input operator removed: {}", op);
          }
        }
        // A shared TSop across branches can not have probeContext that utilizes single branch info
        // Filtered-out rows from one branch might be needed by another branch sharing a TSop
        if (retainableTsOp.getProbeDecodeContext() != null) {
          LOG.debug("Removing probeDecodeCntx for merged TS op {}", retainableTsOp);
          retainableTsOp.setProbeDecodeContext(null);
          retainableTsOp.getConf().setProbeDecodeContext(null);
        }

        // Then we merge the operators of the works we are going to merge
        mergeSchema(discardableTsOp, retainableTsOp);

        if (mode == Mode.DPPUnion) {
          // reparent all
          Collection<Operator<?>> discardableDPP = optimizerCache.tableScanToDPPSource.get(discardableTsOp);
          for (Operator<?> op : discardableDPP) {
            if (op instanceof ReduceSinkOperator) {
              SemiJoinBranchInfo sjInfo = pctx.getRsToSemiJoinBranchInfo().get(op);
              sjInfo.setTableScan(retainableTsOp);
            } else if (op.getConf() instanceof DynamicPruningEventDesc) {
              DynamicPruningEventDesc dynamicPruningEventDesc = (DynamicPruningEventDesc) op.getConf();
              dynamicPruningEventDesc.setTableScan(retainableTsOp);
            }
          }
          optimizerCache.tableScanToDPPSource.get(retainableTsOp).addAll(discardableDPP);
          discardableDPP.clear();
        }
        optimizerCache.removeOpAndCombineWork(discardableTsOp, retainableTsOp);

        removedOps.add(discardableTsOp);
        // Finally we remove the expression from the tree
        for (Operator<?> op : sr.discardableOps) {
          OperatorUtils.removeOperator(op);
          optimizerCache.removeOp(op);
          removedOps.add(op);
          LOG.debug("Operator removed: {}", op);
        }

        if (pctx.getConf().getBoolVar(ConfVars.HIVE_SHARED_WORK_DOWNSTREAM_MERGE)) {
          if (sr.discardableOps.size() == 1) {
            downStreamMerge(retainableTsOp, optimizerCache, pctx);
          }
        }

        break;
      }

      if (removedOps.contains(discardableTsOp)) {
        // This operator has been removed, remove it from the list of existing operators
        // FIXME: there is no point of this
        retainedScans.remove(discardableTsOp);
      } else {
        // This operator has not been removed, include it in the list of existing operators
        retainedScans.add(discardableTsOp);
      }
    }

    // Remove unused table scan operators
    pctx.getTopOps().entrySet().removeIf((Entry<String, TableScanOperator> e) -> e.getValue().getNumChild() == 0);

    tableScans.removeAll(removedOps);

    return mergedExecuted;
  }

  // FIXME: probably this should also be integrated with isSame() logics
  protected boolean areMergeable(ParseContext pctx, TableScanOperator tsOp1, TableScanOperator tsOp2)
      throws SemanticException {
    // If row limit does not match, we currently do not merge
    if (tsOp1.getConf().getRowLimit() != tsOp2.getConf().getRowLimit()) {
      LOG.debug("rowlimit differ {} ~ {}", tsOp1.getConf().getRowLimit(), tsOp2.getConf().getRowLimit());
      return false;
    }
    // If table properties do not match, we currently do not merge
    if (!Objects.equals(tsOp1.getConf().getOpProps(), tsOp2.getConf().getOpProps())) {
      LOG.debug("opProps differ {} ~ {}", tsOp1.getConf().getOpProps(), tsOp2.getConf().getOpProps());
      return false;
    }
    // If partitions do not match, we currently do not merge
    PrunedPartitionList prevTsOpPPList = pctx.getPrunedPartitions(tsOp1);
    PrunedPartitionList tsOpPPList = pctx.getPrunedPartitions(tsOp2);
    if (!prevTsOpPPList.getPartitions().equals(tsOpPPList.getPartitions())) {
      LOG.debug("partitions differ {} ~ {}", prevTsOpPPList.getPartitions().size(), tsOpPPList.getPartitions().size());
      return false;
    }

    if (!Objects.equals(tsOp1.getConf().getIncludedBuckets(), tsOp2.getConf().getIncludedBuckets())) {
      LOG.debug("includedBuckets differ {} ~ {}", tsOp1.getConf().getIncludedBuckets(),
          tsOp2.getConf().getIncludedBuckets());
      return false;
    }

    return true;
  }

  protected void mergeSchema(TableScanOperator discardableTsOp, TableScanOperator retainableTsOp) {
    for (int colId : discardableTsOp.getConf().getNeededColumnIDs()) {
      if (!retainableTsOp.getConf().getNeededColumnIDs().contains(colId)) {
        retainableTsOp.getConf().getNeededColumnIDs().add(colId);
      }
    }
    for (String col : discardableTsOp.getConf().getNeededColumns()) {
      if (!retainableTsOp.getConf().getNeededColumns().contains(col)) {
        retainableTsOp.getConf().getNeededColumns().add(col);
      }
    }
    for (VirtualColumn col : discardableTsOp.getConf().getVirtualCols()) {
      if (!retainableTsOp.getConf().getVirtualCols().contains(col)) {
        retainableTsOp.getConf().getVirtualCols().add(col);
      }
    }
  }

  private static boolean compatibleSchema(TableScanOperator tsOp1, TableScanOperator tsOp2) {
    return Objects.equals(tsOp1.getNeededColumns(), tsOp2.getNeededColumns())
        && Objects.equals(tsOp1.getNeededColumnIDs(), tsOp2.getNeededColumnIDs())
        && Objects.equals(tsOp1.getConf().getVirtualCols(), tsOp2.getConf().getVirtualCols());
  }


  /**
   * When we call this method, we have already verified that the SJ expressions targeting
   * two TS operators are the same.
   * Since we already had a method to push the filter expressions on top of the discardable
   * TS (pushFilterToTopOfTableScan), here we remove the old SJ expressions from the
   * discardable TS (and follow-up Filters if present) and we add the SJ expressions
   * from the retainable TS. That way the SJ expressions will be pushed on top of the
   * discardable TS by pushFilterToTopOfTableScan.
   */
  private static void replaceSemijoinExpressions(TableScanOperator tsOp, List<ExprNodeDesc> semijoinExprNodes) {
    ExprNodeDesc constNode = new ExprNodeConstantDesc(
        TypeInfoFactory.booleanTypeInfo, Boolean.TRUE);
    // TS operator
    if (tsOp.getConf().getFilterExpr() != null) {
      ExprNodeDesc tsFilterExpr = tsOp.getConf().getFilterExpr();
      if (FunctionRegistry.isOpAnd(tsFilterExpr)) {
        tsFilterExpr.getChildren().removeIf(SharedWorkOptimizer::isSemijoinExpr);
        tsFilterExpr.getChildren().addAll(semijoinExprNodes);
        if (tsFilterExpr.getChildren().isEmpty() ||
            (tsFilterExpr.getChildren().size() == 1 && !(tsFilterExpr.getChildren().get(0) instanceof ExprNodeGenericFuncDesc))) {
          tsOp.getConf().setFilterExpr(null);
        }
      }
    }
    // Filter operators on top
    if (tsOp.getChildOperators() != null) {
      for (Operator op : tsOp.getChildOperators()) {
        if (op instanceof FilterOperator) {
          FilterOperator filterOp = (FilterOperator) op;
          ExprNodeDesc filterExpr = filterOp.getConf().getPredicate();
          if (FunctionRegistry.isOpAnd(filterExpr)) {
            filterExpr.getChildren().removeIf(SharedWorkOptimizer::isSemijoinExpr);
            if (filterExpr.getChildren().isEmpty()) {
              filterOp.getConf().setPredicate(constNode);
            } else if (filterExpr.getChildren().size() == 1) {
              filterOp.getConf().setPredicate(filterExpr.getChildren().get(0));
            }
          }
        }
      }
    }
  }

  private static void downStreamMerge(Operator<?> op, SharedWorkOptimizerCache optimizerCache, ParseContext pctx)
      throws SemanticException {
    List<Operator<?>> childs = op.getChildOperators();
    for (int i = 0; i < childs.size(); i++) {
      Operator<?> cI = childs.get(i);
      if (cI instanceof ReduceSinkOperator || cI instanceof JoinOperator || cI.getParentOperators().size() != 1) {
        continue;
      }
      for (int j = i + 1; j < childs.size(); j++) {
        Operator<?> cJ = childs.get(j);
        if (cI.logicalEquals(cJ)) {
          LOG.debug("downstream merge: from {} into {}", cJ, cI);
          adoptChildren(cI, cJ);
          op.removeChild(cJ);
          optimizerCache.removeOp(cJ);
          j--;
          downStreamMerge(cI, optimizerCache, pctx);
        }
      }
    }
  }

  private static void adoptChildren(Operator<?> target, Operator<?> donor) {
    List<Operator<?>> children = donor.getChildOperators();
    for (Operator<?> c : children) {
      c.replaceParent(donor, target);
    }
    target.getChildOperators().addAll(children);
    children.clear();
  }

  private static boolean isSemijoinExpr(ExprNodeDesc expr) {
    if (expr instanceof ExprNodeDynamicListDesc) {
      // DYNAMIC PARTITION PRUNING
      return true;
    }
    if (FunctionRegistry.isOpBetween(expr) &&
        expr.getChildren().get(2) instanceof ExprNodeDynamicValueDesc) {
      // BETWEEN in SJ
      return true;
    }
    if (FunctionRegistry.isOpInBloomFilter(expr) &&
        expr.getChildren().get(1) instanceof ExprNodeDynamicValueDesc) {
      // IN_BLOOM_FILTER in SJ
      return true;
    }
    return false;
  }

  private static void splitExpressions(ExprNodeDesc exprNode,
      List<ExprNodeDesc> allExprNodesExceptSemijoin, List<ExprNodeDesc> semijoinExprNodes) {
    if (FunctionRegistry.isOpAnd(exprNode)) {
      for (ExprNodeDesc expr : exprNode.getChildren()) {
        if (isSemijoinExpr(expr)) {
          semijoinExprNodes.add(expr);
        } else {
          allExprNodesExceptSemijoin.add(expr);
        }
      }
    } else if (isSemijoinExpr(exprNode)) {
      semijoinExprNodes.add(exprNode);
    } else {
      allExprNodesExceptSemijoin.add(exprNode);
    }
  }

  private static void sharedWorkExtendedOptimization(ParseContext pctx, SharedWorkOptimizerCache optimizerCache)
      throws SemanticException {
    // Gather RS operators that 1) belong to root works, i.e., works containing TS operators,
    // and 2) share the same input operator.
    // These will be the first target for extended shared work optimization
    Multimap<Operator<?>, ReduceSinkOperator> parentToRsOps = ArrayListMultimap.create();
    Set<Operator<?>> visited = new HashSet<>();
    for (Entry<String, TableScanOperator> e : pctx.getTopOps().entrySet()) {
      gatherReduceSinkOpsByInput(parentToRsOps, visited,
          findWorkOperators(optimizerCache, e.getValue()));
    }

    Set<Operator<?>> removedOps = new HashSet<>();
    while (!parentToRsOps.isEmpty()) {
      // As above, we enforce a certain order when we do the reutilization.
      // In particular, we use size of data in RS x number of uses.
      List<Entry<Operator<?>, Long>> sortedRSGroups =
          rankOpsByAccumulatedSize(parentToRsOps.keySet());
      LOG.debug("Sorted operators by size: {}", sortedRSGroups);

      // Execute extended optimization
      // For each RS, check whether other RS in same work could be merge into this one.
      // If they are merged, RS operators in the resulting work will be considered
      // mergeable in next loop iteration.
      Multimap<Operator<?>, ReduceSinkOperator> existingRsOps = ArrayListMultimap.create();
      for (Entry<Operator<?>, Long> rsGroupInfo : sortedRSGroups) {
        Operator<?> rsParent = rsGroupInfo.getKey();
        for (ReduceSinkOperator discardableRsOp : parentToRsOps.get(rsParent)) {
          if (removedOps.contains(discardableRsOp)) {
            LOG.debug("Skip {} as it has already been removed", discardableRsOp);
            continue;
          }
          Collection<ReduceSinkOperator> otherRsOps = existingRsOps.get(rsParent);
          for (ReduceSinkOperator retainableRsOp : otherRsOps) {
            if (retainableRsOp.getChildOperators().size() == 0) {
              // just skip this RS - its a semijoin/bloomfilter related RS
              continue;
            }
            if (removedOps.contains(retainableRsOp)) {
              LOG.debug("Skip {} as it has already been removed", retainableRsOp);
              continue;
            }

            // First we quickly check if the two RS operators can actually be merged.
            // We already know that these two RS operators have the same parent, but
            // we need to check whether both RS are actually equal. Further, we check
            // whether their child is also equal. If any of these conditions are not
            // met, we are not going to try to merge.
            boolean mergeable = compareOperator(pctx, retainableRsOp, discardableRsOp) &&
                compareOperator(pctx, retainableRsOp.getChildOperators().get(0),
                    discardableRsOp.getChildOperators().get(0));
            if (!mergeable) {
              // Skip
              LOG.debug("{} and {} cannot be merged", retainableRsOp, discardableRsOp);
              continue;
            }

            LOG.debug("Checking additional conditions for merging subtree starting at {}"
                + " into subtree starting at {}", discardableRsOp, retainableRsOp);

            // Secondly, we extract information about the part of the tree that can be merged
            // as well as some structural information (memory consumption) that needs to be
            // used to determined whether the merge can happen
            Operator<?> retainableRsOpChild = retainableRsOp.getChildOperators().get(0);
            Operator<?> discardableRsOpChild = discardableRsOp.getChildOperators().get(0);
            SharedResult sr = extractSharedOptimizationInfo(
                pctx, optimizerCache, retainableRsOp, discardableRsOp,
                retainableRsOpChild, discardableRsOpChild);

            // It seems these two operators can be merged.
            // Check that plan meets some preconditions before doing it.
            // In particular, in the presence of map joins in the upstream plan:
            // - we cannot exceed the noconditional task size, and
            // - if we already merged the big table, we cannot merge the broadcast
            // tables.
            if (sr.retainableOps.isEmpty() || !validPreConditions(pctx, optimizerCache, sr)) {
              // Skip
              LOG.debug("{} and {} do not meet preconditions", retainableRsOp, discardableRsOp);
              continue;
            }

            deduplicateReduceTraits(retainableRsOp.getConf(), discardableRsOp.getConf());

            // We can merge
            Operator<?> lastRetainableOp = sr.retainableOps.get(sr.retainableOps.size() - 1);
            Operator<?> lastDiscardableOp = sr.discardableOps.get(sr.discardableOps.size() - 1);
            if (lastDiscardableOp.getNumChild() != 0) {
              List<Operator<? extends OperatorDesc>> allChildren =
                  Lists.newArrayList(lastDiscardableOp.getChildOperators());
              for (Operator<? extends OperatorDesc> op : allChildren) {
                lastDiscardableOp.getChildOperators().remove(op);
                op.replaceParent(lastDiscardableOp, lastRetainableOp);
                lastRetainableOp.getChildOperators().add(op);
              }
            }

            LOG.debug("Merging subtree starting at {} into subtree starting at {}",
                discardableRsOp, retainableRsOp);

            // First we remove the input operators of the expression that
            // we are going to eliminate
            for (Operator<?> op : sr.discardableInputOps) {
              OperatorUtils.removeOperator(op);
              optimizerCache.removeOp(op);
              removedOps.add(op);
              // Remove DPP predicates
              if (op instanceof ReduceSinkOperator) {
                SemiJoinBranchInfo sjbi = pctx.getRsToSemiJoinBranchInfo().get(op);
                if (sjbi != null && !sr.discardableOps.contains(sjbi.getTsOp()) &&
                    !sr.discardableInputOps.contains(sjbi.getTsOp())) {
                  GenTezUtils.removeSemiJoinOperator(
                      pctx, (ReduceSinkOperator) op, sjbi.getTsOp());
                  optimizerCache.tableScanToDPPSource.remove(sjbi.getTsOp(), op);
                }
              } else if (op instanceof AppMasterEventOperator) {
                DynamicPruningEventDesc dped = (DynamicPruningEventDesc) op.getConf();
                if (!sr.discardableOps.contains(dped.getTableScan()) &&
                    !sr.discardableInputOps.contains(dped.getTableScan())) {
                  GenTezUtils.removeSemiJoinOperator(
                      pctx, (AppMasterEventOperator) op, dped.getTableScan());
                  optimizerCache.tableScanToDPPSource.remove(dped.getTableScan(), op);
                }
              }
              LOG.debug("Input operator removed: {}", op);
            }
            // We remove the discardable RS operator
            OperatorUtils.removeOperator(discardableRsOp);
            optimizerCache.removeOp(discardableRsOp);
            removedOps.add(discardableRsOp);
            LOG.debug("Operator removed: {}", discardableRsOp);
            // Then we merge the operators of the works we are going to merge
            optimizerCache.removeOpAndCombineWork(discardableRsOpChild, retainableRsOpChild);
            // Finally we remove the rest of the expression from the tree
            for (Operator<?> op : sr.discardableOps) {
              OperatorUtils.removeOperator(op);
              optimizerCache.removeOp(op);
              removedOps.add(op);
              LOG.debug("Operator removed: {}", op);
            }

            if (pctx.getConf().getBoolVar(ConfVars.HIVE_SHARED_WORK_DOWNSTREAM_MERGE)) {
              if (sr.discardableOps.size() == 1) {
                downStreamMerge(retainableRsOp, optimizerCache, pctx);
              }
            }

            break;
          }

          if (removedOps.contains(discardableRsOp)) {
            // This operator has been removed, remove it from the list of existing operators
            existingRsOps.remove(rsParent, discardableRsOp);
          } else {
            // This operator has not been removed, include it in the list of existing operators
            existingRsOps.put(rsParent, discardableRsOp);
          }
        }
      }

      // We gather the operators that will be used for next iteration of extended optimization
      // (if any)
      parentToRsOps = ArrayListMultimap.create();
      visited = new HashSet<>();
      for (Entry<Operator<?>, ReduceSinkOperator> e : existingRsOps.entries()) {
        if (removedOps.contains(e.getValue()) || e.getValue().getNumChild() < 1) {
          // If 1) RS has been removed, or 2) it does not have a child (for instance, it is a
          // semijoin RS), we can quickly skip this one
          continue;
        }
        gatherReduceSinkOpsByInput(parentToRsOps, visited,
            findWorkOperators(optimizerCache, e.getValue().getChildOperators().get(0)));
      }
    }

    // Remove unused table scan operators
    pctx.getTopOps().entrySet().removeIf(
        (Entry<String, TableScanOperator> e) -> e.getValue().getNumChild() == 0);
  }

  /**
   * Try to reuse cache for broadcast side in mapjoin operators that share the same input.
   */
  @VisibleForTesting
  public void runMapJoinCacheReuseOptimization(
      ParseContext pctx, SharedWorkOptimizerCache optimizerCache) throws SemanticException {
    // First we group together all the mapjoin operators whose first broadcast input is the same.
    final Multimap<Operator<?>, MapJoinOperator> parentToMapJoinOperators = ArrayListMultimap.create();
    for (Set<Operator<?>> workOperators : optimizerCache.getWorkGroups()) {
      for (Operator<?> op : workOperators) {
        if (op instanceof MapJoinOperator) {
          MapJoinOperator mapJoinOp = (MapJoinOperator) op;
          // Only allowed for mapjoin operator
          if (!mapJoinOp.getConf().isBucketMapJoin() &&
              !mapJoinOp.getConf().isDynamicPartitionHashJoin()) {
            parentToMapJoinOperators.put(
                obtainFirstBroadcastInput(mapJoinOp).getParentOperators().get(0), mapJoinOp);
          }
        }
      }
    }

    // For each group, set the cache key accordingly if there is more than one operator
    // and input RS operator are equal
    for (Collection<MapJoinOperator> c : parentToMapJoinOperators.asMap().values()) {
      Map<MapJoinOperator, String> mapJoinOpToCacheKey = new HashMap<>();
      for (MapJoinOperator mapJoinOp : c) {
        String cacheKey = null;
        for (Entry<MapJoinOperator, String> e: mapJoinOpToCacheKey.entrySet()) {
          if (canShareBroadcastInputs(pctx, mapJoinOp, e.getKey())) {
            cacheKey = e.getValue();
            break;
          }
        }

        if (cacheKey == null) {
          // Either it is the first map join operator or there was no equivalent broadcast input,
          // hence generate cache key
          cacheKey = MapJoinDesc.generateCacheKey(mapJoinOp.getOperatorId());
          mapJoinOpToCacheKey.put(mapJoinOp, cacheKey);
        }

        mapJoinOp.getConf().setCacheKey(cacheKey);
      }
    }
  }

  private ReduceSinkOperator obtainFirstBroadcastInput(MapJoinOperator mapJoinOp) {
    return mapJoinOp.getParentOperators().get(0) instanceof ReduceSinkOperator ?
        (ReduceSinkOperator) mapJoinOp.getParentOperators().get(0) :
        (ReduceSinkOperator) mapJoinOp.getParentOperators().get(1);
  }

  private boolean canShareBroadcastInputs(
      ParseContext pctx, MapJoinOperator mapJoinOp1, MapJoinOperator mapJoinOp2) throws SemanticException {
    if (mapJoinOp1.getNumParent() != mapJoinOp2.getNumParent()) {
      return false;
    }

    if (mapJoinOp1.getConf().getPosBigTable() != mapJoinOp2.getConf().getPosBigTable()) {
      return false;
    }

    for (int i = 0; i < mapJoinOp1.getNumParent(); i ++) {
      if (i == mapJoinOp1.getConf().getPosBigTable()) {
        continue;
      }

      ReduceSinkOperator parentRS1 = (ReduceSinkOperator) mapJoinOp1.getParentOperators().get(i);
      ReduceSinkOperator parentRS2 = (ReduceSinkOperator) mapJoinOp2.getParentOperators().get(i);

      if (!compareOperator(pctx, parentRS1, parentRS2)) {
        return false;
      }

      Operator<?> grandParent1 = parentRS1.getParentOperators().get(0);
      Operator<?> grandParent2 = parentRS2.getParentOperators().get(0);

      if (grandParent1 != grandParent2) {
        return false;
      }
    }

    return true;
  }

  /**
   * This method gathers the TS operators with DPP from the context and
   * stores them into the input optimization cache.
   */
  private static void gatherDPPTableScanOps(
          ParseContext pctx, SharedWorkOptimizerCache optimizerCache) throws SemanticException {
    // Find TS operators with partition pruning enabled in plan
    // because these TS may potentially read different data for
    // different pipeline.
    // These can be:
    // 1) TS with DPP.
    // 2) TS with semijoin DPP.
    Map<String, TableScanOperator> topOps = pctx.getTopOps();
    Collection<Operator<?>> tableScanOps = Lists.<Operator<?>> newArrayList(topOps.values());
    Set<AppMasterEventOperator> s = OperatorUtils.findOperators(tableScanOps, AppMasterEventOperator.class);
    for (AppMasterEventOperator a : s) {
      if (a.getConf() instanceof DynamicPruningEventDesc) {
        DynamicPruningEventDesc dped = (DynamicPruningEventDesc) a.getConf();
        optimizerCache.tableScanToDPPSource.put(dped.getTableScan(), a);
      }
    }
    for (Entry<ReduceSinkOperator, SemiJoinBranchInfo> e : pctx.getRsToSemiJoinBranchInfo().entrySet()) {
      optimizerCache.tableScanToDPPSource.put(e.getValue().getTsOp(), e.getKey());
    }
    LOG.debug("DPP information stored in the cache: {}", optimizerCache.tableScanToDPPSource);
  }

  /**
   * Orders TS operators in decreasing order by "weight".
   */
  static class TSComparator implements Comparator<TableScanOperator> {

    @Override
    public int compare(TableScanOperator o1, TableScanOperator o2) {
      int r;
      r=cmpFiltered(o1,o2);
      if(r!=0) {
        return r;
      }
      r=cmpDataSize(o1,o2);
      if(r!=0) {
        return r;
      }

      return o1.toString().compareTo(o2.toString());
    }

    private int cmpFiltered(TableScanOperator o1, TableScanOperator o2) {
      if (o1.getConf().getFilterExpr() == null ^ o2.getConf().getFilterExpr() == null) {
            return (o1.getConf().getFilterExpr() == null) ? -1 : 1;
      }
      return 0;

    }

    private int cmpDataSize(TableScanOperator o1, TableScanOperator o2) {
      long ds1 = o1.getStatistics() == null ? -1 : o1.getStatistics().getDataSize();
      long ds2 = o2.getStatistics() == null ? -1 : o2.getStatistics().getDataSize();
      if (ds1 == ds2) {
        return 0;
      }
      if (ds1 < ds2) {
        return 1;
      } else {
        return -1;
      }
    }
  }

  private static ArrayListMultimap<String, TableScanOperator> splitTableScanOpsByTable(
          ParseContext pctx) {
    ArrayListMultimap<String, TableScanOperator> tableNameToOps = ArrayListMultimap.create();
    // Sort by operator ID so we get deterministic results
    TSComparator comparator = new TSComparator();
    Queue<TableScanOperator> sortedTopOps = new PriorityQueue<>(comparator);
    sortedTopOps.addAll(pctx.getTopOps().values());
    for (TableScanOperator tsOp : sortedTopOps) {
      tableNameToOps.put(tsOp.getTableName().toString(), tsOp);
    }
    return tableNameToOps;
  }

  private static List<Entry<String, Long>> rankTablesByAccumulatedSize(ParseContext pctx) {
    Map<String, Long> tableToTotalSize = new HashMap<>();
    for (Entry<String, TableScanOperator> e : pctx.getTopOps().entrySet()) {
      TableScanOperator tsOp = e.getValue();
      String tableName = tsOp.getTableName().toString();
      long tableSize = tsOp.getStatistics() != null ?
              tsOp.getStatistics().getDataSize() : 0L;
      Long totalSize = tableToTotalSize.get(tableName);
      if (totalSize != null) {
        tableToTotalSize.put(tableName,
                StatsUtils.safeAdd(totalSize, tableSize));
      } else {
        tableToTotalSize.put(tableName, tableSize);
      }
    }
    List<Entry<String, Long>> sortedTables =
        new ArrayList<>(tableToTotalSize.entrySet());
    Collections.sort(sortedTables, Collections.reverseOrder(
        new Comparator<Map.Entry<String, Long>>() {
          @Override
          public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
            return (o1.getValue()).compareTo(o2.getValue());
          }
        }));
    return sortedTables;
  }

  private static void gatherReduceSinkOpsByInput(Multimap<Operator<?>,
      ReduceSinkOperator> parentToRsOps, Set<Operator<?>> visited, Set<Operator<?>> ops) {
    for (Operator<?> op : ops) {
      // If the RS has other RS siblings, we will add it to be considered in next iteration
      if (op instanceof ReduceSinkOperator && !visited.contains(op)) {
        Operator<?> parent = op.getParentOperators().get(0);
        Set<ReduceSinkOperator> s = new LinkedHashSet<>();
        for (Operator<?> c : parent.getChildOperators()) {
          if (c instanceof ReduceSinkOperator) {
            s.add((ReduceSinkOperator) c);
            visited.add(c);
          }
        }
        if (s.size() > 1) {
          parentToRsOps.putAll(parent, s);
        }
      }
    }
  }

  private static List<Entry<Operator<?>, Long>> rankOpsByAccumulatedSize(Set<Operator<?>> opsSet) {
    Map<Operator<?>, Long> opToTotalSize = new HashMap<>();
    for (Operator<?> op : opsSet) {
      long size = op.getStatistics() != null ?
          op.getStatistics().getDataSize() : 0L;
      opToTotalSize.put(op,
          StatsUtils.safeMult(op.getChildOperators().size(), size));
    }
    List<Entry<Operator<?>, Long>> sortedOps =
        new ArrayList<>(opToTotalSize.entrySet());
    Collections.sort(sortedOps, Collections.reverseOrder(
        new Comparator<Map.Entry<Operator<?>, Long>>() {
          @Override
          public int compare(Map.Entry<Operator<?>, Long> o1, Map.Entry<Operator<?>, Long> o2) {
            int valCmp = o1.getValue().compareTo(o2.getValue());
            if (valCmp == 0) {
              return o1.getKey().toString().compareTo(o2.getKey().toString());
            }
            return valCmp;
          }
        }));
    return sortedOps;
  }

  private static boolean areMergeableExtendedCheck(ParseContext pctx, SharedWorkOptimizerCache optimizerCache,
      TableScanOperator tsOp1, TableScanOperator tsOp2) throws SemanticException {
    // If is a DPP, check if actually it refers to same target, column, etc.
    // Further, the DPP value needs to be generated from same subtree
    List<Operator<?>> dppsOp1 = new ArrayList<>(optimizerCache.tableScanToDPPSource.get(tsOp1));
    List<Operator<?>> dppsOp2 = new ArrayList<>(optimizerCache.tableScanToDPPSource.get(tsOp2));
    if (dppsOp1.isEmpty() && dppsOp2.isEmpty()) {
      return true;
    }
    for (int i = 0; i < dppsOp1.size(); i++) {
      Operator<?> op = dppsOp1.get(i);
      if (op instanceof ReduceSinkOperator) {
        Set<Operator<?>> ascendants =
            findAscendantWorkOperators(pctx, optimizerCache, op);
        if (ascendants.contains(tsOp2)) {
          // This should not happen, we cannot merge
          return false;
        }
      }
    }
    for (int i = 0; i < dppsOp2.size(); i++) {
      Operator<?> op = dppsOp2.get(i);
      if (op instanceof ReduceSinkOperator) {
        Set<Operator<?>> ascendants =
            findAscendantWorkOperators(pctx, optimizerCache, op);
        if (ascendants.contains(tsOp1)) {
          // This should not happen, we cannot merge
          return false;
        }
      }
    }
    if (dppsOp1.size() != dppsOp2.size()) {
      // Only first or second operator contains DPP pruning
      return false;
    }
    // Check if DPP branches are equal
    BitSet bs = new BitSet();
    for (int i = 0; i < dppsOp1.size(); i++) {
      Operator<?> dppOp1 = dppsOp1.get(i);
      for (int j = 0; j < dppsOp2.size(); j++) {
        if (!bs.get(j)) {
          // If not visited yet
          Operator<?> dppOp2 = dppsOp2.get(j);
          if (compareAndGatherOps(pctx, optimizerCache, dppOp1, dppOp2) != null) {
            // The DPP operator/branch are equal
            bs.set(j);
            break;
          }
        }
      }
      if (bs.cardinality() < i + 1) {
        return false;
      }
    }
    return true;
  }

  private static boolean areMergeableExcludeSemijoinsExtendedCheck(ParseContext pctx, SharedWorkOptimizerCache optimizerCache,
      TableScanOperator tsOp1, TableScanOperator tsOp2) throws SemanticException {
    // We remove RS-based SJs from consideration, then we compare
    List<Operator<?>> dppsOp1 = new ArrayList<>(optimizerCache.tableScanToDPPSource.get(tsOp1));
    boolean removedDppOp1 = false;
    List<ReduceSinkOperator> rsOpsSemijoin1 = new ArrayList<>();
    List<Operator<?>> dppsOp2 = new ArrayList<>(optimizerCache.tableScanToDPPSource.get(tsOp2));
    boolean removedDppOp2 = false;
    List<ReduceSinkOperator> rsOpsSemijoin2 = new ArrayList<>();
    for (int i = 0; i < dppsOp1.size(); i++) {
      Operator<?> op = dppsOp1.get(i);
      if (op instanceof ReduceSinkOperator) {
        ReduceSinkOperator semijoinRSOp = (ReduceSinkOperator) op;
        if (pctx.getRsToSemiJoinBranchInfo().get(semijoinRSOp).getIsHint()) {
          // This is a hint, we should keep it, hence we bail out
          return false;
        }
        rsOpsSemijoin1.add(semijoinRSOp);
        dppsOp1.remove(i);
        removedDppOp1 = true;
      }
    }
    for (int i = 0; i < dppsOp2.size(); i++) {
      Operator<?> op = dppsOp2.get(i);
      if (op instanceof ReduceSinkOperator) {
        ReduceSinkOperator semijoinRSOp = (ReduceSinkOperator) op;
        if (pctx.getRsToSemiJoinBranchInfo().get(semijoinRSOp).getIsHint()) {
          // This is a hint, we should keep it, hence we bail out
          return false;
        }
        rsOpsSemijoin2.add(semijoinRSOp);
        dppsOp2.remove(i);
        removedDppOp2 = true;
      }
    }
    if (removedDppOp1 && removedDppOp2) {
      // TODO: We do not merge, since currently we only merge when one of the TS operators
      // are not targetted by a SJ edge
      return false;
    }
    if (!removedDppOp1 && !removedDppOp2) {
      // None of them are targetted by a SJ, we skip them
      return false;
    }
    if (dppsOp1.size() != dppsOp2.size()) {
      // We cannot merge, we move to the next couple
      return false;
    }
    // Check if DPP branches are equal
    boolean equalBranches = true;
    BitSet bs = new BitSet();
    for (int i = 0; i < dppsOp1.size(); i++) {
      Operator<?> dppOp1 = dppsOp1.get(i);
      for (int j = 0; j < dppsOp2.size(); j++) {
        if (!bs.get(j)) {
          // If not visited yet
          Operator<?> dppOp2 = dppsOp2.get(j);
          if (compareAndGatherOps(pctx, optimizerCache, dppOp1, dppOp2) != null) {
            // The DPP operator/branch are equal
            bs.set(j);
            break;
          }
        }
      }
      if (bs.cardinality() < i + 1) {
        // We cannot merge, we move to the next group
        equalBranches = false;
        break;
      }
    }
    if (!equalBranches) {
      // Skip
      return false;
    }

    // We reached here, other DPP is the same, these two could potentially be merged.
    // Hence, we perform the last check. To do this, we remove the SJ operators,
    // but we remember their position in the plan. After that, we will reintroduce
    // the SJ operator. If the checks were valid, we will merge and remove the semijoin.
    // If the rest of tests to merge do not pass, we will abort the shared scan optimization
    // and we are done
    TableScanOperator targetTSOp;
    List<ReduceSinkOperator> semijoinRsOps;
    List<SemiJoinBranchInfo> sjBranches = new ArrayList<>();
    if (removedDppOp1) {
      targetTSOp = tsOp1;
      semijoinRsOps = rsOpsSemijoin1;
    } else {
      targetTSOp = tsOp2;
      semijoinRsOps = rsOpsSemijoin2;
    }
    optimizerCache.tableScanToDPPSource.get(targetTSOp).removeAll(semijoinRsOps);
    for (ReduceSinkOperator rsOp : semijoinRsOps) {
      sjBranches.add(pctx.getRsToSemiJoinBranchInfo().remove(rsOp));
    }

    boolean validMerge = validPreConditions(pctx, optimizerCache,
        extractSharedOptimizationInfoForRoot(pctx, optimizerCache, tsOp1, tsOp2, true, true));

    if (validMerge) {
      // We are going to merge, hence we remove the semijoins completely
      for (ReduceSinkOperator semijoinRsOp : semijoinRsOps) {
        Operator<?> branchOp = GenTezUtils.removeBranch(semijoinRsOp);
        while (branchOp != null) {
          optimizerCache.removeOp(branchOp);
          branchOp = branchOp.getNumChild() > 0 ?
              branchOp.getChildOperators().get(0) : null;
        }
        GenTezUtils.removeSemiJoinOperator(pctx, semijoinRsOp, targetTSOp);
      }
    } else {
      // Otherwise, the put the semijoins back in the auxiliary data structures
      optimizerCache.tableScanToDPPSource.get(targetTSOp).addAll(semijoinRsOps);
      for (int i = 0; i < semijoinRsOps.size(); i++) {
        pctx.getRsToSemiJoinBranchInfo().put(semijoinRsOps.get(i), sjBranches.get(i));
      }
    }
    return validMerge;
  }

  private static boolean areMergeableDppUnion(ParseContext pctx,
      SharedWorkOptimizerCache optimizerCache, TableScanOperator tsOp1, TableScanOperator tsOp2)
      throws SemanticException {

    if (!areSupportedDppUnionOps(pctx, optimizerCache, tsOp1, tsOp2)) {
      return false;
    }
    if (!areSupportedDppUnionOps(pctx, optimizerCache, tsOp2, tsOp1)) {
      return false;
    }
    return true;
  }

  private static boolean areSupportedDppUnionOps(ParseContext pctx, SharedWorkOptimizerCache cache, TableScanOperator tsOp1,
      TableScanOperator tsOp2) {
    Collection<Operator<?>> dppOps = cache.tableScanToDPPSource.get(tsOp1);
    if (dppOps.isEmpty()) {
      return false;
    }
    for (Operator<?> op : dppOps) {
      if (op instanceof ReduceSinkOperator) {
        ReduceSinkOperator semijoinRSOp = (ReduceSinkOperator) op;
        if (pctx.getRsToSemiJoinBranchInfo().get(semijoinRSOp).getIsHint()) {
          // This is a hint, we should keep it, hence we bail out
          return false;
        }
      } else if (op.getConf() instanceof DynamicPruningEventDesc) {
        if (!pctx.getConf().getBoolVar(ConfVars.HIVE_SHARED_WORK_DPPUNION_MERGE_EVENTOPS)) {
          return false;
        }
      } else {
        return false;
      }
    }
    return true;
  }

  private static SharedResult extractSharedOptimizationInfoForRoot(ParseContext pctx,
          SharedWorkOptimizerCache optimizerCache,
          TableScanOperator retainableTsOp,
      TableScanOperator discardableTsOp, boolean mayRemoveDownStreamOperators, boolean mayRemoveInputOps)
      throws SemanticException {
    LinkedHashSet<Operator<?>> retainableOps = new LinkedHashSet<>();
    LinkedHashSet<Operator<?>> discardableOps = new LinkedHashSet<>();
    Set<Operator<?>> discardableInputOps = new HashSet<>();
    long dataSize = 0L;
    long maxDataSize = 0L;

    retainableOps.add(retainableTsOp);
    discardableOps.add(discardableTsOp);
    Operator<?> equalOp1 = retainableTsOp;
    Operator<?> equalOp2 = discardableTsOp;
    if (equalOp1.getNumChild() > 1 || equalOp2.getNumChild() > 1) {
      // TODO: Support checking multiple child operators to merge further.
      discardableInputOps.addAll(gatherDPPBranchOps(pctx, optimizerCache, discardableOps));
      return new SharedResult(retainableOps, discardableOps, discardableInputOps,
          dataSize, maxDataSize);
    }
    if (retainableTsOp.getChildOperators().size() == 0 || discardableTsOp.getChildOperators().size() == 0) {
      return new SharedResult(retainableOps, discardableOps, discardableInputOps,
          dataSize, maxDataSize);
    }

    Operator<?> currentOp1 = retainableTsOp.getChildOperators().get(0);
    Operator<?> currentOp2 = discardableTsOp.getChildOperators().get(0);

    // Special treatment for Filter operator that ignores the DPP predicates
    if (mayRemoveDownStreamOperators && currentOp1 instanceof FilterOperator && currentOp2 instanceof FilterOperator) {
      boolean equalFilters = false;
      FilterDesc op1Conf = ((FilterOperator) currentOp1).getConf();
      FilterDesc op2Conf = ((FilterOperator) currentOp2).getConf();

      if (op1Conf.getIsSamplingPred() == op2Conf.getIsSamplingPred() &&
          StringUtils.equals(op1Conf.getSampleDescExpr(), op2Conf.getSampleDescExpr())) {
        Multiset<String> conjsOp1String = extractConjsIgnoringDPPPreds(op1Conf.getPredicate());
        Multiset<String> conjsOp2String = extractConjsIgnoringDPPPreds(op2Conf.getPredicate());
        if (conjsOp1String.equals(conjsOp2String)) {
          equalFilters = true;
        }
      }

      if (equalFilters) {
        equalOp1 = currentOp1;
        equalOp2 = currentOp2;
        retainableOps.add(equalOp1);
        discardableOps.add(equalOp2);
        if (currentOp1.getChildOperators().size() > 1 ||
                currentOp2.getChildOperators().size() > 1) {
          // TODO: Support checking multiple child operators to merge further.
          discardableInputOps.addAll(gatherDPPBranchOps(pctx, optimizerCache, discardableOps));
          discardableInputOps.addAll(gatherDPPBranchOps(pctx, optimizerCache, retainableOps,
              discardableInputOps));
          return new SharedResult(retainableOps, discardableOps, discardableInputOps,
              dataSize, maxDataSize);
        }
        currentOp1 = currentOp1.getChildOperators().get(0);
        currentOp2 = currentOp2.getChildOperators().get(0);
      } else {
        // Bail out
        discardableInputOps.addAll(gatherDPPBranchOps(pctx, optimizerCache, discardableOps));
        discardableInputOps.addAll(gatherDPPBranchOps(pctx, optimizerCache, retainableOps,
            discardableInputOps));
        return new SharedResult(retainableOps, discardableOps, discardableInputOps,
            dataSize, maxDataSize);
      }
    }

    return extractSharedOptimizationInfo(pctx, optimizerCache, equalOp1, equalOp2,
        currentOp1, currentOp2, retainableOps, discardableOps, discardableInputOps, mayRemoveDownStreamOperators,
        mayRemoveInputOps);
  }

  private static SharedResult extractSharedOptimizationInfo(ParseContext pctx,
      SharedWorkOptimizerCache optimizerCache,
      Operator<?> retainableOpEqualParent,
      Operator<?> discardableOpEqualParent,
      Operator<?> retainableOp,
      Operator<?> discardableOp) throws SemanticException {
    return extractSharedOptimizationInfo(pctx, optimizerCache,
        retainableOpEqualParent, discardableOpEqualParent, retainableOp, discardableOp,
        new LinkedHashSet<>(), new LinkedHashSet<>(), new HashSet<>(), true, true);
  }

  private static SharedResult extractSharedOptimizationInfo(ParseContext pctx,
      SharedWorkOptimizerCache optimizerCache,
      Operator<?> retainableOpEqualParent,
      Operator<?> discardableOpEqualParent,
      Operator<?> retainableOp,
      Operator<?> discardableOp,
      LinkedHashSet<Operator<?>> retainableOps,
      LinkedHashSet<Operator<?>> discardableOps,
      Set<Operator<?>> discardableInputOps, boolean mayRemoveDownStreamOperators, boolean mayRemoveInputOps)
      throws SemanticException {
    Operator<?> equalOp1 = retainableOpEqualParent;
    Operator<?> equalOp2 = discardableOpEqualParent;
    Operator<?> currentOp1 = retainableOp;
    Operator<?> currentOp2 = discardableOp;
    long dataSize = 0L;
    long maxDataSize = 0L;
    // Try to merge rest of operators
    while (mayRemoveDownStreamOperators && !(currentOp1 instanceof ReduceSinkOperator)) {
      // Check whether current operators are equal
      if (!compareOperator(pctx, currentOp1, currentOp2)) {
        // If they are not equal, we could zip up till here
        break;
      }
      if (currentOp1.getParentOperators().size() !=
              currentOp2.getParentOperators().size()) {
        // If they are not equal, we could zip up till here
        break;
      }
      if (currentOp1.getParentOperators().size() > 1) {
        List<Operator<?>> discardableOpsForCurrentOp = new ArrayList<>();
        int idx = 0;
        for (; idx < currentOp1.getParentOperators().size(); idx++) {
          Operator<?> parentOp1 = currentOp1.getParentOperators().get(idx);
          Operator<?> parentOp2 = currentOp2.getParentOperators().get(idx);
          if (parentOp1 == equalOp1 && parentOp2 == equalOp2) {
            continue;
          }
          if ((parentOp1 == equalOp1 && parentOp2 != equalOp2) ||
                  (parentOp1 != equalOp1 && parentOp2 == equalOp2)) {
            // Input operator is not in the same position
            break;
          }
          // Compare input
          List<Operator<?>> removeOpsForCurrentInput =
              compareAndGatherOps(pctx, optimizerCache, parentOp1, parentOp2);
          if (removeOpsForCurrentInput == null) {
            // Inputs are not the same, bail out
            break;
          }
          // Add inputs to ops to remove
          discardableOpsForCurrentOp.addAll(removeOpsForCurrentInput);
        }
        if (idx != currentOp1.getParentOperators().size()) {
          // If inputs are not equal, we could zip up till here
          break;
        }
        discardableInputOps.addAll(discardableOpsForCurrentOp);
      }

      equalOp1 = currentOp1;
      equalOp2 = currentOp2;
      retainableOps.add(equalOp1);
      discardableOps.add(equalOp2);
      if (equalOp1 instanceof MapJoinOperator) {
        MapJoinOperator mop = (MapJoinOperator) equalOp1;
        dataSize = StatsUtils.safeAdd(dataSize, mop.getConf().getInMemoryDataSize());
        maxDataSize = mop.getConf().getMemoryMonitorInfo().getAdjustedNoConditionalTaskSize();
      }
      if (currentOp1.getChildOperators().size() > 1 ||
              currentOp2.getChildOperators().size() > 1) {
        // TODO: Support checking multiple child operators to merge further.
        break;
      }
      // Update for next iteration
      currentOp1 = currentOp1.getChildOperators().get(0);
      currentOp2 = currentOp2.getChildOperators().get(0);
    }

    // Add the rest to the memory consumption
    Set<Operator<?>> opsWork1 = findWorkOperators(optimizerCache, currentOp1);
    for (Operator<?> op : opsWork1) {
      if (op instanceof MapJoinOperator && !retainableOps.contains(op)) {
        MapJoinOperator mop = (MapJoinOperator) op;
        dataSize = StatsUtils.safeAdd(dataSize, mop.getConf().getInMemoryDataSize());
        maxDataSize = mop.getConf().getMemoryMonitorInfo().getAdjustedNoConditionalTaskSize();
      }
    }
    Set<Operator<?>> opsWork2 = findWorkOperators(optimizerCache, currentOp2);
    for (Operator<?> op : opsWork2) {
      if (op instanceof MapJoinOperator && !discardableOps.contains(op)) {
        MapJoinOperator mop = (MapJoinOperator) op;
        dataSize = StatsUtils.safeAdd(dataSize, mop.getConf().getInMemoryDataSize());
        maxDataSize = mop.getConf().getMemoryMonitorInfo().getAdjustedNoConditionalTaskSize();
      }
    }

    if (mayRemoveInputOps) {
      discardableInputOps
          .addAll(gatherDPPBranchOps(pctx, optimizerCache, Sets.union(discardableInputOps, discardableOps)));
      discardableInputOps.addAll(gatherDPPBranchOps(pctx, optimizerCache, retainableOps, discardableInputOps));
    }
    return new SharedResult(retainableOps, discardableOps, discardableInputOps,
        dataSize, maxDataSize);
  }

  private static Multiset<String> extractConjsIgnoringDPPPreds(ExprNodeDesc predicate) {
    List<ExprNodeDesc> conjsOp = ExprNodeDescUtils.split(predicate);
    Multiset<String> conjsOpString = TreeMultiset.create();
    for (int i = 0; i < conjsOp.size(); i++) {
      if (conjsOp.get(i) instanceof ExprNodeGenericFuncDesc) {
        ExprNodeGenericFuncDesc func = (ExprNodeGenericFuncDesc) conjsOp.get(i);
        if (GenericUDFInBloomFilter.class == func.getGenericUDF().getClass()) {
          continue;
        } else if (GenericUDFBetween.class == func.getGenericUDF().getClass() &&
            (func.getChildren().get(2) instanceof ExprNodeDynamicValueDesc ||
                func.getChildren().get(3) instanceof ExprNodeDynamicValueDesc)) {
          continue;
        }
      } else if(conjsOp.get(i) instanceof ExprNodeDynamicListDesc) {
        continue;
      }
      conjsOpString.add(conjsOp.get(i).toString());
    }
    return conjsOpString;
  }

  private static Set<Operator<?>> gatherDPPBranchOps(ParseContext pctx,
          SharedWorkOptimizerCache optimizerCache, Set<Operator<?>> ops) {
    Set<Operator<?>> dppBranches = new HashSet<>();
    for (Operator<?> op : ops) {
      if (op instanceof TableScanOperator) {
        Collection<Operator<?>> c = optimizerCache.tableScanToDPPSource
            .get((TableScanOperator) op);
        for (Operator<?> dppSource : c) {
          // Remove the branches
          removeBranch(dppSource, dppBranches, ops);
        }
      }
    }
    return dppBranches;
  }

  private static Set<Operator<?>> gatherDPPBranchOps(ParseContext pctx,
          SharedWorkOptimizerCache optimizerCache, Set<Operator<?>> ops,
          Set<Operator<?>> discardedOps) {
    Set<Operator<?>> dppBranches = new HashSet<>();
    for (Operator<?> op : ops) {
      if (op instanceof TableScanOperator) {
        Collection<Operator<?>> c = optimizerCache.tableScanToDPPSource
            .get((TableScanOperator) op);
        for (Operator<?> dppSource : c) {
          Set<Operator<?>> ascendants =
              findAscendantWorkOperators(pctx, optimizerCache, dppSource);
          if (!Collections.disjoint(ascendants, discardedOps)) {
            // Remove branch
            removeBranch(dppSource, dppBranches, ops);
          }
        }
      }
    }
    return dppBranches;
  }

  private static void removeBranch(Operator<?> currentOp, Set<Operator<?>> branchesOps,
          Set<Operator<?>> discardableOps) {
    if (currentOp.getNumChild() > 1) {
      for (Operator<?> childOp : currentOp.getChildOperators()) {
        if (!branchesOps.contains(childOp) && !discardableOps.contains(childOp)) {
          return;
        }
      }
    }
    branchesOps.add(currentOp);
    if (currentOp.getParentOperators() != null) {
      for (Operator<?> parentOp : currentOp.getParentOperators()) {
        removeBranch(parentOp, branchesOps, discardableOps);
      }
    }
  }

  private static List<Operator<?>> compareAndGatherOps(ParseContext pctx,
      SharedWorkOptimizerCache optimizerCache, Operator<?> op1, Operator<?> op2) throws SemanticException {
    List<Operator<?>> result = new ArrayList<>();
    boolean mergeable = compareAndGatherOps(pctx, optimizerCache, op1, op2, result, true);
    if (!mergeable) {
      return null;
    }
    return result;
  }

  private static boolean compareAndGatherOps(ParseContext pctx,
      SharedWorkOptimizerCache optimizerCache,
      Operator<?> op1,
      Operator<?> op2,
      List<Operator<?>> result,
      boolean gather) throws SemanticException {
    if (!compareOperator(pctx, op1, op2)) {
      LOG.debug("Operators not equal: {} and {}", op1, op2);
      return false;
    }

    if (op1 instanceof TableScanOperator) {
      Boolean areMergeable =
          areMergeableExtendedCheck(pctx, optimizerCache, (TableScanOperator) op1, (TableScanOperator) op2);
      if (!areMergeable) {
        LOG.debug("Operators have different DPP parent: {} and {}", op1, op2);
        return false;
      }
    }

    if (gather && op2.getChildOperators().size() > 1) {
      // If the second operator has more than one child, we stop gathering
      gather = false;
    }

    if (gather) {
      result.add(op2);
    }

    List<Operator<? extends OperatorDesc>> op1ParentOperators = op1.getParentOperators();
    List<Operator<? extends OperatorDesc>> op2ParentOperators = op2.getParentOperators();
    if (op1ParentOperators != null && op2ParentOperators != null) {
      if (op1ParentOperators.size() != op2ParentOperators.size()) {
        return false;
      }
      for (int i = 0; i < op1ParentOperators.size(); i++) {
        Operator<?> op1ParentOp = op1ParentOperators.get(i);
        Operator<?> op2ParentOp = op2ParentOperators.get(i);
        boolean mergeable =
            compareAndGatherOps(pctx, optimizerCache, op1ParentOp, op2ParentOp, result, gather);
        if (!mergeable) {
          return false;
        }
      }
    } else if (op1ParentOperators != null || op2ParentOperators != null) {
      return false;
    }

    return true;
  }

  private static boolean compareOperator(ParseContext pctx, Operator<?> op1, Operator<?> op2)
          throws SemanticException {
    if (!op1.getClass().getName().equals(op2.getClass().getName())) {
      return false;
    }

    // We handle ReduceSinkOperator here as we can safely ignore table alias
    // and the current comparator implementation does not.
    // We can ignore table alias since when we compare ReduceSinkOperator, all
    // its ancestors need to match (down to table scan), thus we make sure that
    // both plans are the same.
    // TODO: move this to logicalEquals
    if (op1 instanceof ReduceSinkOperator) {
      ReduceSinkDesc op1Conf = ((ReduceSinkOperator) op1).getConf();
      ReduceSinkDesc op2Conf = ((ReduceSinkOperator) op2).getConf();

      if (StringUtils.equals(op1Conf.getKeyColString(), op2Conf.getKeyColString()) &&
        StringUtils.equals(op1Conf.getValueColsString(), op2Conf.getValueColsString()) &&
        StringUtils.equals(op1Conf.getParitionColsString(), op2Conf.getParitionColsString()) &&
        op1Conf.getTag() == op2Conf.getTag() &&
        StringUtils.equals(op1Conf.getOrder(), op2Conf.getOrder()) &&
        StringUtils.equals(op1Conf.getNullOrder(), op2Conf.getNullOrder()) &&
        op1Conf.getTopN() == op2Conf.getTopN() &&
        canDeduplicateReduceTraits(op1Conf, op2Conf)) {
        return true;
      } else {
        return false;
      }
    }

    // We handle TableScanOperator here as we can safely ignore table alias
    // and the current comparator implementation does not.
    // TODO: move this to logicalEquals
    if (op1 instanceof TableScanOperator) {
      TableScanOperator tsOp1 = (TableScanOperator) op1;
      TableScanOperator tsOp2 = (TableScanOperator) op2;
      TableScanDesc op1Conf = tsOp1.getConf();
      TableScanDesc op2Conf = tsOp2.getConf();

      Table tableMeta1 = op1Conf.getTableMetadata();
      Table tableMeta2 = op2Conf.getTableMetadata();
      if (StringUtils.equals(tableMeta1.getFullyQualifiedName(), tableMeta2.getFullyQualifiedName())
          && op1Conf.getNeededColumns().equals(op2Conf.getNeededColumns())
          && StringUtils.equals(op1Conf.getFilterExprString(), op2Conf.getFilterExprString())
          && pctx.getPrunedPartitions(tsOp1).getPartitions().equals(
              pctx.getPrunedPartitions(tsOp2).getPartitions())
          && op1Conf.getRowLimit() == op2Conf.getRowLimit()
          && Objects.equals(op1Conf.getIncludedBuckets(), op2Conf.getIncludedBuckets())
          && Objects.equals(op1Conf.getOpProps(), op2Conf.getOpProps())) {
        return true;
      } else {
        return false;
      }
    }

    return op1.logicalEquals(op2);
  }

  private static boolean validPreConditions(ParseContext pctx, SharedWorkOptimizerCache optimizerCache,
          SharedResult sr) {

    // We check whether merging the works would cause the size of
    // the data in memory grow too large.
    // TODO: Currently ignores GBY and PTF which may also buffer data in memory.
    if (sr.dataSize > sr.maxDataSize) {
      // Size surpasses limit, we cannot convert
      LOG.debug("accumulated data size: {} / max size: {}", sr.dataSize, sr.maxDataSize);
      return false;
    }

    Operator<?> op1 = sr.retainableOps.get(0);
    Operator<?> op2 = sr.discardableOps.get(0);

    // 1) The set of operators in the works that we are merging need to meet
    // some requirements. In particular:
    // 1.1. None of the works that we are merging can contain a Union
    // operator. This is not supported yet as we might end up with cycles in
    // the Tez DAG.
    // 1.2. There cannot be any DummyStore operator in the works being merged.
    //  This is due to an assumption in MergeJoinProc that needs to be further explored.
    //  This is also due to some assumption in task generation
    // If any of these conditions are not met, we cannot merge.
    // TODO: Extend rule so it can be applied for these cases.
    final Set<Operator<?>> workOps1 = findWorkOperators(optimizerCache, op1);
    final Set<Operator<?>> workOps2 = findWorkOperators(optimizerCache, op2);
    for (Operator<?> op : workOps1) {
      if (op instanceof UnionOperator) {
        // We cannot merge (1.1)
        return false;
      }
      if (op instanceof DummyStoreOperator) {
        // We cannot merge (1.2)
        return false;
      }
    }
    for (Operator<?> op : workOps2) {
      if (op instanceof UnionOperator) {
        // We cannot merge (1.1)
        return false;
      }
      if (op instanceof DummyStoreOperator) {
        // We cannot merge (1.2)
        return false;
      }
    }

    // 2) We check whether one of the operators is part of a work that is an input for
    // the work of the other operator.
    //
    //   Work1            (merge TS in W1 & W3)        Work1
    //     |                        ->                   |        X
    //   Work2                                         Work2
    //     |                                             |
    //   Work3                                         Work1
    //
    // If we do, we cannot merge, as we would end up with a cycle in the DAG.
    final Set<Operator<?>> descendantWorksOps1 =
        findDescendantWorkOperators(pctx, optimizerCache, op1, sr.discardableInputOps);
    final Set<Operator<?>> descendantWorksOps2 =
        findDescendantWorkOperators(pctx, optimizerCache, op2, sr.discardableInputOps);
    if (!Collections.disjoint(descendantWorksOps1, workOps2) || !Collections.disjoint(workOps1, descendantWorksOps2)) {
      return false;
    }

    // 3) We check whether output works when we merge the operators will collide.
    //
    //   Work1   Work2    (merge TS in W1 & W2)        Work1
    //       \   /                  ->                  | |       X
    //       Work3                                     Work3
    //
    // If we do, we cannot merge. The reason is that Tez currently does
    // not support parallel edges, i.e., multiple edges from same work x
    // into same work y.
    RelaxedVertexEdgePredicate edgePredicate;
    if (pctx.getConf().getBoolVar(ConfVars.HIVE_SHARED_WORK_PARALLEL_EDGE_SUPPORT)) {
      edgePredicate = new RelaxedVertexEdgePredicate(EnumSet.<EdgeType> of(EdgeType.DPP, EdgeType.SEMIJOIN, EdgeType.BROADCAST));
    } else {
      edgePredicate = new RelaxedVertexEdgePredicate(EnumSet.<EdgeType> of(EdgeType.DPP));
    }

    OperatorGraph og = new OperatorGraph(pctx);
    Set<OperatorGraph.Cluster> cc1 = og.clusterOf(op1).childClusters(edgePredicate);
    Set<OperatorGraph.Cluster> cc2 = og.clusterOf(op2).childClusters(edgePredicate);

    if (!Collections.disjoint(cc1, cc2)) {
      LOG.debug("merge would create an unsupported parallel edge(CHILDS)", op1, op2);
      return false;
    }

    if (!og.mayMerge(op1, op2)) {
      LOG.debug("merging {} and {} would violate dag properties", op1, op2);
      return false;
    }

    // 4) We check whether we will end up with same operators inputing on same work.
    //
    //       Work1        (merge TS in W2 & W3)        Work1
    //       /   \                  ->                  | |       X
    //   Work2   Work3                                 Work2
    //
    // If we do, we cannot merge. The reason is the same as above, currently
    // Tez does not support parallel edges.
    //
    // In the check, we exclude the inputs to the root operator that we are trying
    // to merge (only useful for extended merging as TS do not have inputs).
    Set<OperatorGraph.Cluster> pc1 = og.clusterOf(op1).parentClusters(edgePredicate);
    Set<OperatorGraph.Cluster> pc2 = og.clusterOf(op2).parentClusters(edgePredicate);
    Set<Cluster> pc = new HashSet<>(Sets.intersection(pc1, pc2));

    for (Operator<?> o : sr.discardableOps.get(0).getParentOperators()) {
      pc.remove(og.clusterOf(o));
    }
    for (Operator<?> o : sr.discardableInputOps) {
      pc.remove(og.clusterOf(o));
    }

    if (pc.size() > 0) {
      LOG.debug("merge would create an unsupported parallel edge(PARENTS)", op1, op2);
      return false;
    }

    return true;
  }

  static class RelaxedVertexEdgePredicate implements OperatorEdgePredicate {

    private EnumSet<EdgeType> traverseableEdgeTypes;

    public RelaxedVertexEdgePredicate(EnumSet<EdgeType> nonTraverseableEdgeTypes) {
      this.traverseableEdgeTypes = nonTraverseableEdgeTypes;
    }

    @Override
    public boolean accept(Operator<?> s, Operator<?> t, OpEdge opEdge) {
      if (!traverseableEdgeTypes.contains(opEdge.getEdgeType())) {
        return true;
      }
      if (s instanceof ReduceSinkOperator) {
        ReduceSinkOperator rs = (ReduceSinkOperator) s;
        if (!ParallelEdgeFixer.colMappingInverseKeys(rs).isPresent()) {
          return true;
        }
      }
      return false;
    }

  }

  private static Set<Operator<?>> findParentWorkOperators(ParseContext pctx,
          SharedWorkOptimizerCache optimizerCache, Operator<?> start) {
    return findParentWorkOperators(pctx, optimizerCache, start, ImmutableSet.of());
  }

  private static Set<Operator<?>> findParentWorkOperators(ParseContext pctx,
          SharedWorkOptimizerCache optimizerCache, Operator<?> start,
          Set<Operator<?>> excludeOps) {
    // Find operators in work
    Set<Operator<?>> workOps = findWorkOperators(optimizerCache, start);
    // Gather input works operators
    Set<Operator<?>> set = new HashSet<Operator<?>>();
    for (Operator<?> op : workOps) {
      if (op.getParentOperators() != null) {
        for (Operator<?> parent : op.getParentOperators()) {
          if (parent instanceof ReduceSinkOperator && !excludeOps.contains(parent)) {
            set.addAll(findWorkOperators(optimizerCache, parent));
          }
        }
      }
      if (op instanceof TableScanOperator) {
        // Check for DPP and semijoin DPP
        for (Operator<?> parent : optimizerCache.tableScanToDPPSource.get((TableScanOperator) op)) {
          if (!excludeOps.contains(parent)) {
            set.addAll(findWorkOperators(optimizerCache, parent));
          }
        }
      }
    }
    return set;
  }

  private static Set<Operator<?>> findAscendantWorkOperators(ParseContext pctx,
          SharedWorkOptimizerCache optimizerCache, Operator<?> start) {
    // Find operators in work
    Set<Operator<?>> workOps = findWorkOperators(optimizerCache, start);
    // Gather input works operators
    Set<Operator<?>> result = new HashSet<Operator<?>>();
    Set<Operator<?>> set;
    while (!workOps.isEmpty()) {
      set = new HashSet<Operator<?>>();
      for (Operator<?> op : workOps) {
        if (op.getParentOperators() != null) {
          for (Operator<?> parent : op.getParentOperators()) {
            if (parent instanceof ReduceSinkOperator) {
              set.addAll(findWorkOperators(optimizerCache, parent));
            }
          }
        } else if (op instanceof TableScanOperator) {
          // Check for DPP and semijoin DPP
          for (Operator<?> parent : optimizerCache.tableScanToDPPSource.get((TableScanOperator) op)) {
            set.addAll(findWorkOperators(optimizerCache, parent));
          }
        }
      }
      workOps = set;
      result.addAll(set);
    }
    return result;
  }

  private static Set<Operator<?>> findChildWorkOperators(ParseContext pctx,
      SharedWorkOptimizerCache optimizerCache, Operator<?> start, boolean traverseEventOperators) {
    // Find operators in work
    Set<Operator<?>> workOps = findWorkOperators(optimizerCache, start);
    // Gather output works operators
    Set<Operator<?>> set = new HashSet<Operator<?>>();
    for (Operator<?> op : workOps) {
      if (op instanceof ReduceSinkOperator) {
        if (op.getChildOperators() != null) {
          // All children of RS are descendants
          for (Operator<?> child : op.getChildOperators()) {
            set.addAll(findWorkOperators(optimizerCache, child));
          }
        }
        // Semijoin DPP work is considered a child because work needs
        // to finish for it to execute
        SemiJoinBranchInfo sjbi = pctx.getRsToSemiJoinBranchInfo().get(op);
        if (sjbi != null) {
          set.addAll(findWorkOperators(optimizerCache, sjbi.getTsOp()));
        }
      } else if(op.getConf() instanceof DynamicPruningEventDesc) {
        // DPP work is considered a child because work needs
        // to finish for it to execute
        if (traverseEventOperators) {
          set.addAll(findWorkOperators(
                  optimizerCache, ((DynamicPruningEventDesc) op.getConf()).getTableScan()));
        }
      }
    }
    return set;
  }

  private static Set<Operator<?>> findDescendantWorkOperators(ParseContext pctx,
          SharedWorkOptimizerCache optimizerCache, Operator<?> start,
          Set<Operator<?>> excludeOps) {
    // Find operators in work
    Set<Operator<?>> workOps = findWorkOperators(optimizerCache, start);
    // Gather output works operators
    Set<Operator<?>> result = new HashSet<Operator<?>>();
    Set<Operator<?>> set;
    while (!workOps.isEmpty()) {
      set = new HashSet<Operator<?>>();
      for (Operator<?> op : workOps) {
        if (excludeOps.contains(op)) {
          continue;
        }
        if (op instanceof ReduceSinkOperator) {
          if (op.getChildOperators() != null) {
            // All children of RS are descendants
            for (Operator<?> child : op.getChildOperators()) {
              set.addAll(findWorkOperators(optimizerCache, child));
            }
          }
          // Semijoin DPP work is considered a descendant because work needs
          // to finish for it to execute
          SemiJoinBranchInfo sjbi = pctx.getRsToSemiJoinBranchInfo().get(op);
          if (sjbi != null) {
            set.addAll(findWorkOperators(optimizerCache, sjbi.getTsOp()));
          }
        } else if(op.getConf() instanceof DynamicPruningEventDesc) {
          // DPP work is considered a descendant because work needs
          // to finish for it to execute
          set.addAll(findWorkOperators(
                  optimizerCache, ((DynamicPruningEventDesc) op.getConf()).getTableScan()));
        }
      }
      workOps = set;
      result.addAll(set);
    }
    return result;
  }

  // Stores result in cache
  private static Set<Operator<?>> findWorkOperators(
          SharedWorkOptimizerCache optimizerCache, Operator<?> start) {
    Set<Operator<?>> c = optimizerCache.getWorkGroup(start);
    if (!c.isEmpty()) {
      return c;
    }
    c = findWorkOperators(start, new HashSet<Operator<?>>());
    optimizerCache.addWorkGroup(c);
    return c;
  }

  private static Set<Operator<?>> findWorkOperators(Operator<?> start, Set<Operator<?>> found) {
    found.add(start);
    if (start.getParentOperators() != null) {
      for (Operator<?> parent : start.getParentOperators()) {
        if (parent instanceof ReduceSinkOperator) {
          continue;
        }
        if (!found.contains(parent)) {
          findWorkOperators(parent, found);
        }
      }
    }
    if (start instanceof ReduceSinkOperator) {
      return found;
    }
    if (start.getChildOperators() != null) {
      for (Operator<?> child : start.getChildOperators()) {
        if (!found.contains(child)) {
          findWorkOperators(child, found);
        }
      }
    }
    return found;
  }

  private static void pushFilterToTopOfTableScan(
      SharedWorkOptimizerCache optimizerCache, DecomposedTs tsModel)
                  throws UDFArgumentException {
    TableScanOperator tsOp = tsModel.ts;
    ExprNodeGenericFuncDesc tableScanExprNode = (ExprNodeGenericFuncDesc) tsModel.getFullFilterExpr();
    if (tableScanExprNode == null) {
      return;
    }
    List<Operator<? extends OperatorDesc>> allChildren =
        Lists.newArrayList(tsOp.getChildOperators());
    childOperators:
    for (Operator<? extends OperatorDesc> op : allChildren) {
      if (optimizerCache.isKnownFilteringOperator(op)) {
        continue;
      }
      if (op instanceof FilterOperator) {
        FilterOperator filterOp = (FilterOperator) op;
        ExprNodeDesc filterExprNode  = filterOp.getConf().getPredicate();
        if (tableScanExprNode.isSame(filterExprNode)) {
          // We do not need to do anything
          optimizerCache.setKnownFilteringOperator(filterOp);
          continue;
        }
        if (tableScanExprNode.getGenericUDF() instanceof GenericUDFOPOr) {
          for (ExprNodeDesc childExprNode : tableScanExprNode.getChildren()) {
            if (childExprNode.isSame(filterExprNode)) {
              // We do not need to do anything, it is in the OR expression
              // so probably we pushed previously
              optimizerCache.setKnownFilteringOperator(filterOp);
              continue childOperators;
            }
          }
        }
        ExprNodeDesc newFilterExpr = conjunction(filterExprNode, tableScanExprNode);
        if (!isSame(filterOp.getConf().getPredicate(), newFilterExpr)) {
          filterOp.getConf().setPredicate(newFilterExpr);
        }
        optimizerCache.setKnownFilteringOperator(filterOp);
      } else {
        Operator<FilterDesc> newOp = OperatorFactory.get(tsOp.getCompilationOpContext(),
                new FilterDesc(tableScanExprNode.clone(), false),
            new RowSchema(tsOp.getSchema().getSignature()));
        tsOp.replaceChild(op, newOp);
        newOp.getParentOperators().add(tsOp);
        op.replaceParent(tsOp, newOp);
        newOp.getChildOperators().add(op);
        // Add to cache (same group as tsOp)
        optimizerCache.putIfWorkExists(newOp, tsOp);
        optimizerCache.setKnownFilteringOperator(newOp);
      }
    }

  }

  static boolean canDeduplicateReduceTraits(ReduceSinkDesc retainable, ReduceSinkDesc discardable) {
    return deduplicateReduceTraits(retainable, discardable, false);
  }

  static boolean deduplicateReduceTraits(ReduceSinkDesc retainable, ReduceSinkDesc discardable) {
    return deduplicateReduceTraits(retainable, discardable, true);
  }

  private static boolean deduplicateReduceTraits(ReduceSinkDesc retainable,
      ReduceSinkDesc discardable, boolean apply) {

    final EnumSet<ReduceSinkDesc.ReducerTraits> retainableTraits = retainable.getReducerTraits();
    final EnumSet<ReduceSinkDesc.ReducerTraits> discardableTraits = discardable.getReducerTraits();

    final boolean x1 = retainableTraits.contains(UNSET);
    final boolean f1 = retainableTraits.contains(FIXED);
    final boolean u1 = retainableTraits.contains(UNIFORM);
    final boolean a1 = retainableTraits.contains(AUTOPARALLEL);
    final int n1 = retainable.getNumReducers();

    final boolean x2 = discardableTraits.contains(UNSET);
    final boolean f2 = discardableTraits.contains(FIXED);
    final boolean u2 = discardableTraits.contains(UNIFORM);
    final boolean a2 = discardableTraits.contains(AUTOPARALLEL);
    final int n2 = discardable.getNumReducers();

    boolean dedup = false;
    boolean x3 = false;
    boolean f3 = false;
    boolean u3 = false;
    boolean a3 = false;
    int n3 = n1;

    // NOTE: UNSET is exclusive from other traits, so FIXED is.

    if (x1 || x2) {
      // UNSET + X = X
      dedup = true;
      n3 = Math.max(n1, n2);
      x3 = x1 && x2;
      f3 = f1 || f2;
      u3 = u1 || u2;
      a3 = a1 || a2;
    } else if (f1 || f2) {
      if (f1 && f2) {
        // FIXED(x) + FIXED(x) = FIXED(x)
        // FIXED(x) + FIXED(y) = no deduplication (where x != y)
        if (n1 == n2) {
          dedup = true;
          f3 = true;
        }
      } else {
        // FIXED(x) + others = FIXED(x)
        dedup = true;
        f3 = true;
        if (f1) {
          n3 = n1;
        } else {
          n3 = n2;
        }
      }
    } else {
      if (u1 && u2) {
        // UNIFORM(x) + UNIFORM(y) = UNIFORM(max(x, y))
        dedup = true;
        u3 = true;
        n3 = Math.max(n1, n2);
      }
      if (a1 && a2) {
        // AUTOPARALLEL(x) + AUTOPARALLEL(y) = AUTOPARALLEL(max(x, y))
        dedup = true;
        a3 = true;
        n3 = Math.max(n1, n2);
      }
    }

    // Gether the results into the retainable object
    if (apply && dedup) {
      retainable.setNumReducers(n3);

      if (x3) {
        retainableTraits.add(UNSET);
      } else {
        retainableTraits.remove(UNSET);
      }

      if (f3) {
        retainableTraits.add(FIXED);
      } else {
        retainableTraits.remove(FIXED);
      }

      if (u3) {
        retainableTraits.add(UNIFORM);
      } else {
        retainableTraits.remove(UNIFORM);
      }

      if (a3) {
        retainableTraits.add(AUTOPARALLEL);
      } else {
        retainableTraits.remove(AUTOPARALLEL);
      }
    }
    return dedup;
  }

  private static class SharedResult {
    final List<Operator<?>> retainableOps;
    final List<Operator<?>> discardableOps;
    final Set<Operator<?>> discardableInputOps;
    final long dataSize;
    final long maxDataSize;

    private SharedResult(Collection<Operator<?>> retainableOps, Collection<Operator<?>> discardableOps,
            Set<Operator<?>> discardableInputOps, long dataSize, long maxDataSize) {
      this.retainableOps = ImmutableList.copyOf(retainableOps);
      this.discardableOps = ImmutableList.copyOf(discardableOps);
      this.discardableInputOps = ImmutableSet.copyOf(discardableInputOps);
      this.dataSize = dataSize;
      this.maxDataSize = maxDataSize;
    }

    @Override
    public String toString() {
      return "SharedResult { " + this.retainableOps + "; " + this.discardableOps + "; "
          + this.discardableInputOps + "};";
    }
  }

  /** Cache to accelerate optimization */
  static class SharedWorkOptimizerCache {
    // Operators that belong to each work
    private final Map<Operator<?>, Set<Operator<?>>> operatorToWorkOperators = new IdentityHashMap<>();
    // Table scan operators to DPP sources
    final Multimap<TableScanOperator, Operator<?>> tableScanToDPPSource =
            HashMultimap.<TableScanOperator, Operator<?>>create();
    private Set<Operator<?>> knownFilterOperators = new HashSet<>();

    // Add new operator to cache work group of existing operator (if group exists)
    void putIfWorkExists(Operator<?> opToAdd, Operator<?> existingOp) {
      Set<Operator<?>> group = operatorToWorkOperators.get(existingOp);
      if (group == null) {
        return;
      }
      group.add(opToAdd);
      operatorToWorkOperators.put(opToAdd, group);
    }

    public void addWorkGroup(Collection<Operator<?>> c) {
      Set<Operator<?>> group = Sets.newIdentityHashSet();
      group.addAll(c);
      for (Operator<?> op : c) {
        operatorToWorkOperators.put(op, group);
      }
    }

    public Set<Operator<?>> getWorkGroup(Operator<?> start) {
      Set<Operator<?>> set = operatorToWorkOperators.get(start);
      if (set == null) {
        return Collections.emptySet();
      }
      return set;
    }

    public Set<Set<Operator<?>>> getWorkGroups() {
      Set<Set<Operator<?>>> ret = Sets.newIdentityHashSet();
      ret.addAll(operatorToWorkOperators.values());
      return ret;
    }

    public boolean isKnownFilteringOperator(Operator<? extends OperatorDesc> op) {
      return knownFilterOperators.contains(op);
    }

    public void setKnownFilteringOperator(Operator<?> filterOp) {
      knownFilterOperators.add(filterOp);
    }

    // Remove operator
    void removeOp(Operator<?> opToRemove) {
      Set<Operator<?>> group = operatorToWorkOperators.get(opToRemove);
      if (group == null) {
        return;
      }
      group.remove(opToRemove);
      operatorToWorkOperators.remove(opToRemove);
    }

    // Remove operator and combine
    void removeOpAndCombineWork(Operator<?> opToRemove, Operator<?> replacementOp) {
      Set<Operator<?>> group1 = operatorToWorkOperators.get(opToRemove);
      Set<Operator<?>> group2 = operatorToWorkOperators.get(replacementOp);

      group1.remove(opToRemove);
      operatorToWorkOperators.remove(opToRemove);

      if (group1.size() > group2.size()) {
        Set<Operator<?>> t = group2;
        group2 = group1;
        group1 = t;
      }

      group2.addAll(group1);

      for (Operator<?> o : group1) {
        operatorToWorkOperators.put(o, group2);
      }
    }

    @Override
    public String toString() {
      return "SharedWorkOptimizerCache { \n" + operatorToWorkOperators.toString() + "\n };";
    }
  }

}
