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

package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.AppMasterEventOperator;
import org.apache.hadoop.hive.ql.exec.DummyStoreOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.parse.GenTezUtils;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.SemiJoinBranchInfo;
import org.apache.hadoop.hive.ql.plan.DynamicPruningEventDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeDynamicListDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDynamicValueDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFInBloomFilter;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;

/**
 * Shared computation optimizer.
 *
 * <p>Originally, this rule would find scan operators over the same table
 * in the query plan and merge them if they met some preconditions.
 *
 *  TS   TS             TS
 *  |    |     ->      /  \
 *  Op   Op           Op  Op
 *
 * <p>Now the rule has been extended to find opportunities to other operators
 * downstream, not only a single table scan.
 *
 *  TS1   TS2    TS1   TS2            TS1   TS2
 *   |     |      |     |              |     |
 *   |    RS      |    RS              |    RS
 *    \   /        \   /       ->       \   /
 *   MapJoin      MapJoin              MapJoin
 *      |            |                  /   \
 *      Op           Op                Op   Op
 *
 * <p>A limitation in the current implementation is that the optimizer does not
 * go beyond a work boundary.
 *
 * <p>The optimization only works with the Tez execution engine.
 */
public class SharedWorkOptimizer extends Transform {

  private final static Logger LOG = LoggerFactory.getLogger(SharedWorkOptimizer.class);

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    final Map<String, TableScanOperator> topOps = pctx.getTopOps();
    if (topOps.size() < 2) {
      // Nothing to do, bail out
      return pctx;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Before SharedWorkOptimizer:\n" + Operator.toString(pctx.getTopOps().values()));
    }

    // Cache to use during optimization
    SharedWorkOptimizerCache optimizerCache = new SharedWorkOptimizerCache();

    // Gather information about the DPP table scans and store it in the cache
    gatherDPPTableScanOps(pctx, optimizerCache);

    // Map of dbName.TblName -> TSOperator
    Multimap<String, TableScanOperator> tableNameToOps = splitTableScanOpsByTable(pctx);

    // We enforce a certain order when we do the reutilization.
    // In particular, we use size of table x number of reads to
    // rank the tables.
    List<Entry<String, Long>> sortedTables = rankTablesByAccumulatedSize(pctx);
    LOG.debug("Sorted tables by size: {}", sortedTables);

    // Execute optimization
    Multimap<String, TableScanOperator> existingOps = ArrayListMultimap.create();
    Set<Operator<?>> removedOps = new HashSet<>();
    for (Entry<String, Long> tablePair : sortedTables) {
      String tableName = tablePair.getKey();
      for (TableScanOperator discardableTsOp : tableNameToOps.get(tableName)) {
        if (removedOps.contains(discardableTsOp)) {
          LOG.debug("Skip {} as it has been already removed", discardableTsOp);
          continue;
        }
        Collection<TableScanOperator> prevTsOps = existingOps.get(tableName);
        for (TableScanOperator retainableTsOp : prevTsOps) {
          if (removedOps.contains(retainableTsOp)) {
            LOG.debug("Skip {} as it has been already removed", retainableTsOp);
            continue;
          }

          // First we quickly check if the two table scan operators can actually be merged
          boolean mergeable = areMergeable(pctx, optimizerCache, retainableTsOp, discardableTsOp);
          if (!mergeable) {
            // Skip
            LOG.debug("{} and {} cannot be merged", retainableTsOp, discardableTsOp);
            continue;
          }

          // Secondly, we extract information about the part of the tree that can be merged
          // as well as some structural information (memory consumption) that needs to be
          // used to determined whether the merge can happen
          SharedResult sr = extractSharedOptimizationInfo(
                  pctx, optimizerCache, retainableTsOp, discardableTsOp);

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

          // We can merge
          if (sr.retainableOps.size() > 1) {
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
            // Only TS operator
            ExprNodeGenericFuncDesc exprNode = null;
            if (retainableTsOp.getConf().getFilterExpr() != null) {
              // Push filter on top of children
              pushFilterToTopOfTableScan(optimizerCache, retainableTsOp);
              // Clone to push to table scan
              exprNode = (ExprNodeGenericFuncDesc) retainableTsOp.getConf().getFilterExpr();
            }
            if (discardableTsOp.getConf().getFilterExpr() != null) {
              // Push filter on top
              pushFilterToTopOfTableScan(optimizerCache, discardableTsOp);
              ExprNodeGenericFuncDesc tsExprNode = discardableTsOp.getConf().getFilterExpr();
              if (exprNode != null && !exprNode.isSame(tsExprNode)) {
                // We merge filters from previous scan by ORing with filters from current scan
                if (exprNode.getGenericUDF() instanceof GenericUDFOPOr) {
                  List<ExprNodeDesc> newChildren = new ArrayList<>(exprNode.getChildren().size() + 1);
                  for (ExprNodeDesc childExprNode : exprNode.getChildren()) {
                    if (childExprNode.isSame(tsExprNode)) {
                      // We do not need to do anything, it is in the OR expression
                      break;
                    }
                    newChildren.add(childExprNode);
                  }
                  if (exprNode.getChildren().size() == newChildren.size()) {
                    newChildren.add(tsExprNode);
                    exprNode = ExprNodeGenericFuncDesc.newInstance(
                            new GenericUDFOPOr(),
                            newChildren);
                  }
                } else {
                  exprNode = ExprNodeGenericFuncDesc.newInstance(
                          new GenericUDFOPOr(),
                          Arrays.<ExprNodeDesc>asList(exprNode, tsExprNode));
                }
              }
            }
            // Replace filter
            retainableTsOp.getConf().setFilterExpr(exprNode);
            // Replace table scan operator
            List<Operator<? extends OperatorDesc>> allChildren =
                    Lists.newArrayList(discardableTsOp.getChildOperators());
            for (Operator<? extends OperatorDesc> op : allChildren) {
              discardableTsOp.getChildOperators().remove(op);
              op.replaceParent(discardableTsOp, retainableTsOp);
              retainableTsOp.getChildOperators().add(op);
            }

            LOG.debug("Merging {} into {}", discardableTsOp, retainableTsOp);
          }

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
              }
            } else if (op instanceof AppMasterEventOperator) {
              DynamicPruningEventDesc dped = (DynamicPruningEventDesc) op.getConf();
              if (!sr.discardableOps.contains(dped.getTableScan()) &&
                      !sr.discardableInputOps.contains(dped.getTableScan())) {
                GenTezUtils.removeSemiJoinOperator(
                        pctx, (AppMasterEventOperator) op, dped.getTableScan());
              }
            }
            LOG.debug("Input operator removed: {}", op);
          }
          // Then we merge the operators of the works we are going to merge
          optimizerCache.removeOpAndCombineWork(discardableTsOp, retainableTsOp);
          removedOps.add(discardableTsOp);
          // Finally we remove the expression from the tree
          for (Operator<?> op : sr.discardableOps) {
            OperatorUtils.removeOperator(op);
            optimizerCache.removeOp(op);
            removedOps.add(op);
            if (sr.discardableOps.size() == 1) {
              // If there is a single discardable operator, it is a TableScanOperator
              // and it means that we have merged filter expressions for it. Thus, we
              // might need to remove DPP predicates from the retainable TableScanOperator
              Collection<Operator<?>> c =
                      optimizerCache.tableScanToDPPSource.get((TableScanOperator) op);
              for (Operator<?> dppSource : c) {
                if (dppSource instanceof ReduceSinkOperator) {
                  GenTezUtils.removeSemiJoinOperator(pctx,
                          (ReduceSinkOperator) dppSource,
                          (TableScanOperator) sr.retainableOps.get(0));
                } else if (dppSource instanceof AppMasterEventOperator) {
                  GenTezUtils.removeSemiJoinOperator(pctx,
                          (AppMasterEventOperator) dppSource,
                          (TableScanOperator) sr.retainableOps.get(0));
                }
              }
            }
            LOG.debug("Operator removed: {}", op);
          }

          break;
        }

        if (removedOps.contains(discardableTsOp)) {
          // This operator has been removed, remove it from the list of existing operators
          existingOps.remove(tableName, discardableTsOp);
        } else {
          // This operator has not been removed, include it in the list of existing operators
          existingOps.put(tableName, discardableTsOp);
        }
      }
    }

    // Remove unused table scan operators
    Iterator<Entry<String, TableScanOperator>> it = topOps.entrySet().iterator();
    while (it.hasNext()) {
      Entry<String, TableScanOperator> e = it.next();
      if (e.getValue().getNumChild() == 0) {
        it.remove();
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("After SharedWorkOptimizer:\n" + Operator.toString(pctx.getTopOps().values()));
    }

    return pctx;
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
    Collection<Operator<? extends OperatorDesc>> tableScanOps =
            Lists.<Operator<?>>newArrayList(topOps.values());
    Set<AppMasterEventOperator> s =
            OperatorUtils.findOperators(tableScanOps, AppMasterEventOperator.class);
    for (AppMasterEventOperator a : s) {
      if (a.getConf() instanceof DynamicPruningEventDesc) {
        DynamicPruningEventDesc dped = (DynamicPruningEventDesc) a.getConf();
        optimizerCache.tableScanToDPPSource.put(dped.getTableScan(), a);
      }
    }
    for (Entry<ReduceSinkOperator, SemiJoinBranchInfo> e
            : pctx.getRsToSemiJoinBranchInfo().entrySet()) {
      optimizerCache.tableScanToDPPSource.put(e.getValue().getTsOp(), e.getKey());
    }
    LOG.debug("DPP information stored in the cache: {}", optimizerCache.tableScanToDPPSource);
  }

  private static Multimap<String, TableScanOperator> splitTableScanOpsByTable(
          ParseContext pctx) {
    Multimap<String, TableScanOperator> tableNameToOps = ArrayListMultimap.create();
    for (Entry<String, TableScanOperator> e : pctx.getTopOps().entrySet()) {
      TableScanOperator tsOp = e.getValue();
      tableNameToOps.put(
              tsOp.getConf().getTableMetadata().getDbName() + "."
                      + tsOp.getConf().getTableMetadata().getTableName(), tsOp);
    }
    return tableNameToOps;
  }

  private static List<Entry<String, Long>> rankTablesByAccumulatedSize(ParseContext pctx) {
    Map<String, Long> tableToTotalSize = new HashMap<>();
    for (Entry<String, TableScanOperator> e : pctx.getTopOps().entrySet()) {
      TableScanOperator tsOp = e.getValue();
      String tableName = tsOp.getConf().getTableMetadata().getDbName() + "."
              + tsOp.getConf().getTableMetadata().getTableName();
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
            new LinkedList<>(tableToTotalSize.entrySet());
    Collections.sort(sortedTables, Collections.reverseOrder(
            new Comparator<Map.Entry<String, Long>>() {
              public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                return (o1.getValue()).compareTo(o2.getValue());
              }
            }));
    return sortedTables;
  }

  private static boolean areMergeable(ParseContext pctx, SharedWorkOptimizerCache optimizerCache,
          TableScanOperator tsOp1, TableScanOperator tsOp2) throws SemanticException {
    // First we check if the two table scan operators can actually be merged
    // If schemas do not match, we currently do not merge
    List<String> prevTsOpNeededColumns = tsOp1.getNeededColumns();
    List<String> tsOpNeededColumns = tsOp2.getNeededColumns();
    if (prevTsOpNeededColumns.size() != tsOpNeededColumns.size()) {
      return false;
    }
    boolean notEqual = false;
    for (int i = 0; i < prevTsOpNeededColumns.size(); i++) {
      if (!prevTsOpNeededColumns.get(i).equals(tsOpNeededColumns.get(i))) {
        notEqual = true;
        break;
      }
    }
    if (notEqual) {
      return false;
    }
    // If row limit does not match, we currently do not merge
    if (tsOp1.getConf().getRowLimit() != tsOp2.getConf().getRowLimit()) {
      return false;
    }
    // If partitions do not match, we currently do not merge
    PrunedPartitionList prevTsOpPPList = pctx.getPrunedPartitions(tsOp1);
    PrunedPartitionList tsOpPPList = pctx.getPrunedPartitions(tsOp2);
    if (!prevTsOpPPList.getPartitions().equals(tsOpPPList.getPartitions())) {
      return false;
    }
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
          dppsOp1.remove(i);
          i--;
        }
      }
    }
    for (int i = 0; i < dppsOp2.size(); i++) {
      Operator<?> op = dppsOp2.get(i);
      if (op instanceof ReduceSinkOperator) {
        Set<Operator<?>> ascendants =
            findAscendantWorkOperators(pctx, optimizerCache, op);
        if (ascendants.contains(tsOp1)) {
          dppsOp2.remove(i);
          i--;
        }
      }
    }
    if (dppsOp1.size() != dppsOp2.size()) {
      // Only first or second operator contains DPP pruning
      return false;
    }
    // Check if DPP branches are equal
    for (int i = 0; i < dppsOp1.size(); i++) {
      Operator<?> dppOp1 = dppsOp1.get(i);
      BitSet bs = new BitSet();
      for (int j = 0; j < dppsOp2.size(); j++) {
        if (!bs.get(j)) {
          // If not visited yet
          Operator<?> dppOp2 = dppsOp2.get(j);
          if (compareAndGatherOps(pctx, dppOp1, dppOp2) != null) {
            // The DPP operator/branch are equal
            bs.set(j);
            break;
          }
        }
      }
      if (bs.cardinality() == i) {
        return false;
      }
    }
    return true;
  }

  private static SharedResult extractSharedOptimizationInfo(ParseContext pctx,
          SharedWorkOptimizerCache optimizerCache,
          TableScanOperator retainableTsOp,
          TableScanOperator discardableTsOp) throws SemanticException {
    Set<Operator<?>> retainableOps = new LinkedHashSet<>();
    Set<Operator<?>> discardableOps = new LinkedHashSet<>();
    Set<Operator<?>> discardableInputOps = new HashSet<>();
    long dataSize = 0l;
    long maxDataSize = 0l;

    retainableOps.add(retainableTsOp);
    discardableOps.add(discardableTsOp);
    Operator<?> equalOp1 = retainableTsOp;
    Operator<?> equalOp2 = discardableTsOp;
    if (equalOp1.getNumChild() > 1 || equalOp2.getNumChild() > 1) {
      // TODO: Support checking multiple child operators to merge further.
      discardableInputOps.addAll(gatherDPPBranchOps(pctx, optimizerCache, discardableOps));
      return new SharedResult(retainableOps, discardableOps, discardableInputOps, dataSize, maxDataSize);
    }
    Operator<?> currentOp1 = retainableTsOp.getChildOperators().get(0);
    Operator<?> currentOp2 = discardableTsOp.getChildOperators().get(0);

    // Special treatment for Filter operator that ignores the DPP predicates
    if (currentOp1 instanceof FilterOperator && currentOp2 instanceof FilterOperator) {
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
          discardableInputOps.addAll(gatherDPPBranchOps(pctx, optimizerCache, discardableInputOps));
          discardableInputOps.addAll(gatherDPPBranchOps(pctx, optimizerCache, discardableOps));
          discardableInputOps.addAll(gatherDPPBranchOps(pctx, optimizerCache, retainableOps, discardableInputOps));
          return new SharedResult(retainableOps, discardableOps, discardableInputOps, dataSize, maxDataSize);
        }
        currentOp1 = currentOp1.getChildOperators().get(0);
        currentOp2 = currentOp2.getChildOperators().get(0);
      } else {
        // Bail out
        discardableInputOps.addAll(gatherDPPBranchOps(pctx, optimizerCache, discardableInputOps));
        discardableInputOps.addAll(gatherDPPBranchOps(pctx, optimizerCache, discardableOps));
        discardableInputOps.addAll(gatherDPPBranchOps(pctx, optimizerCache, retainableOps, discardableInputOps));
        return new SharedResult(retainableOps, discardableOps, discardableInputOps, dataSize, maxDataSize);
      }
    }

    // Try to merge rest of operators
    while (!(currentOp1 instanceof ReduceSinkOperator)) {
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
          List<Operator<?>> removeOpsForCurrentInput = compareAndGatherOps(pctx, parentOp1, parentOp2);
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

    discardableInputOps.addAll(gatherDPPBranchOps(pctx, optimizerCache, discardableInputOps));
    discardableInputOps.addAll(gatherDPPBranchOps(pctx, optimizerCache, discardableOps));
    discardableInputOps.addAll(gatherDPPBranchOps(pctx, optimizerCache, retainableOps, discardableInputOps));
    return new SharedResult(retainableOps, discardableOps, discardableInputOps, dataSize, maxDataSize);
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
        Collection<Operator<?>> c = optimizerCache.tableScanToDPPSource.get((TableScanOperator) op);
        for (Operator<?> dppSource : c) {
          // Remove the branches
          Operator<?> currentOp = dppSource;
          while (currentOp.getNumChild() <= 1) {
            dppBranches.add(currentOp);
            currentOp = currentOp.getParentOperators().get(0);
          }
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
        Collection<Operator<?>> c = optimizerCache.tableScanToDPPSource.get((TableScanOperator) op);
        for (Operator<?> dppSource : c) {
          Set<Operator<?>> ascendants =
              findAscendantWorkOperators(pctx, optimizerCache, dppSource);
          if (!Collections.disjoint(ascendants, discardedOps)) {
            // Remove branch
            Operator<?> currentOp = dppSource;
            while (currentOp.getNumChild() <= 1) {
              dppBranches.add(currentOp);
              currentOp = currentOp.getParentOperators().get(0);
            }
          }
        }
      }
    }
    return dppBranches;
  }

  private static List<Operator<?>> compareAndGatherOps(ParseContext pctx,
          Operator<?> op1, Operator<?> op2) throws SemanticException {
    List<Operator<?>> result = new ArrayList<>();
    boolean mergeable = compareAndGatherOps(pctx, op1, op2, result, true);
    if (!mergeable) {
      return null;
    }
    return result;
  }

  private static boolean compareAndGatherOps(ParseContext pctx, Operator<?> op1, Operator<?> op2,
      List<Operator<?>> result, boolean gather) throws SemanticException {
    if (!compareOperator(pctx, op1, op2)) {
      LOG.debug("Operators not equal: {} and {}", op1, op2);
      return false;
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
        boolean mergeable;
        if (gather && op2ParentOp.getChildOperators().size() < 2) {
          mergeable = compareAndGatherOps(pctx, op1ParentOp, op2ParentOp, result, true);
        } else {
          mergeable = compareAndGatherOps(pctx, op1ParentOp, op2ParentOp, result, false);
        }
        if (!mergeable) {
          return false;
        }
      }
    } else if (op1ParentOperators != null || op2ParentOperators != null) {
      return false;
    }

    return true;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
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
    if (op1 instanceof ReduceSinkOperator) {
      ReduceSinkDesc op1Conf = ((ReduceSinkOperator) op1).getConf();
      ReduceSinkDesc op2Conf = ((ReduceSinkOperator) op2).getConf();

      if (StringUtils.equals(op1Conf.getKeyColString(), op2Conf.getKeyColString()) &&
        StringUtils.equals(op1Conf.getValueColsString(), op2Conf.getValueColsString()) &&
        StringUtils.equals(op1Conf.getParitionColsString(), op2Conf.getParitionColsString()) &&
        op1Conf.getTag() == op2Conf.getTag() &&
        StringUtils.equals(op1Conf.getOrder(), op2Conf.getOrder()) &&
        op1Conf.getTopN() == op2Conf.getTopN() &&
        op1Conf.isAutoParallel() == op2Conf.isAutoParallel()) {
        return true;
      } else {
        return false;
      }
    }

    // We handle TableScanOperator here as we can safely ignore table alias
    // and the current comparator implementation does not.
    if (op1 instanceof TableScanOperator) {
      TableScanOperator tsOp1 = (TableScanOperator) op1;
      TableScanOperator tsOp2 = (TableScanOperator) op2;
      TableScanDesc op1Conf = tsOp1.getConf();
      TableScanDesc op2Conf = tsOp2.getConf();

      if (StringUtils.equals(
              op1Conf.getTableMetadata().getDbName() + "." + op1Conf.getTableMetadata().getTableName(),
              op2Conf.getTableMetadata().getDbName() + "." + op2Conf.getTableMetadata().getTableName()) &&
        op1Conf.getNeededColumns().equals(op2Conf.getNeededColumns()) &&
        StringUtils.equals(op1Conf.getFilterExprString(), op2Conf.getFilterExprString()) &&
        pctx.getPrunedPartitions(tsOp1).getPartitions().equals(
              pctx.getPrunedPartitions(tsOp2).getPartitions()) &&
        op1Conf.getRowLimit() == op2Conf.getRowLimit()) {
        return true;
      } else {
        return false;
      }
    }

    OperatorComparatorFactory.OperatorComparator operatorComparator =
      OperatorComparatorFactory.getOperatorComparator(op1.getClass());
    return operatorComparator.equals(op1, op2);
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

    TableScanOperator tsOp1 = (TableScanOperator) sr.retainableOps.get(0);
    TableScanOperator tsOp2 = (TableScanOperator) sr.discardableOps.get(0);

    // 1) The set of operators in the works of the TS operators need to meet
    // some requirements. In particular:
    // 1.1. None of the works that contain the TS operators can contain a Union
    // operator. This is not supported yet as we might end up with cycles in
    // the Tez DAG.
    // 1.2. There cannot be more than one DummyStore operator in the new resulting
    // work when the TS operators are merged. This is due to an assumption in
    // MergeJoinProc that needs to be further explored.
    // If any of these conditions are not met, we cannot merge.
    // TODO: Extend rule so it can be applied for these cases.
    final Set<Operator<?>> workOps1 = findWorkOperators(optimizerCache, tsOp1);
    final Set<Operator<?>> workOps2 = findWorkOperators(optimizerCache, tsOp2);
    boolean foundDummyStoreOp = false;
    for (Operator<?> op : workOps1) {
      if (op instanceof UnionOperator) {
        // We cannot merge (1.1)
        return false;
      }
      if (op instanceof DummyStoreOperator) {
        foundDummyStoreOp = true;
      }
    }
    for (Operator<?> op : workOps2) {
      if (op instanceof UnionOperator) {
        // We cannot merge (1.1)
        return false;
      }
      if (foundDummyStoreOp && op instanceof DummyStoreOperator) {
        // We cannot merge (1.2)
        return false;
      }
    }
    // 2) We check whether output works when we merge the operators will collide.
    //
    //   Work1   Work2    (merge TS in W1 & W2)        Work1
    //       \   /                  ->                  | |       X
    //       Work3                                     Work3
    //
    // If we do, we cannot merge. The reason is that Tez currently does
    // not support parallel edges, i.e., multiple edges from same work x
    // into same work y.
    final Set<Operator<?>> outputWorksOps1 = findChildWorkOperators(pctx, optimizerCache, tsOp1);
    final Set<Operator<?>> outputWorksOps2 = findChildWorkOperators(pctx, optimizerCache, tsOp2);
    if (!Collections.disjoint(outputWorksOps1, outputWorksOps2)) {
      // We cannot merge
      return false;
    }
    // 3) We check whether we will end up with same operators inputing on same work.
    //
    //       Work1        (merge TS in W2 & W3)        Work1
    //       /   \                  ->                  | |       X
    //   Work2   Work3                                 Work2
    //
    // If we do, we cannot merge. The reason is the same as above, currently
    // Tez currently does not support parallel edges.
    final Set<Operator<?>> inputWorksOps1 = findParentWorkOperators(pctx, optimizerCache, tsOp1);
    final Set<Operator<?>> inputWorksOps2 =
            findParentWorkOperators(pctx, optimizerCache, tsOp2, sr.discardableInputOps);
    if (!Collections.disjoint(inputWorksOps1, inputWorksOps2)) {
      // We cannot merge
      return false;
    }
    // 4) We check whether one of the operators is part of a work that is an input for
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
            findDescendantWorkOperators(pctx, optimizerCache, tsOp1, sr.discardableInputOps);
    final Set<Operator<?>> descendantWorksOps2 =
            findDescendantWorkOperators(pctx, optimizerCache, tsOp2, sr.discardableInputOps);
    if (!Collections.disjoint(descendantWorksOps1, workOps2)
            || !Collections.disjoint(workOps1, descendantWorksOps2)) {
      return false;
    }
    return true;
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
      } else if (op instanceof TableScanOperator) {
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
          SharedWorkOptimizerCache optimizerCache, Operator<?> start) {
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
        set.addAll(findWorkOperators(
                optimizerCache, ((DynamicPruningEventDesc) op.getConf()).getTableScan()));
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
    Set<Operator<?>> c = optimizerCache.operatorToWorkOperators.get(start);
    if (!c.isEmpty()) {
      return c;
    }
    c = findWorkOperators(start, new HashSet<Operator<?>>());
    for (Operator<?> op : c) {
      optimizerCache.operatorToWorkOperators.putAll(op, c);
    }
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
          SharedWorkOptimizerCache optimizerCache, TableScanOperator tsOp)
                  throws UDFArgumentException {
    ExprNodeGenericFuncDesc tableScanExprNode = tsOp.getConf().getFilterExpr();
    List<Operator<? extends OperatorDesc>> allChildren =
            Lists.newArrayList(tsOp.getChildOperators());
    for (Operator<? extends OperatorDesc> op : allChildren) {
      if (op instanceof FilterOperator) {
        FilterOperator filterOp = (FilterOperator) op;
        ExprNodeDesc filterExprNode  = filterOp.getConf().getPredicate();
        if (tableScanExprNode.isSame(filterExprNode)) {
          // We do not need to do anything
          return;
        }
        if (tableScanExprNode.getGenericUDF() instanceof GenericUDFOPOr) {
          for (ExprNodeDesc childExprNode : tableScanExprNode.getChildren()) {
            if (childExprNode.isSame(filterExprNode)) {
              // We do not need to do anything, it is in the OR expression
              // so probably we pushed previously
              return;
            }
          }
        }
        ExprNodeGenericFuncDesc newPred = ExprNodeGenericFuncDesc.newInstance(
                new GenericUDFOPAnd(),
                Arrays.<ExprNodeDesc>asList(tableScanExprNode.clone(), filterExprNode));
        filterOp.getConf().setPredicate(newPred);
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
      }
    }
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
  }

  /** Cache to accelerate optimization */
  private static class SharedWorkOptimizerCache {
    // Operators that belong to each work
    final HashMultimap<Operator<?>, Operator<?>> operatorToWorkOperators =
            HashMultimap.<Operator<?>, Operator<?>>create();
    // Table scan operators to DPP sources
    final Multimap<TableScanOperator, Operator<?>> tableScanToDPPSource =
            HashMultimap.<TableScanOperator, Operator<?>>create();

    // Add new operator to cache work group of existing operator (if group exists)
    void putIfWorkExists(Operator<?> opToAdd, Operator<?> existingOp) {
      List<Operator<?>> c = ImmutableList.copyOf(operatorToWorkOperators.get(existingOp));
      if (!c.isEmpty()) {
        for (Operator<?> op : c) {
          operatorToWorkOperators.get(op).add(opToAdd);
        }
        operatorToWorkOperators.putAll(opToAdd, c);
        operatorToWorkOperators.put(opToAdd, opToAdd);
      }
    }

    // Remove operator
    void removeOp(Operator<?> opToRemove) {
      Set<Operator<?>> s = operatorToWorkOperators.get(opToRemove);
      s.remove(opToRemove);
      List<Operator<?>> c1 = ImmutableList.copyOf(s);
      if (!c1.isEmpty()) {
        for (Operator<?> op1 : c1) {
          operatorToWorkOperators.remove(op1, opToRemove); // Remove operator
        }
        operatorToWorkOperators.removeAll(opToRemove); // Remove entry for operator
      }
    }

    // Remove operator and combine
    void removeOpAndCombineWork(Operator<?> opToRemove, Operator<?> replacementOp) {
      Set<Operator<?>> s = operatorToWorkOperators.get(opToRemove);
      s.remove(opToRemove);
      List<Operator<?>> c1 = ImmutableList.copyOf(s);
      List<Operator<?>> c2 = ImmutableList.copyOf(operatorToWorkOperators.get(replacementOp));
      if (!c1.isEmpty() && !c2.isEmpty()) {
        for (Operator<?> op1 : c1) {
          operatorToWorkOperators.remove(op1, opToRemove); // Remove operator
          operatorToWorkOperators.putAll(op1, c2); // Add ops of new collection
        }
        operatorToWorkOperators.removeAll(opToRemove); // Remove entry for operator
        for (Operator<?> op2 : c2) {
          operatorToWorkOperators.putAll(op2, c1); // Add ops to existing collection
        }
      }
    }
  }

}
