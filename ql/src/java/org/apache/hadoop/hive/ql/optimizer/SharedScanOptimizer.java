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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.SemiJoinBranchInfo;
import org.apache.hadoop.hive.ql.plan.DynamicPruningEventDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

/**
 * Shared scan optimizer. This rule finds scan operator over the same table
 * in the query plan and merges them if they meet some preconditions.
 *
 *  TS   TS             TS
 *  |    |     ->      /  \
 *  Op   Op           Op  Op
 *
 * <p>Currently it only works with the Tez execution engine.
 */
public class SharedScanOptimizer extends Transform {

  private final static Logger LOG = LoggerFactory.getLogger(SharedScanOptimizer.class);

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    final Map<String, TableScanOperator> topOps = pctx.getTopOps();
    if (topOps.size() < 2) {
      // Nothing to do, bail out
      return pctx;
    }

    // Cache to use during optimization
    SharedScanOptimizerCache optimizerCache = new SharedScanOptimizerCache();

    // We will not apply this optimization on some table scan operators.
    Set<TableScanOperator> excludeTableScanOps = gatherNotValidTableScanOps(pctx, optimizerCache);
    LOG.debug("Exclude TableScan ops: {}", excludeTableScanOps);

    // Map of dbName.TblName -> Pair(tableAlias, TSOperator)
    Multimap<String, Entry<String, TableScanOperator>> tableNameToOps = splitTableScanOpsByTable(pctx);

    // We enforce a certain order when we do the reutilization.
    // In particular, we use size of table x number of reads to
    // rank the tables.
    List<Entry<String, Long>> sortedTables = rankTablesByAccumulatedSize(pctx, excludeTableScanOps);
    LOG.debug("Sorted tables by size: {}", sortedTables);

    // Execute optimization
    Multimap<String, TableScanOperator> existingOps = ArrayListMultimap.create();
    Set<String> entriesToRemove = new HashSet<>();
    for (Entry<String, Long> tablePair : sortedTables) {
      for (Entry<String, TableScanOperator> tableScanOpPair : tableNameToOps.get(tablePair.getKey())) {
        TableScanOperator tsOp = tableScanOpPair.getValue();
        if (excludeTableScanOps.contains(tsOp)) {
          // Skip operator, currently we do not merge
          continue;
        }
        String tableName = tablePair.getKey();
        Collection<TableScanOperator> prevTsOps = existingOps.get(tableName);
        if (!prevTsOps.isEmpty()) {
          for (TableScanOperator prevTsOp : prevTsOps) {

            // First we check if the two table scan operators can actually be merged
            // If schemas do not match, we currently do not merge
            List<String> prevTsOpNeededColumns = prevTsOp.getNeededColumns();
            List<String> tsOpNeededColumns = tsOp.getNeededColumns();
            if (prevTsOpNeededColumns.size() != tsOpNeededColumns.size()) {
              // Skip
              continue;
            }
            boolean notEqual = false;
            for (int i = 0; i < prevTsOpNeededColumns.size(); i++) {
              if (!prevTsOpNeededColumns.get(i).equals(tsOpNeededColumns.get(i))) {
                notEqual = true;
                break;
              }
            }
            if (notEqual) {
              // Skip
              continue;
            }
            // If row limit does not match, we currently do not merge
            if (prevTsOp.getConf().getRowLimit() != tsOp.getConf().getRowLimit()) {
              // Skip
              continue;
            }

            // It seems these two operators can be merged.
            // Check that plan meets some preconditions before doing it.
            // In particular, in the presence of map joins in the upstream plan:
            // - we cannot exceed the noconditional task size, and
            // - if we already merged the big table, we cannot merge the broadcast
            // tables.
            if (!validPreConditions(pctx, optimizerCache, prevTsOp, tsOp)) {
              // Skip
              LOG.debug("{} does not meet preconditions", tsOp);
              continue;
            }

            // We can merge
            ExprNodeGenericFuncDesc exprNode = null;
            if (prevTsOp.getConf().getFilterExpr() != null) {
              // Push filter on top of children
              pushFilterToTopOfTableScan(optimizerCache, prevTsOp);
              // Clone to push to table scan
              exprNode = (ExprNodeGenericFuncDesc) prevTsOp.getConf().getFilterExpr();
            }
            if (tsOp.getConf().getFilterExpr() != null) {
              // Push filter on top
              pushFilterToTopOfTableScan(optimizerCache, tsOp);
              ExprNodeGenericFuncDesc tsExprNode = tsOp.getConf().getFilterExpr();
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
            prevTsOp.getConf().setFilterExpr(exprNode);
            // Replace table scan operator
            List<Operator<? extends OperatorDesc>> allChildren =
                    Lists.newArrayList(tsOp.getChildOperators());
            for (Operator<? extends OperatorDesc> op : allChildren) {
              tsOp.getChildOperators().remove(op);
              op.replaceParent(tsOp, prevTsOp);
              prevTsOp.getChildOperators().add(op);
            }
            entriesToRemove.add(tableScanOpPair.getKey());
            // Remove and combine
            optimizerCache.removeOpAndCombineWork(tsOp, prevTsOp);

            LOG.debug("Merged {} into {}", tsOp, prevTsOp);

            break;
          }
          if (!entriesToRemove.contains(tableScanOpPair.getKey())) {
            existingOps.put(tableName, tsOp);
          }
        } else {
          // Add to existing ops
          existingOps.put(tableName, tsOp);
        }
      }
    }
    // Remove unused operators
    for (String key : entriesToRemove) {
      topOps.remove(key);
    }

    return pctx;
  }

  private static Set<TableScanOperator> gatherNotValidTableScanOps(
          ParseContext pctx, SharedScanOptimizerCache optimizerCache) {
    // Find TS operators with partition pruning enabled in plan
    // because these TS may potentially read different data for
    // different pipeline.
    // These can be:
    // 1) TS with static partitioning.
    //    TODO: Check partition list of different TS and do not add if they are identical
    // 2) TS with DPP.
    //    TODO: Check if dynamic filters are identical and do not add.
    // 3) TS with semijoin DPP.
    //    TODO: Check for dynamic filters.
    Set<TableScanOperator> notValidTableScanOps = new HashSet<>();
    // 1) TS with static partitioning.
    Map<String, TableScanOperator> topOps = pctx.getTopOps();
    for (TableScanOperator tsOp : topOps.values()) {
      if (tsOp.getConf().getPartColumns() != null &&
              !tsOp.getConf().getPartColumns().isEmpty()) {
        notValidTableScanOps.add(tsOp);
      }
    }
    // 2) TS with DPP.
    Collection<Operator<? extends OperatorDesc>> tableScanOps =
            Lists.<Operator<?>>newArrayList(topOps.values());
    Set<AppMasterEventOperator> s =
            OperatorUtils.findOperators(tableScanOps, AppMasterEventOperator.class);
    for (AppMasterEventOperator a : s) {
      if (a.getConf() instanceof DynamicPruningEventDesc) {
        DynamicPruningEventDesc dped = (DynamicPruningEventDesc) a.getConf();
        notValidTableScanOps.add(dped.getTableScan());
        optimizerCache.tableScanToDPPSource.put(dped.getTableScan(), a);
      }
    }
    // 3) TS with semijoin DPP.
    for (Entry<ReduceSinkOperator, SemiJoinBranchInfo> e
            : pctx.getRsToSemiJoinBranchInfo().entrySet()) {
      notValidTableScanOps.add(e.getValue().getTsOp());
      optimizerCache.tableScanToDPPSource.put(e.getValue().getTsOp(), e.getKey());
    }
    return notValidTableScanOps;
  }

  private static Multimap<String, Entry<String, TableScanOperator>> splitTableScanOpsByTable(
          ParseContext pctx) {
    Multimap<String, Entry<String, TableScanOperator>> tableNameToOps = ArrayListMultimap.create();
    for (Entry<String, TableScanOperator> e : pctx.getTopOps().entrySet()) {
      TableScanOperator tsOp = e.getValue();
      tableNameToOps.put(
              tsOp.getConf().getTableMetadata().getDbName() + "."
                      + tsOp.getConf().getTableMetadata().getTableName(), e);
    }
    return tableNameToOps;
  }

  private static List<Entry<String, Long>> rankTablesByAccumulatedSize(ParseContext pctx,
          Set<TableScanOperator> excludeTables) {
    Map<String, Long> tableToTotalSize = new HashMap<>();
    for (Entry<String, TableScanOperator> e : pctx.getTopOps().entrySet()) {
      TableScanOperator tsOp = e.getValue();
      if (excludeTables.contains(tsOp)) {
        // Skip operator, currently we do not merge
        continue;
      }
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

  private static boolean validPreConditions(ParseContext pctx, SharedScanOptimizerCache optimizerCache,
          TableScanOperator prevTsOp, TableScanOperator tsOp) {
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
    final Set<Operator<?>> workOps1 = findWorkOperators(optimizerCache, prevTsOp);
    final Set<Operator<?>> workOps2 = findWorkOperators(optimizerCache, tsOp);
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
    final Set<Operator<?>> outputWorksOps1 = findChildWorkOperators(pctx, optimizerCache, prevTsOp);
    final Set<Operator<?>> outputWorksOps2 = findChildWorkOperators(pctx, optimizerCache, tsOp);
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
    final Set<Operator<?>> inputWorksOps1 = findParentWorkOperators(pctx, optimizerCache, prevTsOp);
    final Set<Operator<?>> inputWorksOps2 = findParentWorkOperators(pctx, optimizerCache, tsOp);
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
            findDescendantWorkOperators(pctx, optimizerCache, prevTsOp);
    final Set<Operator<?>> descendantWorksOps2 =
            findDescendantWorkOperators(pctx, optimizerCache, tsOp);
    if (!Collections.disjoint(descendantWorksOps1, workOps2)
            || !Collections.disjoint(workOps1, descendantWorksOps2)) {
      return false;
    }
    // 5) We check whether merging the works would cause the size of
    // the data in memory grow too large.
    // TODO: Currently ignores GBY and PTF which may also buffer data in memory.
    final Set<Operator<?>> newWorkOps = workOps1;
    newWorkOps.addAll(workOps2);
    long dataSize = 0L;
    for (Operator<?> op : newWorkOps) {
      if (op instanceof MapJoinOperator) {
        MapJoinOperator mop = (MapJoinOperator) op;
        dataSize = StatsUtils.safeAdd(dataSize, mop.getConf().getInMemoryDataSize());
        if (dataSize > mop.getConf().getMemoryMonitorInfo().getAdjustedNoConditionalTaskSize()) {
          // Size surpasses limit, we cannot convert
          LOG.debug("accumulated data size: {} / max size: {}",
                  dataSize, mop.getConf().getMemoryMonitorInfo().getAdjustedNoConditionalTaskSize());
          return false;
        }
      }
    }
    return true;
  }

  private static Set<Operator<?>> findParentWorkOperators(ParseContext pctx,
          SharedScanOptimizerCache optimizerCache, Operator<?> start) {
    // Find operators in work
    Set<Operator<?>> workOps = findWorkOperators(optimizerCache, start);
    // Gather input works operators
    Set<Operator<?>> set = new HashSet<Operator<?>>();
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
    return set;
  }

  private static Set<Operator<?>> findChildWorkOperators(ParseContext pctx,
          SharedScanOptimizerCache optimizerCache, Operator<?> start) {
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
          SharedScanOptimizerCache optimizerCache, Operator<?> start) {
    // Find operators in work
    Set<Operator<?>> workOps = findWorkOperators(optimizerCache, start);
    // Gather output works operators
    Set<Operator<?>> result = new HashSet<Operator<?>>();
    Set<Operator<?>> set;
    while (!workOps.isEmpty()) {
      set = new HashSet<Operator<?>>();
      for (Operator<?> op : workOps) {
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
          SharedScanOptimizerCache optimizerCache, Operator<?> start) {
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
          SharedScanOptimizerCache optimizerCache, TableScanOperator tsOp)
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

  /** Cache to accelerate optimization */
  private static class SharedScanOptimizerCache {
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
