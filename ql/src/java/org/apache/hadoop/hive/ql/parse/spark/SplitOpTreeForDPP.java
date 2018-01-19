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

package org.apache.hadoop.hive.ql.parse.spark;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.spark.SparkUtilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;


/**
 * This processor triggers on SparkPartitionPruningSinkOperator. For a operator tree like
 * this:
 *
 * Original Tree:
 *     TS1       TS2
 *      |          |
 *      FIL       FIL
 *      |          |
 *      RS      /   \   \
 *      |      |    \    \
 *      |     RS  SEL  SEL
 *      \   /      |     |
 *      JOIN      GBY   GBY
 *                  |    |
 *                  |  SPARKPRUNINGSINK
 *                  |
 *              SPARKPRUNINGSINK
 *
 * It removes the branch containing SPARKPRUNINGSINK from the original operator tree, and splits it into
 * two separate trees:
 * Tree #1:                       Tree #2
 *      TS1    TS2                 TS2
 *      |      |                    |
 *      FIL    FIL                 FIL
 *      |       |                   |_____
 *      RS     SEL                  |     \
 *      |       |                   SEL    SEL
 *      |     RS                    |      |
 *      \   /                       GBY    GBY
 *      JOIN                        |      |
 *                                  |    SPARKPRUNINGSINK
 *                                 SPARKPRUNINGSINK

 * For MapJoinOperator, this optimizer will not do anything - it should be executed within
 * the same SparkTask.
 */
public class SplitOpTreeForDPP implements NodeProcessor {

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
                        Object... nodeOutputs) throws SemanticException {
    SparkPartitionPruningSinkOperator pruningSinkOp = (SparkPartitionPruningSinkOperator) nd;
    GenSparkProcContext context = (GenSparkProcContext) procCtx;

    for (Operator<?> op : context.pruningSinkSet) {
      if (pruningSinkOp.getOperatorId().equals(op.getOperatorId())) {
        return null;
      }
    }

    // If pruning sink operator is with map join, then pruning sink need not be split to a
    // separate tree.  Add the pruning sink operator to context and return
    if (pruningSinkOp.isWithMapjoin()) {
      context.pruningSinkSet.add(pruningSinkOp);
      return null;
    }

    List<Operator<?>> roots = new LinkedList<Operator<?>>();
    collectRoots(roots, pruningSinkOp);

    Operator<?> branchingOp = pruningSinkOp.getBranchingOp();
    List<Operator<?>> savedChildOps = branchingOp.getChildOperators();
    List<Operator<?>> firstNodesOfPruningBranch = findFirstNodesOfPruningBranch(branchingOp);
    branchingOp.setChildOperators(Utilities.makeList(firstNodesOfPruningBranch.toArray(new
        Operator<?>[firstNodesOfPruningBranch.size()])));

    // Now clone the tree above selOp
    List<Operator<?>> newRoots = SerializationUtilities.cloneOperatorTree(roots);
    for (int i = 0; i < roots.size(); i++) {
      TableScanOperator newTs = (TableScanOperator) newRoots.get(i);
      TableScanOperator oldTs = (TableScanOperator) roots.get(i);
      newTs.getConf().setTableMetadata(oldTs.getConf().getTableMetadata());
    }
    context.clonedPruningTableScanSet.addAll(newRoots);

    //Find all pruningSinkSet in old roots
    List<Operator<?>> oldsinkList = new ArrayList<>();
    for (Operator<?> root : roots) {
      SparkUtilities.collectOp(oldsinkList, root, SparkPartitionPruningSinkOperator.class);
    }

    // Restore broken links between operators, and remove the branch from the original tree
    branchingOp.setChildOperators(savedChildOps);
    for (Operator selOp : firstNodesOfPruningBranch) {
      branchingOp.removeChild(selOp);
    }

    //Find all pruningSinkSet in new roots
    Set<Operator<?>> sinkSet = new LinkedHashSet<>();
    for (Operator<?> root : newRoots) {
      SparkUtilities.collectOp(sinkSet, root, SparkPartitionPruningSinkOperator.class);
    }

    int i = 0;
    for (Operator<?> clonedPruningSinkOp : sinkSet) {
      SparkPartitionPruningSinkOperator oldsinkOp = (SparkPartitionPruningSinkOperator) oldsinkList.get(i++);
      ((SparkPartitionPruningSinkOperator) clonedPruningSinkOp).getConf().setTableScan(oldsinkOp.getConf().getTableScan());
      context.pruningSinkSet.add(clonedPruningSinkOp);

    }
    return null;
  }

  //find operators which are the children of specified filterOp and there are SparkPartitionPruningSink in these
  //branches.
  private List<Operator<?>> findFirstNodesOfPruningBranch(Operator<?> branchingOp) {
    List<Operator<?>> res = new ArrayList<>();
    for (Operator child : branchingOp.getChildOperators()) {
      List<Operator<?>> pruningList = new ArrayList<>();
      SparkUtilities.collectOp(pruningList, child, SparkPartitionPruningSinkOperator.class);
      if (pruningList.size() > 0) {
        res.add(child);
      }
    }
    return res;
  }

    /**
   * Recursively collect all roots (e.g., table scans) that can be reached via this op.
   * @param result contains all roots can be reached via op
   * @param op the op to examine.
   */
  private void collectRoots(List<Operator<?>> result, Operator<?> op) {
    if (op.getNumParent() == 0) {
      result.add(op);
    } else {
      for (Operator<?> parentOp : op.getParentOperators()) {
        collectRoots(result, parentOp);
      }
    }
  }

}
