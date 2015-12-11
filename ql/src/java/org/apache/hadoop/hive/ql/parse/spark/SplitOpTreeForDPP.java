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

package org.apache.hadoop.hive.ql.parse.spark;

import java.util.HashSet;
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

import com.google.common.base.Preconditions;

/**
 * This processor triggers on SparkPartitionPruningSinkOperator. For a operator tree like
 * this:
 *
 * Original Tree:
 *     TS    TS
 *      |     |
 *     FIL   FIL
 *      |     | \
 *     RS     RS SEL
 *       \   /    |
 *        JOIN   GBY
 *                |
 *               SPARKPRUNINGSINK
 *
 * It removes the branch containing SPARKPRUNINGSINK from the original operator tree, and splits it into
 * two separate trees:
 *
 * Tree #1:                 Tree #2:
 *     TS    TS               TS
 *      |     |                |
 *     FIL   FIL              FIL
 *      |     |                |
 *     RS     RS              SEL
 *       \   /                 |
 *       JOIN                 GBY
 *                             |
 *                            SPARKPRUNINGSINK
 *
 * For MapJoinOperator, this optimizer will not do anything - it should be executed within
 * the same SparkTask.
 */
public class SplitOpTreeForDPP implements NodeProcessor {
  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
                        Object... nodeOutputs) throws SemanticException {
    SparkPartitionPruningSinkOperator pruningSinkOp = (SparkPartitionPruningSinkOperator) nd;
    GenSparkProcContext context = (GenSparkProcContext) procCtx;

    // Locate the op where the branch starts
    // This is guaranteed to succeed since the branch always follow the pattern
    // as shown in the first picture above.
    Operator<?> filterOp = pruningSinkOp;
    Operator<?> selOp = null;
    while (filterOp != null) {
      if (filterOp.getNumChild() > 1) {
        break;
      } else {
        selOp = filterOp;
        filterOp = filterOp.getParentOperators().get(0);
      }
    }

    // Check if this is a MapJoin. If so, do not split.
    for (Operator<?> childOp : filterOp.getChildOperators()) {
      if (childOp instanceof ReduceSinkOperator &&
          childOp.getChildOperators().get(0) instanceof MapJoinOperator) {
        context.pruningSinkSet.add(pruningSinkOp);
        return null;
      }
    }

    List<Operator<?>> roots = new LinkedList<Operator<?>>();
    collectRoots(roots, pruningSinkOp);

    List<Operator<?>> savedChildOps = filterOp.getChildOperators();
    filterOp.setChildOperators(Utilities.makeList(selOp));

    // Now clone the tree above selOp
    List<Operator<?>> newRoots = SerializationUtilities.cloneOperatorTree(roots);
    for (int i = 0; i < roots.size(); i++) {
      TableScanOperator newTs = (TableScanOperator) newRoots.get(i);
      TableScanOperator oldTs = (TableScanOperator) roots.get(i);
      newTs.getConf().setTableMetadata(oldTs.getConf().getTableMetadata());
    }
    context.clonedPruningTableScanSet.addAll(newRoots);

    // Restore broken links between operators, and remove the branch from the original tree
    filterOp.setChildOperators(savedChildOps);
    filterOp.removeChild(selOp);

    // Find the cloned PruningSink and add it to pruningSinkSet
    Set<Operator<?>> sinkSet = new HashSet<Operator<?>>();
    for (Operator<?> root : newRoots) {
      SparkUtilities.collectOp(sinkSet, root, SparkPartitionPruningSinkOperator.class);
    }
    Preconditions.checkArgument(sinkSet.size() == 1,
        "AssertionError: expected to only contain one SparkPartitionPruningSinkOperator," +
            " but found " + sinkSet.size());
    SparkPartitionPruningSinkOperator clonedPruningSinkOp =
        (SparkPartitionPruningSinkOperator) sinkSet.iterator().next();
    clonedPruningSinkOp.getConf().setTableScan(pruningSinkOp.getConf().getTableScan());
    context.pruningSinkSet.add(clonedPruningSinkOp);

    return null;
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
