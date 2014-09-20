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

import com.clearspring.analytics.util.Preconditions;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.ForwardOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;

public class SparkTableScanProcessor implements NodeProcessor {

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
                        Object... nodeOutputs) throws SemanticException {
    GenSparkProcContext context = (GenSparkProcContext) procCtx;
    TableScanOperator tblScan = (TableScanOperator) nd;

    context.opToTaskMap.put(tblScan, context.defaultTask);

    // For multi-table insertion, we first look for potential multiple FSs that can be reached
    // from this TS. In the process of searching, we also record the path to each of these FS.
    // Then, we find the LCA for these FSs.
    //
    // That is, in scenarios like the following:
    //
    //                   OP 1 (TS, UNION, etc)
    //                 /    \
    //               OP 2   OP 3
    //
    // If we find such an operator, we record all of its children to context, and unlink
    //   them with this operator later, in SparkMultiInsertionProcessor, and it will be become:
    //
    //                  OP 1 (TS, UNION, FOR, etc)
    //                  |
    //                  FS
    //
    //              TS      TS
    //              |        |
    //             OP 2     OP 3
    //
    // where the two branches starting with TS are in different Spark tasks.
    //
    // Because of the restrictions on multi-insertion queries, there could only be two
    // categories of TS here: one through which we can reach multiple FSs, and one through
    // which we can only reach one FS. For all TS in the first category, they should only
    // be able to reach the same set of FS.
    // A further conclusion is, there should only be one LCA for the entire operator tree.
    //
    // N.B.: one special case is when OP is ForwardOperator, in which case we shouldn't break
    // the tree since it's already optimized.
    Map<FileSinkOperator, Stack<Operator<? extends OperatorDesc>>> fsToPath
        = new HashMap<FileSinkOperator, Stack<Operator<? extends OperatorDesc>>>();
    Queue<Stack<Operator<? extends OperatorDesc>>> paths =
        new LinkedList<Stack<Operator<? extends OperatorDesc>>>();
    Stack<Operator<? extends OperatorDesc>> p = new Stack<Operator<? extends OperatorDesc>>();
    p.push(tblScan);
    paths.offer(p);

    while (!paths.isEmpty()) {
      Stack<Operator<? extends OperatorDesc>> currPath = paths.poll();
      Operator<? extends OperatorDesc> currOp = currPath.peek();
      if (currOp instanceof FileSinkOperator) {
        FileSinkOperator fsOp = (FileSinkOperator) currOp;
        // In case there are multiple paths lead to this FS, we keep the shortest one.
        // (We could also keep the longest one - it doesn't matter)
        if (!fsToPath.containsKey(fsOp) || currPath.size() < fsToPath.get(fsOp).size()) {
          fsToPath.put(fsOp, currPath);
        }
      }

      for (Operator<? extends OperatorDesc> nextOp : currOp.getChildOperators()) {
        Stack<Operator<? extends OperatorDesc>> nextPath = new Stack<Operator<? extends OperatorDesc>>();
        nextPath.addAll(currPath);
        nextPath.push(nextOp);
        paths.offer(nextPath);
      }
    }

    if (fsToPath.size() > 1) {
      // Now, compute the LOWEST height for all these FSs
      int lowest = -1;
      for (Map.Entry<FileSinkOperator, Stack<Operator<? extends OperatorDesc>>> e : fsToPath.entrySet()) {
        if (lowest < 0 || e.getValue().size() < lowest) {
          lowest = e.getValue().size();
        }
      }

      // Now, we move up those path that has length larger than the lowest
      for (Stack<Operator<? extends OperatorDesc>> st : fsToPath.values()) {
        while (st.size() > lowest) {
          st.pop();
        }
      }

      // Now, we move all paths up together, until we reach a least common ancestor
      Operator<? extends OperatorDesc> lca;
      while (true) {
        lca = null;
        boolean same = true;
        for (Stack<Operator<? extends OperatorDesc>> st : fsToPath.values()) {
          Operator<? extends OperatorDesc> op = st.pop();
          if (lca == null) {
            lca = op;
          } else if (lca != op) {
            same = false; // but we still need to pop the rest..
          }
        }
        if (same) {
          break;
        }
      }

      Preconditions.checkArgument(lca.getNumChild() > 1,
          "AssertionError: the LCA should have multiple children, but got " + lca.getNumChild());

      // Special case: don't break if LCA is FOR.
      if (!(lca instanceof ForwardOperator)) {
        for (Operator<? extends OperatorDesc> childOp : lca.getChildOperators()) {
          context.opToParentMap.put(childOp, lca);
        }
      }
    }

    return null;
  }
}
