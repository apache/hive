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

package org.apache.hadoop.hive.ql.optimizer.ppr;

import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

/**
 * Operator factory for partition pruning processing of operator graph We find
 * all the filter operators that appear just beneath the table scan operators.
 * We then pass the filter to the partition pruner to construct a pruner for
 * that table alias and store a mapping from the table scan operator to that
 * pruner. We call that pruner later during plan generation.
 */
public class OpProcFactory {

  /**
   * Determines the partition pruner for the filter. This is called only when
   * the filter follows a table scan operator.
   */
  public static class FilterPPR implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      OpWalkerCtx owc = (OpWalkerCtx) procCtx;
      FilterOperator fop = (FilterOperator) nd;
      FilterOperator fop2 = null;

      // The stack contains either ... TS, Filter or
      // ... TS, Filter, Filter with the head of the stack being the rightmost
      // symbol. So we just pop out the two elements from the top and if the
      // second one of them is not a table scan then the operator on the top of
      // the stack is the Table scan operator.
      Node tmp = stack.pop();
      Node tmp2 = stack.pop();
      TableScanOperator top = null;
      if (tmp2 instanceof TableScanOperator) {
        top = (TableScanOperator) tmp2;
      } else {
        top = (TableScanOperator) stack.peek();
        fop2 = (FilterOperator) tmp2;
      }
      stack.push(tmp2);
      stack.push(tmp);

      // If fop2 exists (i.e this is not the top level filter and fop2 is not
      // a sampling filter then we ignore the current filter
      if (fop2 != null && !fop2.getConf().getIsSamplingPred()) {
        return null;
      }

      // ignore the predicate in case it is not a sampling predicate
      if (fop.getConf().getIsSamplingPred()) {
        return null;
      }

      // Otherwise this is not a sampling predicate and we need to
      ExprNodeDesc predicate = fop.getConf().getPredicate();
      String alias = top.getConf().getAlias();

      // Generate the partition pruning predicate
      boolean hasNonPartCols = false;
      ExprNodeDesc ppr_pred = ExprProcFactory.genPruner(alias, predicate,
          hasNonPartCols);
      owc.addHasNonPartCols(hasNonPartCols);

      // Add the pruning predicate to the table scan operator
      addPruningPred(owc.getOpToPartPruner(), top, ppr_pred);

      return null;
    }

    private void addPruningPred(Map<TableScanOperator, ExprNodeDesc> opToPPR,
        TableScanOperator top, ExprNodeDesc new_ppr_pred) {
      ExprNodeDesc old_ppr_pred = opToPPR.get(top);
      ExprNodeDesc ppr_pred = null;
      if (old_ppr_pred != null) {
        // or the old_ppr_pred and the new_ppr_pred
        ppr_pred = TypeCheckProcFactory.DefaultExprProcessor
            .getFuncExprNodeDesc("OR", old_ppr_pred, new_ppr_pred);
      } else {
        ppr_pred = new_ppr_pred;
      }

      // Put the mapping from table scan operator to ppr_pred
      opToPPR.put(top, ppr_pred);

      return;
    }
  }

  /**
   * Default processor which just merges its children
   */
  public static class DefaultPPR implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      // Nothing needs to be done.
      return null;
    }
  }

  public static NodeProcessor getFilterProc() {
    return new FilterPPR();
  }

  public static NodeProcessor getDefaultProc() {
    return new DefaultPPR();
  }

}
