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

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

/**
 * Operator factory for pruning processing of operator graph We find
 * all the filter operators that appear just beneath the table scan operators.
 * We then pass the filter to the pruner to construct a pruner for
 * that table alias and store a mapping from the table scan operator to that
 * pruner. We call that pruner later during plan generation.
 *
 * Create this class from org.apache.hadoop.hive.ql.optimizer.ppr.OpProcFactory
 * so that in addition to ppr, other pruner like list bucketin pruner can use it.
 */
public abstract class PrunerOperatorFactory {

  /**
   * Determines the partition pruner for the filter. This is called only when
   * the filter follows a table scan operator.
   */
  public static abstract class FilterPruner implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
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

      generatePredicate(procCtx, fop, top);

      return null;
    }

    /**
     * Generate predicate.
     *
     * Subclass should implement the function. Please refer to {@link OpProcFactory.FilterPPR}
     *
     * @param procCtx
     * @param fop
     * @param top
     * @throws SemanticException
     * @throws UDFArgumentException
     */
    protected abstract void generatePredicate(NodeProcessorCtx procCtx, FilterOperator fop,
        TableScanOperator top) throws SemanticException, UDFArgumentException;
    /**
     * Add pruning predicate.
     *
     * @param opToPrunner
     * @param top
     * @param new_pruner_pred
     * @throws UDFArgumentException
     */
    protected void addPruningPred(Map<TableScanOperator, ExprNodeDesc> opToPrunner,
        TableScanOperator top, ExprNodeDesc new_pruner_pred) throws UDFArgumentException {
      ExprNodeDesc old_pruner_pred = opToPrunner.get(top);
      ExprNodeDesc pruner_pred = null;
      if (old_pruner_pred != null) {
        // or the old_pruner_pred and the new_ppr_pred
        pruner_pred = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("OR",
            old_pruner_pred, new_pruner_pred);
      } else {
        pruner_pred = new_pruner_pred;
      }

      // Put the mapping from table scan operator to pruner_pred
      opToPrunner.put(top, pruner_pred);

      return;
    }

    /**
     * Add pruning predicate.
     *
     * @param opToPrunner
     * @param top
     * @param new_pruner_pred
     * @param part
     * @throws UDFArgumentException
     */
    protected void addPruningPred(Map<TableScanOperator, Map<String, ExprNodeDesc>> opToPrunner,
        TableScanOperator top, ExprNodeDesc new_pruner_pred, Partition part)
        throws UDFArgumentException {
      Map<String, ExprNodeDesc> oldPartToPruner = opToPrunner.get(top);
      Map<String, ExprNodeDesc> partToPruner = null;
      ExprNodeDesc pruner_pred = null;
      if (oldPartToPruner == null) {
        pruner_pred = new_pruner_pred;
        // create new mapping
        partToPruner = new HashMap<String, ExprNodeDesc>();
      } else {
        partToPruner = oldPartToPruner;
        ExprNodeDesc old_pruner_pred = oldPartToPruner.get(part.getName());
        if (old_pruner_pred != null) {
          // or the old_pruner_pred and the new_ppr_pred
          pruner_pred = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("OR",
              old_pruner_pred, new_pruner_pred);
        } else {
          pruner_pred = new_pruner_pred;
        }
      }

      // Put the mapping from part to pruner_pred
      partToPruner.put(part.getName(), pruner_pred);

      // Put the mapping from table scan operator to part-pruner map
      opToPrunner.put(top, partToPruner);

      return;
    }
  }

  /**
   * Default processor which just merges its children.
   */
  public static class DefaultPruner implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      // Nothing needs to be done.
      return null;
    }
  }

  /**
   * Instantiate default processor.
   *
   * It's not supposed to be overwritten.
   *
   * @return
   */
  public final static NodeProcessor getDefaultProc() {
    return new DefaultPruner();
  }

}
