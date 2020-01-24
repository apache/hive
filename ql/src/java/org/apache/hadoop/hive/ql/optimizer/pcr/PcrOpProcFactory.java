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

package org.apache.hadoop.hive.ql.optimizer.pcr;

import java.util.ArrayList;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.optimizer.ConstantPropagateProcFactory;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * PcrOpProcFactory contains processors that process expression tree of filter operators
 * following table scan operators. It walks the expression tree of the filter operator
 * to remove partition predicates when possible. If the filter operator can be removed,
 * the whole operator is marked to be removed later on, otherwise the predicate is changed
 */
public final class PcrOpProcFactory {

  // The log
  private static final Logger LOG = LoggerFactory
      .getLogger("hive.ql.optimizer.pcr.OpProcFactory");

  /**
   * Remove partition condition in a filter operator when possible. This is
   * called only when the filter follows a table scan operator.
   */
  public static class FilterPCR implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      PcrOpWalkerCtx owc = (PcrOpWalkerCtx) procCtx;
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
      Operator<? extends OperatorDesc> pop = null;
      if (tmp2 instanceof TableScanOperator) {
        top = (TableScanOperator) tmp2;
        pop = top;
      } else {
        top = (TableScanOperator) stack.peek();
        fop2 = (FilterOperator) tmp2;
        pop = fop2;
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

      if (fop.getParentOperators().size() > 1) {
        // It's not likely if there is no bug. But in case it happens, we must
        // have found a wrong filter operator. We skip the optimization then.
        return null;
      }


      ParseContext pctx = owc.getParseContext();
      PrunedPartitionList prunedPartList;
      try {
        String alias = (String) owc.getParseContext().getTopOps().keySet().toArray()[0];
        prunedPartList = pctx.getPrunedPartitions(alias, top);
      } catch (HiveException e) {
        // Has to use full name to make sure it does not conflict with
        // org.apache.commons.lang3.StringUtils
        throw new SemanticException(e.getMessage(), e);
      }

      // Otherwise this is not a sampling predicate. We need to process it.
      ExprNodeDesc predicate = fop.getConf().getPredicate();
      String alias = top.getConf().getAlias();

      ArrayList<Partition> partitions = new ArrayList<Partition>();
      if (prunedPartList == null) {
        return null;
      }

      for (Partition p : prunedPartList.getPartitions()) {
        if (!p.getTable().isPartitioned()) {
          return null;
        }
      }

      partitions.addAll(prunedPartList.getPartitions());

      PcrExprProcFactory.NodeInfoWrapper wrapper = PcrExprProcFactory.walkExprTree(
          alias, partitions, top.getConf().getVirtualCols(), predicate);

      if (wrapper.state == PcrExprProcFactory.WalkState.TRUE) {
        owc.getOpToRemove().add(new PcrOpWalkerCtx.OpToDeleteInfo(pop, fop));
      } else if (wrapper.state == PcrExprProcFactory.WalkState.CONSTANT && wrapper.outExpr instanceof ExprNodeGenericFuncDesc) {
        ExprNodeDesc desc = ConstantPropagateProcFactory.foldExpr((ExprNodeGenericFuncDesc)wrapper.outExpr);
        if (desc != null && desc instanceof ExprNodeConstantDesc && Boolean.TRUE.equals(((ExprNodeConstantDesc)desc).getValue())) {
          owc.getOpToRemove().add(new PcrOpWalkerCtx.OpToDeleteInfo(pop, fop));
        } else {
          fop.getConf().setPredicate(wrapper.outExpr);
        }
      }
      else if (wrapper.state != PcrExprProcFactory.WalkState.FALSE) {
        fop.getConf().setPredicate(wrapper.outExpr);
      } else {
        LOG.warn("Filter passes no row");
        fop.getConf().setPredicate(wrapper.outExpr);
      }

      return null;
    }
  }

  /**
   * Default processor which does nothing
   */
  public static class DefaultPCR implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      // Nothing needs to be done.
      return null;
    }
  }

  public static NodeProcessor getFilterProc() {
    return new FilterPCR();
  }

  public static NodeProcessor getDefaultProc() {
    return new DefaultPCR();
  }

  private PcrOpProcFactory() {
    // prevent instantiation
  }
}
