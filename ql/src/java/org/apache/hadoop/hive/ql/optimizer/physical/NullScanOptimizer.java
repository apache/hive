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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.physical.MetadataOnlyOptimizer.WalkerCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This optimizer attempts following two optimizations:
 * 1. If it finds TS followed By FIL which has been determined at compile time to evaluate to
 *    zero, it removes all input paths for that table scan.
 * 2. If it finds TS followed by Limit 0, it removes all input paths from table scan.
 */
public class NullScanOptimizer implements PhysicalPlanResolver {

  private static final Logger LOG =
      LoggerFactory.getLogger(NullScanOptimizer.class);

  @Override
  public PhysicalContext resolve(PhysicalContext pctx)
      throws SemanticException {
    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<>();
    opRules.put(
        new RuleRegExp("R1",
            TableScanOperator.getOperatorName() + "%.*"
                + FilterOperator.getOperatorName() + "%"),
        new WhereFalseProcessor());
    SemanticDispatcher disp = new NullScanTaskDispatcher(pctx, opRules);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);
    List<Node> topNodes = new ArrayList<>(pctx.getRootTasks());
    ogw.startWalking(topNodes, null);

    opRules.clear();

    opRules.put(new RuleRegExp("R1", TableScanOperator.getOperatorName() + "%"),
        new TSMarker());
    opRules.put(new RuleRegExp("R2", LimitOperator.getOperatorName() + "%"),
        new Limit0Processor());
    disp = new NullScanTaskDispatcher(pctx, opRules);
    ogw = new DefaultGraphWalker(disp);
    topNodes = new ArrayList<>(pctx.getRootTasks());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  //We need to make sure that Null Operator (LIM or FIL) is present in all branches of multi-insert query before
  //applying the optimization. This method does full tree traversal starting from TS and will return true only if
  //it finds target Null operator on each branch.
  private static boolean isNullOpPresentInAllBranches(TableScanOperator ts, Node causeOfNullNode) {
    Queue<Node> middleNodes = new ArrayDeque<>();
    middleNodes.add(ts);
    while (!middleNodes.isEmpty()) {
      Node curNode = middleNodes.remove();
      List<? extends Node> curChd = curNode.getChildren();
      for (Node chd: curChd) {
        List<? extends Node> children = chd.getChildren();
        if (CollectionUtils.isEmpty(children) || chd == causeOfNullNode) {
         // If there is an end node that not the limit0/wherefalse..
          if (chd != causeOfNullNode) {
            return false;
          }
        } else {
          middleNodes.add(chd);
        }
      }
    }
    return true;
  }

  private static class WhereFalseProcessor implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      FilterOperator filter = (FilterOperator) nd;
      ExprNodeDesc condition = filter.getConf().getPredicate();
      if (!(condition instanceof ExprNodeConstantDesc)) {
        return null;
      }
      ExprNodeConstantDesc c = (ExprNodeConstantDesc) condition;
      if (!Boolean.FALSE.equals(c.getValue())) {
        return null;
      }

      WalkerCtx ctx = (WalkerCtx) procCtx;
      for (Node op : stack) {
        if (op instanceof TableScanOperator) {
          if (isNullOpPresentInAllBranches((TableScanOperator)op, filter)) {
            ctx.setMayBeMetadataOnly((TableScanOperator)op);
            LOG.debug("Found where false TableScan. {}", op);
          }
        }
      }
      ctx.convertMetadataOnly();
      return null;
    }
  }

  private static class Limit0Processor implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      LimitOperator limitOp = (LimitOperator)nd;
      if (!(limitOp.getConf().getLimit() == 0)) {
        return null;
      }

      Set<TableScanOperator> tsOps =
          ((WalkerCtx) procCtx).getMayBeMetadataOnlyTableScans();
      if (tsOps != null) {
        for (Iterator<TableScanOperator> tsOp = tsOps.iterator(); tsOp.hasNext();) {
          if (!isNullOpPresentInAllBranches(tsOp.next(), limitOp)) {
            tsOp.remove();
          }
        }
      }
      LOG.debug("Found Limit 0 TableScan. {}", nd);
      ((WalkerCtx)procCtx).convertMetadataOnly();
      return null;
    }
  }

  private static class TSMarker implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ((WalkerCtx)procCtx).setMayBeMetadataOnly((TableScanOperator)nd);
      return null;
    }
  }
}
