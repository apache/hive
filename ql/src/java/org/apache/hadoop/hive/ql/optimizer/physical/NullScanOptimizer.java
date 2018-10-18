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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.physical.MetadataOnlyOptimizer.WalkerCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

/**
 * This optimizer attempts following two optimizations:
 * 1. If it finds TS followed By FIL which has been determined at compile time to evaluate to
 *    zero, it removes all input paths for that table scan.
 * 2. If it finds TS followed by Limit 0, it removes all input paths from table scan.
 */
public class NullScanOptimizer implements PhysicalPlanResolver {

  private static final Logger LOG = LoggerFactory.getLogger(NullScanOptimizer.class.getName());
  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", TableScanOperator.getOperatorName() + "%.*" +
      FilterOperator.getOperatorName() + "%"), new WhereFalseProcessor());
    Dispatcher disp = new NullScanTaskDispatcher(pctx, opRules);
    GraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());
    ogw.startWalking(topNodes, null);

    opRules.clear();

    opRules.put(new RuleRegExp("R1", TableScanOperator.getOperatorName()+ "%"),new TSMarker());
    opRules.put(new RuleRegExp("R2", LimitOperator.getOperatorName()+ "%"), new Limit0Processor());
    disp = new NullScanTaskDispatcher(pctx, opRules);
    ogw = new DefaultGraphWalker(disp);
    topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  //We need to make sure that Null Operator (LIM or FIL) is present in all branches of multi-insert query before
  //applying the optimization. This method does full tree traversal starting from TS and will return true only if
  //it finds target Null operator on each branch.
  static private boolean isNullOpPresentInAllBranches(TableScanOperator ts, Node causeOfNullNode) {
    Node curNode = null;
    List<? extends Node> curChd = null;
    LinkedList<Node> middleNodes = new LinkedList<Node>();
    middleNodes.addLast(ts);
    while (!middleNodes.isEmpty()) {
      curNode = middleNodes.remove();
      curChd = curNode.getChildren();
      for (Node chd: curChd) {
        if (chd.getChildren() == null || chd.getChildren().isEmpty() || chd == causeOfNullNode) {
          if (chd != causeOfNullNode) { // If there is an end node that not the limit0/wherefalse..
            return false;
          }
        }
        else {
          middleNodes.addLast(chd);
        }
      }

    }
    return true;
  }

  static private class WhereFalseProcessor implements NodeProcessor {

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
            LOG.info("Found where false TableScan. " + op);
          }
        }
      }
      ctx.convertMetadataOnly();
      return null;
    }
  }

  static private class Limit0Processor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      LimitOperator limitOp = (LimitOperator)nd;
      if(!(limitOp.getConf().getLimit() == 0)) {
        return null;
      }

      HashSet<TableScanOperator> tsOps = ((WalkerCtx)procCtx).getMayBeMetadataOnlyTableScans();
      if (tsOps != null) {
        for (Iterator<TableScanOperator> tsOp = tsOps.iterator(); tsOp.hasNext();) {
          if (!isNullOpPresentInAllBranches(tsOp.next(),limitOp))
            tsOp.remove();
        }
      }
      LOG.info("Found Limit 0 TableScan. " + nd);
      ((WalkerCtx)procCtx).convertMetadataOnly();
      return null;
    }

  }

  static private class TSMarker implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ((WalkerCtx)procCtx).setMayBeMetadataOnly((TableScanOperator)nd);
      return null;
    }
  }
}
