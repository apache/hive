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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
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

  private static final Log LOG = LogFactory.getLog(NullScanOptimizer.class.getName());
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
          ctx.setMayBeMetadataOnly((TableScanOperator)op);
          LOG.info("Found where false TableScan. " + op);
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

      if(!(((LimitOperator)nd).getConf().getLimit() == 0)) {
        return null;
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
