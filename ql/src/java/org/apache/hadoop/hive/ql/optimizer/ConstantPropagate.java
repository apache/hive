/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.ConstantPropagateProcCtx.ConstantPropagateOption;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Implementation of one of the rule-based optimization steps. ConstantPropagate traverse the DAG
 * from root to child. For each conditional expression, process as follows:
 *
 * 1. Fold constant expression: if the expression is a UDF and all parameters are constant.
 *
 * 2. Shortcut expression: if the expression is a logical operator and it can be shortcut by
 * some constants of its parameters.
 *
 * 3. Propagate expression: if the expression is an assignment like column=constant, the expression
 * will be propagate to parents to see if further folding operation is possible.
 */
public class ConstantPropagate implements Transform {

  private static final Log LOG = LogFactory.getLog(ConstantPropagate.class);
  protected ParseContext pGraphContext;
  private ConstantPropagateOption constantPropagateOption;

  public ConstantPropagate() {
    this(ConstantPropagateOption.FULL);
  }

  public ConstantPropagate(ConstantPropagateOption option) {
    this.constantPropagateOption = option;
  }

  /**
   * Transform the query tree.
   *
   * @param pactx
   *        the current parse context
   */
  @Override
  public ParseContext transform(ParseContext pactx) throws SemanticException {
    pGraphContext = pactx;

    // generate pruned column list for all relevant operators
    ConstantPropagateProcCtx cppCtx = new ConstantPropagateProcCtx(constantPropagateOption);

    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack. The dispatcher
    // generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    opRules.put(new RuleRegExp("R1", FilterOperator.getOperatorName() + "%"),
        ConstantPropagateProcFactory.getFilterProc());
    opRules.put(new RuleRegExp("R2", GroupByOperator.getOperatorName() + "%"),
        ConstantPropagateProcFactory.getGroupByProc());
    opRules.put(new RuleRegExp("R3", SelectOperator.getOperatorName() + "%"),
        ConstantPropagateProcFactory.getSelectProc());
    opRules.put(new RuleRegExp("R4", FileSinkOperator.getOperatorName() + "%"),
        ConstantPropagateProcFactory.getFileSinkProc());
    opRules.put(new RuleRegExp("R5", ReduceSinkOperator.getOperatorName() + "%"),
        ConstantPropagateProcFactory.getReduceSinkProc());
    opRules.put(new RuleRegExp("R6", JoinOperator.getOperatorName() + "%"),
        ConstantPropagateProcFactory.getJoinProc());
    opRules.put(new RuleRegExp("R7", TableScanOperator.getOperatorName() + "%"),
        ConstantPropagateProcFactory.getTableScanProc());
    opRules.put(new RuleRegExp("R8", ScriptOperator.getOperatorName() + "%"),
        ConstantPropagateProcFactory.getStopProc());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(ConstantPropagateProcFactory
        .getDefaultProc(), opRules, cppCtx);
    GraphWalker ogw = new ConstantPropagateWalker(disp);

    // Create a list of operator nodes to start the walking.
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pGraphContext.getTopOps().values());
    ogw.startWalking(topNodes, null);
    for (Operator<? extends Serializable> opToDelete : cppCtx.getOpToDelete()) {
      if (opToDelete.getParentOperators() == null || opToDelete.getParentOperators().size() != 1) {
        throw new RuntimeException("Error pruning operator " + opToDelete
            + ". It should have only 1 parent.");
      }
      opToDelete.getParentOperators().get(0).removeChildAndAdoptItsChildren(opToDelete);
    }
    cppCtx.getOpToDelete().clear();
    return pGraphContext;
  }


  /**
   * Walks the op tree in root first order.
   */
  public static class ConstantPropagateWalker extends DefaultGraphWalker {

    public ConstantPropagateWalker(Dispatcher disp) {
      super(disp);
    }

    @Override
    public void walk(Node nd) throws SemanticException {

      List<Node> parents = ((Operator) nd).getParentOperators();
      if ((parents == null)
          || getDispatchedList().containsAll(parents)) {
        opStack.push(nd);

        // all children are done or no need to walk the children
        dispatch(nd, opStack);
        opStack.pop();
      } else {
        getToWalk().removeAll(parents);
        getToWalk().add(0, nd);
        getToWalk().addAll(0, parents);
        return;
      }

      // move all the children to the front of queue
      List<? extends Node> children = nd.getChildren();
      if (children != null) {
        getToWalk().removeAll(children);
        getToWalk().addAll(children);
      }
    }
  }

}
