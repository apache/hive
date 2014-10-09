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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewForwardOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewJoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.PTFOperator;
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
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * Implementation of one of the rule-based optimization steps. ColumnPruner gets
 * the current operator tree. The \ tree is traversed to find out the columns
 * used for all the base tables. If all the columns for a table are not used, a
 * select is pushed on top of that table (to select only those columns). Since
 * this changes the row resolver, the tree is built again. This can be optimized
 * later to patch the tree.
 */
public class ColumnPruner implements Transform {
  protected ParseContext pGraphContext;
  private HashMap<Operator<? extends OperatorDesc>, OpParseContext> opToParseCtxMap;

  /**
   * empty constructor.
   */
  public ColumnPruner() {
    pGraphContext = null;
  }

  /**
   * Transform the query tree. For each table under consideration, check if all
   * columns are needed. If not, only select the operators needed at the
   * beginning and proceed.
   *
   * @param pactx
   *          the current parse context
   */
  @Override
  public ParseContext transform(ParseContext pactx) throws SemanticException {
    pGraphContext = pactx;
    opToParseCtxMap = pGraphContext.getOpParseCtx();

    // generate pruned column list for all relevant operators
    ColumnPrunerProcCtx cppCtx = new ColumnPrunerProcCtx(pactx);

    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack. The dispatcher
    // generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1",
      FilterOperator.getOperatorName() + "%"),
      ColumnPrunerProcFactory.getFilterProc());
    opRules.put(new RuleRegExp("R2",
      GroupByOperator.getOperatorName() + "%"),
      ColumnPrunerProcFactory.getGroupByProc());
    opRules.put(new RuleRegExp("R3",
      ReduceSinkOperator.getOperatorName() + "%"),
      ColumnPrunerProcFactory.getReduceSinkProc());
    opRules.put(new RuleRegExp("R4",
      SelectOperator.getOperatorName() + "%"),
      ColumnPrunerProcFactory.getSelectProc());
    opRules.put(new RuleRegExp("R5",
      CommonJoinOperator.getOperatorName() + "%"),
      ColumnPrunerProcFactory.getJoinProc());
    opRules.put(new RuleRegExp("R6",
      MapJoinOperator.getOperatorName() + "%"),
      ColumnPrunerProcFactory.getMapJoinProc());
    opRules.put(new RuleRegExp("R7",
      TableScanOperator.getOperatorName() + "%"),
      ColumnPrunerProcFactory.getTableScanProc());
    opRules.put(new RuleRegExp("R8",
      LateralViewJoinOperator.getOperatorName() + "%"),
      ColumnPrunerProcFactory.getLateralViewJoinProc());
    opRules.put(new RuleRegExp("R9",
      LateralViewForwardOperator.getOperatorName() + "%"),
      ColumnPrunerProcFactory.getLateralViewForwardProc());
    opRules.put(new RuleRegExp("R10",
        PTFOperator.getOperatorName() + "%"),
        ColumnPrunerProcFactory.getPTFProc());
    opRules.put(new RuleRegExp("R11",
        ScriptOperator.getOperatorName() + "%"),
        ColumnPrunerProcFactory.getScriptProc());
    opRules.put(new RuleRegExp("R12",
        LimitOperator.getOperatorName() + "%"),
        ColumnPrunerProcFactory.getLimitProc());
    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(ColumnPrunerProcFactory
        .getDefaultProc(), opRules, cppCtx);
    GraphWalker ogw = new ColumnPrunerWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pGraphContext.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return pGraphContext;
  }

  /**
   * Walks the op tree in post order fashion (skips selects with file sink or
   * script op children).
   */
  public static class ColumnPrunerWalker extends DefaultGraphWalker {

    public ColumnPrunerWalker(Dispatcher disp) {
      super(disp);
    }

    /**
     * Walk the given operator.
     */
    @Override
    public void walk(Node nd) throws SemanticException {
      boolean walkChildren = true;
      opStack.push(nd);

      // no need to go further down for a select op with a file sink or script
      // child
      // since all cols are needed for these ops
      if (nd instanceof SelectOperator) {
        for (Node child : nd.getChildren()) {
          if ((child instanceof FileSinkOperator)
              || (child instanceof ScriptOperator)) {
            walkChildren = false;
          }
        }
      }

      if ((nd.getChildren() == null)
          || getDispatchedList().containsAll(nd.getChildren()) || !walkChildren) {
        // all children are done or no need to walk the children
        dispatch(nd, opStack);
        opStack.pop();
        return;
      }
      // move all the children to the front of queue
      getToWalk().removeAll(nd.getChildren());
      getToWalk().addAll(0, nd.getChildren());
      // add self to the end of the queue
      getToWalk().add(nd);
      opStack.pop();
    }
  }
}
