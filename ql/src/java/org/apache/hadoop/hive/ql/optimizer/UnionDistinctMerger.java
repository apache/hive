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

package org.apache.hadoop.hive.ql.optimizer;

import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

public class UnionDistinctMerger extends Transform {
  private static final Logger LOG = LoggerFactory.getLogger(UnionDistinctMerger.class);

  private static final String PATTERN_STRING = new StringBuilder()
      .append(UnionOperator.getOperatorName()).append("%")
      .append(GroupByOperator.getOperatorName()).append("%")
      .append(ReduceSinkOperator.getOperatorName()).append("%")
      .append(GroupByOperator.getOperatorName()).append("%")
      .append(UnionOperator.getOperatorName()).append("%")
      .append(GroupByOperator.getOperatorName()).append("%")
      .append(ReduceSinkOperator.getOperatorName()).append("%")
      .append(GroupByOperator.getOperatorName()).append("%")
      .toString();

  private static class UnionMergeContext implements NodeProcessorCtx {
    public final ParseContext pCtx;

    public UnionMergeContext(ParseContext pCtx) {
      this.pCtx = pCtx;
    }
  }

  private class UnionMergeProcessor implements SemanticNodeProcessor {
    @Override
    public Void process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      UnionMergeContext context = (UnionMergeContext) procCtx;

      // The stack contains at least 8 operators, UNION-GBY-RS-GBY-UNION-GBY-RS-GBY.
      // The leftmost UNION is on stack.size() - 8 and the rightmost GBY is on stack.size() - 1.
      Set<Operator> allOps = new HashSet<>(context.pCtx.getAllOps());
      for (int i = 1; i <= 8; i ++) {
        Operator<?> op = (Operator<?>) stack.get(stack.size() - i);

        // We do not apply the optimization if some operators do not belong to query plan.
        // This can be happened when we already merged some UNIONs before.
        // For example, Suppose that a query plan looks like the below graph:
        //   (1)UNION-GBY-RS-GBY-(3)UNION-GBY-RS-GBY
        //   (2)UNION-GBY-RS-GBY-
        // Then we merge (1)-(3) to (1) and them move on to merging (2)-(3). Without checking the presence of
        // operators in (2) and (3), the merge process will fail as (3) is already removed.
        if (!allOps.contains(op)) {
          return null;
        }

        // We do not apply the optimization if intermediate outputs are used by other operators.
        if (i != 1 && op.getChildOperators().size() > 1) {
          return null;
        }
      }

      UnionOperator upperUnionOperator = (UnionOperator) stack.get(stack.size() - 8);
      GroupByOperator upperFinalGroupByOperator = (GroupByOperator) stack.get(stack.size() - 5);

      UnionOperator lowerUnionOperator = (UnionOperator) stack.get(stack.size() - 4);
      GroupByOperator lowerFinalGroupByOperator = (GroupByOperator) stack.get(stack.size() - 1);

      // We can apply the optimization if there is no aggregators in final GroupBy operators. The absence of
      // aggregators ensures that we are merging two distinct computation.
      if (upperFinalGroupByOperator.getConf().getAggregators().isEmpty() &&
          lowerFinalGroupByOperator.getConf().getAggregators().isEmpty()) {
        LOG.info("Detect duplicate UNION-DISTINCT GBY patterns. Remove the latter one.");

        // Step 0. UNION1->GBY1->RS1->GBY2->UNION2->GBY3->RS2->GBY4

        // Step 1. Cut GBY2->UNION2
        lowerUnionOperator.removeParent(upperFinalGroupByOperator);

        // Step 2.
        //   Connect the parent of lowerUnionOperator and upperUnionOperator.
        //   Disconnect lowerUnionOperator from operator graph.
        // Before step 2:
        //    {OP1, 2}-UNION1->GBY1->RS1->GBY2-{}
        //    {OP3, 4}-UNION2->GBY3->RS2->GBY4-{OP5, 6, ...}
        // After step 2:
        //    {OP1, 2, 3, 4}-UNION1->GBY1->RS1->GBY2-{}
        //                {}-UNION2->GBY3->RS2->GBY4-{OP5, 6, ...}
        for (Operator<?> lowerUnionParent: lowerUnionOperator.getParentOperators()) {
          lowerUnionParent.replaceChild(lowerUnionOperator, upperUnionOperator);
          upperUnionOperator.getParentOperators().add(lowerUnionParent);
        }
        lowerUnionOperator.setParentOperators(new ArrayList<>());

        // Step 3.
        //   Connect upperFinalGroupByOperator and the children of lowerFinalGroupByOperator.
        //   Disconnect lowerFinalGroupByOperator from operator graph.
        // Before step 3:
        //    {OP1, 2, ...}-UNION1->GBY1->RS1->GBY2-{}
        //               {}-UNION2->GBY3->RS2->GBY4-{OP5, 6, ...}
        // After step 3:
        //    {OP1, 2, ...}-UNION1->GBY1->RS1->GBY2-{OP5, 6, ...}
        //               {}-UNION2->GBY3->RS2->GBY4-{}
        for (Operator<?> lowerFinalGroupByChild: lowerFinalGroupByOperator.getChildOperators()) {
          lowerFinalGroupByChild.replaceParent(lowerFinalGroupByOperator, upperFinalGroupByOperator);
          upperFinalGroupByOperator.getChildOperators().add(lowerFinalGroupByChild);
        }
        upperUnionOperator.getConf().setNumInputs(upperUnionOperator.getNumParent());
      }

      return null;
    }
  }

  private static class NoSkipGraphWalker extends DefaultGraphWalker {
    public NoSkipGraphWalker(SemanticDispatcher disp) {
      super(disp);
    }

    @Override
    public void startWalking(Collection<Node> startNodes,
        HashMap<Node, Object> nodeOutput) throws SemanticException {
      toWalk.addAll(startNodes);
      while (!toWalk.isEmpty()) {
        Node nd = toWalk.remove(0);
        walk(nd);
        // We need to revisit GroupBy operator for every distinct operator path.
        // GraphWalker uses retMap to determine if an operator has been visited.
        // Clearing it after each walk() ensures that we visit GroupBy operator in every possible path.
        retMap.clear();
      }
    }
  }

  public ParseContext transform(ParseContext pCtx) throws SemanticException {
    Map<SemanticRule, SemanticNodeProcessor> testRules = new LinkedHashMap<>();
    testRules.put(new RuleRegExp("AdjacentDistinctUnion", PATTERN_STRING), new UnionMergeProcessor());
    SemanticDispatcher disp = new DefaultRuleDispatcher(null, testRules, new UnionMergeContext(pCtx));
    SemanticGraphWalker ogw = new NoSkipGraphWalker(disp);

    List<Node> topNodes = new ArrayList<>();
    topNodes.addAll(pCtx.getTopOps().values());
    ogw.startWalking(topNodes, null);

    return pCtx;
  }
}

