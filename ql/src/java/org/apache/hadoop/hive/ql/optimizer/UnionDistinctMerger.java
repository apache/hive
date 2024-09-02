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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

public class UnionDistinctMerger extends Transform {
  private static final Logger LOG = LoggerFactory.getLogger(UnionDistinctMerger.class);

  private static class UnionMergeContext implements NodeProcessorCtx {
    public final ParseContext pCtx;

    public UnionMergeContext(ParseContext pCtx) {
      this.pCtx = pCtx;
    }
  }

  private class UnionMergeProcessor implements SemanticNodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      UnionMergeContext context = (UnionMergeContext) procCtx;
      Collection<Operator> allOps = context.pCtx.getAllOps();
      for (int i = 1; i <= 8; i ++) {
        if (!allOps.contains(stack.get(stack.size() - i))) {
          return null;
        }
      }

      UnionOperator upperUnionOperator = (UnionOperator) stack.get(stack.size() - 8);
      GroupByOperator upperMiddleGroupByOperator = (GroupByOperator) stack.get(stack.size() - 7);
      ReduceSinkOperator upperReduceSinkOperator = (ReduceSinkOperator) stack.get(stack.size() - 6);
      GroupByOperator upperFinalGroupByOperator = (GroupByOperator) stack.get(stack.size() - 5);

      UnionOperator lowerUnionOperator = (UnionOperator) stack.get(stack.size() - 4);
      GroupByOperator lowerFinalGroupByOperator = (GroupByOperator) stack.get(stack.size() - 1);

      if (upperFinalGroupByOperator.getConf().getAggregators().isEmpty() &&
          lowerFinalGroupByOperator.getConf().getAggregators().isEmpty() &&
          upperUnionOperator.getChildOperators().size() == 1 &&
          upperMiddleGroupByOperator.getChildOperators().size() == 1 &&
          upperReduceSinkOperator.getChildOperators().size() == 1 &&
          upperFinalGroupByOperator.getChildOperators().size() == 1) {
        LOG.info("Detect duplicate UNION-DISTINCT GBY patterns. Remove the later one.");

        lowerUnionOperator.removeParent(upperFinalGroupByOperator);
        for (Operator<?> lowerUnionParent: lowerUnionOperator.getParentOperators()) {
          lowerUnionParent.replaceChild(lowerUnionOperator, upperUnionOperator);
          upperUnionOperator.getParentOperators().add(lowerUnionParent);
        }
        lowerUnionOperator.setParentOperators(new ArrayList<>());

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

    public void startWalking(Collection<Node> startNodes,
        HashMap<Node, Object> nodeOutput) throws SemanticException {
      toWalk.addAll(startNodes);
      while (toWalk.size() > 0) {
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
    StringBuilder pattern = new StringBuilder();
    pattern.append(UnionOperator.getOperatorName()).append("%")
        .append(GroupByOperator.getOperatorName()).append("%")
        .append(ReduceSinkOperator.getOperatorName()).append("%")
        .append(GroupByOperator.getOperatorName()).append("%")
        .append(UnionOperator.getOperatorName()).append("%")
        .append(GroupByOperator.getOperatorName()).append("%")
        .append(ReduceSinkOperator.getOperatorName()).append("%")
        .append(GroupByOperator.getOperatorName()).append("%");

    testRules.put(new RuleRegExp("AdjacentDistinctUnion", pattern.toString()), new UnionMergeProcessor());

    SemanticDispatcher disp = new DefaultRuleDispatcher(null, testRules, new UnionMergeContext(pCtx));
    SemanticGraphWalker ogw = new NoSkipGraphWalker(disp);

    List<Node> topNodes = new ArrayList<>();
    topNodes.addAll(pCtx.getTopOps().values());
    ogw.startWalking(topNodes, null);

    return pCtx;
  }
}

