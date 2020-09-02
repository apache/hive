/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hadoop.hive.ql.optimizer;

import static org.apache.hadoop.hive.ql.optimizer.topnkey.TopNKeyProcessor.copyDown;
import static org.apache.hadoop.hive.ql.optimizer.topnkey.TopNKeyPushdownProcessor.moveDown;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Push LIMIT without an Order By through Selects and Left Outer Joins
 */
public class OrderlessLimitPushDownOptimizer extends Transform {
  private static final Logger LOG = LoggerFactory.getLogger(OrderlessLimitPushDownOptimizer.class);

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    opRules.put(
            new RuleRegExp("LIMIT push down", LimitOperator.getOperatorName() + "%"),
            new LimitPushDown());
    SemanticGraphWalker walker = new DefaultGraphWalker(new DefaultRuleDispatcher(null, opRules, null));
    walker.startWalking(new ArrayList<>(pctx.getTopOps().values()), null);
    return pctx;
  }

  private static class LimitPushDown implements SemanticNodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
      ReduceSinkOperator reduceSink = findReduceSink(stack);
      if (reduceSink == null || !reduceSink.getConf().hasOrderBy()) { // LIMIT + ORDER BY handled by TopNKey push down
        pushDown((LimitOperator) nd);
      }
      return null;
    }

    private ReduceSinkOperator findReduceSink(Stack<Node> stack) {
      for (int i = stack.size() - 2 ; i >= 0; i--) {
        Operator<?> operator = (Operator<?>) stack.get(i);
        if (operator instanceof ReduceSinkOperator) {
          return ((ReduceSinkOperator) operator);
        }
      }
      return null;
    }

    private void pushDown(LimitOperator limit) throws SemanticException {
      Operator<? extends OperatorDesc> parent = limit.getParentOperators().get(0);
      if (parent.getNumChild() != 1) {
        return;
      }
      switch (parent.getType()) {
        case LIMIT:
          combineLimits(limit);
          break;
        case SELECT:
        case FORWARD:
          pushdownThroughParent(limit);
          break;
        case MERGEJOIN:
        case JOIN:
        case MAPJOIN:
          pushThroughLeftOuterJoin(limit);
          break;
        default:
          break;
      }
    }

    private void combineLimits(LimitOperator childLimit) throws SemanticException {
      LimitOperator parentLimit = (LimitOperator) childLimit.getParentOperators().get(0);
      LimitDesc parentConf = parentLimit.getConf();
      LimitDesc childConf = childLimit.getConf();
      if (parentConf.getOffset() == childConf.getOffset()) {
        int min = Math.min(parentConf.getLimit(), childConf.getLimit());
        LOG.debug("Combining two limits child={}, parent={}, newLimit={}", childLimit, parentLimit, min);
        parentConf.setLimit(min);
        parentLimit.removeChildAndAdoptItsChildren(childLimit);
        pushDown(parentLimit);
      }
    }

    private void pushdownThroughParent(LimitOperator limit) throws SemanticException {
      Operator<? extends OperatorDesc> parent = limit.getParentOperators().get(0);
      LOG.debug("Pushing {} through {}", limit.getName(), parent.getName());
      moveDown(limit);
      pushDown(limit);
    }

    private void pushThroughLeftOuterJoin(LimitOperator limit)
            throws SemanticException {
      CommonJoinOperator<? extends JoinDesc> join =
              (CommonJoinOperator<? extends JoinDesc>) limit.getParentOperators().get(0);
      JoinCondDesc[] joinConds = join.getConf().getConds();
      JoinCondDesc firstJoinCond = joinConds[0];
      for (JoinCondDesc joinCond : joinConds) {
        if (!firstJoinCond.equals(joinCond)) {
          return;
        }
      }
      if (firstJoinCond.getType() == JoinDesc.LEFT_OUTER_JOIN) {
        List<Operator<? extends OperatorDesc>> joinInputs = join.getParentOperators();
        final ReduceSinkOperator reduceSinkOperator = (ReduceSinkOperator) joinInputs.get(0);

        pushDown((LimitOperator) copyDown(reduceSinkOperator, new LimitDesc(limit.getConf())));
        // the copied limit will take care of the offset, need to reset the offset in the original to not to lose rows
        limit.getConf().setOffset(0);
      }
    }
  }
}
