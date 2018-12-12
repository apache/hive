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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.LimitDesc;

/**
 * Make RS calculate top-K selection for limit clause.
 * It's only works with RS for limit operation which means between RS and LITMIT,
 * there should not be other operators which may change number of rows like FilterOperator.
 * see {@link Operator#acceptLimitPushdown}
 *
 * If RS is only for limiting rows, RSHash counts row with same key separately.
 * But if RS is for GBY, RSHash should forward all the rows with the same key.
 *
 * Legend : A(a) --> key A, value a, row A(a)
 *
 * If each RS in mapper tasks is forwarded rows like this
 *
 * MAP1(RS) : 40(a)-10(b)-30(c)-10(d)-70(e)-80(f)
 * MAP2(RS) : 90(g)-80(h)-60(i)-40(j)-30(k)-20(l)
 * MAP3(RS) : 40(m)-50(n)-30(o)-30(p)-60(q)-70(r)
 *
 * OBY or GBY makes result like this,
 *
 * REDUCER : 10(b,d)-20(l)-30(c,k,o,p)-40(a,j,m)-50(n)-60(i,q)-70(e,r)-80(f,h)-90(g)
 * LIMIT 3 for GBY: 10(b,d)-20(l)-30(c,k,o,p)
 * LIMIT 3 for OBY: 10(b,d)-20(l)
 *
 * with the optimization, the amount of shuffling can be reduced, making identical result
 *
 * For GBY,
 *
 * MAP1 : 40(a)-10(b)-30(c)-10(d)
 * MAP2 : 40(j)-30(k)-20(l)
 * MAP3 : 40(m)-50(n)-30(o)-30(p)
 *
 * REDUCER : 10(b,d)-20(l)-30(c,k,o,p)-40(a,j,m)-50(n)
 * LIMIT 3 : 10(b,d)-20(l)-30(c,k,o,p)
 *
 * For OBY,
 *
 * MAP1 : 10(b)-30(c)-10(d)
 * MAP2 : 40(j)-30(k)-20(l)
 * MAP3 : 40(m)-50(n)-30(o)
 *
 * REDUCER : 10(b,d)-20(l)-30(c,k,o)-40(j,m)-50(n)
 * LIMIT 3 : 10(b,d)-20(l)
 */
public class LimitPushdownOptimizer extends Transform {

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1",
        ReduceSinkOperator.getOperatorName() + "%" +
        ".*" +
        LimitOperator.getOperatorName() + "%"),
        new TopNReducer());
    opRules.put(new RuleRegExp("R2",
        ReduceSinkOperator.getOperatorName() + "%" +
        ".*" +
        ReduceSinkOperator.getOperatorName() + "%"),
        new TopNPropagator());

    LimitPushdownContext context = new LimitPushdownContext(pctx.getConf());
    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, context);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    List<Node> topNodes = new ArrayList<Node>(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  private static class TopNReducer implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack,
        NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
      ReduceSinkOperator rs = null;
      for (int i = stack.size() - 2 ; i >= 0; i--) {
        Operator<?> operator = (Operator<?>) stack.get(i);
        if (operator.getNumChild() != 1) {
          return false; // multi-GBY single-RS (TODO)
        }
        if (operator instanceof ReduceSinkOperator) {
          rs = (ReduceSinkOperator) operator;
          break;
        }
        if (!operator.acceptLimitPushdown()) {
          return false;
        }
      }
      if (rs != null) {
        Operator<?> currentOp = rs;
        boolean foundGroupByOperator = false;
        while (currentOp != nd) { // nd = limitOp
          if (currentOp instanceof GroupByOperator) {
            if (foundGroupByOperator) {
              // Not safe to continue for RS-GBY-GBY-LIM kind of pipelines. See HIVE-10607 for more.
              return false;
            }
            foundGroupByOperator = true;
          }
          currentOp = currentOp.getChildOperators().get(0);
        }
        LimitOperator limit = (LimitOperator) nd;
        LimitDesc limitDesc = limit.getConf();
        Integer offset = limitDesc.getOffset();
        rs.getConf().setTopN(limitDesc.getLimit() + ((offset == null) ? 0 : offset));
        rs.getConf().setTopNMemoryUsage(((LimitPushdownContext) procCtx).threshold);
        if (rs.getNumChild() == 1 && rs.getChildren().get(0) instanceof GroupByOperator) {
          rs.getConf().setMapGroupBy(true);
        }
      }
      return true;
    }
  }

  private static class TopNPropagator implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack,
        NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
      ReduceSinkOperator cRS = (ReduceSinkOperator) nd;
      if (cRS.getConf().getTopN() == -1) {
        // No limit, nothing to propagate, we just bail out
        return false;
      }
      ReduceSinkOperator pRS = null;
      for (int i = stack.size() - 2 ; i >= 0; i--) {
        Operator<?> operator = (Operator<?>) stack.get(i);
        if (operator.getNumChild() != 1) {
          return false; // multi-GBY single-RS (TODO)
        }
        if (operator instanceof ReduceSinkOperator) {
          pRS = (ReduceSinkOperator) operator;
          break;
        }
        if (!operator.acceptLimitPushdown()) {
          return false;
        }
      }
      if (pRS != null) {
        Operator<?> currentOp = pRS;
        boolean foundGroupByOperator = false;
        while (currentOp != nd) { // nd = cRS
          if (currentOp instanceof GroupByOperator) {
            if (foundGroupByOperator) {
              // Not safe to continue for RS-GBY-GBY-LIM kind of pipelines. See HIVE-10607 for more.
              return false;
            }
            foundGroupByOperator = true;
          }
          currentOp = currentOp.getChildOperators().get(0);
        }
        List<ExprNodeDesc> cKeys = cRS.getConf().getKeyCols();
        List<ExprNodeDesc> pKeys = pRS.getConf().getKeyCols();
        if (pRS.getChildren().get(0) instanceof GroupByOperator &&
                pRS.getChildren().get(0).getChildren().get(0) == cRS) {
          // RS-GB-RS
          GroupByOperator gBy = (GroupByOperator) pRS.getChildren().get(0);
          List<ExprNodeDesc> gKeys = gBy.getConf().getKeys();
          if (!ExprNodeDescUtils.checkPrefixKeysUpstream(cKeys, pKeys, cRS, pRS)) {
            // We might still be able to push the limit
            if (!ExprNodeDescUtils.checkPrefixKeys(cKeys, gKeys, cRS, gBy) ||
                    !ExprNodeDescUtils.checkPrefixKeys(gKeys, pKeys, gBy, pRS)) {
              // We cannot push limit; bail out
              return false;
            }
          }
        } else {
          if (!ExprNodeDescUtils.checkPrefixKeysUpstream(cKeys, pKeys, cRS, pRS)) {
            // We cannot push limit; bail out
            return false;
          }
        }
        // Copy order
        StringBuilder order;
        StringBuilder orderNull;
        if (pRS.getConf().getOrder().length() > cRS.getConf().getOrder().length()) {
          order = new StringBuilder(cRS.getConf().getOrder());
          orderNull = new StringBuilder(cRS.getConf().getNullOrder());
          order.append(pRS.getConf().getOrder().substring(order.length()));
          orderNull.append(pRS.getConf().getNullOrder().substring(orderNull.length()));
        } else {
          order = new StringBuilder(cRS.getConf().getOrder().substring(
                  0, pRS.getConf().getOrder().length()));
          orderNull = new StringBuilder(cRS.getConf().getNullOrder().substring(
                  0, pRS.getConf().getNullOrder().length()));
        }
        pRS.getConf().setOrder(order.toString());
        pRS.getConf().setNullOrder(orderNull.toString());
        // Copy limit
        pRS.getConf().setTopN(cRS.getConf().getTopN());
        pRS.getConf().setTopNMemoryUsage(cRS.getConf().getTopNMemoryUsage());
        if (pRS.getNumChild() == 1 && pRS.getChildren().get(0) instanceof GroupByOperator) {
          pRS.getConf().setMapGroupBy(true);
        }
      }
      return true;
    }
  }

  private static class LimitPushdownContext implements NodeProcessorCtx {

    private final float threshold;

    public LimitPushdownContext(HiveConf conf) throws SemanticException {
      threshold = conf.getFloatVar(HiveConf.ConfVars.HIVELIMITPUSHDOWNMEMORYUSAGE);
      if (threshold <= 0 || threshold >= 1) {
        throw new SemanticException("Invalid memory usage value " + threshold +
            " for " + HiveConf.ConfVars.HIVELIMITPUSHDOWNMEMORYUSAGE);
      }
    }
  }
}
