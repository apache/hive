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

import java.io.Serializable;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMapRedCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * Processor for the rule - table scan followed by reduce sink.
 */
public class GenMRRedSink1 implements NodeProcessor {

  public GenMRRedSink1() {
  }

  /**
   * Reduce Sink encountered.
   * a) If we are seeing this RS for first time, we initialize plan corresponding to this RS.
   * b) If we are seeing this RS for second or later time then either query had a join in which
   *    case we will merge this plan with earlier plan involving this RS or plan for this RS
   *    needs to be split in two branches.
   *
   * @param nd
   *          the reduce sink operator encountered
   * @param opProcCtx
   *          context
   */
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx opProcCtx,
      Object... nodeOutputs) throws SemanticException {
    ReduceSinkOperator op = (ReduceSinkOperator) nd;
    GenMRProcContext ctx = (GenMRProcContext) opProcCtx;

    Map<Operator<? extends OperatorDesc>, GenMapRedCtx> mapCurrCtx = ctx
        .getMapCurrCtx();
    GenMapRedCtx mapredCtx = mapCurrCtx.get(stack.get(stack.size() - 2));
    Task<? extends Serializable> currTask = mapredCtx.getCurrTask();
    MapredWork currPlan = (MapredWork) currTask.getWork();
    String currAliasId = mapredCtx.getCurrAliasId();

    if (op.getNumChild() != 1) {
      throw new IllegalStateException("Expecting operator " + op + " to have one child. " +
          "But found multiple children : " + op.getChildOperators());
    }
    Operator<? extends OperatorDesc> reducer = op.getChildOperators().get(0);
    Task<? extends Serializable> oldTask = ctx.getOpTaskMap().get(reducer);

    ctx.setCurrAliasId(currAliasId);
    ctx.setCurrTask(currTask);

    // If the plan for this reducer does not exist, initialize the plan
    if (oldTask == null) {
      if (currPlan.getReduceWork() == null) {
        GenMapRedUtils.initPlan(op, ctx);
      } else {
        GenMapRedUtils.splitPlan(op, ctx);
      }
    } else {
      // This will happen in case of joins. The current plan can be thrown away
      // after being merged with the original plan
      GenMapRedUtils.joinPlan(currTask, oldTask, ctx);
      currTask = oldTask;
      ctx.setCurrTask(currTask);
    }

    mapCurrCtx.put(op, new GenMapRedCtx(ctx.getCurrTask(), ctx.getCurrAliasId()));

    if (GenMapRedUtils.hasBranchFinished(nodeOutputs)) {
      ctx.addRootIfPossible(currTask);
      return false;
    }

    return true;
  }
}
