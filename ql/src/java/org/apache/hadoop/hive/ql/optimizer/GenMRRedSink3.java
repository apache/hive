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

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Utils;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMapRedCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * Processor for the rule - union followed by reduce sink.
 */
public class GenMRRedSink3 implements SemanticNodeProcessor {

  public GenMRRedSink3() {
  }

  /**
   * Reduce Scan encountered.
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

    // union consisted on a bunch of map-reduce jobs, and it has been split at
    // the union
    Operator<? extends OperatorDesc> reducer = op.getChildOperators().get(0);
    UnionOperator union = Utils.findNode(stack, UnionOperator.class);
    assert union != null;

    Map<Operator<? extends OperatorDesc>, GenMapRedCtx> mapCurrCtx = ctx
        .getMapCurrCtx();
    GenMapRedCtx mapredCtx = mapCurrCtx.get(union);

    Task<?> unionTask = null;
    if(mapredCtx != null) {
      unionTask = mapredCtx.getCurrTask();
    } else {
      unionTask = ctx.getCurrTask();
    }


    MapredWork plan = (MapredWork) unionTask.getWork();
    HashMap<Operator<? extends OperatorDesc>, Task<?>> opTaskMap = ctx
        .getOpTaskMap();
    Task<?> reducerTask = opTaskMap.get(reducer);

    ctx.setCurrTask(unionTask);

    // If the plan for this reducer does not exist, initialize the plan
    if (reducerTask == null) {
      // When the reducer is encountered for the first time
      if (plan.getReduceWork() == null) {
        GenMapRedUtils.initUnionPlan(op, union, ctx, unionTask);
        // When union is followed by a multi-table insert
      } else {
        GenMapRedUtils.splitPlan(op, ctx);
      }
    } else if (plan.getReduceWork() != null && plan.getReduceWork().getReducer() == reducer) {
      // The union is already initialized. However, the union is walked from
      // another input
      // initUnionPlan is idempotent
      GenMapRedUtils.initUnionPlan(op, union, ctx, unionTask);
    } else {
      GenMapRedUtils.joinUnionPlan(ctx, union, unionTask, reducerTask, false);
      ctx.setCurrTask(reducerTask);
    }

    mapCurrCtx.put(op, new GenMapRedCtx(ctx.getCurrTask(),
        ctx.getCurrAliasId()));

    // the union operator has been processed
    ctx.setCurrUnionOp(null);
    return true;
  }
}
