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

import java.io.Serializable;
import java.util.HashMap;
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

/**
 * Processor for the rule - map join followed by reduce sink
 */
public class GenMRRedSink4 implements NodeProcessor {

  public GenMRRedSink4() {
  }

  /**
   * Reduce Scan encountered
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

    ctx.getParseCtx();

    // map-join consisted on a bunch of map-only jobs, and it has been split
    // after the mapjoin
    Operator<? extends Serializable> reducer = op.getChildOperators().get(0);
    Map<Operator<? extends Serializable>, GenMapRedCtx> mapCurrCtx = ctx
        .getMapCurrCtx();
    GenMapRedCtx mapredCtx = mapCurrCtx.get(op.getParentOperators().get(0));
    Task<? extends Serializable> currTask = mapredCtx.getCurrTask();
    MapredWork plan = (MapredWork) currTask.getWork();
    HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> opTaskMap = ctx
        .getOpTaskMap();
    Task<? extends Serializable> opMapTask = opTaskMap.get(reducer);

    ctx.setCurrTask(currTask);

    // If the plan for this reducer does not exist, initialize the plan
    if (opMapTask == null) {
      // When the reducer is encountered for the first time
      if (plan.getReducer() == null) {
        GenMapRedUtils.initMapJoinPlan(op, ctx, true, false, true, -1);
        // When mapjoin is followed by a multi-table insert
      } else {
        GenMapRedUtils.splitPlan(op, ctx);
      }
    }
    // There is a join after mapjoin. One of the branches of mapjoin has already
    // been initialized.
    // Initialize the current branch, and join with the original plan.
    else {
      assert plan.getReducer() != reducer;
      GenMapRedUtils.joinPlan(op, currTask, opMapTask, ctx, -1, false, true,
          false);
    }

    mapCurrCtx.put(op, new GenMapRedCtx(ctx.getCurrTask(), ctx.getCurrTopOp(),
        ctx.getCurrAliasId()));

    // the mapjoin operator has been processed
    ctx.setCurrMapJoinOp(null);
    return null;
  }
}
