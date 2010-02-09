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
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.MapredWork;

/**
 * Processor for the rule - union followed by reduce sink.
 */
public class GenMRRedSink3 implements NodeProcessor {

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

    ParseContext parseCtx = ctx.getParseCtx();
    UnionProcContext uCtx = parseCtx.getUCtx();

    // union was map only - no special processing needed
    if (uCtx.isMapOnlySubq()) {
      return (new GenMRRedSink1()).process(nd, stack, opProcCtx, nodeOutputs);
    }

    // union consisted on a bunch of map-reduce jobs, and it has been split at
    // the union
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
        GenMapRedUtils.initUnionPlan(op, ctx);
        // When union is followed by a multi-table insert
      } else {
        GenMapRedUtils.splitPlan(op, ctx);
      }
    } else if (plan.getReducer() == reducer) {
      // The union is already initialized. However, the union is walked from
      // another input
      // initUnionPlan is idempotent
      GenMapRedUtils.initUnionPlan(op, ctx);
    } else {
      GenMapRedUtils.initUnionPlan(ctx, currTask, false);
      GenMapRedUtils.joinPlan(op, currTask, opMapTask, ctx, -1, true, false,
          false);
    }

    mapCurrCtx.put(op, new GenMapRedCtx(ctx.getCurrTask(), ctx.getCurrTopOp(),
        ctx.getCurrAliasId()));

    // the union operator has been processed
    ctx.setCurrUnionOp(null);
    return null;
  }
}
