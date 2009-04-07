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

import java.util.Map;
import java.util.HashMap;
import java.util.Stack;
import java.io.Serializable;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMapRedCtx;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext.UnionParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.reduceSinkDesc;

/**
 * Processor for the rule - table scan followed by reduce sink
 */
public class GenMRRedSink3 implements NodeProcessor {

  public GenMRRedSink3() {
  }

  /**
   * Reduce Scan encountered 
   * @param nd the reduce sink operator encountered
   * @param opProcCtx context
   */
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx opProcCtx, Object... nodeOutputs) throws SemanticException {
    ReduceSinkOperator op = (ReduceSinkOperator)nd;
    GenMRProcContext ctx = (GenMRProcContext)opProcCtx;

    ParseContext parseCtx = ctx.getParseCtx();
    UnionProcContext uCtx = parseCtx.getUCtx();

    // union was map only - no special processing needed
    if (uCtx.isMapOnlySubq())
      return (new GenMRRedSink1()).process(nd, stack, opProcCtx, nodeOutputs);

    // union consisted on a bunch of map-reduce jobs, and it has been split at the union
    Operator<? extends Serializable> reducer = op.getChildOperators().get(0);
    Map<Operator<? extends Serializable>, GenMapRedCtx> mapCurrCtx = ctx.getMapCurrCtx();
    GenMapRedCtx mapredCtx = mapCurrCtx.get(op.getParentOperators().get(0));
    Task<? extends Serializable> currTask    = mapredCtx.getCurrTask();
    mapredWork plan = (mapredWork) currTask.getWork();
    HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> opTaskMap = ctx.getOpTaskMap();
    
    opTaskMap.put(reducer, currTask);
    plan.setReducer(reducer);
    reduceSinkDesc desc = (reduceSinkDesc)op.getConf();
    
    plan.setNumReduceTasks(desc.getNumReducers());
    
    if (reducer.getClass() == JoinOperator.class)
      plan.setNeedsTagging(true);
    
    ctx.setCurrTask(currTask);
    ctx.setCurrTopOp(null);
    ctx.setCurrAliasId(null);
    return null;
  }
}
