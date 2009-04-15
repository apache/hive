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

import java.util.List;
import java.util.HashMap;
import java.util.Stack;
import java.io.Serializable;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Processor for the rule - table scan followed by reduce sink
 */
public class GenMRFileSink1 implements NodeProcessor {

  public GenMRFileSink1() {
  }

  /**
   * File Sink Operator encountered 
   * @param nd the file sink operator encountered
   * @param opProcCtx context
   */
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx opProcCtx, Object... nodeOutputs) throws SemanticException {
    GenMRProcContext ctx = (GenMRProcContext)opProcCtx;
    boolean ret = false;

    Task<? extends Serializable> mvTask = ctx.getMvTask();
    Task<? extends Serializable> currTask = ctx.getCurrTask();
    Operator<? extends Serializable> currTopOp = ctx.getCurrTopOp();
    String currAliasId = ctx.getCurrAliasId();
    HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> opTaskMap = ctx.getOpTaskMap();
    List<Operator<? extends Serializable>> seenOps = ctx.getSeenOps();
    List<Task<? extends Serializable>>  rootTasks = ctx.getRootTasks();

    // Set the move task to be dependent on the current task
    if (mvTask != null) 
      ret = currTask.addDependentTask(mvTask);
    
    // In case of multi-table insert, the path to alias mapping is needed for all the sources. Since there is no
    // reducer, treat it as a plan with null reducer
    // If it is a map-only job, the task needs to be processed
    if (currTopOp != null) {
      Task<? extends Serializable> mapTask = opTaskMap.get(null);
      if (mapTask == null) {
        assert (!seenOps.contains(currTopOp));
        seenOps.add(currTopOp);
        GenMapRedUtils.setTaskPlan(currAliasId, currTopOp, null, (mapredWork) currTask.getWork(), false, ctx);
        opTaskMap.put(null, currTask);
        rootTasks.add(currTask);
      }
      else {
        if (!seenOps.contains(currTopOp)) {
          seenOps.add(currTopOp);
          GenMapRedUtils.setTaskPlan(currAliasId, currTopOp, null, (mapredWork) mapTask.getWork(), false, ctx);
        }
        if (ret)
          currTask.removeDependentTask(mvTask);
      }
    }
    return null;
  }
}
