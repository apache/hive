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

package org.apache.hadoop.hive.ql.hooks;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezWork;

/**
 * Checks whenever operator ids are not reused.
 */
public class NoOperatorReuseCheckerHook implements ExecuteWithHookContext {

  static class UniqueOpIdChecker implements SemanticNodeProcessor {

    Map<String, Operator<?>> opMap = new HashMap<>();

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
        throws SemanticException {
      Operator op = (Operator) nd;
      String opKey = op.getOperatorId();
      Operator<?> found = opMap.get(opKey);
      if (found != null) {
        throw new RuntimeException("operator id reuse found: " + opKey);
      }
      opMap.put(opKey, op);
      return null;
    }
  }

  @Override
  public void run(HookContext hookContext) throws Exception {

    List<Node> rootOps = Lists.newArrayList();

    List<Task<?>> roots = hookContext.getQueryPlan().getRootTasks();
    for (Task<?> task : roots) {

      Object work = task.getWork();
      if (work instanceof MapredWork) {
        MapredWork mapredWork = (MapredWork) work;
        MapWork mapWork = mapredWork.getMapWork();
        if (mapWork != null) {
          rootOps.addAll(mapWork.getAllRootOperators());
        }
        ReduceWork reduceWork = mapredWork.getReduceWork();
        if (reduceWork != null) {
          rootOps.addAll(reduceWork.getAllRootOperators());
        }
      }
      if (work instanceof TezWork) {
        for (BaseWork bw : ((TezWork) work).getAllWorkUnsorted()) {
          rootOps.addAll(bw.getAllRootOperators());
        }
      }
    }
    if (rootOps.isEmpty()) {
      return;
    }

    SemanticDispatcher disp = new DefaultRuleDispatcher(new UniqueOpIdChecker(), new HashMap<>(), null);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);

    HashMap<Node, Object> nodeOutput = new HashMap<Node, Object>();
    ogw.startWalking(rootOps, nodeOutput);

  }
}
