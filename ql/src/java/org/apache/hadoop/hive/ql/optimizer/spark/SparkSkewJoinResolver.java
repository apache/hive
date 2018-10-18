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

package org.apache.hadoop.hive.ql.optimizer.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.PreOrderWalker;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalContext;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalPlanResolver;
import org.apache.hadoop.hive.ql.optimizer.physical.SkewJoinResolver;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SparkWork;

/**
 * Spark version of SkewJoinResolver.
 */
public class SparkSkewJoinResolver implements PhysicalPlanResolver {
  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    SparkSkewJoinProcFactory.getVisitedJoinOp().clear();
    Dispatcher disp = new SparkSkewJoinTaskDispatcher(pctx);
    // since we may split current task, use a pre-order walker
    GraphWalker ogw = new PreOrderWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  class SparkSkewJoinTaskDispatcher implements Dispatcher {
    private PhysicalContext physicalContext;

    public SparkSkewJoinTaskDispatcher(PhysicalContext context) {
      super();
      physicalContext = context;
    }

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs)
        throws SemanticException {

      @SuppressWarnings("unchecked")
      Task<? extends Serializable> task = (Task<? extends Serializable>) nd;
      if (task instanceof SparkTask) {
        SparkWork sparkWork = ((SparkTask) task).getWork();
        SparkSkewJoinProcCtx skewJoinProcCtx =
            new SparkSkewJoinProcCtx(task, physicalContext.getParseContext());
        Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
        opRules.put(new RuleRegExp("R1", CommonJoinOperator.getOperatorName() + "%"),
            SparkSkewJoinProcFactory.getJoinProc());
        Dispatcher disp = new DefaultRuleDispatcher(
            SparkSkewJoinProcFactory.getDefaultProc(), opRules, skewJoinProcCtx);
        GraphWalker ogw = new DefaultGraphWalker(disp);
        ArrayList<Node> topNodes = new ArrayList<Node>();
        // since we may need to split the task, let's walk the graph bottom-up
        List<ReduceWork> reduceWorkList = sparkWork.getAllReduceWork();
        Collections.reverse(reduceWorkList);
        for (ReduceWork reduceWork : reduceWorkList) {
          topNodes.add(reduceWork.getReducer());
          skewJoinProcCtx.getReducerToReduceWork().put(reduceWork.getReducer(), reduceWork);
        }
        ogw.startWalking(topNodes, null);
      }
      return null;
    }

    public PhysicalContext getPhysicalContext() {
      return physicalContext;
    }

    public void setPhysicalContext(PhysicalContext physicalContext) {
      this.physicalContext = physicalContext;
    }
  }

  public static class SparkSkewJoinProcCtx extends SkewJoinResolver.SkewJoinProcCtx {
    // need a map from the reducer to the corresponding ReduceWork
    private Map<Operator<?>, ReduceWork> reducerToReduceWork;

    public SparkSkewJoinProcCtx(Task<? extends Serializable> task,
                                ParseContext parseCtx) {
      super(task, parseCtx);
      reducerToReduceWork = new HashMap<Operator<?>, ReduceWork>();
    }

    public Map<Operator<?>, ReduceWork> getReducerToReduceWork() {
      return reducerToReduceWork;
    }
  }
}
