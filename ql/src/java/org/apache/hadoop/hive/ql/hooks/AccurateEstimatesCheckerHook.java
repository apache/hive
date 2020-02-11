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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ExplainWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.TezWork;

import com.google.common.collect.Lists;

/**
 * Adds an extra check for Explain Analyze queries.
 */
public class AccurateEstimatesCheckerHook extends AbstractSemanticAnalyzerHook {

  private double absErr;
  private double relErr;

  class EstimateCheckerHook implements SemanticNodeProcessor {

    Map<String, Operator<?>> opMap = new HashMap<>();

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
        throws SemanticException {
      Operator op = (Operator) nd;
      Statistics stats = op.getStatistics();
      if (stats != null && stats.getRunTimeNumRows() >= 0) {
        try {
          ensureEstimateAcceptable(stats.getNumRows(), stats.getRunTimeNumRows());
        } catch (HiveException e) {
          throw new SemanticException("On operator: " + op, e);
        }
      }
      return null;
    }

  }

  private void ensureEstimateAcceptable(long numRows, long runTimeNumRows) throws HiveException {
    double currentDelta = Math.abs(runTimeNumRows - numRows);
    if (currentDelta <= absErr) {
      return;
    }
    if (runTimeNumRows > 0) {
      double a = currentDelta / runTimeNumRows;
      if (a <= relErr) {
        return;
      }
    }
    throw new HiveException("Estimation error is unacceptable " + numRows + " / " + runTimeNumRows + " with absErr:"
        + absErr+ ", relErr:" + relErr);
  }

  @Override
  public void postAnalyze(HiveSemanticAnalyzerHookContext context, List<Task<?>> rootTasks) throws SemanticException {

    HiveSemanticAnalyzerHookContext hookContext = context;
    HiveConf conf = (HiveConf) hookContext.getConf();
    absErr = conf.getDouble("accurate.estimate.checker.absolute.error", 3.0);
    relErr = conf.getDouble("accurate.estimate.checker.relative.error", .1);
    List<Node> rootOps = Lists.newArrayList();

    List<Task<?>> roots = rootTasks;
    for (Task<?> task0 : roots) {

      if (task0 instanceof ExplainTask) {
        ExplainTask explainTask = (ExplainTask) task0;
        ExplainWork w = explainTask.getWork();
        List<Task<?>> explainRoots = w.getRootTasks();

        for (Task<?> task : explainRoots) {

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
      }

    }
    if (rootOps.isEmpty()) {
      return;
    }

    SemanticDispatcher disp = new DefaultRuleDispatcher(new EstimateCheckerHook(), new HashMap<>(), null);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);

    HashMap<Node, Object> nodeOutput = new HashMap<Node, Object>();
    ogw.startWalking(rootOps, nodeOutput);

  }
}
