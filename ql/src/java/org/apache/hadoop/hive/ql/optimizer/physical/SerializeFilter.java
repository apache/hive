/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.physical;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.StatsTask;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MergeJoinWork;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SerializeFilter is a simple physical optimizer that serializes all filter expressions in
 * Tablescan Operators.
 */
public class SerializeFilter implements PhysicalPlanResolver {

  protected static transient final Logger LOG = LoggerFactory.getLogger(SerializeFilter.class);

  public class Serializer implements SemanticDispatcher {

    private final PhysicalContext pctx;

    public Serializer(PhysicalContext pctx) {
      this.pctx = pctx;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs)
      throws SemanticException {
      Task<?> currTask = (Task<?>) nd;
      if (currTask instanceof StatsTask) {
        currTask = ((StatsTask) currTask).getWork().getSourceTask();
      }
      if (currTask instanceof TezTask) {
        TezWork work = ((TezTask) currTask).getWork();
        for (BaseWork w : work.getAllWork()) {
          evaluateWork(w);
        }
      }
      return null;
    }

    private void evaluateWork(BaseWork w) throws SemanticException {

      if (w instanceof MapWork) {
        evaluateMapWork((MapWork) w);
      } else if (w instanceof ReduceWork) {
        evaluateReduceWork((ReduceWork) w);
      } else if (w instanceof MergeJoinWork) {
        evaluateMergeWork((MergeJoinWork) w);
      } else {
        LOG.info("We are not going to evaluate this work type: " + w.getClass().getCanonicalName());
      }
    }

    private void evaluateMergeWork(MergeJoinWork w) throws SemanticException {
      for (BaseWork baseWork : w.getBaseWorkList()) {
        evaluateOperators(baseWork, pctx);
      }
    }

    private void evaluateReduceWork(ReduceWork w) throws SemanticException {
      evaluateOperators(w, pctx);
    }

    private void evaluateMapWork(MapWork w) throws SemanticException {
      evaluateOperators(w, pctx);
    }

    private void evaluateOperators(BaseWork w, PhysicalContext pctx) throws SemanticException {

      SemanticDispatcher disp = null;
      final Set<TableScanOperator> tableScans = new LinkedHashSet<TableScanOperator>();

      LinkedHashMap<SemanticRule, SemanticNodeProcessor> rules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
      rules.put(new RuleRegExp("TS finder",
              TableScanOperator.getOperatorName() + "%"), new SemanticNodeProcessor() {
          @Override
          public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
              Object... nodeOutputs) {
            tableScans.add((TableScanOperator) nd);
            return null;
          }
        });
      disp = new DefaultRuleDispatcher(null, rules, null);

      SemanticGraphWalker ogw = new DefaultGraphWalker(disp);

      ArrayList<Node> topNodes = new ArrayList<Node>();
      topNodes.addAll(w.getAllRootOperators());

      LinkedHashMap<Node, Object> nodeOutput = new LinkedHashMap<Node, Object>();
      ogw.startWalking(topNodes, nodeOutput);

      for (TableScanOperator ts: tableScans) {
        if (ts.getConf() != null && ts.getConf().getFilterExpr() != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Serializing: " + ts.getConf().getFilterExpr().getExprString());
          }
          ts.getConf().setSerializedFilterExpr(
              SerializationUtilities.serializeExpression(ts.getConf().getFilterExpr()));
        }

        if (ts.getConf() != null && ts.getConf().getFilterObject() != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Serializing: " + ts.getConf().getFilterObject());
          }

          ts.getConf().setSerializedFilterObject(
              SerializationUtilities.serializeObject(ts.getConf().getFilterObject()));
        }
      }
    }

    public class DefaultRule implements SemanticNodeProcessor {

      @Override
      public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
          Object... nodeOutputs) throws SemanticException {
        return null;
      }
    }
  }

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {

    pctx.getConf();

    // create dispatcher and graph walker
    SemanticDispatcher disp = new Serializer(pctx);
    TaskGraphWalker ogw = new TaskGraphWalker(disp);

    // get all the tasks nodes from root task
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());

    // begin to walk through the task tree.
    ogw.startWalking(topNodes, null);
    return pctx;
  }
}
