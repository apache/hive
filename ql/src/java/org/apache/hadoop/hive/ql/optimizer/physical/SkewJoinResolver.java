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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.LoadFileDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.slf4j.Logger;

/**
 * An implementation of PhysicalPlanResolver. It iterator each task with a rule
 * dispatcher for its reducer operator tree, for task with join op in reducer,
 * it will try to add a conditional task associated a list of skew join tasks.
 */
public class SkewJoinResolver implements PhysicalPlanResolver {
  private final static Logger LOG = org.slf4j.LoggerFactory.getLogger(SkewJoinResolver.class);

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    SemanticDispatcher disp = new SkewJoinTaskDispatcher(pctx);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  /**
   * Iterator a task with a rule dispatcher for its reducer operator tree.
   */
  class SkewJoinTaskDispatcher implements SemanticDispatcher {

    private PhysicalContext physicalContext;

    public SkewJoinTaskDispatcher(PhysicalContext context) {
      super();
      physicalContext = context;
    }

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs)
        throws SemanticException {
      Task<?> task = (Task<?>) nd;

      if (!task.isMapRedTask() || task instanceof ConditionalTask
          || ((MapredWork) task.getWork()).getReduceWork() == null) {
        return null;
      }

      ParseContext pc = physicalContext.getParseContext();
      if (pc.getLoadTableWork() != null) {
        for (LoadTableDesc ltd : pc.getLoadTableWork()) {
          if (!ltd.isMmTable()) {
            continue;
          }
          // See the path in FSOP that calls fs.exists on finalPath.
          LOG.debug("Not using skew join because the destination table "
              + ltd.getTable().getTableName() + " is an insert_only table");
          return null;
        }
      }
      if (pc.getLoadFileWork() != null) {
        for (LoadFileDesc lfd : pc.getLoadFileWork()) {
          if (!lfd.isMmCtas()) {
            continue;
          }
          LOG.debug("Not using skew join because the destination table is an insert_only table");
          return null;
        }
      }

      SkewJoinProcCtx skewJoinProcContext = new SkewJoinProcCtx(task, pc);

      Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
      opRules.put(new RuleRegExp("R1",
        CommonJoinOperator.getOperatorName() + "%"),
        SkewJoinProcFactory.getJoinProc());

      // The dispatcher fires the processor corresponding to the closest
      // matching rule and passes the context along
      SemanticDispatcher disp = new DefaultRuleDispatcher(SkewJoinProcFactory
          .getDefaultProc(), opRules, skewJoinProcContext);
      SemanticGraphWalker ogw = new DefaultGraphWalker(disp);

      // iterator the reducer operator tree
      ArrayList<Node> topNodes = new ArrayList<Node>();
      if (((MapredWork)task.getWork()).getReduceWork() != null) {
        topNodes.add(((MapredWork) task.getWork()).getReduceWork().getReducer());
      }
      ogw.startWalking(topNodes, null);
      return null;
    }

    public PhysicalContext getPhysicalContext() {
      return physicalContext;
    }

    public void setPhysicalContext(PhysicalContext physicalContext) {
      this.physicalContext = physicalContext;
    }
  }

  /**
   * A container of current task and parse context.
   */
  public static class SkewJoinProcCtx implements NodeProcessorCtx {
    private Task<?> currentTask;
    private ParseContext parseCtx;

    public SkewJoinProcCtx(Task<?> task,
        ParseContext parseCtx) {
      currentTask = task;
      this.parseCtx = parseCtx;
    }

    public Task<?> getCurrentTask() {
      return currentTask;
    }

    public void setCurrentTask(Task<?> currentTask) {
      this.currentTask = currentTask;
    }

    public ParseContext getParseCtx() {
      return parseCtx;
    }

    public void setParseCtx(ParseContext parseCtx) {
      this.parseCtx = parseCtx;
    }
  }
}
