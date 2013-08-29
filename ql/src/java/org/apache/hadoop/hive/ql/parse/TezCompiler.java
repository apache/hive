/**
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hive.ql.parse;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TezWork;

/**
 * TezCompiler translates the operator plan into TezTasks.
 */
public class TezCompiler extends TaskCompiler {

  protected final Log LOG = LogFactory.getLog(TezCompiler.class);

  public TezCompiler() {
  }

  @Override
  protected void optimizeOperatorPlan(ParseContext pCtx, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs) throws SemanticException {

    // Sequence of TableScan operators to be walked
    Deque<Operator<?>> deque = new LinkedList<Operator<?>>();
    deque.addAll(pCtx.getTopOps().values());

    // Create the context for the walker
    OptimizeTezProcContext procCtx
      = new OptimizeTezProcContext(conf, pCtx, inputs, outputs, deque);

    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack.
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp(new String("Set parallelism - ReduceSink"),
        ReduceSinkOperator.getOperatorName() + "%"),
        new SetReducerParallelism());

    // if this is an explain statement add rule to generate statistics for
    // the whole tree.
    if (pCtx.getContext().getExplain()) {
      opRules.put(new RuleRegExp(new String("Set statistics - FileSink"),
          FileSinkOperator.getOperatorName() + "%"),
          new SetStatistics());
    }

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, procCtx);
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pCtx.getTopOps().values());
    GraphWalker ogw = new TezWalker(disp);
    ogw.startWalking(topNodes, null);
  }

  @Override
  protected void generateTaskTree(List<Task<? extends Serializable>> rootTasks, ParseContext pCtx,
      List<Task<MoveWork>> mvTask, Set<ReadEntity> inputs, Set<WriteEntity> outputs)
      throws SemanticException {

    ParseContext tempParseContext = getParseContext(pCtx, rootTasks);

    // Sequence of TableScan operators to be walked
    Deque<Operator<?>> deque = new LinkedList<Operator<?>>();
    deque.addAll(pCtx.getTopOps().values());

    GenTezProcContext procCtx = new GenTezProcContext(
        conf, tempParseContext, mvTask, rootTasks, inputs, outputs, deque);

    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack.
    // The dispatcher generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp(new String("Split Work - ReduceSink"),
        ReduceSinkOperator.getOperatorName() + "%"),
        new GenTezWork());
    opRules.put(new RuleRegExp(new String("Split Work - FileSink"),
        FileSinkOperator.getOperatorName() + "%"),
        new GenTezWork());


    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, procCtx);
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pCtx.getTopOps().values());
    GraphWalker ogw = new TezWalker(disp);
    ogw.startWalking(topNodes, null);
  }

  @Override
  protected void setInputFormat(Task<? extends Serializable> task) {
    if (task instanceof TezTask) {
      TezWork work = ((TezTask)task).getWork();
      Set<BaseWork> roots = work.getRoots();
      for (BaseWork w: roots) {
        assert w instanceof MapWork;
        MapWork mapWork = (MapWork)w;
        HashMap<String, Operator<? extends OperatorDesc>> opMap = mapWork.getAliasToWork();
        if (!opMap.isEmpty()) {
          for (Operator<? extends OperatorDesc> op : opMap.values()) {
            setInputFormat(mapWork, op);
          }
        }
      }
    } else if (task instanceof ConditionalTask) {
      List<Task<? extends Serializable>> listTasks
        = ((ConditionalTask) task).getListTasks();
      for (Task<? extends Serializable> tsk : listTasks) {
        setInputFormat(tsk);
      }
    }

    if (task.getChildTasks() != null) {
      for (Task<? extends Serializable> childTask : task.getChildTasks()) {
        setInputFormat(childTask);
      }
    }
  }

  private void setInputFormat(MapWork work, Operator<? extends OperatorDesc> op) {
    if (op.isUseBucketizedHiveInputFormat()) {
      work.setUseBucketizedHiveInputFormat(true);
      return;
    }

    if (op.getChildOperators() != null) {
      for (Operator<? extends OperatorDesc> childOp : op.getChildOperators()) {
        setInputFormat(work, childOp);
      }
    }
  }

  @Override
  protected void generateCountersTask(Task<? extends Serializable> task) {
    if (task instanceof TezTask) {
      TezWork work = ((TezTask)task).getWork();
      List<BaseWork> workItems = work.getAllWork();
      for (BaseWork w: workItems) {
        List<Operator<?>> ops = w.getAllOperators();
        for (Operator<?> op: ops) {
          generateCountersOperator(op);
        }
      }
    } else if (task instanceof ConditionalTask) {
      List<Task<? extends Serializable>> listTasks = ((ConditionalTask) task)
          .getListTasks();
      for (Task<? extends Serializable> tsk : listTasks) {
        generateCountersTask(tsk);
      }
    }

    Operator.resetLastEnumUsed();

    if (task.getChildTasks() == null) {
      return;
    }

    for (Task<? extends Serializable> childTask : task.getChildTasks()) {
      generateCountersTask(childTask);
    }
  }

  private void generateCountersOperator(Operator<?> op) {
    op.assignCounterNameToEnum();

    if (op.getChildOperators() == null) {
      return;
    }

    for (Operator<?> child : op.getChildOperators()) {
      generateCountersOperator(child);
    }
  }

  @Override
  protected void decideExecMode(List<Task<? extends Serializable>> rootTasks, Context ctx,
      GlobalLimitCtx globalLimitCtx)
      throws SemanticException {
    // currently all Tez work is on the cluster
    return;
  }

  @Override
  protected void optimizeTaskPlan(List<Task<? extends Serializable>> rootTasks, ParseContext pCtx,
      Context ctx) throws SemanticException {
    // no additional optimization needed
    return;
  }
}
