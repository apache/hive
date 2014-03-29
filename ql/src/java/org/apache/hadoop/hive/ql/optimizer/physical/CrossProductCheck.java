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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.AbstractMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.session.SessionState;

/*
 * Check each MapJoin and ShuffleJoin Operator to see they are performing a cross product.
 * If yes, output a warning to the Session's console.
 * The Checks made are the following:
 * 1. MR, Shuffle Join:
 * Check the parent ReduceSinkOp of the JoinOp. If its keys list is size = 0, then
 * this is a cross product.
 * The parent ReduceSinkOp is in the MapWork for the same Stage.
 * 2. MR, MapJoin:
 * If the keys expr list on the mapJoin Desc is an empty list for any input,
 * this implies a cross product.
 * 3. Tez, Shuffle Join:
 * Check the parent ReduceSinkOp of the JoinOp. If its keys list is size = 0, then
 * this is a cross product.
 * The parent ReduceSinkOp checked is based on the ReduceWork.tagToInput map on the
 * reduceWork that contains the JoinOp.
 * 4. Tez, Map Join:
 * If the keys expr list on the mapJoin Desc is an empty list for any input,
 * this implies a cross product.
 */
public class CrossProductCheck implements PhysicalPlanResolver, Dispatcher {

  protected static transient final Log LOG = LogFactory
      .getLog(CrossProductCheck.class);

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    TaskGraphWalker ogw = new TaskGraphWalker(this);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());

    ogw.startWalking(topNodes, null);
    return pctx;
  }

  @Override
  public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs)
      throws SemanticException {
    @SuppressWarnings("unchecked")
    Task<? extends Serializable> currTask = (Task<? extends Serializable>) nd;
    if (currTask instanceof MapRedTask) {
      MapRedTask mrTsk = (MapRedTask)currTask;
      MapredWork mrWrk = mrTsk.getWork();
      checkMapJoins(mrTsk);
      checkMRReducer(currTask.toString(), mrWrk);
    } else if (currTask instanceof ConditionalTask ) {
      List<Task<? extends Serializable>> taskListInConditionalTask =
          ((ConditionalTask) currTask).getListTasks();
      for(Task<? extends Serializable> tsk: taskListInConditionalTask){
        dispatch(tsk, stack, nodeOutputs);
      }

    } else if (currTask instanceof TezTask) {
      TezTask tzTask = (TezTask) currTask;
      TezWork tzWrk = tzTask.getWork();
      checkMapJoins(tzWrk);
      checkTezReducer(tzWrk);
    }
    return null;
  }

  private void warn(String msg) {
    SessionState.getConsole().getInfoStream().println(
        String.format("Warning: %s", msg));
  }

  private void checkMapJoins(MapRedTask mrTsk) throws SemanticException {
    MapredWork mrWrk = mrTsk.getWork();
    MapWork mapWork = mrWrk.getMapWork();
    List<String> warnings = new MapJoinCheck(mrTsk.toString()).analyze(mapWork);
    if (!warnings.isEmpty()) {
      for (String w : warnings) {
        warn(w);
      }
    }
    ReduceWork redWork = mrWrk.getReduceWork();
    if (redWork != null) {
      warnings = new MapJoinCheck(mrTsk.toString()).analyze(redWork);
      if (!warnings.isEmpty()) {
        for (String w : warnings) {
          warn(w);
        }
      }
    }
  }

  private void checkMapJoins(TezWork tzWrk) throws SemanticException {
    for(BaseWork wrk : tzWrk.getAllWork() ) {
      List<String> warnings = new MapJoinCheck(wrk.getName()).analyze(wrk);
      if ( !warnings.isEmpty() ) {
        for(String w : warnings) {
          warn(w);
        }
      }
    }
  }

  private void checkTezReducer(TezWork tzWrk) throws SemanticException {
    for(BaseWork wrk : tzWrk.getAllWork() ) {
      if ( !(wrk instanceof ReduceWork) ) {
        continue;
      }
      ReduceWork rWork = (ReduceWork) wrk;
      Operator<? extends OperatorDesc> reducer = ((ReduceWork)wrk).getReducer();
      if ( reducer instanceof JoinOperator ) {
        Map<Integer, ExtractReduceSinkInfo.Info> rsInfo =
            new HashMap<Integer, ExtractReduceSinkInfo.Info>();
        for(Map.Entry<Integer, String> e : rWork.getTagToInput().entrySet()) {
          rsInfo.putAll(getReducerInfo(tzWrk, rWork.getName(), e.getValue()));
        }
        checkForCrossProduct(rWork.getName(), reducer, rsInfo);
      }
    }
  }

  private void checkMRReducer(String taskName, MapredWork mrWrk) throws SemanticException {
    ReduceWork rWrk = mrWrk.getReduceWork();
    if ( rWrk == null) {
      return;
    }
    Operator<? extends OperatorDesc> reducer = rWrk.getReducer();
    if ( reducer instanceof JoinOperator ) {
      BaseWork prntWork = mrWrk.getMapWork();
      checkForCrossProduct(taskName, reducer,
          new ExtractReduceSinkInfo(null).analyze(prntWork));
    }
  }

  private void checkForCrossProduct(String taskName,
      Operator<? extends OperatorDesc> reducer,
      Map<Integer, ExtractReduceSinkInfo.Info> rsInfo) {
    if ( rsInfo.isEmpty() ) {
      return;
    }
    Iterator<ExtractReduceSinkInfo.Info> it = rsInfo.values().iterator();
    ExtractReduceSinkInfo.Info info = it.next();
    if (info.keyCols.size() == 0) {
      List<String> iAliases = new ArrayList<String>();
      iAliases.addAll(info.inputAliases);
      while (it.hasNext()) {
        info = it.next();
        iAliases.addAll(info.inputAliases);
      }
      String warning = String.format(
          "Shuffle Join %s[tables = %s] in Stage '%s' is a cross product",
          reducer.toString(),
          iAliases,
          taskName);
      warn(warning);
    }
  }

  private Map<Integer, ExtractReduceSinkInfo.Info> getReducerInfo(TezWork tzWrk, String vertex, String prntVertex)
      throws SemanticException {
    BaseWork prntWork = tzWrk.getWorkMap().get(prntVertex);
    return new ExtractReduceSinkInfo(vertex).analyze(prntWork);
  }

  /*
   * Given a Work descriptor and the TaskName for the work
   * this is responsible to check each MapJoinOp for cross products.
   * The analyze call returns the warnings list.
   * <p>
   * For MR the taskname is the StageName, for Tez it is the vertex name.
   */
  class MapJoinCheck implements NodeProcessor, NodeProcessorCtx {

    final List<String> warnings;
    final String taskName;

    MapJoinCheck(String taskName) {
      this.taskName = taskName;
      warnings = new ArrayList<String>();
    }

    List<String> analyze(BaseWork work) throws SemanticException {
      Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
      opRules.put(new RuleRegExp("R1", MapJoinOperator.getOperatorName()
          + "%"), this);
      Dispatcher disp = new DefaultRuleDispatcher(new NoopProcessor(), opRules, this);
      GraphWalker ogw = new DefaultGraphWalker(disp);
      ArrayList<Node> topNodes = new ArrayList<Node>();
      topNodes.addAll(work.getAllRootOperators());
      ogw.startWalking(topNodes, null);
      return warnings;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      @SuppressWarnings("unchecked")
      AbstractMapJoinOperator<? extends MapJoinDesc> mjOp = (AbstractMapJoinOperator<? extends MapJoinDesc>) nd;
      MapJoinDesc mjDesc = mjOp.getConf();

      String bigTablAlias = mjDesc.getBigTableAlias();
      if ( bigTablAlias == null ) {
        Operator<? extends OperatorDesc> parent = null;
        for(Operator<? extends OperatorDesc> op : mjOp.getParentOperators() ) {
          if ( op instanceof TableScanOperator ) {
            parent = op;
          }
        }
        if ( parent != null) {
          TableScanDesc tDesc = ((TableScanOperator)parent).getConf();
          bigTablAlias = tDesc.getAlias();
        }
      }
      bigTablAlias = bigTablAlias == null ? "?" : bigTablAlias;

      List<ExprNodeDesc> joinExprs = mjDesc.getKeys().values().iterator().next();

      if ( joinExprs.size() == 0 ) {
        warnings.add(
            String.format("Map Join %s[bigTable=%s] in task '%s' is a cross product",
                mjOp.toString(), bigTablAlias, taskName));
      }

      return null;
    }
  }

  /*
   * for a given Work Descriptor, it extracts information about the ReduceSinkOps
   * in the Work. For Tez, you can restrict it to ReduceSinks for a particular output
   * vertex.
   */
  static class ExtractReduceSinkInfo implements NodeProcessor, NodeProcessorCtx {

    static class Info {
      List<ExprNodeDesc> keyCols;
      List<String> inputAliases;

      Info(List<ExprNodeDesc> keyCols, List<String> inputAliases) {
        this.keyCols = keyCols;
        this.inputAliases = inputAliases == null ? new ArrayList<String>() : inputAliases;
      }

      Info(List<ExprNodeDesc> keyCols, String[] inputAliases) {
        this.keyCols = keyCols;
        this.inputAliases = inputAliases == null ? new ArrayList<String>() : Arrays.asList(inputAliases);
      }
    }

    final String outputTaskName;
    final Map<Integer, Info> reduceSinkInfo;

    ExtractReduceSinkInfo(String parentTaskName) {
      this.outputTaskName = parentTaskName;
      reduceSinkInfo = new HashMap<Integer, Info>();
    }

    Map<Integer, Info> analyze(BaseWork work) throws SemanticException {
      Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
      opRules.put(new RuleRegExp("R1", ReduceSinkOperator.getOperatorName()
          + "%"), this);
      Dispatcher disp = new DefaultRuleDispatcher(new NoopProcessor(), opRules, this);
      GraphWalker ogw = new DefaultGraphWalker(disp);
      ArrayList<Node> topNodes = new ArrayList<Node>();
      topNodes.addAll(work.getAllRootOperators());
      ogw.startWalking(topNodes, null);
      return reduceSinkInfo;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ReduceSinkOperator rsOp = (ReduceSinkOperator) nd;
      ReduceSinkDesc rsDesc = rsOp.getConf();

      if ( outputTaskName != null ) {
        String rOutputName = rsDesc.getOutputName();
        if ( rOutputName == null || !outputTaskName.equals(rOutputName)) {
          return null;
        }
      }

      reduceSinkInfo.put(rsDesc.getTag(),
          new Info(rsDesc.getKeyCols(), rsOp.getInputAliases()));

      return null;
    }
  }

  static class NoopProcessor implements NodeProcessor {
    @Override
    public final Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
     return nd;
    }
  }
}
