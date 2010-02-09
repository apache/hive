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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMRMapJoinCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMapRedCtx;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext;
import org.apache.hadoop.hive.ql.parse.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;

/**
 * Operator factory for MapJoin processing.
 */
public final class MapJoinFactory {

  public static int getPositionParent(MapJoinOperator op, Stack<Node> stack) {
    int pos = 0;
    int size = stack.size();
    assert size >= 2 && stack.get(size - 1) == op;
    Operator<? extends Serializable> parent = (Operator<? extends Serializable>) stack
        .get(size - 2);
    List<Operator<? extends Serializable>> parOp = op.getParentOperators();
    pos = parOp.indexOf(parent);
    assert pos < parOp.size();
    return pos;
  }

  /**
   * TableScan followed by MapJoin.
   */
  public static class TableScanMapJoin implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      MapJoinOperator mapJoin = (MapJoinOperator) nd;
      GenMRProcContext ctx = (GenMRProcContext) procCtx;

      // find the branch on which this processor was invoked
      int pos = getPositionParent(mapJoin, stack);

      Map<Operator<? extends Serializable>, GenMapRedCtx> mapCurrCtx = ctx
          .getMapCurrCtx();
      GenMapRedCtx mapredCtx = mapCurrCtx.get(mapJoin.getParentOperators().get(
          pos));
      Task<? extends Serializable> currTask = mapredCtx.getCurrTask();
      MapredWork currPlan = (MapredWork) currTask.getWork();
      Operator<? extends Serializable> currTopOp = mapredCtx.getCurrTopOp();
      String currAliasId = mapredCtx.getCurrAliasId();
      Operator<? extends Serializable> reducer = mapJoin;
      HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> opTaskMap = ctx
          .getOpTaskMap();
      Task<? extends Serializable> opMapTask = opTaskMap.get(reducer);

      ctx.setCurrTopOp(currTopOp);
      ctx.setCurrAliasId(currAliasId);
      ctx.setCurrTask(currTask);

      // If the plan for this reducer does not exist, initialize the plan
      if (opMapTask == null) {
        assert currPlan.getReducer() == null;
        GenMapRedUtils.initMapJoinPlan(mapJoin, ctx, false, false, false, pos);
      } else {
        // The current plan can be thrown away after being merged with the
        // original plan
        GenMapRedUtils.joinPlan(mapJoin, null, opMapTask, ctx, pos, false,
            false, false);
        currTask = opMapTask;
        ctx.setCurrTask(currTask);
      }

      mapCurrCtx.put(mapJoin, new GenMapRedCtx(ctx.getCurrTask(), ctx
          .getCurrTopOp(), ctx.getCurrAliasId()));
      return null;
    }
  }

  /**
   * ReduceSink followed by MapJoin.
   */
  public static class ReduceSinkMapJoin implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      MapJoinOperator mapJoin = (MapJoinOperator) nd;
      GenMRProcContext opProcCtx = (GenMRProcContext) procCtx;

      MapredWork cplan = GenMapRedUtils.getMapRedWork();
      ParseContext parseCtx = opProcCtx.getParseCtx();
      Task<? extends Serializable> redTask = TaskFactory.get(cplan, parseCtx
          .getConf());
      Task<? extends Serializable> currTask = opProcCtx.getCurrTask();

      // find the branch on which this processor was invoked
      int pos = getPositionParent(mapJoin, stack);
      boolean local = (pos == (mapJoin.getConf()).getPosBigTable()) ? false
          : true;

      GenMapRedUtils.splitTasks(mapJoin, currTask, redTask, opProcCtx, false,
          local, pos);

      currTask = opProcCtx.getCurrTask();
      HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> opTaskMap = opProcCtx
          .getOpTaskMap();
      Task<? extends Serializable> opMapTask = opTaskMap.get(mapJoin);

      // If the plan for this reducer does not exist, initialize the plan
      if (opMapTask == null) {
        assert cplan.getReducer() == null;
        opTaskMap.put(mapJoin, currTask);
        opProcCtx.setCurrMapJoinOp(null);
      } else {
        // The current plan can be thrown away after being merged with the
        // original plan
        GenMapRedUtils.joinPlan(mapJoin, currTask, opMapTask, opProcCtx, pos,
            false, false, false);
        currTask = opMapTask;
        opProcCtx.setCurrTask(currTask);
      }

      return null;
    }
  }

  /**
   * MapJoin followed by Select.
   */
  public static class MapJoin implements NodeProcessor {

    /**
     * Create a task by splitting the plan below the join. The reason, we have
     * to do so in the processing of Select and not MapJoin is due to the
     * walker. While processing a node, it is not safe to alter its children
     * because that will decide the course of the walk. It is perfectly fine to
     * muck around with its parents though, since those nodes have already been
     * visited.
     */
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      SelectOperator sel = (SelectOperator) nd;
      MapJoinOperator mapJoin = (MapJoinOperator) sel.getParentOperators().get(
          0);
      assert sel.getParentOperators().size() == 1;

      GenMRProcContext ctx = (GenMRProcContext) procCtx;
      ParseContext parseCtx = ctx.getParseCtx();

      // is the mapjoin followed by a reducer
      List<MapJoinOperator> listMapJoinOps = parseCtx
          .getListMapJoinOpsNoReducer();

      if (listMapJoinOps.contains(mapJoin)) {
        ctx.setCurrAliasId(null);
        ctx.setCurrTopOp(null);
        Map<Operator<? extends Serializable>, GenMapRedCtx> mapCurrCtx = ctx
            .getMapCurrCtx();
        mapCurrCtx.put((Operator<? extends Serializable>) nd, new GenMapRedCtx(
            ctx.getCurrTask(), null, null));
        return null;
      }

      ctx.setCurrMapJoinOp(mapJoin);

      Task<? extends Serializable> currTask = ctx.getCurrTask();
      GenMRMapJoinCtx mjCtx = ctx.getMapJoinCtx(mapJoin);
      if (mjCtx == null) {
        mjCtx = new GenMRMapJoinCtx();
        ctx.setMapJoinCtx(mapJoin, mjCtx);
      }

      MapredWork mjPlan = GenMapRedUtils.getMapRedWork();
      Task<? extends Serializable> mjTask = TaskFactory.get(mjPlan, parseCtx
          .getConf());

      TableDesc tt_desc = PlanUtils.getIntermediateFileTableDesc(PlanUtils
          .getFieldSchemasFromRowSchema(mapJoin.getSchema(), "temporarycol"));

      // generate the temporary file
      Context baseCtx = parseCtx.getContext();
      String taskTmpDir = baseCtx.getMRTmpFileURI();

      // Add the path to alias mapping
      mjCtx.setTaskTmpDir(taskTmpDir);
      mjCtx.setTTDesc(tt_desc);
      mjCtx.setRootMapJoinOp(sel);

      sel.setParentOperators(null);

      // Create a file sink operator for this file name
      Operator<? extends Serializable> fs_op = OperatorFactory.get(
          new FileSinkDesc(taskTmpDir, tt_desc, parseCtx.getConf().getBoolVar(
          HiveConf.ConfVars.COMPRESSINTERMEDIATE)), mapJoin.getSchema());

      assert mapJoin.getChildOperators().size() == 1;
      mapJoin.getChildOperators().set(0, fs_op);

      List<Operator<? extends Serializable>> parentOpList = new ArrayList<Operator<? extends Serializable>>();
      parentOpList.add(mapJoin);
      fs_op.setParentOperators(parentOpList);

      currTask.addDependentTask(mjTask);

      ctx.setCurrTask(mjTask);
      ctx.setCurrAliasId(null);
      ctx.setCurrTopOp(null);

      Map<Operator<? extends Serializable>, GenMapRedCtx> mapCurrCtx = ctx
          .getMapCurrCtx();
      mapCurrCtx.put((Operator<? extends Serializable>) nd, new GenMapRedCtx(
          ctx.getCurrTask(), null, null));

      return null;
    }
  }

  /**
   * MapJoin followed by MapJoin.
   */
  public static class MapJoinMapJoin implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      MapJoinOperator mapJoin = (MapJoinOperator) nd;
      GenMRProcContext ctx = (GenMRProcContext) procCtx;

      ctx.getParseCtx();
      MapJoinOperator oldMapJoin = ctx.getCurrMapJoinOp();
      assert oldMapJoin != null;
      GenMRMapJoinCtx mjCtx = ctx.getMapJoinCtx(mapJoin);
      if (mjCtx != null) {
        mjCtx.setOldMapJoin(oldMapJoin);
      } else {
        ctx.setMapJoinCtx(mapJoin, new GenMRMapJoinCtx(null, null, null,
            oldMapJoin));
      }
      ctx.setCurrMapJoinOp(mapJoin);

      // find the branch on which this processor was invoked
      int pos = getPositionParent(mapJoin, stack);

      Map<Operator<? extends Serializable>, GenMapRedCtx> mapCurrCtx = ctx
          .getMapCurrCtx();
      GenMapRedCtx mapredCtx = mapCurrCtx.get(mapJoin.getParentOperators().get(
          pos));
      Task<? extends Serializable> currTask = mapredCtx.getCurrTask();
      MapredWork currPlan = (MapredWork) currTask.getWork();
      mapredCtx.getCurrAliasId();
      Operator<? extends Serializable> reducer = mapJoin;
      HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> opTaskMap = ctx
          .getOpTaskMap();
      Task<? extends Serializable> opMapTask = opTaskMap.get(reducer);

      ctx.setCurrTask(currTask);

      // If the plan for this reducer does not exist, initialize the plan
      if (opMapTask == null) {
        assert currPlan.getReducer() == null;
        GenMapRedUtils.initMapJoinPlan(mapJoin, ctx, true, false, false, pos);
      } else {
        // The current plan can be thrown away after being merged with the
        // original plan
        GenMapRedUtils.joinPlan(mapJoin, currTask, opMapTask, ctx, pos, false,
            true, false);
        currTask = opMapTask;
        ctx.setCurrTask(currTask);
      }

      mapCurrCtx.put(mapJoin, new GenMapRedCtx(ctx.getCurrTask(), null, null));
      return null;
    }
  }

  /**
   * Union followed by MapJoin.
   */
  public static class UnionMapJoin implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      GenMRProcContext ctx = (GenMRProcContext) procCtx;

      ParseContext parseCtx = ctx.getParseCtx();
      UnionProcContext uCtx = parseCtx.getUCtx();

      // union was map only - no special processing needed
      if (uCtx.isMapOnlySubq()) {
        return (new TableScanMapJoin())
            .process(nd, stack, procCtx, nodeOutputs);
      }

      UnionOperator currUnion = ctx.getCurrUnionOp();
      assert currUnion != null;
      ctx.getUnionTask(currUnion);
      MapJoinOperator mapJoin = (MapJoinOperator) nd;

      // find the branch on which this processor was invoked
      int pos = getPositionParent(mapJoin, stack);

      Map<Operator<? extends Serializable>, GenMapRedCtx> mapCurrCtx = ctx
          .getMapCurrCtx();
      GenMapRedCtx mapredCtx = mapCurrCtx.get(mapJoin.getParentOperators().get(
          pos));
      Task<? extends Serializable> currTask = mapredCtx.getCurrTask();
      MapredWork currPlan = (MapredWork) currTask.getWork();
      Operator<? extends Serializable> reducer = mapJoin;
      HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> opTaskMap = ctx
          .getOpTaskMap();
      Task<? extends Serializable> opMapTask = opTaskMap.get(reducer);

      // union result cannot be a map table
      boolean local = (pos == (mapJoin.getConf()).getPosBigTable()) ? false
          : true;
      if (local) {
        throw new SemanticException(ErrorMsg.INVALID_MAPJOIN_TABLE.getMsg());
      }

      // If the plan for this reducer does not exist, initialize the plan
      if (opMapTask == null) {
        assert currPlan.getReducer() == null;
        ctx.setCurrMapJoinOp(mapJoin);
        GenMapRedUtils.initMapJoinPlan(mapJoin, ctx, true, true, false, pos);
        ctx.setCurrUnionOp(null);
      } else {
        // The current plan can be thrown away after being merged with the
        // original plan
        Task<? extends Serializable> uTask = ctx.getUnionTask(
            ctx.getCurrUnionOp()).getUTask();
        if (uTask.getId().equals(opMapTask.getId())) {
          GenMapRedUtils.joinPlan(mapJoin, null, opMapTask, ctx, pos, false,
              false, true);
        } else {
          GenMapRedUtils.joinPlan(mapJoin, uTask, opMapTask, ctx, pos, false,
              false, true);
        }
        currTask = opMapTask;
        ctx.setCurrTask(currTask);
      }

      mapCurrCtx.put(mapJoin, new GenMapRedCtx(ctx.getCurrTask(), ctx
          .getCurrTopOp(), ctx.getCurrAliasId()));
      return null;
    }
  }

  public static NodeProcessor getTableScanMapJoin() {
    return new TableScanMapJoin();
  }

  public static NodeProcessor getUnionMapJoin() {
    return new UnionMapJoin();
  }

  public static NodeProcessor getReduceSinkMapJoin() {
    return new ReduceSinkMapJoin();
  }

  public static NodeProcessor getMapJoin() {
    return new MapJoin();
  }

  public static NodeProcessor getMapJoinMapJoin() {
    return new MapJoinMapJoin();
  }

  private MapJoinFactory() {
    // prevent instantiation
  }
}
