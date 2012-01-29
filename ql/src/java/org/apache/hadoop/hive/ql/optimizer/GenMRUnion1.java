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
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.AbstractMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMRMapJoinCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMRUnionCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMapRedCtx;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcFactory;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext.UnionParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;

/**
 * Processor for the rule - TableScan followed by Union.
 */
public class GenMRUnion1 implements NodeProcessor {

  public GenMRUnion1() {
  }

  /**
   * Process the union if all sub-queries are map-only
   *
   * @return
   * @throws SemanticException
   */
  private Object processMapOnlyUnion(UnionOperator union, Stack<Node> stack,
      GenMRProcContext ctx, UnionProcContext uCtx) throws SemanticException {

    // merge currTask from multiple topOps
    GenMRUnionCtx uCtxTask = ctx.getUnionTask(union);
    if (uCtxTask != null) {
      // get task associated with this union
      Task<? extends Serializable> uTask = ctx.getUnionTask(union).getUTask();
      if (uTask != null) {
        if (ctx.getCurrTask() != null && ctx.getCurrTask() != uTask) {
          // if ctx.getCurrTask() is in rootTasks, should be removed
          ctx.getRootTasks().remove(ctx.getCurrTask());
        }
        ctx.setCurrTask(uTask);
      }
    }

    UnionParseContext uPrsCtx = uCtx.getUnionParseContext(union);
    if ((uPrsCtx != null) && (uPrsCtx.getMapJoinQuery())) {
      GenMapRedUtils.mergeMapJoinUnion(union, ctx,
          UnionProcFactory.getPositionParent(union, stack));
    }
    else {
      ctx.getMapCurrCtx().put(
          (Operator<? extends Serializable>) union,
          new GenMapRedCtx(ctx.getCurrTask(), ctx.getCurrTopOp(),
              ctx.getCurrAliasId()));
    }

    // if the union is the first time seen, set current task to GenMRUnionCtx
    uCtxTask = ctx.getUnionTask(union);
    if (uCtxTask == null) {
      uCtxTask = new GenMRUnionCtx();
      uCtxTask.setUTask(ctx.getCurrTask());
      ctx.setUnionTask(union, uCtxTask);
    }

    Task<? extends Serializable> uTask=ctx.getCurrTask();
    if (uTask.getParentTasks() == null
        || uTask.getParentTasks().isEmpty()) {
      if (!ctx.getRootTasks().contains(uTask)) {
        ctx.getRootTasks().add(uTask);
      }
    }

    return null;
  }

  /**
   * Process the union when the parent is a map-reduce job. Create a temporary
   * output, and let the union task read from the temporary output.
   *
   * The files created for all the inputs are in the union context and later
   * used to initialize the union plan
   *
   * @param parent
   * @param child
   * @param uTask
   * @param ctx
   * @param uCtxTask
   */
  private void processSubQueryUnionCreateIntermediate(
      Operator<? extends Serializable> parent,
      Operator<? extends Serializable> child,
      Task<? extends Serializable> uTask, GenMRProcContext ctx,
      GenMRUnionCtx uCtxTask) {
    ParseContext parseCtx = ctx.getParseCtx();

    TableDesc tt_desc = PlanUtils.getIntermediateFileTableDesc(PlanUtils.getFieldSchemasFromRowSchema(
        parent.getSchema(), "temporarycol"));

    // generate the temporary file
    Context baseCtx = parseCtx.getContext();
    String taskTmpDir = baseCtx.getMRTmpFileURI();

    // Create a file sink operator for this file name
    Operator<? extends Serializable> fs_op = OperatorFactory.get(
        new FileSinkDesc(taskTmpDir, tt_desc, parseCtx.getConf().getBoolVar(
            HiveConf.ConfVars.COMPRESSINTERMEDIATE)), parent.getSchema());

    assert parent.getChildOperators().size() == 1;
    parent.getChildOperators().set(0, fs_op);

    List<Operator<? extends Serializable>> parentOpList = new ArrayList<Operator<? extends Serializable>>();
    parentOpList.add(parent);
    fs_op.setParentOperators(parentOpList);

    // Create a dummy table scan operator
    Operator<? extends Serializable> ts_op = OperatorFactory.get(
        new TableScanDesc(), parent.getSchema());
    List<Operator<? extends Serializable>> childOpList = new ArrayList<Operator<? extends Serializable>>();
    childOpList.add(child);
    ts_op.setChildOperators(childOpList);
    child.replaceParent(parent, ts_op);

    // Add the path to alias mapping

    uCtxTask.addTaskTmpDir(taskTmpDir);
    uCtxTask.addTTDesc(tt_desc);
    uCtxTask.addListTopOperators(ts_op);

    // The union task is empty. The files created for all the inputs are
    // assembled in the union context and later used to initialize the union
    // plan

    Task<? extends Serializable> currTask = ctx.getCurrTask();
    currTask.addDependentTask(uTask);
    if (ctx.getRootTasks().contains(uTask)) {
      ctx.getRootTasks().remove(uTask);
      if (!ctx.getRootTasks().contains(currTask)) {
        ctx.getRootTasks().add(currTask);
      }
    }
  }

  /**
   * Union Operator encountered. A map-only query is encountered at the given
   * position. However, at least one sub-query is a map-reduce job. Copy the
   * information from the current top operator to the union context.
   *
   * @param ctx
   * @param uCtxTask
   * @param union
   * @param stack
   * @throws SemanticException
   */
  private void processSubQueryUnionMerge(GenMRProcContext ctx,
      GenMRUnionCtx uCtxTask, UnionOperator union, Stack<Node> stack)
      throws SemanticException {
    // The current plan can be thrown away after being merged with the union
    // plan
    Task<? extends Serializable> uTask = uCtxTask.getUTask();
    MapredWork plan = (MapredWork) uTask.getWork();
    ctx.setCurrTask(uTask);
    List<Operator<? extends Serializable>> seenOps = ctx.getSeenOps();
    Operator<? extends Serializable> topOp = ctx.getCurrTopOp();
    if (!seenOps.contains(topOp) && topOp != null) {
      seenOps.add(topOp);
      GenMapRedUtils.setTaskPlan(ctx.getCurrAliasId(), ctx
          .getCurrTopOp(), plan, false, ctx);
    }
  }

  private void processSubQueryUnionMapJoin(GenMRProcContext ctx) {
    AbstractMapJoinOperator<? extends MapJoinDesc> mjOp = ctx.getCurrMapJoinOp();
    assert mjOp != null;
    GenMRMapJoinCtx mjCtx = ctx.getMapJoinCtx(mjOp);
    assert mjCtx != null;
    MapredWork plan = (MapredWork) ctx.getCurrTask().getWork();

    String taskTmpDir = mjCtx.getTaskTmpDir();
    TableDesc tt_desc = mjCtx.getTTDesc();
    assert plan.getPathToAliases().get(taskTmpDir) == null;
    plan.getPathToAliases().put(taskTmpDir, new ArrayList<String>());
    plan.getPathToAliases().get(taskTmpDir).add(taskTmpDir);
    plan.getPathToPartitionInfo().put(taskTmpDir,
        new PartitionDesc(tt_desc, null));
    plan.getAliasToWork().put(taskTmpDir, mjCtx.getRootMapJoinOp());
  }

  /**
   * Union Operator encountered . Currently, the algorithm is pretty simple: If
   * all the sub-queries are map-only, don't do anything. However, if there is a
   * mapjoin followed by the union, merge at the union Otherwise, insert a
   * FileSink on top of all the sub-queries.
   *
   * This can be optimized later on.
   *
   * @param nd
   *          the file sink operator encountered
   * @param opProcCtx
   *          context
   */
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx opProcCtx,
      Object... nodeOutputs) throws SemanticException {
    UnionOperator union = (UnionOperator) nd;
    GenMRProcContext ctx = (GenMRProcContext) opProcCtx;
    ParseContext parseCtx = ctx.getParseCtx();
    UnionProcContext uCtx = parseCtx.getUCtx();

    // Map-only subqueries can be optimized in future to not write to a file in
    // future
    Map<Operator<? extends Serializable>, GenMapRedCtx> mapCurrCtx = ctx.getMapCurrCtx();

    UnionParseContext uPrsCtx = uCtx.getUnionParseContext(union);

    ctx.setCurrUnionOp(union);
    // The plan needs to be broken only if one of the sub-queries involve a
    // map-reduce job
    if (uPrsCtx.allMapOnlySubQ()) {
      return processMapOnlyUnion(union, stack, ctx, uCtx);
    }

    assert uPrsCtx != null;

    Task<? extends Serializable> currTask = ctx.getCurrTask();
    int pos = UnionProcFactory.getPositionParent(union, stack);

    Task<? extends Serializable> uTask = null;
    MapredWork uPlan = null;

    // union is encountered for the first time
    GenMRUnionCtx uCtxTask = ctx.getUnionTask(union);
    if (uCtxTask == null) {
      uCtxTask = new GenMRUnionCtx();
      uPlan = GenMapRedUtils.getMapRedWork(parseCtx);
      uTask = TaskFactory.get(uPlan, parseCtx.getConf());
      uCtxTask.setUTask(uTask);
      ctx.setUnionTask(union, uCtxTask);
    }
    else {
      uTask = uCtxTask.getUTask();
    }

    // Copy into the current union task plan if
    if (uPrsCtx.getMapOnlySubq(pos)
        && !uPrsCtx.getMapJoinSubq(pos) && uPrsCtx.getRootTask(pos)) {
      processSubQueryUnionMerge(ctx, uCtxTask, union, stack);
    }
    // If it a map-reduce job, create a temporary file
    else {
      // is the current task a root task
      if (shouldBeRootTask(currTask, parseCtx)
          && (!ctx.getRootTasks().contains(currTask))) {
        ctx.getRootTasks().add(currTask);
      }
      // If there is a mapjoin at position 'pos'
      if (uPrsCtx.getMapJoinSubq(pos)) {
        processSubQueryUnionMapJoin(ctx);
      }

      processSubQueryUnionCreateIntermediate(union.getParentOperators().get(pos), union, uTask, ctx, uCtxTask);
      //the currAliasId and CurrTopOp is not valid any more
      ctx.setCurrAliasId(null);
      ctx.setCurrTopOp(null);
      ctx.getOpTaskMap().put(null, uTask);
    }

    ctx.setCurrTask(uTask);

    mapCurrCtx.put((Operator<? extends Serializable>) nd,
        new GenMapRedCtx(ctx.getCurrTask(), null, null));

    return null;
  }

  private boolean shouldBeRootTask(
      Task<? extends Serializable> currTask, ParseContext parseContext) {
    return currTask.getParentTasks() == null
        || (currTask.getParentTasks().size() == 0);
  }

}
