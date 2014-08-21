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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * Processor for the rule - table scan followed by reduce sink.
 */
public class GenMRFileSink1 implements NodeProcessor {

  static final private Log LOG = LogFactory.getLog(GenMRFileSink1.class.getName());

  public GenMRFileSink1() {
  }

  /**
   * File Sink Operator encountered.
   *
   * @param nd
   *          the file sink operator encountered
   * @param opProcCtx
   *          context
   */
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx opProcCtx,
      Object... nodeOutputs) throws SemanticException {
    GenMRProcContext ctx = (GenMRProcContext) opProcCtx;
    ParseContext parseCtx = ctx.getParseCtx();
    boolean chDir = false;
    Task<? extends Serializable> currTask = ctx.getCurrTask();
    ctx.addRootIfPossible(currTask);

    FileSinkOperator fsOp = (FileSinkOperator) nd;
    boolean isInsertTable = // is INSERT OVERWRITE TABLE
        GenMapRedUtils.isInsertInto(parseCtx, fsOp);
    HiveConf hconf = parseCtx.getConf();

    // Mark this task as a final map reduce task (ignoring the optional merge task)
    ((MapredWork)currTask.getWork()).setFinalMapRed(true);

    // If this file sink desc has been processed due to a linked file sink desc,
    // use that task
    Map<FileSinkDesc, Task<? extends Serializable>> fileSinkDescs = ctx.getLinkedFileDescTasks();
    if (fileSinkDescs != null) {
      Task<? extends Serializable> childTask = fileSinkDescs.get(fsOp.getConf());
      processLinkedFileDesc(ctx, childTask);
      return true;
    }

    // In case of unions or map-joins, it is possible that the file has
    // already been seen.
    // So, no need to attempt to merge the files again.
    if ((ctx.getSeenFileSinkOps() == null)
        || (!ctx.getSeenFileSinkOps().contains(nd))) {
      chDir = GenMapRedUtils.isMergeRequired(ctx.getMvTask(), hconf, fsOp, currTask, isInsertTable);
    }

    Path finalName = processFS(fsOp, stack, opProcCtx, chDir);

    if (chDir) {
      // Merge the files in the destination table/partitions by creating Map-only merge job
      // If underlying data is RCFile or OrcFile, RCFileBlockMerge task or
      // OrcFileStripeMerge task would be created.
      LOG.info("using CombineHiveInputformat for the merge job");
      GenMapRedUtils.createMRWorkForMergingFiles(fsOp, finalName,
          ctx.getDependencyTaskForMultiInsert(), ctx.getMvTask(),
          hconf, currTask);
    }

    FileSinkDesc fileSinkDesc = fsOp.getConf();
    if (fileSinkDesc.isLinkedFileSink()) {
      Map<FileSinkDesc, Task<? extends Serializable>> linkedFileDescTasks =
        ctx.getLinkedFileDescTasks();
      if (linkedFileDescTasks == null) {
        linkedFileDescTasks = new HashMap<FileSinkDesc, Task<? extends Serializable>>();
        ctx.setLinkedFileDescTasks(linkedFileDescTasks);
      }

      // The child tasks may be null in case of a select
      if ((currTask.getChildTasks() != null) &&
        (currTask.getChildTasks().size() == 1)) {
        for (FileSinkDesc fileDesc : fileSinkDesc.getLinkedFileSinkDesc()) {
          linkedFileDescTasks.put(fileDesc, currTask.getChildTasks().get(0));
        }
      }
    }

    FetchTask fetchTask = parseCtx.getFetchTask();
    if (fetchTask != null && currTask.getNumChild() == 0) {
      if (fetchTask.isFetchFrom(fileSinkDesc)) {
        currTask.setFetchSource(true);
      }
    }
    return true;
  }

  /*
   * Multiple file sink descriptors are linked.
   * Use the task created by the first linked file descriptor
   */
  private void processLinkedFileDesc(GenMRProcContext ctx,
      Task<? extends Serializable> childTask) throws SemanticException {
    Task<? extends Serializable> currTask = ctx.getCurrTask();
    Operator<? extends OperatorDesc> currTopOp = ctx.getCurrTopOp();
    if (currTopOp != null && !ctx.isSeenOp(currTask, currTopOp)) {
      String currAliasId = ctx.getCurrAliasId();
      GenMapRedUtils.setTaskPlan(currAliasId, currTopOp, currTask, false, ctx);
    }

    if (childTask != null) {
      currTask.addDependentTask(childTask);
    }
  }

  /**
   * Process the FileSink operator to generate a MoveTask if necessary.
   *
   * @param fsOp
   *          current FileSink operator
   * @param stack
   *          parent operators
   * @param opProcCtx
   * @param chDir
   *          whether the operator should be first output to a tmp dir and then merged
   *          to the final dir later
   * @return the final file name to which the FileSinkOperator should store.
   * @throws SemanticException
   */
  private Path processFS(FileSinkOperator fsOp, Stack<Node> stack,
      NodeProcessorCtx opProcCtx, boolean chDir) throws SemanticException {

    GenMRProcContext ctx = (GenMRProcContext) opProcCtx;
    Task<? extends Serializable> currTask = ctx.getCurrTask();

    // If the directory needs to be changed, send the new directory
    Path dest = null;

    List<FileSinkOperator> seenFSOps = ctx.getSeenFileSinkOps();
    if (seenFSOps == null) {
      seenFSOps = new ArrayList<FileSinkOperator>();
    }
    if (!seenFSOps.contains(fsOp)) {
      seenFSOps.add(fsOp);
    }
    ctx.setSeenFileSinkOps(seenFSOps);

    dest = GenMapRedUtils.createMoveTask(ctx.getCurrTask(), chDir, fsOp, ctx.getParseCtx(),
        ctx.getMvTask(), ctx.getConf(), ctx.getDependencyTaskForMultiInsert());

    Operator<? extends OperatorDesc> currTopOp = ctx.getCurrTopOp();
    String currAliasId = ctx.getCurrAliasId();
    HashMap<Operator<? extends OperatorDesc>, Task<? extends Serializable>> opTaskMap =
        ctx.getOpTaskMap();

    // In case of multi-table insert, the path to alias mapping is needed for
    // all the sources. Since there is no
    // reducer, treat it as a plan with null reducer
    // If it is a map-only job, the task needs to be processed
    if (currTopOp != null) {
      Task<? extends Serializable> mapTask = opTaskMap.get(null);
      if (mapTask == null) {
        if (!ctx.isSeenOp(currTask, currTopOp)) {
          GenMapRedUtils.setTaskPlan(currAliasId, currTopOp, currTask, false, ctx);
        }
        opTaskMap.put(null, currTask);
      } else {
        if (!ctx.isSeenOp(currTask, currTopOp)) {
          GenMapRedUtils.setTaskPlan(currAliasId, currTopOp, mapTask, false, ctx);
        } else {
          UnionOperator currUnionOp = ctx.getCurrUnionOp();
          if (currUnionOp != null) {
            opTaskMap.put(null, currTask);
            ctx.setCurrTopOp(null);
            GenMapRedUtils.initUnionPlan(ctx, currUnionOp, currTask, false);
            return dest;
          }
        }
        // mapTask and currTask should be merged by and join/union operator
        // (e.g., GenMRUnion1) which has multiple topOps.
        // assert mapTask == currTask : "mapTask.id = " + mapTask.getId()
        // + "; currTask.id = " + currTask.getId();
      }

      return dest;

    }

    UnionOperator currUnionOp = ctx.getCurrUnionOp();

    if (currUnionOp != null) {
      opTaskMap.put(null, currTask);
      GenMapRedUtils.initUnionPlan(ctx, currUnionOp, currTask, false);
      return dest;
    }

    return dest;
  }
}
