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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.AbstractMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMapRedCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BucketMapJoinContext;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * Operator factory for MapJoin processing.
 */
public final class MapJoinFactory {

  public static int getPositionParent(AbstractMapJoinOperator<? extends MapJoinDesc> op,
      Stack<Node> stack) {
    int pos = 0;
    int size = stack.size();
    assert size >= 2 && stack.get(size - 1) == op;
    Operator<? extends OperatorDesc> parent =
        (Operator<? extends OperatorDesc>) stack.get(size - 2);
    List<Operator<? extends OperatorDesc>> parOp = op.getParentOperators();
    pos = parOp.indexOf(parent);
    assert pos < parOp.size();
    return pos;
  }

  /**
   * MapJoin processor.
   * The user can specify a mapjoin hint to specify that the input should be processed as a
   * mapjoin instead of map-reduce join. If hive.auto.convert.join is set to true, the
   * user need not specify the hint explicitly, but hive will automatically process the joins
   * as a mapjoin whenever possible. However, a join can only be processed as a bucketized
   * map-side join or a sort merge join, if the user has provided the hint explicitly. This
   * will be fixed as part of HIVE-3433, and eventually, we should remove support for mapjoin
   * hint.
   * However, currently, the mapjoin hint is processed as follows:
   * A mapjoin will have 'n' parents for a n-way mapjoin, and therefore the mapjoin operator
   * will be encountered 'n' times (one for each parent). Since a reduceSink operator is not
   * allowed before a mapjoin, the task for the mapjoin will always be a root task. The task
   * corresponding to the mapjoin is converted to a root task when the operator is encountered
   * for the first time. When the operator is encountered subsequently, the current task is
   * merged with the root task for the mapjoin. Note that, it is possible that the map-join task
   * may be performed as a bucketized map-side join (or sort-merge join), the map join operator
   * is enhanced to contain the bucketing info. when it is encountered.
   */
  private static class TableScanMapJoinProcessor implements NodeProcessor {

    public static void setupBucketMapJoinInfo(MapWork plan,
        AbstractMapJoinOperator<? extends MapJoinDesc> currMapJoinOp) {
      if (currMapJoinOp != null) {
        Map<String, Map<String, List<String>>> aliasBucketFileNameMapping =
            currMapJoinOp.getConf().getAliasBucketFileNameMapping();
        if (aliasBucketFileNameMapping != null) {
          MapredLocalWork localPlan = plan.getMapLocalWork();
          if (localPlan == null) {
            if (currMapJoinOp instanceof SMBMapJoinOperator) {
              localPlan = ((SMBMapJoinOperator) currMapJoinOp).getConf().getLocalWork();
            }
          } else {
            // local plan is not null, we want to merge it into SMBMapJoinOperator's local work
            if (currMapJoinOp instanceof SMBMapJoinOperator) {
              MapredLocalWork smbLocalWork = ((SMBMapJoinOperator) currMapJoinOp).getConf()
                  .getLocalWork();
              if (smbLocalWork != null) {
                localPlan.getAliasToFetchWork().putAll(smbLocalWork.getAliasToFetchWork());
                localPlan.getAliasToWork().putAll(smbLocalWork.getAliasToWork());
              }
            }
          }

          if (localPlan == null) {
            return;
          }

          if (currMapJoinOp instanceof SMBMapJoinOperator) {
            plan.setMapLocalWork(null);
            ((SMBMapJoinOperator) currMapJoinOp).getConf().setLocalWork(localPlan);
          } else {
            plan.setMapLocalWork(localPlan);
          }
          BucketMapJoinContext bucketMJCxt = new BucketMapJoinContext();
          localPlan.setBucketMapjoinContext(bucketMJCxt);
          bucketMJCxt.setAliasBucketFileNameMapping(aliasBucketFileNameMapping);
          bucketMJCxt.setBucketFileNameMapping(
              currMapJoinOp.getConf().getBigTableBucketNumMapping());
          localPlan.setInputFileChangeSensitive(true);
          bucketMJCxt.setMapJoinBigTableAlias(currMapJoinOp.getConf().getBigTableAlias());
          bucketMJCxt
              .setBucketMatcherClass(org.apache.hadoop.hive.ql.exec.DefaultBucketMatcher.class);
          bucketMJCxt.setBigTablePartSpecToFileMapping(
              currMapJoinOp.getConf().getBigTablePartSpecToFileMapping());
          // BucketizedHiveInputFormat should be used for either sort merge join or bucket map join
          if ((currMapJoinOp instanceof SMBMapJoinOperator)
              || (currMapJoinOp.getConf().isBucketMapJoin())) {
            plan.setUseBucketizedHiveInputFormat(true);
          }
        }
      }
    }

    /**
     * Initialize the current plan by adding it to root tasks. Since a reduce sink
     * cannot be present before a mapjoin, and the mapjoin operator is encountered
     * for the first time, the task corresposding to the mapjoin is added to the
     * root tasks.
     *
     * @param op
     *          the map join operator encountered
     * @param opProcCtx
     *          processing context
     * @param pos
     *          position of the parent
     */
    private static void initMapJoinPlan(AbstractMapJoinOperator<? extends MapJoinDesc> op,
        Task<? extends Serializable> currTask,
        GenMRProcContext opProcCtx, boolean local)
        throws SemanticException {

      // The map is overloaded to keep track of mapjoins also
      opProcCtx.getOpTaskMap().put(op, currTask);

      Operator<? extends OperatorDesc> currTopOp = opProcCtx.getCurrTopOp();
      String currAliasId = opProcCtx.getCurrAliasId();
      GenMapRedUtils.setTaskPlan(currAliasId, currTopOp, currTask, local, opProcCtx);
    }

    /**
     * Merge the current task with the task for the current mapjoin. The mapjoin operator
     * has already been encountered.
     *
     * @param op
     *          operator being processed
     * @param oldTask
     *          the old task for the current mapjoin
     * @param opProcCtx
     *          processing context
     * @param pos
     *          position of the parent in the stack
     */
    private static void joinMapJoinPlan(AbstractMapJoinOperator<? extends MapJoinDesc> op,
        Task<? extends Serializable> oldTask,
        GenMRProcContext opProcCtx, boolean local)
        throws SemanticException {
      Operator<? extends OperatorDesc> currTopOp = opProcCtx.getCurrTopOp();
      GenMapRedUtils.mergeInput(currTopOp, opProcCtx, oldTask, local);
    }

    /*
     * The mapjoin operator will be encountered many times (n times for a n-way join). Since a
     * reduceSink operator is not allowed before a mapjoin, the task for the mapjoin will always
     * be a root task. The task corresponding to the mapjoin is converted to a root task when the
     * operator is encountered for the first time. When the operator is encountered subsequently,
     * the current task is merged with the root task for the mapjoin. Note that, it is possible
     * that the map-join task may be performed as a bucketized map-side join (or sort-merge join),
     * the map join operator is enhanced to contain the bucketing info. when it is encountered.
     */
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      AbstractMapJoinOperator<MapJoinDesc> mapJoin = (AbstractMapJoinOperator<MapJoinDesc>) nd;
      GenMRProcContext ctx = (GenMRProcContext) procCtx;

      // find the branch on which this processor was invoked
      int pos = getPositionParent(mapJoin, stack);

      Map<Operator<? extends OperatorDesc>, GenMapRedCtx> mapCurrCtx = ctx
          .getMapCurrCtx();
      GenMapRedCtx mapredCtx = mapCurrCtx.get(mapJoin.getParentOperators().get(pos));
      Task<? extends Serializable> currTask = mapredCtx.getCurrTask();
      MapredWork currPlan = (MapredWork) currTask.getWork();
      String currAliasId = mapredCtx.getCurrAliasId();
      HashMap<Operator<? extends OperatorDesc>, Task<? extends Serializable>> opTaskMap =
          ctx.getOpTaskMap();
      Task<? extends Serializable> oldTask = opTaskMap.get(mapJoin);

      ctx.setCurrAliasId(currAliasId);
      ctx.setCurrTask(currTask);

      // If we are seeing this mapjoin for the first time, initialize the plan.
      // If we are seeing this mapjoin for the second or later time then atleast one of the
      // branches for this mapjoin have been encounered. Join the plan with the plan created
      // the first time.
      boolean local = pos != mapJoin.getConf().getPosBigTable();
      if (oldTask == null) {
        assert currPlan.getReduceWork() == null;
        initMapJoinPlan(mapJoin, currTask, ctx, local);
      } else {
        // The current plan can be thrown away after being merged with the
        // original plan
        joinMapJoinPlan(mapJoin, oldTask, ctx, local);
        ctx.setCurrTask(currTask = oldTask);
      }
      MapredWork plan = (MapredWork) currTask.getWork();
      setupBucketMapJoinInfo(plan.getMapWork(), mapJoin);

      mapCurrCtx.put(mapJoin, new GenMapRedCtx(ctx.getCurrTask(), ctx.getCurrAliasId()));

      // local aliases need not to hand over context further
      return !local;
    }
  }

  public static NodeProcessor getTableScanMapJoin() {
    return new TableScanMapJoinProcessor();
  }

  private MapJoinFactory() {
    // prevent instantiation
  }
}
