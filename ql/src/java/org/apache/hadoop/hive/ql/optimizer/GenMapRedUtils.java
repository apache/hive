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
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.AbstractMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMRMapJoinCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMRUnionCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMapRedCtx;
import org.apache.hadoop.hive.ql.optimizer.listbucketingpruner.ListBucketingPruner;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext.UnionParseContext;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BucketMapJoinContext;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc.sampleDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;

/**
 * General utility common functions for the Processor to convert operator into
 * map-reduce tasks.
 */
public final class GenMapRedUtils {
  private static Log LOG;

  static {
    LOG = LogFactory.getLog("org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils");
  }

  /**
   * Initialize the current plan by adding it to root tasks.
   *
   * @param op
   *          the reduce sink operator encountered
   * @param opProcCtx
   *          processing context
   */
  public static void initPlan(ReduceSinkOperator op, GenMRProcContext opProcCtx)
      throws SemanticException {
    Operator<? extends OperatorDesc> reducer = op.getChildOperators().get(0);
    Map<Operator<? extends OperatorDesc>, GenMapRedCtx> mapCurrCtx =
      opProcCtx.getMapCurrCtx();
    GenMapRedCtx mapredCtx = mapCurrCtx.get(op.getParentOperators().get(0));
    Task<? extends Serializable> currTask = mapredCtx.getCurrTask();
    MapredWork plan = (MapredWork) currTask.getWork();
    HashMap<Operator<? extends OperatorDesc>, Task<? extends Serializable>> opTaskMap =
      opProcCtx.getOpTaskMap();
    Operator<? extends OperatorDesc> currTopOp = opProcCtx.getCurrTopOp();

    opTaskMap.put(reducer, currTask);
    plan.setReducer(reducer);
    ReduceSinkDesc desc = op.getConf();

    plan.setNumReduceTasks(desc.getNumReducers());

    List<Task<? extends Serializable>> rootTasks = opProcCtx.getRootTasks();

    if (!rootTasks.contains(currTask)) {
        rootTasks.add(currTask);
    }
    if (reducer.getClass() == JoinOperator.class) {
      plan.setNeedsTagging(true);
    }

    assert currTopOp != null;
    List<Operator<? extends OperatorDesc>> seenOps = opProcCtx.getSeenOps();
    String currAliasId = opProcCtx.getCurrAliasId();

    if (!seenOps.contains(currTopOp)) {
      seenOps.add(currTopOp);
      setTaskPlan(currAliasId, currTopOp, plan, false, opProcCtx);
    }

    currTopOp = null;
    currAliasId = null;

    opProcCtx.setCurrTask(currTask);
    opProcCtx.setCurrTopOp(currTopOp);
    opProcCtx.setCurrAliasId(currAliasId);
  }

  public static void initMapJoinPlan(
    Operator<? extends OperatorDesc> op, GenMRProcContext ctx,
    boolean readInputMapJoin, boolean readInputUnion, boolean setReducer, int pos)
    throws SemanticException {
    initMapJoinPlan(op, ctx, readInputMapJoin, readInputUnion, setReducer, pos, false);
  }

  /**
   * Initialize the current plan by adding it to root tasks.
   *
   * @param op
   *          the map join operator encountered
   * @param opProcCtx
   *          processing context
   * @param pos
   *          position of the parent
   */
  public static void initMapJoinPlan(Operator<? extends OperatorDesc> op,
      GenMRProcContext opProcCtx, boolean readInputMapJoin,
      boolean readInputUnion, boolean setReducer, int pos, boolean createLocalPlan)
      throws SemanticException {
    Map<Operator<? extends OperatorDesc>, GenMapRedCtx> mapCurrCtx =
      opProcCtx.getMapCurrCtx();
    assert (((pos == -1) && (readInputMapJoin)) || (pos != -1));
    int parentPos = (pos == -1) ? 0 : pos;
    GenMapRedCtx mapredCtx = mapCurrCtx.get(op.getParentOperators().get(
        parentPos));
    Task<? extends Serializable> currTask = mapredCtx.getCurrTask();
    MapredWork plan = (MapredWork) currTask.getWork();
    HashMap<Operator<? extends OperatorDesc>, Task<? extends Serializable>>  opTaskMap =
      opProcCtx.getOpTaskMap();
    Operator<? extends OperatorDesc> currTopOp = opProcCtx.getCurrTopOp();

    // The mapjoin has already been encountered. Some context must be stored
    // about that
    if (readInputMapJoin) {
      AbstractMapJoinOperator<? extends MapJoinDesc> currMapJoinOp = opProcCtx.getCurrMapJoinOp();
      assert currMapJoinOp != null;
      boolean local = ((pos == -1) || (pos == (currMapJoinOp.getConf()).getPosBigTable())) ?
          false : true;

      if (setReducer) {
        Operator<? extends OperatorDesc> reducer = op.getChildOperators().get(0);
        plan.setReducer(reducer);
        opTaskMap.put(reducer, currTask);
        if (reducer.getClass() == JoinOperator.class) {
          plan.setNeedsTagging(true);
        }
        ReduceSinkDesc desc = (ReduceSinkDesc) op.getConf();
        plan.setNumReduceTasks(desc.getNumReducers());
      } else {
        opTaskMap.put(op, currTask);
      }

      if (!readInputUnion) {
        GenMRMapJoinCtx mjCtx = opProcCtx.getMapJoinCtx(currMapJoinOp);
        String taskTmpDir;
        TableDesc tt_desc;
        Operator<? extends OperatorDesc> rootOp;

        if (mjCtx.getOldMapJoin() == null || setReducer) {
          taskTmpDir = mjCtx.getTaskTmpDir();
          tt_desc = mjCtx.getTTDesc();
          rootOp = mjCtx.getRootMapJoinOp();
        } else {
          GenMRMapJoinCtx oldMjCtx = opProcCtx.getMapJoinCtx(mjCtx
              .getOldMapJoin());
          taskTmpDir = oldMjCtx.getTaskTmpDir();
          tt_desc = oldMjCtx.getTTDesc();
          rootOp = oldMjCtx.getRootMapJoinOp();
        }

        setTaskPlan(taskTmpDir, taskTmpDir, rootOp, plan, local, tt_desc);
        setupBucketMapJoinInfo(plan, currMapJoinOp, createLocalPlan);
      } else {
        initUnionPlan(opProcCtx, currTask, false);
      }

      opProcCtx.setCurrMapJoinOp(null);
    } else {
      MapJoinDesc desc = (MapJoinDesc) op.getConf();

      // The map is overloaded to keep track of mapjoins also
      opTaskMap.put(op, currTask);

      List<Task<? extends Serializable>> rootTasks = opProcCtx.getRootTasks();
      if (!rootTasks.contains(currTask)) {
        rootTasks.add(currTask);
      }

      assert currTopOp != null;
      List<Operator<? extends OperatorDesc>> seenOps = opProcCtx.getSeenOps();
      String currAliasId = opProcCtx.getCurrAliasId();

      seenOps.add(currTopOp);
      boolean local = (pos == desc.getPosBigTable()) ? false : true;
      setTaskPlan(currAliasId, currTopOp, plan, local, opProcCtx);
      setupBucketMapJoinInfo(plan, (AbstractMapJoinOperator<? extends MapJoinDesc>)op, createLocalPlan);
    }

    opProcCtx.setCurrTask(currTask);
    opProcCtx.setCurrTopOp(null);
    opProcCtx.setCurrAliasId(null);
  }

  private static void setupBucketMapJoinInfo(MapredWork plan,
      AbstractMapJoinOperator<? extends MapJoinDesc> currMapJoinOp, boolean createLocalPlan) {
    if (currMapJoinOp != null) {
      Map<String, Map<String, List<String>>> aliasBucketFileNameMapping =
        currMapJoinOp.getConf().getAliasBucketFileNameMapping();
      if(aliasBucketFileNameMapping!= null) {
        MapredLocalWork localPlan = plan.getMapLocalWork();
        if(localPlan == null) {
          if(currMapJoinOp instanceof SMBMapJoinOperator) {
            localPlan = ((SMBMapJoinOperator)currMapJoinOp).getConf().getLocalWork();
          }
          if (localPlan == null && createLocalPlan) {
            localPlan = new MapredLocalWork(
                new LinkedHashMap<String, Operator<? extends OperatorDesc>>(),
                new LinkedHashMap<String, FetchWork>());
          }
        } else {
          //local plan is not null, we want to merge it into SMBMapJoinOperator's local work
          if(currMapJoinOp instanceof SMBMapJoinOperator) {
            MapredLocalWork smbLocalWork = ((SMBMapJoinOperator)currMapJoinOp).getConf().getLocalWork();
            if(smbLocalWork != null) {
              localPlan.getAliasToFetchWork().putAll(smbLocalWork.getAliasToFetchWork());
              localPlan.getAliasToWork().putAll(smbLocalWork.getAliasToWork());
            }
          }
        }

        if(localPlan == null) {
          return;
        }

        if(currMapJoinOp instanceof SMBMapJoinOperator) {
          plan.setMapLocalWork(null);
          ((SMBMapJoinOperator)currMapJoinOp).getConf().setLocalWork(localPlan);
        } else {
          plan.setMapLocalWork(localPlan);
        }
        BucketMapJoinContext bucketMJCxt = new BucketMapJoinContext();
        localPlan.setBucketMapjoinContext(bucketMJCxt);
        bucketMJCxt.setAliasBucketFileNameMapping(aliasBucketFileNameMapping);
        bucketMJCxt.setBucketFileNameMapping(currMapJoinOp.getConf().getBigTableBucketNumMapping());
        localPlan.setInputFileChangeSensitive(true);
        bucketMJCxt.setMapJoinBigTableAlias(currMapJoinOp.getConf().getBigTableAlias());
        bucketMJCxt.setBucketMatcherClass(org.apache.hadoop.hive.ql.exec.DefaultBucketMatcher.class);
        bucketMJCxt.setBigTablePartSpecToFileMapping(
          currMapJoinOp.getConf().getBigTablePartSpecToFileMapping());
        plan.setUseBucketizedHiveInputFormat(currMapJoinOp instanceof SMBMapJoinOperator);
      }
    }
  }

  /**
   * Initialize the current union plan.
   *
   * @param op
   *          the reduce sink operator encountered
   * @param opProcCtx
   *          processing context
   */
  public static void initUnionPlan(ReduceSinkOperator op,
      GenMRProcContext opProcCtx,
      Task<? extends Serializable> unionTask) throws SemanticException {
    Operator<? extends OperatorDesc> reducer = op.getChildOperators().get(0);

    MapredWork plan = (MapredWork) unionTask.getWork();
    HashMap<Operator<? extends OperatorDesc>, Task<? extends Serializable>> opTaskMap =
      opProcCtx.getOpTaskMap();

    opTaskMap.put(reducer, unionTask);
    plan.setReducer(reducer);
    ReduceSinkDesc desc = op.getConf();

    plan.setNumReduceTasks(desc.getNumReducers());

    if (reducer.getClass() == JoinOperator.class) {
      plan.setNeedsTagging(true);
    }

    initUnionPlan(opProcCtx, unionTask, false);
  }

  private static void setUnionPlan(GenMRProcContext opProcCtx,
      boolean local, MapredWork plan, GenMRUnionCtx uCtx,
      boolean mergeTask) throws SemanticException {
    Operator<? extends OperatorDesc> currTopOp = opProcCtx.getCurrTopOp();

    if (currTopOp != null) {
      List<Operator<? extends OperatorDesc>> seenOps = opProcCtx.getSeenOps();
      String currAliasId = opProcCtx.getCurrAliasId();
      if (!seenOps.contains(currTopOp) || mergeTask) {
        seenOps.add(currTopOp);
        setTaskPlan(currAliasId, currTopOp, plan, local, opProcCtx);
      }
      currTopOp = null;
      opProcCtx.setCurrTopOp(currTopOp);
    } else {
      List<String> taskTmpDirLst = uCtx.getTaskTmpDir();
      if ((taskTmpDirLst != null) && !(taskTmpDirLst.isEmpty())) {
        List<TableDesc> tt_descLst = uCtx.getTTDesc();
        assert !taskTmpDirLst.isEmpty() && !tt_descLst.isEmpty();
        assert taskTmpDirLst.size() == tt_descLst.size();
        int size = taskTmpDirLst.size();
        assert local == false;

        List<Operator<? extends OperatorDesc>> topOperators =
            uCtx.getListTopOperators();

        for (int pos = 0; pos < size; pos++) {
          String taskTmpDir = taskTmpDirLst.get(pos);
          TableDesc tt_desc = tt_descLst.get(pos);
          if (plan.getPathToAliases().get(taskTmpDir) == null) {
            plan.getPathToAliases().put(taskTmpDir,
                new ArrayList<String>());
            plan.getPathToAliases().get(taskTmpDir).add(taskTmpDir);
            plan.getPathToPartitionInfo().put(taskTmpDir,
                new PartitionDesc(tt_desc, null));
            plan.getAliasToWork().put(taskTmpDir, topOperators.get(pos));
          }
        }
      }
    }
  }

  /*
   * It is a idempotent function to add various intermediate files as the source
   * for the union. The plan has already been created.
   */
  public static void initUnionPlan(GenMRProcContext opProcCtx,
      Task<? extends Serializable> currTask, boolean local)
      throws SemanticException {
    MapredWork plan = (MapredWork) currTask.getWork();
    UnionOperator currUnionOp = opProcCtx.getCurrUnionOp();
    // In case of lateral views followed by a join, the same tree
    // can be traversed more than one
    if (currUnionOp != null) {
      GenMRUnionCtx uCtx = opProcCtx.getUnionTask(currUnionOp);
      assert uCtx != null;
      setUnionPlan(opProcCtx, local, plan, uCtx, false);
    }
  }

  /*
   * join current union task to old task
   */
  public static void joinUnionPlan(GenMRProcContext opProcCtx,
      Task<? extends Serializable> currentUnionTask,
      Task<? extends Serializable> existingTask, boolean local)
      throws SemanticException {
    MapredWork plan = (MapredWork) existingTask.getWork();
    UnionOperator currUnionOp = opProcCtx.getCurrUnionOp();
    assert currUnionOp != null;
    GenMRUnionCtx uCtx = opProcCtx.getUnionTask(currUnionOp);
    assert uCtx != null;

    setUnionPlan(opProcCtx, local, plan, uCtx, true);

    List<Task<? extends Serializable>> parTasks = null;
    if (opProcCtx.getRootTasks().contains(currentUnionTask)) {
      opProcCtx.getRootTasks().remove(currentUnionTask);
      if (!opProcCtx.getRootTasks().contains(existingTask) &&
          (existingTask.getParentTasks() == null || existingTask.getParentTasks().isEmpty())) {
        opProcCtx.getRootTasks().add(existingTask);
      }
    }

    if ((currentUnionTask != null) && (currentUnionTask.getParentTasks() != null)
        && !currentUnionTask.getParentTasks().isEmpty()) {
      parTasks = new ArrayList<Task<? extends Serializable>>();
      parTasks.addAll(currentUnionTask.getParentTasks());
      Object[] parTaskArr = parTasks.toArray();
      for (Object parTask : parTaskArr) {
        ((Task<? extends Serializable>) parTask)
            .removeDependentTask(currentUnionTask);
      }
    }

    if ((currentUnionTask != null) && (parTasks != null)) {
      for (Task<? extends Serializable> parTask : parTasks) {
        parTask.addDependentTask(existingTask);
        if (opProcCtx.getRootTasks().contains(existingTask)) {
          opProcCtx.getRootTasks().remove(existingTask);
        }
      }
    }

    opProcCtx.setCurrTask(existingTask);
  }

  public static void joinPlan(Operator<? extends OperatorDesc> op,
      Task<? extends Serializable> oldTask, Task<? extends Serializable> task,
      GenMRProcContext opProcCtx, int pos, boolean split,
      boolean readMapJoinData, boolean readUnionData) throws SemanticException {
    joinPlan(op, oldTask, task, opProcCtx, pos, split, readMapJoinData, readUnionData, false);
  }

  /**
   * Merge the current task with the task for the current reducer.
   *
   * @param op
   *          operator being processed
   * @param oldTask
   *          the old task for the current reducer
   * @param task
   *          the current task for the current reducer
   * @param opProcCtx
   *          processing context
   * @param pos
   *          position of the parent in the stack
   */
  public static void joinPlan(Operator<? extends OperatorDesc> op,
      Task<? extends Serializable> oldTask, Task<? extends Serializable> task,
      GenMRProcContext opProcCtx, int pos, boolean split,
      boolean readMapJoinData, boolean readUnionData, boolean createLocalWork)
      throws SemanticException {
    Task<? extends Serializable> currTask = task;
    MapredWork plan = (MapredWork) currTask.getWork();
    Operator<? extends OperatorDesc> currTopOp = opProcCtx.getCurrTopOp();
    List<Task<? extends Serializable>> parTasks = null;

    // terminate the old task and make current task dependent on it
    if (split) {
      assert oldTask != null;
      splitTasks(op, oldTask, currTask, opProcCtx, true, false, 0);
    } else {
      if ((oldTask != null) && (oldTask.getParentTasks() != null)
          && !oldTask.getParentTasks().isEmpty()) {
        parTasks = new ArrayList<Task<? extends Serializable>>();
        parTasks.addAll(oldTask.getParentTasks());

        Object[] parTaskArr = parTasks.toArray();
        for (Object element : parTaskArr) {
          ((Task<? extends Serializable>) element).removeDependentTask(oldTask);
        }
      }
    }

    if (currTopOp != null) {
      List<Operator<? extends OperatorDesc>> seenOps = opProcCtx.getSeenOps();
      String currAliasId = opProcCtx.getCurrAliasId();

      if (!seenOps.contains(currTopOp)) {
        seenOps.add(currTopOp);
        boolean local = false;
        if (pos != -1) {
          local = (pos == ((MapJoinDesc) op.getConf()).getPosBigTable()) ? false
              : true;
        }
        setTaskPlan(currAliasId, currTopOp, plan, local, opProcCtx);
        if(op instanceof AbstractMapJoinOperator) {
          setupBucketMapJoinInfo(plan, (AbstractMapJoinOperator<? extends MapJoinDesc>)op, createLocalWork);
        }
      }
      currTopOp = null;
      opProcCtx.setCurrTopOp(currTopOp);
    } else if (opProcCtx.getCurrMapJoinOp() != null) {
      AbstractMapJoinOperator<? extends MapJoinDesc> mjOp = opProcCtx.getCurrMapJoinOp();
      if (readUnionData) {
        initUnionPlan(opProcCtx, currTask, false);
      } else {
        GenMRMapJoinCtx mjCtx = opProcCtx.getMapJoinCtx(mjOp);

        // In case of map-join followed by map-join, the file needs to be
        // obtained from the old map join
        AbstractMapJoinOperator<? extends MapJoinDesc> oldMapJoin = mjCtx.getOldMapJoin();
        String taskTmpDir = null;
        TableDesc tt_desc = null;
        Operator<? extends OperatorDesc> rootOp = null;

        boolean local = ((pos == -1) || (pos == (mjOp.getConf())
            .getPosBigTable())) ? false : true;
        if (oldMapJoin == null) {
          if (opProcCtx.getParseCtx().getListMapJoinOpsNoReducer().contains(mjOp)
              || local || (oldTask != null) && (parTasks != null)) {
            taskTmpDir = mjCtx.getTaskTmpDir();
            tt_desc = mjCtx.getTTDesc();
            rootOp = mjCtx.getRootMapJoinOp();
          }
        } else {
          GenMRMapJoinCtx oldMjCtx = opProcCtx.getMapJoinCtx(oldMapJoin);
          assert oldMjCtx != null;
          taskTmpDir = oldMjCtx.getTaskTmpDir();
          tt_desc = oldMjCtx.getTTDesc();
          rootOp = oldMjCtx.getRootMapJoinOp();
        }

        setTaskPlan(taskTmpDir, taskTmpDir, rootOp, plan, local, tt_desc);
        setupBucketMapJoinInfo(plan, oldMapJoin, createLocalWork);
      }
      opProcCtx.setCurrMapJoinOp(null);
    }

    if ((oldTask != null) && (parTasks != null)) {
      for (Task<? extends Serializable> parTask : parTasks) {
        parTask.addDependentTask(currTask);
        if(opProcCtx.getRootTasks().contains(currTask)) {
          opProcCtx.getRootTasks().remove(currTask);
        }
      }
    }

    opProcCtx.setCurrTask(currTask);
  }

  /**
   * Split the current plan by creating a temporary destination.
   *
   * @param op
   *          the reduce sink operator encountered
   * @param opProcCtx
   *          processing context
   */
  public static void splitPlan(ReduceSinkOperator op, GenMRProcContext opProcCtx)
  throws SemanticException {
    // Generate a new task
    ParseContext parseCtx = opProcCtx.getParseCtx();
    MapredWork cplan = getMapRedWork(parseCtx);
    Task<? extends Serializable> redTask = TaskFactory.get(cplan, parseCtx
        .getConf());
    Operator<? extends OperatorDesc> reducer = op.getChildOperators().get(0);

    // Add the reducer
    cplan.setReducer(reducer);
    ReduceSinkDesc desc = op.getConf();

    cplan.setNumReduceTasks(new Integer(desc.getNumReducers()));

    HashMap<Operator<? extends OperatorDesc>, Task<? extends Serializable>> opTaskMap =
      opProcCtx.getOpTaskMap();
    opTaskMap.put(reducer, redTask);
    Task<? extends Serializable> currTask = opProcCtx.getCurrTask();

    splitTasks(op, currTask, redTask, opProcCtx, true, false, 0);
    opProcCtx.getRootOps().add(op);
  }

  /**
   * set the current task in the mapredWork.
   *
   * @param alias_id
   *          current alias
   * @param topOp
   *          the top operator of the stack
   * @param plan
   *          current plan
   * @param local
   *          whether you need to add to map-reduce or local work
   * @param opProcCtx
   *          processing context
   */
  public static void setTaskPlan(String alias_id,
      Operator<? extends OperatorDesc> topOp, MapredWork plan, boolean local,
      GenMRProcContext opProcCtx) throws SemanticException {
    setTaskPlan(alias_id, topOp, plan, local, opProcCtx, null);
  }

  /**
   * set the current task in the mapredWork.
   *
   * @param alias_id
   *          current alias
   * @param topOp
   *          the top operator of the stack
   * @param plan
   *          current plan
   * @param local
   *          whether you need to add to map-reduce or local work
   * @param opProcCtx
   *          processing context
   * @param pList
   *          pruned partition list. If it is null it will be computed on-the-fly.
   */
  public static void setTaskPlan(String alias_id,
      Operator<? extends OperatorDesc> topOp, MapredWork plan, boolean local,
      GenMRProcContext opProcCtx, PrunedPartitionList pList) throws SemanticException {
    ParseContext parseCtx = opProcCtx.getParseCtx();
    Set<ReadEntity> inputs = opProcCtx.getInputs();

    ArrayList<Path> partDir = new ArrayList<Path>();
    ArrayList<PartitionDesc> partDesc = new ArrayList<PartitionDesc>();

    Path tblDir = null;
    TableDesc tblDesc = null;

    PrunedPartitionList partsList = pList;

    plan.setNameToSplitSample(parseCtx.getNameToSplitSample());

    if (partsList == null) {
      try {
        partsList = parseCtx.getOpToPartList().get((TableScanOperator)topOp);
        if (partsList == null) {
          partsList = PartitionPruner.prune(parseCtx.getTopToTable().get(topOp),
            parseCtx.getOpToPartPruner().get(topOp), opProcCtx.getConf(),
            alias_id, parseCtx.getPrunedPartitions());
          parseCtx.getOpToPartList().put((TableScanOperator)topOp, partsList);
        }
      } catch (SemanticException e) {
        throw e;
      } catch (HiveException e) {
        LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
        throw new SemanticException(e.getMessage(), e);
      }
    }

    // Generate the map work for this alias_id
    Set<Partition> parts = null;
    // pass both confirmed and unknown partitions through the map-reduce
    // framework

    parts = partsList.getConfirmedPartns();
    parts.addAll(partsList.getUnknownPartns());
    PartitionDesc aliasPartnDesc = null;
    try {
      if (!parts.isEmpty()) {
        aliasPartnDesc = Utilities.getPartitionDesc(parts.iterator().next());
      }
    } catch (HiveException e) {
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      throw new SemanticException(e.getMessage(), e);
    }

    // The table does not have any partitions
    if (aliasPartnDesc == null) {
      aliasPartnDesc = new PartitionDesc(Utilities.getTableDesc(parseCtx
          .getTopToTable().get(topOp)), null);

    }

    plan.getAliasToPartnInfo().put(alias_id, aliasPartnDesc);

    long sizeNeeded = Integer.MAX_VALUE;
    int fileLimit = -1;
    if (parseCtx.getGlobalLimitCtx().isEnable()) {
      long sizePerRow = HiveConf.getLongVar(parseCtx.getConf(), HiveConf.ConfVars.HIVELIMITMAXROWSIZE);
      sizeNeeded = parseCtx.getGlobalLimitCtx().getGlobalLimit() * sizePerRow;
      // for the optimization that reduce number of input file, we limit number
      // of files allowed. If more than specific number of files have to be
      // selected, we skip this optimization. Since having too many files as
      // inputs can cause unpredictable latency. It's not necessarily to be
      // cheaper.
      fileLimit =
        HiveConf.getIntVar(parseCtx.getConf(), HiveConf.ConfVars.HIVELIMITOPTLIMITFILE);

      if (sizePerRow <= 0 || fileLimit <= 0) {
        LOG.info("Skip optimization to reduce input size of 'limit'");
        parseCtx.getGlobalLimitCtx().disableOpt();
      } else if (parts.isEmpty()) {
        LOG.info("Empty input: skip limit optimiztion");
      } else {
        LOG.info("Try to reduce input size for 'limit' " +
            "sizeNeeded: " + sizeNeeded +
            "  file limit : " + fileLimit);
      }
    }
    boolean isFirstPart = true;
    boolean emptyInput = true;
    boolean singlePartition = (parts.size() == 1);
    for (Partition part : parts) {
      if (part.getTable().isPartitioned()) {
        inputs.add(new ReadEntity(part));
      } else {
        inputs.add(new ReadEntity(part.getTable()));
      }

      // Later the properties have to come from the partition as opposed
      // to from the table in order to support versioning.
      Path[] paths = null;
      sampleDesc sampleDescr = parseCtx.getOpToSamplePruner().get(topOp);

      // Lookup list bucketing pruner
      Map<String, ExprNodeDesc> partToPruner = parseCtx.getOpToPartToSkewedPruner().get(topOp);
      ExprNodeDesc listBucketingPruner = (partToPruner != null) ? partToPruner.get(part.getName())
          : null;

      if (sampleDescr != null) {
        assert (listBucketingPruner == null) : "Sampling and list bucketing can't coexit.";
        paths = SamplePruner.prune(part, sampleDescr);
        parseCtx.getGlobalLimitCtx().disableOpt();
      } else if (listBucketingPruner != null) {
        assert (sampleDescr == null) : "Sampling and list bucketing can't coexist.";
        /* Use list bucketing prunner's path. */
        paths = ListBucketingPruner.prune(parseCtx, part, listBucketingPruner);
      } else {
        // Now we only try the first partition, if the first partition doesn't
        // contain enough size, we change to normal mode.
        if (parseCtx.getGlobalLimitCtx().isEnable()) {
          if (isFirstPart) {
            long sizeLeft = sizeNeeded;
            ArrayList<Path> retPathList = new ArrayList<Path>();
            SamplePruner.LimitPruneRetStatus status = SamplePruner.limitPrune(part, sizeLeft,
                fileLimit, retPathList);
            if (status.equals(SamplePruner.LimitPruneRetStatus.NoFile)) {
              continue;
            } else if (status.equals(SamplePruner.LimitPruneRetStatus.NotQualify)) {
              LOG.info("Use full input -- first " + fileLimit + " files are more than "
                  + sizeNeeded
                  + " bytes");

              parseCtx.getGlobalLimitCtx().disableOpt();

            } else {
              emptyInput = false;
              paths = new Path[retPathList.size()];
              int index = 0;
              for (Path path : retPathList) {
                paths[index++] = path;
              }
              if (status.equals(SamplePruner.LimitPruneRetStatus.NeedAllFiles) && singlePartition) {
                // if all files are needed to meet the size limit, we disable
                // optimization. It usually happens for empty table/partition or
                // table/partition with only one file. By disabling this
                // optimization, we can avoid retrying the query if there is
                // not sufficient rows.
                parseCtx.getGlobalLimitCtx().disableOpt();
              }
            }
            isFirstPart = false;
          } else {
            paths = new Path[0];
          }
        }
        if (!parseCtx.getGlobalLimitCtx().isEnable()) {
          paths = part.getPath();
        }
      }

      // is it a partitioned table ?
      if (!part.getTable().isPartitioned()) {
        assert ((tblDir == null) && (tblDesc == null));

        tblDir = paths[0];
        tblDesc = Utilities.getTableDesc(part.getTable());
      } else if (tblDesc == null) {
        tblDesc = Utilities.getTableDesc(part.getTable());
      }

      for (Path p : paths) {
        if (p == null) {
          continue;
        }
        String path = p.toString();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Adding " + path + " of table" + alias_id);
        }

        partDir.add(p);
        try {
          partDesc.add(Utilities.getPartitionDescFromTableDesc(tblDesc, part));
        } catch (HiveException e) {
          LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
          throw new SemanticException(e.getMessage(), e);
        }
      }
    }
    if (emptyInput) {
      parseCtx.getGlobalLimitCtx().disableOpt();
    }

    Iterator<Path> iterPath = partDir.iterator();
    Iterator<PartitionDesc> iterPartnDesc = partDesc.iterator();

    if (!local) {
      while (iterPath.hasNext()) {
        assert iterPartnDesc.hasNext();
        String path = iterPath.next().toString();

        PartitionDesc prtDesc = iterPartnDesc.next();

        // Add the path to alias mapping
        if (plan.getPathToAliases().get(path) == null) {
          plan.getPathToAliases().put(path, new ArrayList<String>());
        }
        plan.getPathToAliases().get(path).add(alias_id);
        plan.getPathToPartitionInfo().put(path, prtDesc);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Information added for path " + path);
        }
      }

      assert plan.getAliasToWork().get(alias_id) == null;
      plan.getAliasToWork().put(alias_id, topOp);
    } else {
      // populate local work if needed
      MapredLocalWork localPlan = plan.getMapLocalWork();
      if (localPlan == null) {
        localPlan = new MapredLocalWork(
            new LinkedHashMap<String, Operator<? extends OperatorDesc>>(),
            new LinkedHashMap<String, FetchWork>());
      }

      assert localPlan.getAliasToWork().get(alias_id) == null;
      assert localPlan.getAliasToFetchWork().get(alias_id) == null;
      localPlan.getAliasToWork().put(alias_id, topOp);
      if (tblDir == null) {
        tblDesc = Utilities.getTableDesc(partsList.getSourceTable());
        localPlan.getAliasToFetchWork().put(
            alias_id,
            new FetchWork(FetchWork.convertPathToStringArray(partDir), partDesc, tblDesc));
      } else {
        localPlan.getAliasToFetchWork().put(alias_id,
            new FetchWork(tblDir.toString(), tblDesc));
      }
      plan.setMapLocalWork(localPlan);
    }
  }

  /**
   * set the current task in the mapredWork.
   *
   * @param alias
   *          current alias
   * @param topOp
   *          the top operator of the stack
   * @param plan
   *          current plan
   * @param local
   *          whether you need to add to map-reduce or local work
   * @param tt_desc
   *          table descriptor
   */
  public static void setTaskPlan(String path, String alias,
      Operator<? extends OperatorDesc> topOp, MapredWork plan, boolean local,
      TableDesc tt_desc) throws SemanticException {

    if(path == null || alias == null) {
      return;
    }

    if (!local) {
      if (plan.getPathToAliases().get(path) == null) {
        plan.getPathToAliases().put(path, new ArrayList<String>());
      }
      plan.getPathToAliases().get(path).add(alias);
      plan.getPathToPartitionInfo().put(path, new PartitionDesc(tt_desc, null));
      plan.getAliasToWork().put(alias, topOp);
    } else {
      // populate local work if needed
      MapredLocalWork localPlan = plan.getMapLocalWork();
      if (localPlan == null) {
        localPlan = new MapredLocalWork(
            new LinkedHashMap<String, Operator<? extends OperatorDesc>>(),
            new LinkedHashMap<String, FetchWork>());
      }

      assert localPlan.getAliasToWork().get(alias) == null;
      assert localPlan.getAliasToFetchWork().get(alias) == null;
      localPlan.getAliasToWork().put(alias, topOp);
      localPlan.getAliasToFetchWork().put(alias, new FetchWork(alias, tt_desc));
      plan.setMapLocalWork(localPlan);
    }
  }

  /**
   * set key and value descriptor.
   *
   * @param plan
   *          current plan
   * @param topOp
   *          current top operator in the path
   */
  public static void setKeyAndValueDesc(MapredWork plan,
      Operator<? extends OperatorDesc> topOp) {
    if (topOp == null) {
      return;
    }

    if (topOp instanceof ReduceSinkOperator) {
      ReduceSinkOperator rs = (ReduceSinkOperator) topOp;
      plan.setKeyDesc(rs.getConf().getKeySerializeInfo());
      int tag = Math.max(0, rs.getConf().getTag());
      List<TableDesc> tagToSchema = plan.getTagToValueDesc();
      while (tag + 1 > tagToSchema.size()) {
        tagToSchema.add(null);
      }
      tagToSchema.set(tag, rs.getConf().getValueSerializeInfo());
    } else {
      List<Operator<? extends OperatorDesc>> children = topOp.getChildOperators();
      if (children != null) {
        for (Operator<? extends OperatorDesc> op : children) {
          setKeyAndValueDesc(plan, op);
        }
      }
    }
  }

  /**
   * create a new plan and return.
   *
   * @return the new plan
   */
  public static MapredWork getMapRedWork(ParseContext parseCtx) {
    MapredWork work = getMapRedWorkFromConf(parseCtx.getConf());
    work.setNameToSplitSample(parseCtx.getNameToSplitSample());
    return work;
  }

  /**
   * create a new plan and return. The pan won't contain the name to split
   * sample information in parse context.
   *
   * @return the new plan
   */
  public static MapredWork getMapRedWorkFromConf(HiveConf conf) {
    MapredWork work = new MapredWork();

    boolean mapperCannotSpanPartns =
      conf.getBoolVar(
        HiveConf.ConfVars.HIVE_MAPPER_CANNOT_SPAN_MULTIPLE_PARTITIONS);
    work.setMapperCannotSpanPartns(mapperCannotSpanPartns);
    work.setPathToAliases(new LinkedHashMap<String, ArrayList<String>>());
    work.setPathToPartitionInfo(new LinkedHashMap<String, PartitionDesc>());
    work.setAliasToWork(new LinkedHashMap<String, Operator<? extends OperatorDesc>>());
    work.setTagToValueDesc(new ArrayList<TableDesc>());
    work.setReducer(null);
    work.setHadoopSupportsSplittable(
        conf.getBoolVar(HiveConf.ConfVars.HIVE_COMBINE_INPUT_FORMAT_SUPPORTS_SPLITTABLE));
    return work;
  }

  /**
   * insert in the map for the operator to row resolver.
   *
   * @param op
   *          operator created
   * @param rr
   *          row resolver
   * @param parseCtx
   *          parse context
   */
  @SuppressWarnings("nls")
  public static Operator<? extends OperatorDesc> putOpInsertMap(
      Operator<? extends OperatorDesc> op, RowResolver rr, ParseContext parseCtx) {
    OpParseContext ctx = new OpParseContext(rr);
    parseCtx.getOpParseCtx().put(op, ctx);
    return op;
  }

  @SuppressWarnings("nls")
  /**
   * Merge the tasks - by creating a temporary file between them.
   * @param op reduce sink operator being processed
   * @param oldTask the parent task
   * @param task the child task
   * @param opProcCtx context
   * @param setReducer does the reducer needs to be set
   * @param pos position of the parent
   **/
  public static void splitTasks(Operator<? extends OperatorDesc> op,
      Task<? extends Serializable> parentTask,
      Task<? extends Serializable> childTask, GenMRProcContext opProcCtx,
      boolean setReducer, boolean local, int posn) throws SemanticException {
    childTask.getWork();
    Operator<? extends OperatorDesc> currTopOp = opProcCtx.getCurrTopOp();

    ParseContext parseCtx = opProcCtx.getParseCtx();
    parentTask.addDependentTask(childTask);

    // Root Task cannot depend on any other task, therefore childTask cannot be
    // a root Task
    List<Task<? extends Serializable>> rootTasks = opProcCtx.getRootTasks();
    if (rootTasks.contains(childTask)) {
      rootTasks.remove(childTask);
    }

    // generate the temporary file
    Context baseCtx = parseCtx.getContext();
    String taskTmpDir = baseCtx.getMRTmpFileURI();

    Operator<? extends OperatorDesc> parent = op.getParentOperators().get(posn);
    TableDesc tt_desc = PlanUtils.getIntermediateFileTableDesc(PlanUtils
        .getFieldSchemasFromRowSchema(parent.getSchema(), "temporarycol"));

    // Create a file sink operator for this file name
    boolean compressIntermediate = parseCtx.getConf().getBoolVar(
        HiveConf.ConfVars.COMPRESSINTERMEDIATE);
    FileSinkDesc desc = new FileSinkDesc(taskTmpDir, tt_desc,
        compressIntermediate);
    if (compressIntermediate) {
      desc.setCompressCodec(parseCtx.getConf().getVar(
          HiveConf.ConfVars.COMPRESSINTERMEDIATECODEC));
      desc.setCompressType(parseCtx.getConf().getVar(
          HiveConf.ConfVars.COMPRESSINTERMEDIATETYPE));
    }
    Operator<? extends OperatorDesc> fs_op = putOpInsertMap(OperatorFactory
        .get(desc, parent.getSchema()), null, parseCtx);

    // replace the reduce child with this operator
    List<Operator<? extends OperatorDesc>> childOpList = parent
    .getChildOperators();
    for (int pos = 0; pos < childOpList.size(); pos++) {
      if (childOpList.get(pos) == op) {
        childOpList.set(pos, fs_op);
        break;
      }
    }

    List<Operator<? extends OperatorDesc>> parentOpList =
      new ArrayList<Operator<? extends OperatorDesc>>();
    parentOpList.add(parent);
    fs_op.setParentOperators(parentOpList);

    // create a dummy tableScan operator on top of op
    // TableScanOperator is implicitly created here for each MapOperator
    RowResolver rowResolver = opProcCtx.getParseCtx().getOpParseCtx().get(parent).getRowResolver();
    Operator<? extends OperatorDesc> ts_op = putOpInsertMap(OperatorFactory
        .get(TableScanDesc.class, parent.getSchema()), rowResolver, parseCtx);

    childOpList = new ArrayList<Operator<? extends OperatorDesc>>();
    childOpList.add(op);
    ts_op.setChildOperators(childOpList);
    op.getParentOperators().set(posn, ts_op);

    Map<Operator<? extends OperatorDesc>, GenMapRedCtx> mapCurrCtx =
      opProcCtx.getMapCurrCtx();
    mapCurrCtx.put(ts_op, new GenMapRedCtx(childTask, null, null));

    String streamDesc = taskTmpDir;
    MapredWork cplan = (MapredWork) childTask.getWork();

    if (setReducer) {
      Operator<? extends OperatorDesc> reducer = op.getChildOperators().get(0);

      if (reducer.getClass() == JoinOperator.class) {
        String origStreamDesc;
        streamDesc = "$INTNAME";
        origStreamDesc = streamDesc;
        int pos = 0;
        while (cplan.getAliasToWork().get(streamDesc) != null) {
          streamDesc = origStreamDesc.concat(String.valueOf(++pos));
        }
      }

      // TODO: Allocate work to remove the temporary files and make that
      // dependent on the redTask
      if (reducer.getClass() == JoinOperator.class) {
        cplan.setNeedsTagging(true);
      }
    }

    // Add the path to alias mapping
    setTaskPlan(taskTmpDir, streamDesc, ts_op, cplan, local, tt_desc);

    // This can be cleaned up as a function table in future
    if (op instanceof AbstractMapJoinOperator<?>) {
      AbstractMapJoinOperator<? extends MapJoinDesc> mjOp = (AbstractMapJoinOperator<? extends MapJoinDesc>) op;
      opProcCtx.setCurrMapJoinOp(mjOp);
      GenMRMapJoinCtx mjCtx = opProcCtx.getMapJoinCtx(mjOp);
      if (mjCtx == null) {
        mjCtx = new GenMRMapJoinCtx(taskTmpDir, tt_desc, ts_op, null);
      } else {
        mjCtx.setTaskTmpDir(taskTmpDir);
        mjCtx.setTTDesc(tt_desc);
        mjCtx.setRootMapJoinOp(ts_op);
      }
      opProcCtx.setMapJoinCtx(mjOp, mjCtx);
      opProcCtx.getMapCurrCtx().put(parent,
          new GenMapRedCtx(childTask, null, null));
      setupBucketMapJoinInfo(cplan, mjOp, false);
    }

    currTopOp = null;
    String currAliasId = null;

    opProcCtx.setCurrTopOp(currTopOp);
    opProcCtx.setCurrAliasId(currAliasId);
    opProcCtx.setCurrTask(childTask);
  }

  public static void mergeMapJoinUnion(UnionOperator union,
      GenMRProcContext ctx, int pos) throws SemanticException {
    ParseContext parseCtx = ctx.getParseCtx();
    UnionProcContext uCtx = parseCtx.getUCtx();

    UnionParseContext uPrsCtx = uCtx.getUnionParseContext(union);
    assert uPrsCtx != null;

    Task<? extends Serializable> currTask = ctx.getCurrTask();

    GenMRUnionCtx uCtxTask = ctx.getUnionTask(union);
    Task<? extends Serializable> uTask = null;

    union.getParentOperators().get(pos);
    MapredWork uPlan = null;

    // union is encountered for the first time
    if (uCtxTask == null) {
      uCtxTask = new GenMRUnionCtx();
      uPlan = GenMapRedUtils.getMapRedWork(parseCtx);
      uTask = TaskFactory.get(uPlan, parseCtx.getConf());
      uCtxTask.setUTask(uTask);
      ctx.setUnionTask(union, uCtxTask);
    } else {
      uTask = uCtxTask.getUTask();
      uPlan = (MapredWork) uTask.getWork();
    }

    // If there is a mapjoin at position 'pos'
    if (uPrsCtx.getMapJoinSubq(pos)) {
      GenMRMapJoinCtx mjCtx = ctx.getMapJoinCtx(ctx.getCurrMapJoinOp());
      String taskTmpDir = mjCtx.getTaskTmpDir();
      if (uPlan.getPathToAliases().get(taskTmpDir) == null) {
        uPlan.getPathToAliases().put(taskTmpDir, new ArrayList<String>());
        uPlan.getPathToAliases().get(taskTmpDir).add(taskTmpDir);
        uPlan.getPathToPartitionInfo().put(taskTmpDir,
            new PartitionDesc(mjCtx.getTTDesc(), null));
        uPlan.getAliasToWork().put(taskTmpDir, mjCtx.getRootMapJoinOp());
      }

      for (Task t : currTask.getParentTasks()) {
        t.addDependentTask(uTask);
      }
      try {
        boolean notDone = true;
        while (notDone) {
          for (Task t : currTask.getParentTasks()) {
            t.removeDependentTask(currTask);
          }
          notDone = false;
        }
      } catch (ConcurrentModificationException e) {
      }
    } else {
      setTaskPlan(ctx.getCurrAliasId(), ctx.getCurrTopOp(), uPlan, false, ctx);
    }

    ctx.setCurrTask(uTask);
    ctx.setCurrAliasId(null);
    ctx.setCurrTopOp(null);
    ctx.setCurrMapJoinOp(null);

    ctx.getMapCurrCtx().put(union,
        new GenMapRedCtx(ctx.getCurrTask(), null, null));
  }

  private GenMapRedUtils() {
    // prevent instantiation
  }

}
