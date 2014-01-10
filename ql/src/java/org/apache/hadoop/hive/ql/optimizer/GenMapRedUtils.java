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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.DemuxOperator;
import org.apache.hadoop.hive.ql.exec.DependencyCollectionTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MoveTask;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecDriver;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.rcfile.merge.MergeWork;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMRUnionCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMapRedCtx;
import org.apache.hadoop.hive.ql.optimizer.listbucketingpruner.ListBucketingPruner;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverMergeFiles;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverMergeFiles.ConditionalResolverMergeFilesCtx;
import org.apache.hadoop.hive.ql.plan.ConditionalWork;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc.sampleDesc;
import org.apache.hadoop.hive.ql.plan.LoadFileDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.StatsWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.stats.StatsFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.InputFormat;

/**
 * General utility common functions for the Processor to convert operator into
 * map-reduce tasks.
 */
public final class GenMapRedUtils {
  private static Log LOG;

  static {
    LOG = LogFactory.getLog("org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils");
  }

  public static boolean needsTagging(ReduceWork rWork) {
    return rWork != null && (rWork.getReducer().getClass() == JoinOperator.class ||
         rWork.getReducer().getClass() == DemuxOperator.class);
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
    plan.setReduceWork(new ReduceWork());
    plan.getReduceWork().setReducer(reducer);
    ReduceSinkDesc desc = op.getConf();

    plan.getReduceWork().setNumReduceTasks(desc.getNumReducers());

    if (needsTagging(plan.getReduceWork())) {
      plan.getReduceWork().setNeedsTagging(true);
    }

    assert currTopOp != null;
    String currAliasId = opProcCtx.getCurrAliasId();

    if (!opProcCtx.isSeenOp(currTask, currTopOp)) {
      setTaskPlan(currAliasId, currTopOp, currTask, false, opProcCtx);
    }

    currTopOp = null;
    currAliasId = null;

    opProcCtx.setCurrTask(currTask);
    opProcCtx.setCurrTopOp(currTopOp);
    opProcCtx.setCurrAliasId(currAliasId);
  }


  /**
   * Initialize the current union plan.
   *
   * @param op
   *          the reduce sink operator encountered
   * @param opProcCtx
   *          processing context
   */
  public static void initUnionPlan(ReduceSinkOperator op, UnionOperator currUnionOp,
      GenMRProcContext opProcCtx,
      Task<? extends Serializable> unionTask) throws SemanticException {
    Operator<? extends OperatorDesc> reducer = op.getChildOperators().get(0);

    MapredWork plan = (MapredWork) unionTask.getWork();
    HashMap<Operator<? extends OperatorDesc>, Task<? extends Serializable>> opTaskMap =
        opProcCtx.getOpTaskMap();

    opTaskMap.put(reducer, unionTask);

    plan.setReduceWork(new ReduceWork());
    plan.getReduceWork().setReducer(reducer);
    plan.getReduceWork().setReducer(reducer);
    ReduceSinkDesc desc = op.getConf();

    plan.getReduceWork().setNumReduceTasks(desc.getNumReducers());

    if (needsTagging(plan.getReduceWork())) {
      plan.getReduceWork().setNeedsTagging(true);
    }

    initUnionPlan(opProcCtx, currUnionOp, unionTask, false);
  }

  private static void setUnionPlan(GenMRProcContext opProcCtx,
      boolean local, Task<? extends Serializable> currTask, GenMRUnionCtx uCtx,
      boolean mergeTask) throws SemanticException {
    Operator<? extends OperatorDesc> currTopOp = opProcCtx.getCurrTopOp();

    if (currTopOp != null) {
      String currAliasId = opProcCtx.getCurrAliasId();
      if (mergeTask || !opProcCtx.isSeenOp(currTask, currTopOp)) {
        setTaskPlan(currAliasId, currTopOp, currTask, local, opProcCtx);
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

        MapredWork plan = (MapredWork) currTask.getWork();
        for (int pos = 0; pos < size; pos++) {
          String taskTmpDir = taskTmpDirLst.get(pos);
          TableDesc tt_desc = tt_descLst.get(pos);
          MapWork mWork = plan.getMapWork();
          if (mWork.getPathToAliases().get(taskTmpDir) == null) {
            mWork.getPathToAliases().put(taskTmpDir,
                new ArrayList<String>());
            mWork.getPathToAliases().get(taskTmpDir).add(taskTmpDir);
            mWork.getPathToPartitionInfo().put(taskTmpDir,
                new PartitionDesc(tt_desc, null));
            mWork.getAliasToWork().put(taskTmpDir, topOperators.get(pos));
          }
        }
      }
    }
  }

  /*
   * It is a idempotent function to add various intermediate files as the source
   * for the union. The plan has already been created.
   */
  public static void initUnionPlan(GenMRProcContext opProcCtx, UnionOperator currUnionOp,
      Task<? extends Serializable> currTask, boolean local)
      throws SemanticException {
    // In case of lateral views followed by a join, the same tree
    // can be traversed more than one
    if (currUnionOp != null) {
      GenMRUnionCtx uCtx = opProcCtx.getUnionTask(currUnionOp);
      assert uCtx != null;
      setUnionPlan(opProcCtx, local, currTask, uCtx, false);
    }
  }

  /*
   * join current union task to old task
   */
  public static void joinUnionPlan(GenMRProcContext opProcCtx,
      UnionOperator currUnionOp,
      Task<? extends Serializable> currentUnionTask,
      Task<? extends Serializable> existingTask, boolean local)
      throws SemanticException {
    assert currUnionOp != null;
    GenMRUnionCtx uCtx = opProcCtx.getUnionTask(currUnionOp);
    assert uCtx != null;

    setUnionPlan(opProcCtx, local, existingTask, uCtx, true);

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

  /**
   * Merge the current task into the old task for the reducer
   *
   * @param currTask
   *          the current task for the current reducer
   * @param oldTask
   *          the old task for the current reducer
   * @param opProcCtx
   *          processing context
   */
  public static void joinPlan(Task<? extends Serializable> currTask,
      Task<? extends Serializable> oldTask, GenMRProcContext opProcCtx)
      throws SemanticException {
    assert currTask != null && oldTask != null;

    Operator<? extends OperatorDesc> currTopOp = opProcCtx.getCurrTopOp();
    List<Task<? extends Serializable>> parTasks = null;
    // terminate the old task and make current task dependent on it
    if (currTask.getParentTasks() != null
        && !currTask.getParentTasks().isEmpty()) {
      parTasks = new ArrayList<Task<? extends Serializable>>();
      parTasks.addAll(currTask.getParentTasks());

      Object[] parTaskArr = parTasks.toArray();
      for (Object element : parTaskArr) {
        ((Task<? extends Serializable>) element).removeDependentTask(currTask);
      }
    }

    if (currTopOp != null) {
      mergeInput(currTopOp, opProcCtx, oldTask, false);
    }

    if (parTasks != null) {
      for (Task<? extends Serializable> parTask : parTasks) {
        parTask.addDependentTask(oldTask);
      }
    }

    if (oldTask instanceof MapRedTask && currTask instanceof MapRedTask) {
      ((MapRedTask)currTask).getWork().getMapWork()
        .mergingInto(((MapRedTask) oldTask).getWork().getMapWork());
    }

    opProcCtx.setCurrTopOp(null);
    opProcCtx.setCurrTask(oldTask);
  }

  /**
   * If currTopOp is not set for input of the task, add input for to the task
   */
  static boolean mergeInput(Operator<? extends OperatorDesc> currTopOp,
      GenMRProcContext opProcCtx, Task<? extends Serializable> task, boolean local)
      throws SemanticException {
    if (!opProcCtx.isSeenOp(task, currTopOp)) {
      String currAliasId = opProcCtx.getCurrAliasId();
      setTaskPlan(currAliasId, currTopOp, task, local, opProcCtx);
      return true;
    }
    return false;
  }

  /**
   * Met cRS in pRS(parentTask)-cRS-OP(childTask) case
   * Split and link two tasks by temporary file : pRS-FS / TS-cRS-OP
   */
  static void splitPlan(ReduceSinkOperator cRS,
      Task<? extends Serializable> parentTask, Task<? extends Serializable> childTask,
      GenMRProcContext opProcCtx) throws SemanticException {
    assert parentTask != null && childTask != null;
    splitTasks(cRS, parentTask, childTask, opProcCtx);
  }

  /**
   * Met cRS in pOP(parentTask with RS)-cRS-cOP(noTask) case
   * Create new child task for cRS-cOP and link two tasks by temporary file : pOP-FS / TS-cRS-cOP
   *
   * @param cRS
   *          the reduce sink operator encountered
   * @param opProcCtx
   *          processing context
   */
  static void splitPlan(ReduceSinkOperator cRS, GenMRProcContext opProcCtx)
      throws SemanticException {
    // Generate a new task
    ParseContext parseCtx = opProcCtx.getParseCtx();
    Task<? extends Serializable> parentTask = opProcCtx.getCurrTask();

    MapredWork childPlan = getMapRedWork(parseCtx);
    Task<? extends Serializable> childTask = TaskFactory.get(childPlan, parseCtx
        .getConf());
    Operator<? extends OperatorDesc> reducer = cRS.getChildOperators().get(0);

    // Add the reducer
    ReduceWork rWork = new ReduceWork();
    childPlan.setReduceWork(rWork);
    rWork.setReducer(reducer);
    ReduceSinkDesc desc = cRS.getConf();
    childPlan.getReduceWork().setNumReduceTasks(new Integer(desc.getNumReducers()));

    opProcCtx.getOpTaskMap().put(reducer, childTask);

    splitTasks(cRS, parentTask, childTask, opProcCtx);
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
      Operator<? extends OperatorDesc> topOp, Task<?> task, boolean local,
      GenMRProcContext opProcCtx) throws SemanticException {
    setTaskPlan(alias_id, topOp, task, local, opProcCtx, null);
  }

  private static ReadEntity getParentViewInfo(String alias_id,
      Map<String, ReadEntity> viewAliasToInput) {
    String[] aliases = alias_id.split(":");

    String currentAlias = null;
    ReadEntity currentInput = null;
    // Find the immediate parent possible.
    // For eg: for a query like 'select * from V3', where V3 -> V2, V2 -> V1, V1 -> T
    // -> implies depends on.
    // T's parent would be V1
    for (int pos = 0; pos < aliases.length; pos++) {
      currentAlias = currentAlias == null ? aliases[pos] : currentAlias + ":" + aliases[pos];
      ReadEntity input = viewAliasToInput.get(currentAlias);
      if (input == null) {
        return currentInput;
      }
      currentInput = input;
    }

    return currentInput;
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
      Operator<? extends OperatorDesc> topOp, Task<?> task, boolean local,
      GenMRProcContext opProcCtx, PrunedPartitionList pList) throws SemanticException {
    setMapWork(((MapredWork) task.getWork()).getMapWork(), opProcCtx.getParseCtx(),
        opProcCtx.getInputs(), pList, topOp, alias_id, opProcCtx.getConf(), local);
    opProcCtx.addSeenOp(task, topOp);
  }

  /**
   * initialize MapWork
   *
   * @param alias_id
   *          current alias
   * @param topOp
   *          the top operator of the stack
   * @param plan
   *          map work to initialize
   * @param local
   *          whether you need to add to map-reduce or local work
   * @param pList
   *          pruned partition list. If it is null it will be computed on-the-fly.
   * @param inputs
   *          read entities for the map work
   * @param conf
   *          current instance of hive conf
   */
  public static void setMapWork(MapWork plan, ParseContext parseCtx, Set<ReadEntity> inputs,
      PrunedPartitionList partsList, Operator<? extends OperatorDesc> topOp, String alias_id,
      HiveConf conf, boolean local) throws SemanticException {
    ArrayList<Path> partDir = new ArrayList<Path>();
    ArrayList<PartitionDesc> partDesc = new ArrayList<PartitionDesc>();

    Path tblDir = null;
    TableDesc tblDesc = null;

    plan.setNameToSplitSample(parseCtx.getNameToSplitSample());

    if (partsList == null) {
      try {
        TableScanOperator tsOp = (TableScanOperator) topOp;
        partsList = PartitionPruner.prune(tsOp, parseCtx, alias_id);
      } catch (SemanticException e) {
        throw e;
      } catch (HiveException e) {
        LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
        throw new SemanticException(e.getMessage(), e);
      }
    }

    // Generate the map work for this alias_id
    // pass both confirmed and unknown partitions through the map-reduce
    // framework
    Set<Partition> parts = partsList.getPartitions();
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

    Map<String, String> props = parseCtx.getTopToProps().get(topOp);
    if (props != null) {
      Properties target = aliasPartnDesc.getProperties();
      if (target == null) {
        aliasPartnDesc.setProperties(target = new Properties());
      }
      target.putAll(props);
    }

    plan.getAliasToPartnInfo().put(alias_id, aliasPartnDesc);

    long sizeNeeded = Integer.MAX_VALUE;
    int fileLimit = -1;
    if (parseCtx.getGlobalLimitCtx().isEnable()) {
      long sizePerRow = HiveConf.getLongVar(parseCtx.getConf(),
          HiveConf.ConfVars.HIVELIMITMAXROWSIZE);
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

    // Track the dependencies for the view. Consider a query like: select * from V;
    // where V is a view of the form: select * from T
    // The dependencies should include V at depth 0, and T at depth 1 (inferred).
    ReadEntity parentViewInfo = getParentViewInfo(alias_id, parseCtx.getViewAliasToInput());

    // The table should also be considered a part of inputs, even if the table is a
    // partitioned table and whether any partition is selected or not
    PlanUtils.addInput(inputs,
        new ReadEntity(parseCtx.getTopToTable().get(topOp), parentViewInfo));

    for (Partition part : parts) {
      if (part.getTable().isPartitioned()) {
        PlanUtils.addInput(inputs, new ReadEntity(part, parentViewInfo));
      } else {
        PlanUtils.addInput(inputs, new ReadEntity(part.getTable(), parentViewInfo));
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

      if (props != null) {
        Properties target = tblDesc.getProperties();
        if (target == null) {
          tblDesc.setProperties(target = new Properties());
        }
        target.putAll(props);
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
          if (part.getTable().isPartitioned()) {
            partDesc.add(Utilities.getPartitionDesc(part));
          }
          else {
            partDesc.add(Utilities.getPartitionDescFromTableDesc(tblDesc, part));
          }
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
            new FetchWork(partDir, partDesc, tblDesc));
      } else {
        localPlan.getAliasToFetchWork().put(alias_id,
            new FetchWork(tblDir, tblDesc));
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
      Operator<? extends OperatorDesc> topOp, MapWork plan, boolean local,
      TableDesc tt_desc) throws SemanticException {

    if (path == null || alias == null) {
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
      localPlan.getAliasToFetchWork().put(alias, new FetchWork(new Path(alias), tt_desc));
      plan.setMapLocalWork(localPlan);
    }
  }

  /**
   * Set key and value descriptor
   * @param work RedueWork
   * @param rs ReduceSinkOperator
   */
  public static void setKeyAndValueDesc(ReduceWork work, ReduceSinkOperator rs) {
    work.setKeyDesc(rs.getConf().getKeySerializeInfo());
    int tag = Math.max(0, rs.getConf().getTag());
    List<TableDesc> tagToSchema = work.getTagToValueDesc();
    while (tag + 1 > tagToSchema.size()) {
      tagToSchema.add(null);
    }
    tagToSchema.set(tag, rs.getConf().getValueSerializeInfo());
  }

  /**
   * set key and value descriptor.
   *
   * @param plan
   *          current plan
   * @param topOp
   *          current top operator in the path
   */
  public static void setKeyAndValueDesc(ReduceWork plan,
      Operator<? extends OperatorDesc> topOp) {
    if (topOp == null) {
      return;
    }

    if (topOp instanceof ReduceSinkOperator) {
      ReduceSinkOperator rs = (ReduceSinkOperator) topOp;
      setKeyAndValueDesc(plan, rs);
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
   * Set the key and value description for all the tasks rooted at the given
   * task. Loops over all the tasks recursively.
   *
   * @param task
   */
  public static void setKeyAndValueDescForTaskTree(Task<? extends Serializable> task) {

    if (task instanceof ConditionalTask) {
      List<Task<? extends Serializable>> listTasks = ((ConditionalTask) task)
          .getListTasks();
      for (Task<? extends Serializable> tsk : listTasks) {
        setKeyAndValueDescForTaskTree(tsk);
      }
    } else if (task instanceof ExecDriver) {
      MapredWork work = (MapredWork) task.getWork();
      work.getMapWork().deriveExplainAttributes();
      HashMap<String, Operator<? extends OperatorDesc>> opMap = work
          .getMapWork().getAliasToWork();
      if (opMap != null && !opMap.isEmpty()) {
        for (Operator<? extends OperatorDesc> op : opMap.values()) {
          setKeyAndValueDesc(work.getReduceWork(), op);
        }
      }
    }

    if (task.getChildTasks() == null) {
      return;
    }

    for (Task<? extends Serializable> childTask : task.getChildTasks()) {
      setKeyAndValueDescForTaskTree(childTask);
    }
  }

  /**
   * create a new plan and return.
   *
   * @return the new plan
   */
  public static MapredWork getMapRedWork(ParseContext parseCtx) {
    MapredWork work = getMapRedWorkFromConf(parseCtx.getConf());
    work.getMapWork().setNameToSplitSample(parseCtx.getNameToSplitSample());
    return work;
  }

  /**
   * create a new plan and return. The pan won't contain the name to split
   * sample information in parse context.
   *
   * @return the new plan
   */
  public static MapredWork getMapRedWorkFromConf(HiveConf conf) {
    MapredWork mrWork = new MapredWork();
    MapWork work = mrWork.getMapWork();

    boolean mapperCannotSpanPartns =
        conf.getBoolVar(
            HiveConf.ConfVars.HIVE_MAPPER_CANNOT_SPAN_MULTIPLE_PARTITIONS);
    work.setMapperCannotSpanPartns(mapperCannotSpanPartns);
    work.setPathToAliases(new LinkedHashMap<String, ArrayList<String>>());
    work.setPathToPartitionInfo(new LinkedHashMap<String, PartitionDesc>());
    work.setAliasToWork(new LinkedHashMap<String, Operator<? extends OperatorDesc>>());
    work.setHadoopSupportsSplittable(
        conf.getBoolVar(HiveConf.ConfVars.HIVE_COMBINE_INPUT_FORMAT_SUPPORTS_SPLITTABLE));
    return mrWork;
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

  public static TableScanOperator createTemporaryTableScanOperator(RowSchema rowSchema) {
    TableScanOperator tableScanOp =
        (TableScanOperator) OperatorFactory.get(new TableScanDesc(), rowSchema);
    // Set needed columns for this dummy TableScanOperator
    List<Integer> neededColumnIds = new ArrayList<Integer>();
    List<String> neededColumnNames = new ArrayList<String>();
    List<ColumnInfo> parentColumnInfos = rowSchema.getSignature();
    for (int i = 0 ; i < parentColumnInfos.size(); i++) {
      neededColumnIds.add(i);
      neededColumnNames.add(parentColumnInfos.get(i).getInternalName());
    }
    tableScanOp.setNeededColumnIDs(neededColumnIds);
    tableScanOp.setNeededColumns(neededColumnNames);
    return tableScanOp;
  }

  /**
   * Break the pipeline between parent and child, and then
   * output data generated by parent to a temporary file stored in taskTmpDir.
   * A FileSinkOperator is added after parent to output the data.
   * Before child, we add a TableScanOperator to load data stored in the temporary
   * file back.
   * @param parent
   * @param child
   * @param taskTmpDir
   * @param tt_desc
   * @param parseCtx
   * @return The TableScanOperator inserted before child.
   */
  protected static TableScanOperator createTemporaryFile(
      Operator<? extends OperatorDesc> parent, Operator<? extends OperatorDesc> child,
      String taskTmpDir, TableDesc tt_desc, ParseContext parseCtx) {

    // Create a FileSinkOperator for the file name of taskTmpDir
    boolean compressIntermediate =
        parseCtx.getConf().getBoolVar(HiveConf.ConfVars.COMPRESSINTERMEDIATE);
    FileSinkDesc desc = new FileSinkDesc(new Path(taskTmpDir), tt_desc, compressIntermediate);
    if (compressIntermediate) {
      desc.setCompressCodec(parseCtx.getConf().getVar(
          HiveConf.ConfVars.COMPRESSINTERMEDIATECODEC));
      desc.setCompressType(parseCtx.getConf().getVar(
          HiveConf.ConfVars.COMPRESSINTERMEDIATETYPE));
    }
    Operator<? extends OperatorDesc> fileSinkOp = putOpInsertMap(OperatorFactory
        .get(desc, parent.getSchema()), null, parseCtx);

    // Connect parent to fileSinkOp
    parent.replaceChild(child, fileSinkOp);
    fileSinkOp.setParentOperators(Utilities.makeList(parent));

    // Create a dummy TableScanOperator for the file generated through fileSinkOp
    RowResolver parentRowResolver =
        parseCtx.getOpParseCtx().get(parent).getRowResolver();
    TableScanOperator tableScanOp = (TableScanOperator) putOpInsertMap(
        createTemporaryTableScanOperator(parent.getSchema()),
        parentRowResolver, parseCtx);

    // Connect this TableScanOperator to child.
    tableScanOp.setChildOperators(Utilities.makeList(child));
    child.replaceParent(parent, tableScanOp);

    return tableScanOp;
  }

  @SuppressWarnings("nls")
  /**
   * Split two tasks by creating a temporary file between them.
   *
   * @param op reduce sink operator being processed
   * @param parentTask the parent task
   * @param childTask the child task
   * @param opProcCtx context
   **/
  private static void splitTasks(ReduceSinkOperator op,
      Task<? extends Serializable> parentTask, Task<? extends Serializable> childTask,
      GenMRProcContext opProcCtx) throws SemanticException {
    if (op.getNumParent() != 1) {
      throw new IllegalStateException("Expecting operator " + op + " to have one parent. " +
          "But found multiple parents : " + op.getParentOperators());
    }

    ParseContext parseCtx = opProcCtx.getParseCtx();
    parentTask.addDependentTask(childTask);

    // Root Task cannot depend on any other task, therefore childTask cannot be
    // a root Task
    List<Task<? extends Serializable>> rootTasks = opProcCtx.getRootTasks();
    if (rootTasks.contains(childTask)) {
      rootTasks.remove(childTask);
    }

    // Generate the temporary file name
    Context baseCtx = parseCtx.getContext();
    String taskTmpDir = baseCtx.getMRTmpFileURI();

    Operator<? extends OperatorDesc> parent = op.getParentOperators().get(0);
    TableDesc tt_desc = PlanUtils.getIntermediateFileTableDesc(PlanUtils
        .getFieldSchemasFromRowSchema(parent.getSchema(), "temporarycol"));

    // Create the temporary file, its corresponding FileSinkOperaotr, and
    // its corresponding TableScanOperator.
    TableScanOperator tableScanOp =
        createTemporaryFile(parent, op, taskTmpDir, tt_desc, parseCtx);

    Map<Operator<? extends OperatorDesc>, GenMapRedCtx> mapCurrCtx =
        opProcCtx.getMapCurrCtx();
    mapCurrCtx.put(tableScanOp, new GenMapRedCtx(childTask, null));

    String streamDesc = taskTmpDir;
    MapredWork cplan = (MapredWork) childTask.getWork();

    if (needsTagging(cplan.getReduceWork())) {
      Operator<? extends OperatorDesc> reducerOp = cplan.getReduceWork().getReducer();
      QBJoinTree joinTree = null;
      if (reducerOp instanceof JoinOperator) {
        joinTree = parseCtx.getJoinContext().get(reducerOp);
      } else if (reducerOp instanceof MapJoinOperator) {
        joinTree = parseCtx.getMapJoinContext().get(reducerOp);
      } else if (reducerOp instanceof SMBMapJoinOperator) {
        joinTree = parseCtx.getSmbMapJoinContext().get(reducerOp);
      }

      if (joinTree != null && joinTree.getId() != null) {
        streamDesc = joinTree.getId() + ":$INTNAME";
      } else {
        streamDesc = "$INTNAME";
      }

      String origStreamDesc = streamDesc;
      int pos = 0;
      while (cplan.getMapWork().getAliasToWork().get(streamDesc) != null) {
        streamDesc = origStreamDesc.concat(String.valueOf(++pos));
      }

      // TODO: Allocate work to remove the temporary files and make that
      // dependent on the redTask
      cplan.getReduceWork().setNeedsTagging(true);
    }

    // Add the path to alias mapping
    setTaskPlan(taskTmpDir, streamDesc, tableScanOp, cplan.getMapWork(), false, tt_desc);
    opProcCtx.setCurrTopOp(null);
    opProcCtx.setCurrAliasId(null);
    opProcCtx.setCurrTask(childTask);
    opProcCtx.addRootIfPossible(parentTask);
  }

  static boolean hasBranchFinished(Object... children) {
    for (Object child : children) {
      if (child == null) {
        return false;
      }
    }
    return true;
  }



  /**
   * Replace the Map-side operator tree associated with targetAlias in
   * target with the Map-side operator tree associated with sourceAlias in source.
   * @param sourceAlias
   * @param targetAlias
   * @param source
   * @param target
   */
  public static void replaceMapWork(String sourceAlias, String targetAlias,
      MapWork source, MapWork target) {
    Map<String, ArrayList<String>> sourcePathToAliases = source.getPathToAliases();
    Map<String, PartitionDesc> sourcePathToPartitionInfo = source.getPathToPartitionInfo();
    Map<String, Operator<? extends OperatorDesc>> sourceAliasToWork = source.getAliasToWork();
    Map<String, PartitionDesc> sourceAliasToPartnInfo = source.getAliasToPartnInfo();

    Map<String, ArrayList<String>> targetPathToAliases = target.getPathToAliases();
    Map<String, PartitionDesc> targetPathToPartitionInfo = target.getPathToPartitionInfo();
    Map<String, Operator<? extends OperatorDesc>> targetAliasToWork = target.getAliasToWork();
    Map<String, PartitionDesc> targetAliasToPartnInfo = target.getAliasToPartnInfo();

    if (!sourceAliasToWork.containsKey(sourceAlias) ||
        !targetAliasToWork.containsKey(targetAlias)) {
      // Nothing to do if there is no operator tree associated with
      // sourceAlias in source or there is not operator tree associated
      // with targetAlias in target.
      return;
    }

    if (sourceAliasToWork.size() > 1) {
      // If there are multiple aliases in source, we do not know
      // how to merge.
      return;
    }

    // Remove unnecessary information from target
    targetAliasToWork.remove(targetAlias);
    targetAliasToPartnInfo.remove(targetAlias);
    List<String> pathsToRemove = new ArrayList<String>();
    for (Entry<String, ArrayList<String>> entry: targetPathToAliases.entrySet()) {
      ArrayList<String> aliases = entry.getValue();
      aliases.remove(targetAlias);
      if (aliases.isEmpty()) {
        pathsToRemove.add(entry.getKey());
      }
    }
    for (String pathToRemove: pathsToRemove) {
      targetPathToAliases.remove(pathToRemove);
      targetPathToPartitionInfo.remove(pathToRemove);
    }

    // Add new information from source to target
    targetAliasToWork.put(sourceAlias, sourceAliasToWork.get(sourceAlias));
    targetAliasToPartnInfo.putAll(sourceAliasToPartnInfo);
    targetPathToPartitionInfo.putAll(sourcePathToPartitionInfo);
    List<String> pathsToAdd = new ArrayList<String>();
    for (Entry<String, ArrayList<String>> entry: sourcePathToAliases.entrySet()) {
      ArrayList<String> aliases = entry.getValue();
      if (aliases.contains(sourceAlias)) {
        pathsToAdd.add(entry.getKey());
      }
    }
    for (String pathToAdd: pathsToAdd) {
      if (!targetPathToAliases.containsKey(pathToAdd)) {
        targetPathToAliases.put(pathToAdd, new ArrayList<String>());
      }
      targetPathToAliases.get(pathToAdd).add(sourceAlias);
    }
  }

  /**
   * @param fsInput The FileSink operator.
   * @param ctx The MR processing context.
   * @param finalName the final destination path the merge job should output.
   * @param dependencyTask
   * @param mvTasks
   * @param conf
   * @param currTask
   * @throws SemanticException

   * create a Map-only merge job using CombineHiveInputFormat for all partitions with
   * following operators:
   *          MR job J0:
   *          ...
   *          |
   *          v
   *          FileSinkOperator_1 (fsInput)
   *          |
   *          v
   *          Merge job J1:
   *          |
   *          v
   *          TableScan (using CombineHiveInputFormat) (tsMerge)
   *          |
   *          v
   *          FileSinkOperator (fsMerge)
   *
   *          Here the pathToPartitionInfo & pathToAlias will remain the same, which means the paths
   *          do
   *          not contain the dynamic partitions (their parent). So after the dynamic partitions are
   *          created (after the first job finished before the moveTask or ConditionalTask start),
   *          we need to change the pathToPartitionInfo & pathToAlias to include the dynamic
   *          partition
   *          directories.
   *
   */
  public static void createMRWorkForMergingFiles (FileSinkOperator fsInput,
   Path finalName, DependencyCollectionTask dependencyTask,
   List<Task<MoveWork>> mvTasks, HiveConf conf,
   Task<? extends Serializable> currTask) throws SemanticException {

    //
    // 1. create the operator tree
    //
    FileSinkDesc fsInputDesc = fsInput.getConf();

    // Create a TableScan operator
    RowSchema inputRS = fsInput.getSchema();
    Operator<? extends OperatorDesc> tsMerge =
        GenMapRedUtils.createTemporaryTableScanOperator(inputRS);

    // Create a FileSink operator
    TableDesc ts = (TableDesc) fsInputDesc.getTableInfo().clone();
    FileSinkDesc fsOutputDesc = new FileSinkDesc(finalName, ts,
      conf.getBoolVar(ConfVars.COMPRESSRESULT));
    FileSinkOperator fsOutput = (FileSinkOperator) OperatorFactory.getAndMakeChild(
      fsOutputDesc, inputRS, tsMerge);

    // If the input FileSinkOperator is a dynamic partition enabled, the tsMerge input schema
    // needs to include the partition column, and the fsOutput should have
    // a DynamicPartitionCtx to indicate that it needs to dynamically partitioned.
    DynamicPartitionCtx dpCtx = fsInputDesc.getDynPartCtx();
    if (dpCtx != null && dpCtx.getNumDPCols() > 0) {
      // adding DP ColumnInfo to the RowSchema signature
      ArrayList<ColumnInfo> signature = inputRS.getSignature();
      String tblAlias = fsInputDesc.getTableInfo().getTableName();
      LinkedHashMap<String, String> colMap = new LinkedHashMap<String, String>();
      StringBuilder partCols = new StringBuilder();
      for (String dpCol : dpCtx.getDPColNames()) {
        ColumnInfo colInfo = new ColumnInfo(dpCol,
            TypeInfoFactory.stringTypeInfo, // all partition column type should be string
            tblAlias, true); // partition column is virtual column
        signature.add(colInfo);
        colMap.put(dpCol, dpCol); // input and output have the same column name
        partCols.append(dpCol).append('/');
      }
      partCols.setLength(partCols.length() - 1); // remove the last '/'
      inputRS.setSignature(signature);

      // create another DynamicPartitionCtx, which has a different input-to-DP column mapping
      DynamicPartitionCtx dpCtx2 = new DynamicPartitionCtx(dpCtx);
      dpCtx2.setInputToDPCols(colMap);
      fsOutputDesc.setDynPartCtx(dpCtx2);

      // update the FileSinkOperator to include partition columns
      fsInputDesc.getTableInfo().getProperties().setProperty(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS,
        partCols.toString()); // list of dynamic partition column names
    } else {
      // non-partitioned table
      fsInputDesc.getTableInfo().getProperties().remove(
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS);
    }

    //
    // 2. Constructing a conditional task consisting of a move task and a map reduce task
    //
    MoveWork dummyMv = new MoveWork(null, null, null,
         new LoadFileDesc(fsInputDesc.getFinalDirName(), finalName, true, null, null), false);
    MapWork cplan;
    Serializable work;

    if (conf.getBoolVar(ConfVars.HIVEMERGERCFILEBLOCKLEVEL) &&
        fsInputDesc.getTableInfo().getInputFileFormatClass().equals(RCFileInputFormat.class)) {

      // Check if InputFormatClass is valid
      String inputFormatClass = conf.getVar(ConfVars.HIVEMERGEINPUTFORMATBLOCKLEVEL);
      try {
        Class c = (Class<? extends InputFormat>) Class.forName(inputFormatClass);

        LOG.info("RCFile format- Using block level merge");
        cplan = GenMapRedUtils.createRCFileMergeTask(fsInputDesc, finalName,
            dpCtx != null && dpCtx.getNumDPCols() > 0);
        work = cplan;
      } catch (ClassNotFoundException e) {
        String msg = "Illegal input format class: " + inputFormatClass;
        throw new SemanticException(msg);
      }

    } else {
      cplan = createMRWorkForMergingFiles(conf, tsMerge, fsInputDesc);
      if (conf.getVar(ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
        work = new TezWork();
        cplan.setName("Merge");
        ((TezWork)work).add(cplan);
      } else {
        work = new MapredWork();
        ((MapredWork)work).setMapWork(cplan);
      }
    }
    // use CombineHiveInputFormat for map-only merging
    cplan.setInputformat("org.apache.hadoop.hive.ql.io.CombineHiveInputFormat");
    // NOTE: we should gather stats in MR1 rather than MR2 at merge job since we don't
    // know if merge MR2 will be triggered at execution time
    ConditionalTask cndTsk = GenMapRedUtils.createCondTask(conf, currTask, dummyMv, work,
        fsInputDesc.getFinalDirName().toString());

    // keep the dynamic partition context in conditional task resolver context
    ConditionalResolverMergeFilesCtx mrCtx =
        (ConditionalResolverMergeFilesCtx) cndTsk.getResolverCtx();
    mrCtx.setDPCtx(fsInputDesc.getDynPartCtx());
    mrCtx.setLbCtx(fsInputDesc.getLbCtx());

    //
    // 3. add the moveTask as the children of the conditional task
    //
    linkMoveTask(fsOutput, cndTsk, mvTasks, conf, dependencyTask);
  }

  /**
   * Make the move task in the GenMRProcContext following the FileSinkOperator a dependent of all
   * possible subtrees branching from the ConditionalTask.
   *
   * @param newOutput
   * @param cndTsk
   * @param mvTasks
   * @param hconf
   * @param dependencyTask
   */
  public static void linkMoveTask(FileSinkOperator newOutput,
      ConditionalTask cndTsk, List<Task<MoveWork>> mvTasks, HiveConf hconf,
      DependencyCollectionTask dependencyTask) {

    Task<MoveWork> mvTask = GenMapRedUtils.findMoveTask(mvTasks, newOutput);

    for (Task<? extends Serializable> tsk : cndTsk.getListTasks()) {
      linkMoveTask(mvTask, tsk, hconf, dependencyTask);
    }
  }

  /**
   * Follows the task tree down from task and makes all leaves parents of mvTask
   *
   * @param mvTask
   * @param task
   * @param hconf
   * @param dependencyTask
   */
  public static void linkMoveTask(Task<MoveWork> mvTask,
      Task<? extends Serializable> task, HiveConf hconf,
      DependencyCollectionTask dependencyTask) {

    if (task.getDependentTasks() == null || task.getDependentTasks().isEmpty()) {
      // If it's a leaf, add the move task as a child
      addDependentMoveTasks(mvTask, hconf, task, dependencyTask);
    } else {
      // Otherwise, for each child run this method recursively
      for (Task<? extends Serializable> childTask : task.getDependentTasks()) {
        linkMoveTask(mvTask, childTask, hconf, dependencyTask);
      }
    }
  }

  /**
   * Adds the dependencyTaskForMultiInsert in ctx as a dependent of parentTask. If mvTask is a
   * load table, and HIVE_MULTI_INSERT_ATOMIC_OUTPUTS is set, adds mvTask as a dependent of
   * dependencyTaskForMultiInsert in ctx, otherwise adds mvTask as a dependent of parentTask as
   * well.
   *
   * @param mvTask
   * @param hconf
   * @param parentTask
   * @param dependencyTask
   */
  public static void addDependentMoveTasks(Task<MoveWork> mvTask, HiveConf hconf,
      Task<? extends Serializable> parentTask, DependencyCollectionTask dependencyTask) {

    if (mvTask != null) {
      if (dependencyTask != null) {
        parentTask.addDependentTask(dependencyTask);
        if (mvTask.getWork().getLoadTableWork() != null) {
          // Moving tables/partitions depend on the dependencyTask
          dependencyTask.addDependentTask(mvTask);
        } else {
          // Moving files depends on the parentTask (we still want the dependencyTask to depend
          // on the parentTask)
          parentTask.addDependentTask(mvTask);
        }
      } else {
        parentTask.addDependentTask(mvTask);
      }
    }
  }


  /**
   * Add the StatsTask as a dependent task of the MoveTask
   * because StatsTask will change the Table/Partition metadata. For atomicity, we
   * should not change it before the data is actually there done by MoveTask.
   *
   * @param nd
   *          the FileSinkOperator whose results are taken care of by the MoveTask.
   * @param mvTask
   *          The MoveTask that moves the FileSinkOperator's results.
   * @param currTask
   *          The MapRedTask that the FileSinkOperator belongs to.
   * @param hconf
   *          HiveConf
   */
  public static void addStatsTask(FileSinkOperator nd, MoveTask mvTask,
      Task<? extends Serializable> currTask, HiveConf hconf) {

    MoveWork mvWork = mvTask.getWork();
    StatsWork statsWork = null;
    if (mvWork.getLoadTableWork() != null) {
      statsWork = new StatsWork(mvWork.getLoadTableWork());
    } else if (mvWork.getLoadFileWork() != null) {
      statsWork = new StatsWork(mvWork.getLoadFileWork());
    }
    assert statsWork != null : "Error when genereting StatsTask";

    statsWork.setSourceTask(currTask);
    statsWork.setStatsReliable(hconf.getBoolVar(ConfVars.HIVE_STATS_RELIABLE));

    if (currTask.getWork() instanceof MapredWork) {
      MapredWork mrWork = (MapredWork) currTask.getWork();
      mrWork.getMapWork().setGatheringStats(true);
      if (mrWork.getReduceWork() != null) {
        mrWork.getReduceWork().setGatheringStats(true);
      }
    } else {
      TezWork work = (TezWork) currTask.getWork();
      for (BaseWork w: work.getAllWork()) {
        w.setGatheringStats(true);
      }
    }

    // AggKey in StatsWork is used for stats aggregation while StatsAggPrefix
    // in FileSinkDesc is used for stats publishing. They should be consistent.
    statsWork.setAggKey(nd.getConf().getStatsAggPrefix());
    Task<? extends Serializable> statsTask = TaskFactory.get(statsWork, hconf);

    // mark the MapredWork and FileSinkOperator for gathering stats
    nd.getConf().setGatherStats(true);
    nd.getConf().setStatsReliable(hconf.getBoolVar(ConfVars.HIVE_STATS_RELIABLE));
    nd.getConf().setMaxStatsKeyPrefixLength(StatsFactory.getMaxPrefixLength(hconf));
    // mrWork.addDestinationTable(nd.getConf().getTableInfo().getTableName());

    // subscribe feeds from the MoveTask so that MoveTask can forward the list
    // of dynamic partition list to the StatsTask
    mvTask.addDependentTask(statsTask);
    statsTask.subscribeFeed(mvTask);
  }

  /**
   * Returns true iff current query is an insert into for the given file sink
   *
   * @param parseCtx
   * @param fsOp
   * @return
   */
  public static boolean isInsertInto(ParseContext parseCtx, FileSinkOperator fsOp) {
    return fsOp.getConf().getTableInfo().getTableName() != null &&
        parseCtx.getQB().getParseInfo().isInsertToTable();
  }

  /**
   * Create a MapredWork based on input path, the top operator and the input
   * table descriptor.
   *
   * @param conf
   * @param topOp
   *          the table scan operator that is the root of the MapReduce task.
   * @param fsDesc
   *          the file sink descriptor that serves as the input to this merge task.
   * @param parentMR
   *          the parent MapReduce work
   * @param parentFS
   *          the last FileSinkOperator in the parent MapReduce work
   * @return the MapredWork
   */
  private static MapWork createMRWorkForMergingFiles (HiveConf conf,
    Operator<? extends OperatorDesc> topOp,  FileSinkDesc fsDesc) {

    ArrayList<String> aliases = new ArrayList<String>();
    String inputDir = fsDesc.getFinalDirName().toString();
    TableDesc tblDesc = fsDesc.getTableInfo();
    aliases.add(inputDir); // dummy alias: just use the input path

    // constructing the default MapredWork
    MapredWork cMrPlan = GenMapRedUtils.getMapRedWorkFromConf(conf);
    MapWork cplan = cMrPlan.getMapWork();
    cplan.getPathToAliases().put(inputDir, aliases);
    cplan.getPathToPartitionInfo().put(inputDir, new PartitionDesc(tblDesc, null));
    cplan.getAliasToWork().put(inputDir, topOp);
    cplan.setMapperCannotSpanPartns(true);

    return cplan;
  }

  /**
   * Create a block level merge task for RCFiles.
   *
   * @param fsInputDesc
   * @param finalName
   * @return MergeWork if table is stored as RCFile,
   *         null otherwise
   */
  public static MapWork createRCFileMergeTask(FileSinkDesc fsInputDesc,
      Path finalName, boolean hasDynamicPartitions) throws SemanticException {

    Path inputDir = fsInputDesc.getFinalDirName();
    TableDesc tblDesc = fsInputDesc.getTableInfo();

    if (tblDesc.getInputFileFormatClass().equals(RCFileInputFormat.class)) {
      ArrayList<Path> inputDirs = new ArrayList<Path>(1);
      ArrayList<String> inputDirstr = new ArrayList<String>(1);
      if (!hasDynamicPartitions
          && !GenMapRedUtils.isSkewedStoredAsDirs(fsInputDesc)) {
        inputDirs.add(inputDir);
        inputDirstr.add(inputDir.toString());
      }

      MergeWork work = new MergeWork(inputDirs, finalName,
          hasDynamicPartitions, fsInputDesc.getDynPartCtx());
      LinkedHashMap<String, ArrayList<String>> pathToAliases =
          new LinkedHashMap<String, ArrayList<String>>();
      pathToAliases.put(inputDir.toString(), (ArrayList<String>) inputDirstr.clone());
      work.setMapperCannotSpanPartns(true);
      work.setPathToAliases(pathToAliases);
      work.setAliasToWork(
          new LinkedHashMap<String, Operator<? extends OperatorDesc>>());
      if (hasDynamicPartitions
          || GenMapRedUtils.isSkewedStoredAsDirs(fsInputDesc)) {
        work.getPathToPartitionInfo().put(inputDir.toString(),
            new PartitionDesc(tblDesc, null));
      }
      work.setListBucketingCtx(fsInputDesc.getLbCtx());

      return work;
    }

    throw new SemanticException("createRCFileMergeTask called on non-RCFile table");
  }

  /**
   * Construct a conditional task given the current leaf task, the MoveWork and the MapredWork.
   *
   * @param conf
   *          HiveConf
   * @param currTask
   *          current leaf task
   * @param mvWork
   *          MoveWork for the move task
   * @param mergeWork
   *          MapredWork for the merge task.
   * @param inputPath
   *          the input directory of the merge/move task
   * @return The conditional task
   */
  @SuppressWarnings("unchecked")
  public static ConditionalTask createCondTask(HiveConf conf,
      Task<? extends Serializable> currTask, MoveWork mvWork,
      Serializable mergeWork, String inputPath) {

    // There are 3 options for this ConditionalTask:
    // 1) Merge the partitions
    // 2) Move the partitions (i.e. don't merge the partitions)
    // 3) Merge some partitions and move other partitions (i.e. merge some partitions and don't
    // merge others) in this case the merge is done first followed by the move to prevent
    // conflicts.
    Task<? extends Serializable> mergeOnlyMergeTask = TaskFactory.get(mergeWork, conf);
    Task<? extends Serializable> moveOnlyMoveTask = TaskFactory.get(mvWork, conf);
    Task<? extends Serializable> mergeAndMoveMergeTask = TaskFactory.get(mergeWork, conf);
    Task<? extends Serializable> mergeAndMoveMoveTask = TaskFactory.get(mvWork, conf);

    // NOTE! It is necessary merge task is the parent of the move task, and not
    // the other way around, for the proper execution of the execute method of
    // ConditionalTask
    mergeAndMoveMergeTask.addDependentTask(mergeAndMoveMoveTask);

    List<Serializable> listWorks = new ArrayList<Serializable>();
    listWorks.add(mvWork);
    listWorks.add(mergeWork);

    ConditionalWork cndWork = new ConditionalWork(listWorks);

    List<Task<? extends Serializable>> listTasks = new ArrayList<Task<? extends Serializable>>();
    listTasks.add(moveOnlyMoveTask);
    listTasks.add(mergeOnlyMergeTask);
    listTasks.add(mergeAndMoveMergeTask);

    ConditionalTask cndTsk = (ConditionalTask) TaskFactory.get(cndWork, conf);
    cndTsk.setListTasks(listTasks);

    // create resolver
    cndTsk.setResolver(new ConditionalResolverMergeFiles());
    ConditionalResolverMergeFilesCtx mrCtx =
        new ConditionalResolverMergeFilesCtx(listTasks, inputPath);
    cndTsk.setResolverCtx(mrCtx);

    // make the conditional task as the child of the current leaf task
    currTask.addDependentTask(cndTsk);

    return cndTsk;
  }

  /**
   * check if it is skewed table and stored as dirs.
   *
   * @param fsInputDesc
   * @return
   */
  public static boolean isSkewedStoredAsDirs(FileSinkDesc fsInputDesc) {
    return (fsInputDesc.getLbCtx() == null) ? false : fsInputDesc.getLbCtx()
        .isSkewedStoredAsDir();
  }

  public static Task<MoveWork> findMoveTask(
      List<Task<MoveWork>> mvTasks, FileSinkOperator fsOp) {
    // find the move task
    for (Task<MoveWork> mvTsk : mvTasks) {
      MoveWork mvWork = mvTsk.getWork();
      Path srcDir = null;
      if (mvWork.getLoadFileWork() != null) {
        srcDir = mvWork.getLoadFileWork().getSourcePath();
      } else if (mvWork.getLoadTableWork() != null) {
        srcDir = mvWork.getLoadTableWork().getSourcePath();
      }

      if ((srcDir != null)
          && (srcDir.equals(fsOp.getConf().getFinalDirName()))) {
        return mvTsk;
      }
    }
    return null;
  }

  /**
   * Returns true iff the fsOp requires a merge
   * @param mvTasks
   * @param hconf
   * @param fsOp
   * @param currTask
   * @param isInsertTable
   * @return
   */
  public static boolean isMergeRequired(List<Task<MoveWork>> mvTasks, HiveConf hconf, FileSinkOperator fsOp,
      Task<? extends Serializable> currTask, boolean isInsertTable) {

    // Has the user enabled merging of files for map-only jobs or for all jobs
    if ((mvTasks != null) && (!mvTasks.isEmpty())) {

      // no need of merging if the move is to a local file system
      MoveTask mvTask = (MoveTask) GenMapRedUtils.findMoveTask(mvTasks, fsOp);

      if (mvTask != null && isInsertTable && hconf.getBoolVar(ConfVars.HIVESTATSAUTOGATHER)) {
        GenMapRedUtils.addStatsTask(fsOp, mvTask, currTask, hconf);
      }

      if ((mvTask != null) && !mvTask.isLocal() && fsOp.getConf().canBeMerged()) {
        if (fsOp.getConf().isLinkedFileSink()) {
          // If the user has HIVEMERGEMAPREDFILES set to false, the idea was the
          // number of reducers are few, so the number of files anyway are small.
          // However, with this optimization, we are increasing the number of files
          // possibly by a big margin. So, merge aggresively.
          if (hconf.getBoolVar(ConfVars.HIVEMERGEMAPFILES) ||
              hconf.getBoolVar(ConfVars.HIVEMERGEMAPREDFILES)) {
            return true;
          }
        } else {
          // There are separate configuration parameters to control whether to
          // merge for a map-only job
          // or for a map-reduce job
          if (currTask.getWork() instanceof TezWork) {
            return hconf.getBoolVar(ConfVars.HIVEMERGEMAPFILES) || 
                hconf.getBoolVar(ConfVars.HIVEMERGEMAPREDFILES);
          } else if (currTask.getWork() instanceof MapredWork) {
            ReduceWork reduceWork = ((MapredWork) currTask.getWork()).getReduceWork();
            boolean mergeMapOnly =
                hconf.getBoolVar(ConfVars.HIVEMERGEMAPFILES) && reduceWork == null;
            boolean mergeMapRed =
                hconf.getBoolVar(ConfVars.HIVEMERGEMAPREDFILES) &&
                reduceWork != null;
            if (mergeMapOnly || mergeMapRed) {
              return true;
            }
          } else {
            return false;
          }
        }
      }
    }
    return false;
  }

  /**
   * Create and add any dependent move tasks
   *
   * @param currTask
   * @param chDir
   * @param fsOp
   * @param parseCtx
   * @param mvTasks
   * @param hconf
   * @param dependencyTask
   * @return
   */
  public static Path createMoveTask(Task<? extends Serializable> currTask, boolean chDir,
      FileSinkOperator fsOp, ParseContext parseCtx, List<Task<MoveWork>> mvTasks,
      HiveConf hconf, DependencyCollectionTask dependencyTask) {

    Path dest = null;

    if (chDir) {
      dest = fsOp.getConf().getFinalDirName();

      // generate the temporary file
      // it must be on the same file system as the current destination
      Context baseCtx = parseCtx.getContext();
      Path tmpDir = baseCtx.getExternalTmpPath(dest.toUri());

      FileSinkDesc fileSinkDesc = fsOp.getConf();
      // Change all the linked file sink descriptors
      if (fileSinkDesc.isLinkedFileSink()) {
        for (FileSinkDesc fsConf:fileSinkDesc.getLinkedFileSinkDesc()) {
          fsConf.setParentDir(tmpDir);
          fsConf.setDirName(new Path(tmpDir, fsConf.getDirName().getName()));
        }
      } else {
        fileSinkDesc.setDirName(tmpDir);
      }
    }

    Task<MoveWork> mvTask = null;

    if (!chDir) {
      mvTask = GenMapRedUtils.findMoveTask(mvTasks, fsOp);
    }

    // Set the move task to be dependent on the current task
    if (mvTask != null) {
      GenMapRedUtils.addDependentMoveTasks(mvTask, hconf, currTask, dependencyTask);
    }

    return dest;
  }

  private GenMapRedUtils() {
    // prevent instantiation
  }
}
