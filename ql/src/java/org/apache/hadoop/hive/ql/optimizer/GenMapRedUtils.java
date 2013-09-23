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
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.DemuxOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecDriver;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMRUnionCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMapRedCtx;
import org.apache.hadoop.hive.ql.optimizer.listbucketingpruner.ListBucketingPruner;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc.sampleDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
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

  private static boolean needsTagging(ReduceWork rWork) {
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
    MapWork plan = ((MapredWork) task.getWork()).getMapWork();
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
            new FetchWork(FetchWork.convertPathToStringArray(partDir), partDesc, tblDesc));
      } else {
        localPlan.getAliasToFetchWork().put(alias_id,
            new FetchWork(tblDir.toString(), tblDesc));
      }
      plan.setMapLocalWork(localPlan);
    }
    opProcCtx.addSeenOp(task, topOp);
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
  public static void setKeyAndValueDesc(ReduceWork plan,
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
    FileSinkDesc desc = new FileSinkDesc(taskTmpDir, tt_desc, compressIntermediate);
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
      String origStreamDesc;
      streamDesc = "$INTNAME";
      origStreamDesc = streamDesc;
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

  private GenMapRedUtils() {
    // prevent instantiation
  }
}
