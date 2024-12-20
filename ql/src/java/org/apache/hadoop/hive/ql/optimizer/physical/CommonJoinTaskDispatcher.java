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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewForwardOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.optimizer.MapJoinProcessor;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverCommonJoin;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverCommonJoin.ConditionalResolverCommonJoinCtx;
import org.apache.hadoop.hive.ql.plan.ConditionalWork;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Convert tasks involving JOIN into MAPJOIN.
 * If hive.auto.convert.join is true, the tasks involving join are converted.
 * Consider the query:
 * select .... from T1 join T2 on T1.key = T2.key join T3 on T1.key = T3.key
 *
 * There is a map-reduce task which performs a 3-way join (T1, T2, T3).
 * The task would be converted to a conditional task which would have 4 children
 * a. Mapjoin considering T1 as the big table
 * b. Mapjoin considering T2 as the big table
 * c. Mapjoin considering T3 as the big table
 * d. Map-reduce join (the original task).
 *
 *  Note that the sizes of all the inputs may not be available at compile time. At runtime, it is
 *  determined which branch we want to pick up from the above.
 *
 * However, if hive.auto.convert.join.noconditionaltask is set to true, and
 * the sum of any n-1 tables is smaller than hive.auto.convert.join.noconditionaltask.size,
 * then a mapjoin is created instead of the conditional task. For the above, if the size of
 * T1 + T2 is less than the threshold, then the task is converted to a mapjoin task with T3 as
 * the big table.
 *
 * In this case, further optimization is performed by merging 2 consecutive map-only jobs.
 * Consider the query:
 * select ... from T1 join T2 on T1.key1 = T2.key1 join T3 on T1.key2 = T3.key2
 *
 * Initially, the plan would consist of 2 Map-reduce jobs (1 to perform join for T1 and T2)
 * followed by another map-reduce job (to perform join of the result with T3). After the
 * optimization, both these tasks would be converted to map-only tasks. These 2 map-only jobs
 * are then merged into a single map-only job. As a followup (HIVE-3952), it would be possible to
 * merge a map-only task with a map-reduce task.
 * Consider the query:
 * select T1.key2, count(*) from T1 join T2 on T1.key1 = T2.key1 group by T1.key2;
 * Initially, the plan would consist of 2 Map-reduce jobs (1 to perform join for T1 and T2)
 * followed by another map-reduce job (to perform groupby of the result). After the
 * optimization, the join task would be converted to map-only tasks. After HIVE-3952, the map-only
 * task would be merged with the map-reduce task to create a single map-reduce task.
 */

/**
 * Iterator each tasks. If this task has a local work,create a new task for this local work, named
 * MapredLocalTask. then make this new generated task depends on current task's parent task, and
 * make current task depends on this new generated task
 */
public class CommonJoinTaskDispatcher extends AbstractJoinTaskDispatcher implements SemanticDispatcher {

  protected final Logger LOG = LoggerFactory.getLogger(CommonJoinTaskDispatcher.class);

  HashMap<String, Long> aliasToSize = null;

  public CommonJoinTaskDispatcher(PhysicalContext context) {
    super(context);
  }

  /**
   * Calculate the total size of local tables in loclWork.
   * @param localWork
   * @return the total size of local tables. Or -1, if the total
   * size is unknown.
   */
  private long calculateLocalTableTotalSize(MapredLocalWork localWork) {
    long localTableTotalSize = 0;
    if (localWork == null) {
      return localTableTotalSize;
    }
    for (String alias : localWork.getAliasToWork().keySet()) {
      Long tabSize = aliasToSize.get(alias);
      if (tabSize == null) {
        // if the size is unavailable, we need to assume a size 1 greater than
        // localTableTotalSizeLimit this implies that merge cannot happen
        // so we will return false.
        return -1;
      }
      localTableTotalSize += tabSize;
    }
    return localTableTotalSize;
  }

  /**
   * Check if the total size of local tables will be under
   * the limit after we merge localWork1 and localWork2.
   * The limit of the total size of local tables is defined by
   * HiveConf.ConfVars.HIVE_CONVERT_JOIN_NOCONDITIONAL_TASK_THRESHOLD.
   * @param conf
   * @param localWorks
   * @return
   */
  private boolean isLocalTableTotalSizeUnderLimitAfterMerge(
      Configuration conf,
      MapredLocalWork... localWorks) {
    final long localTableTotalSizeLimit = HiveConf.getLongVar(conf,
        HiveConf.ConfVars.HIVE_CONVERT_JOIN_NOCONDITIONAL_TASK_THRESHOLD);
    long localTableTotalSize = 0;
    for (int i = 0; i < localWorks.length; i++) {
      final long localWorkTableTotalSize = calculateLocalTableTotalSize(localWorks[i]);
      if (localWorkTableTotalSize < 0) {
        // The total size of local tables in localWork[i] is unknown.
        return false;
      }
      localTableTotalSize += localWorkTableTotalSize;
    }

    if (localTableTotalSize > localTableTotalSizeLimit) {
      // The total size of local tables after we merge localWorks
      // is larger than the limit set by
      // HiveConf.ConfVars.HIVE_CONVERT_JOIN_NOCONDITIONAL_TASK_THRESHOLD.
      return false;
    }

    return true;
  }

  // create map join task and set big table as bigTablePosition
  private MapRedTask convertTaskToMapJoinTask(MapredWork newWork, int bigTablePosition)
      throws SemanticException {
    // create a mapred task for this work
    MapRedTask newTask = (MapRedTask) TaskFactory.get(newWork);
    JoinOperator newJoinOp = getJoinOp(newTask);
    // optimize this newWork given the big table position
    MapJoinProcessor.genMapJoinOpAndLocalWork(physicalContext.getParseContext().getConf(),
        newWork, newJoinOp, bigTablePosition);
    return newTask;
  }

  /*
   * A task and its child task has been converted from join to mapjoin.
   * See if the two tasks can be merged.
   */
  private void mergeMapJoinTaskIntoItsChildMapRedTask(MapRedTask mapJoinTask, Configuration conf)
      throws SemanticException{
    // Step 1: Check if mapJoinTask has a single child.
    // If so, check if we can merge mapJoinTask into that child.
    if (mapJoinTask.getChildTasks() == null
        || mapJoinTask.getChildTasks().size() > 1) {
      // No child-task to merge, nothing to do or there are more than one
      // child-tasks in which case we don't want to do anything.
      return;
    }

    Task<?> childTask = mapJoinTask.getChildTasks().get(0);
    if (!(childTask instanceof MapRedTask)) {
      // Nothing to do if it is not a MapReduce task.
      return;
    }

    MapRedTask childMapRedTask = (MapRedTask) childTask;
    MapWork mapJoinMapWork = mapJoinTask.getWork().getMapWork();
    MapWork childMapWork = childMapRedTask.getWork().getMapWork();

    Map<String, Operator<? extends OperatorDesc>> mapJoinAliasToWork =
        mapJoinMapWork.getAliasToWork();
    if (mapJoinAliasToWork.size() > 1) {
      // Do not merge if the MapredWork of MapJoin has multiple input aliases.
      return;
    }

    Entry<String, Operator<? extends OperatorDesc>> mapJoinAliasToWorkEntry =
        mapJoinAliasToWork.entrySet().iterator().next();
    String mapJoinAlias = mapJoinAliasToWorkEntry.getKey();
    TableScanOperator mapJoinTaskTableScanOperator =
        OperatorUtils.findSingleOperator(
            mapJoinAliasToWorkEntry.getValue(), TableScanOperator.class);
    if (mapJoinTaskTableScanOperator == null) {
      throw new SemanticException("Expected a " + TableScanOperator.getOperatorName() +
          " operator as the work associated with alias " + mapJoinAlias +
          ". Found a " + mapJoinAliasToWork.get(mapJoinAlias).getName() + " operator.");
    }

    Set<FileSinkOperator> mapJoinTaskFileSinkOperators = 
        OperatorUtils.findOperators(mapJoinTaskTableScanOperator, FileSinkOperator.class);
    if (mapJoinTaskFileSinkOperators.isEmpty()) {
      throw new SemanticException("Cannot find the " + FileSinkOperator.getOperatorName() +
          " operator at the last operator of the MapJoin Task.");
    }
    if (mapJoinTaskFileSinkOperators.size() > 1) {
      LOG.warn("Multiple " + FileSinkOperator.getOperatorName() + " operators found at the last operator of the MapJoin Task.");
      return;
    }
    // The mapJoinTaskFileSinkOperator writes to a different directory
    FileSinkOperator mapJoinTaskFileSinkOperator = mapJoinTaskFileSinkOperators.iterator().next();
    Path childMRPath = mapJoinTaskFileSinkOperator.getConf().getDirName();
    List<String> childMRAliases = childMapWork.getPathToAliases().get(childMRPath);
    if (childMRAliases == null || childMRAliases.size() != 1) {
      return;
    }
    String childMRAlias = childMRAliases.get(0);

    // Sanity check to make sure there is no alias conflict after merge.
    for (Entry<Path, List<String>> entry : childMapWork.getPathToAliases().entrySet()) {
      Path path = entry.getKey();
      List<String> aliases = entry.getValue();

      if (path.equals(childMRPath)) {
        continue;
      }

      if (aliases.contains(mapJoinAlias)) {
        // alias confict should not happen here.
        return;
      }
    }

    MapredLocalWork mapJoinLocalWork = mapJoinMapWork.getMapRedLocalWork();
    MapredLocalWork childLocalWork = childMapWork.getMapRedLocalWork();

    if ((mapJoinLocalWork != null && mapJoinLocalWork.getBucketMapjoinContext() != null) ||
        (childLocalWork != null && childLocalWork.getBucketMapjoinContext() != null)) {
      // Right now, we do not handle the case that either of them is bucketed.
      // We should relax this constraint with a follow-up jira.
      return;
    }

    // We need to check if the total size of local tables is under the limit.
    // At here, we are using a strong condition, which is the total size of
    // local tables used by all input paths. Actually, we can relax this condition
    // to check the total size of local tables for every input path.
    // Example:
    //               UNION_ALL
    //              /         \
    //             /           \
    //            /             \
    //           /               \
    //       MapJoin1          MapJoin2
    //      /   |   \         /   |   \
    //     /    |    \       /    |    \
    //   Big1   S1   S2    Big2   S3   S4
    // In this case, we have two MapJoins, MapJoin1 and MapJoin2. Big1 and Big2 are two
    // big tables, and S1, S2, S3, and S4 are four small tables. Hash tables of S1 and S2
    // will only be used by Map tasks processing Big1. Hash tables of S3 and S4 will only
    // be used by Map tasks processing Big2. If Big1!=Big2, we should only check if the size
    // of S1 + S2 is under the limit, and if the size of S3 + S4 is under the limit.
    // But, right now, we are checking the size of S1 + S2 + S3 + S4 is under the limit.
    // If Big1=Big2, we will only scan a path once. So, MapJoin1 and MapJoin2 will be executed
    // in the same Map task. In this case, we need to make sure the size of S1 + S2 + S3 + S4
    // is under the limit.
    if (!isLocalTableTotalSizeUnderLimitAfterMerge(conf, mapJoinLocalWork, childLocalWork)){
      // The total size of local tables may not be under
      // the limit after we merge mapJoinLocalWork and childLocalWork.
      // Do not merge.
      return;
    }

    TableScanOperator childMRTaskTableScanOperator =
        OperatorUtils.findSingleOperator(
            childMapWork.getAliasToWork().get(childMRAlias.toString()), TableScanOperator.class);
    if (childMRTaskTableScanOperator == null) {
      throw new SemanticException("Expected a " + TableScanOperator.getOperatorName() +
          " operator as the work associated with alias " + childMRAlias +
          ". Found a " + childMapWork.getAliasToWork().get(childMRAlias).getName() + " operator.");
    }

    List<Operator<? extends OperatorDesc>> parentsInMapJoinTask =
        mapJoinTaskFileSinkOperator.getParentOperators();
    List<Operator<? extends OperatorDesc>> childrenInChildMRTask =
        childMRTaskTableScanOperator.getChildOperators();
    if (parentsInMapJoinTask.size() > 1 || childrenInChildMRTask.size() > 1) {
      // Do not merge if we do not know how to connect two operator trees.
      return;
    }

    // Step 2: Merge mapJoinTask into the Map-side of its child.
    // Step 2.1: Connect the operator trees of two MapRedTasks.
    Operator<? extends OperatorDesc> parentInMapJoinTask = parentsInMapJoinTask.get(0);
    Operator<? extends OperatorDesc> childInChildMRTask = childrenInChildMRTask.get(0);
    parentInMapJoinTask.replaceChild(mapJoinTaskFileSinkOperator, childInChildMRTask);
    childInChildMRTask.replaceParent(childMRTaskTableScanOperator, parentInMapJoinTask);

    // Step 2.2: Replace the corresponding part childMRWork's MapWork.
    GenMapRedUtils.replaceMapWork(mapJoinAlias, childMRAlias.toString(), mapJoinMapWork, childMapWork);

    // Step 2.3: Fill up stuff in local work
    if (mapJoinLocalWork != null) {
      if (childLocalWork == null) {
        childMapWork.setMapRedLocalWork(mapJoinLocalWork);
      } else {
        childLocalWork.getAliasToFetchWork().putAll(mapJoinLocalWork.getAliasToFetchWork());
        childLocalWork.getAliasToWork().putAll(mapJoinLocalWork.getAliasToWork());
      }
    }

    // Step 2.4: Remove this MapJoin task
    List<Task<?>> parentTasks = mapJoinTask.getParentTasks();
    mapJoinTask.setParentTasks(null);
    mapJoinTask.setChildTasks(null);
    childMapRedTask.getParentTasks().remove(mapJoinTask);
    if (parentTasks != null) {
      childMapRedTask.getParentTasks().addAll(parentTasks);
      for (Task<?> parentTask : parentTasks) {
        parentTask.getChildTasks().remove(mapJoinTask);
        if (!parentTask.getChildTasks().contains(childMapRedTask)) {
          parentTask.getChildTasks().add(childMapRedTask);
        }
      }
    } else {
      if (physicalContext.getRootTasks().contains(mapJoinTask)) {
        physicalContext.removeFromRootTask(mapJoinTask);
        if (childMapRedTask.getParentTasks() != null &&
            childMapRedTask.getParentTasks().size() == 0 &&
            !physicalContext.getRootTasks().contains(childMapRedTask)) {
          physicalContext.addToRootTask(childMapRedTask);
        }
      }
    }
    if (childMapRedTask.getParentTasks().size() == 0) {
      childMapRedTask.setParentTasks(null);
    }
  }

  public static boolean cannotConvert(long aliasKnownSize,
      long aliasTotalKnownInputSize, long ThresholdOfSmallTblSizeSum) {
    if (aliasKnownSize > 0 &&
        aliasTotalKnownInputSize - aliasKnownSize > ThresholdOfSmallTblSizeSum) {
      return true;
    }
    return false;
  }

  @Override
  public Task<?> processCurrentTask(MapRedTask currTask,
      ConditionalTask conditionalTask, Context context)
      throws SemanticException {

    // whether it contains common join op; if contains, return this common join op
    JoinOperator joinOp = getJoinOp(currTask);
    if (joinOp == null || joinOp.getConf().isFixedAsSorted()) {
      return null;
    }
    currTask.setTaskTag(Task.COMMON_JOIN);

    MapWork currWork = currTask.getWork().getMapWork();

    // create conditional work list and task list
    List<Serializable> listWorks = new ArrayList<Serializable>();
    List<Task<?>> listTasks = new ArrayList<Task<?>>();

    // create task to aliases mapping and alias to input file mapping for resolver
    // Must be deterministic order map for consistent q-test output across Java versions
    HashMap<Task<?>, Set<String>> taskToAliases =
        new LinkedHashMap<Task<?>, Set<String>>();
    Map<Path, List<String>> pathToAliases = currWork.getPathToAliases();
    Map<String, Operator<? extends OperatorDesc>> aliasToWork = currWork.getAliasToWork();

    // start to generate multiple map join tasks
    JoinDesc joinDesc = joinOp.getConf();

    if (aliasToSize == null) {
      aliasToSize = new HashMap<String, Long>();
    }

    try {
      long aliasTotalKnownInputSize =
          getTotalKnownInputSize(context, currWork, pathToAliases, aliasToSize);

      Set<Integer> bigTableCandidates = MapJoinProcessor.getBigTableCandidates(joinDesc
          .getConds());

      // no table could be the big table; there is no need to convert
      if (bigTableCandidates.isEmpty()) {
        return null;
      }

      // if any of bigTableCandidates is from multi-sourced, bigTableCandidates should
      // only contain multi-sourced because multi-sourced cannot be hashed or direct readable
      bigTableCandidates = multiInsertBigTableCheck(joinOp, bigTableCandidates);

      Configuration conf = context.getConf();

      // If sizes of at least n-1 tables in a n-way join is known, and their sum is smaller than
      // the threshold size, convert the join into map-join and don't create a conditional task
      boolean convertJoinMapJoin = HiveConf.getBoolVar(conf,
          HiveConf.ConfVars.HIVE_CONVERT_JOIN_NOCONDITIONALTASK);
      int bigTablePosition = -1;
      if (convertJoinMapJoin) {
        // This is the threshold that the user has specified to fit in mapjoin
        long mapJoinSize = HiveConf.getLongVar(conf,
            HiveConf.ConfVars.HIVE_CONVERT_JOIN_NOCONDITIONAL_TASK_THRESHOLD);

        Long bigTableSize = null;
        Set<String> aliases = aliasToWork.keySet();
        for (int tablePosition : bigTableCandidates) {
          Operator<?> parent = joinOp.getParentOperators().get(tablePosition);
          Set<String> participants = GenMapRedUtils.findAliases(currWork, parent);
          long sumOfOthers = Utilities.sumOfExcept(aliasToSize, aliases, participants);
          if (sumOfOthers < 0 || sumOfOthers > mapJoinSize) {
            continue; // some small alias is not known or too big
          }
          if (bigTableSize == null && bigTablePosition >= 0 && tablePosition < bigTablePosition) {
            continue; // prefer right most alias
          }
          long aliasSize = Utilities.sumOf(aliasToSize, participants);
          if (bigTableSize == null || bigTableSize < 0 || (aliasSize >= 0 && aliasSize >= bigTableSize)) {
            bigTablePosition = tablePosition;
            bigTableSize = aliasSize;
          }
        }
      }

      currWork.setLeftInputJoin(joinOp.getConf().isLeftInputJoin());
      currWork.setBaseSrc(joinOp.getConf().getBaseSrc());
      currWork.setMapAliases(joinOp.getConf().getMapAliases());

      if (bigTablePosition >= 0) {
        // create map join task and set big table as bigTablePosition
        MapRedTask newTask = convertTaskToMapJoinTask(currTask.getWork(), bigTablePosition);

        newTask.setTaskTag(Task.MAPJOIN_ONLY_NOBACKUP);
        newTask.setFetchSource(currTask.isFetchSource());
        replaceTask(currTask, newTask);

        // Can this task be merged with the child task. This can happen if a big table is being
        // joined with multiple small tables on different keys
        if ((newTask.getChildTasks() != null) && (newTask.getChildTasks().size() == 1)) {
          mergeMapJoinTaskIntoItsChildMapRedTask(newTask, conf);
        }

        return newTask;
      }

      long ThresholdOfSmallTblSizeSum = HiveConf.getLongVar(conf,
          HiveConf.ConfVars.HIVE_SMALL_TABLES_FILESIZE);
      for (int pos = 0; pos < joinOp.getNumParent(); pos++) {
        // this table cannot be big table
        if (!bigTableCandidates.contains(pos)) {
          continue;
        }

        Operator<?> startOp = joinOp.getParentOperators().get(pos);
        Set<String> aliases = GenMapRedUtils.findAliases(currWork, startOp);

        long aliasKnownSize = Utilities.sumOf(aliasToSize, aliases);
        if (cannotConvert(aliasKnownSize, aliasTotalKnownInputSize, ThresholdOfSmallTblSizeSum)) {
          continue;
        }

        MapredWork newWork = SerializationUtilities.clonePlan(currTask.getWork());

        // create map join task and set big table as i
        MapRedTask newTask = convertTaskToMapJoinTask(newWork, pos);

        // add into conditional task
        listWorks.add(newTask.getWork());
        listTasks.add(newTask);
        newTask.setTaskTag(Task.CONVERTED_MAPJOIN);
        newTask.setFetchSource(currTask.isFetchSource());

        // set up backup task
        newTask.setBackupTask(currTask);
        newTask.setBackupChildrenTasks(currTask.getChildTasks());

        // put the mapping task to aliases
        taskToAliases.put(newTask, aliases);
      }
    } catch (Exception e) {
      throw new SemanticException("Generate Map Join Task Error: " + e.getMessage(), e);
    }

    if (listTasks.isEmpty()) {
      return currTask;
    }

    // insert current common join task to conditional task
    listWorks.add(currTask.getWork());
    listTasks.add(currTask);
    // clear JoinTree and OP Parse Context
    currWork.setLeftInputJoin(false);
    currWork.setBaseSrc(null);
    currWork.setMapAliases(null);

    // create conditional task and insert conditional task into task tree
    ConditionalWork cndWork = new ConditionalWork(listWorks);
    ConditionalTask cndTsk = (ConditionalTask) TaskFactory.get(cndWork);
    cndTsk.setListTasks(listTasks);

    // set resolver and resolver context
    cndTsk.setResolver(new ConditionalResolverCommonJoin());
    ConditionalResolverCommonJoinCtx resolverCtx = new ConditionalResolverCommonJoinCtx();
    resolverCtx.setPathToAliases(pathToAliases);
    resolverCtx.setAliasToKnownSize(aliasToSize);
    resolverCtx.setTaskToAliases(taskToAliases);
    resolverCtx.setCommonJoinTask(currTask);
    resolverCtx.setLocalTmpDir(context.getLocalScratchDir(false));
    resolverCtx.setHdfsTmpDir(context.getMRScratchDir());
    cndTsk.setResolverCtx(resolverCtx);

    // replace the current task with the new generated conditional task
    replaceTaskWithConditionalTask(currTask, cndTsk);
    return cndTsk;
  }

  /*
   * If any operator which does not allow map-side conversion is present in the mapper, dont
   * convert it into a conditional task.
   */
  private boolean checkOperatorOKMapJoinConversion(Operator<? extends OperatorDesc> op) {
    if (!op.opAllowedConvertMapJoin()) {
      return false;
    }

    for (Operator<? extends OperatorDesc> childOp : op.getChildOperators()) {
      if (!checkOperatorOKMapJoinConversion(childOp)) {
        return false;
      }
    }

    return true;
  }

  private JoinOperator getJoinOp(MapRedTask task) throws SemanticException {
    MapWork mWork = task.getWork().getMapWork();
    ReduceWork rWork = task.getWork().getReduceWork();
    if (rWork == null) {
      return null;
    }
    Operator<? extends OperatorDesc> reducerOp = rWork.getReducer();
    if (reducerOp instanceof JoinOperator) {
      /* Is any operator present, which prevents the conversion */
      Map<String, Operator<? extends OperatorDesc>> aliasToWork = mWork.getAliasToWork();
      for (Operator<? extends OperatorDesc> op : aliasToWork.values()) {
        if (!checkOperatorOKMapJoinConversion(op)) {
          return null;
        }
      }
      return (JoinOperator) reducerOp;
    } else {
      return null;
    }
  }


  /**
   * In the case of a multi-insert statement the Source Operator will have multiple children.
   * For e.g.
   * from src b
   * INSERT OVERWRITE TABLE src_4
   *  select *
   *  where b.key in
   *    (select a.key from src a where b.value = a.value and a.key > '9')
   * INSERT OVERWRITE TABLE src_5
   * select *
   * where b.key not in
   *   ( select key from src s1 where s1.key > '2')
   *
   * The TableScan on 'src'(for alias b) will have 2 children one for each destination.
   *
   * In such cases only the Source side of the Join is the candidate Big Table.
   * The reason being, it cannot be replaced by a HashTable as its rows must flow into the other children
   * of the TableScan Operator.
   */
  private Set<Integer> multiInsertBigTableCheck(JoinOperator joinOp, Set<Integer> bigTableCandidates) {
    int multiChildrenSource = -1;
    for (int tablePosition : bigTableCandidates.toArray(new Integer[0])) {
      Operator<?> parent = joinOp.getParentOperators().get(tablePosition);
      for (; parent != null;
           parent = parent.getNumParent() > 0 ? parent.getParentOperators().get(0) : null) {
        if (parent.getNumChild() > 1 && !(parent instanceof LateralViewForwardOperator)) {
          if (multiChildrenSource >= 0) {
            return Collections.emptySet();
          }
          multiChildrenSource = tablePosition;
        }
      }
    }
    return multiChildrenSource < 0 ? bigTableCandidates :
        new HashSet<Integer>(Arrays.asList(multiChildrenSource));
  }
}
