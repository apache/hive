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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.optimizer.MapJoinProcessor;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverCommonJoin;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverCommonJoin.ConditionalResolverCommonJoinCtx;
import org.apache.hadoop.hive.ql.plan.ConditionalWork;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;

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
public class CommonJoinTaskDispatcher extends AbstractJoinTaskDispatcher implements Dispatcher {

  HashMap<String, Long> aliasToSize = null;

  public CommonJoinTaskDispatcher(PhysicalContext context) {
    super(context);
  }

  // Get the position of the big table for this join operator and the given alias
  private int getPosition(MapredWork work, Operator<? extends OperatorDesc> joinOp,
      String alias) {
    Operator<? extends OperatorDesc> parentOp = work.getAliasToWork().get(alias);

    // reduceSinkOperator's child is null, but joinOperator's parents is reduceSink
    while ((parentOp.getChildOperators() != null) &&
        (!parentOp.getChildOperators().isEmpty())) {
      parentOp = parentOp.getChildOperators().get(0);
    }

    return joinOp.getParentOperators().indexOf(parentOp);
  }

  /*
   * A task and its child task has been converted from join to mapjoin.
   * See if the two tasks can be merged.
   */
  private void mergeMapJoinTaskWithChildMapJoinTask(MapRedTask task, Configuration conf) {
    MapRedTask childTask = (MapRedTask) task.getChildTasks().get(0);
    MapredWork work = task.getWork();
    MapredLocalWork localWork = work.getMapLocalWork();
    MapredWork childWork = childTask.getWork();
    MapredLocalWork childLocalWork = childWork.getMapLocalWork();

    // Can this be merged
    Map<String, Operator<? extends OperatorDesc>> aliasToWork = work.getAliasToWork();
    if (aliasToWork.size() > 1) {
      return;
    }

    Operator<? extends OperatorDesc> op = aliasToWork.values().iterator().next();
    while (op.getChildOperators() != null) {
      // Dont perform this optimization for multi-table inserts
      if (op.getChildOperators().size() > 1) {
        return;
      }
      op = op.getChildOperators().get(0);
    }

    if (!(op instanceof FileSinkOperator)) {
      return;
    }

    FileSinkOperator fop = (FileSinkOperator) op;
    String workDir = fop.getConf().getDirName();

    Map<String, ArrayList<String>> childPathToAliases = childWork.getPathToAliases();
    if (childPathToAliases.size() > 1) {
      return;
    }

    // The filesink writes to a different directory
    if (!childPathToAliases.keySet().iterator().next().equals(workDir)) {
      return;
    }

    // Either of them should not be bucketed
    if ((localWork.getBucketMapjoinContext() != null) ||
        (childLocalWork.getBucketMapjoinContext() != null)) {
      return;
    }

    // Merge the trees
    if (childWork.getAliasToWork().size() > 1) {
      return;
    }

    long mapJoinSize = HiveConf.getLongVar(conf,
        HiveConf.ConfVars.HIVECONVERTJOINNOCONDITIONALTASKTHRESHOLD);
    long localTableTotalSize = 0;
    for (String alias : localWork.getAliasToWork().keySet()) {
      Long tabSize = aliasToSize.get(alias);
      if (tabSize == null) {
        /*
         * if the size is unavailable, we need to assume a size 1 greater than mapJoinSize
         * this implies that merge cannot happen so we can return.
         */
        return;
      }
      localTableTotalSize += tabSize;
    }

    for (String alias : childLocalWork.getAliasToWork().keySet()) {
      Long tabSize = aliasToSize.get(alias);
      if (tabSize == null) {
        /*
         * if the size is unavailable, we need to assume a size 1 greater than mapJoinSize
         * this implies that merge cannot happen so we can return.
         */
        return;
      }
      localTableTotalSize += tabSize;
      if (localTableTotalSize > mapJoinSize) {
        return;
      }
    }

    Operator<? extends Serializable> childAliasOp =
        childWork.getAliasToWork().values().iterator().next();
    if (fop.getParentOperators().size() > 1) {
      return;
    }

    // Merge the 2 trees - remove the FileSinkOperator from the first tree pass it to the
    // top of the second
    Operator<? extends Serializable> parentFOp = fop.getParentOperators().get(0);
    parentFOp.getChildOperators().remove(fop);
    parentFOp.getChildOperators().add(childAliasOp);
    List<Operator<? extends OperatorDesc>> parentOps =
        new ArrayList<Operator<? extends OperatorDesc>>();
    parentOps.add(parentFOp);
    childAliasOp.setParentOperators(parentOps);

    work.getAliasToPartnInfo().putAll(childWork.getAliasToPartnInfo());
    for (Map.Entry<String, PartitionDesc> childWorkEntry : childWork.getPathToPartitionInfo()
        .entrySet()) {
      if (childWork.getAliasToPartnInfo().containsValue(childWorkEntry.getKey())) {
        work.getPathToPartitionInfo().put(childWorkEntry.getKey(), childWorkEntry.getValue());
      }
    }

    localWork.getAliasToFetchWork().putAll(childLocalWork.getAliasToFetchWork());
    localWork.getAliasToWork().putAll(childLocalWork.getAliasToWork());

    // remove the child task
    List<Task<? extends Serializable>> oldChildTasks = childTask.getChildTasks();
    task.setChildTasks(oldChildTasks);
    if (oldChildTasks != null) {
      for (Task<? extends Serializable> oldChildTask : oldChildTasks) {
        oldChildTask.getParentTasks().remove(childTask);
        oldChildTask.getParentTasks().add(task);
      }
    }

    boolean convertToSingleJob = HiveConf.getBoolVar(conf,
        HiveConf.ConfVars.HIVEOPTIMIZEMAPJOINFOLLOWEDBYMR);
    if (convertToSingleJob) {
      copyReducerConf(task, childTask);
    }
  }

  /**
   * Copy reducer configuration if the childTask also has a reducer.
   *
   * @param task
   * @param childTask
   */
  private void copyReducerConf(MapRedTask task, MapRedTask childTask) {
    MapredWork childWork = childTask.getWork();
    Operator childReducer = childWork.getReducer();
    MapredWork work = task.getWork();
    if (childReducer == null) {
      return;
    }
    work.setReducer(childReducer);
    work.setNumReduceTasks(childWork.getNumReduceTasks());
    work.setJoinTree(childWork.getJoinTree());
    work.setNeedsTagging(childWork.getNeedsTagging());

    // Make sure the key configuration is correct, clear and regenerate.
    work.getTagToValueDesc().clear();
    GenMapRedUtils.setKeyAndValueDescForTaskTree(task);
  }

  // create map join task and set big table as bigTablePosition
  private ObjectPair<MapRedTask, String> convertTaskToMapJoinTask(MapredWork newWork,
      int bigTablePosition) throws UnsupportedEncodingException, SemanticException {
    // create a mapred task for this work
    MapRedTask newTask = (MapRedTask) TaskFactory.get(newWork, physicalContext
        .getParseContext().getConf());
    JoinOperator newJoinOp = getJoinOp(newTask);

    // optimize this newWork given the big table position
    String bigTableAlias =
        MapJoinProcessor.genMapJoinOpAndLocalWork(newWork, newJoinOp, bigTablePosition);
    return new ObjectPair<MapRedTask, String>(newTask, bigTableAlias);
  }

  /*
   * A task and its child task has been converted from join to mapjoin.
   * See if the two tasks can be merged.
   */
  private void mergeMapJoinTaskWithMapReduceTask(MapRedTask mapJoinTask, Configuration conf) {
    if (mapJoinTask.getChildTasks() == null
        || mapJoinTask.getChildTasks().size() > 1) {
      // No child-task to merge, nothing to do or there are more than one
      // child-tasks in which case we don't want to do anything.
      return;
    }
    Task<? extends Serializable> firstChildTask = mapJoinTask.getChildTasks().get(0);
    if (!(firstChildTask instanceof MapRedTask)) {
      // Nothing to do if it is not a mapreduce task.
      return;
    }
    MapRedTask childTask = (MapRedTask) firstChildTask;
    MapredWork mapJoinWork = mapJoinTask.getWork();
    MapredWork childWork = childTask.getWork();
    Operator childReducer = childWork.getReducer();
    if (childReducer == null) {
      // Not a MR job, nothing to merge.
      return;
    }

    // Can this be merged
    Map<String, Operator<? extends OperatorDesc>> aliasToWork = mapJoinWork.getAliasToWork();
    if (aliasToWork.size() > 1) {
      return;
    }
    Map<String, ArrayList<String>> childPathToAliases = childWork.getPathToAliases();
    if (childPathToAliases.size() > 1) {
      return;
    }

    // Locate leaf operator of the map-join task. Start by initializing leaf
    // operator to be root operator.
    Operator<? extends OperatorDesc> mapJoinLeafOperator = aliasToWork.values().iterator().next();
    while (mapJoinLeafOperator.getChildOperators() != null) {
      // Dont perform this optimization for multi-table inserts
      if (mapJoinLeafOperator.getChildOperators().size() > 1) {
        return;
      }
      mapJoinLeafOperator = mapJoinLeafOperator.getChildOperators().get(0);
    }

    assert (mapJoinLeafOperator instanceof FileSinkOperator);
    if (!(mapJoinLeafOperator instanceof FileSinkOperator)) {
      // Sanity check, shouldn't happen.
      return;
    }

    FileSinkOperator mapJoinTaskFileSinkOperator = (FileSinkOperator) mapJoinLeafOperator;

    // The filesink writes to a different directory
    String workDir = mapJoinTaskFileSinkOperator.getConf().getDirName();
    if (!childPathToAliases.keySet().iterator().next().equals(workDir)) {
      return;
    }

    MapredLocalWork mapJoinLocalWork = mapJoinWork.getMapLocalWork();
    MapredLocalWork childLocalWork = childWork.getMapLocalWork();

    // Either of them should not be bucketed
    if ((mapJoinLocalWork != null && mapJoinLocalWork.getBucketMapjoinContext() != null) ||
        (childLocalWork != null && childLocalWork.getBucketMapjoinContext() != null)) {
      return;
    }

    if (childWork.getAliasToWork().size() > 1) {
      return;
    }

    Operator<? extends Serializable> childAliasOp =
        childWork.getAliasToWork().values().iterator().next();
    if (mapJoinTaskFileSinkOperator.getParentOperators().size() > 1) {
      return;
    }

    // remove the unnecessary TableScan
    if (childAliasOp instanceof TableScanOperator) {
      TableScanOperator tso = (TableScanOperator)childAliasOp;
      if (tso.getNumChild() != 1) {
        // shouldn't happen
        return;
      }
      childAliasOp = tso.getChildOperators().get(0);
      childAliasOp.getParentOperators().remove(tso);
    }

    // Merge the 2 trees - remove the FileSinkOperator from the first tree pass it to the
    // top of the second
    Operator<? extends Serializable> parentFOp = mapJoinTaskFileSinkOperator
        .getParentOperators().get(0);
    parentFOp.getChildOperators().remove(mapJoinTaskFileSinkOperator);
    parentFOp.getChildOperators().add(childAliasOp);
    List<Operator<? extends OperatorDesc>> parentOps =
        new ArrayList<Operator<? extends OperatorDesc>>();
    parentOps.add(parentFOp);
    childAliasOp.setParentOperators(parentOps);

    mapJoinWork.getAliasToPartnInfo().putAll(childWork.getAliasToPartnInfo());
    for (Map.Entry<String, PartitionDesc> childWorkEntry : childWork.getPathToPartitionInfo()
        .entrySet()) {
      if (childWork.getAliasToPartnInfo().containsValue(childWorkEntry.getKey())) {
        mapJoinWork.getPathToPartitionInfo()
            .put(childWorkEntry.getKey(), childWorkEntry.getValue());
      }
    }

    // Fill up stuff in local work
    if (mapJoinLocalWork != null && childLocalWork != null) {
      mapJoinLocalWork.getAliasToFetchWork().putAll(childLocalWork.getAliasToFetchWork());
      mapJoinLocalWork.getAliasToWork().putAll(childLocalWork.getAliasToWork());
    }

    // remove the child task
    List<Task<? extends Serializable>> oldChildTasks = childTask.getChildTasks();
    mapJoinTask.setChildTasks(oldChildTasks);
    if (oldChildTasks != null) {
      for (Task<? extends Serializable> oldChildTask : oldChildTasks) {
        oldChildTask.getParentTasks().remove(childTask);
        oldChildTask.getParentTasks().add(mapJoinTask);
      }
    }

    // Copy the reducer conf.
    copyReducerConf(mapJoinTask, childTask);
  }

  @Override
  public Task<? extends Serializable> processCurrentTask(MapRedTask currTask,
      ConditionalTask conditionalTask, Context context)
      throws SemanticException {

    // whether it contains common join op; if contains, return this common join op
    JoinOperator joinOp = getJoinOp(currTask);
    if (joinOp == null || joinOp.getConf().isFixedAsSorted()) {
      return null;
    }
    currTask.setTaskTag(Task.COMMON_JOIN);

    MapredWork currWork = currTask.getWork();

    // create conditional work list and task list
    List<Serializable> listWorks = new ArrayList<Serializable>();
    List<Task<? extends Serializable>> listTasks = new ArrayList<Task<? extends Serializable>>();

    // create alias to task mapping and alias to input file mapping for resolver
    HashMap<String, Task<? extends Serializable>> aliasToTask =
        new HashMap<String, Task<? extends Serializable>>();
    HashMap<String, ArrayList<String>> pathToAliases = currWork.getPathToAliases();
    Map<String, Operator<? extends OperatorDesc>> aliasToWork = currWork.getAliasToWork();

    // get parseCtx for this Join Operator
    ParseContext parseCtx = physicalContext.getParseContext();
    QBJoinTree joinTree = parseCtx.getJoinContext().get(joinOp);

    // start to generate multiple map join tasks
    JoinDesc joinDesc = joinOp.getConf();
    Byte[] order = joinDesc.getTagOrder();
    int numAliases = order.length;

    if (aliasToSize == null) {
      aliasToSize = new HashMap<String, Long>();
    }

    try {
      long aliasTotalKnownInputSize =
          getTotalKnownInputSize(context, currWork, pathToAliases, aliasToSize);

      HashSet<Integer> bigTableCandidates = MapJoinProcessor.getBigTableCandidates(joinDesc
          .getConds());

      // no table could be the big table; there is no need to convert
      if (bigTableCandidates == null) {
        return null;
      }

      Configuration conf = context.getConf();

      // If sizes of atleast n-1 tables in a n-way join is known, and their sum is smaller than
      // the threshold size, convert the join into map-join and don't create a conditional task
      boolean convertJoinMapJoin = HiveConf.getBoolVar(conf,
          HiveConf.ConfVars.HIVECONVERTJOINNOCONDITIONALTASK);
      int bigTablePosition = -1;
      if (convertJoinMapJoin) {
        // This is the threshold that the user has specified to fit in mapjoin
        long mapJoinSize = HiveConf.getLongVar(conf,
            HiveConf.ConfVars.HIVECONVERTJOINNOCONDITIONALTASKTHRESHOLD);

        boolean bigTableFound = false;
        long largestBigTableCandidateSize = -1;
        long sumTableSizes = 0;
        for (String alias : aliasToWork.keySet()) {
          int tablePosition = getPosition(currWork, joinOp, alias);
          boolean bigTableCandidate = bigTableCandidates.contains(tablePosition);
          Long size = aliasToSize.get(alias);
          // The size is not available at compile time if the input is a sub-query.
          // If the size of atleast n-1 inputs for a n-way join are available at compile time,
          // and the sum of them is less than the specified threshold, then convert the join
          // into a map-join without the conditional task.
          if ((size == null) || (size > mapJoinSize)) {
            sumTableSizes += largestBigTableCandidateSize;
            if (bigTableFound || (sumTableSizes > mapJoinSize) || !bigTableCandidate) {
              convertJoinMapJoin = false;
              break;
            }
            bigTableFound = true;
            bigTablePosition = tablePosition;
            largestBigTableCandidateSize = mapJoinSize + 1;
          } else {
            if (bigTableCandidate && size > largestBigTableCandidateSize) {
              bigTablePosition = tablePosition;
              sumTableSizes += largestBigTableCandidateSize;
              largestBigTableCandidateSize = size;
            } else {
              sumTableSizes += size;
            }
            if (sumTableSizes > mapJoinSize) {
              convertJoinMapJoin = false;
              break;
            }
          }
        }
      }

      String bigTableAlias = null;
      currWork.setOpParseCtxMap(parseCtx.getOpParseCtx());
      currWork.setJoinTree(joinTree);

      if (convertJoinMapJoin) {
        // create map join task and set big table as bigTablePosition
        MapRedTask newTask = convertTaskToMapJoinTask(currWork, bigTablePosition).getFirst();

        newTask.setTaskTag(Task.MAPJOIN_ONLY_NOBACKUP);
        replaceTask(currTask, newTask, physicalContext);

        // Can this task be merged with the child task. This can happen if a big table is being
        // joined with multiple small tables on different keys
        if ((newTask.getChildTasks() != null) && (newTask.getChildTasks().size() == 1)) {
          if (newTask.getChildTasks().get(0).getTaskTag() == Task.MAPJOIN_ONLY_NOBACKUP) {
            // Merging two map-join tasks
            mergeMapJoinTaskWithChildMapJoinTask(newTask, conf);
          }

          // Converted the join operator into a map-join. Now see if it can
          // be merged into the following map-reduce job.
          boolean convertToSingleJob = HiveConf.getBoolVar(conf,
              HiveConf.ConfVars.HIVEOPTIMIZEMAPJOINFOLLOWEDBYMR);
          if (convertToSingleJob) {
            // Try merging a map-join task with a mapreduce job to have a
            // single job.
            mergeMapJoinTaskWithMapReduceTask(newTask, conf);
          }
        }

        return newTask;
      }

      long ThresholdOfSmallTblSizeSum = HiveConf.getLongVar(conf,
          HiveConf.ConfVars.HIVESMALLTABLESFILESIZE);
      for (int i = 0; i < numAliases; i++) {
        // this table cannot be big table
        if (!bigTableCandidates.contains(i)) {
          continue;
        }
        // deep copy a new mapred work from xml
        // Once HIVE-4396 is in, it would be faster to use a cheaper method to clone the plan
        String xml = currWork.toXML();
        InputStream in = new ByteArrayInputStream(xml.getBytes("UTF-8"));
        MapredWork newWork = Utilities.deserializeMapRedWork(in, physicalContext.getConf());

        // create map join task and set big table as i
        ObjectPair<MapRedTask, String> newTaskAlias = convertTaskToMapJoinTask(newWork, i);
        MapRedTask newTask = newTaskAlias.getFirst();
        bigTableAlias = newTaskAlias.getSecond();

        Long aliasKnownSize = aliasToSize.get(bigTableAlias);
        if (aliasKnownSize != null && aliasKnownSize.longValue() > 0) {
          long smallTblTotalKnownSize = aliasTotalKnownInputSize
              - aliasKnownSize.longValue();
          if (smallTblTotalKnownSize > ThresholdOfSmallTblSizeSum) {
            // this table is not good to be a big table.
            continue;
          }
        }

        // add into conditional task
        listWorks.add(newTask.getWork());
        listTasks.add(newTask);
        newTask.setTaskTag(Task.CONVERTED_MAPJOIN);

        // set up backup task
        newTask.setBackupTask(currTask);
        newTask.setBackupChildrenTasks(currTask.getChildTasks());

        // put the mapping alias to task
        aliasToTask.put(bigTableAlias, newTask);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new SemanticException("Generate Map Join Task Error: " + e.getMessage());
    }

    // insert current common join task to conditional task
    listWorks.add(currTask.getWork());
    listTasks.add(currTask);
    // clear JoinTree and OP Parse Context
    currWork.setOpParseCtxMap(null);
    currWork.setJoinTree(null);

    // create conditional task and insert conditional task into task tree
    ConditionalWork cndWork = new ConditionalWork(listWorks);
    ConditionalTask cndTsk = (ConditionalTask) TaskFactory.get(cndWork, parseCtx.getConf());
    cndTsk.setListTasks(listTasks);

    // set resolver and resolver context
    cndTsk.setResolver(new ConditionalResolverCommonJoin());
    ConditionalResolverCommonJoinCtx resolverCtx = new ConditionalResolverCommonJoinCtx();
    resolverCtx.setPathToAliases(pathToAliases);
    resolverCtx.setAliasToKnownSize(aliasToSize);
    resolverCtx.setAliasToTask(aliasToTask);
    resolverCtx.setCommonJoinTask(currTask);
    resolverCtx.setLocalTmpDir(context.getLocalScratchDir(false));
    resolverCtx.setHdfsTmpDir(context.getMRScratchDir());
    cndTsk.setResolverCtx(resolverCtx);

    // replace the current task with the new generated conditional task
    replaceTaskWithConditionalTask(currTask, cndTsk, physicalContext);
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

    if (op.getChildOperators() == null) {
      return true;
    }

    for (Operator<? extends OperatorDesc> childOp : op.getChildOperators()) {
      if (!checkOperatorOKMapJoinConversion(childOp)) {
        return false;
      }
    }

    return true;
  }

  private JoinOperator getJoinOp(MapRedTask task) throws SemanticException {
    MapredWork work = task.getWork();
    if (work == null) {
      return null;
    }
    Operator<? extends OperatorDesc> reducerOp = work.getReducer();
    if (reducerOp instanceof JoinOperator) {
      /* Is any operator present, which prevents the conversion */
      Map<String, Operator<? extends OperatorDesc>> aliasToWork = work.getAliasToWork();
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
}
