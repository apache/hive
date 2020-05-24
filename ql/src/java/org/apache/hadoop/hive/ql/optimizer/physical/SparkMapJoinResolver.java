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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.SparkHashTableSinkOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ConditionalResolver;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverSkewJoin;
import org.apache.hadoop.hive.ql.plan.ConditionalWork;
import org.apache.hadoop.hive.ql.plan.BucketMapJoinContext;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SparkBucketMapJoinContext;
import org.apache.hadoop.hive.ql.plan.SparkHashTableSinkDesc;
import org.apache.hadoop.hive.ql.plan.SparkWork;

public class SparkMapJoinResolver implements PhysicalPlanResolver {

  // prevents a task from being processed multiple times
  private final Set<Task<?>> visitedTasks = new HashSet<>();

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {

    SemanticDispatcher dispatcher = new SparkMapJoinTaskDispatcher(pctx);
    TaskGraphWalker graphWalker = new TaskGraphWalker(dispatcher);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());
    graphWalker.startWalking(topNodes, null);
    return pctx;
  }

  // Check whether the specified BaseWork's operator tree contains a operator
  // of the specified operator class
  private boolean containsOp(BaseWork work, Class<?> clazz) {
    Set<Operator<?>> matchingOps = OperatorUtils.getOp(work, clazz);
    return matchingOps != null && !matchingOps.isEmpty();
  }

  private boolean containsOp(SparkWork sparkWork, Class<?> clazz) {
    for (BaseWork work : sparkWork.getAllWorkUnsorted()) {
      if (containsOp(work, clazz)) {
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  class SparkMapJoinTaskDispatcher implements SemanticDispatcher {

    private final PhysicalContext physicalContext;

    // For each BaseWork with MJ operator, we build a SparkWork for its small table BaseWorks
    // This map records such information
    private final Map<BaseWork, SparkWork> sparkWorkMap;

    // SparkWork dependency graph - from a SparkWork with MJ operators to all
    // of its parent SparkWorks for the small tables
    private final Map<SparkWork, List<SparkWork>> dependencyGraph;

    public SparkMapJoinTaskDispatcher(PhysicalContext pc) {
      super();
      physicalContext = pc;
      sparkWorkMap = new LinkedHashMap<BaseWork, SparkWork>();
      dependencyGraph = new LinkedHashMap<SparkWork, List<SparkWork>>();
    }

    // Move the specified work from the sparkWork to the targetWork
    // Note that, in order not to break the graph (since we need it for the edges),
    // we don't remove the work from the sparkWork here. The removal is done later.
    private void moveWork(SparkWork sparkWork, BaseWork work, SparkWork targetWork) {
      List<BaseWork> parentWorks = sparkWork.getParents(work);
      if (sparkWork != targetWork) {
        targetWork.add(work);

        // If any child work for this work is already added to the targetWork earlier,
        // we should connect this work with it
        for (BaseWork childWork : sparkWork.getChildren(work)) {
          if (targetWork.contains(childWork)) {
            targetWork.connect(work, childWork, sparkWork.getEdgeProperty(work, childWork));
          }
        }
      }

      if (!containsOp(work, MapJoinOperator.class)) {
        for (BaseWork parent : parentWorks) {
          moveWork(sparkWork, parent, targetWork);
        }
      } else {
        // Create a new SparkWork for all the small tables of this work
        SparkWork parentWork =
            new SparkWork(physicalContext.conf.getVar(HiveConf.ConfVars.HIVEQUERYID));
        // copy cloneToWork to ensure RDD cache still works
        parentWork.setCloneToWork(sparkWork.getCloneToWork());

        dependencyGraph.get(targetWork).add(parentWork);
        dependencyGraph.put(parentWork, new ArrayList<SparkWork>());

        // this work is now moved to the parentWork, thus we should
        // update this information in sparkWorkMap
        sparkWorkMap.put(work, parentWork);
        for (BaseWork parent : parentWorks) {
          if (containsOp(parent, SparkHashTableSinkOperator.class)) {
            moveWork(sparkWork, parent, parentWork);
          } else {
            moveWork(sparkWork, parent, targetWork);
          }
        }
      }
    }

    private void generateLocalWork(SparkTask originalTask) {
      SparkWork originalWork = originalTask.getWork();
      Collection<BaseWork> allBaseWorks = originalWork.getAllWork();
      Context ctx = physicalContext.getContext();

      for (BaseWork work : allBaseWorks) {
        if (work.getMapRedLocalWork() == null) {
          if (containsOp(work, SparkHashTableSinkOperator.class) ||
              containsOp(work, MapJoinOperator.class)) {
            work.setMapRedLocalWork(new MapredLocalWork());
          }
          Set<Operator<?>> ops = OperatorUtils.getOp(work, MapJoinOperator.class);
          if (ops == null || ops.isEmpty()) {
            continue;
          }
          Path tmpPath = Utilities.generateTmpPath(ctx.getMRTmpPath(), originalTask.getId());
          MapredLocalWork bigTableLocalWork = work.getMapRedLocalWork();
          List<Operator<? extends OperatorDesc>> dummyOps =
              new ArrayList<Operator<? extends OperatorDesc>>(work.getDummyOps());
          bigTableLocalWork.setDummyParentOp(dummyOps);
          bigTableLocalWork.setTmpPath(tmpPath);

          // In one work, only one map join operator can be bucketed
          SparkBucketMapJoinContext bucketMJCxt = null;
          for (Operator<? extends OperatorDesc> op : ops) {
            MapJoinOperator mapJoinOp = (MapJoinOperator) op;
            MapJoinDesc mapJoinDesc = mapJoinOp.getConf();
            if (mapJoinDesc.isBucketMapJoin()) {
              bucketMJCxt = new SparkBucketMapJoinContext(mapJoinDesc);
              bucketMJCxt.setBucketMatcherClass(
                  org.apache.hadoop.hive.ql.exec.DefaultBucketMatcher.class);
              bucketMJCxt.setPosToAliasMap(mapJoinOp.getPosToAliasMap());
              ((MapWork) work).setUseBucketizedHiveInputFormat(true);
              bigTableLocalWork.setBucketMapjoinContext(bucketMJCxt);
              bigTableLocalWork.setInputFileChangeSensitive(true);
              break;
            }
          }

          for (BaseWork parentWork : originalWork.getParents(work)) {
            Set<Operator<?>> hashTableSinkOps =
                OperatorUtils.getOp(parentWork, SparkHashTableSinkOperator.class);
            if (hashTableSinkOps == null || hashTableSinkOps.isEmpty()) {
              continue;
            }
            MapredLocalWork parentLocalWork = parentWork.getMapRedLocalWork();
            parentLocalWork.setTmpHDFSPath(tmpPath);
            if (bucketMJCxt != null) {
              // We only need to update the work with the hashtable
              // sink operator with the same mapjoin desc. We can tell
              // that by comparing the bucket file name mapping map
              // instance. They should be exactly the same one due to
              // the way how the bucket mapjoin context is constructed.
              for (Operator<? extends OperatorDesc> op : hashTableSinkOps) {
                SparkHashTableSinkOperator hashTableSinkOp = (SparkHashTableSinkOperator) op;
                SparkHashTableSinkDesc hashTableSinkDesc = hashTableSinkOp.getConf();
                BucketMapJoinContext original = hashTableSinkDesc.getBucketMapjoinContext();
                if (original != null && original.getBucketFileNameMapping()
                    == bucketMJCxt.getBucketFileNameMapping()) {
                  ((MapWork) parentWork).setUseBucketizedHiveInputFormat(true);
                  parentLocalWork.setBucketMapjoinContext(bucketMJCxt);
                  parentLocalWork.setInputFileChangeSensitive(true);
                  break;
                }
              }
            }
          }
        }
      }
    }

    // Create a new SparkTask for the specified SparkWork, recursively compute
    // all the parent SparkTasks that this new task is depend on, if they don't already exists.
    private SparkTask createSparkTask(SparkTask originalTask,
                                      SparkWork sparkWork,
                                      Map<SparkWork, SparkTask> createdTaskMap,
                                      ConditionalTask conditionalTask) {
      if (createdTaskMap.containsKey(sparkWork)) {
        return createdTaskMap.get(sparkWork);
      }
      SparkTask resultTask = originalTask.getWork() == sparkWork ?
          originalTask : (SparkTask) TaskFactory.get(sparkWork);
      if (!dependencyGraph.get(sparkWork).isEmpty()) {
        for (SparkWork parentWork : dependencyGraph.get(sparkWork)) {
          SparkTask parentTask =
              createSparkTask(originalTask, parentWork, createdTaskMap, conditionalTask);
          parentTask.addDependentTask(resultTask);
        }
      } else {
        if (originalTask != resultTask) {
          List<Task<?>> parentTasks = originalTask.getParentTasks();
          if (parentTasks != null && parentTasks.size() > 0) {
            // avoid concurrent modification
            originalTask.setParentTasks(new ArrayList<Task<?>>());
            for (Task<?> parentTask : parentTasks) {
              parentTask.addDependentTask(resultTask);
              parentTask.removeDependentTask(originalTask);
            }
          } else {
            if (conditionalTask == null) {
              physicalContext.addToRootTask(resultTask);
              physicalContext.removeFromRootTask(originalTask);
            } else {
              updateConditionalTask(conditionalTask, originalTask, resultTask);
            }
          }
        }
      }

      createdTaskMap.put(sparkWork, resultTask);
      return resultTask;
    }

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nos)
        throws SemanticException {
      Task<?> currentTask = (Task<?>) nd;
      if(currentTask.isMapRedTask()) {
        if (currentTask instanceof ConditionalTask) {
          List<Task<?>> taskList =
              ((ConditionalTask) currentTask).getListTasks();
          for (Task<?> tsk : taskList) {
            if (tsk instanceof SparkTask) {
              processCurrentTask((SparkTask) tsk, (ConditionalTask) currentTask);
              visitedTasks.add(tsk);
            }
          }
        } else if (currentTask instanceof SparkTask) {
          processCurrentTask((SparkTask) currentTask, null);
          visitedTasks.add(currentTask);
        }
      }

      return null;
    }

    /**
     * @param sparkTask The current spark task we're processing.
     * @param conditionalTask If conditional task is not null, it means the current task is
     *                        wrapped in its task list.
     */
    private void processCurrentTask(SparkTask sparkTask, ConditionalTask conditionalTask) {
      SparkWork sparkWork = sparkTask.getWork();
      if (!visitedTasks.contains(sparkTask)) {
        dependencyGraph.clear();
        sparkWorkMap.clear();

        // Generate MapredLocalWorks for MJ and HTS
        generateLocalWork(sparkTask);

        dependencyGraph.put(sparkWork, new ArrayList<SparkWork>());
        Set<BaseWork> leaves = sparkWork.getLeaves();
        for (BaseWork leaf : leaves) {
          moveWork(sparkWork, leaf, sparkWork);
        }

        // Now remove all BaseWorks in all the childSparkWorks that we created
        // from the original SparkWork
        for (SparkWork newSparkWork : sparkWorkMap.values()) {
          for (BaseWork work : newSparkWork.getAllWorkUnsorted()) {
            sparkWork.remove(work);
          }
        }

        Map<SparkWork, SparkTask> createdTaskMap = new LinkedHashMap<SparkWork, SparkTask>();

        // Now create SparkTasks from the SparkWorks, also set up dependency
        for (SparkWork work : dependencyGraph.keySet()) {
          createSparkTask(sparkTask, work, createdTaskMap, conditionalTask);
        }
      } else if (conditionalTask != null) {
        // We may need to update the conditional task's list. This happens when a common map join
        // task exists in the task list and has already been processed. In such a case,
        // the current task is the map join task and we need to replace it with
        // its parent, i.e. the small table task.
        if (sparkTask.getParentTasks() != null && sparkTask.getParentTasks().size() == 1 &&
            sparkTask.getParentTasks().get(0) instanceof SparkTask) {
          SparkTask parent = (SparkTask) sparkTask.getParentTasks().get(0);
          if (containsOp(sparkWork, MapJoinOperator.class) &&
              containsOp(parent.getWork(), SparkHashTableSinkOperator.class)) {
            updateConditionalTask(conditionalTask, sparkTask, parent);
          }
        }
      }
    }

    /**
     * Update the task/work list of this conditional task to replace originalTask with newTask.
     * For runtime skew join, also update dirToTaskMap for the conditional resolver
     */
    private void updateConditionalTask(ConditionalTask conditionalTask,
        SparkTask originalTask, SparkTask newTask) {
      ConditionalWork conditionalWork = conditionalTask.getWork();
      SparkWork originWork = originalTask.getWork();
      SparkWork newWork = newTask.getWork();
      List<Task<?>> listTask = conditionalTask.getListTasks();
      List<Serializable> listWork = (List<Serializable>) conditionalWork.getListWorks();
      int taskIndex = listTask.indexOf(originalTask);
      int workIndex = listWork.indexOf(originWork);
      if (taskIndex < 0 || workIndex < 0) {
        return;
      }
      listTask.set(taskIndex, newTask);
      listWork.set(workIndex, newWork);
      ConditionalResolver resolver = conditionalTask.getResolver();
      if (resolver instanceof ConditionalResolverSkewJoin) {
        // get bigKeysDirToTaskMap
        ConditionalResolverSkewJoin.ConditionalResolverSkewJoinCtx context =
            (ConditionalResolverSkewJoin.ConditionalResolverSkewJoinCtx) conditionalTask
                .getResolverCtx();
        HashMap<Path, Task<?>> bigKeysDirToTaskMap = context
            .getDirToTaskMap();
        // to avoid concurrent modify the hashmap
        HashMap<Path, Task<?>> newbigKeysDirToTaskMap =
            new HashMap<Path, Task<?>>();
        // reset the resolver
        for (Map.Entry<Path, Task<?>> entry :
            bigKeysDirToTaskMap.entrySet()) {
          Task<?> task = entry.getValue();
          Path bigKeyDir = entry.getKey();
          if (task.equals(originalTask)) {
            newbigKeysDirToTaskMap.put(bigKeyDir, newTask);
          } else {
            newbigKeysDirToTaskMap.put(bigKeyDir, task);
          }
        }
        context.setDirToTaskMap(newbigKeysDirToTaskMap);
        // update no skew task
        if (context.getNoSkewTask() != null && context.getNoSkewTask().equals(originalTask)) {
          List<Task<?>> noSkewTask = new ArrayList<>();
          noSkewTask.add(newTask);
          context.setNoSkewTask(noSkewTask);
        }
      }
    }
  }
}
