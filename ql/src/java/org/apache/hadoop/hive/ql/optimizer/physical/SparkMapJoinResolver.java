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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SparkHashTableSinkOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SparkWork;

public class SparkMapJoinResolver implements PhysicalPlanResolver {

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {

    Dispatcher dispatcher = new SparkMapJoinTaskDispatcher(pctx);
    TaskGraphWalker graphWalker = new TaskGraphWalker(dispatcher);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());
    graphWalker.startWalking(topNodes, null);
    return pctx;
  }

  // Check whether the specified BaseWork's operator tree contains a operator
  // of the specified operator class
  private boolean containsOp(BaseWork work, Class<?> clazz) {
    for (Operator<? extends OperatorDesc> op : work.getAllOperators()) {
      if (clazz.isInstance(op)) {
        return true;
      }
    }
    return false;
  }

  class SparkMapJoinTaskDispatcher implements Dispatcher {

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
      sparkWorkMap = new HashMap<BaseWork, SparkWork>();
      dependencyGraph = new HashMap<SparkWork, List<SparkWork>>();
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
      Collection<BaseWork> allBaseWorks = originalWork.getAllWorkUnsorted();

      for (BaseWork work : allBaseWorks) {
        if (containsOp(work, SparkHashTableSinkOperator.class) ||
            containsOp(work, MapJoinOperator.class)) {
          work.setMapRedLocalWork(new MapredLocalWork());
        }
      }

      Context ctx = physicalContext.getContext();

      for (BaseWork work : allBaseWorks) {
        if (containsOp(work, MapJoinOperator.class)) {
          Path tmpPath = Utilities.generateTmpPath(ctx.getMRTmpPath(), originalTask.getId());
          MapredLocalWork bigTableLocalWork = work.getMapRedLocalWork();
          List<Operator<? extends OperatorDesc>> dummyOps =
              new ArrayList<Operator<? extends OperatorDesc>>(work.getDummyOps());
          bigTableLocalWork.setDummyParentOp(dummyOps);

          for (BaseWork parentWork : originalWork.getParents(work)) {
            if (containsOp(parentWork,SparkHashTableSinkOperator.class)) {
              parentWork.getMapRedLocalWork().setTmpHDFSPath(tmpPath);
              parentWork.getMapRedLocalWork().setDummyParentOp(
                  new ArrayList<Operator<? extends OperatorDesc>>());
            }
          }

          bigTableLocalWork.setAliasToWork(
              new LinkedHashMap<String, Operator<? extends OperatorDesc>>());
          bigTableLocalWork.setAliasToFetchWork(new LinkedHashMap<String, FetchWork>());
          bigTableLocalWork.setTmpPath(tmpPath);

          // TODO: set inputFileChangeSensitive and BucketMapjoinContext,
          // TODO: enable non-staged mapjoin
        }
      }
    }

    // Create a new SparkTask for the specified SparkWork, recursively compute
    // all the parent SparkTasks that this new task is depend on, if they don't already exists.
    private SparkTask createSparkTask(SparkTask originalTask,
                                      SparkWork sparkWork,
                                      Map<SparkWork, SparkTask> createdTaskMap) {
      if (createdTaskMap.containsKey(sparkWork)) {
        return createdTaskMap.get(sparkWork);
      }
      SparkTask resultTask = originalTask.getWork() == sparkWork ?
          originalTask : (SparkTask) TaskFactory.get(sparkWork, physicalContext.conf);
      if (!dependencyGraph.get(sparkWork).isEmpty()) {
        for (SparkWork parentWork : dependencyGraph.get(sparkWork)) {
          SparkTask parentTask = createSparkTask(originalTask, parentWork, createdTaskMap);
          parentTask.addDependentTask(resultTask);
        }
      } else {
        if (originalTask != resultTask) {
          List<Task<? extends Serializable>> parentTasks = originalTask.getParentTasks();
          if (parentTasks != null && parentTasks.size() > 0) {
            for (Task<? extends Serializable> parentTask : parentTasks) {
              parentTask.addDependentTask(resultTask);
            }
          } else {
            physicalContext.addToRootTask(resultTask);
            physicalContext.removeFromRootTask(originalTask);
          }
        }
      }

      createdTaskMap.put(sparkWork, resultTask);
      return resultTask;
    }

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nos)
        throws SemanticException {
      Task<? extends Serializable> currentTask = (Task<? extends Serializable>) nd;
      if (currentTask instanceof SparkTask) {
        SparkTask sparkTask = (SparkTask) currentTask;
        SparkWork sparkWork = sparkTask.getWork();

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

        Map<SparkWork, SparkTask> createdTaskMap = new HashMap<SparkWork, SparkTask>();

        // Now create SparkTasks from the SparkWorks, also set up dependency
        for (SparkWork work : dependencyGraph.keySet()) {
          createSparkTask(sparkTask, work, createdTaskMap);
        }
      }

      return null;
    }
  }
}