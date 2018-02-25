/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Set;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.optimizer.spark.SparkPartitionPruningSinkDesc;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.spark.SparkPartitionPruningSinkOperator;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;


/**
 * A physical optimization that disables DPP if the source {@link MapWork} and target {@link MapWork} aren't in
 * dependent {@link SparkTask}s.
 *
 * <p>
 *   When DPP is run, the source {@link MapWork} produces a temp file that is read by the target {@link MapWork}. The
 *   source {@link MapWork} must be run before the target {@link MapWork} is run, otherwise the target {@link MapWork}
 *   will throw a {@link java.io.FileNotFoundException}. In order to guarantee this, the source {@link MapWork} must be
 *   inside a {@link SparkTask} that runs before the {@link SparkTask} containing the target {@link MapWork}.
 * </p>
 *
 * <p>
 *   This {@link PhysicalPlanResolver} works by walking through the {@link Task} DAG and iterating over all the
 *   {@link SparkPartitionPruningSinkOperator}s inside the {@link SparkTask}. For each sink operator, it takes the
 *   target {@link MapWork} and checks if it exists in any of the child {@link SparkTask}s. If the target {@link MapWork}
 *   is not in any child {@link SparkTask} then it removes the operator subtree that contains the
 *   {@link SparkPartitionPruningSinkOperator}.
 * </p>
 */
public class SparkDynamicPartitionPruningResolver implements PhysicalPlanResolver {

  private static final Logger LOG = LoggerFactory.getLogger(SparkDynamicPartitionPruningResolver.class.getName());

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    // Walk through the Task Graph and invoke SparkDynamicPartitionPruningDispatcher
    TaskGraphWalker graphWalker = new TaskGraphWalker(new SparkDynamicPartitionPruningDispatcher());

    ArrayList<Node> rootTasks = new ArrayList<>();
    rootTasks.addAll(pctx.getRootTasks());
    graphWalker.startWalking(rootTasks, null);
    return pctx;
  }

  private class SparkDynamicPartitionPruningDispatcher implements Dispatcher {

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs) throws SemanticException {
      Task<? extends Serializable> task = (Task<? extends Serializable>) nd;

      // If the given Task is a SparkTask then search its Work DAG for SparkPartitionPruningSinkOperator
      if (task instanceof SparkTask) {

        // Search for any SparkPartitionPruningSinkOperator in the SparkTask
        for (BaseWork baseWork : ((SparkTask) task).getWork().getAllWork()) {
          Set<Operator<?>> pruningSinkOps = OperatorUtils.getOp(baseWork, SparkPartitionPruningSinkOperator.class);

          // For each SparkPartitionPruningSinkOperator, take the target MapWork and see if it is in a dependent SparkTask
          for (Operator<?> op : pruningSinkOps) {
            SparkPartitionPruningSinkOperator pruningSinkOp = (SparkPartitionPruningSinkOperator) op;
            MapWork targetMapWork = pruningSinkOp.getConf().getTargetMapWork();

            // Check if the given SparkTask has a child SparkTask that contains the target MapWork
            // If it does not, then remove the DPP op
            if (!taskContainsDependentMapWork(task, targetMapWork)) {
              LOG.info("Disabling DPP for source work " + baseWork.getName() + " for target work "
                      + targetMapWork.getName() + " as no dependency exists between the source and target work");
              removeSparkPartitionPruningSink(baseWork, targetMapWork, pruningSinkOp);
            }
          }
        }
      }
      return null;
    }
  }

  /**
   * Remove a {@link SparkPartitionPruningSinkOperator} from a given {@link BaseWork}. Unlink the target {@link MapWork}
   * and the given {@link SparkPartitionPruningSinkOperator}.
   */
  private void removeSparkPartitionPruningSink(BaseWork sourceWork, MapWork targetMapWork,
                                               SparkPartitionPruningSinkOperator pruningSinkOp) {
    // Remove the DPP operator subtree
    OperatorUtils.removeBranch(pruningSinkOp);

    // Remove all event source info from the target MapWork
    String sourceWorkId = pruningSinkOp.getUniqueId();
    SparkPartitionPruningSinkDesc pruningSinkDesc = pruningSinkOp.getConf();
    targetMapWork.getEventSourceTableDescMap().get(sourceWorkId).remove(pruningSinkDesc.getTable());
    targetMapWork.getEventSourceColumnNameMap().get(sourceWorkId).remove(pruningSinkDesc.getTargetColumnName());
    targetMapWork.getEventSourceColumnTypeMap().get(sourceWorkId).remove(pruningSinkDesc.getTargetColumnType());
    targetMapWork.getEventSourcePartKeyExprMap().get(sourceWorkId).remove(pruningSinkDesc.getTargetPartKey());
  }

  /**
   * Recursively go through the children of the given {@link Task} and check if any child {@link SparkTask} contains
   * the specified {@link MapWork} object.
   */
  private boolean taskContainsDependentMapWork(Task<? extends Serializable> task,
                                               MapWork work) throws SemanticException {
    if (task == null || task.getChildTasks() == null) {
      return false;
    }
    for (Task<? extends Serializable> childTask : task.getChildTasks()) {
      if (childTask != null && childTask instanceof SparkTask && childTask.getMapWork().contains(work)) {
        return true;
      } else if (taskContainsDependentMapWork(childTask, work)) {
        return true;
      }
    }
    return false;
  }
}
