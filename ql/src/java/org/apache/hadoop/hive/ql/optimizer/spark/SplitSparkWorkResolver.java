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

package org.apache.hadoop.hive.ql.optimizer.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalContext;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalPlanResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.spark.GenSparkUtils;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SparkEdgeProperty;
import org.apache.hadoop.hive.ql.plan.SparkWork;

import com.google.common.base.Preconditions;


/**
 * Do a BFS on the sparkWork graph, and look for any work that has more than one child.
 * If we found such a work, we split it into multiple ones, one for each of its child.
 */
public class SplitSparkWorkResolver implements PhysicalPlanResolver {
  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    for (Task<? extends Serializable> task : pctx.getRootTasks()) {
      if (task instanceof SparkTask) {
        splitSparkWork(((SparkTask) task).getWork());
      }
    }
    return pctx;
  }

  private void splitSparkWork(SparkWork sparkWork) {
    Queue<BaseWork> queue = new LinkedList<BaseWork>();
    Set<BaseWork> visited = new HashSet<BaseWork>();
    queue.addAll(sparkWork.getRoots());
    while (!queue.isEmpty()) {
      BaseWork work = queue.poll();
      if (!visited.add(work)) {
        continue;
      }

      List<BaseWork> childWorks = sparkWork.getChildren(work);
      // First, add all children of this work into queue, to be processed later.
      for (BaseWork w : childWorks) {
        queue.add(w);
      }

      // Second, check if this work has multiple reduceSinks. If so, do split.
      splitBaseWork(sparkWork, work, childWorks);
    }
  }

  // Split work into multiple branches, one for each childWork in childWorks.
  // It also set up the connection between each parent work and child work.
  private void splitBaseWork(SparkWork sparkWork, BaseWork parentWork, List<BaseWork> childWorks) {
    if (getAllReduceSinks(parentWork).size() <= 1) {
      return;
    }

    // Grand-parent works - we need to set these to be the parents of the cloned works.
    List<BaseWork> grandParentWorks = sparkWork.getParents(parentWork);
    boolean isFirst = true;

    for (BaseWork childWork : childWorks) {
      BaseWork clonedParentWork = SerializationUtilities.cloneBaseWork(parentWork);
      // give the cloned work a different name
      clonedParentWork.setName(clonedParentWork.getName().replaceAll("^([a-zA-Z]+)(\\s+)(\\d+)",
          "$1$2" + GenSparkUtils.getUtils().getNextSeqNumber()));
      setStatistics(parentWork, clonedParentWork);
      String childReducerName = childWork.getName();
      SparkEdgeProperty clonedEdgeProperty = sparkWork.getEdgeProperty(parentWork, childWork);

      // We need to remove those branches that
      // 1, ended with a ReduceSinkOperator, and
      // 2, the ReduceSinkOperator's name is not the same as childReducerName.
      // Also, if the cloned work is not the first, we remove ALL leaf operators except
      // the corresponding ReduceSinkOperator.
      for (Operator<?> op : clonedParentWork.getAllLeafOperators()) {
        if (op instanceof ReduceSinkOperator) {
          if (!((ReduceSinkOperator) op).getConf().getOutputName().equals(childReducerName)) {
            removeOpRecursive(op);
          }
        } else if (!isFirst) {
          removeOpRecursive(op);
        }
      }

      isFirst = false;

      // Then, we need to set up the graph connection. Especially:
      // 1, we need to connect this cloned parent work with all the grand-parent works.
      // 2, we need to connect this cloned parent work with the corresponding child work.
      sparkWork.add(clonedParentWork);
      for (BaseWork gpw : grandParentWorks) {
        sparkWork.connect(gpw, clonedParentWork, sparkWork.getEdgeProperty(gpw, parentWork));
      }
      sparkWork.connect(clonedParentWork, childWork, clonedEdgeProperty);
      sparkWork.getCloneToWork().put(clonedParentWork, parentWork);
    }

    sparkWork.remove(parentWork);
  }

  private Set<Operator<?>> getAllReduceSinks(BaseWork work) {
    Set<Operator<?>> resultSet = work.getAllLeafOperators();
    Iterator<Operator<?>> it = resultSet.iterator();
    while (it.hasNext()) {
      if (!(it.next() instanceof ReduceSinkOperator)) {
        it.remove();
      }
    }
    return resultSet;
  }

  // Remove op from all its parents' child list.
  // Recursively remove any of its parent who only have this op as child.
  private void removeOpRecursive(Operator<?> operator) {
    List<Operator<?>> parentOperators = new ArrayList<Operator<?>>();
    for (Operator<?> op : operator.getParentOperators()) {
      parentOperators.add(op);
    }
    for (Operator<?> parentOperator : parentOperators) {
      Preconditions.checkArgument(parentOperator.getChildOperators().contains(operator),
          "AssertionError: parent of " + operator.getName() + " doesn't have it as child.");
      parentOperator.removeChild(operator);
      if (parentOperator.getNumChild() == 0) {
        removeOpRecursive(parentOperator);
      }
    }
  }

  // we lost statistics & opTraits through cloning, try to get them back
  private void setStatistics(BaseWork origin, BaseWork clone) {
    if (origin instanceof MapWork && clone instanceof MapWork) {
      MapWork originMW = (MapWork) origin;
      MapWork cloneMW = (MapWork) clone;
      for (Map.Entry<String, Operator<? extends OperatorDesc>> entry
        : originMW.getAliasToWork().entrySet()) {
        String alias = entry.getKey();
        Operator<? extends OperatorDesc> cloneOP = cloneMW.getAliasToWork().get(alias);
        if (cloneOP != null) {
          setStatistics(entry.getValue(), cloneOP);
        }
      }
    } else if (origin instanceof ReduceWork && clone instanceof ReduceWork) {
      setStatistics(((ReduceWork) origin).getReducer(), ((ReduceWork) clone).getReducer());
    }
  }

  private void setStatistics(Operator<? extends OperatorDesc> origin,
      Operator<? extends OperatorDesc> clone) {
    clone.getConf().setStatistics(origin.getConf().getStatistics());
    clone.getConf().setTraits(origin.getConf().getTraits());
    if (origin.getChildOperators().size() == clone.getChildOperators().size()) {
      for (int i = 0; i < clone.getChildOperators().size(); i++) {
        setStatistics(origin.getChildOperators().get(i), clone.getChildOperators().get(i));
      }
    }
  }
}
