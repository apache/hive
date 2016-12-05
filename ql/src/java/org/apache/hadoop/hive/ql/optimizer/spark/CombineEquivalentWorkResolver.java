/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.optimizer.spark;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.optimizer.OperatorComparatorFactory;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalContext;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalPlanResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SparkEdgeProperty;
import org.apache.hadoop.hive.ql.plan.SparkWork;


/**
 * CombineEquivalentWorkResolver would search inside SparkWork, find and combine equivalent
 * works.
 */
public class CombineEquivalentWorkResolver implements PhysicalPlanResolver {
  protected static transient Logger LOG = LoggerFactory.getLogger(CombineEquivalentWorkResolver.class);

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());
    TaskGraphWalker taskWalker = new TaskGraphWalker(new EquivalentWorkMatcher());
    HashMap<Node, Object> nodeOutput = Maps.newHashMap();
    taskWalker.startWalking(topNodes, nodeOutput);
    return pctx;
  }

  class EquivalentWorkMatcher implements Dispatcher {
    private Comparator<BaseWork> baseWorkComparator = new Comparator<BaseWork>() {
      @Override
      public int compare(BaseWork o1, BaseWork o2) {
        return o1.getName().compareTo(o2.getName());
      }
    };

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs) throws SemanticException {
      if (nd instanceof SparkTask) {
        SparkTask sparkTask = (SparkTask) nd;
        SparkWork sparkWork = sparkTask.getWork();
        Set<BaseWork> roots = sparkWork.getRoots();
        compareWorksRecursively(roots, sparkWork);
      }
      return null;
    }

    private void compareWorksRecursively(Set<BaseWork> works, SparkWork sparkWork) {
      // find out all equivalent works in the Set.
      Set<Set<BaseWork>> equivalentWorks = compareChildWorks(works, sparkWork);
      // combine equivalent work into single one in SparkWork's work graph.
      Set<BaseWork> removedWorks = combineEquivalentWorks(equivalentWorks, sparkWork);

      // try to combine next level works recursively.
      for (BaseWork work : works) {
        if (!removedWorks.contains(work)) {
          Set<BaseWork> children = Sets.newHashSet();
          children.addAll(sparkWork.getChildren(work));
          if (children.size() > 0) {
            compareWorksRecursively(children, sparkWork);
          }
        }
      }
    }

    private Set<Set<BaseWork>> compareChildWorks(Set<BaseWork> children, SparkWork sparkWork) {
      Set<Set<BaseWork>> equivalentChildren = Sets.newHashSet();
      if (children.size() > 1) {
        for (BaseWork work : children) {
          boolean assigned = false;
          for (Set<BaseWork> set : equivalentChildren) {
            if (belongToSet(set, work, sparkWork)) {
              set.add(work);
              assigned = true;
              break;
            }
          }
          if (!assigned) {
            // sort the works so that we get consistent query plan for multi executions(for test verification).
            Set<BaseWork> newSet = Sets.newTreeSet(baseWorkComparator);
            newSet.add(work);
            equivalentChildren.add(newSet);
          }
        }
      }
      return equivalentChildren;
    }

    private boolean belongToSet(Set<BaseWork> set, BaseWork work, SparkWork sparkWork) {
      if (set.isEmpty()) {
        return true;
      } else if (compareWork(set.iterator().next(), work, sparkWork)) {
        return true;
      }
      return false;
    }

    private Set<BaseWork> combineEquivalentWorks(Set<Set<BaseWork>> equivalentWorks, SparkWork sparkWork) {
      Set<BaseWork> removedWorks = Sets.newHashSet();
      for (Set<BaseWork> workSet : equivalentWorks) {
        if (workSet.size() > 1) {
          Iterator<BaseWork> iterator = workSet.iterator();
          BaseWork first = iterator.next();
          while (iterator.hasNext()) {
            BaseWork next = iterator.next();
            replaceWork(next, first, sparkWork);
            removedWorks.add(next);
          }
        }
      }
      return removedWorks;
    }

    private void replaceWork(BaseWork previous, BaseWork current, SparkWork sparkWork) {
      updateReference(previous, current, sparkWork);
      List<BaseWork> parents = sparkWork.getParents(previous);
      List<BaseWork> children = sparkWork.getChildren(previous);
      if (parents != null) {
        for (BaseWork parent : parents) {
          // we do not need to connect its parent to its counterpart, as they have the same parents.
          sparkWork.disconnect(parent, previous);
        }
      }
      if (children != null) {
        for (BaseWork child : children) {
          SparkEdgeProperty edgeProperty = sparkWork.getEdgeProperty(previous, child);
          sparkWork.disconnect(previous, child);
          sparkWork.connect(current, child, edgeProperty);
        }
      }
      sparkWork.remove(previous);
    }

    /*
    * update the Work name which referred by Operators in following Works.
    */
    private void updateReference(BaseWork previous, BaseWork current, SparkWork sparkWork) {
      String previousName = previous.getName();
      String currentName = current.getName();
      List<BaseWork> children = sparkWork.getAllWork();
      for (BaseWork child : children) {
        Set<Operator<?>> allOperators = child.getAllOperators();
        for (Operator<?> operator : allOperators) {
          if (operator instanceof MapJoinOperator) {
            MapJoinDesc mapJoinDesc = ((MapJoinOperator) operator).getConf();
            Map<Integer, String> parentToInput = mapJoinDesc.getParentToInput();
            for (Integer id : parentToInput.keySet()) {
              String parent = parentToInput.get(id);
              if (parent.equals(previousName)) {
                parentToInput.put(id, currentName);
              }
            }
          }
        }
      }
    }

    private boolean compareWork(BaseWork first, BaseWork second, SparkWork sparkWork) {

      if (!first.getClass().getName().equals(second.getClass().getName())) {
        return false;
      }

      if (!hasSameParent(first, second, sparkWork)) {
        return false;
      }

      // leave work's output may be read in further SparkWork/FetchWork, we should not combine
      // leave works without notifying further SparkWork/FetchWork.
      if (sparkWork.getLeaves().contains(first) && sparkWork.getLeaves().contains(second)) {
        return false;
      }

      // need to check paths and partition desc for MapWorks
      if (first instanceof MapWork && !compareMapWork((MapWork) first, (MapWork) second)) {
        return false;
      }

      Set<Operator<?>> firstRootOperators = first.getAllRootOperators();
      Set<Operator<?>> secondRootOperators = second.getAllRootOperators();
      if (firstRootOperators.size() != secondRootOperators.size()) {
        return false;
      }

      Iterator<Operator<?>> firstIterator = firstRootOperators.iterator();
      Iterator<Operator<?>> secondIterator = secondRootOperators.iterator();
      while (firstIterator.hasNext()) {
        boolean result = compareOperatorChain(firstIterator.next(), secondIterator.next());
        if (!result) {
          return result;
        }
      }

      return true;
    }

    private boolean compareMapWork(MapWork first, MapWork second) {
      Map<Path, PartitionDesc> pathToPartition1 = first.getPathToPartitionInfo();
      Map<Path, PartitionDesc> pathToPartition2 = second.getPathToPartitionInfo();
      if (pathToPartition1.size() == pathToPartition2.size()) {
        for (Map.Entry<Path, PartitionDesc> entry : pathToPartition1.entrySet()) {
          Path path1 = entry.getKey();
          PartitionDesc partitionDesc1 = entry.getValue();
          PartitionDesc partitionDesc2 = pathToPartition2.get(path1);
          if (!partitionDesc1.equals(partitionDesc2)) {
            return false;
          }
        }
        return true;
      }
      return false;
    }

    private boolean hasSameParent(BaseWork first, BaseWork second, SparkWork sparkWork) {
      boolean result = true;
      List<BaseWork> firstParents = sparkWork.getParents(first);
      List<BaseWork> secondParents = sparkWork.getParents(second);
      if (firstParents.size() != secondParents.size()) {
        result = false;
      }
      for (BaseWork parent : firstParents) {
        if (!secondParents.contains(parent)) {
          result = false;
          break;
        }
      }
      return result;
    }

    private boolean compareOperatorChain(Operator<?> firstOperator, Operator<?> secondOperator) {
      boolean result = compareCurrentOperator(firstOperator, secondOperator);
      if (!result) {
        return result;
      }

      List<Operator<? extends OperatorDesc>> firstOperatorChildOperators = firstOperator.getChildOperators();
      List<Operator<? extends OperatorDesc>> secondOperatorChildOperators = secondOperator.getChildOperators();
      if (firstOperatorChildOperators == null && secondOperatorChildOperators != null) {
        return false;
      } else if (firstOperatorChildOperators != null && secondOperatorChildOperators == null) {
        return false;
      } else if (firstOperatorChildOperators != null && secondOperatorChildOperators != null) {
        if (firstOperatorChildOperators.size() != secondOperatorChildOperators.size()) {
          return false;
        }
        int size = firstOperatorChildOperators.size();
        for (int i = 0; i < size; i++) {
          result = compareOperatorChain(firstOperatorChildOperators.get(i), secondOperatorChildOperators.get(i));
          if (!result) {
            return false;
          }
        }
      }

      return true;
    }

    /**
     * Compare Operators through their Explain output string.
     *
     * @param firstOperator
     * @param secondOperator
     * @return
     */
    private boolean compareCurrentOperator(Operator<?> firstOperator, Operator<?> secondOperator) {
      if (!firstOperator.getClass().getName().equals(secondOperator.getClass().getName())) {
        return false;
      }

      OperatorComparatorFactory.OperatorComparator operatorComparator =
        OperatorComparatorFactory.getOperatorComparator(firstOperator.getClass());
      return operatorComparator.equals(firstOperator, secondOperator);
    }
  }
}
