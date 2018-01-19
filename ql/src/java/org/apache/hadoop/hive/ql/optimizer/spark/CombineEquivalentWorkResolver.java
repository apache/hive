/*
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

import java.io.Serializable;
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
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.spark.SparkUtilities;
import org.apache.hadoop.hive.ql.parse.spark.SparkPartitionPruningSinkOperator;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
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
  private List<String> removedMapWorkNames = new ArrayList<String>();
  private PhysicalContext pctx;
  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    this.pctx = pctx;
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

    // maps from a work to the DPPs it contains
    private Map<BaseWork, List<SparkPartitionPruningSinkOperator>> workToDpps = new HashMap<>();

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs) throws SemanticException {
      if (nd instanceof SparkTask) {
        SparkTask sparkTask = (SparkTask) nd;
        SparkWork sparkWork = sparkTask.getWork();
        // For dpp case, dpp sink will appear in Task1 and the target work of dpp sink will appear in Task2.
        // Task2 is the child task of Task1. Task2 will be traversed before task1 because TaskGraphWalker will first
        // put children task in the front of task queue.
        // If a spark work which is equal to other is found and removed in Task2, the dpp sink can be removed when Task1
        // is traversed(More detailed see HIVE-16948)
        if (removedMapWorkNames.size() > 0) {
          removeDynamicPartitionPruningSink(removedMapWorkNames, sparkWork);
          if (sparkWork.getAllWork().size() == 0) {
            removeEmptySparkTask(sparkTask);
          }
        }

        Set<BaseWork> roots = sparkWork.getRoots();
        compareWorksRecursively(roots, sparkWork);
      }
      return null;
    }

    private void compareWorksRecursively(Set<BaseWork> works, SparkWork sparkWork) {
      workToDpps.clear();
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

    // merge the second into the first
    private void combineEquivalentDPPSinks(SparkPartitionPruningSinkDesc first,
        SparkPartitionPruningSinkDesc second, String firstId, String secondId) {
      MapWork target2 = second.getTargetMapWork();

      first.addTarget(second.getTargetColumnName(), second.getTargetColumnType(),
          second.getTargetPartKey(), target2);

      // update the target map work of the second
      target2.setTmpPathForPartitionPruning(first.getTmpPathOfTargetWork());

      List<ExprNodeDesc> partKey = target2.getEventSourcePartKeyExprMap().get(secondId);
      partKey.remove(second.getTargetPartKey());
      if (partKey.isEmpty()) {
        target2.getEventSourcePartKeyExprMap().remove(secondId);
      }
      List<ExprNodeDesc> newPartKey = target2.getEventSourcePartKeyExprMap().computeIfAbsent(
          firstId, v -> new ArrayList<>());
      newPartKey.add(second.getTargetPartKey());

      List<TableDesc> tableDesc = target2.getEventSourceTableDescMap().get(secondId);
      tableDesc.remove(second.getTable());
      if (tableDesc.isEmpty()) {
        target2.getEventSourceTableDescMap().remove(secondId);
      }
      List<TableDesc> newTableDesc = target2.getEventSourceTableDescMap().computeIfAbsent(
          firstId, v -> new ArrayList<>());
      newTableDesc.add(second.getTable());

      List<String> columnName = target2.getEventSourceColumnNameMap().get(secondId);
      columnName.remove(second.getTargetColumnName());
      if (columnName.isEmpty()) {
        target2.getEventSourceColumnNameMap().remove(secondId);
      }
      List<String> newColumnName = target2.getEventSourceColumnNameMap().computeIfAbsent(
          firstId, v -> new ArrayList<>());
      newColumnName.add(second.getTargetColumnName());

      List<String> columnType = target2.getEventSourceColumnTypeMap().get(secondId);
      columnType.remove(second.getTargetColumnType());
      if (columnType.isEmpty()) {
        target2.getEventSourceColumnTypeMap().remove(secondId);
      }
      List<String> newColumnType = target2.getEventSourceColumnTypeMap().computeIfAbsent(
          firstId, v -> new ArrayList<>());
      newColumnType.add(second.getTargetColumnType());
    }

    private Set<BaseWork> combineEquivalentWorks(Set<Set<BaseWork>> equivalentWorks, SparkWork sparkWork) {
      Set<BaseWork> removedWorks = Sets.newHashSet();
      for (Set<BaseWork> workSet : equivalentWorks) {
        if (workSet.size() > 1) {
          Iterator<BaseWork> iterator = workSet.iterator();
          BaseWork first = iterator.next();
          List<SparkPartitionPruningSinkOperator> dppList1 = workToDpps.get(first);
          while (iterator.hasNext()) {
            BaseWork next = iterator.next();
            if (dppList1 != null) {
              List<SparkPartitionPruningSinkOperator> dppList2 = workToDpps.get(next);
              // equivalent works must have dpp lists of same size
              for (int i = 0; i < dppList1.size(); i++) {
                combineEquivalentDPPSinks(dppList1.get(i).getConf(), dppList2.get(i).getConf(),
                    dppList1.get(i).getUniqueId(), dppList2.get(i).getUniqueId());
              }
            }
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
      // In order to fix HIVE-16948
      if (previous instanceof MapWork) {
        removedMapWorkNames.add(previous.getName());
      }
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
        Set<Operator<? extends OperatorDesc>> leafOps = first.getAllLeafOperators();
        leafOps.addAll(second.getAllLeafOperators());
        for (Operator operator : leafOps) {
          // we know how to handle DPP sinks
          if (!(operator instanceof SparkPartitionPruningSinkOperator)) {
            return false;
          }
        }
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
        boolean result = compareOperatorChain(firstIterator.next(), secondIterator.next(),
            first, second);
        if (!result) {
          return false;
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

    private boolean compareOperatorChain(Operator<?> firstOperator, Operator<?> secondOperator,
        BaseWork first, BaseWork second) {
      boolean result = compareCurrentOperator(firstOperator, secondOperator);
      if (!result) {
        return false;
      }

      List<Operator<? extends OperatorDesc>> firstOperatorChildOperators = firstOperator.getChildOperators();
      List<Operator<? extends OperatorDesc>> secondOperatorChildOperators = secondOperator.getChildOperators();
      if (firstOperatorChildOperators == null && secondOperatorChildOperators != null) {
        return false;
      } else if (firstOperatorChildOperators != null && secondOperatorChildOperators == null) {
        return false;
      } else if (firstOperatorChildOperators != null) {
        if (firstOperatorChildOperators.size() != secondOperatorChildOperators.size()) {
          return false;
        }
        int size = firstOperatorChildOperators.size();
        for (int i = 0; i < size; i++) {
          result = compareOperatorChain(firstOperatorChildOperators.get(i),
              secondOperatorChildOperators.get(i), first, second);
          if (!result) {
            return false;
          }
        }
      }

      if (firstOperator instanceof SparkPartitionPruningSinkOperator) {
        List<SparkPartitionPruningSinkOperator> dpps = workToDpps.computeIfAbsent(
            first, k -> new ArrayList<>());
        dpps.add(((SparkPartitionPruningSinkOperator) firstOperator));
        dpps = workToDpps.computeIfAbsent(second, k -> new ArrayList<>());
        dpps.add(((SparkPartitionPruningSinkOperator) secondOperator));
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
      return firstOperator.logicalEquals(secondOperator);
    }

    /**
     * traverse the children in sparkWork to find the dpp sink operator which target work is included in
     * removedMapWorkList
     * If there is branch, remove prune sink operator branch in the BaseWork
     * If there is no branch, remove the whole BaseWork
     *
     * @param removedMapWorkList: the name of the map work has been deleted because they are equals to other works.
     * @param sparkWork:          current spark work
     */
    private void removeDynamicPartitionPruningSink(List<String> removedMapWorkList, SparkWork sparkWork) {
      List<BaseWork> allWorks = sparkWork.getAllWork();
      for (BaseWork baseWork : allWorks) {
        Set<Operator<?>> rootOperators = baseWork.getAllRootOperators();
        for (Operator root : rootOperators) {
          List<Operator<?>> pruningList = new ArrayList<>();
          SparkUtilities.collectOp(pruningList, root, SparkPartitionPruningSinkOperator.class);
          for (Operator pruneSinkOp : pruningList) {
            SparkPartitionPruningSinkOperator sparkPruneSinkOp = (SparkPartitionPruningSinkOperator) pruneSinkOp;
            if (removedMapWorkList.contains(sparkPruneSinkOp.getConf().getTargetMapWork().getName())) {
              LOG.debug("ready to remove the sparkPruneSinkOp which target work is " +
                  sparkPruneSinkOp.getConf().getTargetWorks() + " because the MapWork is equals to other map work and " +
                  "has been deleted!");
              // If there is branch, remove prune sink operator branch in the baseWork
              // If there is no branch, remove the whole baseWork
              if (OperatorUtils.isInBranch(sparkPruneSinkOp)) {
                OperatorUtils.removeBranch(sparkPruneSinkOp);
              } else {
                sparkWork.remove(baseWork);
              }
            }
          }
        }
      }
    }

    private void removeEmptySparkTask(SparkTask currTask) {
      // If currTask is rootTasks, remove it and add its children to the rootTasks which currTask is its only parent
      // task
      if (pctx.getRootTasks().contains(currTask)) {
        pctx.removeFromRootTask(currTask);
        List<Task<? extends Serializable>> newRoots = currTask.getChildTasks();
        for (Task newRoot : newRoots) {
          if (newRoot.getParentTasks().size() == 1) {
            pctx.addToRootTask(newRoot);
          }
        }
      }
      SparkUtilities.removeEmptySparkTask(currTask);
    }
  }
}
