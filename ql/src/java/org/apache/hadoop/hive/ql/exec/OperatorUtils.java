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

package org.apache.hadoop.hive.ql.exec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.NodeUtils.Function;
import org.apache.hadoop.hive.ql.parse.spark.SparkPartitionPruningSinkOperator;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.mapred.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

public class OperatorUtils {

  private static final Logger LOG = LoggerFactory.getLogger(OperatorUtils.class);

  public static <T> Set<T> findOperators(Operator<?> start, Class<T> clazz) {
    return findOperators(start, clazz, new HashSet<T>());
  }

  public static <T> T findSingleOperator(Operator<?> start, Class<T> clazz) {
    Set<T> found = findOperators(start, clazz, new HashSet<T>());
    return found.size() == 1 ? found.iterator().next() : null;
  }

  public static <T> Set<T> findOperators(Collection<Operator<?>> starts, Class<T> clazz) {
    Set<T> found = new HashSet<T>();
    for (Operator<?> start : starts) {
      if (start == null) {
        continue;
      }
      findOperators(start, clazz, found);
    }
    return found;
  }

  @SuppressWarnings("unchecked")
  private static <T> Set<T> findOperators(Operator<?> start, Class<T> clazz, Set<T> found) {
    if (clazz.isInstance(start)) {
      found.add((T) start);
    }
    if (start.getChildOperators() != null) {
      for (Operator<?> child : start.getChildOperators()) {
        findOperators(child, clazz, found);
      }
    }
    return found;
  }

  public static <T> Set<T> findOperatorsUpstream(Operator<?> start, Class<T> clazz) {
    return findOperatorsUpstream(start, clazz, new HashSet<T>());
  }

  public static <T> T findSingleOperatorUpstream(Operator<?> start, Class<T> clazz) {
    Set<T> found = findOperatorsUpstream(start, clazz, new HashSet<T>());
    return found.size() == 1 ? found.iterator().next() : null;
  }

  public static <T> T findSingleOperatorUpstreamJoinAccounted(Operator<?> start, Class<T> clazz) {
    Set<T> found = findOperatorsUpstreamJoinAccounted(start, clazz, new HashSet<T>());
    return found.size() >= 1 ? found.iterator().next(): null;
  }

  public static <T> Set<T> findOperatorsUpstream(Collection<Operator<?>> starts, Class<T> clazz) {
    Set<T> found = new HashSet<T>();
    for (Operator<?> start : starts) {
      findOperatorsUpstream(start, clazz, found);
    }
    return found;
  }

  @SuppressWarnings("unchecked")
  private static <T> Set<T> findOperatorsUpstream(Operator<?> start, Class<T> clazz, Set<T> found) {
    if (clazz.isInstance(start)) {
      found.add((T) start);
    }
    if (start.getParentOperators() != null) {
      for (Operator<?> parent : start.getParentOperators()) {
        findOperatorsUpstream(parent, clazz, found);
      }
    }
    return found;
  }

  public static <T> Set<T> findOperatorsUpstreamJoinAccounted(Operator<?> start, Class<T> clazz,
      Set<T> found) {
    if (clazz.isInstance(start)) {
      found.add((T) start);
    }
    int onlyIncludeIndex = -1;
    if (start instanceof AbstractMapJoinOperator) {
      AbstractMapJoinOperator mapJoinOp = (AbstractMapJoinOperator) start;
      MapJoinDesc desc = (MapJoinDesc) mapJoinOp.getConf();
      onlyIncludeIndex = desc.getPosBigTable();
    }
    if (start.getParentOperators() != null) {
      int i = 0;
      for (Operator<?> parent : start.getParentOperators()) {
        if (onlyIncludeIndex >= 0) {
          if (onlyIncludeIndex == i) {
            findOperatorsUpstreamJoinAccounted(parent, clazz, found);
          }
        } else {
          findOperatorsUpstreamJoinAccounted(parent, clazz, found);
        }
        i++;
      }
    }
    return found;
  }


  public static void setChildrenCollector(List<Operator<? extends OperatorDesc>> childOperators, OutputCollector out) {
    if (childOperators == null) {
      return;
    }
    for (Operator<? extends OperatorDesc> op : childOperators) {
      if (op.getName().equals(ReduceSinkOperator.getOperatorName())) {
        op.setOutputCollector(out);
      } else {
        setChildrenCollector(op.getChildOperators(), out);
      }
    }
  }

  public static void setChildrenCollector(List<Operator<? extends OperatorDesc>> childOperators, Map<String, OutputCollector> outMap) {
    if (childOperators == null) {
      return;
    }
    for (Operator<? extends OperatorDesc> op : childOperators) {
      if (op.getIsReduceSink()) {
        String outputName = op.getReduceOutputName();
        if (outMap.containsKey(outputName)) {
          LOG.info("Setting output collector: " + op + " --> " + outputName);
          op.setOutputCollector(outMap.get(outputName));
        }
      } else {
        setChildrenCollector(op.getChildOperators(), outMap);
      }
    }
  }

  /**
   * Starting at the input operator, finds the last operator in the stream that
   * is an instance of the input class.
   *
   * @param op the starting operator
   * @param clazz the class that the operator that we are looking for instantiates
   * @return null if no such operator exists or multiple branches are found in
   * the stream, the last operator otherwise
   */
  @SuppressWarnings("unchecked")
  public static <T> T findLastOperator(Operator<?> op, Class<T> clazz) {
    Operator<?> currentOp = op;
    T lastOp = null;
    while (currentOp != null) {
      if (clazz.isInstance(currentOp)) {
        lastOp = (T) currentOp;
      }
      if (currentOp.getChildOperators().size() == 1) {
        currentOp = currentOp.getChildOperators().get(0);
      }
      else {
        currentOp = null;
      }
    }
    return lastOp;
  }

  public static void iterateParents(Operator<?> operator, Function<Operator<?>> function) {
    iterateParents(operator, function, new HashSet<Operator<?>>());
  }

  private static void iterateParents(Operator<?> operator, Function<Operator<?>> function, Set<Operator<?>> visited) {
    if (!visited.add(operator)) {
      return;
    }
    function.apply(operator);
    if (operator.getNumParent() > 0) {
      for (Operator<?> parent : operator.getParentOperators()) {
        iterateParents(parent, function, visited);
      }
    }
  }

  /**
   * Given an operator and a set of classes, it classifies the operators it finds
   * in the stream depending on the classes they instantiate.
   *
   * If a given operator object is an instance of more than one of the input classes,
   * e.g. the operator instantiates one of the classes in the input set that is a
   * subclass of another class in the set, the operator will be associated to both
   * classes in the output map.
   *
   * @param start the start operator
   * @param classes the set of classes
   * @return a multimap from each of the classes to the operators that instantiate
   * them
   */
  public static Multimap<Class<? extends Operator<?>>, Operator<?>> classifyOperators(
      Operator<?> start, Set<Class<? extends Operator<?>>> classes) {
    ImmutableMultimap.Builder<Class<? extends Operator<?>>, Operator<?>> resultMap =
        new ImmutableMultimap.Builder<Class<? extends Operator<?>>, Operator<?>>();
    List<Operator<?>> ops = new ArrayList<Operator<?>>();
    ops.add(start);
    while (!ops.isEmpty()) {
      List<Operator<?>> allChildren = new ArrayList<Operator<?>>();
      for (Operator<?> op: ops) {
        for (Class<? extends Operator<?>> clazz: classes) {
          if (clazz.isInstance(op)) {
            resultMap.put(clazz, op);
          }
        }
        allChildren.addAll(op.getChildOperators());
      }
      ops = allChildren;
    }
    return resultMap.build();
  }

  /**
   * Given an operator and a set of classes, it classifies the operators it finds
   * upstream depending on the classes it instantiates.
   *
   * If a given operator object is an instance of more than one of the input classes,
   * e.g. the operator instantiates one of the classes in the input set that is a
   * subclass of another class in the set, the operator will be associated to both
   * classes in the output map.
   *
   * @param start the start operator
   * @param classes the set of classes
   * @return a multimap from each of the classes to the operators that instantiate
   * them
   */
  public static Multimap<Class<? extends Operator<?>>, Operator<?>> classifyOperatorsUpstream(
      Operator<?> start, Set<Class<? extends Operator<?>>> classes) {
    ImmutableMultimap.Builder<Class<? extends Operator<?>>, Operator<?>> resultMap =
        new ImmutableMultimap.Builder<Class<? extends Operator<?>>, Operator<?>>();
    List<Operator<?>> ops = new ArrayList<Operator<?>>();
    ops.add(start);
    while (!ops.isEmpty()) {
      List<Operator<?>> allParent = new ArrayList<Operator<?>>();
      for (Operator<?> op: ops) {
        for (Class<? extends Operator<?>> clazz: classes) {
          if (clazz.isInstance(op)) {
            resultMap.put(clazz, op);
          }
        }
        if (op.getParentOperators() != null) {
          allParent.addAll(op.getParentOperators());
        }
      }
      ops = allParent;
    }
    return resultMap.build();
  }

  /**
   * Given an operator and a set of classes, it returns the number of operators it finds
   * upstream that instantiate any of the given classes.
   *
   * @param start the start operator
   * @param classes the set of classes
   * @return the number of operators
   */
  public static int countOperatorsUpstream(Operator<?> start, Set<Class<? extends Operator<?>>> classes) {
    Multimap<Class<? extends Operator<?>>, Operator<?>> ops = classifyOperatorsUpstream(start, classes);
    int numberOperators = 0;
    Set<Operator<?>> uniqueOperators = new HashSet<Operator<?>>();
    for (Operator<?> op : ops.values()) {
      if (uniqueOperators.add(op)) {
        numberOperators++;
      }
    }
    return numberOperators;
  }

  public static void setMemoryAvailable(final List<Operator<? extends OperatorDesc>> operators,
    final long memoryAvailableToTask) {
    if (operators == null) {
      return;
    }

    for (Operator<? extends OperatorDesc> op : operators) {
      if (op.getConf() != null) {
        op.getConf().setMaxMemoryAvailable(memoryAvailableToTask);
      }
      if (op.getChildOperators() != null && !op.getChildOperators().isEmpty()) {
        setMemoryAvailable(op.getChildOperators(), memoryAvailableToTask);
      }
    }
  }

  /**
   * Given the input operator 'op', walk up the operator tree from 'op', and collect all the
   * roots that can be reached from it. The results are stored in 'roots'.
   */
  public static void findRoots(Operator<?> op, Collection<Operator<?>> roots) {
    List<Operator<?>> parents = op.getParentOperators();
    if (parents == null || parents.isEmpty()) {
      roots.add(op);
      return;
    }
    for (Operator<?> p : parents) {
      findRoots(p, roots);
    }
  }

  /**
   * Remove the branch that contains the specified operator. Do nothing if there's no branching,
   * i.e. all the upstream operators have only one child.
   */
  public static void removeBranch(SparkPartitionPruningSinkOperator op) {
    Operator<?> child = op;
    Operator<?> curr = op;

    while (curr.getChildOperators().size() <= 1) {
      child = curr;
      if (curr.getParentOperators() == null || curr.getParentOperators().isEmpty()) {
        return;
      }
      curr = curr.getParentOperators().get(0);
    }

    curr.removeChild(child);
  }

  /**
   * Remove operator from the tree, disconnecting it from its
   * parents and children.
   */
  public static void removeOperator(Operator<?> op) {
    if (op.getNumParent() != 0) {
      List<Operator<? extends OperatorDesc>> allParent =
              Lists.newArrayList(op.getParentOperators());
      for (Operator<?> parentOp : allParent) {
        parentOp.removeChild(op);
      }
    }
    if (op.getNumChild() != 0) {
      List<Operator<? extends OperatorDesc>> allChildren =
              Lists.newArrayList(op.getChildOperators());
      for (Operator<?> childOp : allChildren) {
        childOp.removeParent(op);
      }
    }
  }

  public static String getOpNamePretty(Operator<?> op) {
    if (op instanceof TableScanOperator) {
      return op.toString() + " (" + ((TableScanOperator) op).getConf().getAlias() + ")";
    }
    return op.toString();
  }

  /**
   * Return true if contain branch otherwise return false
   */
  public static boolean isInBranch(SparkPartitionPruningSinkOperator op) {
    Operator<?> curr = op;
    while (curr.getChildOperators().size() <= 1) {
      if (curr.getParentOperators() == null || curr.getParentOperators().isEmpty()) {
        return false;
      }
      curr = curr.getParentOperators().get(0);
    }
    return true;
  }

  public static Set<Operator<?>> getOp(BaseWork work, Class<?> clazz) {
    Set<Operator<?>> ops = new HashSet<Operator<?>>();
    if (work instanceof MapWork) {
      Collection<Operator<?>> opSet = ((MapWork) work).getAliasToWork().values();
      Stack<Operator<?>> opStack = new Stack<Operator<?>>();
      opStack.addAll(opSet);

      while (!opStack.empty()) {
        Operator<?> op = opStack.pop();
        ops.add(op);
        if (op.getChildOperators() != null) {
          opStack.addAll(op.getChildOperators());
        }
      }
    } else {
      ops.addAll(work.getAllOperators());
    }

    Set<Operator<? extends OperatorDesc>> matchingOps =
      new HashSet<Operator<? extends OperatorDesc>>();
    for (Operator<? extends OperatorDesc> op : ops) {
      if (clazz.isInstance(op)) {
        matchingOps.add(op);
      }
    }
    return matchingOps;
  }
}
