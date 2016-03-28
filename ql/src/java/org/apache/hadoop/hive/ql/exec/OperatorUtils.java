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

package org.apache.hadoop.hive.ql.exec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.NodeUtils.Function;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.mapred.OutputCollector;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

public class OperatorUtils {

  private static final Log LOG = LogFactory.getLog(OperatorUtils.class);

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
    return found.size() == 1 ? found.iterator().next(): null;
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
            findOperatorsUpstream(parent, clazz, found);
          }
        } else {
          findOperatorsUpstream(parent, clazz, found);
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
      if(op.getName().equals(ReduceSinkOperator.getOperatorName())) {
        ReduceSinkOperator rs = ((ReduceSinkOperator)op);
        if (outMap.containsKey(rs.getConf().getOutputName())) {
          LOG.info("Setting output collector: " + rs + " --> "
            + rs.getConf().getOutputName());
          rs.setOutputCollector(outMap.get(rs.getConf().getOutputName()));
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

  /**
   * Starting at the input operator, finds the last operator upstream that is
   * an instance of the input class.
   *
   * @param op the starting operator
   * @param clazz the class that the operator that we are looking for instantiates
   * @return null if no such operator exists or multiple branches are found in
   * the stream, the last operator otherwise
   */
  @SuppressWarnings("unchecked")
  public static <T> T findLastOperatorUpstream(Operator<?> op, Class<T> clazz) {
    Operator<?> currentOp = op;
    T lastOp = null;
    while (currentOp != null) {
      if (clazz.isInstance(currentOp)) {
        lastOp = (T) currentOp;
      }
      if (currentOp.getParentOperators().size() == 1) {
        currentOp = currentOp.getParentOperators().get(0);
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

  public static boolean sameRowSchema(Operator<?> operator1, Operator<?> operator2) {
    return operator1.getSchema().equals(operator2.getSchema());
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
}
