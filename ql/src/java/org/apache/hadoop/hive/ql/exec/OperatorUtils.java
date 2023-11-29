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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.NodeUtils.Function;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.SemiJoinBranchInfo;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
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

  /**
   * Return the ancestor of the specified operator at the provided path or null if the path is invalid.
   *
   * The method is equivalent to following code:
   * <pre>{@code
   *     op.getParentOperators().get(path[0])
   *     .getParentOperators().get(path[1])
   *     ...
   *     .getParentOperators().get(path[n])
   * }</pre>
   * with additional checks about the validity of the provided path and the type of the ancestor.
   *
   * @param op the operator for which we
   * @param clazz the class of the ancestor operator
   * @param path the path leading to the desired ancestor
   * @param <T> the type of the ancestor
   * @return the ancestor of the specified operator at the provided path or null if the path is invalid.
   */
  public static <T> T ancestor(Operator<?> op, Class<T> clazz, int... path) {
    Operator<?> target = op;
    for (int i = 0; i < path.length; i++) {
      if (target.getParentOperators() == null || path[i] > target.getParentOperators().size()) {
        return null;
      }
      target = target.getParentOperators().get(path[i]);
    }
    return clazz.isInstance(target) ? clazz.cast(target) : null;
  }

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

  /**
   * Check whether there are more operators in the specified operator tree branch than the given limit
   * until a ReduceSinkOperator is reached.
   * The method traverses the parent operators of the specified root operator in dept first manner.
   * @param start root of the operator tree to check
   * @param opClazz type of operator to track
   * @param limit maximum allowed number of operator in a branch of the tree
   * @return true if limit is exceeded false otherwise
   */
  public static boolean hasMoreOperatorsThan(
      Operator<?> start, Class<?> opClazz, int limit) {
    return hasMoreOperatorsThan(start, opClazz, limit, new HashSet<>());
  }

  private static boolean hasMoreOperatorsThan(
      Operator<?> start, Class<?> opClazz, int limit, Set<Operator<?>> visited) {
    if (!visited.add(start)) {
      return limit < 0;
    }

    if (limit < 0) {
      return false;
    }

    if (start instanceof ReduceSinkOperator) {
      return false;
    }

    if (opClazz.isInstance(start)) {
      limit--;
    }

    if (start.getParentOperators() != null) {
      for (Operator<?> parent : start.getParentOperators()) {
        if (hasMoreOperatorsThan(parent, opClazz, limit, visited)) {
          return true;
        }
      }
    }
    return limit < 0;
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

  public static Operator<?> findOperatorByMarker(Operator<?> start, String marker) {
    Deque<Operator<?>> queue = new ArrayDeque<>();
    queue.add(start);
    while (!queue.isEmpty()) {
      Operator<?> op = queue.remove();
      if (marker.equals(op.getMarker())) {
        return op;
      }
      if (op.getChildOperators() != null) {
        queue.addAll(op.getChildOperators());
      }
    }
    return null;
  }

  public static Set<Operator<?>>
  findWorkOperatorsAndSemiJoinEdges(Operator<?> start,
                                    final Map<ReduceSinkOperator, SemiJoinBranchInfo> rsToSemiJoinBranchInfo,
                                    Set<ReduceSinkOperator> semiJoinOps, Set<TerminalOperator<?>> terminalOps) {
    Set<Operator<?>> found = new HashSet<>();
    findWorkOperatorsAndSemiJoinEdges(start,
            found, rsToSemiJoinBranchInfo, semiJoinOps, terminalOps);
    return found;
  }

  private static void
  findWorkOperatorsAndSemiJoinEdges(Operator<?> start, Set<Operator<?>> found,
                                    final Map<ReduceSinkOperator, SemiJoinBranchInfo> rsToSemiJoinBranchInfo,
                                    Set<ReduceSinkOperator> semiJoinOps, Set<TerminalOperator<?>> terminalOps) {
    found.add(start);

    if (start.getParentOperators() != null) {
      for (Operator<?> parent : start.getParentOperators()) {
        if (parent instanceof ReduceSinkOperator) {
          continue;
        }
        if (!found.contains(parent)) {
          findWorkOperatorsAndSemiJoinEdges(parent, found, rsToSemiJoinBranchInfo, semiJoinOps, terminalOps);
        }
      }
    }
    if (start instanceof TerminalOperator) {
      // This could be RS1 in semijoin edge which looks like,
      // SEL->GBY1->RS1->GBY2->RS2
      boolean semiJoin = false;
      if (start.getChildOperators().size() == 1) {
        Operator<?> gb2 = start.getChildOperators().get(0);
        if (gb2 instanceof GroupByOperator && gb2.getChildOperators().size() == 1) {
          Operator<?> rs2 = gb2.getChildOperators().get(0);
          if (rs2 instanceof ReduceSinkOperator && (rsToSemiJoinBranchInfo.get(rs2) != null)) {
            // Semijoin edge found. Add all the operators to the set
            found.add(start);
            found.add(gb2);
            found.add(rs2);
            semiJoinOps.add((ReduceSinkOperator)rs2);
            semiJoin = true;
          }
        }
      }
      if (!semiJoin) {
        terminalOps.add((TerminalOperator)start);
      }
      return;
    }
    if (start.getChildOperators() != null) {
      for (Operator<?> child : start.getChildOperators()) {
        if (!found.contains(child)) {
          findWorkOperatorsAndSemiJoinEdges(child, found, rsToSemiJoinBranchInfo, semiJoinOps, terminalOps);
        }
      }
    }
    return;
  }

  private static List<ExprNodeDesc> backtrackAll(List<ExprNodeDesc> exprs, Operator<? extends  OperatorDesc> start,
                                                 Operator<? extends OperatorDesc> terminal) {
    List<ExprNodeDesc> backtrackedExprs = new ArrayList<>();
    try {
      for (ExprNodeDesc expr : exprs) {
        ExprNodeDesc backtrackedExpr = ExprNodeDescUtils.backtrack(expr, start, terminal);
        if(backtrackedExpr == null) {
          return null;
        }
        backtrackedExprs.add(backtrackedExpr);

      }
    } catch (SemanticException e) {
      return null;
    }
    return backtrackedExprs;
  }

  // set of expressions are considered compatible if following are true:
  //  * they are both same size
  //  * if the are column expressions their table alias is same as well (this is checked because otherwise
  //      expressions coming out of multiple RS (e.g. children of JOIN) are ended up same
  private static boolean areBacktrackedExprsCompatible(final List<ExprNodeDesc> orgexprs,
                                                       final List<ExprNodeDesc> backtrackedExprs) {
    if(backtrackedExprs == null || backtrackedExprs.size() != orgexprs.size()) {
      return false;
    }
    for(int i=0; i<orgexprs.size(); i++) {
      if(orgexprs.get(i) instanceof ExprNodeColumnDesc && backtrackedExprs.get(i) instanceof ExprNodeColumnDesc) {
        ExprNodeColumnDesc orgColExpr = (ExprNodeColumnDesc)orgexprs.get(i);
        ExprNodeColumnDesc backExpr = (ExprNodeColumnDesc)backtrackedExprs.get(i);
        String orgTabAlias = orgColExpr.getTabAlias();
        String backTabAlias = backExpr.getTabAlias();

        if(orgTabAlias != null && backTabAlias != null && !orgTabAlias.equals(backTabAlias)) {
          return false;
        }
      }
    }
    return true;
  }

  /***
   * This method backtracks the given expressions to the source RS. Note that expressions could
   * further be backtracked to e.g. table source, but we are interested in RS only because this
   * is used to estimate number of rows for group by and estimation will be better at RS since all
   * the filters etc will have already been applied
   * @param start
   * @param exprs
   * @return null if RS is not found
   */
  public static Operator<? extends OperatorDesc> findSourceRS(Operator<?> start, List<ExprNodeDesc> exprs) {
    Operator currRS = null; //keep track of the RS
    if (start instanceof ReduceSinkOperator) {
      currRS = start;
    }

    if (start instanceof UnionOperator) {
      //Union keeps the schema same but can change the cardinality, therefore we don't want to backtrack further
      // into Union
      return currRS;
    }

    List<Operator<? extends OperatorDesc>> parents = start.getParentOperators();
    if (parents == null | parents.isEmpty()) {
      // reached end e.g. TS operator
      return null;
    }

    Operator<? extends OperatorDesc> nextOp = null;
    List<ExprNodeDesc> backtrackedExprs = null;
    for (int i = 0; i < parents.size(); i++) {
      backtrackedExprs = backtrackAll(exprs, start, parents.get(i));
      if (areBacktrackedExprsCompatible(exprs, backtrackedExprs)) {
        nextOp = parents.get(i);
        break;
      }
    }
    if (nextOp != null) {
      Operator<? extends OperatorDesc> nextRS = findSourceRS(nextOp, backtrackedExprs);
      if (nextRS != null) {
        currRS = nextRS;
      }
    }
    return currRS;
  }

  /***
   * Given group by operator on reduce side, this tries to get to the group by on map side (partial/merge).
   * @param reduceSideGbOp Make sure this is group by side reducer
   * @return map side gb if any, else null
   */
  public static GroupByOperator findMapSideGb(final GroupByOperator reduceSideGbOp) {
    Operator<? extends OperatorDesc> parentOp = reduceSideGbOp;
    while(parentOp.getParentOperators() != null && parentOp.getParentOperators().size() > 0) {
      if(parentOp.getParentOperators().size() > 1) {
        return null;
      }
      parentOp = parentOp.getParentOperators().get(0);
      if(parentOp instanceof GroupByOperator) {
        return (GroupByOperator)parentOp;
      }
    }
    return null;
  }

  /**
   *  Determines if the two trees are using independent inputs.
   */
  public static boolean treesWithIndependentInputs(Operator<?> tree1, Operator<?> tree2) {
    Set<String> tables1 = signaturesOf(OperatorUtils.findOperatorsUpstream(tree1, TableScanOperator.class));
    Set<String> tables2 = signaturesOf(OperatorUtils.findOperatorsUpstream(tree2, TableScanOperator.class));

    tables1.retainAll(tables2);
    return tables1.isEmpty();
  }

  private static Set<String> signaturesOf(Set<TableScanOperator> ops) {
    Set<String> ret = new HashSet<>();
    for (TableScanOperator o : ops) {
      ret.add(o.getConf().getQualifiedTable());
    }
    return ret;
  }

  /**
   * Given start, end operators and a colName, look for the original TS colExpr
   * that it originates from. This method checks column Expr mappings (aliases)
   * from the start operator and its parents up to the end operator.
   * It then returns the unwrapped Expr found within a ExprNodeGenericFuncDesc (if any).
   * @param colName colName to backtrack
   * @param start start backtracking from this Op
   * @param end end backtracking at this Op
   * @return the original column name or null if not found
   */
  public static ExprNodeColumnDesc findTableOriginColExpr(ExprNodeColumnDesc colName, Operator<?> start, Operator<?> end)
      throws SemanticException {
    ExprNodeDesc res = ExprNodeDescUtils.backtrack(colName, start, end, false, true);
    return (res != null) ? ExprNodeDescUtils.getColumnExpr(res) : null;
  }

  public static Set<Operator<?>> getAllOperatorsForSimpleFetch(Set<Operator<?>> opSet) {
    Set<Operator<?>> returnSet = new LinkedHashSet<Operator<?>>();
    Stack<Operator<?>> opStack = new Stack<Operator<?>>();
    // add all children
    opStack.addAll(opSet);
    while (!opStack.empty()) {
      Operator<?> op = opStack.pop();
      returnSet.add(op);
      if (op.getChildOperators() != null) {
        opStack.addAll(op.getChildOperators());
      }
    }
    return returnSet;
  }

  /**
   * Given a {@link FetchTask} this returns a set of all the operators within the task
   * @param task - Fetch Task
   */
  public static Set<Operator<?>> getAllFetchOperators(FetchTask task) {
    if (task.getWork().getSource() == null)  {
      return Collections.EMPTY_SET;
    }
    Set<Operator<?>> operatorList =  new HashSet<>();
    operatorList.add(task.getWork().getSource());
    return getAllOperatorsForSimpleFetch(operatorList);
  }

}
