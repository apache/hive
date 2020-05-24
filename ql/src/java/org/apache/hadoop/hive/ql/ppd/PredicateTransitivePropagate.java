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

package org.apache.hadoop.hive.ql.ppd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.LevelOrderWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * propagates filters to other aliases based on join condition
 */
public class PredicateTransitivePropagate extends Transform {

  private ParseContext pGraphContext;

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    pGraphContext = pctx;

    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    opRules.put(new RuleRegExp("R1", "(" +
        FilterOperator.getOperatorName() + "%" +
        ReduceSinkOperator.getOperatorName() + "%" +
        JoinOperator.getOperatorName() + "%)"), new JoinTransitive());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    TransitiveContext context = new TransitiveContext();
    SemanticDispatcher disp = new DefaultRuleDispatcher(null, opRules, context);
    SemanticGraphWalker ogw = new LevelOrderWalker(disp, 2);

    // Create a list of topop nodes
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pGraphContext.getTopOps().values());
    ogw.startWalking(topNodes, null);

    Map<ReduceSinkOperator, List<ExprNodeDesc>> newFilters = context.getNewfilters();

    // insert new filter between RS and parent of RS
    for (Map.Entry<ReduceSinkOperator, List<ExprNodeDesc>> entry : newFilters.entrySet()) {
      ReduceSinkOperator reducer = entry.getKey();
      Operator<?> parent = reducer.getParentOperators().get(0);

      List<ExprNodeDesc> exprs = entry.getValue();
      if (parent instanceof FilterOperator) {
        exprs = ExprNodeDescUtils.split(((FilterOperator)parent).getConf().getPredicate(), exprs);
        ExprNodeDesc merged = ExprNodeDescUtils.mergePredicates(exprs);
        ((FilterOperator)parent).getConf().setPredicate(merged);
      } else {
        ExprNodeDesc merged = ExprNodeDescUtils.mergePredicates(exprs);
        RowSchema parentRS = parent.getSchema();
        Operator<FilterDesc> newFilter = createFilter(reducer, parent, parentRS, merged);
      }
    }

    return pGraphContext;
  }

  // insert filter operator between target(child) and input(parent)
  private Operator<FilterDesc> createFilter(Operator<?> target, Operator<?> parent,
      RowSchema parentRS, ExprNodeDesc filterExpr) {
    Operator<FilterDesc> filter = OperatorFactory.get(parent.getCompilationOpContext(),
        new FilterDesc(filterExpr, false), new RowSchema(parentRS.getSignature()));
    filter.getParentOperators().add(parent);
    filter.getChildOperators().add(target);
    parent.replaceChild(target, filter);
    target.replaceParent(parent, filter);
    return filter;
  }

  private static class TransitiveContext implements NodeProcessorCtx {

    private final Map<CommonJoinOperator, int[][]> filterPropagates;
    private final Map<ReduceSinkOperator, List<ExprNodeDesc>> newFilters;

    public TransitiveContext() {
      filterPropagates = new HashMap<CommonJoinOperator, int[][]>();
      newFilters = new HashMap<ReduceSinkOperator, List<ExprNodeDesc>>();
    }

    public Map<CommonJoinOperator, int[][]> getFilterPropagates() {
      return filterPropagates;
    }

    public Map<ReduceSinkOperator, List<ExprNodeDesc>> getNewfilters() {
      return newFilters;
    }
  }

  private static class JoinTransitive implements SemanticNodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      @SuppressWarnings("unchecked")
      CommonJoinOperator<JoinDesc> join = (CommonJoinOperator) nd;
      ReduceSinkOperator source = (ReduceSinkOperator) stack.get(stack.size() - 2);
      FilterOperator filter = (FilterOperator) stack.get(stack.size() - 3);
      int srcPos = join.getParentOperators().indexOf(source);

      TransitiveContext context = (TransitiveContext) procCtx;
      Map<CommonJoinOperator, int[][]> filterPropagates = context.getFilterPropagates();
      Map<ReduceSinkOperator, List<ExprNodeDesc>> newFilters = context.getNewfilters();

      int[][] targets = filterPropagates.get(join);
      if (targets == null) {
        filterPropagates.put(join, targets = getTargets(join));
      }

      List<Operator<? extends OperatorDesc>> parents = join.getParentOperators();
      for (int targetPos : targets[srcPos]) {
        ReduceSinkOperator target = (ReduceSinkOperator) parents.get(targetPos);
        List<ExprNodeDesc> sourceKeys = source.getConf().getKeyCols();
        List<ExprNodeDesc> targetKeys = target.getConf().getKeyCols();

        ExprNodeDesc predicate = filter.getConf().getPredicate();
        ExprNodeDesc replaced = ExprNodeDescUtils.replace(predicate, sourceKeys, targetKeys);
        if (replaced != null && !filterExists(target, replaced)) {
          List<ExprNodeDesc> prev = newFilters.get(target);
          if (prev == null) {
            newFilters.put(target, ExprNodeDescUtils.split(replaced));
          } else {
            ExprNodeDescUtils.split(replaced, prev);
          }
        }
      }
      return null;
    }

    // check same filter exists already
    private boolean filterExists(ReduceSinkOperator target, ExprNodeDesc replaced) {
      Operator<?> operator = target.getParentOperators().get(0);
      for (; operator instanceof FilterOperator; operator = operator.getParentOperators().get(0)) {
        ExprNodeDesc predicate = ((FilterOperator) operator).getConf().getPredicate();
        if (ExprNodeDescUtils.containsPredicate(predicate, replaced)) {
          return true;
        }
      }
      return false;
    }
  }

  // calculate filter propagation directions for each alias
  // L<->R for inner/semi join, L->R for left outer join, R->L for right outer join
  public static int[][] getTargets(CommonJoinOperator<JoinDesc> join) {
    JoinCondDesc[] conds = join.getConf().getConds();

    int aliases = conds.length + 1;
    Vectors vector = new Vectors(aliases);
    for (JoinCondDesc cond : conds) {
      int left = cond.getLeft();
      int right = cond.getRight();
      switch (cond.getType()) {
        case JoinDesc.INNER_JOIN:
        case JoinDesc.LEFT_SEMI_JOIN:
          vector.add(left, right);
          vector.add(right, left);
          break;
        case JoinDesc.LEFT_OUTER_JOIN:
          vector.add(left, right);
          break;
        case JoinDesc.RIGHT_OUTER_JOIN:
          vector.add(right, left);
          break;
        case JoinDesc.FULL_OUTER_JOIN:
          break;
      }
    }
    int[][] result = new int[aliases][];
    for (int pos = 0 ; pos < aliases; pos++) {
      // find all targets recursively
      result[pos] = vector.traverse(pos);
    }
    return result;
  }

  private static class Vectors {

    private final Set<Integer>[] vector;

    @SuppressWarnings("unchecked")
    public Vectors(int length) {
      vector = new Set[length];
    }

    public void add(int from, int to) {
      if (vector[from] == null) {
        vector[from] = new HashSet<Integer>();
      }
      vector[from].add(to);
    }

    public int[] traverse(int pos) {
      Set<Integer> targets = new HashSet<Integer>();
      traverse(targets, pos);
      return toArray(targets, pos);
    }

    private int[] toArray(Set<Integer> values, int pos) {
      values.remove(pos);
      int index = 0;
      int[] result = new int[values.size()];
      for (int value : values) {
        result[index++] = value;
      }
      return result;
    }

    private void traverse(Set<Integer> targets, int pos) {
      if (vector[pos] == null) {
        return;
      }
      for (int target : vector[pos]) {
        if (targets.add(target)) {
          traverse(targets, target);
        }
      }
    }
  }
}
