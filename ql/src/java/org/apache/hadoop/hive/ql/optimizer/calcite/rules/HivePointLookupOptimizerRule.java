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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveIn;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

/**
 * This optimization attempts to identify and close expanded INs.
 *
 * Basically:
 * <pre>
 * (c) IN ( v1, v2, ...) &lt;=&gt; c1=v1 || c1=v2 || ...
 * </pre>
 * If c is struct; then c=v1 is a group of anded equations.
 */
public abstract class HivePointLookupOptimizerRule extends RelOptRule {

  /** Rule adapter to apply the transformation to Filter conditions. */
  public static class FilterCondition extends HivePointLookupOptimizerRule {
    public FilterCondition (int minNumORClauses) {
      super(operand(Filter.class, any()), minNumORClauses);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Filter filter = call.rel(0);
      final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
      final RexNode condition = RexUtil.pullFactors(rexBuilder, filter.getCondition());

      RexNode newCondition = analyzeRexNode(rexBuilder, condition);

      // If we could not transform anything, we bail out
      if (newCondition.toString().equals(condition.toString())) {
        return;
      }
      RelNode newNode = filter.copy(filter.getTraitSet(), filter.getInput(), newCondition);

      call.transformTo(newNode);
    }
  }

  /** Rule adapter to apply the transformation to Join conditions. */
  public static class JoinCondition extends HivePointLookupOptimizerRule {
    public JoinCondition (int minNumORClauses) {
      super(operand(Join.class, any()), minNumORClauses);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Join join = call.rel(0);
      final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
      final RexNode condition = RexUtil.pullFactors(rexBuilder, join.getCondition());

      RexNode newCondition = analyzeRexNode(rexBuilder, condition);

      // If we could not transform anything, we bail out
      if (newCondition.toString().equals(condition.toString())) {
        return;
      }

      RelNode newNode = join.copy(join.getTraitSet(),
          newCondition,
          join.getLeft(),
          join.getRight(),
          join.getJoinType(),
          join.isSemiJoinDone());

      call.transformTo(newNode);
    }
  }

  /** Rule adapter to apply the transformation to Projections. */
  public static class ProjectionExpressions extends HivePointLookupOptimizerRule {
    public ProjectionExpressions(int minNumORClauses) {
      super(operand(Project.class, any()), minNumORClauses);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Project project = call.rel(0);
      boolean changed = false;
      final RexBuilder rexBuilder = project.getCluster().getRexBuilder();
      List<RexNode> newProjects = new ArrayList<>();
      for (RexNode oldNode : project.getProjects()) {
        RexNode newNode = analyzeRexNode(rexBuilder, oldNode);
        if (!newNode.toString().equals(oldNode.toString())) {
          changed = true;
          newProjects.add(newNode);
        } else {
          newProjects.add(oldNode);
        }
      }
      if (!changed) {
        return;
      }
      Project newProject = project.copy(project.getTraitSet(), project.getInput(), newProjects,
          project.getRowType(), project.getFlags());
      call.transformTo(newProject);

    }

  }

  protected static final Logger LOG = LoggerFactory.getLogger(HivePointLookupOptimizerRule.class);

  // Minimum number of OR clauses needed to transform into IN clauses
  protected final int minNumORClauses;

  protected HivePointLookupOptimizerRule(
    RelOptRuleOperand operand, int minNumORClauses) {
    super(operand);
    this.minNumORClauses = minNumORClauses;
  }

  public RexNode analyzeRexNode(RexBuilder rexBuilder, RexNode condition) {
    // 1. We try to transform possible candidates
    RexTransformIntoInClause transformIntoInClause = new RexTransformIntoInClause(rexBuilder, minNumORClauses);
    RexNode newCondition = transformIntoInClause.apply(condition);

    // 2. We merge IN expressions
    RexMergeInClause mergeInClause = new RexMergeInClause(rexBuilder);
    newCondition = mergeInClause.apply(newCondition);
    return newCondition;
  }


  /**
   * Transforms OR clauses into IN clauses, when possible.
   */
  protected static class RexTransformIntoInClause extends RexShuttle {
    private final RexBuilder rexBuilder;
    private final int minNumORClauses;

    RexTransformIntoInClause(RexBuilder rexBuilder, int minNumORClauses) {
      this.rexBuilder = rexBuilder;
      this.minNumORClauses = minNumORClauses;
    }

    @Override
    public RexNode visitCall(RexCall inputCall) {
      RexNode node = super.visitCall(inputCall);
      if (node instanceof RexCall) {
        RexCall call = (RexCall) node;
        switch (call.getKind()) {
        case OR:
          try {
            RexNode newNode = transformIntoInClauseCondition(rexBuilder,
                call, minNumORClauses);
            if (newNode != null) {
              return newNode;
            }
          } catch (SemanticException e) {
            LOG.error("Exception in HivePointLookupOptimizerRule", e);
            return call;
          }
        default:
          break;
        }
      }
      return node;
    }

    /**
     * This class just wraps around a RexNode enables equals/hashCode based on toString.
     *
     * After CALCITE-2632 this might not be needed anymore */
    static class RexNodeRef {

      public static Comparator<RexNodeRef> COMPARATOR =
          (RexNodeRef o1, RexNodeRef o2) -> o1.node.toString().compareTo(o2.node.toString());
      private RexNode node;

      public RexNodeRef(RexNode node) {
        this.node = node;
      }

      public RexNode getRexNode() {
        return node;
      }

      @Override
      public int hashCode() {
        return node.toString().hashCode();
      }

      @Override
      public boolean equals(Object o) {
        if (o instanceof RexNodeRef) {
          RexNodeRef otherRef = (RexNodeRef) o;
          return node.toString().equals(otherRef.node.toString());
        }
        return false;
      }

      @Override
      public String toString() {
        return "ref for:" + node.toString();
      }
    }
    /**
     * Represents a contraint.
     *
     * Example: a=1
     * substr(a,1,2) = concat('asd','xxx')
     */
    static class Constraint {

      private RexNode exprNode;
      private RexNode constNode;

      public Constraint(RexNode exprNode, RexNode constNode) {
        this.exprNode = constNode;
        this.constNode = exprNode;
      }

      /**
       * Interprets argument as a constraint; if not possible returns null.
       */
      public static Constraint of(RexNode n) {
        if (!(n instanceof RexCall)) {
          return null;
        }
        RexCall call = (RexCall) n;
        if (call.getOperator().getKind() != SqlKind.EQUALS) {
          return null;
        }
        RexNode opA = call.operands.get(0);
        RexNode opB = call.operands.get(1);
        if (RexUtil.isNull(opA) || RexUtil.isNull(opB)) {
          // dont try to compare nulls
          return null;
        }
        if (isConstExpr(opA) && isColumnExpr(opB)) {
          return new Constraint(opB, opA);
        }
        if (isColumnExpr(opA) && isConstExpr(opB)) {
          return new Constraint(opA, opB);
        }
        return null;
      }

      private static boolean isColumnExpr(RexNode node) {
        return !node.getType().isStruct() && HiveCalciteUtil.getInputRefs(node).size() > 0
            && HiveCalciteUtil.isDeterministic(node);
      }

      private static boolean isConstExpr(RexNode node) {
        return !node.getType().isStruct() && HiveCalciteUtil.getInputRefs(node).size() == 0
            && HiveCalciteUtil.isDeterministic(node);
      }

      public RexNodeRef getKey() {
        return new RexNodeRef(constNode);
      }

    }

    /**
     * A group of Constraints.
     *
     * Examples:
     *  (a=1 && b=1)
     *  (a=1)
     *
     * Note: any rexNode is accepted as constraint; but it might be keyed with the empty key;
     * which means it can't be parsed as a constraint for some reason; but for completeness...
     *
     */
    static class ConstraintGroup {
      public static final Function<ConstraintGroup, Set<RexNodeRef>> KEY_FUNCTION =
          new Function<ConstraintGroup, Set<RexNodeRef>>() {

            @Override
            public Set<RexNodeRef> apply(ConstraintGroup cg) {
              return cg.key;
            }
          };
      private Map<RexNodeRef, Constraint> constraints = new HashMap<>();
      private RexNode originalRexNode;
      private final Set<RexNodeRef> key;

      public ConstraintGroup(RexNode rexNode) {
        originalRexNode = rexNode;

        final List<RexNode> conjunctions = RelOptUtil.conjunctions(rexNode);

        for (RexNode n : conjunctions) {

          Constraint c = Constraint.of(n);
          if (c == null) {
            // interpretation failed; make this node opaque
            key = Collections.emptySet();
            return;
          }
          constraints.put(c.getKey(), c);
        }
        if (constraints.size() != conjunctions.size()) {
          LOG.debug("unexpected situation; giving up on this branch");
          key = Collections.emptySet();
          return;
        }
        key = constraints.keySet();
      }

      public List<RexNode> getValuesInOrder(List<RexNodeRef> columns) throws SemanticException {
        List<RexNode> ret = new ArrayList<>();
        for (RexNodeRef rexInputRef : columns) {
          Constraint constraint = constraints.get(rexInputRef);
          if (constraint == null) {
            throw new SemanticException("Unable to find constraint which was earlier added.");
          }
          ret.add(constraint.exprNode);
        }
        return ret;
      }
    }

    private RexNode transformIntoInClauseCondition(RexBuilder rexBuilder, RexNode condition,
            int minNumORClauses) throws SemanticException {
      assert condition.getKind() == SqlKind.OR;

      ImmutableList<RexNode> operands = RexUtil.flattenOr(((RexCall) condition).getOperands());
      if (operands.size() < minNumORClauses) {
        // We bail out
        return null;
      }
      List<ConstraintGroup> allNodes = new ArrayList<>();
      List<ConstraintGroup> processedNodes = new ArrayList<>();
      for (int i = 0; i < operands.size(); i++) {
        ConstraintGroup m = new ConstraintGroup(operands.get(i));
        allNodes.add(m);
      }

      Multimap<Set<RexNodeRef>, ConstraintGroup> assignmentGroups =
          Multimaps.index(allNodes, ConstraintGroup.KEY_FUNCTION);

      for (Entry<Set<RexNodeRef>, Collection<ConstraintGroup>> sa : assignmentGroups.asMap().entrySet()) {
        // skip opaque
        if (sa.getKey().size() == 0) {
          continue;
        }
        // not enough equalities should not be handled
        if (sa.getValue().size() < 2 || sa.getValue().size() < minNumORClauses) {
          continue;
        }

        allNodes.add(new ConstraintGroup(buildInFor(sa.getKey(), sa.getValue())));
        processedNodes.addAll(sa.getValue());
      }

      if (processedNodes.isEmpty()) {
        return null;
      }
      allNodes.removeAll(processedNodes);
      List<RexNode> ops = new ArrayList<>();
      for (ConstraintGroup mx : allNodes) {
        ops.add(mx.originalRexNode);
      }
      if (ops.size() == 1) {
        return ops.get(0);
      } else {
        return rexBuilder.makeCall(SqlStdOperatorTable.OR, ops);
      }

    }

    private RexNode buildInFor(Set<RexNodeRef> set, Collection<ConstraintGroup> value) throws SemanticException {

      List<RexNodeRef> columns = new ArrayList<>();
      columns.addAll(set);
      columns.sort(RexNodeRef.COMPARATOR);
      List<RexNode >operands = new ArrayList<>();

      List<RexNode> columnNodes = columns.stream().map(n -> n.getRexNode()).collect(Collectors.toList());
      operands.add(useStructIfNeeded(columnNodes));
      for (ConstraintGroup node : value) {
        List<RexNode> values = node.getValuesInOrder(columns);
        operands.add(useStructIfNeeded(values));
      }

      return rexBuilder.makeCall(HiveIn.INSTANCE, operands);
    }

    private RexNode useStructIfNeeded(List<? extends RexNode> columns) {
      // Create STRUCT clause
      if (columns.size() == 1) {
        return columns.get(0);
      } else {
        return rexBuilder.makeCall(SqlStdOperatorTable.ROW, columns);
      }
    }

  }

  /**
   * Merge IN clauses, when possible.
   */
  protected static class RexMergeInClause extends RexShuttle {
    private final RexBuilder rexBuilder;

    RexMergeInClause(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override public RexNode visitCall(RexCall call) {
      RexNode node;
      final List<RexNode> operands;
      final List<RexNode> newOperands;
      Map<String,RexNode> stringToExpr = Maps.newHashMap();
      Multimap<String,String> inLHSExprToRHSExprs = LinkedHashMultimap.create();
      switch (call.getKind()) {
        case AND:
          // IN clauses need to be combined by keeping only common elements
          operands = Lists.newArrayList(RexUtil.flattenAnd(call.getOperands()));
          for (int i = 0; i < operands.size(); i++) {
            RexNode operand = operands.get(i);
            if (operand.getKind() == SqlKind.IN) {
              RexCall inCall = (RexCall) operand;
              if (!HiveCalciteUtil.isDeterministic(inCall.getOperands().get(0))) {
                continue;
              }
              String ref = inCall.getOperands().get(0).toString();
              stringToExpr.put(ref, inCall.getOperands().get(0));
              if (inLHSExprToRHSExprs.containsKey(ref)) {
                Set<String> expressions = Sets.newHashSet();
                for (int j = 1; j < inCall.getOperands().size(); j++) {
                  String expr = inCall.getOperands().get(j).toString();
                  expressions.add(expr);
                  stringToExpr.put(expr, inCall.getOperands().get(j));
                }
                inLHSExprToRHSExprs.get(ref).retainAll(expressions);
              } else {
                for (int j = 1; j < inCall.getOperands().size(); j++) {
                  String expr = inCall.getOperands().get(j).toString();
                  inLHSExprToRHSExprs.put(ref, expr);
                  stringToExpr.put(expr, inCall.getOperands().get(j));
                }
              }
              operands.remove(i);
              --i;
            }
          }
          // Create IN clauses
          newOperands = createInClauses(rexBuilder, stringToExpr, inLHSExprToRHSExprs);
          newOperands.addAll(operands);
          // Return node
          node = RexUtil.composeConjunction(rexBuilder, newOperands, false);
          break;
        case OR:
          // IN clauses need to be combined by keeping all elements
          operands = Lists.newArrayList(RexUtil.flattenOr(call.getOperands()));
          for (int i = 0; i < operands.size(); i++) {
            RexNode operand = operands.get(i);
            if (operand.getKind() == SqlKind.IN) {
              RexCall inCall = (RexCall) operand;
              if (!HiveCalciteUtil.isDeterministic(inCall.getOperands().get(0))) {
                continue;
              }
              String ref = inCall.getOperands().get(0).toString();
              stringToExpr.put(ref, inCall.getOperands().get(0));
              for (int j = 1; j < inCall.getOperands().size(); j++) {
                String expr = inCall.getOperands().get(j).toString();
                inLHSExprToRHSExprs.put(ref, expr);
                stringToExpr.put(expr, inCall.getOperands().get(j));
              }
              operands.remove(i);
              --i;
            }
          }
          // Create IN clauses
          newOperands = createInClauses(rexBuilder, stringToExpr, inLHSExprToRHSExprs);
          newOperands.addAll(operands);
          // Return node
          node = RexUtil.composeDisjunction(rexBuilder, newOperands, false);
          break;
        default:
          return super.visitCall(call);
      }
      return node;
    }

    private static List<RexNode> createInClauses(RexBuilder rexBuilder, Map<String, RexNode> stringToExpr,
            Multimap<String, String> inLHSExprToRHSExprs) {
      List<RexNode> newExpressions = Lists.newArrayList();
      for (Entry<String,Collection<String>> entry : inLHSExprToRHSExprs.asMap().entrySet()) {
        String ref = entry.getKey();
        Collection<String> exprs = entry.getValue();
        if (exprs.isEmpty()) {
          newExpressions.add(rexBuilder.makeLiteral(false));
        } else {
          List<RexNode> newOperands = new ArrayList<RexNode>(exprs.size() + 1);
          newOperands.add(stringToExpr.get(ref));
          for (String expr : exprs) {
            newOperands.add(stringToExpr.get(expr));
          }
          newExpressions.add(rexBuilder.makeCall(HiveIn.INSTANCE, newOperands));
        }
      }
      return newExpressions;
    }

  }

}
