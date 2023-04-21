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
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveBetween;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveIn;
import org.apache.hadoop.hive.ql.optimizer.graph.DiGraph;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

/**
 * This optimization attempts to identify and close expanded INs and BETWEENs
 *
 * Basically:
 * <pre>
 * (c) IN ( v1, v2, ...) &lt;=&gt; c1=v1 || c1=v2 || ...
 * </pre>
 * If c is struct; then c=v1 is a group of anded equations.
 *
 * Similarly
 * <pre>
 * v1 &lt;= c1 and c1 &lt;= v2
 * </pre>
 * is rewritten to <p>c1 between v1 and v2</p>
 */
public abstract class HivePointLookupOptimizerRule extends RelOptRule {

  /** Rule adapter to apply the transformation to Filter conditions. */
  public static class FilterCondition extends HivePointLookupOptimizerRule {
    public FilterCondition (int minNumORClauses) {
      super(operand(Filter.class, any()), minNumORClauses,
          "HivePointLookupOptimizerRule(FilterCondition)");
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
      super(operand(Join.class, any()), minNumORClauses,
          "HivePointLookupOptimizerRule(JoinCondition)");
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
      super(operand(Project.class, any()), minNumORClauses,
          "HivePointLookupOptimizerRule(ProjectionExpressions)");
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
          project.getRowType());
      call.transformTo(newProject);
    }

  }

  protected static final Logger LOG = LoggerFactory.getLogger(HivePointLookupOptimizerRule.class);

  // Minimum number of OR clauses needed to transform into IN clauses
  protected final int minNumORClauses;

  protected HivePointLookupOptimizerRule(
      RelOptRuleOperand operand, int minNumORClauses, String description) {
    super(operand, description);
    this.minNumORClauses = minNumORClauses;
  }

  public RexNode analyzeRexNode(RexBuilder rexBuilder, RexNode condition) {
    // 1. We try to transform possible candidates
    RexTransformIntoInClause transformIntoInClause = new RexTransformIntoInClause(rexBuilder, minNumORClauses);
    RexNode newCondition = transformIntoInClause.apply(condition);

    // 2. We merge IN expressions
    RexMergeInClause mergeInClause = new RexMergeInClause(rexBuilder);
    newCondition = mergeInClause.apply(newCondition);

    // 3. Close BETWEEN expressions if possible
    RexTransformIntoBetween t = new RexTransformIntoBetween(rexBuilder);
    newCondition = t.apply(newCondition);
    return newCondition;
  }

  /**
   * Transforms inequality candidates into [NOT] BETWEEN calls.
   *
   */
  protected static class RexTransformIntoBetween extends RexShuttle {
    private final RexBuilder rexBuilder;

    RexTransformIntoBetween(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall inputCall) {
      RexNode node = super.visitCall(inputCall);
      if (node instanceof RexCall) {
        RexCall call = (RexCall) node;
        switch (call.getKind()) {
        case AND:
          return processComparisons(call, SqlKind.LESS_THAN_OR_EQUAL, false);
        case OR:
          return processComparisons(call, SqlKind.GREATER_THAN, true);
        default:
          break;
        }
      }
      return node;
    }

    /**
     * Represents a replacement candidate.
     */
    static class BetweenCandidate {

      private final RexNode newNode;
      private final RexNode[] oldNodes;
      // keeps track if this candidate was already used during replacement
      private boolean used;

      public BetweenCandidate(RexNode newNode, RexNode... oldNodes) {
        this.newNode = newNode;
        this.oldNodes = oldNodes;
      }
    }

    private RexNode processComparisons(RexCall call, SqlKind forwardEdge, boolean invert) {
      DiGraph<RexNodeRef, RexCall> g =
          buildComparisonGraph(call.getOperands(), forwardEdge);
      Map<RexNode, BetweenCandidate> replacedNodes = new IdentityHashMap<>();
      for (RexNodeRef n : g.nodes()) {
        Set<RexNodeRef> pred = g.predecessors(n);
        Set<RexNodeRef> succ = g.successors(n);
        if (pred.size() > 0 && succ.size() > 0) {
          RexNodeRef p = pred.iterator().next();
          RexNodeRef s = succ.iterator().next();

          RexNode between = rexBuilder.makeCall(HiveBetween.INSTANCE,
              rexBuilder.makeLiteral(invert), n.node, p.node, s.node);
          BetweenCandidate bc = new BetweenCandidate(
              between,
              g.removeEdge(p, n),
              g.removeEdge(n, s));

          for (RexNode node : bc.oldNodes) {
            replacedNodes.put(node, bc);
          }
        }
      }
      if (replacedNodes.isEmpty()) {
        // no effect
        return call;
      }
      List<RexNode> newOperands = new ArrayList<>();
      for (RexNode o : call.getOperands()) {
        BetweenCandidate candidate = replacedNodes.get(o);
        if (candidate == null) {
          newOperands.add(o);
        } else {
          if (!candidate.used) {
            newOperands.add(candidate.newNode);
            candidate.used = true;
          }
        }
      }

      if (newOperands.size() == 1) {
        return newOperands.get(0);
      } else {
        return rexBuilder.makeCall(call.getOperator(), newOperands);
      }
    }

    /**
     * Builds a graph of the given comparison type.
     *
     * The graph edges are annotated with the RexNodes representing the comparison.
     */
    private DiGraph<RexNodeRef, RexCall> buildComparisonGraph(List<RexNode> operands, SqlKind cmpForward) {
      DiGraph<RexNodeRef, RexCall> g = new DiGraph<>();
      for (RexNode node : operands) {
        if(!(node instanceof RexCall) ) {
          continue;
        }
        RexCall rexCall = (RexCall) node;
        SqlKind kind = rexCall.getKind();
        if (kind == cmpForward) {
          RexNode opA = rexCall.getOperands().get(0);
          RexNode opB = rexCall.getOperands().get(1);
          g.putEdgeValue(new RexNodeRef(opA), new RexNodeRef(opB), rexCall);
        } else if (kind == cmpForward.reverse()) {
          RexNode opA = rexCall.getOperands().get(1);
          RexNode opB = rexCall.getOperands().get(0);
          g.putEdgeValue(new RexNodeRef(opA), new RexNodeRef(opB), rexCall);
        }
      }
      return g;
    }
  }

  /**
   * This class just wraps around a RexNode enables equals/hashCode based on toString.
   *
   * After CALCITE-2632 this might not be needed anymore */
  static class RexNodeRef {

    public static Comparator<RexNodeRef> COMPARATOR =
        Comparator.comparing((RexNodeRef o) -> o.node.toString());
    private final RexNode node;

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
   * Represents a constraint.
   *
   * Example: a=1
   * substr(a,1,2) = concat('asd','xxx')
   */
  static class Constraint {

    private final RexNode exprNode;
    private final RexNode constNode;

    public Constraint(RexNode exprNode, RexNode constNode) {
      this.exprNode = exprNode;
      this.constNode = constNode;
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
      return !node.getType().isStruct() && !HiveCalciteUtil.getInputRefs(node).isEmpty()
          && HiveCalciteUtil.isDeterministic(node);
    }

    private static boolean isConstExpr(RexNode node) {
      return !node.getType().isStruct() && HiveCalciteUtil.getInputRefs(node).isEmpty()
          && HiveCalciteUtil.isDeterministic(node);
    }

    public RexNodeRef getKey() {
      return new RexNodeRef(exprNode);
    }

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
        if (call.getKind() == SqlKind.OR) {
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
        }
      }
      return node;
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
      private final Map<RexNodeRef, Constraint> constraints = new HashMap<>();
      private final RexNode originalRexNode;
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
          ret.add(constraint.constNode);
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
          Multimaps.index(allNodes, cg -> cg.key);

      for (Entry<Set<RexNodeRef>, Collection<ConstraintGroup>> sa : assignmentGroups.asMap().entrySet()) {
        // skip opaque
        if (sa.getKey().isEmpty()) {
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

      List<RexNodeRef> columns = new ArrayList<>(set);
      columns.sort(RexNodeRef.COMPARATOR);
      List<RexNode >operands = new ArrayList<>();

      List<RexNode> columnNodes = columns.stream().map(RexNodeRef::getRexNode).collect(Collectors.toList());
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
      switch (call.getKind()) {
        case AND:
          return handleAND(rexBuilder, call);
        case OR:
          return handleOR(rexBuilder, call);
        default:
          return super.visitCall(call);
      }
    }

    private static RexNode handleAND(RexBuilder rexBuilder, RexCall call) {
      // Visited nodes
      final Set<RexNode> visitedRefs = new LinkedHashSet<>();
      // IN clauses need to be combined by keeping only common elements
      final Multimap<RexNode,SimilarRexNodeElement> inLHSExprToRHSExprs = LinkedHashMultimap.create();
      // We will use this set to keep those expressions that may evaluate
      // into a null value.
      final Multimap<RexNode,RexNode> inLHSExprToRHSNullableExprs = LinkedHashMultimap.create();
      final List<RexNode> operands = new ArrayList<>(RexUtil.flattenAnd(call.getOperands()));

      for (int i = 0; i < operands.size(); i++) {
        RexNode operand = operands.get(i);
        if (operand.getKind() == SqlKind.IN) {
          RexCall inCall = (RexCall) operand;
          if (!HiveCalciteUtil.isDeterministic(inCall.getOperands().get(0))) {
            continue;
          }
          RexNode ref = inCall.getOperands().get(0);
          visitedRefs.add(ref);
          if (ref.getType().isNullable()) {
            inLHSExprToRHSNullableExprs.put(ref, ref);
          }
          if (inLHSExprToRHSExprs.containsKey(ref)) {
            Set<SimilarRexNodeElement> expressions = Sets.newHashSet();
            for (int j = 1; j < inCall.getOperands().size(); j++) {
              RexNode constNode = inCall.getOperands().get(j);
              expressions.add(new SimilarRexNodeElement(constNode));
              if (constNode.getType().isNullable()) {
                inLHSExprToRHSNullableExprs.put(ref, constNode);
              }
            }
            Collection<SimilarRexNodeElement> knownConstants = inLHSExprToRHSExprs.get(ref);
            if (!shareSameType(knownConstants, expressions)) {
              return call;
            }
            knownConstants.retainAll(expressions);
          } else {
            for (int j = 1; j < inCall.getOperands().size(); j++) {
              RexNode constNode = inCall.getOperands().get(j);
              inLHSExprToRHSExprs.put(ref, new SimilarRexNodeElement(constNode));
              if (constNode.getType().isNullable()) {
                inLHSExprToRHSNullableExprs.put(ref, constNode);
              }
            }
          }
          operands.remove(i);
          --i;
        } else if (operand.getKind() == SqlKind.EQUALS) {
          Constraint c = Constraint.of(operand);
          if (c == null || !HiveCalciteUtil.isDeterministic(c.exprNode)) {
            continue;
          }
          visitedRefs.add(c.exprNode);
          if (c.exprNode.getType().isNullable()) {
            inLHSExprToRHSNullableExprs.put(c.exprNode, c.exprNode);
          }
          if (c.constNode.getType().isNullable()) {
            inLHSExprToRHSNullableExprs.put(c.exprNode, c.constNode);
          }
          if (inLHSExprToRHSExprs.containsKey(c.exprNode)) {
            Collection<SimilarRexNodeElement> knownConstants = inLHSExprToRHSExprs.get(c.exprNode);
            Collection<SimilarRexNodeElement> nextConstant = Collections.singleton(new SimilarRexNodeElement(c.constNode));
            if (!shareSameType(knownConstants, nextConstant)) {
              return call;
            }
            knownConstants.retainAll(nextConstant);
          } else {
            inLHSExprToRHSExprs.put(c.exprNode, new SimilarRexNodeElement(c.constNode));
          }
          operands.remove(i);
          --i;
        }
      }
      // Create IN clauses
      final List<RexNode> newOperands = createInClauses(rexBuilder,
          visitedRefs, inLHSExprToRHSExprs, inLHSExprToRHSNullableExprs);
      newOperands.addAll(operands);
      // Return node
      return RexUtil.composeConjunction(rexBuilder, newOperands, false);
    }

    protected static class SimilarRexNodeElement {
      private final RexNode rexNode;

      protected SimilarRexNodeElement(RexNode rexNode) {
        this.rexNode = rexNode;
      }

      public RexNode getRexNode() {
        return rexNode;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimilarRexNodeElement that = (SimilarRexNodeElement) o;
        return equalsWithSimilarType(rexNode, that.rexNode);
      }

      private static boolean equalsWithSimilarType(RexNode rexNode1, RexNode rexNode2) {
        if (!(rexNode1 instanceof RexLiteral) || !(rexNode2 instanceof RexLiteral)) {
          return rexNode1.equals(rexNode2);
        }

        RexLiteral rexLiteral1 = (RexLiteral) rexNode1;
        RexLiteral rexLiteral2 = (RexLiteral) rexNode2;

        if (rexLiteral1.getValue() == null && rexLiteral2.getValue() == null) {
          return true;
        }

        return rexLiteral1.getValue() != null && rexLiteral1.getValue().compareTo(rexLiteral2.getValue()) == 0 &&
                rexLiteral1.getType().getSqlTypeName().equals(rexLiteral2.getType().getSqlTypeName());
      }

      @Override
      public int hashCode() {
        if (rexNode instanceof RexLiteral) {
          RexLiteral rexLiteral = (RexLiteral) rexNode;
          return Objects.hash(rexLiteral.getValue(), rexLiteral.getType().getSqlTypeName());
        }
        return Objects.hash(rexNode);
      }
    }

    /**
     * Check if the type of nodes in the two collections is homogeneous within the collections
     * and identical between them.
     * @param nodes1 the first collection of nodes
     * @param nodes2 the second collection of nodes
     * @return true if nodes in both collections is unique and identical, false otherwise
     */
    private static boolean shareSameType(
            Collection<SimilarRexNodeElement> nodes1, Collection<SimilarRexNodeElement> nodes2) {
      return Stream.of(nodes1, nodes2).flatMap(Collection::stream)
          .map(n -> n.getRexNode().getType().getSqlTypeName())
          .distinct()
          .count() == 1;
    }

    private static RexNode handleOR(RexBuilder rexBuilder, RexCall call) {
      // IN clauses need to be combined by keeping all elements
      final List<RexNode> operands = new ArrayList<>(RexUtil.flattenOr(call.getOperands()));
      final Multimap<RexNode,SimilarRexNodeElement> inLHSExprToRHSExprs = LinkedHashMultimap.create();
      for (int i = 0; i < operands.size(); i++) {
        RexNode operand = operands.get(i);
        if (operand.getKind() == SqlKind.IN) {
          RexCall inCall = (RexCall) operand;
          if (!HiveCalciteUtil.isDeterministic(inCall.getOperands().get(0))) {
            continue;
          }
          RexNode ref = inCall.getOperands().get(0);
          for (int j = 1; j < inCall.getOperands().size(); j++) {
            inLHSExprToRHSExprs.put(ref, new SimilarRexNodeElement(inCall.getOperands().get(j)));
          }
          operands.remove(i);
          --i;
        }
      }
      // Create IN clauses (fourth parameter is not needed since no expressions were removed)
      final List<RexNode> newOperands = createInClauses(rexBuilder,
          inLHSExprToRHSExprs.keySet(), inLHSExprToRHSExprs, null);
      newOperands.addAll(operands);
      // Return node
      RexNode result = RexUtil.composeDisjunction(rexBuilder, newOperands, false);
      if (!result.getType().equals(call.getType())) {
        return rexBuilder.makeCast(call.getType(), result, true);
      }
      return result;
    }

    private static RexNode createResultFromEmptySet(RexBuilder rexBuilder,
        RexNode ref, Multimap<RexNode, RexNode> inLHSExprToRHSNullableExprs) {
      if (inLHSExprToRHSNullableExprs.containsKey(ref)) {
        // We handle possible null values in the expressions.
        List<RexNode> nullableExprs =
            inLHSExprToRHSNullableExprs.get(ref)
                .stream()
                .map(n -> rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, ImmutableList.of(n)))
                .collect(Collectors.toList());
        return RexUtil.composeConjunction(rexBuilder,
            ImmutableList.of(
                RexUtil.composeDisjunction(rexBuilder, nullableExprs, false),
                rexBuilder.makeNullLiteral(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN))),
            false);
      }
      return rexBuilder.makeLiteral(false);
    }

    private static List<RexNode> createInClauses(RexBuilder rexBuilder, Set<RexNode> visitedRefs,
        Multimap<RexNode, SimilarRexNodeElement> inLHSExprToRHSExprs, Multimap<RexNode,RexNode> inLHSExprToRHSNullableExprs) {
      final List<RexNode> newExpressions = new ArrayList<>();
      for (RexNode ref : visitedRefs) {
        Collection<SimilarRexNodeElement> exprs = inLHSExprToRHSExprs.get(ref);
        if (exprs.isEmpty()) {
          // Note that Multimap does not keep a key if all its values are removed.
          newExpressions.add(createResultFromEmptySet(rexBuilder, ref, inLHSExprToRHSNullableExprs));
        } else if (exprs.size() == 1) {
          List<RexNode> newOperands = new ArrayList<>(2);
          newOperands.add(ref);
          newOperands.add(exprs.iterator().next().getRexNode());
          newExpressions.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, newOperands));
        } else {
          List<RexNode> newOperands = new ArrayList<>(exprs.size() + 1);
          newOperands.add(ref);
          newOperands.addAll(exprs.stream().map(SimilarRexNodeElement::getRexNode).collect(Collectors.toList()));
          newExpressions.add(rexBuilder.makeCall(HiveIn.INSTANCE, newOperands));
        }
      }
      return newExpressions;
    }

  }

}
