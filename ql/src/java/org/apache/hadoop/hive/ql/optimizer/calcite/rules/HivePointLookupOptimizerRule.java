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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
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

public abstract class HivePointLookupOptimizerRule extends RelOptRule {

/**
 * This optimization will take a Filter or expression, and if its predicate contains
 * an OR operator whose children are constant equality expressions, it will try
 * to generate an IN clause (which is more efficient). If the OR operator contains
 * AND operator children, the optimization might generate an IN clause that uses
 * structs.
 */
  public static class FilterCondition extends HivePointLookupOptimizerRule {
    public FilterCondition (int minNumORClauses) {
      super(operand(Filter.class, any()), minNumORClauses);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Filter filter = call.rel(0);
      final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
      final RexNode condition = RexUtil.pullFactors(rexBuilder, filter.getCondition());
      analyzeCondition(call , rexBuilder, filter, condition);
    }

    @Override protected RelNode copyNode(AbstractRelNode node, RexNode newCondition) {
      final Filter filter  = (Filter) node;
      return filter.copy(filter.getTraitSet(), filter.getInput(), newCondition);
    }
  }

/**
 * This optimization will take a Join or expression, and if its join condition contains
 * an OR operator whose children are constant equality expressions, it will try
 * to generate an IN clause (which is more efficient). If the OR operator contains
 * AND operator children, the optimization might generate an IN clause that uses
 * structs.
 */
  public static class JoinCondition extends HivePointLookupOptimizerRule {
    public JoinCondition (int minNumORClauses) {
      super(operand(Join.class, any()), minNumORClauses);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Join join = call.rel(0);
      final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
      final RexNode condition = RexUtil.pullFactors(rexBuilder, join.getCondition());
      analyzeCondition(call , rexBuilder, join, condition);
    }

    @Override protected RelNode copyNode(AbstractRelNode node, RexNode newCondition) {
      final Join join = (Join) node;
      return join.copy(join.getTraitSet(),
              newCondition,
              join.getLeft(),
              join.getRight(),
              join.getJoinType(),
              join.isSemiJoinDone());
    }
  }

  protected static final Logger LOG = LoggerFactory.getLogger(HivePointLookupOptimizerRule.class);

  // Minimum number of OR clauses needed to transform into IN clauses
  protected final int minNumORClauses;

  protected abstract RelNode copyNode(AbstractRelNode node, RexNode newCondition);

  protected HivePointLookupOptimizerRule(
    RelOptRuleOperand operand, int minNumORClauses) {
    super(operand);
    this.minNumORClauses = minNumORClauses;
  }

  public void analyzeCondition(RelOptRuleCall call,
          RexBuilder rexBuilder,
          AbstractRelNode node,
          RexNode condition) {

    // 1. We try to transform possible candidates
    RexTransformIntoInClause transformIntoInClause = new RexTransformIntoInClause(rexBuilder, node,
            minNumORClauses);
    RexNode newCondition = transformIntoInClause.apply(condition);

    // 2. We merge IN expressions
    RexMergeInClause mergeInClause = new RexMergeInClause(rexBuilder);
    newCondition = mergeInClause.apply(newCondition);

    // 3. If we could not transform anything, we bail out
    if (newCondition.toString().equals(condition.toString())) {
      return;
    }

    // 4. We create the Filter/Join with the new condition
    RelNode newNode = copyNode(node, newCondition);

    call.transformTo(newNode);
  }


  /**
   * Transforms OR clauses into IN clauses, when possible.
   */
  protected static class RexTransformIntoInClause extends RexShuttle {
    private final RexBuilder rexBuilder;
    private final AbstractRelNode nodeOp;
    private final int minNumORClauses;

    RexTransformIntoInClause(RexBuilder rexBuilder, AbstractRelNode nodeOp, int minNumORClauses) {
      this.nodeOp = nodeOp;
      this.rexBuilder = rexBuilder;
      this.minNumORClauses = minNumORClauses;
    }

    @Override public RexNode visitCall(RexCall call) {
      RexNode node;
      switch (call.getKind()) {
      // FIXME: I don't think there is a need for this right now...calcite have already done the flattening/etc
      // removing this case clause will not miss the OR below AND
      case AND:
        ImmutableList<RexNode> operands = RexUtil.flattenAnd(call.getOperands());
        List<RexNode> newOperands = new ArrayList<RexNode>();
        for (RexNode operand : operands) {
          RexNode newOperand;
          if (operand.getKind() == SqlKind.OR) {
            try {
              newOperand = transformIntoInClauseCondition(rexBuilder,
                  nodeOp.getRowType(), operand, minNumORClauses);
              if (newOperand == null) {
                newOperand = operand;
              }
            } catch (SemanticException e) {
              LOG.error("Exception in HivePointLookupOptimizerRule", e);
              return call;
            }
          } else {
            newOperand = operand;
          }
          newOperands.add(newOperand);
        }
        node = RexUtil.composeConjunction(rexBuilder, newOperands, false);
        break;
        case OR:
          try {
            node = transformIntoInClauseCondition(rexBuilder,
                    nodeOp.getRowType(), call, minNumORClauses);
            if (node == null) {
              return super.visitCall(call);
            }
          } catch (SemanticException e) {
            LOG.error("Exception in HivePointLookupOptimizerRule", e);
            return call;
          }
          break;
        default:
          return super.visitCall(call);
      }
      return node;
    }

    /**
     * Represents a simple contraint.
     *
     * Example: a=1
     */
    static class Constraint {

      private RexLiteral literal;
      private RexInputRef inputRef;

      public Constraint(RexInputRef inputRef, RexLiteral literal) {
        this.literal = literal;
        this.inputRef = inputRef;
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
        if (opA instanceof RexLiteral && opB instanceof RexInputRef) {
          RexLiteral rexLiteral = (RexLiteral) opA;
          RexInputRef rexInputRef = (RexInputRef) opB;
          return new Constraint(rexInputRef, rexLiteral);
        }
        if (opA instanceof RexInputRef && opB instanceof RexLiteral) {
          RexLiteral rexLiteral = (RexLiteral) opB;
          RexInputRef rexInputRef = (RexInputRef) opA;
          return new Constraint(rexInputRef, rexLiteral);
        }
        return null;
      }

      public RexInputRef getKey() {
        return inputRef;
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

      public static final Function<ConstraintGroup, Set<RexInputRef>> KEY_FUNCTION = new Function<ConstraintGroup, Set<RexInputRef>>() {

        @Override
        public Set<RexInputRef> apply(ConstraintGroup a) {
          return a.key;
        }
      };
      private Map<RexInputRef, Constraint> constraints = new HashMap<>();
      private RexNode originalRexNode;
      private final Set<RexInputRef> key;

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

      public List<RexNode> getValuesInOrder(List<RexInputRef> columns) throws SemanticException {
        List<RexNode> ret = new ArrayList<>();
        for (RexInputRef rexInputRef : columns) {
          Constraint constraint = constraints.get(rexInputRef);
          if (constraint == null) {
            throw new SemanticException("Unable to find constraint which was earlier added.");
          }
          ret.add(constraint.literal);
        }
        return ret;
      }
    }

    private RexNode transformIntoInClauseCondition(RexBuilder rexBuilder, RelDataType inputSchema,
            RexNode condition, int minNumORClauses) throws SemanticException {
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

      Multimap<Set<RexInputRef>, ConstraintGroup> assignmentGroups =
          Multimaps.index(allNodes, ConstraintGroup.KEY_FUNCTION);

      for (Entry<Set<RexInputRef>, Collection<ConstraintGroup>> sa : assignmentGroups.asMap().entrySet()) {
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

    private RexNode buildInFor(Set<RexInputRef> set, Collection<ConstraintGroup> value) throws SemanticException {

      List<RexInputRef> columns = new ArrayList<RexInputRef>();
      columns.addAll(set);
      List<RexNode >operands = new ArrayList<>();

      operands.add(useStructIfNeeded(columns));
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
