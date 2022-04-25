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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hive.common.util.AnnotationUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Planner rule that infers predicates from on a
 * {@link org.apache.calcite.rel.core.Join} and creates
 * {@link org.apache.calcite.rel.core.Filter}s if those predicates can be pushed
 * to its inputs.
 *
 * <p>Uses {@link org.apache.calcite.rel.metadata.RelMdPredicates} to infer
 * the predicates,
 * returns them in a {@link org.apache.calcite.plan.RelOptPredicateList}
 * and applies them appropriately.
 */
public class HiveJoinPushTransitivePredicatesRule extends RelOptRule {

  private final boolean allowDisjunctivePredicates;

  public HiveJoinPushTransitivePredicatesRule(Class<? extends Join> clazz, boolean allowDisjunctivePredicates) {
    super(operand(clazz, any()));
    this.allowDisjunctivePredicates = allowDisjunctivePredicates;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);

    RelOptPredicateList preds = call.getMetadataQuery().getPulledUpPredicates(join);

    HiveRulesRegistry registry = call.getPlanner().getContext().unwrap(HiveRulesRegistry.class);
    assert registry != null;
    RexBuilder rB = join.getCluster().getRexBuilder();
    RelNode lChild = join.getLeft();
    RelNode rChild = join.getRight();

    Set<String> leftPushedPredicates = Sets.newHashSet(registry.getPushedPredicates(join, 0));
    List<RexNode> leftPreds =
        getValidPreds(lChild, leftPushedPredicates, preds.leftInferredPredicates, lChild.getRowType());
    Set<String> rightPushedPredicates = Sets.newHashSet(registry.getPushedPredicates(join, 1));
    List<RexNode> rightPreds =
        getValidPreds(rChild, rightPushedPredicates, preds.rightInferredPredicates, rChild.getRowType());

    RexNode newLeftPredicate = RexUtil.composeConjunction(rB, leftPreds, false);
    RexNode newRightPredicate = RexUtil.composeConjunction(rB, rightPreds, false);
    if (newLeftPredicate.isAlwaysTrue() && newRightPredicate.isAlwaysTrue()) {
      return;
    }

    if (!newLeftPredicate.isAlwaysTrue()) {
      RelNode curr = lChild;
      lChild = HiveRelFactories.HIVE_FILTER_FACTORY.createFilter(
          lChild, newLeftPredicate.accept(new RexReplacer(lChild)), ImmutableSet.of());
      call.getPlanner().onCopy(curr, lChild);
    }

    if (!newRightPredicate.isAlwaysTrue()) {
      RelNode curr = rChild;
      rChild = HiveRelFactories.HIVE_FILTER_FACTORY.createFilter(
          rChild, newRightPredicate.accept(new RexReplacer(rChild)), ImmutableSet.of());
      call.getPlanner().onCopy(curr, rChild);
    }

    RelNode newRel = join.copy(join.getTraitSet(), join.getCondition(),
        lChild, rChild, join.getJoinType(), join.isSemiJoinDone());
    call.getPlanner().onCopy(join, newRel);

    // Register information about pushed predicates
    registry.getPushedPredicates(newRel, 0).addAll(leftPushedPredicates);
    registry.getPushedPredicates(newRel, 1).addAll(rightPushedPredicates);

    call.transformTo(newRel);
  }

  private ImmutableList<RexNode> getValidPreds(RelNode child, Set<String> predicatesToExclude,
      List<RexNode> rexs, RelDataType rType) {
    InputRefValidator validator = new InputRefValidator(rType.getFieldList());
    List<RexNode> valids = new ArrayList<>(rexs.size());
    for (RexNode rex : rexs) {
      try {
        rex.accept(validator);
        valids.add(rex);
      } catch (Util.FoundOne e) {
        Util.swallow(e, null);
      }
    }

    // We need to filter:
    //  i) those that have been pushed already as stored in the join,
    //  ii) those that were already in the subtree rooted at child.
    List<RexNode> toPush = HiveCalciteUtil.getPredsNotPushedAlready(predicatesToExclude, child, valids);

    // Disjunctive predicates, when merged with other existing predicates, might become redundant but RexSimplify still
    // cannot simplify them. This situation generally leads to OOM, since these new predicates keep getting inferred
    // between the LHS and the RHS recursively, they grow by getting merged with existing predicates, but they can
    // never be simplified by RexSimplify, in this way the fix-point is never reached.
    // This restriction can be lifted if RexSimplify gets more powerful, and it can handle such cases.
    if (!allowDisjunctivePredicates) {
      toPush = toPush.stream()
          .filter(e -> !HiveCalciteUtil.hasDisjuction(e))
          .collect(Collectors.toList());
    }

    return ImmutableList.copyOf(toPush);
  }

  //~ Inner Classes ----------------------------------------------------------

  private static class InputRefValidator extends RexVisitorImpl<Void> {

    private final List<RelDataTypeField> types;
    protected InputRefValidator(List<RelDataTypeField> types) {
      super(true);
      this.types = types;
    }

    @Override
    public Void visitCall(RexCall call) {

      if(AnnotationUtils.getAnnotation(
          GenericUDFOPNotNull.class, Description.class).name().equals(call.getOperator().getName())) {
        if(call.getOperands().get(0) instanceof RexInputRef &&
            !types.get(((RexInputRef)call.getOperands().get(0)).getIndex()).getType().isNullable()) {
          // No need to add not null filter for a constant.
          throw new Util.FoundOne(call);
        }
      }
      return super.visitCall(call);
    }

    @Override
    public Void visitInputRef(RexInputRef inputRef) {
      if (!areTypesCompatible(inputRef.getType(), types.get(inputRef.getIndex()).getType())) {
        throw new Util.FoundOne(inputRef);
      }
      return super.visitInputRef(inputRef);
    }

    private boolean areTypesCompatible(RelDataType type1, RelDataType type2) {
      if (type1.equals(type2)) {
        return true;
      }
      SqlTypeName sqlType1 = type1.getSqlTypeName();
      if (sqlType1 != null) {
        return sqlType1.equals(type2.getSqlTypeName());
      }
      return false;
    }
  }

  /* Changes the type of the input references to adjust nullability */
  private static class RexReplacer extends RexShuttle {
    private final RelNode input;

    RexReplacer(RelNode input) {
      this.input = input;
    }

    @Override public RexNode visitInputRef(RexInputRef inputRef) {
      return RexInputRef.of(inputRef.getIndex(), input.getRowType());
    }
  }
}
