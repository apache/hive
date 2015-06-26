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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveMultiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Rule that merges a join with multijoin/join children if
 * the equi compared the same set of input columns.
 */
public class HiveJoinToMultiJoinRule extends RelOptRule {

  public static final HiveJoinToMultiJoinRule INSTANCE =
      new HiveJoinToMultiJoinRule(HiveJoin.class, HiveProject.DEFAULT_PROJECT_FACTORY);

  private final ProjectFactory projectFactory;


  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a JoinToMultiJoinRule.
   */
  public HiveJoinToMultiJoinRule(Class<? extends Join> clazz, ProjectFactory projectFactory) {
    super(operand(clazz,
            operand(RelNode.class, any()),
            operand(RelNode.class, any())));
    this.projectFactory = projectFactory;
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public void onMatch(RelOptRuleCall call) {
    final HiveJoin join = call.rel(0);
    final RelNode left = call.rel(1);
    final RelNode right = call.rel(2);

    // 1. We try to merge this join with the left child
    RelNode multiJoin = mergeJoin(join, left, right);
    if (multiJoin != null) {
      call.transformTo(multiJoin);
      return;
    }

    // 2. If we cannot, we swap the inputs so we can try
    //    to merge it with its right child
    RelNode swapped = JoinCommuteRule.swap(join, true);
    assert swapped != null;

    //    The result of the swapping operation is either
    //    i)  a Project or,
    //    ii) if the project is trivial, a raw join
    final Join newJoin;
    Project topProject = null;
    if (swapped instanceof Join) {
      newJoin = (Join) swapped;
    } else {
      topProject = (Project) swapped;
      newJoin = (Join) swapped.getInput(0);
    }

    // 3. We try to merge the join with the right child
    multiJoin = mergeJoin(newJoin, right, left);
    if (multiJoin != null) {
      if (topProject != null) {
        multiJoin = projectFactory.createProject(multiJoin,
                topProject.getChildExps(),
                topProject.getRowType().getFieldNames());
      }
      call.transformTo(multiJoin);
      return;
    }
  }

  // This method tries to merge the join with its left child. The left
  // child should be a join for this to happen.
  private static RelNode mergeJoin(Join join, RelNode left, RelNode right) {
    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();

    // We check whether the join can be combined with any of its children
    final List<RelNode> newInputs = Lists.newArrayList();
    final List<RexNode> newJoinFilters = Lists.newArrayList();
    newJoinFilters.add(join.getCondition());
    final List<Pair<Pair<Integer,Integer>, JoinRelType>> joinSpecs = Lists.newArrayList();

    // Left child
    if (left instanceof Join || left instanceof HiveMultiJoin) {
      final RexNode leftCondition;
      final List<Pair<Integer,Integer>> leftJoinInputs;
      final List<JoinRelType> leftJoinTypes;
      if (left instanceof Join) {
        Join hj = (Join) left;
        leftCondition = hj.getCondition();
        leftJoinInputs = ImmutableList.of(Pair.of(0, 1));
        leftJoinTypes = ImmutableList.of(hj.getJoinType());
      } else {
        HiveMultiJoin hmj = (HiveMultiJoin) left;
        leftCondition = hmj.getCondition();
        leftJoinInputs = hmj.getJoinInputs();
        leftJoinTypes = hmj.getJoinTypes();
      }

      boolean combinable = isCombinablePredicate(join, join.getCondition(),
              leftCondition);
      if (combinable) {
        newJoinFilters.add(leftCondition);
        for (int i = 0; i < leftJoinInputs.size(); i++) {
          joinSpecs.add(Pair.of(leftJoinInputs.get(i), leftJoinTypes.get(i)));
        }
        newInputs.addAll(left.getInputs());
      } else { // The join operation in the child is not on the same keys
        return null;
      }
    } else { // The left child is not a join or multijoin operator
      return null;
    }
    final int numberLeftInputs = newInputs.size();

    // Right child
    newInputs.add(right);

    // If we cannot combine any of the children, we bail out
    if (newJoinFilters.size() == 1) {
      return null;
    }

    final List<RelDataTypeField> systemFieldList = ImmutableList.of();
    List<List<RexNode>> joinKeyExprs = new ArrayList<List<RexNode>>();
    List<Integer> filterNulls = new ArrayList<Integer>();
    for (int i=0; i<newInputs.size(); i++) {
      joinKeyExprs.add(new ArrayList<RexNode>());
    }
    RexNode otherCondition = HiveRelOptUtil.splitJoinCondition(systemFieldList, newInputs, join.getCondition(),
        joinKeyExprs, filterNulls, null);
    // If there are remaining parts in the condition, we bail out
    if (!otherCondition.isAlwaysTrue()) {
      return null;
    }
    ImmutableBitSet.Builder keysInInputsBuilder = ImmutableBitSet.builder();
    for (int i=0; i<newInputs.size(); i++) {
      List<RexNode> partialCondition = joinKeyExprs.get(i);
      if (!partialCondition.isEmpty()) {
        keysInInputsBuilder.set(i);
      }
    }
    // If we cannot merge, we bail out
    ImmutableBitSet keysInInputs = keysInInputsBuilder.build();
    ImmutableBitSet leftReferencedInputs =
            keysInInputs.intersect(ImmutableBitSet.range(numberLeftInputs));
    ImmutableBitSet rightReferencedInputs =
            keysInInputs.intersect(ImmutableBitSet.range(numberLeftInputs, newInputs.size()));
    if (join.getJoinType() != JoinRelType.INNER &&
            (leftReferencedInputs.cardinality() > 1 || rightReferencedInputs.cardinality() > 1)) {
      return null;
    }
    // Otherwise, we add to the join specs
    if (join.getJoinType() != JoinRelType.INNER) {
      int leftInput = keysInInputs.nextSetBit(0);
      int rightInput = keysInInputs.nextSetBit(numberLeftInputs);
      joinSpecs.add(Pair.of(Pair.of(leftInput, rightInput), join.getJoinType()));
    } else {
      for (int i : leftReferencedInputs) {
        for (int j : rightReferencedInputs) {
          joinSpecs.add(Pair.of(Pair.of(i, j), join.getJoinType()));
        }
      }
    }

    // We can now create a multijoin operator
    RexNode newCondition = RexUtil.flatten(rexBuilder,
            RexUtil.composeConjunction(rexBuilder, newJoinFilters, false));
    return new HiveMultiJoin(
            join.getCluster(),
            newInputs,
            newCondition,
            join.getRowType(),
            Pair.left(joinSpecs),
            Pair.right(joinSpecs));
  }

  private static boolean isCombinablePredicate(Join join,
          RexNode condition, RexNode otherCondition) {
    final JoinPredicateInfo joinPredInfo = HiveCalciteUtil.JoinPredicateInfo.
            constructJoinPredicateInfo(join, condition);
    final JoinPredicateInfo otherJoinPredInfo = HiveCalciteUtil.JoinPredicateInfo.
            constructJoinPredicateInfo(join, otherCondition);
    if (joinPredInfo.getProjsFromLeftPartOfJoinKeysInJoinSchema().
            equals(otherJoinPredInfo.getProjsFromLeftPartOfJoinKeysInJoinSchema())) {
      return false;
    }
    if (joinPredInfo.getProjsFromRightPartOfJoinKeysInJoinSchema().
            equals(otherJoinPredInfo.getProjsFromRightPartOfJoinKeysInJoinSchema())) {
      return false;
    }
    return true;
  }

  /**
   * Shifts a filter originating from the right child of the LogicalJoin to the
   * right, to reflect the filter now being applied on the resulting
   * MultiJoin.
   *
   * @param joinRel     the original LogicalJoin
   * @param left        the left child of the LogicalJoin
   * @param right       the right child of the LogicalJoin
   * @param rightFilter the filter originating from the right child
   * @return the adjusted right filter
   */
  private static RexNode shiftRightFilter(
      Join joinRel,
      RelNode left,
      RelNode right,
      RexNode rightFilter) {
    if (rightFilter == null) {
      return null;
    }

    int nFieldsOnLeft = left.getRowType().getFieldList().size();
    int nFieldsOnRight = right.getRowType().getFieldList().size();
    int[] adjustments = new int[nFieldsOnRight];
    for (int i = 0; i < nFieldsOnRight; i++) {
      adjustments[i] = nFieldsOnLeft;
    }
    rightFilter =
        rightFilter.accept(
            new RelOptUtil.RexInputConverter(
                joinRel.getCluster().getRexBuilder(),
                right.getRowType().getFieldList(),
                joinRel.getRowType().getFieldList(),
                adjustments));
    return rightFilter;
  }

}
