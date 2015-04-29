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

import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinPredicateInfo;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Rule that merges a join with multijoin/join children if
 * the equi compared the same set of input columns.
 */
public class HiveJoinToMultiJoinRule extends RelOptRule {

  public static final HiveJoinToMultiJoinRule INSTANCE =
      new HiveJoinToMultiJoinRule(Join.class);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a JoinToMultiJoinRule.
   */
  public HiveJoinToMultiJoinRule(Class<? extends Join> clazz) {
    super(
            operand(clazz,
                operand(RelNode.class, any()),
                operand(RelNode.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Join join = call.rel(0);
    final RelNode left = call.rel(1);
    final RelNode right = call.rel(2);

    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();

    // We do not merge outer joins currently
    if (join.getJoinType() != JoinRelType.INNER) {
      return;
    }

    // We check whether the join can be combined with any of its children
    final List<RelNode> newInputs = Lists.newArrayList();
    final List<RexNode> newJoinFilters = Lists.newArrayList();
    newJoinFilters.add(join.getCondition());
    final List<Pair<JoinRelType, RexNode>> joinSpecs = Lists.newArrayList();
    final List<ImmutableBitSet> projFields = Lists.newArrayList();

    // Left child
    if (left instanceof Join || left instanceof MultiJoin) {
      final RexNode leftCondition;
      if (left instanceof Join) {
        leftCondition = ((Join) left).getCondition();
      } else {
        leftCondition = ((MultiJoin) left).getJoinFilter();
      }

      boolean combinable = isCombinablePredicate(join, join.getCondition(),
              leftCondition);
      if (combinable) {
        newJoinFilters.add(leftCondition);
        for (RelNode input : left.getInputs()) {
          projFields.add(null);
          joinSpecs.add(Pair.of(JoinRelType.INNER, (RexNode) null));
          newInputs.add(input);
        }
      } else {
        projFields.add(null);
        joinSpecs.add(Pair.of(JoinRelType.INNER, (RexNode) null));
        newInputs.add(left);
      }
    } else {
      projFields.add(null);
      joinSpecs.add(Pair.of(JoinRelType.INNER, (RexNode) null));
      newInputs.add(left);
    }

    // Right child
    if (right instanceof Join || right instanceof MultiJoin) {
      final RexNode rightCondition;
      if (right instanceof Join) {
        rightCondition = shiftRightFilter(join, left, right,
                ((Join) right).getCondition());
      } else {
        rightCondition = shiftRightFilter(join, left, right,
                ((MultiJoin) right).getJoinFilter());
      }
      
      boolean combinable = isCombinablePredicate(join, join.getCondition(),
              rightCondition);
      if (combinable) {
        newJoinFilters.add(rightCondition);
        for (RelNode input : right.getInputs()) {
          projFields.add(null);
          joinSpecs.add(Pair.of(JoinRelType.INNER, (RexNode) null));
          newInputs.add(input);
        }
      } else {
        projFields.add(null);
        joinSpecs.add(Pair.of(JoinRelType.INNER, (RexNode) null));
        newInputs.add(right);
      }
    } else {
      projFields.add(null);
      joinSpecs.add(Pair.of(JoinRelType.INNER, (RexNode) null));
      newInputs.add(right);
    }

    // If we cannot combine any of the children, we bail out
    if (newJoinFilters.size() == 1) {
      return;
    }

    RexNode newCondition = RexUtil.flatten(rexBuilder,
            RexUtil.composeConjunction(rexBuilder, newJoinFilters, false));
    final ImmutableMap<Integer, ImmutableIntList> newJoinFieldRefCountsMap =
        addOnJoinFieldRefCounts(newInputs,
            join.getRowType().getFieldCount(),
            newCondition);

    List<RexNode> newPostJoinFilters = combinePostJoinFilters(join, left, right);

    RelNode multiJoin =
        new MultiJoin(
            join.getCluster(),
            newInputs,
            newCondition,
            join.getRowType(),
            false,
            Pair.right(joinSpecs),
            Pair.left(joinSpecs),
            projFields,
            newJoinFieldRefCountsMap,
            RexUtil.composeConjunction(rexBuilder, newPostJoinFilters, true));

    call.transformTo(multiJoin);
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
  private RexNode shiftRightFilter(
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

  /**
   * Adds on to the existing join condition reference counts the references
   * from the new join condition.
   *
   * @param multiJoinInputs          inputs into the new MultiJoin
   * @param nTotalFields             total number of fields in the MultiJoin
   * @param joinCondition            the new join condition
   * @param origJoinFieldRefCounts   existing join condition reference counts
   *
   * @return Map containing the new join condition
   */
  private ImmutableMap<Integer, ImmutableIntList> addOnJoinFieldRefCounts(
      List<RelNode> multiJoinInputs,
      int nTotalFields,
      RexNode joinCondition) {
    // count the input references in the join condition
    int[] joinCondRefCounts = new int[nTotalFields];
    joinCondition.accept(new InputReferenceCounter(joinCondRefCounts));

    // add on to the counts for each input into the MultiJoin the
    // reference counts computed for the current join condition
    final Map<Integer, int[]> refCountsMap = Maps.newHashMap();
    int nInputs = multiJoinInputs.size();
    int currInput = -1;
    int startField = 0;
    int nFields = 0;
    for (int i = 0; i < nTotalFields; i++) {
      if (joinCondRefCounts[i] == 0) {
        continue;
      }
      while (i >= (startField + nFields)) {
        startField += nFields;
        currInput++;
        assert currInput < nInputs;
        nFields =
            multiJoinInputs.get(currInput).getRowType().getFieldCount();
      }
      int[] refCounts = refCountsMap.get(currInput);
      if (refCounts == null) {
        refCounts = new int[nFields];
        refCountsMap.put(currInput, refCounts);
      }
      refCounts[i - startField] += joinCondRefCounts[i];
    }

    final ImmutableMap.Builder<Integer, ImmutableIntList> builder =
        ImmutableMap.builder();
    for (Map.Entry<Integer, int[]> entry : refCountsMap.entrySet()) {
      builder.put(entry.getKey(), ImmutableIntList.of(entry.getValue()));
    }
    return builder.build();
  }

  /**
   * Combines the post-join filters from the left and right inputs (if they
   * are MultiJoinRels) into a single AND'd filter.
   *
   * @param joinRel the original LogicalJoin
   * @param left    left child of the LogicalJoin
   * @param right   right child of the LogicalJoin
   * @return combined post-join filters AND'd together
   */
  private List<RexNode> combinePostJoinFilters(
      Join joinRel,
      RelNode left,
      RelNode right) {
    final List<RexNode> filters = Lists.newArrayList();
    if (right instanceof MultiJoin) {
      final MultiJoin multiRight = (MultiJoin) right;
      filters.add(
          shiftRightFilter(joinRel, left, multiRight,
              multiRight.getPostJoinFilter()));
    }

    if (left instanceof MultiJoin) {
      filters.add(((MultiJoin) left).getPostJoinFilter());
    }

    return filters;
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Visitor that keeps a reference count of the inputs used by an expression.
   */
  private class InputReferenceCounter extends RexVisitorImpl<Void> {
    private final int[] refCounts;

    public InputReferenceCounter(int[] refCounts) {
      super(true);
      this.refCounts = refCounts;
    }

    public Void visitInputRef(RexInputRef inputRef) {
      refCounts[inputRef.getIndex()]++;
      return null;
    }
  }
}

// End JoinToMultiJoinRule.java

