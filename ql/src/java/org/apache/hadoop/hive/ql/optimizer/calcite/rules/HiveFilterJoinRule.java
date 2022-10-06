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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.plan.RelOptUtil.RexInputConverter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.ql.optimizer.calcite.Bug;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil.RewritablePKFKJoinInfo;

import static org.apache.calcite.plan.RelOptUtil.conjunctions;

public abstract class HiveFilterJoinRule extends FilterJoinRule {

  public static final HiveFilterJoinRule FILTER_ON_NON_FILTERING_JOIN =
      new HiveFilterNonFilteringJoinMergeRule();

  public static final HiveFilterJoinRule FILTER_ON_JOIN =
      new HiveFilterJoinMergeRule();

  public static final HiveFilterJoinRule JOIN =
      new HiveFilterJoinTransposeRule();

  /**
   * Creates a PushFilterPastJoinRule with an explicit root operand.
   */
  protected HiveFilterJoinRule(RelOptRuleOperand operand, String id, boolean smart,
      RelBuilderFactory relBuilderFactory) {

    super(
      (Config) RelRule.Config.EMPTY
      .withDescription("HiveFilterJoinRule(" + id + ")")
      .withOperandSupplier(b0 ->
        b0.exactly(operand))
      .as(FilterJoinRule.Config.class)
      .withSmart(smart)
      .withPredicate((join, joinType, exp) -> true)
      .withRelBuilderFactory(relBuilderFactory)
    );
  }

  /**
   * Rule that tries to push filter expressions into a join condition and into
   * the inputs of the join, iff the join is a column appending
   * non-filtering join.
   */
  public static class HiveFilterNonFilteringJoinMergeRule extends HiveFilterJoinMergeRule {

    @Override
    public boolean matches(RelOptRuleCall call) {
      Join join = call.rel(1);
      RewritablePKFKJoinInfo joinInfo = HiveRelOptUtil.isRewritablePKFKJoin(
          join, join.getLeft(), join.getRight(), call.getMetadataQuery());
      if (!joinInfo.rewritable) {
        return false;
      }
      return super.matches(call);
    }

  }

  /**
   * Rule that tries to push filter expressions into a join condition and into
   * the inputs of the join.
   */
  public static class HiveFilterJoinMergeRule extends HiveFilterJoinRule {
    public HiveFilterJoinMergeRule() {
      super(operand(Filter.class, operand(Join.class, any())),
          HiveFilterJoinMergeRule.class.getSimpleName(), true, HiveRelFactories.HIVE_BUILDER);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      Filter filter = call.rel(0);
      if (!HiveCalciteUtil.isDeterministic(filter.getCondition())) {
        return false;
      }
      return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      Filter filter = call.rel(0);
      Join join = call.rel(1);
      super.perform(call, filter, join);
    }
  }

  public static class HiveFilterJoinTransposeRule extends HiveFilterJoinRule {
    public HiveFilterJoinTransposeRule() {
      super(RelOptRule.operand(Join.class, RelOptRule.any()),
          HiveFilterJoinTransposeRule.class.getSimpleName(), true, HiveRelFactories.HIVE_BUILDER);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      Join join = call.rel(0);
      List<RexNode> joinConds = RelOptUtil.conjunctions(join.getCondition());

      for (RexNode joinCnd : joinConds) {
        if (!HiveCalciteUtil.isDeterministic(joinCnd)) {
          return false;
        }
      }

      return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      Join join = call.rel(0);
      super.perform(call, null, join);
    }
  }


  /**
   * Perform is duplicated from parent class to be able to call the modified
   * classify filters. The modified classify method can push filter conditions
   * that refer only to the SJ right input to the corresponding input (the
   * fix is in the number of fields for the join, which is inferred from the
   * join in the original method rather than the concatenation of the join
   * inputs).
   * TODO: Remove this method once {@link RelOptUtil#classifyFilters} is fixed.
   */
  protected void perform(RelOptRuleCall call, Filter filter, Join join) {
    final List<RexNode> joinFilters =
        RelOptUtil.conjunctions(join.getCondition());
    final List<RexNode> origJoinFilters = ImmutableList.copyOf(joinFilters);

    // If there is only the joinRel,
    // make sure it does not match a cartesian product joinRel
    // (with "true" condition), otherwise this rule will be applied
    // again on the new cartesian product joinRel.
    if (filter == null && joinFilters.isEmpty()) {
      return;
    }

    final List<RexNode> aboveFilters =
        filter != null
            ? getConjunctions(filter)
            : new ArrayList<>();
    final ImmutableList<RexNode> origAboveFilters =
        ImmutableList.copyOf(aboveFilters);

    // Simplify Outer Joins
    JoinRelType joinType = join.getJoinType();
    if (!origAboveFilters.isEmpty()
        && join.getJoinType() != JoinRelType.INNER) {
      joinType = RelOptUtil.simplifyJoin(join, origAboveFilters, joinType);
    }

    final List<RexNode> leftFilters = new ArrayList<>();
    final List<RexNode> rightFilters = new ArrayList<>();

    // TODO - add logic to derive additional filters.  E.g., from
    // (t1.a = 1 AND t2.a = 2) OR (t1.b = 3 AND t2.b = 4), you can
    // derive table filters:
    // (t1.a = 1 OR t1.b = 3)
    // (t2.a = 2 OR t2.b = 4)

    // Try to push down above filters. These are typically where clause
    // filters. They can be pushed down if they are not on the NULL
    // generating side.
    boolean filterPushed = classifyFilters(
        join,
        aboveFilters,
        joinType == JoinRelType.INNER || joinType == JoinRelType.SEMI,
        joinType == JoinRelType.INNER || joinType == JoinRelType.LEFT ||
            joinType == JoinRelType.SEMI || joinType == JoinRelType.ANTI,
        joinType == JoinRelType.INNER || joinType == JoinRelType.RIGHT,
        joinFilters,
        leftFilters,
        rightFilters);

    // Move join filters up if needed
    validateJoinFilters(aboveFilters, joinFilters, join, joinType);

    // If no filter got pushed after validate, reset filterPushed flag
    if (leftFilters.isEmpty()
        && rightFilters.isEmpty()
        && joinFilters.size() == origJoinFilters.size()) {
      if (Sets.newHashSet(joinFilters)
          .equals(Sets.newHashSet(origJoinFilters))) {
        filterPushed = false;
      }
    }

    // Try to push down filters in ON clause. A ON clause filter can only be
    // pushed down if it does not affect the non-matching set, i.e. it is
    // not on the side which is preserved.

    // Anti-join on conditions can not be pushed into left or right, e.g. for plan:
    //
    //     Join(condition=[AND(cond1, $2)], joinType=[anti])
    //     :  - prj(f0=[$0], f1=[$1], f2=[$2])
    //     :  - prj(f0=[$0])
    //
    // The semantic would change if join condition $2 is pushed into left,
    // that is, the result set may be smaller. The right can not be pushed
    // into for the same reason.
    if (joinType != JoinRelType.ANTI
        && classifyFilters(
        join,
        joinFilters,
        false,
        joinType == JoinRelType.INNER || joinType == JoinRelType.RIGHT || joinType == JoinRelType.SEMI,
        joinType == JoinRelType.INNER || joinType == JoinRelType.LEFT || joinType == JoinRelType.SEMI,
        joinFilters,
        leftFilters,
        rightFilters)) {
      filterPushed = true;
    }

    // if nothing actually got pushed and there is nothing leftover,
    // then this rule is a no-op
    if ((!filterPushed
        && joinType == join.getJoinType())
        || (joinFilters.isEmpty()
        && leftFilters.isEmpty()
        && rightFilters.isEmpty())) {
      return;
    }

    // create Filters on top of the children if any filters were
    // pushed to them
    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    final RelBuilder relBuilder = call.builder();
    final RelNode leftRel =
        relBuilder.push(join.getLeft()).filter(leftFilters).build();
    final RelNode rightRel =
        relBuilder.push(join.getRight()).filter(rightFilters).build();

    // create the new join node referencing the new children and
    // containing its new join filters (if there are any)
    final ImmutableList<RelDataType> fieldTypes =
        ImmutableList.<RelDataType>builder()
            .addAll(RelOptUtil.getFieldTypeList(leftRel.getRowType()))
            .addAll(RelOptUtil.getFieldTypeList(rightRel.getRowType())).build();
    final RexNode joinFilter =
        RexUtil.composeConjunction(rexBuilder,
            RexUtil.fixUp(rexBuilder, joinFilters, fieldTypes));

    // If nothing actually got pushed and there is nothing leftover,
    // then this rule is a no-op
    if (joinFilter.isAlwaysTrue()
        && leftFilters.isEmpty()
        && rightFilters.isEmpty()
        && joinType == join.getJoinType()) {
      return;
    }

    RelNode newJoinRel =
        join.copy(
            join.getTraitSet(),
            joinFilter,
            leftRel,
            rightRel,
            joinType,
            join.isSemiJoinDone());
    call.getPlanner().onCopy(join, newJoinRel);
    if (!leftFilters.isEmpty()) {
      call.getPlanner().onCopy(filter, leftRel);
    }
    if (!rightFilters.isEmpty()) {
      call.getPlanner().onCopy(filter, rightRel);
    }

    relBuilder.push(newJoinRel);

    // Create a project on top of the join if some of the columns have become
    // NOT NULL due to the join-type getting stricter.
    relBuilder.convert(join.getRowType(), false);

    // create a FilterRel on top of the join if needed
    relBuilder.filter(
        RexUtil.fixUp(rexBuilder, aboveFilters,
            RelOptUtil.getFieldTypeList(relBuilder.peek().getRowType())));
    call.transformTo(relBuilder.build());
  }

  private List<RexNode> getConjunctions(Filter filter) {
    List<RexNode> conjunctions = conjunctions(filter.getCondition());
    RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    for (int i = 0; i < conjunctions.size(); i++) {
      RexNode node = conjunctions.get(i);
      if (node instanceof RexCall) {
        conjunctions.set(i,
            RelOptUtil.collapseExpandedIsNotDistinctFromExpr((RexCall) node, rexBuilder));
      }
    }
    return conjunctions;
  }

  /**
   * Classifies filters according to where they should be processed. They
   * either stay where they are, are pushed to the join (if they originated
   * from above the join), or are pushed to one of the children. Filters that
   * are pushed are added to list passed in as input parameters.
   *
   * @param joinRel      join node
   * @param filters      filters to be classified
   * @param pushInto     whether filters can be pushed into the join
   * @param pushLeft     true if filters can be pushed to the left
   * @param pushRight    true if filters can be pushed to the right
   * @param joinFilters  list of filters to push to the join
   * @param leftFilters  list of filters to push to the left child
   * @param rightFilters list of filters to push to the right child
   * @return whether at least one filter was pushed
   */
  private static boolean classifyFilters(
      RelNode joinRel,
      List<RexNode> filters,
      boolean pushInto,
      boolean pushLeft,
      boolean pushRight,
      List<RexNode> joinFilters,
      List<RexNode> leftFilters,
      List<RexNode> rightFilters) {
    if (Bug.CALCITE_4499_FIXED) {
      throw new AssertionError("Remove this method when [CALCITE-4499] "
          + "has been fixed and use directly Calcite's RelOptUtil.classifyFilters.");
    }
    RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
    List<RelDataTypeField> joinFields = joinRel.getRowType().getFieldList();
    final int nSysFields = 0; // joinRel.getSystemFieldList().size();
    final List<RelDataTypeField> leftFields =
        joinRel.getInputs().get(0).getRowType().getFieldList();
    final int nFieldsLeft = leftFields.size();
    final List<RelDataTypeField> rightFields =
        joinRel.getInputs().get(1).getRowType().getFieldList();
    final int nFieldsRight = rightFields.size();
    final int nTotalFields = nFieldsLeft + nFieldsRight;

    // set the reference bitmaps for the left and right children
    ImmutableBitSet leftBitmap =
        ImmutableBitSet.range(nSysFields, nSysFields + nFieldsLeft);
    ImmutableBitSet rightBitmap =
        ImmutableBitSet.range(nSysFields + nFieldsLeft, nTotalFields);

    final List<RexNode> filtersToRemove = new ArrayList<>();
    for (RexNode filter : filters) {
      final InputFinder inputFinder = InputFinder.analyze(filter);
      final ImmutableBitSet inputBits = inputFinder.build();

      // REVIEW - are there any expressions that need special handling
      // and therefore cannot be pushed?

      // filters can be pushed to the left child if the left child
      // does not generate NULLs and the only columns referenced in
      // the filter originate from the left child
      if (pushLeft && leftBitmap.contains(inputBits)) {
        // ignore filters that always evaluate to true
        if (!filter.isAlwaysTrue()) {
          // adjust the field references in the filter to reflect
          // that fields in the left now shift over by the number
          // of system fields
          final RexNode shiftedFilter =
              shiftFilter(
                  nSysFields,
                  nSysFields + nFieldsLeft,
                  -nSysFields,
                  rexBuilder,
                  joinFields,
                  nTotalFields,
                  leftFields,
                  filter);

          leftFilters.add(shiftedFilter);
        }
        filtersToRemove.add(filter);

        // filters can be pushed to the right child if the right child
        // does not generate NULLs and the only columns referenced in
        // the filter originate from the right child
      } else if (pushRight && rightBitmap.contains(inputBits)) {
        if (!filter.isAlwaysTrue()) {
          // adjust the field references in the filter to reflect
          // that fields in the right now shift over to the left;
          // since we never push filters to a NULL generating
          // child, the types of the source should match the dest
          // so we don't need to explicitly pass the destination
          // fields to RexInputConverter
          final RexNode shiftedFilter =
              shiftFilter(
                  nSysFields + nFieldsLeft,
                  nTotalFields,
                  -(nSysFields + nFieldsLeft),
                  rexBuilder,
                  joinFields,
                  nTotalFields,
                  rightFields,
                  filter);
          rightFilters.add(shiftedFilter);
        }
        filtersToRemove.add(filter);

      } else {
        // If the filter can't be pushed to either child, we may push them into the join
        if (pushInto) {
          if (!joinFilters.contains(filter)) {
            joinFilters.add(filter);
          }
          filtersToRemove.add(filter);
        }
      }
    }

    // Remove filters after the loop, to prevent concurrent modification.
    if (!filtersToRemove.isEmpty()) {
      filters.removeAll(filtersToRemove);
    }

    // Did anything change?
    return !filtersToRemove.isEmpty();
  }

  private static RexNode shiftFilter(
      int start,
      int end,
      int offset,
      RexBuilder rexBuilder,
      List<RelDataTypeField> joinFields,
      int nTotalFields,
      List<RelDataTypeField> rightFields,
      RexNode filter) {
    int[] adjustments = new int[nTotalFields];
    for (int i = start; i < end; i++) {
      adjustments[i] = offset;
    }
    return filter.accept(
        new RexInputConverter(
            rexBuilder,
            joinFields,
            rightFields,
            adjustments));
  }
}

// End PushFilterPastJoinRule.java

