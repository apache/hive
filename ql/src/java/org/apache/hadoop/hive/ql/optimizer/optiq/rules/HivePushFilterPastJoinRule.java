package org.apache.hadoop.hive.ql.optimizer.optiq.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveFilterRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;

import com.google.common.collect.ImmutableList;

public abstract class HivePushFilterPastJoinRule extends RelOptRule {

  public static final HivePushFilterPastJoinRule FILTER_ON_JOIN = new HivePushFilterPastJoinRule(
      operand(HiveFilterRel.class, operand(HiveJoinRel.class, any())),
      "HivePushFilterPastJoinRule:filter") {
    @Override
    public void onMatch(RelOptRuleCall call) {
      HiveFilterRel filter = call.rel(0);
      HiveJoinRel join = call.rel(1);
      perform(call, filter, join);
    }
  };

  public static final HivePushFilterPastJoinRule JOIN = new HivePushFilterPastJoinRule(
      operand(HiveJoinRel.class, any()), "HivePushFilterPastJoinRule:no-filter") {
    @Override
    public void onMatch(RelOptRuleCall call) {
      HiveJoinRel join = call.rel(0);
      perform(call, null, join);
    }
  };

  // ~ Constructors -----------------------------------------------------------

  /**
   * Creates a PushFilterPastJoinRule with an explicit root operand.
   */
  private HivePushFilterPastJoinRule(RelOptRuleOperand operand, String id) {
    super(operand, "PushFilterRule: " + id);
  }

  // ~ Methods ----------------------------------------------------------------

  protected void perform(RelOptRuleCall call, FilterRelBase filter,
      JoinRelBase join) {
    final List<RexNode> joinFilters = RelOptUtil.conjunctions(join
        .getCondition());

    /*
     * todo: hb 6/26/14 for left SemiJoin we cannot push predicates yet. The
     * assertion that num(JoinRel columns) = num(leftSrc) + num(rightSrc)
     * doesn't hold. So RelOptUtil.classifyFilters fails.
     */
    if (((HiveJoinRel) join).isLeftSemiJoin()) {
      return;
    }

    if (filter == null) {
      // There is only the joinRel
      // make sure it does not match a cartesian product joinRel
      // (with "true" condition) otherwise this rule will be applied
      // again on the new cartesian product joinRel.
      boolean onlyTrueFilter = true;
      for (RexNode joinFilter : joinFilters) {
        if (!joinFilter.isAlwaysTrue()) {
          onlyTrueFilter = false;
          break;
        }
      }

      if (onlyTrueFilter) {
        return;
      }
    }

    final List<RexNode> aboveFilters = filter != null ? RelOptUtil
        .conjunctions(filter.getCondition()) : ImmutableList.<RexNode> of();

    List<RexNode> leftFilters = new ArrayList<RexNode>();
    List<RexNode> rightFilters = new ArrayList<RexNode>();

    // TODO - add logic to derive additional filters. E.g., from
    // (t1.a = 1 AND t2.a = 2) OR (t1.b = 3 AND t2.b = 4), you can
    // derive table filters:
    // (t1.a = 1 OR t1.b = 3)
    // (t2.a = 2 OR t2.b = 4)

    // Try to push down above filters. These are typically where clause
    // filters. They can be pushed down if they are not on the NULL
    // generating side.
    boolean filterPushed = false;
    if (RelOptUtil.classifyFilters(join, aboveFilters,
    /* join.getJoinType() == JoinRelType.INNER */
    /* we don't allow non-equality conds on JoinOp */
    false, !join.getJoinType().generatesNullsOnLeft(), !join.getJoinType()
        .generatesNullsOnRight(), joinFilters, leftFilters, rightFilters)) {
      filterPushed = true;
    }
    // Try to push down filters in ON clause. A ON clause filter can only be
    // pushed down if it does not affect the non-matching set, i.e. it is
    // not on the side which is preserved.
    if (RelOptUtil.classifyFilters(join, joinFilters, false, !join
        .getJoinType().generatesNullsOnRight(), !join.getJoinType()
        .generatesNullsOnLeft(), joinFilters, leftFilters, rightFilters)) {
      filterPushed = true;
    }

    if (!filterPushed) {
      return;
    }

    // create FilterRels on top of the children if any filters were
    // pushed to them
    RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    RelNode leftRel = createFilterOnRel(rexBuilder, join.getLeft(), leftFilters);
    RelNode rightRel = createFilterOnRel(rexBuilder, join.getRight(),
        rightFilters);

    // create the new join node referencing the new children and
    // containing its new join filters (if there are any)
    RexNode joinFilter;

    if (joinFilters.size() == 0) {
      // if nothing actually got pushed and there is nothing leftover,
      // then this rule is a no-op
      if ((leftFilters.size() == 0) && (rightFilters.size() == 0)) {
        return;
      }
      joinFilter = rexBuilder.makeLiteral(true);
    } else {
      joinFilter = RexUtil.composeConjunction(rexBuilder, joinFilters, true);
    }
    RelNode newJoinRel = HiveJoinRel.getJoin(join.getCluster(), leftRel,
        rightRel, joinFilter, join.getJoinType(), false);

    // create a FilterRel on top of the join if needed
    RelNode newRel = createFilterOnRel(rexBuilder, newJoinRel, aboveFilters);

    call.transformTo(newRel);
  }

  /**
   * If the filter list passed in is non-empty, creates a FilterRel on top of
   * the existing RelNode; otherwise, just returns the RelNode
   *
   * @param rexBuilder
   *          rex builder
   * @param rel
   *          the RelNode that the filter will be put on top of
   * @param filters
   *          list of filters
   * @return new RelNode or existing one if no filters
   */
  private RelNode createFilterOnRel(RexBuilder rexBuilder, RelNode rel,
      List<RexNode> filters) {
    RexNode andFilters = RexUtil.composeConjunction(rexBuilder, filters, false);
    if (andFilters.isAlwaysTrue()) {
      return rel;
    }
    return new HiveFilterRel(rel.getCluster(), rel.getCluster().traitSetOf(
        HiveRel.CONVENTION), rel, andFilters);
  }
}

// End PushFilterPastJoinRule.java

