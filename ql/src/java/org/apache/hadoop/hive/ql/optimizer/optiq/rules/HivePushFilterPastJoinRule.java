package org.apache.hadoop.hive.ql.optimizer.optiq.rules;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveFilterRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelOptUtil.InputFinder;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.util.Holder;

import com.google.common.collect.ImmutableList;

public abstract class HivePushFilterPastJoinRule extends RelOptRule {

  public static final HivePushFilterPastJoinRule FILTER_ON_JOIN = new HivePushFilterPastJoinRule(
      operand(FilterRelBase.class, operand(HiveJoinRel.class, any())),
      "HivePushFilterPastJoinRule:filter", true) {
    @Override
    public void onMatch(RelOptRuleCall call) {
      HiveFilterRel filter = call.rel(0);
      HiveJoinRel join = call.rel(1);
      perform(call, filter, join);
    }
  };

  public static final HivePushFilterPastJoinRule JOIN = new HivePushFilterPastJoinRule(
      operand(HiveJoinRel.class, any()), "HivePushFilterPastJoinRule:no-filter", false) {
    @Override
    public void onMatch(RelOptRuleCall call) {
      HiveJoinRel join = call.rel(0);
      perform(call, null, join);
    }
  };

  /** Whether to try to strengthen join-type. */
  private final boolean smart;

  // ~ Constructors -----------------------------------------------------------

  /**
   * Creates a PushFilterPastJoinRule with an explicit root operand.
   */
  private HivePushFilterPastJoinRule(RelOptRuleOperand operand, String id, boolean smart) {
    super(operand, "PushFilterRule: " + id);
    this.smart = smart;
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
    int origJoinFiltersSz = joinFilters.size();

    // TODO - add logic to derive additional filters. E.g., from
    // (t1.a = 1 AND t2.a = 2) OR (t1.b = 3 AND t2.b = 4), you can
    // derive table filters:
    // (t1.a = 1 OR t1.b = 3)
    // (t2.a = 2 OR t2.b = 4)

    // Try to push down above filters. These are typically where clause
    // filters. They can be pushed down if they are not on the NULL
    // generating side.
    boolean filterPushed = false;
    final Holder<JoinRelType> joinTypeHolder = Holder.of(join.getJoinType());
    if (RelOptUtil.classifyFilters(join, aboveFilters,
        join.getJoinType(), !join.getJoinType().generatesNullsOnLeft(), !join.getJoinType()
        .generatesNullsOnRight(), joinFilters, leftFilters, rightFilters, joinTypeHolder, false)) {
      filterPushed = true;
    }

    /*
     * Any predicates pushed down to joinFilters that aren't equality
     * conditions: put them back as aboveFilters because Hive doesn't support
     * not equi join conditions.
     */
    ListIterator<RexNode> filterIter = joinFilters.listIterator();
    while (filterIter.hasNext()) {
      RexNode exp = filterIter.next();
      if (exp instanceof RexCall) {
        RexCall c = (RexCall) exp;
        if (c.getOperator().getKind() == SqlKind.EQUALS) {
          boolean validHiveJoinFilter = true;
          for (RexNode rn : c.getOperands()) {
            // NOTE: Hive dis-allows projections from both left & right side
            // of join condition. Example: Hive disallows
            // (r1.x=r2.x)=(r1.y=r2.y) on join condition.
            if (filterRefersToBothSidesOfJoin(rn, join)) {
              validHiveJoinFilter = false;
              break;
            }
          }
          if (validHiveJoinFilter)
            continue;
        }
      }
      aboveFilters.add(exp);
      filterIter.remove();
    }

    /*
     * if all pushed filters where put back then set filterPushed to false
     */
    if (leftFilters.size() == 0 && rightFilters.size() == 0
        && joinFilters.size() == origJoinFiltersSz) {
      filterPushed = false;
    }

    // Try to push down filters in ON clause. A ON clause filter can only be
    // pushed down if it does not affect the non-matching set, i.e. it is
    // not on the side which is preserved.
    if (RelOptUtil.classifyFilters(join, joinFilters, null, !join
        .getJoinType().generatesNullsOnRight(), !join.getJoinType()
        .generatesNullsOnLeft(), joinFilters, leftFilters, rightFilters, joinTypeHolder, false)) {
      filterPushed = true;
    }

    if (!filterPushed) {
      return;
    }

    /*
     * Remove always true conditions that got pushed down.
     */
    removeAlwaysTruePredicates(leftFilters);
    removeAlwaysTruePredicates(rightFilters);
    removeAlwaysTruePredicates(joinFilters);

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
      if (leftFilters.isEmpty()
          && rightFilters.isEmpty()
          && joinTypeHolder.get() == join.getJoinType()) {
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

  private void removeAlwaysTruePredicates(List<RexNode> predicates) {
    if (predicates.size() < 2) {
      return;
    }
    ListIterator<RexNode> iter = predicates.listIterator();
    while (iter.hasNext()) {
      RexNode exp = iter.next();
      if (isAlwaysTrue(exp)) {
        iter.remove();
      }
    }
  }

  private boolean isAlwaysTrue(RexNode predicate) {
    if (predicate instanceof RexCall) {
      RexCall c = (RexCall) predicate;
      if (c.getOperator().getKind() == SqlKind.EQUALS) {
        return isAlwaysTrue(c.getOperands().get(0))
            && isAlwaysTrue(c.getOperands().get(1));
      }
    }
    return predicate.isAlwaysTrue();
  }

  private boolean filterRefersToBothSidesOfJoin(RexNode filter, JoinRelBase j) {
    boolean refersToBothSides = false;

    int joinNoOfProjects = j.getRowType().getFieldCount();
    BitSet filterProjs = new BitSet(joinNoOfProjects);
    BitSet allLeftProjs = new BitSet(joinNoOfProjects);
    BitSet allRightProjs = new BitSet(joinNoOfProjects);
    allLeftProjs.set(0, j.getInput(0).getRowType().getFieldCount(), true);
    allRightProjs.set(j.getInput(0).getRowType().getFieldCount(), joinNoOfProjects, true);

    InputFinder inputFinder = new InputFinder(filterProjs);
    filter.accept(inputFinder);

    if (allLeftProjs.intersects(filterProjs) && allRightProjs.intersects(filterProjs))
      refersToBothSides = true;

    return refersToBothSides;
  }
}

// End PushFilterPastJoinRule.java

