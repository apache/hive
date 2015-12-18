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

import java.util.BitSet;
import java.util.List;
import java.util.ListIterator;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;

public abstract class HiveFilterJoinRule extends FilterJoinRule {

  public static final HiveFilterJoinRule FILTER_ON_JOIN = new HiveFilterJoinMergeRule();

  public static final HiveFilterJoinRule JOIN           = new HiveFilterJoinTransposeRule();

  /**
   * Creates a PushFilterPastJoinRule with an explicit root operand.
   */
  protected HiveFilterJoinRule(RelOptRuleOperand operand, String id, boolean smart,
      RelBuilderFactory relBuilderFactory) {
    super(operand, id, smart, relBuilderFactory, TRUE_PREDICATE);
  }

  /**
   * Rule that tries to push filter expressions into a join condition and into
   * the inputs of the join.
   */
  public static class HiveFilterJoinMergeRule extends HiveFilterJoinRule {
    public HiveFilterJoinMergeRule() {
      super(RelOptRule.operand(Filter.class, RelOptRule.operand(Join.class, RelOptRule.any())),
          "HiveFilterJoinRule:filter", true, HiveRelFactories.HIVE_BUILDER);
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
      super(RelOptRule.operand(Join.class, RelOptRule.any()), "HiveFilterJoinRule:no-filter", true,
          HiveRelFactories.HIVE_BUILDER);
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

  /*
   * Any predicates pushed down to joinFilters that aren't equality conditions:
   * put them back as aboveFilters because Hive doesn't support not equi join
   * conditions.
   */
  @Override
  protected void validateJoinFilters(List<RexNode> aboveFilters, List<RexNode> joinFilters,
      Join join, JoinRelType joinType) {
    if (joinType.equals(JoinRelType.INNER)) {
      ListIterator<RexNode> filterIter = joinFilters.listIterator();
      while (filterIter.hasNext()) {
        RexNode exp = filterIter.next();

        if (exp instanceof RexCall) {
          RexCall c = (RexCall) exp;
          boolean validHiveJoinFilter = false;

          if ((c.getOperator().getKind() == SqlKind.EQUALS)) {
            validHiveJoinFilter = true;
            for (RexNode rn : c.getOperands()) {
              // NOTE: Hive dis-allows projections from both left & right side
              // of join condition. Example: Hive disallows
              // (r1.x +r2.x)=(r1.y+r2.y) on join condition.
              if (filterRefersToBothSidesOfJoin(rn, join)) {
                validHiveJoinFilter = false;
                break;
              }
            }
          } else if ((c.getOperator().getKind() == SqlKind.LESS_THAN)
              || (c.getOperator().getKind() == SqlKind.GREATER_THAN)
              || (c.getOperator().getKind() == SqlKind.LESS_THAN_OR_EQUAL)
              || (c.getOperator().getKind() == SqlKind.GREATER_THAN_OR_EQUAL)) {
            validHiveJoinFilter = true;
            // NOTE: Hive dis-allows projections from both left & right side of
            // join in in equality condition. Example: Hive disallows (r1.x <
            // r2.x) on join condition.
            if (filterRefersToBothSidesOfJoin(c, join)) {
              validHiveJoinFilter = false;
            }
          }

          if (validHiveJoinFilter)
            continue;
        }

        aboveFilters.add(exp);
        filterIter.remove();
      }
    }
  }

  private boolean filterRefersToBothSidesOfJoin(RexNode filter, Join j) {
    boolean refersToBothSides = false;

    int joinNoOfProjects = j.getRowType().getFieldCount();
    ImmutableBitSet filterProjs = ImmutableBitSet.FROM_BIT_SET.apply(new BitSet(joinNoOfProjects));
    ImmutableBitSet allLeftProjs = filterProjs.union(ImmutableBitSet.range(0, j.getInput(0)
        .getRowType().getFieldCount()));
    ImmutableBitSet allRightProjs = filterProjs.union(ImmutableBitSet.range(j.getInput(0)
        .getRowType().getFieldCount(), joinNoOfProjects));

    filterProjs = filterProjs.union(InputFinder.bits(filter));

    if (allLeftProjs.intersects(filterProjs) && allRightProjs.intersects(filterProjs))
      refersToBothSides = true;

    return refersToBothSides;
  }
}

// End PushFilterPastJoinRule.java

