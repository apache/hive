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
package org.apache.hadoop.hive.ql.optimizer.optiq.rules;

import java.util.BitSet;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveFilterRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel;
import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelFactories;
import org.eigenbase.rel.rules.PushFilterPastJoinRule;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.relopt.RelOptUtil.InputFinder;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlKind;

public abstract class HivePushFilterPastJoinRule extends PushFilterPastJoinRule {

  public static final HivePushFilterPastJoinRule FILTER_ON_JOIN = new HivePushFilterIntoJoinRule();

  public static final HivePushFilterPastJoinRule JOIN           = new HivePushDownJoinConditionRule();

  /**
   * Creates a PushFilterPastJoinRule with an explicit root operand.
   */
  protected HivePushFilterPastJoinRule(RelOptRuleOperand operand, String id, boolean smart,
      RelFactories.FilterFactory filterFactory, RelFactories.ProjectFactory projectFactory) {
    super(operand, id, smart, filterFactory, projectFactory);
  }

  /**
   * Rule that tries to push filter expressions into a join condition and into
   * the inputs of the join.
   */
  public static class HivePushFilterIntoJoinRule extends HivePushFilterPastJoinRule {
    public HivePushFilterIntoJoinRule() {
      super(RelOptRule.operand(FilterRelBase.class,
          RelOptRule.operand(JoinRelBase.class, RelOptRule.any())),
          "HivePushFilterPastJoinRule:filter", true, HiveFilterRel.DEFAULT_FILTER_FACTORY,
          HiveProjectRel.DEFAULT_PROJECT_FACTORY);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      FilterRelBase filter = call.rel(0);
      JoinRelBase join = call.rel(1);
      super.perform(call, filter, join);
    }
  }

  public static class HivePushDownJoinConditionRule extends HivePushFilterPastJoinRule {
    public HivePushDownJoinConditionRule() {
      super(RelOptRule.operand(JoinRelBase.class, RelOptRule.any()),
          "HivePushFilterPastJoinRule:no-filter", true, HiveFilterRel.DEFAULT_FILTER_FACTORY,
          HiveProjectRel.DEFAULT_PROJECT_FACTORY);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      JoinRelBase join = call.rel(0);
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
      JoinRelBase join, JoinRelType joinType) {
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

