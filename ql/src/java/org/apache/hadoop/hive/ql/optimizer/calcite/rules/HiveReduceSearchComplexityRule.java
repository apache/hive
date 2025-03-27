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

import com.google.common.collect.BoundType;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Sarg;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;

import java.util.Collections;
import java.util.Objects;

public class HiveReduceSearchComplexityRule extends RelOptRule {

  // TODO Should we handle JOIN as well?
  public static final RelOptRule FILTER = new HiveReduceSearchComplexityRule(operand(HiveFilter.class, any()));
  public static final RelOptRule PROJECT = new HiveReduceSearchComplexityRule(operand(HiveProject.class, any()));

  protected HiveReduceSearchComplexityRule(RelOptRuleOperand operand) {
    super(operand);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelNode oNode = call.rel(0);
    RelNode tNode = oNode.accept(new SearchNegateShuttle(oNode.getCluster().getRexBuilder()));
    if (oNode != tNode) {
      call.transformTo(tNode);
    }
  }

  private static class SearchNegateShuttle extends RexShuttle {
    private final RexBuilder rexBuilder;

    SearchNegateShuttle(final RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexCall visitCall(RexCall call) {
      switch (call.getOperator().getKind()) {
      case SEARCH:
        RexNode ref = call.getOperands().get(0);
        RexLiteral literal = (RexLiteral) call.operands.get(1);
        Sarg<?> sarg = Objects.requireNonNull(literal.getValueAs(Sarg.class), "Sarg");

        if (isNegationBetter(sarg)) {
          return (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.NOT, Collections.singletonList(
              rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, ref,
                  rexBuilder.makeSearchArgumentLiteral(sarg.negate(), literal.getType()))));
        } else {
          return call;
        }
      default:
        return (RexCall) super.visitCall(call);
      }
    }

    private boolean isNegationBetter(Sarg<?> sarg) {
      int complexity = getComplexity(sarg);
      int negationComplexity = getComplexity(sarg.negate());

      return negationComplexity < complexity || (negationComplexity == complexity
          && countClosedRanges(sarg.negate()) > countClosedRanges(sarg));
    }

    /**
     * Calcite's complexity method ignores complexity of IS NOT NULL.
     * This can be removed if we decide to ignore IS NOT NULLs in Hive.
     * @param sarg SEARCH args
     * @return complexity of sarg
     */
    private int getComplexity(Sarg<?> sarg) {
      int complexity = sarg.complexity();
      if (sarg.nullAs == RexUnknownAs.FALSE) {
        complexity++;
      }
      return complexity;
    }

    private long countClosedRanges(Sarg<?> sarg) {
      return sarg.rangeSet.asRanges().stream().filter(
          r -> r.hasUpperBound() && r.hasLowerBound() && r.lowerBoundType() == BoundType.CLOSED
              && r.upperBoundType() == BoundType.CLOSED).count();
    }
  }
}
