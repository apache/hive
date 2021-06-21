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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that matches a {@link Filter} expression with correlated variables, and rewrites the
 * condition in a simpler form that is more convenient for the decorrelation logic.
 *
 * Uncorrelated calls below a comparison operator are turned into input references by extracting the
 * computation in a {@link org.apache.calcite.rel.core.Project} expression. An additional projection
 * may be added on top of the new filter to retain expression equivalence.
 *
 * <h3>Sub-plan before</h3>
 * <pre>
 * LogicalProject($f0=[true])
 *   LogicalFilter(condition=[=($cor0.DEPTNO, +($7, 30))])
 *     LogicalTableScan(table=[[CATALOG, SALES, EMP]])
 * </pre>
 * <h3>Sub-plan after</h3>
 * <pre>
 * LogicalProject($f0=[true])
 *   LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2],..., COMM=[$6], DEPTNO=[$7], SLACKER=[$8])
 *     LogicalFilter(condition=[=($cor0.DEPTNO, $9)])
 *       LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2],..., SLACKER=[$8], $f9=[+($7, 30)])
 *         LogicalTableScan(table=[[CATALOG, SALES, EMP]])
 * </pre>
 */
public final class FilterFlattenCorrelatedConditionRule
    extends RelOptRule {

  public FilterFlattenCorrelatedConditionRule(Class<? extends Filter> clazz, RelBuilderFactory factory,
      String description) {
    super(operand(clazz, any()), factory, description);
  }

  @Override public boolean matches(RelOptRuleCall call) {
    Filter filter = call.rel(0);
    return RexUtil.containsCorrelation(filter.getCondition());
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Filter filter = call.rel(0);
    RelBuilder b = call.builder();
    b.push(filter.getInput());
    final int proj = b.fields().size();
    List<RexNode> projOperands = new ArrayList<>();
    RexNode newCondition = filter.getCondition().accept(new RexShuttle() {
      @Override public RexNode visitCall(RexCall call) {
        switch (call.getKind()) {
        case EQUALS:
        case NOT_EQUALS:
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
          RexNode op0 = call.operands.get(0);
          RexNode op1 = call.operands.get(1);
          final int replaceIndex;
          if (RexUtil.containsCorrelation(op1) && isUncorrelatedCall(op0)) {
            replaceIndex = 0;
          } else if (RexUtil.containsCorrelation(op0) && isUncorrelatedCall(op1)) {
            replaceIndex = 1;
          } else {
            // Structure does not match, do not replace
            replaceIndex = -1;
          }
          if (replaceIndex != -1) {
            List<RexNode> copyOperands = new ArrayList<>(call.operands);
            RexNode oldOp = call.operands.get(replaceIndex);
            RexNode newOp = b.getRexBuilder()
                .makeInputRef(oldOp.getType(), proj + projOperands.size());
            projOperands.add(oldOp);
            copyOperands.set(replaceIndex, newOp);
            return call.clone(call.type, copyOperands);
          }
          break;
        case AND:
        case OR:
          return super.visitCall(call);
        }
        return call;
      }
    });
    if (newCondition.equals(filter.getCondition())) {
      return;
    }
    b.projectPlus(projOperands);
    b.filter(newCondition);
    b.project(b.fields(ImmutableBitSet.range(proj).asList()));
    call.transformTo(b.build());
  }

  private static boolean isUncorrelatedCall(RexNode node) {
    return node instanceof RexCall && !RexUtil.containsCorrelation(node);
  }
}
