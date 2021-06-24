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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.SqlFunctionConverter;

/**
 * Rule to rewrite a window function containing a last value clause.
 */
public class HiveWindowingLastValueRewrite extends RelOptRule {

  public static final HiveWindowingLastValueRewrite INSTANCE = new HiveWindowingLastValueRewrite();

  private static final String FIRST_VALUE_FUNC = "first_value";
  private static final String LAST_VALUE_FUNC = "last_value";


  private HiveWindowingLastValueRewrite() {
    super(operand(Project.class, any()));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Project project = call.rel(0);

    List<RexNode> newExprs = new ArrayList<>();
    LastValueRewriteRexShuttle lastValueRewrite = new LastValueRewriteRexShuttle(
        project.getCluster().getRexBuilder());
    boolean modified = false;
    for (RexNode expr : project.getProjects()) {
      RexNode newExpr = lastValueRewrite.apply(expr);
      newExprs.add(newExpr);
      modified |= (newExpr != expr);
    }
    if (modified) {
      RelNode newProject = project.copy(
          project.getTraitSet(), project.getInput(), newExprs, project.getRowType());
      call.transformTo(newProject);
    }
  }

  private static class LastValueRewriteRexShuttle extends RexShuttle {

    private final RexBuilder rexBuilder;

    private LastValueRewriteRexShuttle(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    public RexNode visitOver(RexOver over) {
      if (over.op.getName().equals(LAST_VALUE_FUNC) && over.getWindow().getLowerBound().isUnbounded()
        && over.getWindow().getUpperBound().isUnbounded()) {
        ImmutableList<RexFieldCollation> orderKeys = over.getWindow().orderKeys;
        if (CollectionUtils.isEmpty(orderKeys)) {
          return over;
        }
        ImmutableList.Builder<RexFieldCollation> newOrderKeys = ImmutableList.builder();
        for (RexFieldCollation orderKey : orderKeys) {
          Set<SqlKind> flags = new HashSet<>();
          if (orderKey.getDirection() == RelFieldCollation.Direction.ASCENDING) {
            flags.add(SqlKind.DESCENDING);
          }
          if (orderKey.right.contains(SqlKind.NULLS_FIRST)) {
            flags.add(SqlKind.NULLS_LAST);
          } else {
            flags.add(SqlKind.NULLS_FIRST);
          }
          newOrderKeys.add(new RexFieldCollation(orderKey.left, flags));
        }
        SqlAggFunction s = (SqlAggFunction) over.op;
        SqlFunctionConverter.CalciteUDAF newSqlAggFunction = new SqlFunctionConverter.CalciteUDAF(
            over.isDistinct(), FIRST_VALUE_FUNC, s.getReturnTypeInference(), s.getOperandTypeInference(),
            s.getOperandTypeChecker());
        List<RexNode> clonedOperands = visitList(over.operands, new boolean[] {false});
        RexWindow window = visitWindow(over.getWindow());
        return rexBuilder.makeOver(over.type, newSqlAggFunction, clonedOperands,
            window.partitionKeys, newOrderKeys.build(),
            window.getLowerBound(), window.getUpperBound(),
            window.isRows(), true, false, over.isDistinct(), over.ignoreNulls());
      }
      return over;
    }
  }
}
