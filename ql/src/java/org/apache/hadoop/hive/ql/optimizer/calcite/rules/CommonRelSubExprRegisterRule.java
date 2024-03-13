/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.plan.CommonRelSubExprRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.CommonTableExpressionRegistry;

import java.util.function.Predicate;

public final class CommonRelSubExprRegisterRule extends CommonRelSubExprRule {
  private static final Predicate<RelNode> NO_SCAN_PREDICATE = rel -> !(rel instanceof TableScan);
  public static final CommonRelSubExprRegisterRule JOIN = new CommonRelSubExprRegisterRule(operand(Join.class, any()));
  public static final CommonRelSubExprRegisterRule AGGREGATE =
      new CommonRelSubExprRegisterRule(operand(Aggregate.class, any()));
  public static final CommonRelSubExprRegisterRule FILTER =
      new CommonRelSubExprRegisterRule(operand(Filter.class, any()));
  public static final CommonRelSubExprRegisterRule PROJECT =
      new CommonRelSubExprRegisterRule(operand(Project.class, operandJ(RelNode.class, null, NO_SCAN_PREDICATE, any())));

  private CommonRelSubExprRegisterRule(RelOptRuleOperand operand) {
    super(operand);
  }

  @Override
  public void onMatch(final RelOptRuleCall call) {
    CommonTableExpressionRegistry r = call.getPlanner().getContext().unwrap(CommonTableExpressionRegistry.class);
    if (r != null) {
      r.add(call.rel(0));
    }
  }
}
