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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules.jdbc;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc.HiveJdbcConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveFilterJoinRule;

/**
 * Rule that tries to push filter expressions into a join condition and into
 * the inputs of the join.
 */

public class JDBCFilterJoinRule extends HiveFilterJoinRule {

  public static final JDBCFilterJoinRule INSTANCE = new JDBCFilterJoinRule();

  public JDBCFilterJoinRule() {
    super(RelOptRule.operand(HiveFilter.class,
            RelOptRule.operand(HiveJoin.class,
              RelOptRule.operand(HiveJdbcConverter.class, RelOptRule.any()),
              RelOptRule.operand(HiveJdbcConverter.class, RelOptRule.any()))),
        "JDBCFilterJoinRule", true, HiveRelFactories.HIVE_BUILDER);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Filter filter = call.rel(0);
    Join   join = call.rel(1);
    HiveJdbcConverter   conv1 = call.rel(2);
    HiveJdbcConverter   conv2 = call.rel(3);

    if (!conv1.getJdbcDialect().equals(conv2.getJdbcDialect())) {
      return false;
    }

    boolean visitorRes = JDBCRexCallValidator.isValidJdbcOperation(filter.getCondition(), conv1.getJdbcDialect());
    if (visitorRes) {
      return JDBCRexCallValidator.isValidJdbcOperation(join.getCondition(), conv1.getJdbcDialect());
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Filter filter = call.rel(0);
    Join join = call.rel(1);
    super.perform(call, filter, join);
  }
}
