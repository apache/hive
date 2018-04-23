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

import java.util.Arrays;

import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcJoin;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcJoinRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc.HiveJdbcConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBCJoinPushDownRule convert a {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin}
 * into a {@link org.apache.calcite.adapter.jdbc.JdbcRules.JdbcJoin}
 * and pushes it down below the {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc.HiveJdbcConverter}}
 * operator so it will be sent to the external table.
 */

public class JDBCJoinPushDownRule extends RelOptRule {
  private static final Logger LOG = LoggerFactory.getLogger(JDBCJoinPushDownRule.class);

  public static final JDBCJoinPushDownRule INSTANCE = new JDBCJoinPushDownRule();

  public JDBCJoinPushDownRule() {
    super(operand(HiveJoin.class,
            operand(HiveJdbcConverter.class, any()),
            operand(HiveJdbcConverter.class, any())));
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final HiveJoin join = call.rel(0);
    final RexNode cond = join.getCondition();
    final HiveJdbcConverter converter1 = call.rel(1);
    final HiveJdbcConverter converter2 = call.rel(2);

  //The actual check should be the compare of the connection string of the external tables
    /*if (converter1.getJdbcConvention().equals(converter2.getJdbcConvention()) == false) {
      return false;
    }*/

    if (!converter1.getJdbcConvention().getName().equals(converter2.getJdbcConvention().getName())) {
      return false;
    }

    if (cond.isAlwaysTrue()) {
      //We don't want to push cross join
      return false;
    }

    boolean visitorRes = JDBCRexCallValidator.isValidJdbcOperation(cond, converter1.getJdbcDialect());
    return visitorRes;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LOG.debug("JDBCJoinPushDownRule has been called");

    final HiveJoin join = call.rel(0);
    final HiveJdbcConverter converter1 = call.rel(1);
    final HiveJdbcConverter converter2 = call.rel(2);

    RelNode input1 = converter1.getInput();
    RelNode input2 = converter2.getInput();

    HiveJoin newHiveJoin = join.copy(join.getTraitSet(), join.getCondition(), input1, input2, join.getJoinType(),
            join.isSemiJoinDone());
    JdbcJoin newJdbcJoin = (JdbcJoin) new JdbcJoinRule(converter1.getJdbcConvention()).convert(newHiveJoin,
            false);
    if (newJdbcJoin != null) {
      RelNode converterRes = converter1.copy(converter1.getTraitSet(), Arrays.asList(newJdbcJoin));
      if (converterRes != null) {
        call.transformTo(converterRes);
      }
    }
  }

};
