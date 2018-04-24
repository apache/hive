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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.rules.AbstractJoinExtractFilterRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc.HiveJdbcConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;

/**
 * JDBCExtractJoinFilterRule extracts out the
 * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter}
 * from a {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin} operator.
 * if the HiveFilter could be replaced by two HiveFilter operators that one of them could be pushed down below the
 * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc.HiveJdbcConverter}
 */


public final class JDBCExtractJoinFilterRule extends AbstractJoinExtractFilterRule {
  //~ Static fields/initializers ---------------------------------------------
  public static final JDBCExtractJoinFilterRule INSTANCE = new JDBCExtractJoinFilterRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an JoinExtractFilterRule.
   */
  public JDBCExtractJoinFilterRule() {
    super(operand(HiveJoin.class,
            operand(HiveJdbcConverter.class, any()),
            operand(HiveJdbcConverter.class, any())),
          HiveRelFactories.HIVE_BUILDER, null);
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public boolean matches(RelOptRuleCall call) {
    final Join join = call.rel(0);
    final HiveJdbcConverter conv1 = call.rel(1);
    final HiveJdbcConverter conv2 = call.rel(2);
    if (!conv1.getJdbcDialect().equals(conv2.getJdbcDialect())) {
      return false;
    }
    return JDBCAbstractSplitFilterRule.canSplitFilter(join.getCondition(), conv1.getJdbcDialect());
  }

}

// End JoinExtractFilterRule.java
