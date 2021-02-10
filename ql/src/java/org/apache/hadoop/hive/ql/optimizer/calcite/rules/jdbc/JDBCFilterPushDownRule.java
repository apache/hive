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

import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcFilter;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcFilterRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc.HiveJdbcConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBCExtractJoinFilterRule extracts out the
 * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter}
 * from a {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin} operator.
 * if the HiveFilter could be replaced by two HiveFilter operators that one of them could be pushed down below the
 * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc.HiveJdbcConverter}
 */

public class JDBCFilterPushDownRule extends RelOptRule {
  private static final Logger LOG = LoggerFactory.getLogger(JDBCFilterPushDownRule.class);

  public static final JDBCFilterPushDownRule INSTANCE = new JDBCFilterPushDownRule();

  public JDBCFilterPushDownRule() {
    super(operand(HiveFilter.class,
            operand(HiveJdbcConverter.class, any())));
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final HiveFilter filter = call.rel(0);
    final HiveJdbcConverter converter = call.rel(1);

    RexNode cond = filter.getCondition();

    return JDBCRexCallValidator.isValidJdbcOperation(cond, converter.getJdbcDialect());
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LOG.debug("JDBCFilterPushDown has been called");

    final HiveFilter filter = call.rel(0);
    final HiveJdbcConverter converter = call.rel(1);

    Filter newHiveFilter = filter.copy(filter.getTraitSet(), converter.getInput(), filter.getCondition());
    JdbcFilter newJdbcFilter = (JdbcFilter) JdbcFilterRule.create(converter.getJdbcConvention()).convert(newHiveFilter);
    if (newJdbcFilter != null) {
      RelNode converterRes = converter.copy(converter.getTraitSet(), Arrays.asList(newJdbcFilter));

      call.transformTo(converterRes);
    }
  }

};
