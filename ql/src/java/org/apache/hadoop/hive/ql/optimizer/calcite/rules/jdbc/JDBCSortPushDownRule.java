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
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcSortRule;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcSort;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc.HiveJdbcConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBCSortPushDownRule convert a {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit}
 * into a {@link org.apache.calcite.adapter.jdbc.JdbcRules.JdbcSort}
 * and pushes it down below the {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc.HiveJdbcConverter}}
 * operator so it will be sent to the external table.
 */

public class JDBCSortPushDownRule extends RelOptRule {
  private static final Logger LOG = LoggerFactory.getLogger(JDBCSortPushDownRule.class);

  public static final JDBCSortPushDownRule INSTANCE = new JDBCSortPushDownRule();

  public JDBCSortPushDownRule() {
    super(operand(HiveSortLimit.class,
        operand(HiveJdbcConverter.class, any())));
  }

  public boolean matches(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final HiveJdbcConverter conv = call.rel(1);

    for (RexNode currCall : sort.getSortExps()) {
      if (!JDBCRexCallValidator.isValidJdbcOperation(currCall, conv.getJdbcDialect())) {
        return false;
      }
    }

    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LOG.debug("JDBCSortPushDownRule has been called");

    final HiveSortLimit sort = call.rel(0);
    final HiveJdbcConverter converter = call.rel(1);

    RelNode node = (sort.fetch != null && RexLiteral.intValue(sort.fetch) == 0)
      ? new JdbcFilter(
            sort.getCluster(),
            sort.getTraitSet().replace(converter.getJdbcConvention()),
            converter.getInput(),
            call.builder().literal(false))
      : new JdbcSort(
            sort.getCluster(),
            sort.getTraitSet().replace(converter.getJdbcConvention()),
            converter.getInput(),
            sort.getCollation(),
            sort.offset,
            sort.fetch);

    call.transformTo(converter.copy(converter.getTraitSet(), node));
  }

}
