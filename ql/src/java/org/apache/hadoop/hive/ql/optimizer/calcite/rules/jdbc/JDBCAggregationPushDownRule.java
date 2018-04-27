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

import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcAggregate;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcAggregateRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlCountAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc.HiveJdbcConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBCAggregationPushDownRule convert a {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate}
 * into a {@link org.apache.calcite.adapter.jdbc.JdbcRules.JdbcAggregateRule.JdbcAggregate}
 * and pushes it down below the {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc.HiveJdbcConverter}
 * operator so it will be sent to the external table.
 */

public class JDBCAggregationPushDownRule extends RelOptRule {
  private static final Logger LOG = LoggerFactory.getLogger(JDBCAggregationPushDownRule.class);

  public static final JDBCAggregationPushDownRule INSTANCE = new JDBCAggregationPushDownRule();

  public JDBCAggregationPushDownRule() {
    super(operand(HiveAggregate.class,
            operand(HiveJdbcConverter.class, any())));
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final HiveAggregate agg = call.rel(0);
    final HiveJdbcConverter converter = call.rel(1);

    for (AggregateCall relOptRuleOperand : agg.getAggCallList()) {
      SqlAggFunction f = relOptRuleOperand.getAggregation();
      if (f instanceof HiveSqlCountAggFunction) {
        //count distinct with more that one argument is not supported
        HiveSqlCountAggFunction countAgg = (HiveSqlCountAggFunction)f;
        if (countAgg.isDistinct() && 1 < relOptRuleOperand.getArgList().size()) {
          return false;
        }
      }
      SqlKind kind = f.getKind();
      if (!converter.getJdbcDialect().supportsAggregateFunction(kind)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LOG.debug("MyAggregationPushDownRule.onMatch has been called");

    final HiveAggregate agg = call.rel(0);
    final HiveJdbcConverter converter = call.rel(1);

    Aggregate newHiveAggregate = agg.copy(agg.getTraitSet(), converter.getInput(),
            agg.getIndicatorCount() !=0, agg.getGroupSet(), agg.getGroupSets(), agg.getAggCallList());
    JdbcAggregate newJdbcAggregate =
            (JdbcAggregate) new JdbcAggregateRule(converter.getJdbcConvention()).convert(newHiveAggregate);
    if (newJdbcAggregate != null) {
      RelNode converterRes = converter.copy(converter.getTraitSet(), Arrays.asList(newJdbcAggregate));

      call.transformTo(converterRes);
    }
  }

};
