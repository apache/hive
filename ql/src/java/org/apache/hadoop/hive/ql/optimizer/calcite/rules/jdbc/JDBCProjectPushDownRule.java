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

import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcProject;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcProjectRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc.HiveJdbcConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBCProjectPushDownRule convert a {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject}
 * into a {@link org.apache.calcite.adapter.jdbc.JdbcRules.JdbcAggregateRule.JdbcProject}
 * and pushes it down below the {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc.HiveJdbcConverter}}
 * operator so it will be sent to the external table.
 */

public class JDBCProjectPushDownRule extends RelOptRule {
  private static final Logger LOG = LoggerFactory.getLogger(JDBCProjectPushDownRule.class);

  public static final JDBCProjectPushDownRule INSTANCE = new JDBCProjectPushDownRule();

  public JDBCProjectPushDownRule() {
    super(operand(HiveProject.class,
            operand(HiveJdbcConverter.class, any())));
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final HiveProject project = call.rel(0);
    final HiveJdbcConverter conv = call.rel(1);
    for (RexNode currProject : project.getProjects()) {
      if (!JDBCRexCallValidator.isValidJdbcOperation(currProject, conv.getJdbcDialect())) {
        return false;
      }
    }

    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LOG.debug("JDBCProjectPushDownRule has been called");

    final HiveProject project = call.rel(0);
    final HiveJdbcConverter converter = call.rel(1);

    JdbcProject jdbcProject = new JdbcProject(
        project.getCluster(),
        project.getTraitSet().replace(converter.getJdbcConvention()),
        converter.getInput(),
        project.getProjects(),
        project.getRowType());

    call.transformTo(converter.copy(converter.getTraitSet(), jdbcProject));
  }

};
