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

import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcProject;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc.HiveJdbcConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBCProjectPushDownRule convert a {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject}
 * into a {@link JdbcProject}
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
      if (!validDataType(conv.getJdbcDialect(), currProject)) {
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

  /**
   * Returns whether a given expression contains only valid data types for this dialect.
   */
  private static boolean validDataType(SqlDialect dialect, RexNode e) {
    try {
      RexVisitor<Void> visitor = new JdbcDataTypeValidatorVisitor(dialect);
      e.accept(visitor);
      return true;
    } catch (Util.FoundOne ex) {
      Util.swallow(ex, null);
      return false;
    }
  }

  private static final class JdbcDataTypeValidatorVisitor extends RexVisitorImpl<Void> {
    private final SqlDialect dialect;

    private JdbcDataTypeValidatorVisitor(SqlDialect dialect) {
      super(true);
      this.dialect = dialect;
    }

    @Override public Void visitInputRef(RexInputRef inputRef) {
      if (!dialect.supportsDataType(inputRef.getType())) {
        throw Util.FoundOne.NULL;
      }
      return super.visitInputRef(inputRef);
    }

    @Override public Void visitLocalRef(RexLocalRef localRef) {
      if (!dialect.supportsDataType(localRef.getType())) {
        throw Util.FoundOne.NULL;
      }
      return super.visitLocalRef(localRef);
    }

    @Override public Void visitLiteral(RexLiteral literal) {
      if (!dialect.supportsDataType(literal.getType())) {
        throw Util.FoundOne.NULL;
      }
      return super.visitLiteral(literal);
    }

    @Override public Void visitCall(RexCall call) {
      if (!dialect.supportsDataType(call.getType())) {
        throw Util.FoundOne.NULL;
      }
      return super.visitCall(call);
    }

    @Override public Void visitOver(RexOver over) {
      if (!dialect.supportsDataType(over.getType())) {
        throw Util.FoundOne.NULL;
      }
      return super.visitOver(over);
    }

    @Override public Void visitFieldAccess(RexFieldAccess fieldAccess) {
      if (!dialect.supportsDataType(fieldAccess.getType())) {
        throw Util.FoundOne.NULL;
      }
      return super.visitFieldAccess(fieldAccess);
    }

  }

}
