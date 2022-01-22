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

import java.util.ArrayList;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc.HiveJdbcConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * JDBCAbstractSplitFilterRule split a {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter} into
 * two {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter} operators where the lower operator
 * could be pushed down below the
 * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc.HiveJdbcConverter}}
 * operator and therefore could be sent to the external table.
 */

public abstract class JDBCAbstractSplitFilterRule extends RelOptRule {
  private static final Logger LOGGER = LoggerFactory.getLogger(JDBCAbstractSplitFilterRule.class);

  public static final JDBCAbstractSplitFilterRule SPLIT_FILTER_ABOVE_JOIN = new JDBCSplitFilterAboveJoinRule();
  public static final JDBCAbstractSplitFilterRule SPLIT_FILTER_ABOVE_CONVERTER = new JDBCSplitFilterRule();

  /**
   * FilterSupportedFunctionsVisitor traverse all of the Rex call and splits them into
   * two lists, one with supported jdbc calls, and one with not supported jdbc calls.
   */
  public static class FilterSupportedFunctionsVisitor extends RexVisitorImpl<Void> {

    private final SqlDialect dialect;

    public FilterSupportedFunctionsVisitor(SqlDialect dialect) {
      super(true);
      this.dialect = dialect;
    }

    private final ArrayList<RexCall> validJdbcNode = new ArrayList<RexCall>();
    private final ArrayList<RexCall> invalidJdbcNode = new ArrayList<RexCall>();

    public ArrayList<RexCall> getValidJdbcNode() {
      return validJdbcNode;
    }

    public ArrayList<RexCall> getInvalidJdbcNode() {
      return invalidJdbcNode;
    }

    @Override
    public Void visitCall(RexCall call) {
      if (call.getKind() == SqlKind.AND) {
        return super.visitCall(call);
      } else {
        boolean isValidCall = JDBCRexCallValidator.isValidJdbcOperation(call, dialect);
        if (isValidCall) {
          validJdbcNode.add(call);
        } else {
          invalidJdbcNode.add(call);
        }
      }
      return null;
    }

    public boolean canBeSplit() {
      return !validJdbcNode.isEmpty() && !invalidJdbcNode.isEmpty();
    }
  }

  protected JDBCAbstractSplitFilterRule(RelOptRuleOperand operand) {
    super(operand);
  }

  public static boolean canSplitFilter(RexNode cond, SqlDialect dialect) {
    FilterSupportedFunctionsVisitor visitor = new FilterSupportedFunctionsVisitor(dialect);
    cond.accept(visitor);
    return visitor.canBeSplit();
  }

  public void onMatch(RelOptRuleCall call, SqlDialect dialect) {
    LOGGER.debug("MySplitFilter.onMatch has been called");

    final HiveFilter        filter = call.rel(0);

    RexCall callExpression = (RexCall) filter.getCondition();

    FilterSupportedFunctionsVisitor visitor = new FilterSupportedFunctionsVisitor(dialect);
    callExpression.accept(visitor);

    ArrayList<RexCall> validJdbcNode = visitor.getValidJdbcNode();
    ArrayList<RexCall> invalidJdbcNode = visitor.getInvalidJdbcNode();

    assert validJdbcNode.size() != 0 && invalidJdbcNode.size() != 0;

    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();

    RexNode validCondition;
    if (validJdbcNode.size() == 1) {
      validCondition = validJdbcNode.get(0);
    } else {
      validCondition = rexBuilder.makeCall(SqlStdOperatorTable.AND, validJdbcNode);
    }

    HiveFilter newJdbcValidFilter = new HiveFilter(filter.getCluster(), filter.getTraitSet(), filter.getInput(),
            validCondition);

    RexNode invalidCondition;
    if (invalidJdbcNode.size() == 1) {
      invalidCondition = invalidJdbcNode.get(0);
    } else {
      invalidCondition = rexBuilder.makeCall(SqlStdOperatorTable.AND, invalidJdbcNode);
    }

    HiveFilter newJdbcInvalidFilter = new HiveFilter(filter.getCluster(), filter.getTraitSet(),
                                                     newJdbcValidFilter, invalidCondition);

    call.transformTo(newJdbcInvalidFilter);
  }

  /**
   * JDBCSplitFilterAboveJoinRule split splitter above a HiveJoin operator, so we could push it into the HiveJoin.
   */
  public static class JDBCSplitFilterAboveJoinRule extends JDBCAbstractSplitFilterRule {
    public JDBCSplitFilterAboveJoinRule() {
      super(operand(HiveFilter.class,
              operand(HiveJoin.class,
                  operand(HiveJdbcConverter.class, any()))));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      LOGGER.debug("MyUpperJoinFilterFilter.matches has been called");

      final HiveFilter filter = call.rel(0);
      final HiveJoin join = call.rel(1);
      final HiveJdbcConverter conv = call.rel(2);

      RexNode joinCond = join.getCondition();
      SqlDialect dialect = conv.getJdbcDialect();

      return canSplitFilter(filter.getCondition(), dialect)
        && JDBCRexCallValidator.isValidJdbcOperation(joinCond, dialect);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HiveJdbcConverter conv = call.rel(2);
      super.onMatch(call, conv.getJdbcDialect());
    }
  }

  /**
   * JDBCSplitFilterRule splits a HiveFilter rule so we could push part of the HiveFilter into the jdbc.
   */
  public static class JDBCSplitFilterRule extends JDBCAbstractSplitFilterRule {
    public JDBCSplitFilterRule() {
      super(operand(HiveFilter.class,
              operand(HiveJdbcConverter.class, any())));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      final HiveFilter filter = call.rel(0);
      final HiveJdbcConverter conv = call.rel(1);
      return canSplitFilter(filter.getCondition(), conv.getJdbcDialect());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HiveJdbcConverter conv = call.rel(1);
      super.onMatch(call, conv.getJdbcDialect());
    }
  }

};
