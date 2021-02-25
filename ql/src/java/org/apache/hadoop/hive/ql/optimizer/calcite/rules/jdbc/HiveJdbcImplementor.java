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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class HiveJdbcImplementor extends JdbcImplementor {

  public HiveJdbcImplementor(SqlDialect dialect, JavaTypeFactory typeFactory) {
    super(dialect, typeFactory);
  }

  @Override public Result visit(Project e) {
    // This variant keeps the column aliases instead of producing STAR
    Result x = visitInput(e, 0, Clause.SELECT);
    parseCorrelTable(e, x);
    final Builder builder = x.builder(e);
    final List<SqlNode> selectList = new ArrayList<>();
    for (RexNode ref : e.getProjects()) {
      SqlNode sqlExpr = builder.context.toSql(null, ref);
      addSelect(selectList, sqlExpr, e.getRowType());
    }

    builder.setSelect(new SqlNodeList(selectList, POS));
    return builder.result();
  }

  @Override public Result visit(Sort e) {
    Result x = visitInput(e, 0, Clause.ORDER_BY, Clause.FETCH, Clause.OFFSET);
    Builder builder = x.builder(e);
    List<SqlNode> orderByList = Expressions.list();
    for (RelFieldCollation field : e.getCollation().getFieldCollations()) {
      builder.addOrderItem(orderByList, field);
    }

    // Create select list as we want to keep the column aliases
    // instead of producing STAR
    final List<SqlNode> selectList = new ArrayList<>();
    for (int i = 0; i < e.getRowType().getFieldCount(); i++) {
      RexInputRef ref = RexInputRef.of(i, e.getRowType());
      SqlNode sqlExpr = builder.context.toSql(null, ref);
      addSelect(selectList, sqlExpr, e.getRowType());
    }
    builder.setSelect(new SqlNodeList(selectList, POS));

    if (!orderByList.isEmpty()) {
      builder.setOrderBy(new SqlNodeList(orderByList, POS));
      x = builder.result();
    }
    if (e.fetch != null) {
      builder = x.builder(e);
      builder.setFetch(builder.context.toSql(null, e.fetch));
      x = builder.result();
    }
    if (e.offset != null) {
      builder = x.builder(e);
      builder.setOffset(builder.context.toSql(null, e.offset));
      x = builder.result();
    }
    return x;
  }

  @Override public Result visit(Join e) {
    final Result leftResult = visitInput(e, 0).resetAlias();
    final Result rightResult = visitInput(e, 1).resetAlias();
    final Context leftContext = leftResult.qualifiedContext();
    final Context rightContext = rightResult.qualifiedContext();
    SqlNode sqlCondition = null;
    SqlLiteral condType = JoinConditionType.ON.symbol(POS);
    JoinType joinType = joinType(e.getJoinType());
    if (e.getJoinType() == JoinRelType.INNER && e.getCondition().isAlwaysTrue()) {
      joinType = JoinType.COMMA;
      condType = JoinConditionType.NONE.symbol(POS);
    } else {
      sqlCondition = convertConditionToSqlNode(e.getCondition(),
          leftContext,
          rightContext,
          e.getLeft().getRowType().getFieldCount());
    }
    SqlNode join =
        new SqlJoin(POS,
            leftResult.asFrom(),
            SqlLiteral.createBoolean(false, POS),
            joinType.symbol(POS),
            rightResult.asFrom(),
            condType,
            sqlCondition);
    return result(join, leftResult, rightResult);
  }

  /**
   * Converts a {@link RexNode} condition into a {@link SqlNode}.
   *
   * @param node            Join condition
   * @param leftContext     Left context
   * @param rightContext    Right context
   * @param leftFieldCount  Number of fields on left result
   * @return SqlNode that represents the condition
   */
  public static SqlNode convertConditionToSqlNode(RexNode node,
      Context leftContext, Context rightContext, int leftFieldCount) {
    // This variant replaces the default case for a call so it does not
    // throw an Exception
    if (node.isAlwaysTrue()) {
      return SqlLiteral.createBoolean(true, POS);
    }
    if (node.isAlwaysFalse()) {
      return SqlLiteral.createBoolean(false, POS);
    }
    if (node instanceof RexInputRef) {
      Context joinContext = leftContext.implementor().joinContext(leftContext, rightContext);
      return joinContext.toSql(null, node);
    }
    if (!(node instanceof RexCall)) {
      throw new AssertionError(node);
    }
    final List<RexNode> operands;
    final SqlOperator op;
    final Context joinContext;
    switch (node.getKind()) {
    case AND:
    case OR:
      operands = ((RexCall) node).getOperands();
      op = ((RexCall) node).getOperator();
      SqlNode sqlCondition = null;
      for (RexNode operand : operands) {
        SqlNode x = convertConditionToSqlNode(operand, leftContext,
            rightContext, leftFieldCount);
        if (sqlCondition == null) {
          sqlCondition = x;
        } else {
          sqlCondition = op.createCall(POS, sqlCondition, x);
        }
      }
      return sqlCondition;

    case EQUALS:
    case IS_NOT_DISTINCT_FROM:
    case NOT_EQUALS:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
      node = stripCastFromString(node);
      operands = ((RexCall) node).getOperands();
      op = ((RexCall) node).getOperator();
      if (operands.size() == 2
          && operands.get(0) instanceof RexInputRef
          && operands.get(1) instanceof RexInputRef) {
        final RexInputRef op0 = (RexInputRef) operands.get(0);
        final RexInputRef op1 = (RexInputRef) operands.get(1);

        if (op0.getIndex() < leftFieldCount
            && op1.getIndex() >= leftFieldCount) {
          // Arguments were of form 'op0 = op1'
          return op.createCall(POS,
              leftContext.field(op0.getIndex()),
              rightContext.field(op1.getIndex() - leftFieldCount));
        }
        if (op1.getIndex() < leftFieldCount
            && op0.getIndex() >= leftFieldCount) {
          // Arguments were of form 'op1 = op0'
          return reverseOperatorDirection(op).createCall(POS,
              leftContext.field(op1.getIndex()),
              rightContext.field(op0.getIndex() - leftFieldCount));
        }
      }
      joinContext =
          leftContext.implementor().joinContext(leftContext, rightContext);
      return joinContext.toSql(null, node);

    case IS_NULL:
    case IS_NOT_NULL:
      operands = ((RexCall) node).getOperands();
      if (operands.size() == 1
          && operands.get(0) instanceof RexInputRef) {
        op = ((RexCall) node).getOperator();
        final RexInputRef op0 = (RexInputRef) operands.get(0);
        if (op0.getIndex() < leftFieldCount) {
          return op.createCall(POS, leftContext.field(op0.getIndex()));
        } else {
          return op.createCall(POS,
              rightContext.field(op0.getIndex() - leftFieldCount));
        }
      }
      joinContext =
          leftContext.implementor().joinContext(leftContext, rightContext);
      return joinContext.toSql(null, node);

    default:
      joinContext =
          leftContext.implementor().joinContext(leftContext, rightContext);
      return joinContext.toSql(null, node);
    }
  }

  /** Removes cast from string.
   *
   * <p>For example, {@code x > CAST('2015-01-07' AS DATE)}
   * becomes {@code x > '2015-01-07'}.
   */
  private static RexNode stripCastFromString(RexNode node) {
    switch (node.getKind()) {
    case EQUALS:
    case IS_NOT_DISTINCT_FROM:
    case NOT_EQUALS:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
      final RexCall call = (RexCall) node;
      final RexNode o0 = call.operands.get(0);
      final RexNode o1 = call.operands.get(1);
      if (o0.getKind() == SqlKind.CAST
          && o1.getKind() != SqlKind.CAST) {
        final RexNode o0b = ((RexCall) o0).getOperands().get(0);
        switch (o0b.getType().getSqlTypeName()) {
        case CHAR:
        case VARCHAR:
          return call.clone(call.getType(), ImmutableList.of(o0b, o1));
        }
      }
      if (o1.getKind() == SqlKind.CAST
          && o0.getKind() != SqlKind.CAST) {
        final RexNode o1b = ((RexCall) o1).getOperands().get(0);
        switch (o1b.getType().getSqlTypeName()) {
        case CHAR:
        case VARCHAR:
          return call.clone(call.getType(), ImmutableList.of(o0, o1b));
        }
      }
    }
    return node;
  }

  private static SqlOperator reverseOperatorDirection(SqlOperator op) {
    switch (op.kind) {
    case GREATER_THAN:
      return SqlStdOperatorTable.LESS_THAN;
    case GREATER_THAN_OR_EQUAL:
      return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
    case LESS_THAN:
      return SqlStdOperatorTable.GREATER_THAN;
    case LESS_THAN_OR_EQUAL:
      return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
    case EQUALS:
    case IS_NOT_DISTINCT_FROM:
    case NOT_EQUALS:
      return op;
    default:
      throw new AssertionError(op);
    }
  }

  private void parseCorrelTable(RelNode relNode, Result x) {
    for (CorrelationId id : relNode.getVariablesSet()) {
      correlTableMap.put(id, x.qualifiedContext());
    }
  }

  @Override public Result implement(RelNode node) {
    return dispatch(node);
  }
}
