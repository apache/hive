package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class TestHiveRowIsDeletedPropagator extends TestRuleBase {
  @Test
  public void test() {
    RelNode ts1 = createTS(t1NativeMock, "t1");
    RelNode ts2 = createTS(t2NativeMock, "t2");
    RelNode ts3 = createTS(t3NativeMock, "t3");

    RelBuilder relBuilder = HiveRelFactories.HIVE_BUILDER.create(relOptCluster, null);

    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
        REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(0).getType(), 0),
        REX_BUILDER.makeInputRef(ts2.getRowType().getFieldList().get(0).getType(), 5));
    RelNode join1 = relBuilder
        .push(ts1)
        .filter(REX_BUILDER.makeCall(SqlStdOperatorTable.IS_NOT_NULL, REX_BUILDER.makeInputRef(ts1, 0)))
        .push(ts2)
        .filter(REX_BUILDER.makeCall(SqlStdOperatorTable.IS_NOT_NULL, REX_BUILDER.makeInputRef(ts2, 0)))
        .join(JoinRelType.INNER, joinCondition)
        .build();

    RexNode joinCondition2 = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
        REX_BUILDER.makeInputRef(ts3.getRowType().getFieldList().get(0).getType(), 10),
        REX_BUILDER.makeInputRef(join1.getRowType().getFieldList().get(5).getType(), 5));

    RelDataType bigIntType = relBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);

    RexNode writeIdFilter = REX_BUILDER.makeCall(SqlStdOperatorTable.OR,
        REX_BUILDER.makeCall(SqlStdOperatorTable.LESS_THAN, REX_BUILDER.makeLiteral(1, bigIntType, false), rowIdFieldAccess(ts1, 3)),
        REX_BUILDER.makeCall(SqlStdOperatorTable.LESS_THAN, REX_BUILDER.makeLiteral(1, bigIntType, false), rowIdFieldAccess(ts2, 8)),
        REX_BUILDER.makeCall(SqlStdOperatorTable.LESS_THAN, REX_BUILDER.makeLiteral(1, bigIntType, false), rowIdFieldAccess(ts3, 13)));

    RelNode root = relBuilder
        .push(join1)
        .push(ts3)
        .filter(REX_BUILDER.makeCall(SqlStdOperatorTable.IS_NOT_NULL, REX_BUILDER.makeInputRef(ts3, 0)))
        .join(JoinRelType.INNER, joinCondition2)
        .filter(writeIdFilter)
        .build();

    System.out.println(RelOptUtil.toString(root));

    HiveRowIsDeletedPropagator2 propagator = new HiveRowIsDeletedPropagator2(relBuilder);
    RelNode newRoot = propagator.propagate(root);

    System.out.println(RelOptUtil.toString(newRoot));
  }

  private RexNode rowIdFieldAccess(RelNode tableScan, int posInTarget) {
    int rowIDPos = tableScan.getTable().getRowType().getField(
        VirtualColumn.ROWID.getName(), false, false).getIndex();
    return REX_BUILDER.makeFieldAccess(REX_BUILDER.makeInputRef(
        tableScan.getTable().getRowType().getFieldList().get(rowIDPos).getType(), posInTarget), 0);
  }
}