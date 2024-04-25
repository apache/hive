package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.TestRuleBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestRelPlanParser extends TestRuleBase {

  @Mock
  private RelOptHiveTableFactory relOptHiveTableFactory;

  @Test
  public void testParse() throws IOException {
    RelNode ts1 = createTS(t1NativeMock, "t1");
    RelNode ts2 = createTS(t2NativeMock, "t2");

    RelBuilder relBuilder = HiveRelFactories.HIVE_BUILDER.create(relOptCluster, null);
    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
        REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(0).getType(), 0),
        REX_BUILDER.makeInputRef(ts2.getRowType().getFieldList().get(0).getType(), 5));

    RelNode planToSerialize = relBuilder
        .push(ts1)
        .filter(REX_BUILDER.makeCall(SqlStdOperatorTable.IS_NOT_NULL, REX_BUILDER.makeInputRef(ts1, 0)))
        .push(ts2)
        .filter(REX_BUILDER.makeCall(SqlStdOperatorTable.IS_NOT_NULL, REX_BUILDER.makeInputRef(ts2, 0)))
        .join(JoinRelType.INNER, joinCondition)
        .project(
            REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(0).getType(), 0),
            REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(1).getType(), 1),
            REX_BUILDER.makeInputRef(ts2.getRowType().getFieldList().get(0).getType(), ts1.getRowType().getFieldCount()))
        .build();

    // TODO: remove this
    System.out.println(RelOptUtil.toString(planToSerialize));

    String planJson = HiveRelOptUtil.asJSONObjectString(planToSerialize, false);

    // TODO: any() should be replaced with expected actual parameter values
    when(relOptHiveTableFactory.createRelOptHiveTable(eq("t1"), any(), any(), any(), any(), any())).thenReturn(t1NativeMock);
    when(relOptHiveTableFactory.createRelOptHiveTable(eq("t2"), any(), any(), any(), any(), any())).thenReturn(t2NativeMock);

    RelPlanParser parser = new RelPlanParser(relOptCluster, relOptHiveTableFactory, new HashMap<>());
    RelNode parsedPlan = parser.parse(planJson);

    verify(relOptHiveTableFactory, atLeastOnce()).createRelOptHiveTable(eq("t1"), any(), any(), any(), any(), any());
    verify(relOptHiveTableFactory, atLeastOnce()).createRelOptHiveTable(eq("t2"), any(), any(), any(), any(), any());

    // TODO: instead of printing the plan to the console so an assertion
    // The parsed plan must be identical with planToSerialize
    System.out.println(RelOptUtil.toString(parsedPlan));
  }
}