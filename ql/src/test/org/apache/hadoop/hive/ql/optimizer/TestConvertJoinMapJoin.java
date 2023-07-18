package org.apache.hadoop.hive.ql.optimizer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;

import org.junit.Test;

public class TestConvertJoinMapJoin {

  @Test
  public void testHasOuterJoin1() throws SemanticException {
    JoinCondDesc[] condDesc = new JoinCondDesc[2];
    condDesc[0] = new JoinCondDesc(0, 1, JoinDesc.INNER_JOIN);
    condDesc[1] = new JoinCondDesc(1, 2, JoinDesc.LEFT_OUTER_JOIN);
    JoinDesc joinDesc = new JoinDesc();
    joinDesc.setConds(condDesc);
    CompilationOpContext cCtx = new CompilationOpContext();
    JoinOperator joinOperator = (JoinOperator) OperatorFactory.get(cCtx, joinDesc);
    assertTrue(ConvertJoinMapJoin.hasOuterJoin(joinOperator));
  }

  @Test
  public void testHasOuterJoin2() throws SemanticException {
    JoinCondDesc[] condDesc = new JoinCondDesc[2];
    condDesc[0] = new JoinCondDesc(0, 1, JoinDesc.LEFT_OUTER_JOIN);
    condDesc[1] = new JoinCondDesc(1, 2, JoinDesc.INNER_JOIN);
    JoinDesc joinDesc = new JoinDesc();
    joinDesc.setConds(condDesc);
    CompilationOpContext cCtx = new CompilationOpContext();
    JoinOperator joinOperator = (JoinOperator) OperatorFactory.get(cCtx, joinDesc);
    assertTrue(ConvertJoinMapJoin.hasOuterJoin(joinOperator));
  }

  @Test
  public void testHasOuterJoin3() throws SemanticException {
    JoinCondDesc[] condDesc = new JoinCondDesc[2];
    condDesc[0] = new JoinCondDesc(0, 1, JoinDesc.INNER_JOIN);
    condDesc[1] = new JoinCondDesc(1, 2, JoinDesc.INNER_JOIN);
    JoinDesc joinDesc = new JoinDesc();
    joinDesc.setConds(condDesc);
    CompilationOpContext cCtx = new CompilationOpContext();
    JoinOperator joinOperator = (JoinOperator) OperatorFactory.get(cCtx, joinDesc);
    assertFalse(ConvertJoinMapJoin.hasOuterJoin(joinOperator));
  }
}
