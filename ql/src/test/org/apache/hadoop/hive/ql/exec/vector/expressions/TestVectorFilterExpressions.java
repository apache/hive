package org.apache.hadoop.hive.ql.exec.vector.expressions;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColEqualLongScalar;
import org.junit.Test;

public class TestVectorFilterExpressions {

  @Test
  public void testFilterLongColEqualLongScalar() {
    VectorizedRowBatch vrg =
        VectorizedRowGroupGenUtil.getVectorizedRowBatch(1024, 2, 23);
    FilterLongColEqualLongScalar expr = new FilterLongColEqualLongScalar(1, 46);
    expr.evaluate(vrg);
    assertEquals(1, vrg.size);
    assertEquals(2, vrg.selected[0]);
  }

}
