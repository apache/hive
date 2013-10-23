/**
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

package org.apache.hadoop.hive.ql.exec.vector;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterExprAndExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColEqualDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColGreaterLongColumn;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.junit.Test;

/**
 * Test cases for vectorized filter operator.
 */
public class TestVectorFilterOperator {

  /**
   * Fundamental logic and performance tests for vector filters belong here.
   *
   * For tests about filters to cover specific operator and data type combinations,
   * see also the other filter tests under org.apache.hadoop.hive.ql.exec.vector.expressions
   */
  public static class FakeDataReader {
    private final int size;
    private final VectorizedRowBatch vrg;
    private int currentSize = 0;
    private final int numCols;
    private final int len = 1024;

    public FakeDataReader(int size, int numCols) {
      this.size = size;
      this.numCols = numCols;
      vrg = new VectorizedRowBatch(numCols, len);
      for (int i = 0; i < numCols; i++) {
        try {
          Thread.sleep(2);
        } catch (InterruptedException ignore) {}
        vrg.cols[i] = getLongVector(len);
      }
    }

    public VectorizedRowBatch getNext() {
      if (currentSize >= size) {
        vrg.size = 0;
        return vrg;
      } else {
        vrg.size = len;
        currentSize += vrg.size;
        vrg.selectedInUse = false;
        return vrg;
      }
    }

    private LongColumnVector getLongVector(int len) {
      LongColumnVector lcv = new LongColumnVector(len);
      TestVectorizedRowBatch.setRandomLongCol(lcv);
      return lcv;
    }
  }

  private VectorFilterOperator getAVectorFilterOperator() throws HiveException {
    ExprNodeColumnDesc col1Expr = new  ExprNodeColumnDesc(Long.class, "col1", "table", false);
    Map<String, Integer> columnMap = new HashMap<String, Integer>();
    columnMap.put("col1", 1);
    VectorizationContext vc = new VectorizationContext(columnMap, 1);
    FilterDesc fdesc = new FilterDesc();
    fdesc.setPredicate(col1Expr);
    return new VectorFilterOperator(vc, fdesc);
  }

  @Test
  public void testBasicFilterOperator() throws HiveException {
    VectorFilterOperator vfo = getAVectorFilterOperator();
    VectorExpression ve1 = new FilterLongColGreaterLongColumn(0,1);
    VectorExpression ve2 = new FilterLongColEqualDoubleScalar(2, 0);
    VectorExpression ve3 = new FilterExprAndExpr();
    ve3.setChildExpressions(new VectorExpression[] {ve1, ve2});
    vfo.setFilterCondition(ve3);

    FakeDataReader fdr = new FakeDataReader(1024*1, 3);

    VectorizedRowBatch vrg = fdr.getNext();

    vfo.processOp(vrg, 0);

    //Verify
    int rows = 0;
    for (int i =0; i < 1024; i++){
      LongColumnVector l1 = (LongColumnVector) vrg.cols[0];
      LongColumnVector l2 = (LongColumnVector) vrg.cols[1];
      LongColumnVector l3 = (LongColumnVector) vrg.cols[2];
      if ((l1.vector[i] > l2.vector[i]) && (l3.vector[i] == 0)) {
        rows ++;
      }
    }
    Assert.assertEquals(rows, vrg.size);
  }

  @Test
  public void testBasicFilterLargeData() throws HiveException {
    VectorFilterOperator vfo = getAVectorFilterOperator();
    VectorExpression ve1 = new FilterLongColGreaterLongColumn(0,1);
    VectorExpression ve2 = new FilterLongColEqualDoubleScalar(2, 0);
    VectorExpression ve3 = new FilterExprAndExpr();
    ve3.setChildExpressions(new VectorExpression[] {ve1, ve2});
    vfo.setFilterCondition(ve3);

    FakeDataReader fdr = new FakeDataReader(16*1024*1024, 3);

    long startTime = System.currentTimeMillis();
    VectorizedRowBatch vrg = fdr.getNext();

    while (vrg.size > 0) {
      vfo.processOp(vrg, 0);
      vrg = fdr.getNext();
    }
    long endTime = System.currentTimeMillis();
    System.out.println("testBaseFilterOperator Op Time = "+(endTime-startTime));

    //Base time

    fdr = new FakeDataReader(16*1024*1024, 3);

    long startTime1 = System.currentTimeMillis();
    vrg = fdr.getNext();
    LongColumnVector l1 = (LongColumnVector) vrg.cols[0];
    LongColumnVector l2 = (LongColumnVector) vrg.cols[1];
    LongColumnVector l3 = (LongColumnVector) vrg.cols[2];
    int rows = 0;
    for (int j =0; j < 16 *1024; j++) {
      for (int i = 0; i < l1.vector.length && i < l2.vector.length && i < l3.vector.length; i++) {
        if ((l1.vector[i] > l2.vector[i]) && (l3.vector[i] == 0)) {
          rows++;
        }
      }
    }
    long endTime1 = System.currentTimeMillis();
    System.out.println("testBaseFilterOperator base Op Time = "+(endTime1-startTime1));
  }
}

