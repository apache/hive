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

package org.apache.hadoop.hive.ql.exec.vector;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterExprAndExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColEqualDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColGreaterLongColumn;
import org.junit.Assert;

import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.physical.Vectorizer;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorFilterDesc;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

/**
 * Test cases for vectorized filter operator.
 */
public class TestVectorFilterOperator extends TestVectorOperator{

  private VectorFilterOperator getAVectorFilterOperator() throws HiveException {
    ExprNodeColumnDesc col1Expr = new  ExprNodeColumnDesc(Long.class, "col1", "table", false);
    List<String> columns = new ArrayList<String>();
    columns.add("col1");
    FilterDesc fdesc = new FilterDesc();
    fdesc.setPredicate(col1Expr);
    VectorFilterDesc vectorDesc = new VectorFilterDesc();

    Operator<? extends OperatorDesc> filterOp =
        OperatorFactory.get(new CompilationOpContext(), fdesc);

    VectorizationContext vc = new VectorizationContext("name", columns);

    return (VectorFilterOperator) Vectorizer.vectorizeFilterOperator(filterOp, vc, vectorDesc);
  }

  @Test
  public void testBasicFilterOperator() throws HiveException {
    VectorFilterOperator vfo = getAVectorFilterOperator();
    prepareVectorFilterOperation(vfo);

    FakeDataReader fdr = new FakeDataReader(1024*1, 3, FakeDataSampleType.Random);

    VectorizedRowBatch vrg = fdr.getNext();

    vfo.getPredicateExpression().evaluate(vrg);

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
    prepareVectorFilterOperation(vfo);

    FakeDataReader fdr = new FakeDataReader(16*1024*1024, 3, FakeDataSampleType.Random);

    long startTime = System.currentTimeMillis();
    VectorizedRowBatch vrg = fdr.getNext();

    while (vrg.size > 0) {
      vfo.process(vrg, 0);
      vrg = fdr.getNext();
    }
    long endTime = System.currentTimeMillis();
    System.out.println("testBaseFilterOperator Op Time = "+(endTime-startTime));

    //Base time

    fdr = new FakeDataReader(16*1024*1024, 3, FakeDataSampleType.Random);

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

  @Test
  public void testVectorFilterHasSelectedSmallerThanBatchDoNotThrowException() throws HiveException {

    VectorFilterOperator vfo = getAVectorFilterOperator();

    FakeDataReader fdr = new FakeDataReader(1024*1, 3, FakeDataSampleType.OrderedSequence);

    prepareVectorFilterOperation(vfo);

    VectorizedRowBatch vrg = fdr.getNext();

    vrg.selected = new int[] { 1, 2, 3, 4};

    Assertions.assertDoesNotThrow(() -> vfo.process(vrg, 0));
  }

  private void prepareVectorFilterOperation(VectorFilterOperator vfo) throws HiveException {
    vfo.initialize(hiveConf, null);

    VectorExpression ve1 = new FilterLongColGreaterLongColumn(0,1);
    VectorExpression ve2 = new FilterLongColEqualDoubleScalar(2, 0);
    VectorExpression ve3 = new FilterExprAndExpr();
    ve3.setChildExpressions(new VectorExpression[] {ve1, ve2});

    vfo.setFilterCondition(ve3);
  }
}

