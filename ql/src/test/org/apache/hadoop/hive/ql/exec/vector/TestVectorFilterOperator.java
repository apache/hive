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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FilterExprAndExpr;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColEqualDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColGreaterLongColumn;
import org.apache.hadoop.hive.ql.io.orc.ORCRowFilter;
import org.apache.hadoop.hive.ql.io.orc.TestOrcSplitElimination;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.physical.Vectorizer;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorFilterDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test cases for vectorized filter operator.
 */
public class TestVectorFilterOperator {

  HiveConf hconf = new HiveConf();

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
  public void testNonVectorUDFFilter() throws HiveException {
    // Create an Expr for <> 100
    ObjectInspector[] inputOIs = {
            PrimitiveObjectInspectorFactory.writableLongObjectInspector,
            PrimitiveObjectInspectorFactory.writableLongObjectInspector
    };
    GenericUDFOPNotEqual notEqual = new GenericUDFOPNotEqual();
    notEqual.initialize(inputOIs);

    // left is going to be our data and right the 100 constant
    LongWritable left = new LongWritable(100);
    LongWritable right = new LongWritable(100);
    GenericUDF.DeferredObject[] args = {
            new GenericUDF.DeferredJavaObject(left),
            new GenericUDF.DeferredJavaObject(right),
    };

    Assert.assertFalse(((BooleanWritable) notEqual.evaluate(args)).get());
  }

  @Test
  public void testRowLevelFilterCallback() throws Exception {
    // PPD is originally working with ExprNodeDesc
    List<ExprNodeDesc> childExpr = new ArrayList<>();
    childExpr.add(new ExprNodeColumnDesc(Long.class, "key2", "T", false));
    childExpr.add(new ExprNodeConstantDesc(50));

    GenericUDF udf = new GenericUDFOPGreaterThan();
    ObjectInspector oi =  ObjectInspectorFactory
            .getReflectionObjectInspector(TestOrcSplitElimination.AllTypesRow.class,
                    ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    ExprNodeDesc predicateExpr = new ExprNodeGenericFuncDesc(oi, udf, childExpr);
//    String serExpr = SerializationUtilities.serializeExpression((ExprNodeGenericFuncDesc) predicateExpr);
    System.out.println(predicateExpr);

//    VectorExpression ve1 = new FilterLongColGreaterLongScalar(0, 50);
//    List<String> columns = new ArrayList<String>();
//    columns.add("col1");
//    OrcInputFormat.RowLevelFilterCallback rb = new OrcInputFormat.RowLevelFilterCallback(ve1);

    ArrayList<String> allCols = new ArrayList<>();
    allCols.add("one");
    allCols.add("key2");
    allCols.add("three");
    // For row-level filtering we NEED to convert them to VectorExpression
    VectorizationContext vc = new VectorizationContext("row-filter", allCols);

    VectorExpression vectorPredicateExpr = vc.getVectorExpression(predicateExpr, VectorExpressionDescriptor.Mode.FILTER);
    System.out.println(vectorPredicateExpr);

    FakeDataReader fdr = new FakeDataReader(1024*1, 3);
    VectorizedRowBatch vrb = fdr.getNext();
    Assert.assertFalse(vrb.isSelectedInUse());

    // All the magic happens here..
    ORCRowFilter rb = new ORCRowFilter(predicateExpr, allCols);
    rb.rowFilterCallback(vrb);

    //Verify
    int rows = 0;
    int actualSelected[] = new int[1024];
    for (int i =0; i < 1024; i++){
      LongColumnVector l1 = (LongColumnVector) vrb.cols[0];
      LongColumnVector l2 = (LongColumnVector) vrb.cols[1];
      LongColumnVector l3 = (LongColumnVector) vrb.cols[2];
//      System.out.println("C1: "+ l1.vector[i] + " C2: "+ l2.vector[i] + " C3: "+ l3.vector[i]);
      if (l2.vector[i] > 50) {
        actualSelected[rows] = i;
        rows ++;
      }
    }

    Assert.assertTrue(vrb.isSelectedInUse());
    Assert.assertEquals(rows, vrb.size);

    // make sure selected is correct
    for (int i = 0; i < vrb.size; i++){
      Assert.assertEquals(vrb.selected[i], actualSelected[i]);
    }
  }

  @Test
  public void testBasicFilterOperator() throws HiveException {
    VectorFilterOperator vfo = getAVectorFilterOperator();
    vfo.initialize(hconf, null);
    VectorExpression ve1 = new FilterLongColGreaterLongColumn(0,1);
    VectorExpression ve2 = new FilterLongColEqualDoubleScalar(2, 0);
    VectorExpression ve3 = new FilterExprAndExpr();
    ve3.setChildExpressions(new VectorExpression[] {ve1, ve2});
    vfo.setFilterCondition(ve3);

    FakeDataReader fdr = new FakeDataReader(1024*1, 3);

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
    vfo.initialize(hconf, null);
    VectorExpression ve1 = new FilterLongColGreaterLongColumn(0,1);
    VectorExpression ve2 = new FilterLongColEqualDoubleScalar(2, 0);
    VectorExpression ve3 = new FilterExprAndExpr();
    ve3.setChildExpressions(new VectorExpression[] {ve1, ve2});
    vfo.setFilterCondition(ve3);

    FakeDataReader fdr = new FakeDataReader(16*1024*1024, 3);

    long startTime = System.currentTimeMillis();
    VectorizedRowBatch vrg = fdr.getNext();

    while (vrg.size > 0) {
      vfo.process(vrg, 0);
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

