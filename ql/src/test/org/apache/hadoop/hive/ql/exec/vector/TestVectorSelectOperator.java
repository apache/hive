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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.vector.util.VectorizedRowGroupGenUtil;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPPlus;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;

/**
 * Unit tests for vectorized select operator.
 */
public class TestVectorSelectOperator {

  static class ValidatorVectorSelectOperator extends VectorSelectOperator {

    private static final long serialVersionUID = 1L;

    public ValidatorVectorSelectOperator(VectorizationContext ctxt, OperatorDesc conf)
        throws HiveException {
      super(ctxt, conf);
      initializeOp(null);
    }

    /**
     * Override forward to do validation
     */
    @Override
    public void forward(Object row, ObjectInspector rowInspector) throws HiveException {
      VectorizedRowBatch vrg = (VectorizedRowBatch) row;

      int[] projections = vrg.projectedColumns;
      assertEquals(2, vrg.projectionSize);
      assertEquals(3, projections[0]);
      assertEquals(2, projections[1]);

      LongColumnVector out0 = (LongColumnVector) vrg.cols[projections[0]];
      LongColumnVector out1 = (LongColumnVector) vrg.cols[projections[1]];

      LongColumnVector in0 = (LongColumnVector) vrg.cols[0];
      LongColumnVector in1 = (LongColumnVector) vrg.cols[1];
      LongColumnVector in2 = (LongColumnVector) vrg.cols[2];
      LongColumnVector in3 = (LongColumnVector) vrg.cols[3];

      for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
        assertEquals(in0.vector[i] + in1.vector[i], out0.vector[i]);
        assertEquals(in3.vector[i], out0.vector[i]);
        assertEquals(in2.vector[i], out1.vector[i]);
      }
    }
  }

  @Test
  public void testSelectOperator() throws HiveException {

    List<String> columns = new ArrayList<String>();
    columns.add("a");
    columns.add("b");
    columns.add("c");
    VectorizationContext vc = new VectorizationContext("name", columns);

    SelectDesc selDesc = new SelectDesc(false);
    List<ExprNodeDesc> colList = new ArrayList<ExprNodeDesc>();
    ExprNodeColumnDesc colDesc1 = new ExprNodeColumnDesc(Long.class, "a", "table", false);
    ExprNodeColumnDesc colDesc2 = new ExprNodeColumnDesc(Long.class, "b", "table", false);
    ExprNodeColumnDesc colDesc3 = new ExprNodeColumnDesc(Long.class, "c", "table", false);
    ExprNodeGenericFuncDesc plusDesc = new ExprNodeGenericFuncDesc();
    GenericUDF gudf = new GenericUDFOPPlus();

    plusDesc.setGenericUDF(gudf);
    List<ExprNodeDesc> children = new  ArrayList<ExprNodeDesc>();
    children.add(colDesc1);
    children.add(colDesc2);
    plusDesc.setChildren(children);
    plusDesc.setTypeInfo(TypeInfoFactory.longTypeInfo);

    colList.add(plusDesc);
    colList.add(colDesc3);
    selDesc.setColList(colList);

    List<String> outputColNames = new ArrayList<String>();
    outputColNames.add("_col0");
    outputColNames.add("_col1");
    selDesc.setOutputColumnNames(outputColNames);

    ValidatorVectorSelectOperator vso = new ValidatorVectorSelectOperator(vc, selDesc);

    VectorizedRowBatch vrg = VectorizedRowGroupGenUtil.getVectorizedRowBatch(
        VectorizedRowBatch.DEFAULT_SIZE, 4, 17);

    vso.process(vrg, 0);
  }

}
