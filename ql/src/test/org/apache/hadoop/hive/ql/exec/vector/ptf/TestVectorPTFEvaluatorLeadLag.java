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

package org.apache.hadoop.hive.ql.exec.vector.ptf;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ConstantVectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for VectorPTFEvaluatorLag and VectorPTFEvaluatorLead default value initialization.
 */
public class TestVectorPTFEvaluatorLeadLag {

  @Test
  public void testLagDoubleLiteralDefault() throws HiveException {
    TypeInfo doubleTypeInfo = TypeInfoFactory.doubleTypeInfo;
    ConstantVectorExpression constDouble = new ConstantVectorExpression(0, 2.5, doubleTypeInfo);
    VectorPTFEvaluatorLag evaluator = new VectorPTFEvaluatorLag(null, null, 1, Type.DOUBLE, 1, constDouble);

    // rowNum 0 with lag amt 1 is out of range (-1 < 0), returns default value
    Object result = evaluator.runOnRange(0, null, null);
    Assert.assertNotNull(result);
    Assert.assertEquals(2.5, (Double) result, 1e-6);
  }

  @Test
  public void testLeadDoubleLiteralDefault() throws HiveException {
    TypeInfo doubleTypeInfo = TypeInfoFactory.doubleTypeInfo;
    ConstantVectorExpression constDouble = new ConstantVectorExpression(0, 3.75, doubleTypeInfo);
    VectorPTFEvaluatorLead evaluator = new VectorPTFEvaluatorLead(null, null, 1, Type.DOUBLE, 1, constDouble);

    // batches.size() is 0 when null batches, or leadRow >= 0 when rowNum=0 and batches is mock/stub
    // VectorPTFEvaluatorLead: int leadRow = rowNum + amt; if (leadRow < 0 || leadRow >= batches.size())
    // With dummy batches having size 1, leadRow = 1 >= 1 -> returns default
    VectorPTFGroupBatches mockBatches = org.mockito.Mockito.mock(VectorPTFGroupBatches.class);
    org.mockito.Mockito.when(mockBatches.size()).thenReturn(1);

    Object result = evaluator.runOnRange(0, null, mockBatches);
    Assert.assertNotNull(result);
    Assert.assertEquals(3.75, (Double) result, 1e-6);
  }

  @Test
  public void testLagDecimalLiteralDefault() throws HiveException {
    TypeInfo decimalTypeInfo = TypeInfoFactory.getDecimalTypeInfo(10, 2);
    HiveDecimal decimalVal = HiveDecimal.create("2.50");
    ConstantVectorExpression constDecimal = new ConstantVectorExpression(0, decimalVal, decimalTypeInfo);
    VectorPTFEvaluatorLag evaluator = new VectorPTFEvaluatorLag(null, null, 1, Type.DECIMAL, 1, constDecimal);

    Object result = evaluator.runOnRange(0, null, null);
    Assert.assertNotNull(result);
    Assert.assertEquals(new HiveDecimalWritable(decimalVal), result);
  }

  @Test
  public void testLeadDecimalLiteralDefault() throws HiveException {
    TypeInfo decimalTypeInfo = TypeInfoFactory.getDecimalTypeInfo(10, 2);
    HiveDecimal decimalVal = HiveDecimal.create("12.34");
    ConstantVectorExpression constDecimal = new ConstantVectorExpression(0, decimalVal, decimalTypeInfo);
    VectorPTFEvaluatorLead evaluator = new VectorPTFEvaluatorLead(null, null, 1, Type.DECIMAL, 2, constDecimal);

    VectorPTFGroupBatches mockBatches = org.mockito.Mockito.mock(VectorPTFGroupBatches.class);
    org.mockito.Mockito.when(mockBatches.size()).thenReturn(2);

    Object result = evaluator.runOnRange(0, null, mockBatches);
    Assert.assertNotNull(result);
    Assert.assertEquals(new HiveDecimalWritable(decimalVal), result);
  }

  @Test
  public void testLagDecimalFromDoubleDefault() throws HiveException {
    TypeInfo doubleTypeInfo = TypeInfoFactory.doubleTypeInfo;
    ConstantVectorExpression constDouble = new ConstantVectorExpression(0, 2.5, doubleTypeInfo);
    VectorPTFEvaluatorLag evaluator = new VectorPTFEvaluatorLag(null, null, 1, Type.DECIMAL, 1, constDouble);

    Object result = evaluator.runOnRange(0, null, null);
    Assert.assertNotNull(result);
    HiveDecimalWritable expected = new HiveDecimalWritable();
    expected.setFromDouble(2.5);
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testLagLongLiteralDefault() throws HiveException {
    TypeInfo longTypeInfo = TypeInfoFactory.longTypeInfo;
    ConstantVectorExpression constLong = new ConstantVectorExpression(0, 100L, longTypeInfo);
    VectorPTFEvaluatorLag evaluator = new VectorPTFEvaluatorLag(null, null, 1, Type.LONG, 1, constLong);

    Object result = evaluator.runOnRange(0, null, null);
    Assert.assertNotNull(result);
    Assert.assertEquals(100L, result);
  }

  @Test
  public void testLagNullConstantDefault() throws HiveException {
    TypeInfo doubleTypeInfo = TypeInfoFactory.doubleTypeInfo;
    ConstantVectorExpression constNull = new ConstantVectorExpression(0, doubleTypeInfo, true);
    VectorPTFEvaluatorLag evaluator = new VectorPTFEvaluatorLag(null, null, 1, Type.DOUBLE, 1, constNull);

    Object result = evaluator.runOnRange(0, null, null);
    Assert.assertNull(result);
  }

  @Test
  public void testLagNullExpressionDefault() throws HiveException {
    VectorPTFEvaluatorLag evaluator = new VectorPTFEvaluatorLag(null, null, 1, Type.DOUBLE, 1, null);

    Object result = evaluator.runOnRange(0, null, null);
    Assert.assertNull(result);
  }
}
