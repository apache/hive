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

package org.apache.hadoop.hive.ql.exec.vector.udf;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.exec.vector.udf.generic.GenericUDFIsNull;
import org.apache.hadoop.hive.ql.exec.vector.udf.legacy.ConcatTextLongDoubleUDF;
import org.apache.hadoop.hive.ql.exec.vector.udf.legacy.LongUDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;

/*
 * Test the vectorized UDF adaptor to verify that custom legacy and generic
 * UDFs can be run in vectorized mode.
 */

public class TestVectorUDFAdaptor {

  static byte[] blue = null;
  static byte[] red = null;

  static {
    try {
      blue = "blue".getBytes("UTF-8");
      red = "red".getBytes("UTF-8");
    } catch (Exception e) {
      ; // do nothing
    }
  }

  @Test
  public void testLongUDF() throws HiveException {

    // create a syntax tree for a simple function call "longudf(col0)"
    ExprNodeGenericFuncDesc funcDesc;
    TypeInfo typeInfo = TypeInfoFactory.longTypeInfo;
    GenericUDFBridge genericUDFBridge = new GenericUDFBridge("longudf", false,
        LongUDF.class.getName());
    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    ExprNodeColumnDesc colDesc
       = new ExprNodeColumnDesc(typeInfo, "col0", "tablename", false);
    children.add(colDesc);
    VectorUDFArgDesc[] argDescs = new VectorUDFArgDesc[1];
    argDescs[0] = new VectorUDFArgDesc();
    argDescs[0].setVariable(0);
    funcDesc = new ExprNodeGenericFuncDesc(typeInfo, genericUDFBridge,
        genericUDFBridge.getUdfName(), children);

    // create the adaptor for this function call to work in vector mode
    VectorUDFAdaptor vudf = null;
    try {
      vudf = new VectorUDFAdaptor(funcDesc, 1, "Long", argDescs);
    } catch (HiveException e) {

      // We should never get here.
      assertTrue(false);
    }

    VectorizedRowBatch b = getBatchLongInLongOut();
    vudf.evaluate(b);

    // verify output
    LongColumnVector out = (LongColumnVector) b.cols[1];
    assertEquals(1000, out.vector[0]);
    assertEquals(1001, out.vector[1]);
    assertEquals(1002, out.vector[2]);
    assertTrue(out.noNulls);
    assertFalse(out.isRepeating);

    // with nulls
    b = getBatchLongInLongOut();
    out = (LongColumnVector) b.cols[1];
    b.cols[0].noNulls = false;
    vudf.evaluate(b);
    assertFalse(out.noNulls);
    assertEquals(1000, out.vector[0]);
    assertEquals(1001, out.vector[1]);
    assertTrue(out.isNull[2]);
    assertFalse(out.isRepeating);

    // with repeating
    b = getBatchLongInLongOut();
    out = (LongColumnVector) b.cols[1];
    b.cols[0].isRepeating = true;
    vudf.evaluate(b);

    // The implementation may or may not set output it isRepeting.
    // That is implementation-defined.
    assertTrue(b.cols[1].isRepeating && out.vector[0] == 1000
        || !b.cols[1].isRepeating && out.vector[2] == 1000);
    assertEquals(3, b.size);
  }

  @Test
  public void testMultiArgumentUDF() throws HiveException {

    // create a syntax tree for a function call "testudf(col0, col1, col2)"
    ExprNodeGenericFuncDesc funcDesc;
    TypeInfo typeInfoStr = TypeInfoFactory.stringTypeInfo;
    TypeInfo typeInfoLong = TypeInfoFactory.longTypeInfo;
    TypeInfo typeInfoDbl = TypeInfoFactory.doubleTypeInfo;
    GenericUDFBridge genericUDFBridge = new GenericUDFBridge("testudf", false,
        ConcatTextLongDoubleUDF.class.getName());
    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    children.add(new ExprNodeColumnDesc(typeInfoStr, "col0", "tablename", false));
    children.add(new ExprNodeColumnDesc(typeInfoLong, "col1", "tablename", false));
    children.add(new ExprNodeColumnDesc(typeInfoDbl, "col2", "tablename", false));

    VectorUDFArgDesc[] argDescs = new VectorUDFArgDesc[3];
    for (int i = 0; i < 3; i++) {
      argDescs[i] = new VectorUDFArgDesc();
      argDescs[i].setVariable(i);
    }
    funcDesc = new ExprNodeGenericFuncDesc(typeInfoStr, genericUDFBridge,
        genericUDFBridge.getUdfName(), children);

    // create the adaptor for this function call to work in vector mode
    VectorUDFAdaptor vudf = null;
    try {
      vudf = new VectorUDFAdaptor(funcDesc, 3, "String", argDescs);
    } catch (HiveException e) {

      // We should never get here.
      assertTrue(false);
      throw new RuntimeException(e);
    }

    // with no nulls
    VectorizedRowBatch b = getBatchStrDblLongWithStrOut();
    vudf.evaluate(b);
    byte[] result = null;
    byte[] result2 = null;
    try {
      result = "red:1:1.0".getBytes("UTF-8");
      result2 = "blue:0:0.0".getBytes("UTF-8");
    } catch (Exception e) {
      ;
    }
    BytesColumnVector out = (BytesColumnVector) b.cols[3];
    int cmp = StringExpr.compare(result, 0, result.length, out.vector[1],
        out.start[1], out.length[1]);
    assertEquals(0, cmp);
    assertTrue(out.noNulls);

    // with nulls
    b = getBatchStrDblLongWithStrOut();
    b.cols[1].noNulls = false;
    vudf.evaluate(b);
    out = (BytesColumnVector) b.cols[3];
    assertFalse(out.noNulls);
    assertTrue(out.isNull[1]);

    // with all input columns repeating
    b = getBatchStrDblLongWithStrOut();
    b.cols[0].isRepeating = true;
    b.cols[1].isRepeating = true;
    b.cols[2].isRepeating = true;
    vudf.evaluate(b);

    out = (BytesColumnVector) b.cols[3];
    assertTrue(out.isRepeating);
    cmp = StringExpr.compare(result2, 0, result2.length, out.vector[0],
        out.start[0], out.length[0]);
    assertEquals(0, cmp);
    assertTrue(out.noNulls);
  }

  private VectorizedRowBatch getBatchLongInLongOut() {
    VectorizedRowBatch b = new VectorizedRowBatch(2);
    LongColumnVector in = new LongColumnVector();
    LongColumnVector out = new LongColumnVector();
    b.cols[0] = in;
    b.cols[1] = out;
    in.vector[0] = 0;
    in.vector[1] = 1;
    in.vector[2] = 2;
    in.isNull[2] = true;
    in.noNulls = true;
    b.size = 3;
    return b;
  }

  private VectorizedRowBatch getBatchStrDblLongWithStrOut() {
    VectorizedRowBatch b = new VectorizedRowBatch(4);
    BytesColumnVector strCol = new BytesColumnVector();
    LongColumnVector longCol = new LongColumnVector();
    DoubleColumnVector dblCol = new DoubleColumnVector();
    BytesColumnVector outCol = new BytesColumnVector();
    b.cols[0] = strCol;
    b.cols[1] = longCol;
    b.cols[2] = dblCol;
    b.cols[3] = outCol;

    strCol.initBuffer();
    strCol.setVal(0, blue, 0, blue.length);
    strCol.setVal(1, red, 0, red.length);
    longCol.vector[0] = 0;
    longCol.vector[1] = 1;
    dblCol.vector[0] = 0.0;
    dblCol.vector[1] = 1.0;

    // set one null value for possible later use
    longCol.isNull[1] = true;

    // but have no nulls initially
    longCol.noNulls = true;
    strCol.noNulls = true;
    dblCol.noNulls = true;
    outCol.initBuffer();
    b.size = 2;
    return b;
  }


  // test the UDF adaptor for a generic UDF (as opposed to a legacy UDF)
  @Test
  public void testGenericUDF() throws HiveException {

    // create a syntax tree for a function call 'myisnull(col0, "UNKNOWN")'
    ExprNodeGenericFuncDesc funcDesc;
    GenericUDF genericUDF = new GenericUDFIsNull();
    TypeInfo typeInfoStr = TypeInfoFactory.stringTypeInfo;

    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    children.add(new ExprNodeColumnDesc(typeInfoStr, "col0", "tablename", false));
    children.add(new ExprNodeConstantDesc(typeInfoStr, "UNKNOWN"));

    VectorUDFArgDesc[] argDescs = new VectorUDFArgDesc[2];
    for (int i = 0; i < 2; i++) {
      argDescs[i] = new VectorUDFArgDesc();
    }
    argDescs[0].setVariable(0);
    argDescs[1].setConstant((ExprNodeConstantDesc) children.get(1));
    funcDesc = new ExprNodeGenericFuncDesc(typeInfoStr, genericUDF, "myisnull", children);

    // create the adaptor for this function call to work in vector mode
    VectorUDFAdaptor vudf = null;
    try {
      vudf = new VectorUDFAdaptor(funcDesc, 3, "String", argDescs);
    } catch (HiveException e) {

      // We should never get here.
      assertTrue(false);
    }

    VectorizedRowBatch b;

    byte[] red = null;
    byte[] unknown = null;
    try {
      red = "red".getBytes("UTF-8");
      unknown = "UNKNOWN".getBytes("UTF-8");
    } catch (Exception e) {
      ;
    }
    BytesColumnVector out;

    // with nulls
    b = getBatchStrDblLongWithStrOut();
    b.cols[0].noNulls = false;
    b.cols[0].isNull[0] = true; // set 1st entry to null
    vudf.evaluate(b);
    out = (BytesColumnVector) b.cols[3];

    // verify outputs
    int cmp = StringExpr.compare(red, 0, red.length,
        out.vector[1], out.start[1], out.length[1]);
    assertEquals(0, cmp);
    cmp = StringExpr.compare(unknown, 0, unknown.length,
        out.vector[0], out.start[0], out.length[0]);
    assertEquals(0, cmp);

    // output entry should not be null for null input for this particular generic UDF
    assertTrue(out.noNulls || !out.isNull[0]);
  }
}
