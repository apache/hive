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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import static org.junit.Assert.*;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprDoubleColumnDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprLongColumnLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprLongScalarLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprLongScalarLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprDoubleScalarDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprDoubleScalarDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprDoubleColumnDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprStringGroupColumnStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprStringGroupColumnStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprStringScalarStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprStringScalarStringScalar;

import org.junit.Test;

/**
 * Test vectorized conditional expression handling.
 */
public class TestVectorConditionalExpressions {

  private VectorizedRowBatch getBatch4LongVectors() {
    VectorizedRowBatch batch = new VectorizedRowBatch(4);
    LongColumnVector v = new LongColumnVector();

    // set first argument to IF -- boolean flag
    v.vector[0] = 0;
    v.vector[1] = 0;
    v.vector[2] = 1;
    v.vector[3] = 1;
    batch.cols[0] = v;

    // set second argument to IF
    v = new LongColumnVector();
    v.vector[0] = -1;
    v.vector[1] = -2;
    v.vector[2] = -3;
    v.vector[3] = -4;
    batch.cols[1] = v;

    // set third argument to IF
    v = new LongColumnVector();
    v.vector[0] = 1;
    v.vector[1] = 2;
    v.vector[2] = 3;
    v.vector[3] = 4;
    batch.cols[2] = v;

    // set output column
    batch.cols[3] = new LongColumnVector();

    batch.size = 4;
    return batch;
  }

  private VectorizedRowBatch getBatch1Long3DoubleVectors() {
    VectorizedRowBatch batch = new VectorizedRowBatch(4);
    LongColumnVector lv = new LongColumnVector();

    // set first argument to IF -- boolean flag
    lv.vector[0] = 0;
    lv.vector[1] = 0;
    lv.vector[2] = 1;
    lv.vector[3] = 1;
    batch.cols[0] = lv;

    // set second argument to IF
    DoubleColumnVector v = new DoubleColumnVector();
    v.vector[0] = -1;
    v.vector[1] = -2;
    v.vector[2] = -3;
    v.vector[3] = -4;
    batch.cols[1] = v;

    // set third argument to IF
    v = new DoubleColumnVector();
    v.vector[0] = 1;
    v.vector[1] = 2;
    v.vector[2] = 3;
    v.vector[3] = 4;
    batch.cols[2] = v;

    // set output column
    batch.cols[3] = new DoubleColumnVector();

    batch.size = 4;
    return batch;
  }

  private VectorizedRowBatch getBatch1Long3BytesVectors() {
    VectorizedRowBatch batch = new VectorizedRowBatch(4);
    LongColumnVector lv = new LongColumnVector();

    // set first argument to IF -- boolean flag
    lv.vector[0] = 0;
    lv.vector[1] = 0;
    lv.vector[2] = 1;
    lv.vector[3] = 1;
    batch.cols[0] = lv;

    // set second argument to IF
    BytesColumnVector v = new BytesColumnVector();
    v.initBuffer();
    setString(v, 0, "arg2_0");
    setString(v, 1, "arg2_1");
    setString(v, 2, "arg2_2");
    setString(v, 3, "arg2_3");

    batch.cols[1] = v;

    // set third argument to IF
    v = new BytesColumnVector();
    v.initBuffer();
    setString(v, 0, "arg3_0");
    setString(v, 1, "arg3_1");
    setString(v, 2, "arg3_2");
    setString(v, 3, "arg3_3");
    batch.cols[2] = v;

    // set output column
    v = new BytesColumnVector();
    v.initBuffer();
    batch.cols[3] = v;
    batch.size = 4;
    return batch;
  }

  private void setString(BytesColumnVector v, int i, String s) {
    byte[] b = getUTF8Bytes(s);
    v.setVal(i, b, 0, b.length);
  }

  private byte[] getUTF8Bytes(String s) {
    byte[] b = null;
    try {
      b = s.getBytes("UTF-8");
    } catch (Exception e) {
      ; // eat it
    }
    return b;
  }

  private String getString(BytesColumnVector v, int i) {
    String s = null;
    try {
      s = new String(v.vector[i], v.start[i], v.length[i], "UTF-8");
    } catch (Exception e) {
      ; // eat it
    }
    return s;
  }

  @Test
  public void testLongColumnColumnIfExpr()  {
    VectorizedRowBatch batch = getBatch4LongVectors();
    VectorExpression expr = new IfExprLongColumnLongColumn(0, 1, 2, 3);
    expr.evaluate(batch);

    // get result vector
    LongColumnVector r = (LongColumnVector) batch.cols[3];

    // verify standard case
    assertEquals(1, r.vector[0]);
    assertEquals(2, r.vector[1]);
    assertEquals(-3, r.vector[2]);
    assertEquals(-4, r.vector[3]);
    assertEquals(false, r.isRepeating);

    // verify when first argument (boolean flags) is repeating
    batch = getBatch4LongVectors();
    r = (LongColumnVector) batch.cols[3];
    batch.cols[0].isRepeating = true;
    expr.evaluate(batch);
    assertEquals(1, r.vector[0]);
    assertEquals(4, r.vector[3]);

    // verify when second argument is repeating
    batch = getBatch4LongVectors();
    r = (LongColumnVector) batch.cols[3];
    batch.cols[1].isRepeating = true;
    expr.evaluate(batch);
    assertEquals(1, r.vector[0]);
    assertEquals(2, r.vector[1]);
    assertEquals(-1, r.vector[2]);
    assertEquals(-1, r.vector[3]);

    // verify when third argument is repeating
    batch = getBatch4LongVectors();
    r = (LongColumnVector) batch.cols[3];
    batch.cols[2].isRepeating = true;
    expr.evaluate(batch);
    assertEquals(1, r.vector[0]);
    assertEquals(1, r.vector[1]);
    assertEquals(-3, r.vector[2]);
    assertEquals(-4, r.vector[3]);

    // test when first argument has nulls
    batch = getBatch4LongVectors();
    r = (LongColumnVector) batch.cols[3];
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[1] = true;
    batch.cols[0].isNull[2] = true;
    expr.evaluate(batch);
    assertEquals(1, r.vector[0]);
    assertEquals(2, r.vector[1]);
    assertEquals(3, r.vector[2]);
    assertEquals(-4, r.vector[3]);
    assertEquals(false, r.isRepeating);

    // test when second argument has nulls
    batch = getBatch4LongVectors();
    r = (LongColumnVector) batch.cols[3];
    batch.cols[1].noNulls = false;
    batch.cols[1].isNull[1] = true;
    batch.cols[1].isNull[2] = true;
    expr.evaluate(batch);
    assertEquals(1, r.vector[0]);
    assertEquals(2, r.vector[1]);
    assertEquals(true, r.isNull[2]);
    assertEquals(-4, r.vector[3]);
    assertEquals(false, r.noNulls);
    assertEquals(false, r.isRepeating);

    // test when third argument has nulls
    batch = getBatch4LongVectors();
    r = (LongColumnVector) batch.cols[3];
    batch.cols[2].noNulls = false;
    batch.cols[2].isNull[1] = true;
    batch.cols[2].isNull[2] = true;
    expr.evaluate(batch);
    assertEquals(1, r.vector[0]);
    assertEquals(true, r.isNull[1]);
    assertEquals(-3, r.vector[2]);
    assertEquals(-4, r.vector[3]);
    assertEquals(false, r.noNulls);
    assertEquals(false, r.isRepeating);


    // test when second argument has nulls and repeats
    batch = getBatch4LongVectors();
    r = (LongColumnVector) batch.cols[3];
    batch.cols[1].noNulls = false;
    batch.cols[1].isNull[0] = true;
    batch.cols[1].isRepeating = true;
    expr.evaluate(batch);
    assertEquals(1, r.vector[0]);
    assertEquals(2, r.vector[1]);
    assertEquals(true, r.isNull[2]);
    assertEquals(true, r.isNull[3]);
    assertEquals(false, r.noNulls);
    assertEquals(false, r.isRepeating);

    // test when third argument has nulls and repeats
    batch = getBatch4LongVectors();
    r = (LongColumnVector) batch.cols[3];
    batch.cols[2].noNulls = false;
    batch.cols[2].isNull[0] = true;
    batch.cols[2].isRepeating = true;
    expr.evaluate(batch);
    assertEquals(true, r.isNull[0]);
    assertEquals(true, r.isNull[1]);
    assertEquals(-3, r.vector[2]);
    assertEquals(-4, r.vector[3]);
    assertEquals(false, r.noNulls);
    assertEquals(false, r.isRepeating);
  }

  @Test
  public void testDoubleColumnColumnIfExpr()  {
    // Just spot check because we already checked the logic for long.
    // The code is from the same template file.

    VectorizedRowBatch batch = getBatch1Long3DoubleVectors();
    VectorExpression expr = new IfExprDoubleColumnDoubleColumn(0, 1, 2, 3);
    expr.evaluate(batch);

    // get result vector
    DoubleColumnVector r = (DoubleColumnVector) batch.cols[3];

    // verify standard case
    assertEquals(true, 1d == r.vector[0]);
    assertEquals(true, 2d == r.vector[1]);
    assertEquals(true, -3d == r.vector[2]);
    assertEquals(true, -4d == r.vector[3]);
    assertEquals(false, r.isRepeating);
  }

  @Test
  public void testLongColumnScalarIfExpr() {
    VectorizedRowBatch batch = getBatch4LongVectors();
    VectorExpression expr = new IfExprLongColumnLongScalar(0, 1, 100, 3);
    LongColumnVector r = (LongColumnVector) batch.cols[3];
    expr.evaluate(batch);
    assertEquals(100, r.vector[0]);
    assertEquals(100, r.vector[1]);
    assertEquals(-3, r.vector[2]);
    assertEquals(-4, r.vector[3]);
  }

  @Test
  public void testLongScalarColumnIfExpr() {
    VectorizedRowBatch batch = getBatch4LongVectors();
    VectorExpression expr = new IfExprLongScalarLongColumn(0, 100, 2, 3);
    LongColumnVector r = (LongColumnVector) batch.cols[3];
    expr.evaluate(batch);
    assertEquals(1, r.vector[0]);
    assertEquals(2, r.vector[1]);
    assertEquals(100, r.vector[2]);
    assertEquals(100, r.vector[3]);
  }

  @Test
  public void testLongScalarScalarIfExpr() {
    VectorizedRowBatch batch = getBatch4LongVectors();
    VectorExpression expr = new IfExprLongScalarLongScalar(0, 100, 200, 3);
    LongColumnVector r = (LongColumnVector) batch.cols[3];
    expr.evaluate(batch);
    assertEquals(200, r.vector[0]);
    assertEquals(200, r.vector[1]);
    assertEquals(100, r.vector[2]);
    assertEquals(100, r.vector[3]);
  }

  @Test
  public void testDoubleScalarScalarIfExpr() {
    VectorizedRowBatch batch = getBatch1Long3DoubleVectors();
    VectorExpression expr = new IfExprDoubleScalarDoubleScalar(0, 100.0d, 200.0d, 3);
    DoubleColumnVector r = (DoubleColumnVector) batch.cols[3];
    expr.evaluate(batch);
    assertEquals(true, 200d == r.vector[0]);
    assertEquals(true, 200d == r.vector[1]);
    assertEquals(true, 100d == r.vector[2]);
    assertEquals(true, 100d == r.vector[3]);
  }

  @Test
  public void testDoubleScalarColumnIfExpr() {
    VectorizedRowBatch batch = getBatch1Long3DoubleVectors();
    VectorExpression expr = new IfExprDoubleScalarDoubleColumn(0, 100.0d, 2, 3);
    DoubleColumnVector r = (DoubleColumnVector) batch.cols[3];
    expr.evaluate(batch);
    assertEquals(true, 1d == r.vector[0]);
    assertEquals(true, 2d == r.vector[1]);
    assertEquals(true, 100d == r.vector[2]);
    assertEquals(true, 100d == r.vector[3]);
  }

  @Test
  public void testDoubleColumnScalarIfExpr() {
    VectorizedRowBatch batch = getBatch1Long3DoubleVectors();
    VectorExpression expr = new IfExprDoubleColumnDoubleScalar(0, 1, 200d, 3);
    DoubleColumnVector r = (DoubleColumnVector) batch.cols[3];
    expr.evaluate(batch);
    assertEquals(true, 200d == r.vector[0]);
    assertEquals(true, 200d == r.vector[1]);
    assertEquals(true, -3d == r.vector[2]);
    assertEquals(true, -4d == r.vector[3]);
  }

  @Test
  public void testIfExprStringColumnStringColumn() {
    VectorizedRowBatch batch = getBatch1Long3BytesVectors();
    VectorExpression expr = new IfExprStringGroupColumnStringGroupColumn(0, 1, 2, 3);
    BytesColumnVector r = (BytesColumnVector) batch.cols[3];
    expr.evaluate(batch);
    assertTrue(getString(r, 0).equals("arg3_0"));
    assertTrue(getString(r, 1).equals("arg3_1"));
    assertTrue(getString(r, 2).equals("arg2_2"));
    assertTrue(getString(r, 3).equals("arg2_3"));

    // test first IF argument repeating
    batch = getBatch1Long3BytesVectors();
    batch.cols[0].isRepeating = true;
    r = (BytesColumnVector) batch.cols[3];
    expr.evaluate(batch);
    assertTrue(getString(r, 0).equals("arg3_0"));
    assertTrue(getString(r, 1).equals("arg3_1"));
    assertTrue(getString(r, 2).equals("arg3_2"));
    assertTrue(getString(r, 3).equals("arg3_3"));

    // test second IF argument repeating
    batch = getBatch1Long3BytesVectors();
    batch.cols[1].isRepeating = true;
    r = (BytesColumnVector) batch.cols[3];
    expr.evaluate(batch);
    assertTrue(getString(r, 0).equals("arg3_0"));
    assertTrue(getString(r, 1).equals("arg3_1"));
    assertTrue(getString(r, 2).equals("arg2_0"));
    assertTrue(getString(r, 3).equals("arg2_0"));

    // test third IF argument repeating
    batch = getBatch1Long3BytesVectors();
    batch.cols[2].isRepeating = true;
    r = (BytesColumnVector) batch.cols[3];
    expr.evaluate(batch);
    assertTrue(getString(r, 0).equals("arg3_0"));
    assertTrue(getString(r, 1).equals("arg3_0"));
    assertTrue(getString(r, 2).equals("arg2_2"));
    assertTrue(getString(r, 3).equals("arg2_3"));

    // test second IF argument with nulls
    batch = getBatch1Long3BytesVectors();
    batch.cols[1].noNulls = false;
    batch.cols[1].isNull[2] = true;

    // set vector[2] to null to verify correct null handling
    ((BytesColumnVector) batch.cols[1]).vector[2] = null;
    r = (BytesColumnVector) batch.cols[3];
    expr.evaluate(batch);
    assertTrue(getString(r, 0).equals("arg3_0"));
    assertTrue(getString(r, 1).equals("arg3_1"));
    assertTrue(!r.noNulls && r.isNull[2]);
    assertTrue(getString(r, 3).equals("arg2_3"));
    assertFalse(r.isNull[0] || r.isNull[1] || r.isNull[3]);

    // test third IF argument with nulls
    batch = getBatch1Long3BytesVectors();
    batch.cols[2].noNulls = false;
    batch.cols[2].isNull[0] = true;

    // set vector[0] to null object reference to verify correct null handling
    ((BytesColumnVector) batch.cols[2]).vector[0] = null;
    r = (BytesColumnVector) batch.cols[3];
    expr.evaluate(batch);
    assertTrue(!r.noNulls && r.isNull[0]);
    assertTrue(getString(r, 1).equals("arg3_1"));
    assertTrue(getString(r, 2).equals("arg2_2"));
    assertTrue(getString(r, 3).equals("arg2_3"));
    assertFalse(r.isNull[1] || r.isNull[2] || r.isNull[3]);

    // test second IF argument with nulls and repeating
    batch = getBatch1Long3BytesVectors();
    batch.cols[1].noNulls = false;
    batch.cols[1].isNull[0] = true;
    batch.cols[1].isRepeating = true;
    r = (BytesColumnVector) batch.cols[3];
    expr.evaluate(batch);
    assertTrue(getString(r, 0).equals("arg3_0"));
    assertTrue(getString(r, 1).equals("arg3_1"));
    assertTrue(!r.noNulls && r.isNull[2]);
    assertTrue(!r.noNulls && r.isNull[3]);
    assertFalse(r.isNull[0] || r.isNull[1]);
  }

  @Test
  public void testIfExprStringColumnStringScalar() {
    VectorizedRowBatch batch = getBatch1Long3BytesVectors();
    byte[] scalar = getUTF8Bytes("scalar");
    VectorExpression expr = new IfExprStringGroupColumnStringScalar(0, 1, scalar, 3);
    BytesColumnVector r = (BytesColumnVector) batch.cols[3];
    expr.evaluate(batch);
    assertTrue(getString(r, 0).equals("scalar"));
    assertTrue(getString(r, 1).equals("scalar"));
    assertTrue(getString(r, 2).equals("arg2_2"));
    assertTrue(getString(r, 3).equals("arg2_3"));

    // test for null input strings
    batch = getBatch1Long3BytesVectors();
    BytesColumnVector arg2 = (BytesColumnVector) batch.cols[1];
    arg2.noNulls = false;
    arg2.isNull[2] = true;
    arg2.vector[2] = null;
    expr.evaluate(batch);
    r = (BytesColumnVector) batch.cols[3];
    assertTrue(!r.noNulls && r.isNull[2]);
  }

  @Test
  public void testIfExprStringScalarStringColumn() {
    VectorizedRowBatch batch = getBatch1Long3BytesVectors();
    byte[] scalar = getUTF8Bytes("scalar");
    VectorExpression expr = new IfExprStringScalarStringGroupColumn(0,scalar, 2, 3);
    BytesColumnVector r = (BytesColumnVector) batch.cols[3];
    expr.evaluate(batch);
    assertTrue(getString(r, 0).equals("arg3_0"));
    assertTrue(getString(r, 1).equals("arg3_1"));
    assertTrue(getString(r, 2).equals("scalar"));
    assertTrue(getString(r, 3).equals("scalar"));

    // test for null input strings
    batch = getBatch1Long3BytesVectors();
    BytesColumnVector arg3 = (BytesColumnVector) batch.cols[2];
    arg3.noNulls = false;
    arg3.isNull[1] = true;
    arg3.vector[1] = null;
    expr.evaluate(batch);
    r = (BytesColumnVector) batch.cols[3];
    assertTrue(!r.noNulls && r.isNull[1]);
  }

  @Test
  public void testIfExprStringScalarStringScalar() {

    // standard case
    VectorizedRowBatch batch = getBatch1Long3BytesVectors();
    byte[] scalar1 = getUTF8Bytes("scalar1");
    byte[] scalar2 = getUTF8Bytes("scalar2");
    VectorExpression expr = new IfExprStringScalarStringScalar(0,scalar1, scalar2, 3);
    BytesColumnVector r = (BytesColumnVector) batch.cols[3];
    expr.evaluate(batch);
    assertTrue(getString(r, 0).equals("scalar2"));
    assertTrue(getString(r, 1).equals("scalar2"));
    assertTrue(getString(r, 2).equals("scalar1"));
    assertTrue(getString(r, 3).equals("scalar1"));
    assertFalse(r.isRepeating);

    // repeating case for first (boolean flag) argument to IF
    batch = getBatch1Long3BytesVectors();
    batch.cols[0].isRepeating = true;
    expr.evaluate(batch);
    r = (BytesColumnVector) batch.cols[3];
    assertTrue(r.isRepeating);
    assertTrue(getString(r, 0).equals("scalar2"));
  }
}
