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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import junit.framework.Assert;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringColEqualStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringColGreaterEqualStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringColLessStringColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringColLessStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringScalarEqualStringColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringScalarGreaterStringColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringScalarLessEqualStringColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.StringColEqualStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.StringColLessStringColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.StringScalarEqualStringColumn;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * Test vectorized expression and filter evaluation for strings.
 */
public class TestVectorStringExpressions {

  private static byte[] red;
  private static byte[] redred;
  private static byte[] red2; // second copy of red, different object
  private static byte[] green;
  private static byte[] greenred;
  private static byte[] redgreen;
  private static byte[] greengreen;
  private static byte[] blue;
  private static byte[] emptyString;
  private static byte[] mixedUp;
  private static byte[] mixedUpLower;
  private static byte[] mixedUpUpper;
  private static byte[] multiByte;
  private static byte[] mixPercentPattern;
  private static byte[] blanksLeft;
  private static byte[] blanksRight;
  private static byte[] blanksBoth;
  private static byte[] blankString;

  static {
    try {
      blue = "blue".getBytes("UTF-8");
      red = "red".getBytes("UTF-8");
      redred = "redred".getBytes("UTF-8");
      green = "green".getBytes("UTF-8");
      greenred = "greenred".getBytes("UTF-8");
      redgreen = "redgreen".getBytes("UTF-8");
      greengreen = "greengreen".getBytes("UTF-8");
      emptyString = "".getBytes("UTF-8");
      mixedUp = "mixedUp".getBytes("UTF-8");
      mixedUpLower = "mixedup".getBytes("UTF-8");
      mixedUpUpper = "MIXEDUP".getBytes("UTF-8");
      mixPercentPattern = "mix%".getBytes("UTF-8"); // for use as wildcard pattern to test LIKE
      multiByte = new byte[100];
      addMultiByteChars(multiByte);
      blanksLeft = "  foo".getBytes("UTF-8");
      blanksRight = "foo  ".getBytes("UTF-8");
      blanksBoth = "  foo  ".getBytes("UTF-8");
      blankString = "  ".getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    red2 = new byte[red.length];
    System.arraycopy(red, 0, red2, 0, red.length);
  }

  // add some multi-byte characters to test length routine later.
  // total characters = 4; byte length = 10
  static void addMultiByteChars(byte[] b) {
    int i = 0;
    b[i++] = (byte) 0x41; // letter "A" (1 byte)
    b[i++] = (byte) 0xC3; // Latin capital A with grave (2 bytes)
    b[i++] = (byte) 0x80;
    b[i++] = (byte) 0xE2; // Euro sign (3 bytes)
    b[i++] = (byte) 0x82;
    b[i++] = (byte) 0xAC;
    b[i++] = (byte) 0xF0; // Asian character U+24B62 (4 bytes)
    b[i++] = (byte) 0xA4;
    b[i++] = (byte) 0xAD;
    b[i++] = (byte) 0xA2;
  }

  @Test
  // Load a BytesColumnVector by copying in large data, enough to force
  // the buffer to expand.
  public void testLoadBytesColumnVectorByValueLargeData()  {
    BytesColumnVector bcv = new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    bcv.initBuffer(10); // initialize with estimated element size 10
    String s = "0123456789";
    while (s.length() < 500) {
      s += s;
    }
    byte[] b = null;
    try {
      b = s.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    for (int i = 0; i != VectorizedRowBatch.DEFAULT_SIZE; i++) {
      bcv.setVal(i, b, 0, b.length);
    }
    Assert.assertTrue(bcv.bufferSize() >= b.length * VectorizedRowBatch.DEFAULT_SIZE);
  }

  @Test
  // set values by reference, copy the data out, and verify equality
  public void testLoadBytesColumnVectorByRef() {
    BytesColumnVector bcv = new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    String s = "red";
    byte[] b = null;
    try {
      b = s.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    for (int i = 0; i != VectorizedRowBatch.DEFAULT_SIZE; i++) {
      bcv.setRef(i, b, 0, b.length);
    }
    // verify
    byte[] v = new byte[b.length];
    for (int i = 0; i != VectorizedRowBatch.DEFAULT_SIZE; i++) {
      Assert.assertTrue(bcv.length[i] == b.length);
      System.arraycopy(bcv.vector[i], bcv.start[i], v, 0, b.length);
      Assert.assertTrue(Arrays.equals(b, v));
    }
  }

  @Test
  // Test string column to string literal comparison
  public void testStringColCompareStringScalarFilter() {
    VectorizedRowBatch batch = makeStringBatch();
    VectorExpression expr;
    expr = new FilterStringColEqualStringScalar(0, red2);
    expr.evaluate(batch);

    // only red qualifies, and it's in entry 0
    Assert.assertTrue(batch.size == 1);
    Assert.assertTrue(batch.selected[0] == 0);

    batch = makeStringBatch();
    expr = new FilterStringColLessStringScalar(0, red2);
    expr.evaluate(batch);

    // only green qualifies, and it's in entry 1
    Assert.assertTrue(batch.size == 1);
    Assert.assertTrue(batch.selected[0] == 1);

    batch = makeStringBatch();
    expr = new FilterStringColGreaterEqualStringScalar(0, green);
    expr.evaluate(batch);

    // green and red qualify
    Assert.assertTrue(batch.size == 2);
    Assert.assertTrue(batch.selected[0] == 0);
    Assert.assertTrue(batch.selected[1] == 1);
  }

  @Test
  public void testStringColCompareStringScalarProjection() {
    VectorizedRowBatch batch = makeStringBatch();
    VectorExpression expr;

    expr = new StringColEqualStringScalar(0, red2, 2);
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    LongColumnVector outVector = (LongColumnVector) batch.cols[2];
    Assert.assertEquals(1, outVector.vector[0]);
    Assert.assertEquals(0, outVector.vector[1]);
    Assert.assertEquals(0, outVector.vector[2]);

    batch = makeStringBatch();
    expr = new StringColEqualStringScalar(0, green, 2);
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    outVector = (LongColumnVector) batch.cols[2];
    Assert.assertEquals(0, outVector.vector[0]);
    Assert.assertEquals(1, outVector.vector[1]);
    Assert.assertEquals(0, outVector.vector[2]);
  }

  @Test
  // Test string literal to string column comparison
  public void testStringScalarCompareStringCol() {
    VectorizedRowBatch batch = makeStringBatch();
    VectorExpression expr;
    expr = new FilterStringScalarEqualStringColumn(red2, 0);
    expr.evaluate(batch);

    // only red qualifies, and it's in entry 0
    Assert.assertTrue(batch.size == 1);
    Assert.assertTrue(batch.selected[0] == 0);

    batch = makeStringBatch();
    expr = new FilterStringScalarGreaterStringColumn(red2, 0);
    expr.evaluate(batch);

    // only green qualifies, and it's in entry 1
    Assert.assertTrue(batch.size == 1);
    Assert.assertTrue(batch.selected[0] == 1);

    batch = makeStringBatch();
    expr = new FilterStringScalarLessEqualStringColumn(green, 0);
    expr.evaluate(batch);

    // green and red qualify
    Assert.assertTrue(batch.size == 2);
    Assert.assertTrue(batch.selected[0] == 0);
    Assert.assertTrue(batch.selected[1] == 1);
  }

  @Test
  public void testStringScalarCompareStringColProjection() {
    VectorizedRowBatch batch = makeStringBatch();
    VectorExpression expr;

    expr = new StringScalarEqualStringColumn(red2, 0, 2);
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    LongColumnVector outVector = (LongColumnVector) batch.cols[2];
    Assert.assertEquals(1, outVector.vector[0]);
    Assert.assertEquals(0, outVector.vector[1]);
    Assert.assertEquals(0, outVector.vector[2]);

    batch = makeStringBatch();
    expr = new StringScalarEqualStringColumn(green, 0, 2);
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    outVector = (LongColumnVector) batch.cols[2];
    Assert.assertEquals(0, outVector.vector[0]);
    Assert.assertEquals(1, outVector.vector[1]);
    Assert.assertEquals(0, outVector.vector[2]);
  }

  @Test
  public void testStringColCompareStringColFilter() {
    VectorizedRowBatch batch;
    VectorExpression expr;

    /* input data
     *
     * col0       col1
     * ===============
     * blue       red
     * green      green
     * red        blue
     * NULL       red            col0 data is empty string if we un-set NULL property
     */

    // nulls possible on left, right
    batch = makeStringBatchForColColCompare();
    expr = new FilterStringColLessStringColumn(0,1);
    expr.evaluate(batch);
    Assert.assertEquals(1, batch.size);
    Assert.assertEquals(0, batch.selected[0]);

    // no nulls possible
    batch = makeStringBatchForColColCompare();
    batch.cols[0].noNulls = true;
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(2, batch.size);
    Assert.assertEquals(3, batch.selected[1]);

    // nulls on left, no nulls on right
    batch = makeStringBatchForColColCompare();
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(1, batch.size);
    Assert.assertEquals(0, batch.selected[0]);

    // nulls on right, no nulls on left
    batch = makeStringBatchForColColCompare();
    batch.cols[0].noNulls = true;
    batch.cols[1].isNull[3] = true;
    expr.evaluate(batch);
    Assert.assertEquals(1, batch.size);
    Assert.assertEquals(0, batch.selected[0]);

    // Now vary isRepeating
    // nulls possible on left, right

    // left repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    Assert.assertEquals(3, batch.selected[2]);

    // right repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[1].isRepeating = true;
    expr.evaluate(batch);
    Assert.assertEquals(2, batch.size); // first 2 qualify
    Assert.assertEquals(1, batch.selected[1]);

    // left and right repeat
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].isRepeating = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);

    // Now vary isRepeating
    // nulls possible only on left

    // left repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    Assert.assertEquals(3, batch.selected[2]);

    // left repeats and is null
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].noNulls = true;
    batch.cols[0].isNull[0] = true;
    expr.evaluate(batch);
    Assert.assertEquals(0, batch.size);

    // right repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[1].isRepeating = true;
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(2, batch.size);
    Assert.assertEquals(0, batch.selected[0]);
    Assert.assertEquals(1, batch.selected[1]);

    // left and right repeat
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].isRepeating = true;
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);


    // Now vary isRepeating
    // nulls possible only on right

    // left repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    batch.cols[1].isNull[0] = true;
    expr.evaluate(batch);
    Assert.assertEquals(2, batch.size);
    Assert.assertEquals(3, batch.selected[1]);

    // right repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[1].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    Assert.assertEquals(3, batch.selected[2]);

    // right repeats and is null
    batch = makeStringBatchForColColCompare();
    batch.cols[1].isRepeating = true;
    batch.cols[0].noNulls = true;
    batch.cols[1].isNull[0] = true;
    expr.evaluate(batch);
    Assert.assertEquals(0, batch.size);

    // left and right repeat
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);

    // left and right repeat and right is null
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].isRepeating = true;
    batch.cols[0].noNulls = true;
    batch.cols[1].isNull[0] = true;
    expr.evaluate(batch);
    Assert.assertEquals(0, batch.size);
  }

  @Test
  public void testStringColCompareStringColProjection() {
    VectorizedRowBatch batch;
    VectorExpression expr;
    long [] outVector;

    /* input data
     *
     * col0       col1
     * ===============
     * blue       red
     * green      green
     * red        blue
     * NULL       red            col0 data is empty string if we un-set NULL property
     */

    // nulls possible on left, right
    batch = makeStringBatchForColColCompare();
    expr = new StringColLessStringColumn(0, 1, 3);
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);
    Assert.assertFalse(batch.cols[3].isNull[1]);
    Assert.assertEquals(0, outVector[1]);
    Assert.assertFalse(batch.cols[3].isNull[2]);
    Assert.assertEquals(0, outVector[2]);
    Assert.assertTrue(batch.cols[3].isNull[3]);

    // no nulls possible
    batch = makeStringBatchForColColCompare();
    batch.cols[0].noNulls = true;
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertTrue(batch.cols[3].noNulls);
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);
    Assert.assertFalse(batch.cols[3].isNull[1]);
    Assert.assertEquals(0, outVector[1]);
    Assert.assertFalse(batch.cols[3].isNull[2]);
    Assert.assertEquals(0, outVector[2]);
    Assert.assertFalse(batch.cols[3].isNull[3]);
    Assert.assertEquals(1, outVector[3]);

    // nulls on left, no nulls on right
    batch = makeStringBatchForColColCompare();
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);
    Assert.assertFalse(batch.cols[3].isNull[1]);
    Assert.assertEquals(0, outVector[1]);
    Assert.assertFalse(batch.cols[3].isNull[2]);
    Assert.assertEquals(0, outVector[2]);
    Assert.assertTrue(batch.cols[3].isNull[3]);

    // nulls on right, no nulls on left
    batch = makeStringBatchForColColCompare();
    batch.cols[0].noNulls = true;
    batch.cols[1].isNull[3] = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertFalse(batch.cols[3].noNulls);
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);
    Assert.assertFalse(batch.cols[3].isNull[1]);
    Assert.assertEquals(0, outVector[1]);
    Assert.assertFalse(batch.cols[3].isNull[2]);
    Assert.assertEquals(0, outVector[2]);
    Assert.assertTrue(batch.cols[3].isNull[3]);

    // Now vary isRepeating
    // nulls possible on left, right

    // left repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);
    Assert.assertFalse(batch.cols[3].isNull[1]);
    Assert.assertEquals(1, outVector[1]);
    Assert.assertFalse(batch.cols[3].isNull[2]);
    Assert.assertEquals(0, outVector[2]);
    Assert.assertFalse(batch.cols[3].isNull[3]);
    Assert.assertEquals(1, outVector[3]);


    // right repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[1].isRepeating = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertFalse(batch.cols[3].noNulls);
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);
    Assert.assertFalse(batch.cols[3].isNull[1]);
    Assert.assertEquals(1, outVector[1]);
    Assert.assertFalse(batch.cols[3].isNull[2]);
    Assert.assertEquals(0, outVector[2]);
    Assert.assertTrue(batch.cols[3].isNull[3]);

    // left and right repeat
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].isRepeating = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertTrue(batch.cols[3].isRepeating);
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);

    // Now vary isRepeating
    // nulls possible only on left

    // left repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertEquals(4, batch.size);
    Assert.assertFalse(batch.cols[3].noNulls);
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);
    Assert.assertFalse(batch.cols[3].isNull[1]);
    Assert.assertEquals(1, outVector[1]);
    Assert.assertFalse(batch.cols[3].isNull[2]);
    Assert.assertEquals(0, outVector[2]);
    Assert.assertFalse(batch.cols[3].isNull[3]);
    Assert.assertEquals(1, outVector[3]);

    // left repeats and is null
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].noNulls = true;
    batch.cols[0].isNull[0] = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    Assert.assertFalse(batch.cols[3].noNulls);
    Assert.assertTrue(batch.cols[3].isRepeating);
    Assert.assertTrue(batch.cols[3].isNull[0]);

    // right repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[1].isRepeating = true;
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertEquals(4, batch.size);
    Assert.assertFalse(batch.cols[3].noNulls);
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);
    Assert.assertFalse(batch.cols[3].isNull[1]);
    Assert.assertEquals(1, outVector[1]);
    Assert.assertFalse(batch.cols[3].isNull[2]);
    Assert.assertEquals(0, outVector[2]);
    Assert.assertTrue(batch.cols[3].isNull[3]);

    // left and right repeat
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].isRepeating = true;
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertTrue(batch.cols[3].isRepeating);
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);



    // Now vary isRepeating
    // nulls possible only on right

    // left repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    batch.cols[1].isNull[0] = true;
    expr.evaluate(batch);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertEquals(4, batch.size);
    Assert.assertFalse(batch.cols[3].noNulls);
    Assert.assertTrue(batch.cols[3].isNull[0]);
    Assert.assertFalse(batch.cols[3].isNull[1]);
    Assert.assertEquals(1, outVector[1]);
    Assert.assertFalse(batch.cols[3].isNull[2]);
    Assert.assertEquals(0, outVector[2]);
    Assert.assertFalse(batch.cols[3].isNull[3]);
    Assert.assertEquals(1, outVector[3]);

    // right repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[1].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertEquals(4, batch.size);
    Assert.assertFalse(batch.cols[3].noNulls);
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);
    Assert.assertFalse(batch.cols[3].isNull[1]);
    Assert.assertEquals(1, outVector[1]);
    Assert.assertFalse(batch.cols[3].isNull[2]);
    Assert.assertEquals(0, outVector[2]);
    Assert.assertFalse(batch.cols[3].isNull[3]);
    Assert.assertEquals(1, outVector[3]);

    // right repeats and is null
    batch = makeStringBatchForColColCompare();
    batch.cols[1].isRepeating = true;
    batch.cols[0].noNulls = true;
    batch.cols[1].isNull[0] = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    Assert.assertFalse(batch.cols[3].noNulls);
    Assert.assertTrue(batch.cols[3].isRepeating);
    Assert.assertTrue(batch.cols[3].isNull[0]);

    // left and right repeat
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertTrue(batch.cols[3].isRepeating);
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);

    // left and right repeat and right is null
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].isRepeating = true;
    batch.cols[0].noNulls = true;
    batch.cols[1].isNull[0] = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    Assert.assertFalse(batch.cols[3].noNulls);
    Assert.assertTrue(batch.cols[3].isRepeating);
    Assert.assertTrue(batch.cols[3].isNull[0]);
  }

  VectorizedRowBatch makeStringBatch() {
    // create a batch with one string ("Bytes") column
    VectorizedRowBatch batch = new VectorizedRowBatch(3);
    BytesColumnVector v = new BytesColumnVector();
    batch.cols[0] = v;
    batch.cols[1] = new BytesColumnVector();          // to hold output if needed
    batch.cols[2] = new LongColumnVector(batch.size); // to hold boolean output
    /*
     * Add these 3 values:
     *
     * red
     * green
     * NULL
     */
    v.setRef(0, red, 0, red.length);
    v.isNull[0] = false;
    v.setRef(1, green, 0, green.length);
    v.isNull[1] = false;
    v.setRef(2,  emptyString,  0,  emptyString.length);
    v.isNull[2] = true;

    v.noNulls = false;

    batch.size = 3;
    return batch;
  }

  VectorizedRowBatch makeStringBatchMixedCase() {
    // create a batch with two string ("Bytes") columns
    VectorizedRowBatch batch = new VectorizedRowBatch(2, VectorizedRowBatch.DEFAULT_SIZE);
    BytesColumnVector v = new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    batch.cols[0] = v;
    BytesColumnVector outV = new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    outV.initBuffer();
    batch.cols[1] = outV;
    /*
     * Add these 3 values:
     *
     * mixedUp
     * green
     * NULL
     */
    v.setRef(0, mixedUp, 0, mixedUp.length);
    v.isNull[0] = false;
    v.setRef(1, green, 0, green.length);
    v.isNull[1] = false;
    v.setRef(2,  emptyString,  0,  emptyString.length);
    v.isNull[2] = true;
    v.noNulls = false;

    batch.size = 3;
    return batch;
  }

  VectorizedRowBatch makeStringBatchMixedCharSize() {

    // create a new batch with one char column (for input) and one long column (for output)
    VectorizedRowBatch batch = new VectorizedRowBatch(2, VectorizedRowBatch.DEFAULT_SIZE);
    BytesColumnVector v = new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    batch.cols[0] = v;
    LongColumnVector outV = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    batch.cols[1] = outV;

    /*
     * Add these 3 values:
     *
     * mixedUp
     * green
     * NULL
     * <4 char string with mult-byte chars>
     */
    v.setRef(0, mixedUp, 0, mixedUp.length);
    v.isNull[0] = false;
    v.setRef(1, green, 0, green.length);
    v.isNull[1] = false;
    v.setRef(2,  emptyString,  0,  emptyString.length);
    v.isNull[2] = true;
    v.noNulls = false;
    v.setRef(3, multiByte, 0, 10);
    v.isNull[3] = false;

    batch.size = 4;
    return batch;
  }

  @Test
  public void testColLower() {
    // has nulls, not repeating
    VectorizedRowBatch batch = makeStringBatchMixedCase();
    StringLower expr = new StringLower(0, 1);
    expr.evaluate(batch);
    BytesColumnVector outCol = (BytesColumnVector) batch.cols[1];
    int cmp = StringExpr.compare(mixedUpLower, 0, mixedUpLower.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isNull[2]);
    int cmp2 = StringExpr.compare(green, 0, green.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp2);

    // no nulls, not repeating
    batch = makeStringBatchMixedCase();
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(mixedUpLower, 0, mixedUpLower.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.noNulls);

    // has nulls, is repeating
    batch = makeStringBatchMixedCase();
    batch.cols[0].isRepeating = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(mixedUpLower, 0, mixedUpLower.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertFalse(outCol.noNulls);

    // no nulls, is repeating
    batch = makeStringBatchMixedCase();
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(mixedUpLower, 0, mixedUpLower.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertTrue(outCol.noNulls);
  }

  @Test
  public void testColUpper() {

    // no nulls, not repeating

    /* We don't test all the combinations because (at least currently)
     * the logic is inherited to be the same as testColLower, which checks all the cases).
     */
    VectorizedRowBatch batch = makeStringBatchMixedCase();
    StringUpper expr = new StringUpper(0, 1);
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    BytesColumnVector outCol = (BytesColumnVector) batch.cols[1];
    int cmp = StringExpr.compare(mixedUpUpper, 0, mixedUpUpper.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.noNulls);
  }

  @Test
  public void testStringLength() {

    // has nulls, not repeating
    VectorizedRowBatch batch = makeStringBatchMixedCharSize();
    StringLength expr = new StringLength(0, 1);
    expr.evaluate(batch);
    LongColumnVector outCol = (LongColumnVector) batch.cols[1];
    Assert.assertEquals(5, outCol.vector[1]); // length of green is 5
    Assert.assertTrue(outCol.isNull[2]);
    Assert.assertEquals(4, outCol.vector[3]); // this one has the mixed-size chars

    // no nulls, not repeating
    batch = makeStringBatchMixedCharSize();
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (LongColumnVector) batch.cols[1];
    Assert.assertTrue(outCol.noNulls);
    Assert.assertEquals(4, outCol.vector[3]); // this one has the mixed-size chars

    // has nulls, is repeating
    batch = makeStringBatchMixedCharSize();
    batch.cols[0].isRepeating = true;
    expr.evaluate(batch);
    outCol = (LongColumnVector) batch.cols[1];
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertFalse(outCol.noNulls);
    Assert.assertEquals(7, outCol.vector[0]); // length of "mixedUp"

    // no nulls, is repeating
    batch = makeStringBatchMixedCharSize();
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (LongColumnVector) batch.cols[1];
    Assert.assertEquals(7, outCol.vector[0]); // length of "mixedUp"
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertTrue(outCol.noNulls);
  }

  private VectorizedRowBatch makeStringBatch2In1Out() {
    VectorizedRowBatch batch = new VectorizedRowBatch(3);
    BytesColumnVector v = new BytesColumnVector();
    batch.cols[0] = v;
    BytesColumnVector v2 = new BytesColumnVector();
    batch.cols[1] = v2;
    batch.cols[2] = new BytesColumnVector();

    v.setRef(0, red, 0, red.length);
    v.isNull[0] = false;
    v.setRef(1, green, 0, green.length);
    v.isNull[1] = false;
    v.setRef(2,  emptyString,  0,  emptyString.length);
    v.isNull[2] = true;
    v.noNulls = false;

    v2.setRef(0, red, 0, red.length);
    v2.isNull[0] = false;
    v2.setRef(1, green, 0, green.length);
    v2.isNull[1] = false;
    v2.setRef(2,  emptyString,  0,  emptyString.length);
    v2.isNull[2] = true;
    v2.noNulls = false;

    batch.size = 3;
    return batch;
  }

  private VectorizedRowBatch makeStringBatchForColColCompare() {
    VectorizedRowBatch batch = new VectorizedRowBatch(4);
    BytesColumnVector v = new BytesColumnVector();
    batch.cols[0] = v;
    BytesColumnVector v2 = new BytesColumnVector();
    batch.cols[1] = v2;
    batch.cols[2] = new BytesColumnVector();
    batch.cols[3] = new LongColumnVector();

    v.setRef(0, blue, 0, blue.length);
    v.isNull[0] = false;
    v.setRef(1, green, 0, green.length);
    v.isNull[1] = false;
    v.setRef(2,  red,  0,  red.length);
    v.isNull[2] = false;
    v.setRef(3, emptyString, 0, emptyString.length);
    v.isNull[3] = true;
    v.noNulls = false;

    v2.setRef(0, red, 0, red.length);
    v2.isNull[0] = false;
    v2.setRef(1, green, 0, green.length);
    v2.isNull[1] = false;
    v2.setRef(2,  blue,  0,  blue.length);
    v2.isNull[2] = false;
    v2.setRef(3, red, 0, red.length);
    v2.isNull[3] = false;
    v2.noNulls = false;

    batch.size = 4;
    return batch;
  }

  @Test
  public void testStringLike() throws HiveException {

    // has nulls, not repeating
    VectorizedRowBatch batch;
    Text pattern;
    int initialBatchSize;
    batch = makeStringBatchMixedCharSize();
    pattern = new Text(mixPercentPattern);
    FilterStringColLikeStringScalar expr = new FilterStringColLikeStringScalar(0, mixPercentPattern);
    expr.evaluate(batch);

    // verify that the beginning entry is the only one that matches
    Assert.assertEquals(1, batch.size);
    Assert.assertEquals(0, batch.selected[0]);

    // no nulls, not repeating
    batch = makeStringBatchMixedCharSize();
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);

    // verify that the beginning entry is the only one that matches
    Assert.assertEquals(1, batch.size);
    Assert.assertEquals(0, batch.selected[0]);

    // has nulls, is repeating
    batch = makeStringBatchMixedCharSize();
    initialBatchSize = batch.size;
    batch.cols[0].isRepeating = true;
    expr.evaluate(batch);

    // all rows qualify
    Assert.assertEquals(initialBatchSize, batch.size);

    // same, but repeating value is null
    batch = makeStringBatchMixedCharSize();
    batch.cols[0].isRepeating = true;
    batch.cols[0].isNull[0] = true;
    expr.evaluate(batch);

    // no rows qualify
    Assert.assertEquals(0, batch.size);

    // no nulls, is repeating
    batch = makeStringBatchMixedCharSize();
    initialBatchSize = batch.size;
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);

    // all rows qualify
    Assert.assertEquals(initialBatchSize, batch.size);
  }

  public void testStringLikePatternType() throws UnsupportedEncodingException, HiveException {
    FilterStringColLikeStringScalar expr;

    // BEGIN pattern
    expr = new FilterStringColLikeStringScalar(0, "abc%".getBytes());
    Assert.assertEquals(FilterStringColLikeStringScalar.BeginChecker.class,
        expr.checker.getClass());

    // END pattern
    expr = new FilterStringColLikeStringScalar(0, "%abc".getBytes("UTF-8"));
    Assert.assertEquals(FilterStringColLikeStringScalar.EndChecker.class,
        expr.checker.getClass());

    // MIDDLE pattern
    expr = new FilterStringColLikeStringScalar(0, "%abc%".getBytes());
    Assert.assertEquals(FilterStringColLikeStringScalar.MiddleChecker.class,
        expr.checker.getClass());

    // COMPLEX pattern
    expr = new FilterStringColLikeStringScalar(0, "%abc%de".getBytes());
    Assert.assertEquals(FilterStringColLikeStringScalar.ComplexChecker.class,
        expr.checker.getClass());

    // NONE pattern
    expr = new FilterStringColLikeStringScalar(0, "abc".getBytes());
    Assert.assertEquals(FilterStringColLikeStringScalar.NoneChecker.class,
        expr.checker.getClass());
  }

  public void testStringLikeMultiByte() throws HiveException {
    FilterStringColLikeStringScalar expr;
    VectorizedRowBatch batch;

    // verify that a multi byte LIKE expression matches a matching string
    batch = makeStringBatchMixedCharSize();
    expr = new FilterStringColLikeStringScalar(0, ("%" + multiByte + "%").getBytes());
    expr.evaluate(batch);
    Assert.assertEquals(batch.size, 1);

    // verify that a multi byte LIKE expression doesn't match a non-matching string
    batch = makeStringBatchMixedCharSize();
    expr = new FilterStringColLikeStringScalar(0, ("%" + multiByte + "x").getBytes());
    expr.evaluate(batch);
    Assert.assertEquals(batch.size, 0);
  }

  @Test
  public void testColConcatScalar() {

    // has nulls, not repeating
    VectorizedRowBatch batch = makeStringBatch();
    StringConcatColScalar expr = new StringConcatColScalar(0, red, 1);
    expr.evaluate(batch);
    BytesColumnVector outCol = (BytesColumnVector) batch.cols[1];

    int cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isNull[2]);
    int cmp2 = StringExpr.compare(greenred, 0, greenred.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp2);
    Assert.assertFalse(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    // no nulls, not repeating
    batch = makeStringBatch();
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);

    cmp2 = StringExpr.compare(greenred, 0, greenred.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp2);

    int cmp3 = StringExpr.compare(red, 0, red.length, outCol.vector[2],
        outCol.start[2], outCol.length[2]);
    Assert.assertEquals(0, cmp3);

    Assert.assertTrue(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    // has nulls, is repeating
    batch = makeStringBatch();
    batch.cols[0].isRepeating = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertFalse(outCol.noNulls);

    // no nulls, is repeating
    batch = makeStringBatch();
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertTrue(outCol.noNulls);
  }

  @Test
  public void testScalarConcatCol() {

    // has nulls, not repeating
    VectorizedRowBatch batch = makeStringBatch();
    StringConcatScalarCol expr = new StringConcatScalarCol(red, 0, 1);
    expr.evaluate(batch);
    BytesColumnVector outCol = (BytesColumnVector) batch.cols[1];

    int cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isNull[2]);
    int cmp2 = StringExpr.compare(redgreen, 0, redgreen.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp2);
    Assert.assertFalse(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    // no nulls, not repeating
    batch = makeStringBatch();
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);

    cmp2 = StringExpr.compare(redgreen, 0, redgreen.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp2);

    int cmp3 = StringExpr.compare(red, 0, red.length, outCol.vector[2],
        outCol.start[2], outCol.length[2]);
    Assert.assertEquals(0, cmp3);

    Assert.assertTrue(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    // has nulls, is repeating
    batch = makeStringBatch();
    batch.cols[0].isRepeating = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertFalse(outCol.noNulls);

    // no nulls, is repeating
    batch = makeStringBatch();
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertTrue(outCol.noNulls);
  }

  @Test
  public void testColConcatCol() {

    // has nulls, not repeating
    VectorizedRowBatch batch = makeStringBatch2In1Out();
    StringConcatColCol expr = new StringConcatColCol(0, 1, 2);
    expr.evaluate(batch);
    BytesColumnVector outCol = (BytesColumnVector) batch.cols[2];

    int cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isNull[2]);
    int cmp2 = StringExpr.compare(greengreen, 0, greengreen.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp2);
    Assert.assertFalse(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    // no nulls, not repeating
    batch = makeStringBatch2In1Out();
    batch.cols[0].noNulls = true;
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[2];

    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);

    cmp2 = StringExpr.compare(greengreen, 0, greengreen.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp2);

    int cmp3 = StringExpr.compare(emptyString, 0, emptyString.length,
        outCol.vector[2], outCol.start[2], outCol.length[2]);
    Assert.assertEquals(0, cmp3);

    Assert.assertTrue(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    // has nulls, is repeating

    batch = makeStringBatch2In1Out();
    batch.cols[0].isRepeating = true;                  // only left input repeating
    batch.cols[0].isNull[0] = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[2];

    Assert.assertEquals(3, batch.size);
    Assert.assertEquals(true, outCol.isRepeating);
    Assert.assertEquals(true, outCol.isNull[0]);

       // same, but repeating input is not null

    batch = makeStringBatch2In1Out();
    batch.cols[0].isRepeating = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[2];
    Assert.assertEquals(false, outCol.isRepeating);  //TEST FAILED
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertEquals(true, outCol.isNull[2]);

    batch = makeStringBatch2In1Out();
    batch.cols[1].isRepeating = true;                  // only right input repeating
    batch.cols[1].isNull[0] = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[2];

    Assert.assertEquals(3, batch.size);
    Assert.assertEquals(true, outCol.isRepeating);
    Assert.assertEquals(true, outCol.isNull[0]);

    batch = makeStringBatch2In1Out();
    batch.cols[0].isRepeating = true;                  // both inputs repeat
    batch.cols[0].isNull[0] = true;
    batch.cols[1].isRepeating = true;
    batch.cols[1].isNull[0] = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[2];

    Assert.assertEquals(3, batch.size);
    Assert.assertEquals(true, outCol.isRepeating);
    Assert.assertEquals(true, outCol.isNull[0]);

    // no nulls, is repeating
    batch = makeStringBatch2In1Out();
    batch.cols[1].isRepeating = true;             // only right input repeating and has no nulls
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[2];

    Assert.assertEquals(3, batch.size);
    Assert.assertEquals(false, outCol.isRepeating);
    Assert.assertEquals(false, outCol.isNull[0]);
    Assert.assertEquals(false, outCol.noNulls);
    Assert.assertEquals(true, outCol.isNull[2]);
    cmp = StringExpr.compare(greenred, 0, greenred.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp);

         // try again with left input also having no nulls
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(false, outCol.isRepeating);
    Assert.assertEquals(true,  outCol.noNulls);
    cmp = StringExpr.compare(red, 0, red.length, outCol.vector[2],
        outCol.start[2], outCol.length[2]);
    Assert.assertEquals(0, cmp);

    batch = makeStringBatch2In1Out();
    batch.cols[0].isRepeating = true;             // only left input repeating and has no nulls
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[2];

    Assert.assertEquals(3, batch.size);
    Assert.assertEquals(false, outCol.isRepeating);
    Assert.assertEquals(false, outCol.isNull[0]);
    Assert.assertEquals(false, outCol.noNulls);
    Assert.assertEquals(true, outCol.isNull[2]);
    cmp = StringExpr.compare(redgreen, 0, redgreen.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp);

    batch = makeStringBatch2In1Out();
    batch.cols[0].isRepeating = true;                  // both inputs repeat
    batch.cols[0].noNulls = true;
    batch.cols[1].isRepeating = true;
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[2];

    Assert.assertEquals(3, batch.size);
    Assert.assertEquals(true, outCol.isRepeating);
    Assert.assertEquals(false, outCol.isNull[0]);
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
  }

  @Test
  public void testSubstrStart() throws UnsupportedEncodingException {
    // Testing no nulls and no repeating
    VectorizedRowBatch batch = new VectorizedRowBatch(2);
    BytesColumnVector v = new BytesColumnVector();
    batch.cols[0] = v;
    BytesColumnVector outV = new BytesColumnVector();
    batch.cols[1] = outV;
    byte[] data1 = "abcd string".getBytes("UTF-8");
    byte[] data2 = "efgh string".getBytes("UTF-8");
    byte[] data3 = "efgh".getBytes("UTF-8");
    batch.size = 3;
    v.noNulls = true;
    v.setRef(0, data1, 0, data1.length);
    v.isNull[0] = false;
    v.setRef(1, data2, 0, data2.length);
    v.isNull[1] = false;
    v.setRef(2, data3, 0, data3.length);
    v.isNull[2] = false;

    StringSubstrColStart expr = new StringSubstrColStart(0, 6, 1);
    expr.evaluate(batch);
    BytesColumnVector outCol = (BytesColumnVector) batch.cols[1];
    Assert.assertEquals(3, batch.size);
    Assert.assertTrue(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);
    byte[] expected = "string".getBytes("UTF-8");
    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[1], outCol.start[1], outCol.length[1]
        )
    );

    // This yields empty because starting idx is out of bounds.
    Assert.assertEquals(0,
    StringExpr.compare(
            emptyString, 0, emptyString.length, outCol.vector[2], outCol.start[2], outCol.length[2]
        )
    );

    outCol.noNulls = false;
    outCol.isRepeating = true;

    // Testing negative substring index.
    // Start index -6 should yield the last 6 characters of the string

    expr = new StringSubstrColStart(0, -6, 1);
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    Assert.assertEquals(3, batch.size);
    Assert.assertTrue(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);
    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[1], outCol.start[1], outCol.length[1]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            emptyString, 0, emptyString.length, outCol.vector[2], outCol.start[2], outCol.length[2]
        )
    );

    outCol.noNulls = false;
    outCol.isRepeating = true;

    // Testing substring starting from index 1

    expr = new StringSubstrColStart(0, 1, 1);
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    Assert.assertTrue(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    Assert.assertEquals(0,
    StringExpr.compare(
            data1, 0, data1.length, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            data2, 0, data2.length, outCol.vector[1], outCol.start[1], outCol.length[1]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            data3, 0, data3.length, outCol.vector[2], outCol.start[2], outCol.length[2]
        )
    );

    outV.noNulls = false;
    outV.isRepeating = true;

    // Testing with nulls

    expr = new StringSubstrColStart(0, 6, 1);
    v.noNulls = false;
    v.isNull[0] = true;
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    Assert.assertFalse(outV.noNulls);
    Assert.assertTrue(outV.isNull[0]);

    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[1], outCol.start[1], outCol.length[1]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            emptyString, 0, emptyString.length, outCol.vector[2], outCol.start[2], outCol.length[2]
        )
    );

    outCol.noNulls = false;
    outCol.isRepeating = false;

    // Testing with repeating and no nulls

    outV = new BytesColumnVector();
    v = new BytesColumnVector();
    v.isRepeating = true;
    v.noNulls = true;
    v.setRef(0, data1, 0, data1.length);
    batch = new VectorizedRowBatch(2);
    batch.cols[0] = v;
    batch.cols[1] = outV;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    expected = "string".getBytes("UTF-8");
    Assert.assertTrue(outV.isRepeating);
    Assert.assertTrue(outV.noNulls);
    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );

    // Testing multiByte string substring

    v = new BytesColumnVector();
    v.isRepeating = false;
    v.noNulls = true;
    v.setRef(0, multiByte, 0, 10);
    batch.cols[0] = v;
    batch.cols[1] = outV;
    outV.isRepeating = true;
    outV.noNulls = false;
    expr = new StringSubstrColStart(0, 3, 1);
    batch.size = 1;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    Assert.assertFalse(outV.isRepeating);
    Assert.assertTrue(outV.noNulls);
    Assert.assertEquals(0,
    StringExpr.compare(
            // 3nd char starts from index 3 and total length should be 7 bytes as max is 10
            multiByte, 3, 10 - 3, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );


    // Testing multiByte string with reference starting mid array

    v = new BytesColumnVector();
    v.isRepeating = false;
    v.noNulls = true;

    // string is 2 chars long (a 3 byte and a 4 byte char)
    v.setRef(0, multiByte, 3, 7);
    batch.cols[0] = v;
    batch.cols[1] = outV;
    outV.isRepeating = true;
    outV.noNulls = false;
    outCol = (BytesColumnVector) batch.cols[1];
    expr = new StringSubstrColStart(0, 2, 1);
    expr.evaluate(batch);
    Assert.assertFalse(outV.isRepeating);
    Assert.assertTrue(outV.noNulls);
    Assert.assertEquals(0,
    StringExpr.compare(
            // the result is the last 1 character, which occupies 4 bytes
            multiByte, 6, 4, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );
  }

  @Test
  public void testSubstrStartLen() throws UnsupportedEncodingException {
    // Testing no nulls and no repeating

    VectorizedRowBatch batch = new VectorizedRowBatch(2);
    BytesColumnVector v = new BytesColumnVector();
    batch.cols[0] = v;
    BytesColumnVector outV = new BytesColumnVector();
    batch.cols[1] = outV;
    byte[] data1 = "abcd string".getBytes("UTF-8");
    byte[] data2 = "efgh string".getBytes("UTF-8");
    byte[] data3 = "efgh".getBytes("UTF-8");
    batch.size = 3;
    v.noNulls = true;
    v.setRef(0, data1, 0, data1.length);
    v.isNull[0] = false;
    v.setRef(1, data2, 0, data2.length);
    v.isNull[1] = false;
    v.setRef(2, data3, 0, data3.length);
    v.isNull[2] = false;

    outV.isRepeating = true;
    outV.noNulls = false;

    StringSubstrColStartLen expr = new StringSubstrColStartLen(0, 6, 6, 1);
    expr.evaluate(batch);
    BytesColumnVector outCol = (BytesColumnVector) batch.cols[1];
    Assert.assertEquals(3, batch.size);
    Assert.assertTrue(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);
    byte[] expected = "string".getBytes("UTF-8");
    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[1], outCol.start[1], outCol.length[1]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            emptyString, 0, emptyString.length, outCol.vector[2], outCol.start[2], outCol.length[2]
        )
    );

    // Testing negative substring index
    outV.isRepeating = true;
    outV.noNulls = false;

    expr = new StringSubstrColStartLen(0, -6, 6, 1);
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    Assert.assertTrue(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);
    Assert.assertEquals(3, batch.size);

    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );

    Assert.assertEquals(0,
        StringExpr.compare(
            expected, 0, expected.length, outCol.vector[1], outCol.start[1], outCol.length[1]
        )
    );

    // This yields empty because starting index is out of bounds
    Assert.assertEquals(0,
    StringExpr.compare(
            emptyString, 0, emptyString.length, outCol.vector[2], outCol.start[2], outCol.length[2]
        )
    );

    //Testing substring index starting with 1 and zero length

    outV.isRepeating = true;
    outV.noNulls = false;

    expr = new StringSubstrColStartLen(0, 1, 0, 1);
    outCol = (BytesColumnVector) batch.cols[1];
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    Assert.assertTrue(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);
    Assert.assertEquals(0,
        StringExpr.compare(
            data1, 1, 0, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );

    Assert.assertEquals(0,
        StringExpr.compare(
            data2, 1, 0, outCol.vector[1], outCol.start[1], outCol.length[1]
        )
    );

    Assert.assertEquals(0,
        StringExpr.compare(
            data3, 1, 0, outCol.vector[2], outCol.start[2], outCol.length[2]
        )
    );


    //Testing substring index starting with 0 and length equal to array length

    outV.isRepeating = true;
    outV.noNulls = false;

    expr = new StringSubstrColStartLen(0, 0, 11, 1);
    outCol = (BytesColumnVector) batch.cols[1];
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    Assert.assertTrue(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);
    Assert.assertEquals(0,
    StringExpr.compare(
            data1, 0, data1.length, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            data2, 0, data2.length, outCol.vector[1], outCol.start[1], outCol.length[1]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            data3, 0, data3.length, outCol.vector[2], outCol.start[2], outCol.length[2]
        )
    );


    // Testing setting length larger than array length, which should cap to the length itself

    outV.isRepeating = true;
    outV.noNulls = false;

    expr = new StringSubstrColStartLen(0, 6, 10, 1);
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    Assert.assertEquals(3, batch.size);
    Assert.assertTrue(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);
    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[1], outCol.start[1], outCol.length[1]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            emptyString, 0, emptyString.length, outCol.vector[2], outCol.start[2], outCol.length[2]
        )
    );

    outV.isRepeating = true;
    outV.noNulls = true;

    // Testing with nulls

    v.noNulls = false;
    v.isNull[0] = true;
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    Assert.assertFalse(outV.noNulls);
    Assert.assertTrue(outV.isNull[0]);
    Assert.assertFalse(outCol.isRepeating);
    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[1], outCol.start[1], outCol.length[1]
        )
    );

    Assert.assertEquals(0,
        StringExpr.compare(
            emptyString, 0, emptyString.length, outCol.vector[2], outCol.start[2], outCol.length[2]
        )
    );


    // Testing with repeating and no nulls
    outV = new BytesColumnVector();
    v = new BytesColumnVector();
    outV.isRepeating = false;
    outV.noNulls = true;
    v.isRepeating = true;
    v.noNulls = false;
    v.setRef(0, data1, 0, data1.length);
    batch = new VectorizedRowBatch(2);
    batch.cols[0] = v;
    batch.cols[1] = outV;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    Assert.assertTrue(outCol.noNulls);
    Assert.assertTrue(outCol.isRepeating);

    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );

    // Testing with multiByte String
    v = new BytesColumnVector();
    v.isRepeating = false;
    v.noNulls = true;
    batch.size = 1;
    v.setRef(0, multiByte, 0, 10);
    batch.cols[0] = v;
    batch.cols[1] = outV;
    outV.isRepeating = true;
    outV.noNulls = false;
    expr = new StringSubstrColStartLen(0, 3, 2, 1);
    expr.evaluate(batch);
    Assert.assertEquals(1, batch.size);
    Assert.assertFalse(outV.isRepeating);
    Assert.assertTrue(outV.noNulls);
    Assert.assertEquals(0,
    StringExpr.compare(
            // 3rd char starts at index 3, and with length 2 it is covering the rest of the array.
            multiByte, 3, 10 - 3, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );

    // Testing multiByte string with reference set to mid array
    v = new BytesColumnVector();
    v.isRepeating = false;
    v.noNulls = true;
    outV = new BytesColumnVector();
    batch.size = 1;
    v.setRef(0, multiByte, 3, 7);
    batch.cols[0] = v;
    batch.cols[1] = outV;
    outV.isRepeating = true;
    outV.noNulls = false;
    expr = new StringSubstrColStartLen(0, 2, 2, 1);
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    Assert.assertEquals(1, batch.size);
    Assert.assertFalse(outV.isRepeating);
    Assert.assertTrue(outV.noNulls);
    Assert.assertEquals(0,
    StringExpr.compare(
            // 2nd substring index refers to the 6th index (last char in the array)
            multiByte, 6, 10 - 6, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );
  }

  @Test
  public void testVectorLTrim() {
    VectorizedRowBatch b = makeTrimBatch();
    VectorExpression expr = new StringLTrim(0, 1);
    expr.evaluate(b);
    BytesColumnVector outV = (BytesColumnVector) b.cols[1];
    Assert.assertEquals(0,
        StringExpr.compare(emptyString, 0, 0, outV.vector[0], 0, 0));
    Assert.assertEquals(0,
        StringExpr.compare(blanksLeft, 2, 3, outV.vector[1], outV.start[1], outV.length[1]));
    Assert.assertEquals(0,
        StringExpr.compare(blanksRight, 0, 5, outV.vector[2], outV.start[2], outV.length[2]));
    Assert.assertEquals(0,
        StringExpr.compare(blanksBoth, 2, 5, outV.vector[3], outV.start[3], outV.length[3]));
    Assert.assertEquals(0,
        StringExpr.compare(red, 0, 3, outV.vector[4], outV.start[4], outV.length[4]));
    Assert.assertEquals(0,
        StringExpr.compare(blankString, 0, 0, outV.vector[5], outV.start[5], outV.length[5]));
  }

  @Test
  public void testVectorRTrim() {
    VectorizedRowBatch b = makeTrimBatch();
    VectorExpression expr = new StringRTrim(0, 1);
    expr.evaluate(b);
    BytesColumnVector outV = (BytesColumnVector) b.cols[1];
    Assert.assertEquals(0,
        StringExpr.compare(emptyString, 0, 0, outV.vector[0], 0, 0));
    Assert.assertEquals(0,
        StringExpr.compare(blanksLeft, 0, 5, outV.vector[1], outV.start[1], outV.length[1]));
    Assert.assertEquals(0,
        StringExpr.compare(blanksRight, 0, 3, outV.vector[2], outV.start[2], outV.length[2]));
    Assert.assertEquals(0,
        StringExpr.compare(blanksBoth, 0, 5, outV.vector[3], outV.start[3], outV.length[3]));
    Assert.assertEquals(0,
        StringExpr.compare(red, 0, 3, outV.vector[4], outV.start[4], outV.length[4]));
    Assert.assertEquals(0,
        StringExpr.compare(blankString, 0, 0, outV.vector[5], outV.start[5], outV.length[5]));
  }

  @Test
  public void testVectorTrim() {
    VectorizedRowBatch b = makeTrimBatch();
    VectorExpression expr = new StringTrim(0, 1);
    expr.evaluate(b);
    BytesColumnVector outV = (BytesColumnVector) b.cols[1];
    Assert.assertEquals(0,
        StringExpr.compare(emptyString, 0, 0, outV.vector[0], 0, 0));
    Assert.assertEquals(0,
        StringExpr.compare(blanksLeft, 2, 3, outV.vector[1], outV.start[1], outV.length[1]));
    Assert.assertEquals(0,
        StringExpr.compare(blanksRight, 0, 3, outV.vector[2], outV.start[2], outV.length[2]));
    Assert.assertEquals(0,
        StringExpr.compare(blanksBoth, 2, 3, outV.vector[3], outV.start[3], outV.length[3]));
    Assert.assertEquals(0,
        StringExpr.compare(red, 0, 3, outV.vector[4], outV.start[4], outV.length[4]));
    Assert.assertEquals(0,
        StringExpr.compare(blankString, 0, 0, outV.vector[5], outV.start[5], outV.length[5]));
  }

  // Make a batch to test the trim functions.
  private VectorizedRowBatch makeTrimBatch() {
    VectorizedRowBatch b = new VectorizedRowBatch(2);
    BytesColumnVector inV = new BytesColumnVector();
    BytesColumnVector outV = new BytesColumnVector();
    b.cols[0] = inV;
    b.cols[1] = outV;
    inV.setRef(0, emptyString, 0, 0);
    inV.setRef(1, blanksLeft, 0, blanksLeft.length);
    inV.setRef(2, blanksRight, 0, blanksRight.length);
    inV.setRef(3, blanksBoth, 0, blanksBoth.length);
    inV.setRef(4, red, 0, red.length);
    inV.setRef(5, blankString, 0, blankString.length);
    b.size = 5;
    return b;
  }
 }
