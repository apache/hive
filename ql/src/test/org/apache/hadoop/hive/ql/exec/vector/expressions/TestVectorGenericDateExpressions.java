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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TestVectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.junit.Assert;
import org.junit.Test;


import java.io.UnsupportedEncodingException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class TestVectorGenericDateExpressions {
  private int size = 200;
  private Random random = new Random();
  private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  private List<VectorExpression.Type> dateTimestampStringTypes =
      Arrays.<VectorExpression.Type>asList(VectorExpression.Type.DATE, VectorExpression.Type.TIMESTAMP, VectorExpression.Type.STRING);

  private long newRandom(int i) {
    return random.nextInt(i);
  }

  private LongColumnVector newRandomLongColumnVector(int range, int size) {
    LongColumnVector vector = new LongColumnVector(size);
    for (int i = 0; i < size; i++) {
      vector.vector[i] = random.nextInt(range);
    }
    return vector;
  }

  private LongColumnVector toTimestamp(LongColumnVector date) {
    LongColumnVector vector = new LongColumnVector(size);
    for (int i = 0; i < size; i++) {
      if (date.isNull[i]) {
        vector.isNull[i] = true;
        vector.noNulls = false;
      } else {
        vector.vector[i] = toTimestamp(date.vector[i]);
      }
    }
    return vector;
  }

  private long toTimestamp(long date) {
    return DateWritable.daysToMillis((int) date) * 1000000;
  }

  private BytesColumnVector toString(LongColumnVector date) {
    BytesColumnVector bcv = new BytesColumnVector(size);
    for (int i = 0; i < size; i++) {
      if (date.isNull[i]) {
        bcv.isNull[i] = true;
        bcv.noNulls = false;
      } else {
        bcv.vector[i] = toString(date.vector[i]);
        bcv.start[i] = 0;
        bcv.length[i] = bcv.vector[i].length;
      }
    }
    return bcv;
  }

  private byte[] toString(long date) {
    try {
      String formatted = formatter.format(new Date(DateWritable.daysToMillis((int) date)));
      return formatted.getBytes("UTF-8");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void validateDateAdd(VectorizedRowBatch batch, VectorExpression.Type colType1, long scalar2,
                               boolean isPositive, LongColumnVector date1) {
    VectorUDFDateAddColScalar udf;
    if (isPositive) {
      udf = new VectorUDFDateAddColScalar(0, scalar2, 1);
    } else {
      udf = new VectorUDFDateSubColScalar(0, scalar2, 1);
    }
    udf.setInputTypes(colType1, VectorExpression.Type.OTHER);
    udf.evaluate(batch);
    BytesColumnVector output = (BytesColumnVector) batch.cols[1];

    try {
      for (int i = 0; i < size; i++) {
        String expected;
        if (isPositive) {
          expected = new String(toString(date1.vector[i] + scalar2), "UTF-8");
        } else {
          expected = new String(toString(date1.vector[i] - scalar2), "UTF-8");
        }
        if (date1.isNull[i]) {
          Assert.assertTrue(output.isNull[i]);
        } else {
          String actual = new String(output.vector[i], output.start[i], output.start[i] + output.length[i], "UTF-8");
          Assert.assertEquals("expectedLen:" + expected.length() + " actualLen:" + actual.length(), expected, actual);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private ColumnVector castTo(LongColumnVector date, VectorExpression.Type type) {
    switch (type) {
      case DATE:
        return date;

      case TIMESTAMP:
        return toTimestamp(date);

      case STRING:
        return toString(date);
    }
    return null;
  }

  private void testDateAddColScalar(VectorExpression.Type colType1, boolean isPositive) {
    LongColumnVector date1 = newRandomLongColumnVector(10000, size);
    ColumnVector col1 = castTo(date1, colType1);
    long scalar2 = newRandom(1000);
    BytesColumnVector output = new BytesColumnVector(size);

    VectorizedRowBatch batch = new VectorizedRowBatch(2, size);
    batch.cols[0] = col1;
    batch.cols[1] = output;

    validateDateAdd(batch, colType1, scalar2, isPositive, date1);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    validateDateAdd(batch, colType1, scalar2, isPositive, date1);
  }

  @Test
  public void testDateAddColScalar() {
    for (VectorExpression.Type colType1 : dateTimestampStringTypes)
      testDateAddColScalar(colType1, true);

    VectorExpression udf = new VectorUDFDateAddColScalar(0, 0, 1);
    udf.setInputTypes(VectorExpression.Type.STRING, VectorExpression.Type.TIMESTAMP);
    VectorizedRowBatch batch = new VectorizedRowBatch(2, 1);
    batch.cols[0] = new BytesColumnVector(1);
    batch.cols[1] = new BytesColumnVector(1);
    BytesColumnVector bcv = (BytesColumnVector) batch.cols[0];
    byte[] bytes = new byte[0];
    try {
      bytes = "error".getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
    }
    bcv.vector[0] = bytes;
    bcv.start[0] = 0;
    bcv.length[0] = bytes.length;
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[1].isNull[0], true);
  }

  @Test
  public void testDateSubColScalar() {
    for (VectorExpression.Type colType1 : dateTimestampStringTypes)
      testDateAddColScalar(colType1, false);

    VectorExpression udf = new VectorUDFDateSubColScalar(0, 0, 1);
    udf.setInputTypes(VectorExpression.Type.STRING, VectorExpression.Type.TIMESTAMP);
    VectorizedRowBatch batch = new VectorizedRowBatch(2, 1);
    batch.cols[0] = new BytesColumnVector(1);
    batch.cols[1] = new BytesColumnVector(1);
    BytesColumnVector bcv = (BytesColumnVector) batch.cols[0];
    byte[] bytes = new byte[0];
    try {
      bytes = "error".getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
    }
    bcv.vector[0] = bytes;
    bcv.start[0] = 0;
    bcv.length[0] = bytes.length;
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[1].isNull[0], true);
  }

  private void validateDateAdd(VectorizedRowBatch batch, long scalar1, LongColumnVector date2,
                               VectorExpression.Type colType1, boolean isPositive) {
    VectorExpression udf = null;
    if (isPositive) {
      switch (colType1) {
        case DATE:
          udf = new VectorUDFDateAddScalarCol(scalar1, 0, 1);
          break;
        case TIMESTAMP:
          udf = new VectorUDFDateAddScalarCol(toTimestamp(scalar1), 0, 1);
          break;
        case STRING:
          udf = new VectorUDFDateAddScalarCol(toString(scalar1), 0, 1);
          break;
      }
    } else {
      switch (colType1) {
        case DATE:
          udf = new VectorUDFDateSubScalarCol(scalar1, 0, 1);
          break;
        case TIMESTAMP:
          udf = new VectorUDFDateSubScalarCol(toTimestamp(scalar1), 0, 1);
          break;
        case STRING:
          udf = new VectorUDFDateSubScalarCol(toString(scalar1), 0, 1);
          break;
      }
    }
    udf.setInputTypes(colType1, VectorExpression.Type.OTHER);
    udf.evaluate(batch);

    BytesColumnVector output = (BytesColumnVector) batch.cols[1];
    try {
      for (int i = 0; i < date2.vector.length; i++) {
        String expected;
        if (isPositive) {
          expected = new String(toString(scalar1 + date2.vector[i]), "UTF-8");
        } else {
          expected = new String(toString(scalar1 - date2.vector[i]), "UTF-8");
        }
        if (date2.isNull[i]) {
          Assert.assertTrue(output.isNull[i]);
        } else {
          Assert.assertEquals(expected,
              new String(output.vector[i], output.start[i], output.start[i] + output.length[i], "UTF-8"));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void testDateAddScalarCol(VectorExpression.Type colType1, boolean isPositive) {
    LongColumnVector date2 = newRandomLongColumnVector(10000, size);
    long scalar1 = newRandom(1000);

    BytesColumnVector output = new BytesColumnVector(size);

    VectorizedRowBatch batch = new VectorizedRowBatch(2, size);
    batch.cols[0] = date2;
    batch.cols[1] = output;
    validateDateAdd(batch, scalar1, date2, colType1, isPositive);
    TestVectorizedRowBatch.addRandomNulls(date2);
    batch.cols[0] = date2;
    validateDateAdd(batch, scalar1, date2, colType1, isPositive);
  }

  @Test
  public void testDateAddScalarCol() {
    for (VectorExpression.Type scalarType1 : dateTimestampStringTypes)
      testDateAddScalarCol(scalarType1, true);

    VectorExpression udf = null;
    try {
      udf = new VectorUDFDateAddScalarCol("error".getBytes("UTF-8"), 0, 1);
    } catch (UnsupportedEncodingException e) {
    }
    udf.setInputTypes(VectorExpression.Type.STRING, VectorExpression.Type.TIMESTAMP);
    VectorizedRowBatch batch = new VectorizedRowBatch(2, 1);
    batch.cols[0] = new LongColumnVector(1);
    batch.cols[1] = new BytesColumnVector(1);
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[1].isNull[0], true);
  }

  @Test
  public void testDateSubScalarCol() {
    for (VectorExpression.Type scalarType1 : dateTimestampStringTypes)
      testDateAddScalarCol(scalarType1, false);

    VectorExpression udf = null;
    try {
      udf = new VectorUDFDateSubScalarCol("error".getBytes("UTF-8"), 0, 1);
    } catch (UnsupportedEncodingException e) {
    }
    udf.setInputTypes(VectorExpression.Type.STRING, VectorExpression.Type.TIMESTAMP);
    VectorizedRowBatch batch = new VectorizedRowBatch(2, 1);
    batch.cols[0] = new LongColumnVector(1);
    batch.cols[1] = new BytesColumnVector(1);
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[1].isNull[0], true);
  }

  private void validateDateAdd(VectorizedRowBatch batch,
                               LongColumnVector date1, LongColumnVector date2,
                               VectorExpression.Type colType1, boolean isPositive) {
    VectorExpression udf;
    if (isPositive) {
      udf = new VectorUDFDateAddColCol(0, 1, 2);
    } else {
      udf = new VectorUDFDateSubColCol(0, 1, 2);
    }
    udf.setInputTypes(colType1, VectorExpression.Type.OTHER);
    udf.evaluate(batch);
    BytesColumnVector output = (BytesColumnVector) batch.cols[2];
    try {
      for (int i = 0; i < date2.vector.length; i++) {
        String expected;
        if (isPositive) {
          expected = new String(toString(date1.vector[i] + date2.vector[i]), "UTF-8");
        } else {
          expected = new String(toString(date1.vector[i] - date2.vector[i]), "UTF-8");
        }
        if (date1.isNull[i] || date2.isNull[i]) {
          Assert.assertTrue(output.isNull[i]);
        } else {
          Assert.assertEquals(expected,
              new String(output.vector[i], output.start[i], output.start[i] + output.length[i], "UTF-8"));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void testDateAddColCol(VectorExpression.Type colType1, boolean isPositive) {
    LongColumnVector date1 = newRandomLongColumnVector(10000, size);
    LongColumnVector days2 = newRandomLongColumnVector(1000, size);
    ColumnVector col1 = castTo(date1, colType1);

    BytesColumnVector output = new BytesColumnVector(size);

    VectorizedRowBatch batch = new VectorizedRowBatch(3, size);
    batch.cols[0] = col1;
    batch.cols[1] = days2;
    batch.cols[2] = output;

    validateDateAdd(batch, date1, days2, colType1, isPositive);
    TestVectorizedRowBatch.addRandomNulls(date1);
    batch.cols[0] = castTo(date1, colType1);
    validateDateAdd(batch, date1, days2, colType1, isPositive);
    TestVectorizedRowBatch.addRandomNulls(days2);
    batch.cols[1] = days2;
    validateDateAdd(batch, date1, days2, colType1, isPositive);
  }

  @Test
  public void testDateAddColCol() {
    for (VectorExpression.Type colType1 : dateTimestampStringTypes)
      testDateAddColCol(colType1, true);

    VectorExpression udf = new VectorUDFDateAddColCol(0, 1, 2);
    VectorizedRowBatch batch = new VectorizedRowBatch(3, 1);
    BytesColumnVector bcv;
    byte[] bytes = new byte[0];
    try {
      bytes = "error".getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
    }

    udf.setInputTypes(VectorExpression.Type.STRING, VectorExpression.Type.TIMESTAMP);
    batch.cols[0] = new BytesColumnVector(1);
    batch.cols[1] = new LongColumnVector(1);
    batch.cols[2] = new BytesColumnVector(1);
    bcv = (BytesColumnVector) batch.cols[0];
    bcv.vector[0] = bytes;
    bcv.start[0] = 0;
    bcv.length[0] = bytes.length;
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[2].isNull[0], true);
  }

  @Test
  public void testDateSubColCol() {
    for (VectorExpression.Type colType1 : dateTimestampStringTypes)
      testDateAddColCol(colType1, false);

    VectorExpression udf = new VectorUDFDateSubColCol(0, 1, 2);
    VectorizedRowBatch batch = new VectorizedRowBatch(3, 1);
    BytesColumnVector bcv;
    byte[] bytes = new byte[0];
    try {
      bytes = "error".getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
    }

    udf.setInputTypes(VectorExpression.Type.STRING, VectorExpression.Type.TIMESTAMP);
    batch.cols[0] = new BytesColumnVector(1);
    batch.cols[1] = new LongColumnVector(1);
    batch.cols[2] = new BytesColumnVector(1);
    bcv = (BytesColumnVector) batch.cols[0];
    bcv.vector[0] = bytes;
    bcv.start[0] = 0;
    bcv.length[0] = bytes.length;
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[2].isNull[0], true);
  }

  private void validateDateDiff(VectorizedRowBatch batch, long scalar1,
                                VectorExpression.Type scalarType1, VectorExpression.Type colType2,
                                LongColumnVector date2) {
    VectorExpression udf = null;
    switch (scalarType1) {
      case DATE:
        udf = new VectorUDFDateDiffScalarCol(scalar1, 0, 1);
        break;

      case TIMESTAMP:
        udf = new VectorUDFDateDiffScalarCol(toTimestamp(scalar1), 0, 1);
        break;

      case STRING:
        udf = new VectorUDFDateDiffScalarCol(toString(scalar1), 0, 1);
        break;
    }

    udf.setInputTypes(scalarType1, colType2);
    udf.evaluate(batch);

    LongColumnVector output = (LongColumnVector) batch.cols[1];
    for (int i = 0; i < date2.vector.length; i++) {
      Assert.assertEquals(scalar1 - date2.vector[i], output.vector[i]);
    }
  }

  @Test
  public void testDateDiffScalarCol() {
    for (VectorExpression.Type scalarType1 : dateTimestampStringTypes) {
      for (VectorExpression.Type colType2 : dateTimestampStringTypes) {
        LongColumnVector date2 = newRandomLongColumnVector(10000, size);
        LongColumnVector output = new LongColumnVector(size);
        ColumnVector col2 = castTo(date2, colType2);
        VectorizedRowBatch batch = new VectorizedRowBatch(2, size);
        batch.cols[0] = col2;
        batch.cols[1] = output;
        long scalar1 = newRandom(1000);

        validateDateDiff(batch, scalar1, scalarType1, colType2, date2);
        TestVectorizedRowBatch.addRandomNulls(date2);
        batch.cols[0] = castTo(date2, colType2);
        validateDateDiff(batch, scalar1, scalarType1, colType2, date2);
      }
    }

    VectorExpression udf;
    byte[] bytes = new byte[0];
    try {
      bytes = "error".getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
    }
    VectorizedRowBatch batch = new VectorizedRowBatch(2, 1);

    udf = new VectorUDFDateDiffScalarCol(0, 0, 1);
    udf.setInputTypes(VectorExpression.Type.TIMESTAMP, VectorExpression.Type.STRING);
    batch.cols[0] = new BytesColumnVector(1);
    batch.cols[1] = new LongColumnVector(1);

    BytesColumnVector bcv = (BytesColumnVector) batch.cols[0];
    bcv.vector[0] = bytes;
    bcv.start[0] = 0;
    bcv.length[0] = bytes.length;
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[1].isNull[0], true);

    udf = new VectorUDFDateDiffScalarCol(bytes, 0, 1);
    udf.setInputTypes(VectorExpression.Type.STRING, VectorExpression.Type.TIMESTAMP);
    batch.cols[0] = new LongColumnVector(1);
    batch.cols[1] = new LongColumnVector(1);
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[1].isNull[0], true);
  }

  private void validateDateDiff(VectorizedRowBatch batch, LongColumnVector date1, long scalar2,
                                VectorExpression.Type colType1, VectorExpression.Type scalarType2) {
    VectorExpression udf = null;
    switch (scalarType2) {
      case DATE:
        udf = new VectorUDFDateDiffColScalar(0, scalar2, 1);
        break;

      case TIMESTAMP:
        udf = new VectorUDFDateDiffColScalar(0, toTimestamp(scalar2), 1);
        break;

      case STRING:
        udf = new VectorUDFDateDiffColScalar(0, toString(scalar2), 1);
        break;
    }

    udf.setInputTypes(colType1, scalarType2);
    udf.evaluate(batch);

    LongColumnVector output = (LongColumnVector) batch.cols[1];
    for (int i = 0; i < date1.vector.length; i++) {
      Assert.assertEquals(date1.vector[i] - scalar2, output.vector[i]);
    }
  }

  @Test
  public void testDateDiffColScalar() {
    for (VectorExpression.Type colType1 : dateTimestampStringTypes) {
      for (VectorExpression.Type scalarType2 : dateTimestampStringTypes) {
        LongColumnVector date1 = newRandomLongColumnVector(10000, size);
        LongColumnVector output = new LongColumnVector(size);
        VectorizedRowBatch batch = new VectorizedRowBatch(2, size);
        batch.cols[0] = castTo(date1, colType1);
        batch.cols[1] = output;
        long scalar2 = newRandom(1000);

        validateDateDiff(batch, date1, scalar2, colType1, scalarType2);
        TestVectorizedRowBatch.addRandomNulls(date1);
        batch.cols[0] = castTo(date1, colType1);
        validateDateDiff(batch, date1, scalar2, colType1, scalarType2);
      }
    }
    VectorExpression udf;
    byte[] bytes = new byte[0];
    try {
      bytes = "error".getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
    }
    VectorizedRowBatch batch = new VectorizedRowBatch(2, 1);

    udf = new VectorUDFDateDiffColScalar(0, 0, 1);
    udf.setInputTypes(VectorExpression.Type.TIMESTAMP, VectorExpression.Type.STRING);
    batch.cols[0] = new BytesColumnVector(1);
    batch.cols[1] = new LongColumnVector(1);

    BytesColumnVector bcv = (BytesColumnVector) batch.cols[0];
    bcv.vector[0] = bytes;
    bcv.start[0] = 0;
    bcv.length[0] = bytes.length;
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[1].isNull[0], true);

    udf = new VectorUDFDateDiffColScalar(0, bytes, 1);
    udf.setInputTypes(VectorExpression.Type.TIMESTAMP, VectorExpression.Type.STRING);
    batch.cols[0] = new LongColumnVector(1);
    batch.cols[1] = new LongColumnVector(1);
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[1].isNull[0], true);
  }

  private void validateDateDiff(VectorizedRowBatch batch,
                                LongColumnVector date1, LongColumnVector date2,
                                VectorExpression.Type colType1, VectorExpression.Type colType2) {
    VectorExpression udf = new VectorUDFDateDiffColCol(0, 1, 2);
    udf.setInputTypes(colType1, colType2);
    udf.evaluate(batch);
    LongColumnVector output = (LongColumnVector) batch.cols[2];
    for (int i = 0; i < date1.vector.length; i++) {
      if (date1.isNull[i] || date2.isNull[i]) {
        Assert.assertTrue(output.isNull[i]);
      } else {
        Assert.assertEquals(date1.vector[i] - date2.vector[i], output.vector[i]);
      }
    }
  }

  @Test
  public void testDateDiffColCol() {
    for (VectorExpression.Type colType1 : dateTimestampStringTypes) {
      for (VectorExpression.Type colType2 : dateTimestampStringTypes) {
        LongColumnVector date1 = newRandomLongColumnVector(10000, size);
        LongColumnVector date2 = newRandomLongColumnVector(10000, size);
        LongColumnVector output = new LongColumnVector(size);
        VectorizedRowBatch batch = new VectorizedRowBatch(3, size);

        batch.cols[0] = castTo(date1, colType1);
        batch.cols[1] = castTo(date2, colType2);
        batch.cols[2] = output;

        validateDateDiff(batch, date1, date2, colType1, colType2);
        TestVectorizedRowBatch.addRandomNulls(date1);
        batch.cols[0] = castTo(date1, colType1);
        validateDateDiff(batch, date1, date2, colType1, colType2);
        TestVectorizedRowBatch.addRandomNulls(date2);
        batch.cols[1] = castTo(date2, colType2);
        validateDateDiff(batch, date1, date2, colType1, colType2);
      }
    }

    VectorExpression udf = new VectorUDFDateDiffColCol(0, 1, 2);
    VectorizedRowBatch batch = new VectorizedRowBatch(3, 1);
    BytesColumnVector bcv;
    byte[] bytes = new byte[0];
    try {
      bytes = "error".getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
    }

    udf.setInputTypes(VectorExpression.Type.STRING, VectorExpression.Type.TIMESTAMP);
    batch.cols[0] = new BytesColumnVector(1);
    batch.cols[1] = new LongColumnVector(1);
    batch.cols[2] = new LongColumnVector(1);
    bcv = (BytesColumnVector) batch.cols[0];
    bcv.vector[0] = bytes;
    bcv.start[0] = 0;
    bcv.length[0] = bytes.length;
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[2].isNull[0], true);

    udf.setInputTypes(VectorExpression.Type.TIMESTAMP, VectorExpression.Type.STRING);
    batch.cols[0] = new LongColumnVector(1);
    batch.cols[1] = new BytesColumnVector(1);
    batch.cols[2] = new LongColumnVector(1);
    bcv = (BytesColumnVector) batch.cols[1];
    bcv.vector[0] = bytes;
    bcv.start[0] = 0;
    bcv.length[0] = bytes.length;
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[2].isNull[0], true);
  }

  private void validateDate(VectorizedRowBatch batch, VectorExpression.Type colType, LongColumnVector date) {
    VectorExpression udf;
    if (colType == VectorExpression.Type.STRING) {
      udf = new VectorUDFDateString(0, 1);
    } else {
      udf = new VectorUDFDateLong(0, 1);
    }

    udf.setInputTypes(colType);
    udf.evaluate(batch);
    BytesColumnVector output = (BytesColumnVector) batch.cols[1];

    for (int i = 0; i < size; i++) {
      String actual;
      if (output.isNull[i]) {
        actual = null;
      } else {
        try {
          actual = new String(output.vector[i], output.start[i], output.length[i], "UTF-8");
        } catch (UnsupportedEncodingException e) {
          throw new RuntimeException(e);
        }
      }
      if (date.isNull[i]) {
        Assert.assertTrue(output.isNull[i]);
      } else {
        String expected = formatter.format(new Date(DateWritable.daysToMillis((int) date.vector[i])));
        Assert.assertEquals(expected, actual);
      }
    }
  }

  @Test
  public void testDate() {
    for (VectorExpression.Type colType : dateTimestampStringTypes) {
      LongColumnVector date = newRandomLongColumnVector(10000, size);
      BytesColumnVector output = new BytesColumnVector(size);

      VectorizedRowBatch batch = new VectorizedRowBatch(2, size);
      batch.cols[0] = castTo(date, colType);
      batch.cols[1] = output;

      validateDate(batch, colType, date);
      TestVectorizedRowBatch.addRandomNulls(date);
      batch.cols[0] = castTo(date, colType);
      validateDate(batch, colType, date);
    }

    VectorExpression udf = new VectorUDFDateString(0, 1);
    udf.setInputTypes(VectorExpression.Type.STRING);
    VectorizedRowBatch batch = new VectorizedRowBatch(2, 1);
    batch.cols[0] = new BytesColumnVector(1);
    batch.cols[1] = new BytesColumnVector(1);
    BytesColumnVector bcv = (BytesColumnVector) batch.cols[0];
    byte[] bytes = new byte[0];
    try {
      bytes = "error".getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
    }
    bcv.vector[0] = bytes;
    bcv.start[0] = 0;
    bcv.length[0] = bytes.length;
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[1].isNull[0], true);
  }

  private void validateToDate(VectorizedRowBatch batch, VectorExpression.Type colType, LongColumnVector date) {
    VectorExpression udf;
    if (colType == VectorExpression.Type.STRING) {
      udf = new CastStringToDate(0, 1);
    } else {
      udf = new CastLongToDate(0, 1);
    }
    udf.setInputTypes(colType);
    udf.evaluate(batch);
    LongColumnVector output = (LongColumnVector) batch.cols[1];

    for (int i = 0; i < size; i++) {
      long actual = output.vector[i];
      if (date.isNull[i]) {
        Assert.assertTrue(output.isNull[i]);
      } else {
        long expected = date.vector[i];
        Assert.assertEquals(expected, actual);
      }
    }
  }

  @Test
  public void testToDate() {
    for (VectorExpression.Type type :
        Arrays.asList(VectorExpression.Type.TIMESTAMP, VectorExpression.Type.STRING)) {
      LongColumnVector date = newRandomLongColumnVector(10000, size);
      LongColumnVector output = new LongColumnVector(size);

      VectorizedRowBatch batch = new VectorizedRowBatch(2, size);
      batch.cols[0] = castTo(date, type);
      batch.cols[1] = output;

      validateToDate(batch, type, date);
      TestVectorizedRowBatch.addRandomNulls(date);
      batch.cols[0] = castTo(date, type);
      validateToDate(batch, type, date);
    }

    VectorExpression udf = new CastStringToDate(0, 1);
    udf.setInputTypes(VectorExpression.Type.STRING);
    VectorizedRowBatch batch = new VectorizedRowBatch(2, 1);
    batch.cols[0] = new BytesColumnVector(1);
    batch.cols[1] = new LongColumnVector(1);
    BytesColumnVector bcv = (BytesColumnVector) batch.cols[0];
    byte[] bytes = new byte[0];
    try {
      bytes = "error".getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
    }
    bcv.vector[0] = bytes;
    bcv.start[0] = 0;
    bcv.length[0] = bytes.length;
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[1].isNull[0], true);
  }
}
