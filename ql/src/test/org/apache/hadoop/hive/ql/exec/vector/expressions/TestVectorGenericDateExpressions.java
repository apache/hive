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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TestVectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;

public class TestVectorGenericDateExpressions {

  private Charset utf8 = StandardCharsets.UTF_8;
  private int size = 200;
  private Random random = new Random();
  private SimpleDateFormat formatter = getFormatter();
  private List<PrimitiveCategory> dateTimestampStringTypes =
      Arrays.<PrimitiveCategory>asList(PrimitiveCategory.DATE, PrimitiveCategory.TIMESTAMP, PrimitiveCategory.STRING);

  private static SimpleDateFormat getFormatter() {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    return formatter;
  }

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

  private TypeInfo primitiveCategoryToTypeInfo(PrimitiveCategory primitiveCategory) {
    switch (primitiveCategory) {
    case DATE:
      return TypeInfoFactory.dateTypeInfo;
    case STRING:
      return TypeInfoFactory.stringTypeInfo;
    case TIMESTAMP:
      return TypeInfoFactory.timestampTypeInfo;
    default:
      throw new RuntimeException("Unexpected primitive category " + primitiveCategory);
    }
  }
  private TimestampColumnVector toTimestamp(LongColumnVector date) {
    TimestampColumnVector vector = new TimestampColumnVector(size);
    for (int i = 0; i < size; i++) {
      if (date.isNull[i]) {
        vector.isNull[i] = true;
        vector.noNulls = false;
      } else {
        vector.set(i, toTimestamp(date.vector[i]));
      }
    }
    return vector;
  }

  private Timestamp toTimestamp(long date) {
    return new Timestamp(DateWritableV2.daysToMillis((int) date));
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
    String formatted = formatter.format(new Date(DateWritableV2.daysToMillis((int) date)));
    return formatted.getBytes(utf8);
  }

  private void validateDateAdd(VectorizedRowBatch batch, PrimitiveCategory colType1, long scalar2,
                               boolean isPositive, LongColumnVector date1)
                                   throws HiveException {
    VectorUDFDateAddColScalar udf;
    if (isPositive) {
      udf = new VectorUDFDateAddColScalar(0, scalar2, 1);
    } else {
      udf = new VectorUDFDateSubColScalar(0, scalar2, 1);
    }
    udf.setInputTypeInfos(new TypeInfo[] {primitiveCategoryToTypeInfo(colType1), TypeInfoFactory.voidTypeInfo});
    udf.transientInit();
    udf.evaluate(batch);
    LongColumnVector output = (LongColumnVector) batch.cols[1];

    try {
      for (int i = 0; i < size; i++) {
        String expected;
        if (isPositive) {
          expected = new String(toString(date1.vector[i] + scalar2), utf8);
        } else {
          expected = new String(toString(date1.vector[i] - scalar2), utf8);
        }
        if (date1.isNull[i]) {
          Assert.assertTrue(output.isNull[i]);
        } else {
          String actual = new String(toString(output.vector[i]));
          Assert.assertEquals("expectedLen:" + expected.length() + " actualLen:" + actual.length(), expected, actual);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private ColumnVector castTo(LongColumnVector date, PrimitiveCategory type) {
    switch (type) {
      case DATE:
        return date;

      case TIMESTAMP:
        return toTimestamp(date);

      case STRING:
      case CHAR:
      case VARCHAR:
        return toString(date);
      default:
        throw new Error("Unsupported input type " + type.name());
    }
  }

  private void testDateAddColScalar(PrimitiveCategory colType1, boolean isPositive)
      throws HiveException {
    LongColumnVector date1 = newRandomLongColumnVector(10000, size);
    ColumnVector col1 = castTo(date1, colType1);
    long scalar2 = newRandom(1000);
    LongColumnVector output = new LongColumnVector(size);

    VectorizedRowBatch batch = new VectorizedRowBatch(2, size);
    batch.cols[0] = col1;
    batch.cols[1] = output;

    validateDateAdd(batch, colType1, scalar2, isPositive, date1);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    validateDateAdd(batch, colType1, scalar2, isPositive, date1);
  }

  @Test
  public void testDateAddColScalar() throws HiveException {
    for (PrimitiveCategory colType1 : dateTimestampStringTypes)
      testDateAddColScalar(colType1, true);

    VectorExpression udf = new VectorUDFDateAddColScalar(0, 0, 1);
    udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.timestampTypeInfo});
    udf.transientInit();
    VectorizedRowBatch batch = new VectorizedRowBatch(2, 1);
    batch.cols[0] = new BytesColumnVector(1);
    batch.cols[1] = new LongColumnVector(1);
    BytesColumnVector bcv = (BytesColumnVector) batch.cols[0];
    byte[] bytes = "error".getBytes(utf8);
    bcv.vector[0] = bytes;
    bcv.start[0] = 0;
    bcv.length[0] = bytes.length;
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[1].isNull[0], true);
  }

  @Test
  public void testDateSubColScalar() throws HiveException {
    for (PrimitiveCategory colType1 : dateTimestampStringTypes)
      testDateAddColScalar(colType1, false);

    VectorExpression udf = new VectorUDFDateSubColScalar(0, 0, 1);
    udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.timestampTypeInfo});
    udf.transientInit();
    VectorizedRowBatch batch = new VectorizedRowBatch(2, 1);
    batch.cols[0] = new BytesColumnVector(1);
    batch.cols[1] = new LongColumnVector(1);
    BytesColumnVector bcv = (BytesColumnVector) batch.cols[0];
    byte[] bytes = "error".getBytes(utf8);
    bcv.vector[0] = bytes;
    bcv.start[0] = 0;
    bcv.length[0] = bytes.length;
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[1].isNull[0], true);
  }

  private void validateDateAdd(VectorizedRowBatch batch, long scalar1, LongColumnVector date2,
                               PrimitiveCategory colType1, boolean isPositive)
                                  throws HiveException {
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
        case CHAR:
        case VARCHAR:
          udf = new VectorUDFDateAddScalarCol(toString(scalar1), 0, 1);
          break;
        default:
          throw new Error("Invalid input type: " + colType1.name());
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
        case CHAR:
        case VARCHAR:
          udf = new VectorUDFDateSubScalarCol(toString(scalar1), 0, 1);
          break;
        default:
          throw new Error("Invalid input type: " + colType1.name());
      }
    }
    udf.setInputTypeInfos(new TypeInfo[] {primitiveCategoryToTypeInfo(colType1), TypeInfoFactory.voidTypeInfo});
    udf.transientInit();
    udf.evaluate(batch);

    LongColumnVector output = (LongColumnVector) batch.cols[1];
    try {
      for (int i = 0; i < date2.vector.length; i++) {
        String expected;
        if (isPositive) {
          expected = new String(toString(scalar1 + date2.vector[i]), utf8);
        } else {
          expected = new String(toString(scalar1 - date2.vector[i]), utf8);
        }
        if (date2.isNull[i]) {
          Assert.assertTrue(output.isNull[i]);
        } else {
          Assert.assertEquals(expected, new String(toString(output.vector[i])));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void testDateAddScalarCol(PrimitiveCategory colType1, boolean isPositive)
      throws HiveException {
    LongColumnVector date2 = newRandomLongColumnVector(10000, size);
    long scalar1 = newRandom(1000);

    LongColumnVector output = new LongColumnVector(size);

    VectorizedRowBatch batch = new VectorizedRowBatch(2, size);
    batch.cols[0] = date2;
    batch.cols[1] = output;
    validateDateAdd(batch, scalar1, date2, colType1, isPositive);
    TestVectorizedRowBatch.addRandomNulls(date2);
    batch.cols[0] = date2;
    validateDateAdd(batch, scalar1, date2, colType1, isPositive);
  }

  @Test
  public void testDateAddScalarCol() throws HiveException {
    for (PrimitiveCategory scalarType1 : dateTimestampStringTypes)
      testDateAddScalarCol(scalarType1, true);

    VectorExpression udf = new VectorUDFDateAddScalarCol("error".getBytes(utf8), 0, 1);
    udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.timestampTypeInfo});
    udf.transientInit();
    VectorizedRowBatch batch = new VectorizedRowBatch(2, 1);
    batch.cols[0] = new LongColumnVector(1);
    batch.cols[1] = new LongColumnVector(1);
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[1].isNull[0], true);
  }

  @Test
  public void testDateSubScalarCol() throws HiveException {
    for (PrimitiveCategory scalarType1 : dateTimestampStringTypes)
      testDateAddScalarCol(scalarType1, false);

    VectorExpression udf = new VectorUDFDateSubScalarCol("error".getBytes(utf8), 0, 1);
    udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.timestampTypeInfo});
    udf.transientInit();
    VectorizedRowBatch batch = new VectorizedRowBatch(2, 1);
    batch.cols[0] = new LongColumnVector(1);
    batch.cols[1] = new LongColumnVector(1);
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[1].isNull[0], true);
  }

  private void validateDateAdd(VectorizedRowBatch batch,
                               LongColumnVector date1, LongColumnVector date2,
                               PrimitiveCategory colType1, boolean isPositive)
                                   throws HiveException {
    VectorExpression udf;
    if (isPositive) {
      udf = new VectorUDFDateAddColCol(0, 1, 2);
    } else {
      udf = new VectorUDFDateSubColCol(0, 1, 2);
    }
    udf.setInputTypeInfos(new TypeInfo[] {primitiveCategoryToTypeInfo(colType1), TypeInfoFactory.voidTypeInfo});
    udf.transientInit();
    udf.evaluate(batch);
    LongColumnVector output = (LongColumnVector) batch.cols[2];
    try {
      for (int i = 0; i < date2.vector.length; i++) {
        String expected;
        if (isPositive) {
          expected = new String(toString(date1.vector[i] + date2.vector[i]), utf8);
        } else {
          expected = new String(toString(date1.vector[i] - date2.vector[i]), utf8);
        }
        if (date1.isNull[i] || date2.isNull[i]) {
          Assert.assertTrue(output.isNull[i]);
        } else {
          Assert.assertEquals(expected, new String(toString(output.vector[i])));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void testDateAddColCol(PrimitiveCategory colType1, boolean isPositive)
      throws HiveException {
    LongColumnVector date1 = newRandomLongColumnVector(10000, size);
    LongColumnVector days2 = newRandomLongColumnVector(1000, size);
    ColumnVector col1 = castTo(date1, colType1);

    LongColumnVector output = new LongColumnVector(size);

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
  public void testDateAddColCol() throws HiveException {
    for (PrimitiveCategory colType1 : dateTimestampStringTypes)
      testDateAddColCol(colType1, true);

    VectorExpression udf = new VectorUDFDateAddColCol(0, 1, 2);
    VectorizedRowBatch batch = new VectorizedRowBatch(3, 1);
    BytesColumnVector bcv;
    byte[] bytes = "error".getBytes(utf8);

    udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.timestampTypeInfo});
    udf.transientInit();
    batch.cols[0] = new BytesColumnVector(1);
    batch.cols[1] = new LongColumnVector(1);
    batch.cols[2] = new LongColumnVector(1);
    bcv = (BytesColumnVector) batch.cols[0];
    bcv.vector[0] = bytes;
    bcv.start[0] = 0;
    bcv.length[0] = bytes.length;
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[2].isNull[0], true);
  }

  @Test
  public void testDateSubColCol() throws HiveException {
    for (PrimitiveCategory colType1 : dateTimestampStringTypes)
      testDateAddColCol(colType1, false);

    VectorExpression udf = new VectorUDFDateSubColCol(0, 1, 2);
    VectorizedRowBatch batch = new VectorizedRowBatch(3, 1);
    BytesColumnVector bcv;
    byte[] bytes = "error".getBytes(utf8);

    udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.timestampTypeInfo});
    udf.transientInit();
    batch.cols[0] = new BytesColumnVector(1);
    batch.cols[1] = new LongColumnVector(1);
    batch.cols[2] = new LongColumnVector(1);
    bcv = (BytesColumnVector) batch.cols[0];
    bcv.vector[0] = bytes;
    bcv.start[0] = 0;
    bcv.length[0] = bytes.length;
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[2].isNull[0], true);
  }

  private void validateDateDiff(VectorizedRowBatch batch, long scalar1,
                                PrimitiveCategory scalarType1, PrimitiveCategory colType2,
                                LongColumnVector date2)
                                    throws HiveException {
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

    udf.setInputTypeInfos(
        new TypeInfo[] {primitiveCategoryToTypeInfo(scalarType1), primitiveCategoryToTypeInfo(colType2)});
    udf.transientInit();
    udf.evaluate(batch);

    LongColumnVector output = (LongColumnVector) batch.cols[1];
    for (int i = 0; i < date2.vector.length; i++) {
      Assert.assertEquals(scalar1 - date2.vector[i], output.vector[i]);
    }
  }

  @Test
  public void testDateDiffScalarCol() throws HiveException {
    for (PrimitiveCategory scalarType1 : dateTimestampStringTypes) {
      for (PrimitiveCategory colType2 : dateTimestampStringTypes) {
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
    byte[] bytes = "error".getBytes(utf8);
    VectorizedRowBatch batch = new VectorizedRowBatch(2, 1);

    udf = new VectorUDFDateDiffScalarCol(new Timestamp(0), 0, 1);
    udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.timestampTypeInfo, TypeInfoFactory.stringTypeInfo});
    udf.transientInit();
    batch.cols[0] = new BytesColumnVector(1);
    batch.cols[1] = new LongColumnVector(1);

    BytesColumnVector bcv = (BytesColumnVector) batch.cols[0];
    bcv.vector[0] = bytes;
    bcv.start[0] = 0;
    bcv.length[0] = bytes.length;
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[1].isNull[0], true);

    udf = new VectorUDFDateDiffScalarCol(bytes, 0, 1);
    udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.timestampTypeInfo});
    udf.transientInit();
    batch.cols[0] = new LongColumnVector(1);
    batch.cols[1] = new LongColumnVector(1);
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[1].isNull[0], true);
  }

  private void validateDateDiff(VectorizedRowBatch batch, LongColumnVector date1, long scalar2,
                                PrimitiveCategory colType1, PrimitiveCategory scalarType2)
                                    throws HiveException {
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

    udf.setInputTypeInfos(new TypeInfo[] {primitiveCategoryToTypeInfo(colType1), primitiveCategoryToTypeInfo(scalarType2)});
    udf.transientInit();
    udf.evaluate(batch);

    LongColumnVector output = (LongColumnVector) batch.cols[1];
    for (int i = 0; i < date1.vector.length; i++) {
      Assert.assertEquals(date1.vector[i] - scalar2, output.vector[i]);
    }
  }

  @Test
  public void testDateDiffColScalar() throws HiveException {
    for (PrimitiveCategory colType1 : dateTimestampStringTypes) {
      for (PrimitiveCategory scalarType2 : dateTimestampStringTypes) {
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
    byte[] bytes = "error".getBytes(utf8);
    VectorizedRowBatch batch = new VectorizedRowBatch(2, 1);

    udf = new VectorUDFDateDiffColScalar(0, 0L, 1);
    udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.timestampTypeInfo, TypeInfoFactory.stringTypeInfo});
    batch.cols[0] = new BytesColumnVector(1);
    batch.cols[1] = new LongColumnVector(1);

    BytesColumnVector bcv = (BytesColumnVector) batch.cols[0];
    bcv.vector[0] = bytes;
    bcv.start[0] = 0;
    bcv.length[0] = bytes.length;
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[1].isNull[0], true);

    udf = new VectorUDFDateDiffColScalar(0, bytes, 1);
    udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.timestampTypeInfo, TypeInfoFactory.stringTypeInfo});
    udf.transientInit();
    batch.cols[0] = new LongColumnVector(1);
    batch.cols[1] = new LongColumnVector(1);
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[1].isNull[0], true);
  }

  private void validateDateDiff(VectorizedRowBatch batch,
                                LongColumnVector date1, LongColumnVector date2,
                                PrimitiveCategory colType1, PrimitiveCategory colType2)
                                    throws HiveException {
    VectorExpression udf = new VectorUDFDateDiffColCol(0, 1, 2);
    udf.setInputTypeInfos(new TypeInfo[] {primitiveCategoryToTypeInfo(colType1), primitiveCategoryToTypeInfo(colType2)});
    udf.transientInit();
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
  public void testDateDiffColCol() throws HiveException {
    for (PrimitiveCategory colType1 : dateTimestampStringTypes) {
      for (PrimitiveCategory colType2 : dateTimestampStringTypes) {
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
    byte[] bytes = "error".getBytes(utf8);

    udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.timestampTypeInfo});
    udf.transientInit();
    batch.cols[0] = new BytesColumnVector(1);
    batch.cols[1] = new TimestampColumnVector(1);
    batch.cols[2] = new LongColumnVector(1);
    bcv = (BytesColumnVector) batch.cols[0];
    bcv.vector[0] = bytes;
    bcv.start[0] = 0;
    bcv.length[0] = bytes.length;
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[2].isNull[0], true);

    udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.timestampTypeInfo, TypeInfoFactory.stringTypeInfo});
    udf.transientInit();
    batch.cols[0] = new TimestampColumnVector(1);
    batch.cols[1] = new BytesColumnVector(1);
    batch.cols[2] = new LongColumnVector(1);
    bcv = (BytesColumnVector) batch.cols[1];
    bcv.vector[0] = bytes;
    bcv.start[0] = 0;
    bcv.length[0] = bytes.length;
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[2].isNull[0], true);
  }

  private void validateDate(VectorizedRowBatch batch, PrimitiveCategory colType,
      LongColumnVector date) throws HiveException {
    VectorExpression udf;
    if (colType == PrimitiveCategory.STRING) {
      udf = new VectorUDFDateString(0, 1);
    } else if (colType == PrimitiveCategory.TIMESTAMP) {
      udf = new VectorUDFDateTimestamp(0, 1);
    } else {
      udf = new VectorUDFDateLong(0, 1);
    }

    udf.setInputTypeInfos(new TypeInfo[] {primitiveCategoryToTypeInfo(colType)});
    udf.transientInit();
    udf.evaluate(batch);
    LongColumnVector output = (LongColumnVector) batch.cols[1];

    for (int i = 0; i < size; i++) {
      String actual;
      if (output.isNull[i]) {
        actual = null;
      } else {
        actual = new String(toString(output.vector[i]));
      }
      if (date.isNull[i]) {
        Assert.assertTrue(output.isNull[i]);
      } else {
        String expected = formatter.format(new Date(DateWritableV2.daysToMillis((int) date.vector[i])));
        Assert.assertEquals(expected, actual);
      }
    }
  }

  @Test
  public void testDate() throws HiveException {
    for (PrimitiveCategory colType : dateTimestampStringTypes) {
      LongColumnVector date = newRandomLongColumnVector(10000, size);
      LongColumnVector output = new LongColumnVector(size);

      VectorizedRowBatch batch = new VectorizedRowBatch(2, size);
      batch.cols[0] = castTo(date, colType);
      batch.cols[1] = output;

      validateDate(batch, colType, date);
      TestVectorizedRowBatch.addRandomNulls(date);
      batch.cols[0] = castTo(date, colType);
      validateDate(batch, colType, date);
    }

    VectorExpression udf = new VectorUDFDateString(0, 1);
    udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.stringTypeInfo});
    udf.transientInit();
    VectorizedRowBatch batch = new VectorizedRowBatch(2, 1);
    batch.cols[0] = new BytesColumnVector(1);
    batch.cols[1] = new LongColumnVector(1);
    BytesColumnVector bcv = (BytesColumnVector) batch.cols[0];
    byte[] bytes = "error".getBytes(utf8);
    bcv.vector[0] = bytes;
    bcv.start[0] = 0;
    bcv.length[0] = bytes.length;
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[1].isNull[0], true);
  }

  private void validateToDate(VectorizedRowBatch batch, PrimitiveCategory colType,
      LongColumnVector date) throws HiveException {
    VectorExpression udf;
    if (colType == PrimitiveCategory.STRING ||
        colType == PrimitiveCategory.CHAR ||
        colType == PrimitiveCategory.VARCHAR) {
      udf = new CastStringToDate(0, 1);
    } else if (colType == PrimitiveCategory.TIMESTAMP) {
      udf = new CastTimestampToDate(0, 1);
    } else {
      udf = new CastLongToDate(0, 1);
    }
    udf.setInputTypeInfos(new TypeInfo[] {primitiveCategoryToTypeInfo(colType)});
    udf.transientInit();
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
  public void testToDate() throws HiveException {
    for (PrimitiveCategory type :
        Arrays.asList(PrimitiveCategory.TIMESTAMP, PrimitiveCategory.STRING)) {
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
    udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.stringTypeInfo});
    udf.transientInit();
    VectorizedRowBatch batch = new VectorizedRowBatch(2, 1);
    batch.cols[0] = new BytesColumnVector(1);
    batch.cols[1] = new LongColumnVector(1);
    BytesColumnVector bcv = (BytesColumnVector) batch.cols[0];
    byte[] bytes = "error".getBytes(utf8);
    bcv.vector[0] = bytes;
    bcv.start[0] = 0;
    bcv.length[0] = bytes.length;
    udf.evaluate(batch);
    Assert.assertEquals(batch.cols[1].isNull[0], true);
  }
}
