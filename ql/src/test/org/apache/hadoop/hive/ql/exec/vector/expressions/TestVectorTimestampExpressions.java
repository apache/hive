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

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;
import java.util.TimeZone;

import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.hive.common.type.TimestampUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;

import org.apache.hadoop.hive.ql.udf.UDFDayOfMonth;
import org.apache.hadoop.hive.ql.udf.UDFHour;
import org.apache.hadoop.hive.ql.udf.UDFMinute;
import org.apache.hadoop.hive.ql.udf.UDFMonth;
import org.apache.hadoop.hive.ql.udf.UDFSecond;
import org.apache.hadoop.hive.ql.udf.UDFYear;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.RandomTypeUtil;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TestVectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFWeekOfYear;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * Unit tests for timestamp expressions.
 */
public class TestVectorTimestampExpressions {

  private HiveConf hiveConf = new HiveConf();
  private SimpleDateFormat dateFormat = getFormatter();

  private static SimpleDateFormat getFormatter() {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    return formatter;
  }

  private Timestamp[] getAllBoundaries(int minYear, int maxYear) {
     ArrayList<Timestamp> boundaries = new ArrayList<Timestamp>(1);
    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(0); // c.set doesn't reset millis
    for (int year = minYear; year <= maxYear; year++) {
      c.set(year, Calendar.JANUARY, 1, 0, 0, 0);
      if (c.get(Calendar.YEAR) < 0 || c.get(Calendar.YEAR) >= 10000) {
        continue;
      }
      long exactly = c.getTimeInMillis();
      /* one second before and after */
      long before = exactly - 1000;
      long after = exactly + 1000;
      if (minYear != 0) {
        boundaries.add(new Timestamp(before));
      }
      boundaries.add(new Timestamp(exactly));
      if (year != maxYear) {
        boundaries.add(new Timestamp(after));
      }
    }
    return boundaries.toArray(new Timestamp[0]);
  }

  private Timestamp[] getAllBoundaries() {
    return getAllBoundaries(RandomTypeUtil.MIN_YEAR, RandomTypeUtil.MAX_YEAR);
  }

  private VectorizedRowBatch getVectorizedRandomRowBatchTimestampLong(int seed, int size) {
    VectorizedRowBatch batch = new VectorizedRowBatch(2, size);
    TimestampColumnVector tcv = new TimestampColumnVector(size);
    Random rand = new Random(seed);
    for (int i = 0; i < size; i++) {
      tcv.set(i, RandomTypeUtil.getRandTimestamp(rand).toSqlTimestamp());
    }
    batch.cols[0] = tcv;
    batch.cols[1] = new LongColumnVector(size);
    batch.size = size;
    return batch;
  }

  private VectorizedRowBatch getVectorizedRandomRowBatchStringLong(int seed, int size) {
    VectorizedRowBatch batch = new VectorizedRowBatch(2, size);
    BytesColumnVector bcv = new BytesColumnVector(size);
    Random rand = new Random(seed);
    for (int i = 0; i < size; i++) {
      /* all 32 bit numbers qualify & multiply up to get nano-seconds */
      byte[] encoded = encodeTime(RandomTypeUtil.getRandTimestamp(rand).toSqlTimestamp());
      bcv.vector[i] = encoded;
      bcv.start[i] = 0;
      bcv.length[i] = encoded.length;
    }
    batch.cols[0] = bcv;
    batch.cols[1] = new LongColumnVector(size);
    batch.size = size;
    return batch;
  }

  private VectorizedRowBatch getVectorizedRandomRowBatch(int seed, int size, TestType testType) {
    switch (testType) {
      case TIMESTAMP_LONG:
        return getVectorizedRandomRowBatchTimestampLong(seed, size);
      case STRING_LONG:
        return getVectorizedRandomRowBatchStringLong(seed, size);
      default:
        throw new IllegalArgumentException();
    }
  }

  /**
   * Input array is used to fill the entire size of the vector row batch
   */
  private VectorizedRowBatch getVectorizedRowBatchTimestampLong(Timestamp[] inputs, int size) {
    VectorizedRowBatch batch = new VectorizedRowBatch(2, size);
    TimestampColumnVector tcv = new TimestampColumnVector(size);
    for (int i = 0; i < size; i++) {
      tcv.set(i, inputs[i % inputs.length]);
    }
    batch.cols[0] = tcv;
    batch.cols[1] = new LongColumnVector(size);
    batch.size = size;
    return batch;
  }

  /*
   * Input array is used to fill the entire size of the vector row batch
   */
  private VectorizedRowBatch getVectorizedRowBatchStringLong(Timestamp[] inputs, int size) {
    VectorizedRowBatch batch = new VectorizedRowBatch(2, size);
    BytesColumnVector bcv = new BytesColumnVector(size);
    for (int i = 0; i < size; i++) {
      byte[] encoded = encodeTime(inputs[i % inputs.length]);
      bcv.vector[i] = encoded;
      bcv.start[i] = 0;
      bcv.length[i] = encoded.length;
    }
    batch.cols[0] = bcv;
    batch.cols[1] = new LongColumnVector(size);
    batch.size = size;
    return batch;
  }

  private VectorizedRowBatch getVectorizedRowBatchStringLong(byte[] vector, int start, int length) {
    VectorizedRowBatch batch = new VectorizedRowBatch(2, 1);
    BytesColumnVector bcv = new BytesColumnVector(1);

    bcv.vector[0] = vector;
    bcv.start[0] = start;
    bcv.length[0] = length;

    batch.cols[0] = bcv;
    batch.cols[1] = new LongColumnVector(1);
    batch.size = 1;
    return batch;
  }

  private VectorizedRowBatch getVectorizedRowBatch(Timestamp[] inputs, int size, TestType testType) {
    switch (testType) {
      case TIMESTAMP_LONG:
        return getVectorizedRowBatchTimestampLong(inputs, size);
      case STRING_LONG:
        return getVectorizedRowBatchStringLong(inputs, size);
      default:
        throw new IllegalArgumentException();
    }
  }

  private byte[] encodeTime(Timestamp timestamp) {
    ByteBuffer encoded;
    long time = timestamp.getTime();
    try {
      String formatted = dateFormat.format(new Date(time));
      encoded = Text.encode(formatted);
    } catch (CharacterCodingException e) {
      throw new RuntimeException(e);
    }
    return Arrays.copyOf(encoded.array(), encoded.limit());
  }

  private Timestamp decodeTime(byte[] time) {
    try {
      return new Timestamp(dateFormat.parse(Text.decode(time)).getTime());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Timestamp readVectorElementAt(ColumnVector col, int i) {
    if (col instanceof TimestampColumnVector) {
      return ((TimestampColumnVector) col).asScratchTimestamp(i);
    }
    if (col instanceof BytesColumnVector) {
      byte[] timeBytes = ((BytesColumnVector) col).vector[i];
      return decodeTime(timeBytes);
    }
    throw new IllegalArgumentException();
  }

  private enum TestType {
    TIMESTAMP_LONG, STRING_LONG
  }

  private void compareToUDFYearLong(Timestamp t, int y) throws HiveException {
    UDFYear udf = new UDFYear();
    udf.initialize(new ObjectInspector[]{PrimitiveObjectInspectorFactory.writableTimestampObjectInspector});
    TimestampWritableV2 tsw = new TimestampWritableV2(
        org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(t.getTime(), t.getNanos()));
    IntWritable res = (IntWritable) udf.evaluate(
        new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(tsw)});
    Assert.assertEquals(res.get(), y);
  }

  private void verifyUDFYear(VectorizedRowBatch batch, TestType testType)
      throws HiveException {
    VectorExpression udf = null;
    if (testType == TestType.TIMESTAMP_LONG) {
      udf = new VectorUDFYearTimestamp(0, 1);
      udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.timestampTypeInfo});
    } else {
      udf = new VectorUDFYearString(0, 1);
      udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.stringTypeInfo});
    }
    udf.transientInit(hiveConf);
    udf.evaluate(batch);
    final int in = 0;
    final int out = 1;

    for (int i = 0; i < batch.size; i++) {
      if (batch.cols[in].noNulls || !batch.cols[in].isNull[i]) {
        if (!batch.cols[in].noNulls) {
          Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
        }
        Timestamp t = readVectorElementAt(batch.cols[in], i);
        long y = ((LongColumnVector) batch.cols[out]).vector[i];
        compareToUDFYearLong(t, (int) y);
      } else {
        Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
      }
    }
  }

  private void testVectorUDFYear(TestType testType) throws HiveException {
    VectorizedRowBatch batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)},
            VectorizedRowBatch.DEFAULT_SIZE, testType);
    Assert.assertTrue(((LongColumnVector) batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector) batch.cols[1]).isRepeating);
    verifyUDFYear(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFYear(batch, testType);

    Timestamp[] boundaries = getAllBoundaries();
    batch = getVectorizedRowBatch(boundaries, boundaries.length, testType);
    verifyUDFYear(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFYear(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFYear(batch, testType);

    batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)}, 1, testType);
    batch.cols[0].isRepeating = true;
    verifyUDFYear(batch, testType);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFYear(batch, testType);

    batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)}, 1, testType);
    batch.cols[0].isRepeating = true;
    batch.selectedInUse = true;
    batch.selected = new int[] {42};
    verifyUDFYear(batch, testType);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFYear(batch, testType);

    batch = getVectorizedRandomRowBatch(200, VectorizedRowBatch.DEFAULT_SIZE, testType);
    verifyUDFYear(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFYear(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFYear(batch, testType);
  }

  @Test
  public void testVectorUDFYearTimestamp() throws HiveException {
    testVectorUDFYear(TestType.TIMESTAMP_LONG);
  }

  @Test
  public void testVectorUDFYearString() throws HiveException {
    testVectorUDFYear(TestType.STRING_LONG);

    VectorizedRowBatch batch = getVectorizedRowBatchStringLong(new byte[] {'2', '2', '0', '1', '3'}, 1, 3);
    VectorExpression udf = new VectorUDFYearString(0, 1);
    udf.evaluate(batch);
    LongColumnVector lcv = (LongColumnVector) batch.cols[1];
    Assert.assertEquals(false, batch.cols[0].isNull[0]);
    Assert.assertEquals(true, lcv.isNull[0]);
  }

  private void compareToUDFDayOfMonthLong(Timestamp t, int y) throws HiveException {
    UDFDayOfMonth udf = new UDFDayOfMonth();
    udf.initialize(new ObjectInspector[]{PrimitiveObjectInspectorFactory.writableTimestampObjectInspector});
    TimestampWritableV2 tsw = new TimestampWritableV2(
        org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(t.getTime(), t.getNanos()));
    IntWritable res = (IntWritable) udf.evaluate(
        new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(tsw)});
    Assert.assertEquals(res.get(), y);
  }

  private void verifyUDFDayOfMonth(VectorizedRowBatch batch, TestType testType)
      throws HiveException {
    VectorExpression udf = null;
    if (testType == TestType.TIMESTAMP_LONG) {
      udf = new VectorUDFDayOfMonthTimestamp(0, 1);
      udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.timestampTypeInfo});
    } else {
      udf = new VectorUDFDayOfMonthString(0, 1);
      udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.stringTypeInfo});
    }
    udf.transientInit(hiveConf);
    udf.evaluate(batch);
    final int in = 0;
    final int out = 1;

    for (int i = 0; i < batch.size; i++) {
      if (batch.cols[in].noNulls || !batch.cols[in].isNull[i]) {
        if (!batch.cols[in].noNulls) {
          Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
        }
        Timestamp t = readVectorElementAt(batch.cols[in], i);
        long y = ((LongColumnVector) batch.cols[out]).vector[i];
        compareToUDFDayOfMonthLong(t, (int) y);
      } else {
        Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
      }
    }
  }

  private void testVectorUDFDayOfMonth(TestType testType) throws HiveException {
    VectorizedRowBatch batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)},
            VectorizedRowBatch.DEFAULT_SIZE, testType);
    Assert.assertTrue(((LongColumnVector) batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector) batch.cols[1]).isRepeating);
    verifyUDFDayOfMonth(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFDayOfMonth(batch, testType);

    Timestamp[] boundaries = getAllBoundaries();
    batch = getVectorizedRowBatch(boundaries, boundaries.length, testType);
    verifyUDFDayOfMonth(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFDayOfMonth(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFDayOfMonth(batch, testType);

    batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)}, 1, testType);
    batch.cols[0].isRepeating = true;
    verifyUDFDayOfMonth(batch, testType);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFDayOfMonth(batch, testType);

    batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)}, 1, testType);
    batch.cols[0].isRepeating = true;
    batch.selectedInUse = true;
    batch.selected = new int[] {42};
    verifyUDFDayOfMonth(batch, testType);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFDayOfMonth(batch, testType);

    batch = getVectorizedRandomRowBatch(200, VectorizedRowBatch.DEFAULT_SIZE, testType);
    verifyUDFDayOfMonth(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFDayOfMonth(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFDayOfMonth(batch, testType);
  }

  @Test
  public void testVectorUDFDayOfMonthTimestamp() throws HiveException {
    testVectorUDFDayOfMonth(TestType.TIMESTAMP_LONG);
  }

  @Test
  public void testVectorUDFDayOfMonthString() throws HiveException {
    testVectorUDFDayOfMonth(TestType.STRING_LONG);
  }

  private void compareToUDFHourLong(Timestamp t, int y) throws HiveException {
    UDFHour udf = new UDFHour();
    udf.initialize(new ObjectInspector[]{PrimitiveObjectInspectorFactory.writableTimestampObjectInspector});
    TimestampWritableV2 tsw = new TimestampWritableV2(
        org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(t.getTime(), t.getNanos()));
    IntWritable res = (IntWritable) udf.evaluate(
        new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(tsw)});
    Assert.assertEquals(res.get(), y);
  }

  private void verifyUDFHour(VectorizedRowBatch batch, TestType testType) throws HiveException {
    VectorExpression udf = null;
    if (testType == TestType.TIMESTAMP_LONG) {
      udf = new VectorUDFHourTimestamp(0, 1);
      udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.timestampTypeInfo});
    } else {
      udf = new VectorUDFHourString(0, 1);
      udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.stringTypeInfo});
    }
    udf.transientInit(hiveConf);
    udf.evaluate(batch);
    final int in = 0;
    final int out = 1;

    for (int i = 0; i < batch.size; i++) {
      if (batch.cols[in].noNulls || !batch.cols[in].isNull[i]) {
        if (!batch.cols[in].noNulls) {
          Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
        }
        Timestamp t = readVectorElementAt(batch.cols[in], i);
        long y = ((LongColumnVector) batch.cols[out]).vector[i];
        compareToUDFHourLong(t, (int) y);
      } else {
        Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
      }
    }
  }

  private void testVectorUDFHour(TestType testType) throws HiveException {
    VectorizedRowBatch batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)},
            VectorizedRowBatch.DEFAULT_SIZE, testType);
    Assert.assertTrue(((LongColumnVector) batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector) batch.cols[1]).isRepeating);
    verifyUDFHour(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFHour(batch, testType);

    Timestamp[] boundaries = getAllBoundaries();
    batch = getVectorizedRowBatch(boundaries, boundaries.length, testType);
    verifyUDFHour(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFHour(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFHour(batch, testType);

    batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)}, 1, testType);
    batch.cols[0].isRepeating = true;
    verifyUDFHour(batch, testType);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFHour(batch, testType);

    batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)}, 1, testType);
    batch.cols[0].isRepeating = true;
    batch.selectedInUse = true;
    batch.selected = new int[] {42};
    verifyUDFHour(batch, testType);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFHour(batch, testType);

    batch = getVectorizedRandomRowBatch(200, VectorizedRowBatch.DEFAULT_SIZE, testType);
    verifyUDFHour(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFHour(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFHour(batch, testType);
  }

  @Test
  public void testVectorUDFHourTimestamp() throws HiveException {
    testVectorUDFHour(TestType.TIMESTAMP_LONG);
  }

  @Test
  public void testVectorUDFHourString() throws HiveException {
    testVectorUDFHour(TestType.STRING_LONG);
  }

  private void compareToUDFMinuteLong(Timestamp t, int y) throws HiveException {
    UDFMinute udf = new UDFMinute();
    udf.initialize(new ObjectInspector[]{PrimitiveObjectInspectorFactory.writableTimestampObjectInspector});
    TimestampWritableV2 tsw = new TimestampWritableV2(
        org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(t.getTime(), t.getNanos()));
    IntWritable res = (IntWritable) udf.evaluate(
        new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(tsw)});
    Assert.assertEquals(res.get(), y);
  }

  private void verifyUDFMinute(VectorizedRowBatch batch, TestType testType)
      throws HiveException {
    VectorExpression udf = null;
    if (testType == TestType.TIMESTAMP_LONG) {
      udf = new VectorUDFMinuteTimestamp(0, 1);
      udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.timestampTypeInfo});
    } else {
      udf = new VectorUDFMinuteString(0, 1);
      udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.stringTypeInfo});
    }
    udf.transientInit(hiveConf);
    udf.evaluate(batch);
    final int in = 0;
    final int out = 1;

    for (int i = 0; i < batch.size; i++) {
      if (batch.cols[in].noNulls || !batch.cols[in].isNull[i]) {
        if (!batch.cols[in].noNulls) {
          Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
        }
        Timestamp t = readVectorElementAt(batch.cols[in], i);
        long y = ((LongColumnVector) batch.cols[out]).vector[i];
        compareToUDFMinuteLong(t, (int) y);
      } else {
        Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
      }
    }
  }

  private void testVectorUDFMinute(TestType testType) throws HiveException {
    VectorizedRowBatch batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)},
            VectorizedRowBatch.DEFAULT_SIZE, testType);
    Assert.assertTrue(((LongColumnVector) batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector) batch.cols[1]).isRepeating);
    verifyUDFMinute(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFMinute(batch, testType);

    Timestamp[] boundaries = getAllBoundaries();
    batch = getVectorizedRowBatch(boundaries, boundaries.length, testType);
    verifyUDFMinute(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFMinute(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFMinute(batch, testType);

    batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)}, 1, testType);
    batch.cols[0].isRepeating = true;
    verifyUDFMinute(batch, testType);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFMinute(batch, testType);

    batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)}, 1, testType);
    batch.cols[0].isRepeating = true;
    batch.selectedInUse = true;
    batch.selected = new int[] {42};
    verifyUDFMinute(batch, testType);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFMinute(batch, testType);

    batch = getVectorizedRandomRowBatch(200, VectorizedRowBatch.DEFAULT_SIZE, testType);
    verifyUDFMinute(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFMinute(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFMinute(batch, testType);
  }

  @Test
  public void testVectorUDFMinuteLong() throws HiveException {
    testVectorUDFMinute(TestType.TIMESTAMP_LONG);
  }

  @Test
  public void testVectorUDFMinuteString() throws HiveException {
    testVectorUDFMinute(TestType.STRING_LONG);
  }

  private void compareToUDFMonthLong(Timestamp t, int y) throws HiveException {
    UDFMonth udf = new UDFMonth();
    udf.initialize(new ObjectInspector[]{PrimitiveObjectInspectorFactory.writableTimestampObjectInspector});
    TimestampWritableV2 tsw = new TimestampWritableV2(
        org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(t.getTime(), t.getNanos()));
    IntWritable res = (IntWritable) udf.evaluate(
        new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(tsw)});
    Assert.assertEquals(res.get(), y);
  }

  private void verifyUDFMonth(VectorizedRowBatch batch, TestType testType) throws HiveException {
    VectorExpression udf;
    if (testType == TestType.TIMESTAMP_LONG) {
      udf = new VectorUDFMonthTimestamp(0, 1);
      udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.timestampTypeInfo});
    } else {
      udf = new VectorUDFMonthString(0, 1);
      udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.stringTypeInfo});
    }
    udf.transientInit(hiveConf);
    udf.evaluate(batch);
    final int in = 0;
    final int out = 1;

    for (int i = 0; i < batch.size; i++) {
      if (batch.cols[in].noNulls || !batch.cols[in].isNull[i]) {
        if (!batch.cols[in].noNulls) {
          Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
        }
        Timestamp t = readVectorElementAt(batch.cols[in], i);
        long y = ((LongColumnVector) batch.cols[out]).vector[i];
        compareToUDFMonthLong(t, (int) y);
      } else {
        Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
      }
    }
  }

  private void testVectorUDFMonth(TestType testType) throws HiveException {
    VectorizedRowBatch batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)},
            VectorizedRowBatch.DEFAULT_SIZE, testType);
    Assert.assertTrue(((LongColumnVector) batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector) batch.cols[1]).isRepeating);
    verifyUDFMonth(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFMonth(batch, testType);

    Timestamp[] boundaries = getAllBoundaries();
    batch = getVectorizedRowBatch(boundaries, boundaries.length, testType);
    verifyUDFMonth(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFMonth(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFMonth(batch, testType);

    batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)}, 1, testType);
    batch.cols[0].isRepeating = true;
    verifyUDFMonth(batch, testType);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFMonth(batch, testType);

    batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)}, 1, testType);
    batch.cols[0].isRepeating = true;
    batch.selectedInUse = true;
    batch.selected = new int[] {42};
    verifyUDFMonth(batch, testType);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFMonth(batch, testType);

    batch = getVectorizedRandomRowBatch(200, VectorizedRowBatch.DEFAULT_SIZE, testType);
    verifyUDFMonth(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFMonth(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFMonth(batch, testType);
  }

  @Test
  public void testVectorUDFMonthTimestamp() throws HiveException {
    testVectorUDFMonth(TestType.TIMESTAMP_LONG);
  }

  @Test
  public void testVectorUDFMonthString() throws HiveException {
    testVectorUDFMonth(TestType.STRING_LONG);
  }

  private void compareToUDFSecondLong(Timestamp t, int y) throws HiveException {
    UDFSecond udf = new UDFSecond();
    udf.initialize(new ObjectInspector[]{PrimitiveObjectInspectorFactory.writableTimestampObjectInspector});
    TimestampWritableV2 tsw = new TimestampWritableV2(
        org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(t.getTime(), t.getNanos()));
    IntWritable res = (IntWritable) udf.evaluate(
        new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(tsw)});
    Assert.assertEquals(res.get(), y);
  }

  private void verifyUDFSecond(VectorizedRowBatch batch, TestType testType) throws HiveException {
    VectorExpression udf;
    if (testType == TestType.TIMESTAMP_LONG) {
      udf = new VectorUDFSecondTimestamp(0, 1);
      udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.timestampTypeInfo});
    } else {
      udf = new VectorUDFSecondString(0, 1);
      udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.stringTypeInfo});
    }
    udf.transientInit(hiveConf);
    udf.evaluate(batch);
    final int in = 0;
    final int out = 1;

    for (int i = 0; i < batch.size; i++) {
      if (batch.cols[in].noNulls || !batch.cols[in].isNull[i]) {
        if (!batch.cols[in].noNulls) {
          Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
        }
        Timestamp t = readVectorElementAt(batch.cols[in], i);
        long y = ((LongColumnVector) batch.cols[out]).vector[i];
        compareToUDFSecondLong(t, (int) y);
      } else {
        Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
      }
    }
  }

  private void testVectorUDFSecond(TestType testType) throws HiveException {
    VectorizedRowBatch batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)},
            VectorizedRowBatch.DEFAULT_SIZE, testType);
    Assert.assertTrue(((LongColumnVector) batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector) batch.cols[1]).isRepeating);
    verifyUDFSecond(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFSecond(batch, testType);

    Timestamp[] boundaries = getAllBoundaries();
    batch = getVectorizedRowBatch(boundaries, boundaries.length, testType);
    verifyUDFSecond(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFSecond(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFSecond(batch, testType);

    batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)}, 1, testType);
    batch.cols[0].isRepeating = true;
    verifyUDFSecond(batch, testType);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFSecond(batch, testType);

    batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)}, 1, testType);
    batch.cols[0].isRepeating = true;
    batch.selectedInUse = true;
    batch.selected = new int[] {42};
    verifyUDFSecond(batch, testType);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFSecond(batch, testType);

    batch = getVectorizedRandomRowBatch(200, VectorizedRowBatch.DEFAULT_SIZE, testType);
    verifyUDFSecond(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFSecond(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFSecond(batch, testType);
  }

  @Test
  public void testVectorUDFSecondLong() throws HiveException {
    testVectorUDFSecond(TestType.TIMESTAMP_LONG);
  }

  @Test
  public void testVectorUDFSecondString() throws HiveException {
    testVectorUDFSecond(TestType.STRING_LONG);
  }

  private void compareToUDFUnixTimeStampLong(Timestamp ts, long y) {
    TimestampTZ tsTZ = TimestampTZUtil.convert(
        org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(ts.getTime()),
        ZoneId.systemDefault());
    if(tsTZ.getEpochSecond() != y) {
      System.out.printf("%d vs %d for %s\n", tsTZ.getEpochSecond(), y, ts.toString());
      Assert.assertTrue(false);
    }
  }

  private void verifyUDFUnixTimeStamp(VectorizedRowBatch batch, TestType testType)
      throws HiveException {
    VectorExpression udf;
    if (testType == TestType.TIMESTAMP_LONG) {
      udf = new VectorUDFUnixTimeStampTimestamp(0, 1);
      udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.timestampTypeInfo});
    } else {
      udf = new VectorUDFUnixTimeStampString(0, 1);
      udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.stringTypeInfo});
    }
    udf.transientInit(hiveConf);
    udf.evaluate(batch);
    final int in = 0;
    final int out = 1;

    for (int i = 0; i < batch.size; i++) {
      if (batch.cols[in].noNulls || !batch.cols[in].isNull[i]) {
        if (!batch.cols[out].noNulls) {
          Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
        }
        Timestamp t = readVectorElementAt(batch.cols[in], i);
        long y = ((LongColumnVector) batch.cols[out]).vector[i];
        compareToUDFUnixTimeStampLong(t, y);
      } else {
        Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
      }
    }
  }

  private void testVectorUDFUnixTimeStamp(TestType testType) throws HiveException {
    VectorizedRowBatch batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)},
            VectorizedRowBatch.DEFAULT_SIZE, testType);
    Assert.assertTrue(((LongColumnVector) batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector) batch.cols[1]).isRepeating);
    verifyUDFUnixTimeStamp(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFUnixTimeStamp(batch, testType);

    Timestamp[] boundaries = getAllBoundaries();
    batch = getVectorizedRowBatch(boundaries, boundaries.length, testType);
    verifyUDFUnixTimeStamp(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFUnixTimeStamp(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFUnixTimeStamp(batch, testType);

    batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)}, 1, testType);
    batch.cols[0].isRepeating = true;
    verifyUDFUnixTimeStamp(batch, testType);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFUnixTimeStamp(batch, testType);

    batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)}, 1, testType);
    batch.cols[0].isRepeating = true;
    batch.selectedInUse = true;
    batch.selected = new int[] {42};
    verifyUDFUnixTimeStamp(batch, testType);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;

    verifyUDFUnixTimeStamp(batch, testType);
    batch = getVectorizedRandomRowBatch(200, VectorizedRowBatch.DEFAULT_SIZE, testType);
    verifyUDFUnixTimeStamp(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFUnixTimeStamp(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFUnixTimeStamp(batch, testType);
  }

  @Test
  public void testVectorUDFUnixTimeStampTimestamp() throws HiveException {
    testVectorUDFUnixTimeStamp(TestType.TIMESTAMP_LONG);
  }

  @Test
  public void testVectorUDFUnixTimeStampString() throws HiveException {
    testVectorUDFUnixTimeStamp(TestType.STRING_LONG);
  }

  private void compareToUDFWeekOfYearLong(Timestamp t, int y) {
    UDFWeekOfYear udf = new UDFWeekOfYear();
    TimestampWritableV2 tsw = new TimestampWritableV2(
        org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(t.getTime(), t.getNanos()));
    IntWritable res = udf.evaluate(tsw);
    Assert.assertEquals(res.get(), y);
  }

  private void verifyUDFWeekOfYear(VectorizedRowBatch batch, TestType testType)
      throws HiveException {
    VectorExpression udf;
    if (testType == TestType.TIMESTAMP_LONG) {
      udf = new VectorUDFWeekOfYearTimestamp(0, 1);
      udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.timestampTypeInfo});
    } else {
      udf = new VectorUDFWeekOfYearString(0, 1);
      udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.stringTypeInfo});
    }
    udf.transientInit(hiveConf);
    udf.evaluate(batch);
    final int in = 0;
    final int out = 1;

    for (int i = 0; i < batch.size; i++) {
      if (batch.cols[in].noNulls || !batch.cols[in].isNull[i]) {
        Timestamp t = readVectorElementAt(batch.cols[in], i);
        long y = ((LongColumnVector) batch.cols[out]).vector[i];
        compareToUDFWeekOfYearLong(t, (int) y);
      } else {
        Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
      }
    }
  }

  private void testVectorUDFWeekOfYear(TestType testType) throws HiveException {
    VectorizedRowBatch batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)},
            VectorizedRowBatch.DEFAULT_SIZE, testType);
    Assert.assertTrue(((LongColumnVector) batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector) batch.cols[1]).isRepeating);
    verifyUDFWeekOfYear(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFWeekOfYear(batch, testType);

    Timestamp[] boundaries = getAllBoundaries();
    batch = getVectorizedRowBatch(boundaries, boundaries.length, testType);
    verifyUDFWeekOfYear(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFWeekOfYear(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFWeekOfYear(batch, testType);

    batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)}, 1, testType);
    batch.cols[0].isRepeating = true;
    verifyUDFWeekOfYear(batch, testType);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFWeekOfYear(batch, testType);

    batch = getVectorizedRowBatch(new Timestamp[] {new Timestamp(0)}, 1, testType);
    batch.cols[0].isRepeating = true;
    batch.selectedInUse = true;
    batch.selected = new int[] {42};
    verifyUDFWeekOfYear(batch, testType);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFWeekOfYear(batch, testType);

    batch = getVectorizedRandomRowBatch(200, VectorizedRowBatch.DEFAULT_SIZE, testType);
    verifyUDFWeekOfYear(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFWeekOfYear(batch, testType);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFWeekOfYear(batch, testType);
  }

  @Test
  public void testVectorUDFWeekOfYearTimestamp() throws HiveException {
    testVectorUDFWeekOfYear(TestType.TIMESTAMP_LONG);
  }

  @Test
  public void testVectorUDFWeekOfYearString() throws HiveException {
    testVectorUDFWeekOfYear(TestType.STRING_LONG);
  }

  public static void main(String[] args) throws HiveException {
    TestVectorTimestampExpressions self = new TestVectorTimestampExpressions();
    self.testVectorUDFYearTimestamp();
    self.testVectorUDFMonthTimestamp();
    self.testVectorUDFDayOfMonthTimestamp();
    self.testVectorUDFHourTimestamp();
    self.testVectorUDFWeekOfYearTimestamp();
    self.testVectorUDFUnixTimeStampTimestamp();

    self.testVectorUDFYearString();
    self.testVectorUDFMonthString();
    self.testVectorUDFDayOfMonthString();
    self.testVectorUDFHourString();
    self.testVectorUDFWeekOfYearString();
    self.testVectorUDFUnixTimeStampString();
   }
}

