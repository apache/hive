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

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.udf.UDFDayOfMonth;
import org.apache.hadoop.hive.ql.udf.UDFMonth;
import org.apache.hadoop.hive.ql.udf.UDFYear;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TestVectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFWeekOfYear;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TestVectorDateExpressions {

  private ExecutorService runner;

  /* copied over from VectorUDFTimestampFieldLong */
  private TimestampWritableV2 toTimestampWritable(long daysSinceEpoch) {
    return new TimestampWritableV2(
        org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(
            DateWritableV2.daysToMillis((int) daysSinceEpoch)));
  }

  private TimestampLocalTZWritable toTimestampLocalTZWritable(long daysSinceEpoch) {
    return new TimestampLocalTZWritable(
        TimestampTZUtil.convert(
            Timestamp.ofEpochMilli(
                DateWritableV2.daysToMillis((int) daysSinceEpoch)),
            ZoneId.systemDefault()));
  }

  private int[] getAllBoundaries() {
    List<Integer> boundaries = new ArrayList<Integer>(1);
    Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    c.setTimeInMillis(0); // c.set doesn't reset millis
    for (int year = 1902; year <= 2038; year++) {
      c.set(year, Calendar.JANUARY, 1, 0, 0, 0);
      int exactly = (int) (c.getTimeInMillis() / (24 * 60 * 60 * 1000));
      int before = exactly - 1;
      int after = exactly + 1;
      boundaries.add(Integer.valueOf(before));
      boundaries.add(Integer.valueOf(exactly));
      boundaries.add(Integer.valueOf(after));
    }
    Integer[] indices = boundaries.toArray(new Integer[1]);
    return ArrayUtils.toPrimitive(indices);
  }

  private VectorizedRowBatch getVectorizedRandomRowBatch(int seed, int size) {
    VectorizedRowBatch batch = new VectorizedRowBatch(2, size);
    LongColumnVector lcv = new LongColumnVector(size);
    Random rand = new Random(seed);
    for (int i = 0; i < size; i++) {
      lcv.vector[i] = (rand.nextInt());
    }
    batch.cols[0] = lcv;
    batch.cols[1] = new LongColumnVector(size);
    batch.size = size;
    return batch;
  }

  /**
   * Input array is used to fill the entire size of the vector row batch
   */
  private VectorizedRowBatch getVectorizedRowBatch(int[] inputs, int size) {
    VectorizedRowBatch batch = new VectorizedRowBatch(2, size);
    LongColumnVector lcv = new LongColumnVector(size);
    for (int i = 0; i < size; i++) {
      lcv.vector[i] = inputs[i % inputs.length];
    }
    batch.cols[0] = lcv;
    batch.cols[1] = new LongColumnVector(size);
    batch.size = size;
    return batch;
  }

  private void compareToUDFYearDate(long t, int y) throws HiveException {
    UDFYear udf = new UDFYear();
    udf.initialize(new ObjectInspector[]{PrimitiveObjectInspectorFactory.writableTimestampObjectInspector});
    TimestampWritableV2 tsw = toTimestampWritable(t);
    IntWritable res = (IntWritable) udf.evaluate(
        new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(tsw)});
    Assert.assertEquals(res.get(), y);
  }

  private void verifyUDFYear(VectorizedRowBatch batch) throws HiveException {
    VectorExpression udf = null;
    udf = new VectorUDFYearDate(0, 1);
    udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.dateTypeInfo});
    udf.evaluate(batch);
    final int in = 0;
    final int out = 1;

    for (int i = 0; i < batch.size; i++) {
      if (batch.cols[in].noNulls || !batch.cols[in].isNull[i]) {
        if (!batch.cols[in].noNulls) {
          Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
        }
        long t = ((LongColumnVector) batch.cols[in]).vector[i];
        long y = ((LongColumnVector) batch.cols[out]).vector[i];
        compareToUDFYearDate(t, (int) y);
      } else {
        Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
      }
    }
  }

  @Test
  public void testVectorUDFYear() throws HiveException {
    VectorizedRowBatch batch = getVectorizedRowBatch(new int[] {0},
            VectorizedRowBatch.DEFAULT_SIZE);
    Assert.assertTrue(((LongColumnVector) batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector) batch.cols[1]).isRepeating);
    verifyUDFYear(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFYear(batch);

    int[] boundaries = getAllBoundaries();
    batch = getVectorizedRowBatch(boundaries, boundaries.length);
    verifyUDFYear(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFYear(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFYear(batch);

    batch = getVectorizedRowBatch(new int[] {0}, 1);
    batch.cols[0].isRepeating = true;
    verifyUDFYear(batch);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFYear(batch);

    batch = getVectorizedRandomRowBatch(200, VectorizedRowBatch.DEFAULT_SIZE);
    verifyUDFYear(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFYear(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFYear(batch);
  }

  private void compareToUDFDayOfMonthDate(long t, int y) throws HiveException {
    UDFDayOfMonth udf = new UDFDayOfMonth();
    udf.initialize(new ObjectInspector[]{PrimitiveObjectInspectorFactory.writableTimestampObjectInspector});
    TimestampWritableV2 tsw = toTimestampWritable(t);
    IntWritable res = (IntWritable) udf.evaluate(
        new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(tsw)});
    Assert.assertEquals(res.get(), y);
  }

  private void verifyUDFDayOfMonth(VectorizedRowBatch batch) throws HiveException {
    VectorExpression udf = null;
    udf = new VectorUDFDayOfMonthDate(0, 1);
    udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.dateTypeInfo});
    udf.evaluate(batch);
    final int in = 0;
    final int out = 1;

    for (int i = 0; i < batch.size; i++) {
      if (batch.cols[in].noNulls || !batch.cols[in].isNull[i]) {
        if (!batch.cols[in].noNulls) {
          Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
        }
        long t = ((LongColumnVector) batch.cols[in]).vector[i];
        long y = ((LongColumnVector) batch.cols[out]).vector[i];
        compareToUDFDayOfMonthDate(t, (int) y);
      } else {
        Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
      }
    }
  }

  @Test
  public void testVectorUDFDayOfMonth() throws HiveException {
    VectorizedRowBatch batch = getVectorizedRowBatch(new int[] {0},
            VectorizedRowBatch.DEFAULT_SIZE);
    Assert.assertTrue(((LongColumnVector) batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector) batch.cols[1]).isRepeating);
    verifyUDFDayOfMonth(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFDayOfMonth(batch);

    int[] boundaries = getAllBoundaries();
    batch = getVectorizedRowBatch(boundaries, boundaries.length);
    verifyUDFDayOfMonth(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFDayOfMonth(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFDayOfMonth(batch);

    batch = getVectorizedRowBatch(new int[] {0}, 1);
    batch.cols[0].isRepeating = true;
    verifyUDFDayOfMonth(batch);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFDayOfMonth(batch);

    batch = getVectorizedRandomRowBatch(200, VectorizedRowBatch.DEFAULT_SIZE);
    verifyUDFDayOfMonth(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFDayOfMonth(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFDayOfMonth(batch);
  }

  private void compareToUDFMonthDate(long t, int y) throws HiveException {
    UDFMonth udf = new UDFMonth();
    udf.initialize(new ObjectInspector[]{PrimitiveObjectInspectorFactory.writableTimestampObjectInspector});
    TimestampWritableV2 tsw = toTimestampWritable(t);
    IntWritable res = (IntWritable) udf.evaluate(
        new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(tsw)});
    Assert.assertEquals(res.get(), y);
  }

  private void verifyUDFMonth(VectorizedRowBatch batch) throws HiveException {
    VectorExpression udf;
    udf = new VectorUDFMonthDate(0, 1);
    udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.dateTypeInfo});
    udf.evaluate(batch);
    final int in = 0;
    final int out = 1;

    for (int i = 0; i < batch.size; i++) {
      if (batch.cols[in].noNulls || !batch.cols[in].isNull[i]) {
        if (!batch.cols[in].noNulls) {
          Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
        }
        long t = ((LongColumnVector) batch.cols[in]).vector[i];
        long y = ((LongColumnVector) batch.cols[out]).vector[i];
        compareToUDFMonthDate(t, (int) y);
      } else {
        Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
      }
    }
  }

  @Test
  public void testVectorUDFMonth() throws HiveException {
    VectorizedRowBatch batch = getVectorizedRowBatch(new int[] {0},
            VectorizedRowBatch.DEFAULT_SIZE);
    Assert.assertTrue(((LongColumnVector) batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector) batch.cols[1]).isRepeating);
    verifyUDFMonth(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFMonth(batch);

    int[] boundaries = getAllBoundaries();
    batch = getVectorizedRowBatch(boundaries, boundaries.length);
    verifyUDFMonth(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFMonth(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFMonth(batch);

    batch = getVectorizedRowBatch(new int[] {0}, 1);
    batch.cols[0].isRepeating = true;
    verifyUDFMonth(batch);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFMonth(batch);

    batch = getVectorizedRandomRowBatch(200, VectorizedRowBatch.DEFAULT_SIZE);
    verifyUDFMonth(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFMonth(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFMonth(batch);
  }

  private LongWritable getLongWritable(TimestampLocalTZWritable i) {
    LongWritable result = new LongWritable();
    if (i == null) {
      return null;
    } else {
      result.set(i.getSeconds());
      return result;
    }
  }

  private void compareToUDFUnixTimeStampDate(long t, long y) {
    TimestampLocalTZWritable tsw = toTimestampLocalTZWritable(t);
    LongWritable res = getLongWritable(tsw);
    Assert.assertEquals(res.get(), y);
  }

  private void verifyUDFUnixTimeStamp(VectorizedRowBatch batch) throws HiveException {
    VectorExpression udf;
    udf = new VectorUDFUnixTimeStampDate(0, 1);
    udf.transientInit(new HiveConf());
    udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.dateTypeInfo});
    udf.evaluate(batch);
    final int in = 0;
    final int out = 1;

    for (int i = 0; i < batch.size; i++) {
      if (batch.cols[in].noNulls || !batch.cols[in].isNull[i]) {
        if (!batch.cols[out].noNulls) {
          Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
        }
        long t = ((LongColumnVector) batch.cols[in]).vector[i];
        long y = ((LongColumnVector) batch.cols[out]).vector[i];
        compareToUDFUnixTimeStampDate(t, y);
      } else {
        Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
      }
    }
  }

  @Test
  public void testVectorUDFUnixTimeStamp() throws HiveException {
    VectorizedRowBatch batch = getVectorizedRowBatch(new int[] {0},
            VectorizedRowBatch.DEFAULT_SIZE);
    Assert.assertTrue(((LongColumnVector) batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector) batch.cols[1]).isRepeating);
    verifyUDFUnixTimeStamp(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFUnixTimeStamp(batch);

    int[] boundaries = getAllBoundaries();
    batch = getVectorizedRowBatch(boundaries, boundaries.length);
    verifyUDFUnixTimeStamp(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFUnixTimeStamp(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFUnixTimeStamp(batch);

    batch = getVectorizedRowBatch(new int[] {0}, 1);
    batch.cols[0].isRepeating = true;
    verifyUDFUnixTimeStamp(batch);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFUnixTimeStamp(batch);

    batch = getVectorizedRandomRowBatch(200, VectorizedRowBatch.DEFAULT_SIZE);
    verifyUDFUnixTimeStamp(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFUnixTimeStamp(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFUnixTimeStamp(batch);
  }

  private void compareToUDFWeekOfYearDate(long t, int y) {
    UDFWeekOfYear udf = new UDFWeekOfYear();
    TimestampWritableV2 tsw = toTimestampWritable(t);
    IntWritable res = udf.evaluate(tsw);
    Assert.assertEquals(res.get(), y);
  }

  private void verifyUDFWeekOfYear(VectorizedRowBatch batch) throws HiveException {
    VectorExpression udf;
    udf = new VectorUDFWeekOfYearDate(0, 1);
    udf.setInputTypeInfos(new TypeInfo[] {TypeInfoFactory.dateTypeInfo});
    udf.transientInit(new HiveConf());
    udf.evaluate(batch);
    final int in = 0;
    final int out = 1;

    for (int i = 0; i < batch.size; i++) {
      if (batch.cols[in].noNulls || !batch.cols[in].isNull[i]) {
        long t = ((LongColumnVector) batch.cols[in]).vector[i];
        long y = ((LongColumnVector) batch.cols[out]).vector[i];
        compareToUDFWeekOfYearDate(t, (int) y);
      } else {
        Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
      }
    }
  }

  @Test
  public void testVectorUDFWeekOfYear() throws HiveException {
    VectorizedRowBatch batch = getVectorizedRowBatch(new int[] {0},
            VectorizedRowBatch.DEFAULT_SIZE);
    Assert.assertTrue(((LongColumnVector) batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector) batch.cols[1]).isRepeating);
    verifyUDFWeekOfYear(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFWeekOfYear(batch);

    int[] boundaries = getAllBoundaries();
    batch = getVectorizedRowBatch(boundaries, boundaries.length);
    verifyUDFWeekOfYear(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFWeekOfYear(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFWeekOfYear(batch);

    batch = getVectorizedRowBatch(new int[] {0}, 1);
    batch.cols[0].isRepeating = true;
    verifyUDFWeekOfYear(batch);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFWeekOfYear(batch);

    batch = getVectorizedRandomRowBatch(200, VectorizedRowBatch.DEFAULT_SIZE);
    verifyUDFWeekOfYear(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFWeekOfYear(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFWeekOfYear(batch);
  }

  @Before
  public void setUp() throws Exception {
    runner =
        Executors.newFixedThreadPool(3,
            new ThreadFactoryBuilder().setNameFormat("date-tester-thread-%d").build());
  }

  private static final class MultiThreadedDateFormatTest implements Callable<Void> {
    @Override
    public Void call() throws Exception {
      int batchSize = 1024;
      VectorUDFDateString udf = new VectorUDFDateString(0, 1);
      VectorizedRowBatch batch = new VectorizedRowBatch(2, batchSize);
      BytesColumnVector in = new BytesColumnVector(batchSize);
      LongColumnVector out = new LongColumnVector(batchSize);
      batch.cols[0] = in;
      batch.cols[1] = out;
      for (int i = 0; i < batchSize; i++) {
        byte[] data = String.format("1999-%02d-%02d", 1 + (i % 12), 1 + (i % 15)).getBytes("UTF-8");
        in.setRef(i, data, 0, data.length);
        in.isNull[i] = false;
      }
      udf.evaluate(batch);
      // bug if it throws an exception
      return (Void) null;
    }
  }

  // 5s timeout
  @Test(timeout = 5000)
  public void testMultiThreadedVectorUDFDate() throws HiveException {
    List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
    for (int i = 0; i < 200; i++) {
      tasks.add(new MultiThreadedDateFormatTest());
    }
    try {
      List<Future<Void>> results = runner.invokeAll(tasks);
      for (Future<Void> f : results) {
        Assert.assertNull(f.get());
      }
    } catch (InterruptedException ioe) {
      Assert.fail("Interrupted while running tests");
    } catch (Exception e) {
      Assert.fail("Multi threaded operations threw unexpected Exception: " + e.getMessage());
    }
  }

  @After
  public void tearDown() throws Exception {
    if (runner != null) {
      runner.shutdownNow();
    }
  }

  public static void main(String[] args) throws HiveException {
    TestVectorDateExpressions self = new TestVectorDateExpressions();
    self.testVectorUDFYear();
    self.testVectorUDFMonth();
    self.testVectorUDFDayOfMonth();
    self.testVectorUDFWeekOfYear();
    self.testVectorUDFUnixTimeStamp();
    self.testMultiThreadedVectorUDFDate();
  }
}
