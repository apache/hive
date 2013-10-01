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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

import junit.framework.Assert;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TestVectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.udf.UDFDayOfMonth;
import org.apache.hadoop.hive.ql.udf.UDFHour;
import org.apache.hadoop.hive.ql.udf.UDFMinute;
import org.apache.hadoop.hive.ql.udf.UDFMonth;
import org.apache.hadoop.hive.ql.udf.UDFSecond;
import org.apache.hadoop.hive.ql.udf.UDFWeekOfYear;
import org.apache.hadoop.hive.ql.udf.UDFYear;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

/**
 * Unit tests for timestamp expressions.
 */
public class TestVectorTimestampExpressions {

  /* copied over from VectorUDFTimestampFieldLong */
  private TimestampWritable toTimestampWritable(long nanos) {
    long ms = (nanos / (1000 * 1000 * 1000)) * 1000;
    /* the milliseconds should be kept in nanos */
    long ns = nanos % (1000*1000*1000);
    if (ns < 0) {
      /*
       * The nano seconds are always positive,
       * but the milliseconds can be negative
       */
      ms -= 1000;
      ns += 1000*1000*1000;
    }
    Timestamp ts = new Timestamp(ms);
    ts.setNanos((int) ns);
    return new TimestampWritable(ts);
  }

  private long[] getAllBoundaries() {
    List<Long> boundaries = new ArrayList<Long>(1);
    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(0); // c.set doesn't reset millis
    for (int year = 1902; year <= 2038; year++) {
      c.set(year, Calendar.JANUARY, 1, 0, 0, 0);
      long exactly = c.getTimeInMillis() * 1000 * 1000;
      /* one second before and after */
      long before = exactly - 1000 * 1000 * 1000;
      long after = exactly + 1000 * 1000 * 1000;
      boundaries.add(Long.valueOf(before));
      boundaries.add(Long.valueOf(exactly));
      boundaries.add(Long.valueOf(after));
    }
    Long[] indices = boundaries.toArray(new Long[1]);
    return ArrayUtils.toPrimitive(indices);
  }

  private VectorizedRowBatch getVectorizedRandomRowBatchLong2(int seed, int size) {
    VectorizedRowBatch batch = new VectorizedRowBatch(2, size);
    LongColumnVector lcv = new LongColumnVector(size);
    Random rand = new Random(seed);
    for (int i = 0; i < size; i++) {
      /* all 32 bit numbers qualify & multiply up to get nano-seconds */
      lcv.vector[i] = (long)(1000*1000*1000*rand.nextInt());
    }
    batch.cols[0] = lcv;
    batch.cols[1] = new LongColumnVector(size);
    batch.size = size;
    return batch;
  }

  /*
   * Input array is used to fill the entire size of the vector row batch
   */
  private VectorizedRowBatch getVectorizedRowBatchLong2(long[] inputs, int size) {
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

  /*begin-macro*/
  private void compareToUDFYearLong(long t, int y) {
    UDFYear udf = new UDFYear();
    TimestampWritable tsw = toTimestampWritable(t);
    IntWritable res = udf.evaluate(tsw);
    Assert.assertEquals(res.get(), y);
  }

  private void verifyUDFYearLong(VectorizedRowBatch batch) {
    /* col[1] = UDFYear(col[0]) */
    VectorUDFYearLong udf = new VectorUDFYearLong(0, 1);
    udf.evaluate(batch);
    final int in = 0;
    final int out = 1;
    Assert.assertEquals(batch.cols[in].noNulls, batch.cols[out].noNulls);

    for (int i = 0; i < batch.size; i++) {
      if (batch.cols[in].noNulls || !batch.cols[in].isNull[i]) {
        if (!batch.cols[in].noNulls) {
          Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
        }
        long t = ((LongColumnVector) batch.cols[in]).vector[i];
        long y = ((LongColumnVector) batch.cols[out]).vector[i];
        compareToUDFYearLong(t, (int) y);
      } else {
        Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
      }
    }
  }

  @Test
  public void testVectorUDFYearLong() {
    VectorizedRowBatch batch = getVectorizedRowBatchLong2(new long[] {0},
        VectorizedRowBatch.DEFAULT_SIZE);
    Assert.assertTrue(((LongColumnVector) batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector) batch.cols[1]).isRepeating);
    verifyUDFYearLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFYearLong(batch);

    long[] boundaries = getAllBoundaries();
    batch = getVectorizedRowBatchLong2(boundaries, boundaries.length);
    verifyUDFYearLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFYearLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFYearLong(batch);

    batch = getVectorizedRowBatchLong2(new long[] {0}, 1);
    batch.cols[0].isRepeating = true;
    verifyUDFYearLong(batch);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFYearLong(batch);

    batch = getVectorizedRandomRowBatchLong2(200, VectorizedRowBatch.DEFAULT_SIZE);
    verifyUDFYearLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFYearLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFYearLong(batch);
  }
  /*end-macro*/


  private void compareToUDFDayOfMonthLong(long t, int y) {
    UDFDayOfMonth udf = new UDFDayOfMonth();
    TimestampWritable tsw = toTimestampWritable(t);
    IntWritable res = udf.evaluate(tsw);
    Assert.assertEquals(res.get(), y);
  }

  private void verifyUDFDayOfMonthLong(VectorizedRowBatch batch) {
    /* col[1] = UDFDayOfMonth(col[0]) */
    VectorUDFDayOfMonthLong udf = new VectorUDFDayOfMonthLong(0, 1);
    udf.evaluate(batch);
    final int in = 0;
    final int out = 1;
    Assert.assertEquals(batch.cols[in].noNulls, batch.cols[out].noNulls);

    for (int i = 0; i < batch.size; i++) {
      if (batch.cols[in].noNulls || !batch.cols[in].isNull[i]) {
        if (!batch.cols[in].noNulls) {
          Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
        }
        long t = ((LongColumnVector) batch.cols[in]).vector[i];
        long y = ((LongColumnVector) batch.cols[out]).vector[i];
        compareToUDFDayOfMonthLong(t, (int) y);
      } else {
        Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
      }
    }
  }

  @Test
  public void testVectorUDFDayOfMonthLong() {
    VectorizedRowBatch batch = getVectorizedRowBatchLong2(new long[] {0},
        VectorizedRowBatch.DEFAULT_SIZE);
    Assert.assertTrue(((LongColumnVector) batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector) batch.cols[1]).isRepeating);
    verifyUDFDayOfMonthLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFDayOfMonthLong(batch);

    long[] boundaries = getAllBoundaries();
    batch = getVectorizedRowBatchLong2(boundaries, boundaries.length);
    verifyUDFDayOfMonthLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFDayOfMonthLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFDayOfMonthLong(batch);

    batch = getVectorizedRowBatchLong2(new long[] {0}, 1);
    batch.cols[0].isRepeating = true;
    verifyUDFDayOfMonthLong(batch);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFDayOfMonthLong(batch);

    batch = getVectorizedRandomRowBatchLong2(200, VectorizedRowBatch.DEFAULT_SIZE);
    verifyUDFDayOfMonthLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFDayOfMonthLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFDayOfMonthLong(batch);
  }

  private void compareToUDFHourLong(long t, int y) {
    UDFHour udf = new UDFHour();
    TimestampWritable tsw = toTimestampWritable(t);
    IntWritable res = udf.evaluate(tsw);
    Assert.assertEquals(res.get(), y);
  }

  private void verifyUDFHourLong(VectorizedRowBatch batch) {
    /* col[1] = UDFHour(col[0]) */
    VectorUDFHourLong udf = new VectorUDFHourLong(0, 1);
    udf.evaluate(batch);
    final int in = 0;
    final int out = 1;
    Assert.assertEquals(batch.cols[in].noNulls, batch.cols[out].noNulls);

    for (int i = 0; i < batch.size; i++) {
      if (batch.cols[in].noNulls || !batch.cols[in].isNull[i]) {
        if (!batch.cols[in].noNulls) {
          Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
        }
        long t = ((LongColumnVector) batch.cols[in]).vector[i];
        long y = ((LongColumnVector) batch.cols[out]).vector[i];
        compareToUDFHourLong(t, (int) y);
      } else {
        Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
      }
    }
  }

  @Test
  public void testVectorUDFHourLong() {
    VectorizedRowBatch batch = getVectorizedRowBatchLong2(new long[] {0},
        VectorizedRowBatch.DEFAULT_SIZE);
    Assert.assertTrue(((LongColumnVector) batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector) batch.cols[1]).isRepeating);
    verifyUDFHourLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFHourLong(batch);

    long[] boundaries = getAllBoundaries();
    batch = getVectorizedRowBatchLong2(boundaries, boundaries.length);
    verifyUDFHourLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFHourLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFHourLong(batch);

    batch = getVectorizedRowBatchLong2(new long[] {0}, 1);
    batch.cols[0].isRepeating = true;
    verifyUDFHourLong(batch);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFHourLong(batch);

    batch = getVectorizedRandomRowBatchLong2(200, VectorizedRowBatch.DEFAULT_SIZE);
    verifyUDFHourLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFHourLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFHourLong(batch);
  }

  private void compareToUDFMinuteLong(long t, int y) {
    UDFMinute udf = new UDFMinute();
    TimestampWritable tsw = toTimestampWritable(t);
    IntWritable res = udf.evaluate(tsw);
    Assert.assertEquals(res.get(), y);
  }

  private void verifyUDFMinuteLong(VectorizedRowBatch batch) {
    /* col[1] = UDFMinute(col[0]) */
    VectorUDFMinuteLong udf = new VectorUDFMinuteLong(0, 1);
    udf.evaluate(batch);
    final int in = 0;
    final int out = 1;
    Assert.assertEquals(batch.cols[in].noNulls, batch.cols[out].noNulls);

    for (int i = 0; i < batch.size; i++) {
      if (batch.cols[in].noNulls || !batch.cols[in].isNull[i]) {
        if (!batch.cols[in].noNulls) {
          Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
        }
        long t = ((LongColumnVector) batch.cols[in]).vector[i];
        long y = ((LongColumnVector) batch.cols[out]).vector[i];
        compareToUDFMinuteLong(t, (int) y);
      } else {
        Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
      }
    }
  }

  @Test
  public void testVectorUDFMinuteLong() {
    VectorizedRowBatch batch = getVectorizedRowBatchLong2(new long[] {0},
        VectorizedRowBatch.DEFAULT_SIZE);
    Assert.assertTrue(((LongColumnVector) batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector) batch.cols[1]).isRepeating);
    verifyUDFMinuteLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFMinuteLong(batch);

    long[] boundaries = getAllBoundaries();
    batch = getVectorizedRowBatchLong2(boundaries, boundaries.length);
    verifyUDFMinuteLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFMinuteLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFMinuteLong(batch);

    batch = getVectorizedRowBatchLong2(new long[] {0}, 1);
    batch.cols[0].isRepeating = true;
    verifyUDFMinuteLong(batch);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFMinuteLong(batch);

    batch = getVectorizedRandomRowBatchLong2(200, VectorizedRowBatch.DEFAULT_SIZE);
    verifyUDFMinuteLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFMinuteLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFMinuteLong(batch);
  }

  private void compareToUDFMonthLong(long t, int y) {
    UDFMonth udf = new UDFMonth();
    TimestampWritable tsw = toTimestampWritable(t);
    IntWritable res = udf.evaluate(tsw);
    Assert.assertEquals(res.get(), y);
  }

  private void verifyUDFMonthLong(VectorizedRowBatch batch) {
    /* col[1] = UDFMonth(col[0]) */
    VectorUDFMonthLong udf = new VectorUDFMonthLong(0, 1);
    udf.evaluate(batch);
    final int in = 0;
    final int out = 1;
    Assert.assertEquals(batch.cols[in].noNulls, batch.cols[out].noNulls);

    for (int i = 0; i < batch.size; i++) {
      if (batch.cols[in].noNulls || !batch.cols[in].isNull[i]) {
        if (!batch.cols[in].noNulls) {
          Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
        }
        long t = ((LongColumnVector) batch.cols[in]).vector[i];
        long y = ((LongColumnVector) batch.cols[out]).vector[i];
        compareToUDFMonthLong(t, (int) y);
      } else {
        Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
      }
    }
  }

  @Test
  public void testVectorUDFMonthLong() {
    VectorizedRowBatch batch = getVectorizedRowBatchLong2(new long[] {0},
        VectorizedRowBatch.DEFAULT_SIZE);
    Assert.assertTrue(((LongColumnVector) batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector) batch.cols[1]).isRepeating);
    verifyUDFMonthLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFMonthLong(batch);

    long[] boundaries = getAllBoundaries();
    batch = getVectorizedRowBatchLong2(boundaries, boundaries.length);
    verifyUDFMonthLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFMonthLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFMonthLong(batch);

    batch = getVectorizedRowBatchLong2(new long[] {0}, 1);
    batch.cols[0].isRepeating = true;
    verifyUDFMonthLong(batch);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFMonthLong(batch);

    batch = getVectorizedRandomRowBatchLong2(200, VectorizedRowBatch.DEFAULT_SIZE);
    verifyUDFMonthLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFMonthLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFMonthLong(batch);
  }

  private void compareToUDFSecondLong(long t, int y) {
    UDFSecond udf = new UDFSecond();
    TimestampWritable tsw = toTimestampWritable(t);
    IntWritable res = udf.evaluate(tsw);
    Assert.assertEquals(res.get(), y);
  }

  private void verifyUDFSecondLong(VectorizedRowBatch batch) {
    /* col[1] = UDFSecond(col[0]) */
    VectorUDFSecondLong udf = new VectorUDFSecondLong(0, 1);
    udf.evaluate(batch);
    final int in = 0;
    final int out = 1;
    Assert.assertEquals(batch.cols[in].noNulls, batch.cols[out].noNulls);

    for (int i = 0; i < batch.size; i++) {
      if (batch.cols[in].noNulls || !batch.cols[in].isNull[i]) {
        if (!batch.cols[in].noNulls) {
          Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
        }
        long t = ((LongColumnVector) batch.cols[in]).vector[i];
        long y = ((LongColumnVector) batch.cols[out]).vector[i];
        compareToUDFSecondLong(t, (int) y);
      } else {
        Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
      }
    }
  }

  @Test
  public void testVectorUDFSecondLong() {
    VectorizedRowBatch batch = getVectorizedRowBatchLong2(new long[] {0},
        VectorizedRowBatch.DEFAULT_SIZE);
    Assert.assertTrue(((LongColumnVector) batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector) batch.cols[1]).isRepeating);
    verifyUDFSecondLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFSecondLong(batch);

    long[] boundaries = getAllBoundaries();
    batch = getVectorizedRowBatchLong2(boundaries, boundaries.length);
    verifyUDFSecondLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFSecondLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFSecondLong(batch);

    batch = getVectorizedRowBatchLong2(new long[] {0}, 1);
    batch.cols[0].isRepeating = true;
    verifyUDFSecondLong(batch);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFSecondLong(batch);

    batch = getVectorizedRandomRowBatchLong2(200, VectorizedRowBatch.DEFAULT_SIZE);
    verifyUDFSecondLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFSecondLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFSecondLong(batch);
  }

  private LongWritable getLongWritable(TimestampWritable i) {
    LongWritable result = new LongWritable();
    if (i == null) {
      return null;
    } else {
      result.set(i.getSeconds());
      return result;
    }
  }

  private void compareToUDFUnixTimeStampLong(long t, long y) {
    TimestampWritable tsw = toTimestampWritable(t);
    LongWritable res = getLongWritable(tsw);
    if(res.get() != y) {
      System.out.printf("%d vs %d for %d, %d\n", res.get(), y, t,
          tsw.getTimestamp().getTime()/1000);
    }

    Assert.assertEquals(res.get(), y);
  }

  private void verifyUDFUnixTimeStampLong(VectorizedRowBatch batch) {
    /* col[1] = UDFUnixTimeStamp(col[0]) */
    VectorUDFUnixTimeStampLong udf = new VectorUDFUnixTimeStampLong(0, 1);
    udf.evaluate(batch);
    final int in = 0;
    final int out = 1;
    Assert.assertEquals(batch.cols[in].noNulls, batch.cols[out].noNulls);

    for (int i = 0; i < batch.size; i++) {
      if (batch.cols[in].noNulls || !batch.cols[in].isNull[i]) {
        if (!batch.cols[in].noNulls) {
          Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
        }
        long t = ((LongColumnVector) batch.cols[in]).vector[i];
        long y = ((LongColumnVector) batch.cols[out]).vector[i];
        compareToUDFUnixTimeStampLong(t, y);
      } else {
        Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
      }
    }
  }

  @Test
  public void testVectorUDFUnixTimeStampLong() {
    VectorizedRowBatch batch = getVectorizedRowBatchLong2(new long[] {0},
        VectorizedRowBatch.DEFAULT_SIZE);
    Assert.assertTrue(((LongColumnVector) batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector) batch.cols[1]).isRepeating);
    verifyUDFUnixTimeStampLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFUnixTimeStampLong(batch);

    long[] boundaries = getAllBoundaries();
    batch = getVectorizedRowBatchLong2(boundaries, boundaries.length);
    verifyUDFUnixTimeStampLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFUnixTimeStampLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFUnixTimeStampLong(batch);

    batch = getVectorizedRowBatchLong2(new long[] {0}, 1);
    batch.cols[0].isRepeating = true;
    verifyUDFUnixTimeStampLong(batch);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFUnixTimeStampLong(batch);

    batch = getVectorizedRandomRowBatchLong2(200, VectorizedRowBatch.DEFAULT_SIZE);
    verifyUDFUnixTimeStampLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFUnixTimeStampLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFUnixTimeStampLong(batch);
  }

  private void compareToUDFWeekOfYearLong(long t, int y) {
    UDFWeekOfYear udf = new UDFWeekOfYear();
    TimestampWritable tsw = toTimestampWritable(t);
    IntWritable res = udf.evaluate(tsw);
    Assert.assertEquals(res.get(), y);
  }

  private void verifyUDFWeekOfYearLong(VectorizedRowBatch batch) {
    /* col[1] = UDFWeekOfYear(col[0]) */
    VectorUDFWeekOfYearLong udf = new VectorUDFWeekOfYearLong(0, 1);
    udf.evaluate(batch);
    final int in = 0;
    final int out = 1;
    Assert.assertEquals(batch.cols[in].noNulls, batch.cols[out].noNulls);

    for (int i = 0; i < batch.size; i++) {
      if (batch.cols[in].noNulls || !batch.cols[in].isNull[i]) {
        if (!batch.cols[in].noNulls) {
          Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
        }
        long t = ((LongColumnVector) batch.cols[in]).vector[i];
        long y = ((LongColumnVector) batch.cols[out]).vector[i];
        compareToUDFWeekOfYearLong(t, (int) y);
      } else {
        Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
      }
    }
  }

  @Test
  public void testVectorUDFWeekOfYearLong() {
    VectorizedRowBatch batch = getVectorizedRowBatchLong2(new long[] {0},
        VectorizedRowBatch.DEFAULT_SIZE);
    Assert.assertTrue(((LongColumnVector) batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector) batch.cols[1]).isRepeating);
    verifyUDFWeekOfYearLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFWeekOfYearLong(batch);

    long[] boundaries = getAllBoundaries();
    batch = getVectorizedRowBatchLong2(boundaries, boundaries.length);
    verifyUDFWeekOfYearLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFWeekOfYearLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFWeekOfYearLong(batch);

    batch = getVectorizedRowBatchLong2(new long[] {0}, 1);
    batch.cols[0].isRepeating = true;
    verifyUDFWeekOfYearLong(batch);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyUDFWeekOfYearLong(batch);

    batch = getVectorizedRandomRowBatchLong2(200, VectorizedRowBatch.DEFAULT_SIZE);
    verifyUDFWeekOfYearLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyUDFWeekOfYearLong(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[1]);
    verifyUDFWeekOfYearLong(batch);
  }

  public static void main(String[] args) {
    TestVectorTimestampExpressions self = new TestVectorTimestampExpressions();
    self.testVectorUDFYearLong();
    self.testVectorUDFMonthLong();
    self.testVectorUDFDayOfMonthLong();
    self.testVectorUDFHourLong();
    self.testVectorUDFWeekOfYearLong();
    self.testVectorUDFUnixTimeStampLong();
   }
}

