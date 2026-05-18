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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.IntervalDayTimeColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for VectorMapJoinOuterGenerateResultOperator.clearVectorValue().
 *
 * clearVectorValue() zeroes the raw vector slot whenever isNull[i] is set to
 * true in the outer-join null-marking paths, preventing stale non-zero values
 * from being misread by downstream operators (e.g. ColOrCol) that use
 * vector[i] == 0 to distinguish "false" from "null".
 */
public class TestVectorMapJoinOuterGenerateResultOperator {

  private static void callClearVectorValue(ColumnVector colVector, int index) throws Exception {
    Method m = VectorMapJoinOuterGenerateResultOperator.class
        .getDeclaredMethod("clearVectorValue", ColumnVector.class, int.class);
    m.setAccessible(true);
    m.invoke(null, colVector, index);
  }

  @Test
  public void testClearLongColumnVector() throws Exception {
    LongColumnVector cv = new LongColumnVector(4);
    cv.vector[2] = 2025L;

    callClearVectorValue(cv, 2);

    assertEquals(0L, cv.vector[2]);
    // neighbours must be untouched
    assertEquals(0L, cv.vector[1]);
    assertEquals(0L, cv.vector[3]);
  }

  @Test
  public void testClearDoubleColumnVector() throws Exception {
    DoubleColumnVector cv = new DoubleColumnVector(4);
    cv.vector[1] = 3.14;

    callClearVectorValue(cv, 1);

    assertEquals(0.0, cv.vector[1], 0.0);
  }

  @Test
  public void testClearBytesColumnVector() throws Exception {
    BytesColumnVector cv = new BytesColumnVector(4);
    byte[] data = "hello".getBytes();
    cv.vector[0] = data;
    cv.start[0] = 1;
    cv.length[0] = 3;

    callClearVectorValue(cv, 0);

    assertNull(cv.vector[0]);
    assertEquals(0, cv.start[0]);
    assertEquals(0, cv.length[0]);
  }

  @Test
  public void testClearTimestampColumnVector() throws Exception {
    TimestampColumnVector cv = new TimestampColumnVector(4);
    cv.time[2] = 1234567890000L;
    cv.nanos[2] = 999;

    callClearVectorValue(cv, 2);

    // TimestampColumnVector.setNullValue sets time=0, nanos=1
    assertEquals(0L, cv.time[2]);
    assertEquals(1, cv.nanos[2]);
  }

  @Test
  public void testClearDecimalColumnVector() throws Exception {
    DecimalColumnVector cv = new DecimalColumnVector(4, 18, 4);
    cv.vector[1].setFromLong(12345L);

    callClearVectorValue(cv, 1);

    assertEquals(0L, cv.vector[1].serialize64(4));
  }

  @Test
  public void testClearIntervalDayTimeColumnVector() throws Exception {
    IntervalDayTimeColumnVector cv = new IntervalDayTimeColumnVector(4);
    cv.set(3, new org.apache.hadoop.hive.common.type.HiveIntervalDayTime(5, 0));

    callClearVectorValue(cv, 3);

    // IntervalDayTimeColumnVector.setNullValue sets totalSeconds=0, nanos=1
    assertEquals(0L, cv.getTotalSeconds(3));
    assertEquals(1, cv.getNanos(3));
  }
}
