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

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TestVectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDatetimeLegacyHybridCalendar;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests VectorUDFDatetimeLegacyHybridCalendarTimestamp and
 * VectorUDFDatetimeLegacyHybridCalendarDate.
 */
public class TestVectorUDFDatetimeLegacyHybridCalendar {

  @Test
  public void testVectorUDFDatetimeLegacyHybridCalendarTimestamp() throws HiveException {
    VectorizedRowBatch batch = getFreshBatchOfTimestamps(VectorizedRowBatch.DEFAULT_SIZE);
    Assert.assertTrue(((TimestampColumnVector) batch.cols[1]).noNulls);
    Assert.assertFalse(((TimestampColumnVector) batch.cols[1]).isRepeating);
    verifyVectorUDFDatetimeLegacyHybridCalendarTimestamp(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyVectorUDFDatetimeLegacyHybridCalendarTimestamp(batch);

    batch = getFreshBatchOfTimestamps(1);
    batch.cols[0].isRepeating = true; //
    verifyVectorUDFDatetimeLegacyHybridCalendarTimestamp(batch);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyVectorUDFDatetimeLegacyHybridCalendarTimestamp(batch);

    batch = getFreshBatchOfTimestamps(3);
    batch.cols[0].isRepeating = false;
    batch.selectedInUse = true;
    batch.selected = new int[] {0, 1, 2};
    verifyVectorUDFDatetimeLegacyHybridCalendarTimestamp(batch);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyVectorUDFDatetimeLegacyHybridCalendarTimestamp(batch);
  }

  private VectorizedRowBatch getFreshBatchOfTimestamps(int size) {
    return getVectorizedRowBatch(new java.sql.Timestamp[] {
        new java.sql.Timestamp(Timestamp.valueOf("0001-01-01 00:00:00").toEpochMilli()),
        new java.sql.Timestamp(Timestamp.valueOf("1400-01-01 00:30:00.123456").toEpochMilli()),
        new java.sql.Timestamp(Timestamp.valueOf("1500-01-01 00:30:00").toEpochMilli()),
        new java.sql.Timestamp(Timestamp.valueOf("1583-01-01 00:30:00.123").toEpochMilli()),
        },
        size);
  }

  /**
   * Input array is used to fill the entire specified size of the vector row batch.
   */
  private VectorizedRowBatch getVectorizedRowBatch(java.sql.Timestamp[] inputs, int size) {
    VectorizedRowBatch batch = new VectorizedRowBatch(2, size);
    TimestampColumnVector inputCol = new TimestampColumnVector(size);
    for (int i = 0; i < size; i++) {
      inputCol.set(i, inputs[i % inputs.length]);
    }
    batch.cols[0] = inputCol;
    batch.cols[1] = new TimestampColumnVector(size);
    batch.size = size;
    return batch;
  }

  private void verifyVectorUDFDatetimeLegacyHybridCalendarTimestamp(VectorizedRowBatch batch)
      throws HiveException  {
    GenericUDF genUdf = new GenericUDFDatetimeLegacyHybridCalendar();
    genUdf.initialize(new ObjectInspector[]{
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector});

    VectorExpression vecUdf = new VectorUDFDatetimeLegacyHybridCalendarTimestamp(0, 1);
    vecUdf.evaluate(batch);
    final int in = 0;
    final int out = 1;

    for (int i = 0; i < batch.size; i++) {
      if (batch.cols[in].noNulls || !batch.cols[in].isNull[i]) {
        java.sql.Timestamp input =
            ((TimestampColumnVector) batch.cols[in]).asScratchTimestamp(i);
        java.sql.Timestamp result =
            ((TimestampColumnVector) batch.cols[out]).asScratchTimestamp(i);
        compareToUDFDatetimeLegacyHybridCalendar(genUdf, input, result);
      } else {
        Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
      }
    }
  }

  private void compareToUDFDatetimeLegacyHybridCalendar(
      GenericUDF udf, java.sql.Timestamp in, java.sql.Timestamp out) throws HiveException {
    TimestampWritableV2 tswInput = new TimestampWritableV2(
        org.apache.hadoop.hive.common.type.Timestamp.ofEpochMilli(in.getTime(), in.getNanos()));
    TimestampWritableV2 tswOutput = (TimestampWritableV2) udf
        .evaluate(new GenericUDF.DeferredObject[] {new GenericUDF.DeferredJavaObject(tswInput)});
    Assert.assertEquals(tswOutput.getTimestamp(), Timestamp.ofEpochMilli(out.getTime()));
    Assert.assertEquals(tswOutput.getNanos(), out.getNanos());
  }

  @Test
  public void testVectorUDFDatetimeLegacyHybridCalendarDate() throws HiveException {
    VectorizedRowBatch batch = getFreshBatchOfDates(VectorizedRowBatch.DEFAULT_SIZE);
    Assert.assertTrue(((LongColumnVector) batch.cols[1]).noNulls);
    Assert.assertFalse(((LongColumnVector) batch.cols[1]).isRepeating);
    verifyVectorUDFDatetimeLegacyHybridCalendarDate(batch);
    TestVectorizedRowBatch.addRandomNulls(batch.cols[0]);
    verifyVectorUDFDatetimeLegacyHybridCalendarDate(batch);

    batch = getFreshBatchOfDates(1);
    batch.cols[0].isRepeating = true; //
    verifyVectorUDFDatetimeLegacyHybridCalendarDate(batch);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyVectorUDFDatetimeLegacyHybridCalendarDate(batch);

    batch = getFreshBatchOfDates(3);
    batch.cols[0].isRepeating = false;
    batch.selectedInUse = true;
    batch.selected = new int[] {0, 1, 2};
    verifyVectorUDFDatetimeLegacyHybridCalendarDate(batch);
    batch.cols[0].noNulls = false;
    batch.cols[0].isNull[0] = true;
    verifyVectorUDFDatetimeLegacyHybridCalendarDate(batch);
  }

  private VectorizedRowBatch getFreshBatchOfDates(int size) {
    return getVectorizedRowBatch(new Long[] {
        (long) Date.valueOf("0001-01-01").toEpochDay(),
        (long) Date.valueOf("1400-01-01").toEpochDay(),
        (long) Date.valueOf("1500-01-01").toEpochDay(),
        (long) Date.valueOf("1583-01-01").toEpochDay(),
        },
        size);
  }

  /**
   * Input array is used to fill the entire specified size of the vector row batch.
   */
  private VectorizedRowBatch getVectorizedRowBatch(Long[] inputs, int size) {
    VectorizedRowBatch batch = new VectorizedRowBatch(2, size);
    LongColumnVector inputCol = new LongColumnVector(size);
    for (int i = 0; i < size; i++) {
      inputCol.vector[i] = inputs[i % inputs.length];
    }
    batch.cols[0] = inputCol;
    batch.cols[1] = new LongColumnVector(size);
    batch.size = size;
    return batch;
  }


  private void verifyVectorUDFDatetimeLegacyHybridCalendarDate(VectorizedRowBatch batch)
      throws HiveException {
    GenericUDF genUdf = new GenericUDFDatetimeLegacyHybridCalendar();
    genUdf.initialize(
        new ObjectInspector[] {PrimitiveObjectInspectorFactory.writableDateObjectInspector});

    VectorExpression vecUdf = new VectorUDFDatetimeLegacyHybridCalendarDate(0, 1);
    vecUdf.evaluate(batch);
    final int in = 0;
    final int out = 1;

    for (int i = 0; i < batch.size; i++) {
      if (batch.cols[in].noNulls || !batch.cols[in].isNull[i]) {
        long input = ((LongColumnVector) batch.cols[in]).vector[i];
        long output = ((LongColumnVector) batch.cols[out]).vector[i];
        compareToUDFDatetimeLegacyHybridCalendar(genUdf, input, output);
      } else {
        Assert.assertEquals(batch.cols[out].isNull[i], batch.cols[in].isNull[i]);
      }
    }
  }

  private void compareToUDFDatetimeLegacyHybridCalendar(GenericUDF udf, long in, long out)
      throws HiveException {
    DateWritableV2 dateWInput = new DateWritableV2((int) in);
    DateWritableV2 dateWOutput = (DateWritableV2) udf
        .evaluate(new GenericUDF.DeferredObject[] {
            new GenericUDF.DeferredJavaObject(dateWInput)});
    Assert.assertEquals(dateWOutput.get(), Date.ofEpochDay((int) out));
  }
}
