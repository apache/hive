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
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Tests vectorized type cast udfs CastDateToStringWithFormat, CastTimestampToStringWithFormat,
 * CastStringToDateWithFormat, CastStringToTimestampWithFormat.
 */
public class TestVectorTypeCastsWithFormat {

  @Test
  public void testCastDateToStringWithFormat() throws HiveException {
    VectorizedRowBatch b = TestVectorMathFunctions.getVectorizedRowBatchDateInStringOutFormatted();
    BytesColumnVector resultV = (BytesColumnVector) b.cols[1];
    VectorExpression expr = new CastDateToStringWithFormat(0, "yyyy".getBytes(), 1);
    expr.evaluate(b);
    verifyString(0, "2019", resultV);
    verifyString(1, "1776", resultV);
    verifyString(2, "2012", resultV);
    verifyString(3, "1580", resultV);
    verifyString(4, "0005", resultV);
    verifyString(5, "9999", resultV);

    expr = new CastDateToStringWithFormat(0, "MM".getBytes(), 1);
    resultV = new BytesColumnVector();
    b.cols[1] = resultV;
    expr.evaluate(b);
    verifyString(0, "12", resultV);
    verifyString(1, "07", resultV);
    verifyString(2, "02", resultV);
    verifyString(3, "08", resultV);
    verifyString(4, "01", resultV);
    verifyString(5, "12", resultV);
  }

  @Test
  public void testCastTimestampToStringWithFormat() throws HiveException {
    VectorizedRowBatch b =
        TestVectorMathFunctions.getVectorizedRowBatchTimestampInStringOutFormatted();
    BytesColumnVector resultV = (BytesColumnVector) b.cols[1];
    VectorExpression expr = new CastTimestampToStringWithFormat(0, "yyyy".getBytes(), 1);
    expr.evaluate(b);

    Assert.assertEquals("2019", getStringFromBytesColumnVector(resultV, 0));
    Assert.assertEquals("1776", getStringFromBytesColumnVector(resultV, 1));
    Assert.assertEquals("2012", getStringFromBytesColumnVector(resultV, 2));
    Assert.assertEquals("1580", getStringFromBytesColumnVector(resultV, 3));
    Assert.assertEquals("0005", getStringFromBytesColumnVector(resultV, 4));
    Assert.assertEquals("9999", getStringFromBytesColumnVector(resultV, 5));

    resultV = new BytesColumnVector();
    b.cols[1] = resultV;
    expr = new CastTimestampToStringWithFormat(0, "HH24".getBytes(), 1);
    expr.evaluate(b);

    Assert.assertEquals("19", getStringFromBytesColumnVector(resultV, 0));
    Assert.assertEquals("17", getStringFromBytesColumnVector(resultV, 1));
    Assert.assertEquals("23", getStringFromBytesColumnVector(resultV, 2));
    Assert.assertEquals("00", getStringFromBytesColumnVector(resultV, 3));
    Assert.assertEquals("00", getStringFromBytesColumnVector(resultV, 4));
    Assert.assertEquals("23", getStringFromBytesColumnVector(resultV, 5));
  }

  @Test
  public void testCastStringToTimestampWithFormat() throws HiveException {
    VectorizedRowBatch b =
        TestVectorMathFunctions.getVectorizedRowBatchStringInTimestampOutFormatted();
    TimestampColumnVector resultV;
    resultV = new TimestampColumnVector();
    b.cols[1] = resultV;
    VectorExpression expr =
        new CastStringToTimestampWithFormat(0, "yyyy.mm.dd HH24.mi.ss.ff".getBytes(), 1);
    expr.evaluate(b);

    verifyTimestamp("2019-12-31 00:00:00.999999999", resultV, 0);
    verifyTimestamp("1776-07-04 17:07:06.177617761", resultV, 1);
    verifyTimestamp("2012-02-29 23:59:59.999999999", resultV, 2);
    verifyTimestamp("1580-08-08 00:00:00", resultV, 3);
    verifyTimestamp("0005-01-01 00:00:00", resultV, 4);
    verifyTimestamp("9999-12-31 23:59:59.999999999", resultV, 5);
  }

  private void verifyTimestamp(String tsString, TimestampColumnVector resultV, int index) {
    Assert.assertEquals(Timestamp.valueOf(tsString).toEpochMilli(), resultV.time[index]);
    Assert.assertEquals(Timestamp.valueOf(tsString).getNanos(), resultV.nanos[index]);
  }

  @Test
  public void testCastStringToDateWithFormat() throws HiveException {
    VectorizedRowBatch b =
        TestVectorMathFunctions.getVectorizedRowBatchStringInDateOutFormatted();
    LongColumnVector resultV;
    resultV = new LongColumnVector();
    b.cols[1] = resultV;
    VectorExpression expr = new CastStringToDateWithFormat(0, "yyyy.mm.dd".getBytes(), 1);
    expr.evaluate(b);

    Assert.assertEquals(Date.valueOf("2019-12-31").toEpochDay(), resultV.vector[0]);
    Assert.assertEquals(Date.valueOf("1776-07-04").toEpochDay(), resultV.vector[1]);
    Assert.assertEquals(Date.valueOf("2012-02-29").toEpochDay(), resultV.vector[2]);
    Assert.assertEquals(Date.valueOf("1580-08-08").toEpochDay(), resultV.vector[3]);
    Assert.assertEquals(Date.valueOf("0005-01-01").toEpochDay(), resultV.vector[4]);
    Assert.assertEquals(Date.valueOf("9999-12-31").toEpochDay(), resultV.vector[5]);
  }

  private void verifyString(int resultIndex, String expected, BytesColumnVector resultV) {
    String result = getStringFromBytesColumnVector(resultV, resultIndex);
    Assert.assertEquals(expected, result);
  }

  private String getStringFromBytesColumnVector(BytesColumnVector resultV, int i) {
    String result;
    byte[] resultBytes = Arrays.copyOfRange(resultV.vector[i], resultV.start[i],
        resultV.start[i] + resultV.length[i]);
    result = new String(resultBytes, StandardCharsets.UTF_8);
    return result;
  }
}
