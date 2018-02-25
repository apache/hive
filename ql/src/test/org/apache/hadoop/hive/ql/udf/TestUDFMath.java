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

package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.junit.Assert;
import org.junit.Test;

public class TestUDFMath {
  private HiveDecimalWritable input = null;

  @Test
  public void testAcos() throws HiveException {
    UDFAcos udf = new UDFAcos();
    input = createDecimal("0.716");
    DoubleWritable res = udf.evaluate(input);
    Assert.assertEquals(0.7727408115633954, res.get(), 0.000001);
  }

  @Test
  public void testAsin() throws HiveException {
    UDFAsin udf = new UDFAsin();
    input = createDecimal("0.716");
    DoubleWritable res = udf.evaluate(input);
    Assert.assertEquals(0.7980555152315012, res.get(), 0.000001);
  }

  @Test
  public void testAtan() throws HiveException {
    UDFAtan udf = new UDFAtan();
    input = createDecimal("1.0");
    DoubleWritable res = udf.evaluate(input);
    Assert.assertEquals(0.7853981633974483, res.get(), 0.000001);
  }

  @Test
  public void testCos() throws HiveException {
    UDFCos udf = new UDFCos();
    input = createDecimal("0.7727408115633954");
    DoubleWritable res = udf.evaluate(input);
    Assert.assertEquals(0.716, res.get(), 0.000001);
  }

  @Test
  public void testSin() throws HiveException {
    UDFSin udf = new UDFSin();
    input = createDecimal("0.7980555152315012");
    DoubleWritable res = udf.evaluate(input);
    Assert.assertEquals(0.716, res.get(), 0.000001);
  }

  @Test
  public void testTan() throws HiveException {
    UDFTan udf = new UDFTan();
    input = createDecimal("0.7853981633974483");
    DoubleWritable res = udf.evaluate(input);
    Assert.assertEquals(1.0, res.get(), 0.000001);
  }

  @Test
  public void testExp() throws HiveException {
    UDFExp udf = new UDFExp();
    input = createDecimal("2.0");
    DoubleWritable res = udf.evaluate(input);
    Assert.assertEquals(7.38905609893065, res.get(), 0.000001);
  }

  @Test
  public void testLn() throws HiveException {
    UDFLn udf = new UDFLn();
    input = createDecimal("7.38905609893065");
    DoubleWritable res = udf.evaluate(input);
    Assert.assertEquals(2.0, res.get(), 0.000001);
  }

  @Test
  public void testLog() throws HiveException {
    UDFLog udf = new UDFLog();
    input = createDecimal("7.38905609893065");
    DoubleWritable res = udf.evaluate(input);
    Assert.assertEquals(2.0, res.get(), 0.000001);

    res = udf.evaluate(createDecimal("3.0"), createDecimal("9.0"));
    Assert.assertEquals(2.0, res.get(), 0.000001);
}

  @Test
  public void testLog10() throws HiveException {
    UDFLog10 udf = new UDFLog10();
    input = createDecimal("100.0");
    DoubleWritable res = udf.evaluate(input);
    Assert.assertEquals(2.0, res.get(), 0.000001);
  }

  @Test
  public void testLog2() throws HiveException {
    UDFLog2 udf = new UDFLog2();
    input = createDecimal("8.0");
    DoubleWritable res = udf.evaluate(input);
    Assert.assertEquals(3.0, res.get(), 0.000001);
  }

  @Test
  public void testRadians() throws HiveException {
    UDFRadians udf = new UDFRadians();
    input = createDecimal("45.0");
    DoubleWritable res = udf.evaluate(input);
    Assert.assertEquals(0.7853981633974483, res.get(), 0.000001);
  }

  @Test
  public void testDegrees() throws HiveException {
    UDFDegrees udf = new UDFDegrees();
    input = createDecimal("0.7853981633974483");
    DoubleWritable res = udf.evaluate(input);
    Assert.assertEquals(45.0, res.get(), 0.000001);
  }

  @Test
  public void testSqrt() throws HiveException {
    UDFSqrt udf = new UDFSqrt();
    input = createDecimal("49.0");
    DoubleWritable res = udf.evaluate(input);
    Assert.assertEquals(7.0, res.get(), 0.000001);
  }

  private HiveDecimalWritable createDecimal(String input) {
    return new HiveDecimalWritable(HiveDecimal.create(input));
  }

}
