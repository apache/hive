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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test cases for UDFConv class.
 */
public class TestUDFConv {

  private UDFConv udf;

  @Before
  public void setUp() {
    udf = new UDFConv();
  }

  @Test
  public void testConvFunctionSmallNumbers() {
    runAndVerifyStr("3", 10, 2, "11", udf);
    runAndVerifyStr("-15", 10, -16, "-F", udf);
    runAndVerifyStr("-15", 10, 16, "FFFFFFFFFFFFFFF1", udf);
    runAndVerifyStr("big", 36, 16, "3A48", udf);
    runAndVerifyStr(null, 36, 16, null, udf);
    runAndVerifyStr("3", null, 16, null, udf);
    runAndVerifyStr("3", 16, null, null, udf);
    runAndVerifyStr("1234", 10, 37, null, udf);
    runAndVerifyStr("", 10, 16, null, udf);
    runAndVerifyStr("3", -10, 2, null, udf);
    runAndVerifyStr("3", -10, -2, null, udf);
    runAndVerifyStr("3", 0, -2, null, udf);
    runAndVerifyStr("3", 10, 0, null, udf);
    runAndVerifyStr("3", 2, 10, null, udf);
    runAndVerifyStr("a", 36, 10, "10", udf);
    runAndVerifyStr("A", 36, 10, "10", udf);
    runAndVerifyStr("10", 10, 36, "A", udf);
  }

  @Test
  public void testConvFunctionBigNumbers() {
    runAndVerifyStr("9223372036854775807", 36, 16, "12DDAC15F246BAF8C0D551AC7", udf);
    runAndVerifyStr("92233720368547758070", 10, 16, "4FFFFFFFFFFFFFFF6", udf);
    runAndVerifyStr("-92233720368547758070", 10, -16, "-4FFFFFFFFFFFFFFF6", udf);
    runAndVerifyStr("-92233720368547758070", 10, 16, "3000000000000000A", udf);
    runAndVerifyStr("100000000000000000000000000000000000000000000000000000000000000000", 2, 10,
            "36893488147419103232", udf);
    runAndVerifyStr("100000000000000000000000000000000000000000000000000000000000000000", 2, 8,
            "4000000000000000000000", udf);
    runAndVerifyStr("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", 16, 10,
            "115792089237316195423570985008687907853269984665640564039457584007913129639935", udf);
  }

  @Test
  public void testConvFunctionWithInvalidCharacters() {
    runAndVerifyStr("11abc", 10, 16, "B", udf);
    runAndVerifyStr("ABC325TGH", 16, 10, "11256613", udf);
    runAndVerifyStr("ABC325 TGH", 16, 10, "11256613", udf);
    runAndVerifyStr("-11abc", 10, -16, "-B", udf);
    runAndVerifyStr("+010134", 2, 10, "5", udf);
    runAndVerifyStr("+01A0134", 2, 10, "1", udf);
    runAndVerifyStr("+01-01", 2, 10, "1", udf);
    runAndVerifyStr("++01A0134", 2, 10, null, udf);
    runAndVerifyStr("--1", 2, 10, null, udf);
    runAndVerifyStr("++1", 2, 10, null, udf);
    runAndVerifyStr("-", 2, 10, null, udf);
    runAndVerifyStr("+", 2, 10, null, udf);
    runAndVerifyStr("?", 2, 10, null, udf);
  }

  private void runAndVerifyStr(String str, Integer fromBase, Integer toBase, String expResult, UDFConv udf) {
    Text t = str != null ? new Text(str) : null;
    IntWritable fromBaseResolved = fromBase != null ? new IntWritable(fromBase) : null;
    IntWritable toBaseResolved = toBase != null ? new IntWritable(toBase) : null;
    Text output = udf.evaluate(t, fromBaseResolved, toBaseResolved);
    assertEquals("conv function test ", expResult, output != null ? output.toString() : null);
  }
}
