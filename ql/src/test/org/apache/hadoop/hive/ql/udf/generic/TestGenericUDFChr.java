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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFChr;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;


public class TestGenericUDFChr {

  @Test
  public void testChr() throws HiveException {
    UDFChr udf = new UDFChr();

    // Test string "0"
    double d = 48.0d;
    float f = 48.0f;
    long l = 48L;
    int i = 48;
    short s = 48;
    runAndVerify(d, udf, "0");
    runAndVerify(f, udf, "0");
    runAndVerify(l, udf, "0");
    runAndVerify(i, udf, "0");
    runAndVerify(s, udf, "0");

    // Test string "A"
    d = 65.123d;
    f = 65.123f;
    l = 65L;
    i = 65;
    s = 65;
    runAndVerify(d, udf, "A");
    runAndVerify(f, udf, "A");
    runAndVerify(l, udf, "A");
    runAndVerify(i, udf, "A");
    runAndVerify(s, udf, "A");

    // Test negative integers result in ""
    d = -65.123d;
    f = -65.123f;
    l = -65L;
    i = -65;
    s = -65;
    runAndVerify(d, udf, "");
    runAndVerify(f, udf, "");
    runAndVerify(l, udf, "");
    runAndVerify(i, udf, "");
    runAndVerify(s, udf, "");

    // Test 0 is nul character
    d = 0.9d;
    f = 0.9f;
    l = 0L;
    i = 0;
    s = 0;
    char nul = '\u0000';
    String nulString = String.valueOf(nul);
    runAndVerify(d, udf, nulString);
    runAndVerify(f, udf, nulString);
    runAndVerify(l, udf, nulString);
    runAndVerify(i, udf, nulString);
    runAndVerify(s, udf, nulString);

    // Test 256 or greater is n % 256
    d = 256.9d;
    f = 256.9f;
    l = 256L;
    i = 256;
    s = 256;
    runAndVerify(d, udf, nulString);
    runAndVerify(f, udf, nulString);
    runAndVerify(l, udf, nulString);
    runAndVerify(i, udf, nulString);
    runAndVerify(s, udf, nulString);
    
    d = 321.9d;
    f = 321.9f;
    l = 321L;
    i = 321;
    s = 321;
    runAndVerify(d, udf, "A");
    runAndVerify(f, udf, "A");
    runAndVerify(l, udf, "A");
    runAndVerify(i, udf, "A");
    runAndVerify(s, udf, "A");

    // Test down-casting when greater than 256.
    d = Double.MAX_VALUE;
    f = Float.MAX_VALUE;
    l = Long.MAX_VALUE;
    i = Integer.MAX_VALUE;
    s = Short.MAX_VALUE;  // 32767 % 256 = 255
    runAndVerify(d, udf, "");
    runAndVerify(f, udf, "");
    runAndVerify(l, udf, "");
    runAndVerify(i, udf, "");
    runAndVerify(s, udf, "ÿ");

  }

  private void runAndVerify(long v, UDFChr udf, String expV) throws HiveException {
    Text output = (Text) udf.evaluate(new LongWritable(v));
    verifyOutput(output, expV);
  }

  private void runAndVerify(int v, UDFChr udf, String expV) throws HiveException {
    Text output = (Text) udf.evaluate(new LongWritable(v));
    verifyOutput(output, expV);
  }

  private void runAndVerify(short v, UDFChr udf, String expV) throws HiveException {
    Text output = (Text) udf.evaluate(new LongWritable(v));
    verifyOutput(output, expV);
  }

  private void runAndVerify(double v, UDFChr udf, String expV) throws HiveException {
    Text output = (Text) udf.evaluate(new DoubleWritable(v));
    verifyOutput(output, expV);
  }

  private void runAndVerify(float v, UDFChr udf, String expV) throws HiveException {
    Text output = (Text) udf.evaluate(new DoubleWritable(v));
    verifyOutput(output, expV);
  }

  private void verifyOutput(Text output, String expV) {
    if (expV == null) {
      Assert.assertNull(output);
    } else {
      Assert.assertNotNull(output);
      Assert.assertEquals("chr() test ", expV, output.toString());
    }
  }
}
