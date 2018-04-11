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

import junit.framework.TestCase;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TestUDFCrc32 extends TestCase {

  public void testCrc32Str() throws HiveException {
    UDFCrc32 udf = new UDFCrc32();

    runAndVerifyStr("ABC", 2743272264L, udf);
    runAndVerifyStr("", 0L, udf);
    // repeat again
    runAndVerifyStr("ABC", 2743272264L, udf);
    runAndVerifyStr("", 0L, udf);
    // null
    runAndVerifyStr(null, null, udf);
  }

  public void testCrc32Bin() throws HiveException {
    UDFCrc32 udf = new UDFCrc32();

    runAndVerifyBin(new byte[] { 65, 66, 67 }, 2743272264L, udf);
    runAndVerifyBin(new byte[0], 0L, udf);
    // repeat again
    runAndVerifyBin(new byte[] { 65, 66, 67 }, 2743272264L, udf);
    runAndVerifyBin(new byte[0], 0L, udf);
    // null
    runAndVerifyBin(null, null, udf);
  }

  private void runAndVerifyStr(String str, Long expResult, UDFCrc32 udf) throws HiveException {
    Text t = str != null ? new Text(str) : null;
    LongWritable output = (LongWritable) udf.evaluate(t);
    if (expResult == null) {
      assertNull(output);
    } else {
      assertNotNull(output);
      assertEquals("crc32() test ", expResult.longValue(), output.get());
    }
  }

  private void runAndVerifyBin(byte[] binV, Long expResult, UDFCrc32 udf) throws HiveException {
    BytesWritable binWr = binV != null ? new BytesWritable(binV) : null;
    LongWritable output = (LongWritable) udf.evaluate(binWr);
    if (expResult == null) {
      assertNull(output);
    } else {
      assertNotNull(output);
      assertEquals("crc32() test ", expResult.longValue(), output.get());
    }
  }
}
