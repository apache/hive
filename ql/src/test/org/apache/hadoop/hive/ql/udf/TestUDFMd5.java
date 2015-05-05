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
package org.apache.hadoop.hive.ql.udf;

import junit.framework.TestCase;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

public class TestUDFMd5 extends TestCase {

  public void testMD5Str() throws HiveException {
    UDFMd5 udf = new UDFMd5();

    runAndVerifyStr("ABC", "902fbdd2b1df0c4f70b4a5d23525e932", udf);
    runAndVerifyStr("", "d41d8cd98f00b204e9800998ecf8427e", udf);
    // null
    runAndVerifyStr(null, null, udf);
  }

  public void testMD5Bin() throws HiveException {
    UDFMd5 udf = new UDFMd5();

    runAndVerifyBin(new byte[] { 65, 66, 67 }, "902fbdd2b1df0c4f70b4a5d23525e932", udf);
    runAndVerifyBin(new byte[0], "d41d8cd98f00b204e9800998ecf8427e", udf);
    // null
    runAndVerifyBin(null, null, udf);
  }

  private void runAndVerifyStr(String str, String expResult, UDFMd5 udf) throws HiveException {
    Text t = str != null ? new Text(str) : null;
    Text output = (Text) udf.evaluate(t);
    assertEquals("md5() test ", expResult, output != null ? output.toString() : null);
  }

  private void runAndVerifyBin(byte[] binV, String expResult, UDFMd5 udf) throws HiveException {
    BytesWritable binWr = binV != null ? new BytesWritable(binV) : null;
    Text output = (Text) udf.evaluate(binWr);
    assertEquals("md5() test ", expResult, output != null ? output.toString() : null);
  }
}
