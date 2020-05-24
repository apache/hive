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



import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * TestUDFSha1.
 */
public class TestUDFSha1 {

  @Test
  public void testSha1Str() throws HiveException {
    UDFSha1 udf = new UDFSha1();

    runAndVerifyStr("ABC", "3c01bdbb26f358bab27f267924aa2c9a03fcfdb8", udf);
    runAndVerifyStr("", "da39a3ee5e6b4b0d3255bfef95601890afd80709", udf);
    // null
    runAndVerifyStr(null, null, udf);
  }

  @Test
  public void testSha1Bin() throws HiveException {
    UDFSha1 udf = new UDFSha1();

    runAndVerifyBin(new byte[] { 65, 66, 67 }, "3c01bdbb26f358bab27f267924aa2c9a03fcfdb8", udf);
    runAndVerifyBin(new byte[0], "da39a3ee5e6b4b0d3255bfef95601890afd80709", udf);
    // null
    runAndVerifyBin(null, null, udf);
  }

  private void runAndVerifyStr(String str, String expResult, UDFSha1 udf) throws HiveException {
    Text t = str != null ? new Text(str) : null;
    Text output = (Text) udf.evaluate(t);
    assertEquals("sha1() test ", expResult, output != null ? output.toString() : null);
  }

  private void runAndVerifyBin(byte[] binV, String expResult, UDFSha1 udf) throws HiveException {
    BytesWritable binWr = binV != null ? new BytesWritable(binV) : null;
    Text output = (Text) udf.evaluate(binWr);
    assertEquals("sha1() test ", expResult, output != null ? output.toString() : null);
  }
}
