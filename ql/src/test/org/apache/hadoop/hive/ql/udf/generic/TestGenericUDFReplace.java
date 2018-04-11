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

import junit.framework.TestCase;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFReplace;
import org.apache.hadoop.io.Text;

public class TestGenericUDFReplace extends TestCase {

  public void testReplace() throws HiveException {
    UDFReplace udf = new UDFReplace();

    // One of the params is null, then expected is null.
    verify(udf, null, new Text(), new Text(), null);
    verify(udf, new Text(), null, new Text(), null);
    verify(udf, new Text(), new Text(), null, null);

    // Empty string
    verify(udf, new Text(), new Text(), new Text(), "");

    // No match
    verify(udf, new Text("ABCDEF"), new Text("X"), new Text("Z"), "ABCDEF");

    // Case-sensitive string found
    verify(udf, new Text("Hack and Hue"), new Text("H"), new Text("BL"), "BLack and BLue");
    verify(udf, new Text("ABABrdvABrk"), new Text("AB"), new Text("a"), "aardvark");
  }


  private void verify(UDFReplace udf, Text str, Text search, Text replacement, String expResult) throws HiveException {
    Text output = (Text) udf.evaluate(str, search, replacement);
    if (expResult == null) {
      assertNull(output);
    } else {
      assertNotNull(output);
      assertEquals("replace() test ", expResult, output.toString());
    }
  }
}
