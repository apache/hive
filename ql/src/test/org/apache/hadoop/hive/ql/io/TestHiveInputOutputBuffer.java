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
package org.apache.hadoop.hive.ql.io;

import java.io.IOException;

import junit.framework.TestCase;

/**
 * TestHiveInputOutputBuffer.
 *
 */
public class TestHiveInputOutputBuffer extends TestCase {

  public void testReadAndWrite() throws IOException {
    String testString = "test_hive_input_output_number_0";
    byte[] string_bytes = testString.getBytes();
    NonSyncDataInputBuffer inBuffer = new NonSyncDataInputBuffer();
    NonSyncDataOutputBuffer outBuffer = new NonSyncDataOutputBuffer();
    outBuffer.write(string_bytes);
    inBuffer.reset(outBuffer.getData(), 0, outBuffer.getLength());
    byte[] readBytes = new byte[string_bytes.length];
    inBuffer.read(readBytes);
    String readString = new String(readBytes);
    assertEquals("Field testReadAndWrite()", readString, testString);
  }

}
