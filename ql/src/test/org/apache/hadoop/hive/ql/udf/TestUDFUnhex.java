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

import org.apache.hadoop.io.Text;

public class TestUDFUnhex extends TestCase {
  public void testUnhexConversion(){
    Text hex = new Text();
    // Let's make sure we only read the relevant part of the writable in case of reuse
    hex.set("57686174207765726520796F7520686F70696E6720666F723F");
    hex.set("737472696E67");

    byte[] expected = "string".getBytes();

    UDFUnhex udf = new UDFUnhex();
    byte[] output = udf.evaluate(hex);
    assertEquals(expected.length,output.length);
    for (int i = 0; i < expected.length; i++){
      assertEquals(expected[i], output[i]);
    }
  }
}
