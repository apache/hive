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



import org.apache.hadoop.io.Text;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Test;

/**
 * TestUDFUnhex.
 */
public class TestUDFUnhex {
  @Test
  public void testUnhexConversion(){
    Text hex = new Text();
    // Let's make sure we only read the relevant part of the writable in case of reuse
    hex.set("57686174207765726520796F7520686F70696E6720666F723F");
    hex.set("737472696E67");

    byte[] expected = "string".getBytes();

    UDFUnhex udf = new UDFUnhex();
    byte[] output = udf.evaluate(hex);
    assertEquals(expected.length,output.length);
    assertArrayEquals(expected, output);
  }

  @Test
  public void testUnhexOddLength() {
    UDFUnhex udf = new UDFUnhex();

    Text hex1 = new Text("A");
    byte[] expected1 = new byte[] { (byte) 0x0A };
    assertArrayEquals(expected1, udf.evaluate(hex1));

    Text hex2 = new Text("123");
    byte[] expected2 = new byte[] { (byte) 0x01, (byte) 0x23 };
    assertArrayEquals(expected2, udf.evaluate(hex2));
  }

  @Test
  public void testUnhexInvalidCharacters() {
    UDFUnhex udf = new UDFUnhex();

    Text hex = new Text("7374G9");
    assertNull("Should return null for invalid hex characters", udf.evaluate(hex));

    Text hexOddInvalid = new Text("12G");
    assertNull("Should return null for invalid hex characters in odd length string", udf.evaluate(hexOddInvalid));
  }

  @Test
  public void testUnhexNullEmptyCases() {
    UDFUnhex udf = new UDFUnhex();

    assertNull(udf.evaluate(null));

    Text hexEmpty = new Text("");
    byte[] expectedEmpty = new byte[0];
    assertArrayEquals(expectedEmpty, udf.evaluate(hexEmpty));
  }
}
