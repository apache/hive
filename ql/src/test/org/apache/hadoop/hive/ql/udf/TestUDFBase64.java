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



import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * TestUDFBase64.
 */
public class TestUDFBase64 {
  @Test
  public void testBase64Conversion(){
    byte[] bytes = "string".getBytes();
    // Let's make sure we only read the relevant part of the writable in case of reuse
    byte[] longBytes = "longer string".getBytes();
    BytesWritable writable = new BytesWritable(longBytes);
    writable.set(bytes, 0, bytes.length);
    UDFBase64 udf = new UDFBase64();
    Text text = udf.evaluate(writable);
    String base64String = text.toString();
    assertEquals("c3RyaW5n", base64String);
  }
}
