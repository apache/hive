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

package org.apache.hadoop.hive.ql.io.orc;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

public class TestZlib {

  @Test
  public void testNoOverflow() throws Exception {
    ByteBuffer in = ByteBuffer.allocate(10);
    ByteBuffer out = ByteBuffer.allocate(10);
    in.put(new byte[]{1,2,3,4,5,6,7,10});
    in.flip();
    CompressionCodec codec = new ZlibCodec();
    assertEquals(false, codec.compress(in, out, null));
  }

  @Test
  public void testCorrupt() throws Exception {
    ByteBuffer buf = ByteBuffer.allocate(1000);
    buf.put(new byte[]{127,-128,0,99,98,-1});
    buf.flip();
    CompressionCodec codec = new ZlibCodec();
    ByteBuffer out = ByteBuffer.allocate(1000);
    try {
      codec.decompress(buf, out);
      fail();
    } catch (IOException ioe) {
      // EXPECTED
    }
  }
}
