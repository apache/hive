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
package org.apache.hadoop.hive.ql.io.orc.encoded;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.orc.impl.BufferChunk;
import org.junit.Test;

public class TestEncodedReaderImpl {
  @Test
  public void testReadLength() throws IOException {
    ByteBuffer one = ByteBuffer.wrap(new byte[] { 1 }), two = ByteBuffer.wrap(new byte[] { 2 }),
        three = ByteBuffer.wrap(new byte[] { 3 }), twoThree = ByteBuffer.wrap(new byte[] { 2, 3 }),
        oneTwo = ByteBuffer.wrap(new byte[] { 1, 2 });
    BufferChunk bc = new BufferChunk(one, 0);
    int[] result = new int[3];
    List<IncompleteCb> l = new ArrayList<>();
    IoTrace trace = new IoTrace(0, false);
    BufferChunk rv = EncodedReaderImpl.readLengthBytesFromSmallBuffers(
        bc, 0l, result, l, true, trace);
    assertNull(rv);
    one.position(0);
    bc.insertAfter(new BufferChunk(two, 1));
    Arrays.fill(result, -1);
    rv = EncodedReaderImpl.readLengthBytesFromSmallBuffers(bc, 0l, result, l, true, trace);
    assertNull(rv);
    one.position(0);
    two.position(0);
    bc.insertAfter(new BufferChunk(two, 1)).insertAfter(new BufferChunk(three, 2));
    Arrays.fill(result, -1);
    rv = EncodedReaderImpl.readLengthBytesFromSmallBuffers(bc, 0l, result, l, true, trace);
    assertNotNull(rv);
    for (int i = 0; i < result.length; ++i) {
      assertEquals(i + 1, result[i]);
    }
    one.position(0);
    bc.insertAfter(new BufferChunk(twoThree, 1));
    Arrays.fill(result, -1);
    rv = EncodedReaderImpl.readLengthBytesFromSmallBuffers(bc, 0l, result, l, true, trace);
    assertNotNull(rv);
    for (int i = 0; i < result.length; ++i) {
      assertEquals(i + 1, result[i]);
    }
    bc = new BufferChunk(oneTwo, 0);
    Arrays.fill(result, -1);
    rv = EncodedReaderImpl.readLengthBytesFromSmallBuffers(bc, 0l, result, l, true, trace);
    assertNull(rv);
    three.position(0);
    bc.insertAfter(new BufferChunk(three, 2));
    Arrays.fill(result, -1);
    rv = EncodedReaderImpl.readLengthBytesFromSmallBuffers(bc, 0l, result, l, true, trace);
    assertNotNull(rv);
    for (int i = 0; i < result.length; ++i) {
      assertEquals(i + 1, result[i]);
    }
  }
}
