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
import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

public class TestInStream {

  static class OutputCollector implements OutStream.OutputReceiver {
    DynamicByteArray buffer = new DynamicByteArray();

    @Override
    public void output(ByteBuffer buffer) throws IOException {
      this.buffer.add(buffer.array(), buffer.arrayOffset() + buffer.position(),
          buffer.remaining());
    }
  }

  static class PositionCollector implements PositionProvider, PositionRecorder {
    private List<Long> positions = new ArrayList<Long>();
    private int index = 0;

    @Override
    public long getNext() {
      return positions.get(index++);
    }

    @Override
    public void addPosition(long offset) {
      positions.add(offset);
    }
  }

  @Test
  public void testUncompressed() throws Exception {
    OutputCollector collect = new OutputCollector();
    OutStream out = new OutStream("test", 100, null, collect);
    PositionCollector[] positions = new PositionCollector[1024];
    for(int i=0; i < 1024; ++i) {
      positions[i] = new PositionCollector();
      out.getPosition(positions[i]);
      out.write(i);
    }
    out.flush();
    assertEquals(1024, collect.buffer.size());
    for(int i=0; i < 1024; ++i) {
      assertEquals((byte) i, collect.buffer.get(i));
    }
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    InStream in = InStream.create("test", inBuf, null, 100);
    assertEquals("uncompressed stream test base: 0 offset: 0 limit: 1024",
        in.toString());
    for(int i=0; i < 1024; ++i) {
      int x = in.read();
      assertEquals(i & 0xff, x);
    }
    for(int i=1023; i >= 0; --i) {
      in.seek(positions[i]);
      assertEquals(i & 0xff, in.read());
    }
  }

  @Test
  public void testCompressed() throws Exception {
    OutputCollector collect = new OutputCollector();
    CompressionCodec codec = new ZlibCodec();
    OutStream out = new OutStream("test", 300, codec, collect);
    PositionCollector[] positions = new PositionCollector[1024];
    for(int i=0; i < 1024; ++i) {
      positions[i] = new PositionCollector();
      out.getPosition(positions[i]);
      out.write(i);
    }
    out.flush();
    assertEquals("test", out.toString());
    assertEquals(961, collect.buffer.size());
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    InStream in = InStream.create("test", inBuf, codec, 300);
    assertEquals("compressed stream test base: 0 offset: 0 limit: 961",
        in.toString());
    for(int i=0; i < 1024; ++i) {
      int x = in.read();
      assertEquals(i & 0xff, x);
    }
    assertEquals(0, in.available());
    for(int i=1023; i >= 0; --i) {
      in.seek(positions[i]);
      assertEquals(i & 0xff, in.read());
    }
  }

  @Test
  public void testCorruptStream() throws Exception {
    OutputCollector collect = new OutputCollector();
    CompressionCodec codec = new ZlibCodec();
    OutStream out = new OutStream("test", 500, codec, collect);
    PositionCollector[] positions = new PositionCollector[1024];
    for(int i=0; i < 1024; ++i) {
      positions[i] = new PositionCollector();
      out.getPosition(positions[i]);
      out.write(i);
    }
    out.flush();

    // now try to read the stream with a buffer that is too small
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    InStream in = InStream.create("test", inBuf, codec, 100);
    byte[] contents = new byte[1024];
    try {
      in.read(contents);
      fail();
    } catch(IllegalArgumentException iae) {
      // EXPECTED
    }

    // make a corrupted header
    inBuf.clear();
    inBuf.put((byte) 32);
    inBuf.put((byte) 0);
    inBuf.flip();
    in = InStream.create("test2", inBuf, codec, 300);
    try {
      in.read();
      fail();
    } catch (IllegalStateException ise) {
      // EXPECTED
    }
  }
}
