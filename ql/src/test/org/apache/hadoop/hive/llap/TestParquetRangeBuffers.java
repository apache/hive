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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.llap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.StreamCapabilities;
import org.junit.Test;

public class TestParquetRangeBuffers {

  @Test
  public void anExclusiveStreamsBuffersAreReusedAndLimitedToTheRange() throws IOException {
    ParquetRangeBuffers buffers = ParquetRangeBuffers.forStream(stream(false));
    ByteBuffer first = buffers.allocate(100);
    assertEquals(100, first.limit());
    buffers.release(first);
    // The pool hands back anything large enough: the same 100-byte buffer serves a 50-byte range,
    // and must be limited to it or a reader filling remaining() overruns the range.
    ByteBuffer second = buffers.allocate(50);
    assertSame(first.array(), second.array());
    assertEquals(50, second.limit());
    assertEquals(0, second.position());
  }

  @Test
  public void aSlicingStreamsBuffersAreNeverPooled() throws IOException {
    ParquetRangeBuffers buffers = ParquetRangeBuffers.forStream(stream(true));
    ByteBuffer first = buffers.allocate(100);
    buffers.release(first);
    ByteBuffer second = buffers.allocate(50);
    assertNotSame(first.array(), second.array());
    assertEquals(50, second.capacity());
  }

  /** A stream that does nothing but answer whether its vectored-read buffers are slices. */
  private static FSDataInputStream stream(boolean sliced) throws IOException {
    return new FSDataInputStream(new Inert()) {
      @Override
      public boolean hasCapability(String capability) {
        return sliced && StreamCapabilities.VECTOREDIO_BUFFERS_SLICED.equals(capability);
      }
    };
  }

  private static final class Inert extends InputStream implements Seekable, PositionedReadable {
    @Override public int read() { return -1; }
    @Override public void seek(long pos) { }
    @Override public long getPos() { return 0; }
    @Override public boolean seekToNewSource(long targetPos) { return false; }
    @Override public int read(long position, byte[] buffer, int offset, int length) { return -1; }
    @Override public void readFully(long position, byte[] buffer, int offset, int length) { }
    @Override public void readFully(long position, byte[] buffer) { }
  }
}
