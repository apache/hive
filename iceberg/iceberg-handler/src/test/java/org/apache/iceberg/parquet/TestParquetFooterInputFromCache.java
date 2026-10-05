/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.common.io.encoded.MemoryBufferOrBuffers;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.junit.Assert;
import org.junit.Test;

public class TestParquetFooterInputFromCache {

  @Test
  public void testUnencryptedMagicAtPrefixAndTail() throws IOException {
    byte[] footerBytes = new byte[] {10, 20, 30, 40, 50};
    MemoryBufferOrBuffers footerData = singleBuffer(footerBytes);

    ParquetFooterInputFromCache input = new ParquetFooterInputFromCache(footerData);

    // Total length: prefix(4) + footer(5) + tail(4 + 4) = 17
    Assert.assertEquals(17, input.getLength());

    // Read prefix: should be PAR1
    input.seek(0);
    byte[] prefix = new byte[4];
    input.readFully(prefix);
    Assert.assertArrayEquals(ParquetFileWriter.MAGIC, prefix);

    // Read tail magic: last 4 bytes should be PAR1
    input.seek(input.getLength() - 4);
    byte[] tailMagic = new byte[4];
    input.readFully(tailMagic);
    Assert.assertArrayEquals(ParquetFileWriter.MAGIC, tailMagic);

    // Read footer length from tail: 4 bytes before the tail magic
    input.seek(input.getLength() - 8);
    byte[] lenBytes = new byte[4];
    input.readFully(lenBytes);
    int footerLength = (lenBytes[0] & 0xFF) | ((lenBytes[1] & 0xFF) << 8) |
        ((lenBytes[2] & 0xFF) << 16) | ((lenBytes[3] & 0xFF) << 24);
    Assert.assertEquals(5, footerLength);
  }

  @Test
  public void testEncryptedMagicAtPrefixAndTail() throws IOException {
    byte[] footerBytes = new byte[] {1, 2, 3, 4, 5, 6, 7};
    MemoryBufferOrBuffers footerData = singleBuffer(footerBytes);

    ParquetFooterInputFromCache input =
        new ParquetFooterInputFromCache(footerData, ParquetFileWriter.EFMAGIC);

    // Total length: prefix(4) + footer(7) + tail(4 + 4) = 19
    Assert.assertEquals(19, input.getLength());

    // Read prefix: should be PARE
    input.seek(0);
    byte[] prefix = new byte[4];
    input.readFully(prefix);
    Assert.assertArrayEquals(ParquetFileWriter.EFMAGIC, prefix);

    // Read tail magic: last 4 bytes should be PARE
    input.seek(input.getLength() - 4);
    byte[] tailMagic = new byte[4];
    input.readFully(tailMagic);
    Assert.assertArrayEquals(ParquetFileWriter.EFMAGIC, tailMagic);

    // Read footer length from tail
    input.seek(input.getLength() - 8);
    byte[] lenBytes = new byte[4];
    input.readFully(lenBytes);
    int footerLength = (lenBytes[0] & 0xFF) | ((lenBytes[1] & 0xFF) << 8) |
        ((lenBytes[2] & 0xFF) << 16) | ((lenBytes[3] & 0xFF) << 24);
    Assert.assertEquals(7, footerLength);
  }

  @Test
  public void testFooterDataReadCorrectly() throws IOException {
    byte[] footerBytes = new byte[] {0x11, 0x22, 0x33, 0x44, 0x55};
    MemoryBufferOrBuffers footerData = singleBuffer(footerBytes);

    ParquetFooterInputFromCache input =
        new ParquetFooterInputFromCache(footerData, ParquetFileWriter.EFMAGIC);

    // Footer data starts after prefix (4 bytes)
    input.seek(4);
    byte[] readBack = new byte[5];
    input.readFully(readBack);
    Assert.assertArrayEquals(footerBytes, readBack);
  }

  @Test
  public void testSequentialReadAcrossAllRegions() throws IOException {
    byte[] footerBytes = new byte[] {(byte) 0xAA, (byte) 0xBB};
    MemoryBufferOrBuffers footerData = singleBuffer(footerBytes);

    ParquetFooterInputFromCache input =
        new ParquetFooterInputFromCache(footerData, ParquetFileWriter.EFMAGIC);

    // Length: prefix(4) + footer(2) + tail(8) = 14
    Assert.assertEquals(14, input.getLength());

    // Read entire content sequentially from position 0
    input.seek(0);
    byte[] all = new byte[14];
    input.readFully(all);

    // Verify prefix: PARE
    Assert.assertEquals('P', all[0]);
    Assert.assertEquals('A', all[1]);
    Assert.assertEquals('R', all[2]);
    Assert.assertEquals('E', all[3]);

    // Verify footer data
    Assert.assertEquals((byte) 0xAA, all[4]);
    Assert.assertEquals((byte) 0xBB, all[5]);

    // Verify tail: footer length (2, little-endian) + PARE
    Assert.assertEquals(2, all[6]);
    Assert.assertEquals(0, all[7]);
    Assert.assertEquals(0, all[8]);
    Assert.assertEquals(0, all[9]);
    Assert.assertEquals('P', all[10]);
    Assert.assertEquals('A', all[11]);
    Assert.assertEquals('R', all[12]);
    Assert.assertEquals('E', all[13]);
  }

  @Test
  public void testMultipleBuffers() throws IOException {
    byte[] part1 = new byte[] {1, 2, 3};
    byte[] part2 = new byte[] {4, 5};
    MemoryBufferOrBuffers footerData = multipleBuffers(part1, part2);

    ParquetFooterInputFromCache input =
        new ParquetFooterInputFromCache(footerData, ParquetFileWriter.MAGIC);

    // Length: prefix(4) + footer(5) + tail(8) = 17
    Assert.assertEquals(17, input.getLength());

    // Read footer region which spans two buffers
    input.seek(4);
    byte[] footer = new byte[5];
    input.readFully(footer);
    Assert.assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, footer);
  }

  private static MemoryBufferOrBuffers singleBuffer(byte[] data) {
    MemoryBuffer buf = new TestMemoryBuffer(data);
    return new MemoryBufferOrBuffers() {
      @Override
      public MemoryBuffer getSingleBuffer() {
        return buf;
      }

      @Override
      public MemoryBuffer[] getMultipleBuffers() {
        return null;
      }
    };
  }

  private static MemoryBufferOrBuffers multipleBuffers(byte[]... parts) {
    MemoryBuffer[] bufs = new MemoryBuffer[parts.length];
    for (int i = 0; i < parts.length; i++) {
      bufs[i] = new TestMemoryBuffer(parts[i]);
    }
    return new MemoryBufferOrBuffers() {
      @Override
      public MemoryBuffer getSingleBuffer() {
        return null;
      }

      @Override
      public MemoryBuffer[] getMultipleBuffers() {
        return bufs;
      }
    };
  }

  private static class TestMemoryBuffer implements MemoryBuffer {
    private final ByteBuffer bb;

    TestMemoryBuffer(byte[] data) {
      bb = ByteBuffer.wrap(data);
    }

    @Override
    public ByteBuffer getByteBufferRaw() {
      return bb;
    }

    @Override
    public ByteBuffer getByteBufferDup() {
      return bb.duplicate();
    }
  }
}
