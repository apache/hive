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
package org.apache.hadoop.hive.ql.io.parquet.vector;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.common.io.encoded.MemoryBufferOrBuffers;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * Serves cached byte ranges of a Parquet file at the offsets the file itself uses.
 *
 * <p>Parquet seeks to the offset the footer records for a column chunk to read a bloom filter, so unlike
 * {@link ParquetFooterInputFromCache}, which presents the footer as a small file of its own, this keeps the
 * original offsets and backs only the ranges that were cached.
 *
 * <p>Ranges are disjoint and a read stays inside the one it started in, as every structure served here is
 * self contained at its offset.
 */
public final class ParquetFilterDataFromCache
    extends SeekableInputStream implements InputFile {

  /** A cached range of the file, held at the offset the file stores it at. */
  public record Range(long offset, int length, MemoryBufferOrBuffers data) {
  }

  private final long[] starts;
  private final long[] ends;
  private final MemoryBuffer[][] buffers;
  private final Path path;
  private final Configuration conf;

  private long fileLength = -1;
  private long position;
  private int rangeIx = -1;
  private int bufferIx;
  private int bufferPos;

  public ParquetFilterDataFromCache(List<Range> ranges, Path path, Configuration conf) {
    this.path = path;
    this.conf = conf;
    starts = new long[ranges.size()];
    ends = new long[ranges.size()];
    buffers = new MemoryBuffer[ranges.size()][];
    for (int i = 0; i < ranges.size(); ++i) {
      Range range = ranges.get(i);
      MemoryBuffer single = range.data().getSingleBuffer();
      buffers[i] = (single != null) ? new MemoryBuffer[] { single } : range.data().getMultipleBuffers();
      starts[i] = range.offset();
      ends[i] = range.offset() + range.length();
    }
    position = (starts.length == 0) ? 0 : starts[0];
  }

  @Override
  public long getLength() throws IOException {
    // Only selected ranges are backed here, so the length comes from the file rather than from the last
    // range end, which would be short. Parquet does not ask for it when the footer is supplied, so this
    // stats the file at most once and usually never.
    if (fileLength < 0) {
      fileLength = HadoopInputFile.fromPath(path, conf).getLength();
    }
    return fileLength;
  }

  @Override
  public SeekableInputStream newStream() {
    return this;
  }

  @Override
  public long getPos() {
    return position;
  }

  @Override
  public void seek(long targetPos) throws IOException {
    for (int i = 0; i < starts.length; ++i) {
      if (targetPos >= starts[i] && targetPos < ends[i]) {
        position = targetPos;
        rangeIx = i;
        long relative = targetPos - starts[i];
        for (bufferIx = 0; bufferIx < buffers[i].length; ++bufferIx) {
          int size = buffers[i][bufferIx].getByteBufferRaw().remaining();
          if (relative < size) {
            bufferPos = (int) relative;
            return;
          }
          relative -= size;
        }
        bufferPos = 0;
        return;
      }
    }
    throw new IOException("Seek to " + targetPos + " outside the cached ranges " + describeRanges());
  }

  private String describeRanges() {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < starts.length; ++i) {
      sb.append(i == 0 ? "" : ", ").append('[').append(starts[i]).append(", ").append(ends[i]).append(')');
    }
    return sb.append(']').toString();
  }

  private int readInternal(byte[] b, int offset, int len) {
    if (rangeIx < 0) {
      return 0;
    }
    int argPos = offset;
    int argEnd = offset + len;
    MemoryBuffer[] rangeBuffers = buffers[rangeIx];
    while (argPos < argEnd) {
      if (bufferIx >= rangeBuffers.length) {
        return argPos - offset;
      }
      ByteBuffer data = rangeBuffers[bufferIx].getByteBufferDup();
      int available = data.remaining() - bufferPos;
      if (available <= 0) {
        ++bufferIx;
        bufferPos = 0;
        continue;
      }
      int toConsume = Math.min(argEnd - argPos, available);
      data.position(data.position() + bufferPos);
      data.get(b, argPos, toConsume);
      bufferPos += toConsume;
      argPos += toConsume;
      position += toConsume;
    }
    return len;
  }

  @Override
  public void readFully(byte[] b, int offset, int len) throws IOException {
    if (readInternal(b, offset, len) != len) {
      throw new EOFException();
    }
  }

  @Override
  public void readFully(byte[] b) throws IOException {
    readFully(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int offset, int len) {
    int read = readInternal(b, offset, len);
    return (read == 0 && len > 0) ? -1 : read;
  }

  @Override
  public int read() throws IOException {
    byte[] one = new byte[1];
    return (readInternal(one, 0, 1) == 1) ? (one[0] & 0xFF) : -1;
  }

  @Override
  public int read(ByteBuffer bb) throws IOException {
    byte[] buffer = new byte[bb.remaining()];
    int read = readInternal(buffer, 0, buffer.length);
    if (read <= 0) {
      return -1;
    }
    bb.put(buffer, 0, read);
    return read;
  }

  @Override
  public void readFully(ByteBuffer bb) throws IOException {
    byte[] buffer = new byte[bb.remaining()];
    readFully(buffer, 0, buffer.length);
    bb.put(buffer);
  }
}
