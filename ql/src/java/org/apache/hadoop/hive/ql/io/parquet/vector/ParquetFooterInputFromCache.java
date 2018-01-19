/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Arrays;

import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.common.io.encoded.MemoryBufferOrBuffers;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * The Parquet InputFile implementation that allows the reader to
 * read the footer from cache without being aware of the latter.
 * This implements both InputFile and the InputStream that the reader gets from InputFile.
 */
final class ParquetFooterInputFromCache
    extends SeekableInputStream implements InputFile {
  final static int FOOTER_LENGTH_SIZE = 4; // For the file size check.
  private static final int TAIL_LENGTH = ParquetFileWriter.MAGIC.length + FOOTER_LENGTH_SIZE;
  private static final int FAKE_PREFIX_LENGTH = ParquetFileWriter.MAGIC.length;
  private final int length, footerLength;
  private int position = 0, bufferIx = 0, bufferPos = 0;
  private final MemoryBuffer[] cacheData;
  private final int[] positions;

  public ParquetFooterInputFromCache(MemoryBufferOrBuffers footerData) {
    MemoryBuffer oneBuffer = footerData.getSingleBuffer();
    if (oneBuffer != null) {
      cacheData = new MemoryBuffer[2];
      cacheData[0] = oneBuffer;
    } else {
      MemoryBuffer[] bufs = footerData.getMultipleBuffers();
      cacheData = new MemoryBuffer[bufs.length + 1];
      System.arraycopy(bufs, 0, cacheData, 0, bufs.length);
    }
    int footerLength = 0;
    positions = new int[cacheData.length];
    for (int i = 0; i < cacheData.length - 1; ++i) {
      positions[i] = footerLength;
      int dataLen = cacheData[i].getByteBufferRaw().remaining();
      assert dataLen > 0;
      footerLength += dataLen;
    }
    positions[cacheData.length - 1] = footerLength;
    cacheData[cacheData.length - 1] = new FooterEndBuffer(footerLength);
    this.footerLength = footerLength;
    this.length = footerLength + FAKE_PREFIX_LENGTH + TAIL_LENGTH;
  }

  @Override
  public long getLength() throws IOException {
    return length;
  }

  @Override
  public SeekableInputStream newStream() throws IOException {
    // Note: this doesn't maintain proper newStream semantics (if any). 
    //       We could either clone this instead or enforce that this is only called once.
    return this;
  }

  @Override
  public long getPos() throws IOException {
    return position;
  }

  @Override
  public void seek(long targetPos) throws IOException {
    this.position = (int)targetPos;
    targetPos -= FAKE_PREFIX_LENGTH;
    // Not efficient, but we don't expect this to be called frequently.
    for (int i = 1; i <= positions.length; ++i) {
      int endPos = (i == positions.length) ? (length - FAKE_PREFIX_LENGTH) : positions[i];
      if (endPos > targetPos) {
        bufferIx = i - 1;
        bufferPos = (int) (targetPos - positions[i - 1]);
        return;
      }
    }
    throw new IOException("Incorrect seek " + targetPos + "; footer length " + footerLength
        + Arrays.toString(positions));
  }

  @Override
  public void readFully(byte[] b, int offset, int len) throws IOException {
    if (readInternal(b, offset, len) == len) return;
    throw new EOFException();
  }

  public int readInternal(byte[] b, int offset, int len) {
    if (position >= length) return -1;
    int argPos = offset, argEnd = offset + len;
    while (argPos < argEnd) {
      if (bufferIx == cacheData.length) return (argPos - offset);
      ByteBuffer data = cacheData[bufferIx].getByteBufferDup();
      int toConsume = Math.min(argEnd - argPos, data.remaining() - bufferPos);
      data.position(data.position() + bufferPos);
      data.get(b, argPos, toConsume);
      if (data.remaining() == 0) {
        ++bufferIx;
        bufferPos = 0;
      } else {
        bufferPos += toConsume;
      }
      argPos += toConsume;
    }
    return len;
  }

  @Override
  public int read() throws IOException {
    if (position >= length) return -1;
    ++position;
    ByteBuffer data = cacheData[bufferIx].getByteBufferRaw();
    int bp = bufferPos;
    ++bufferPos;
    if (bufferPos == data.remaining()) {
      ++bufferIx; // The first line check should handle the OOB.
      bufferPos = 0;
    }
    return data.get(data.position() + bp) & 0xFF;
  }

  @Override
  public int read(ByteBuffer bb) throws IOException {
    // Simple implementation for now - currently Parquet uses heap buffers.
    int result = -1;
    if (bb.hasArray()) {
      result = readInternal(bb.array(), bb.arrayOffset(), result);
      if (result > 0) {
        bb.position(bb.position() + result);
      }
    } else {
      byte[] b = new byte[bb.remaining()];
      result = readInternal(b, 0, result);
      bb.put(b, 0, result);
    }
    return result;
  }

  @Override
  public void readFully(byte[] arg0) throws IOException {
    readFully(arg0, 0, arg0.length);
  }

  @Override
  public void readFully(ByteBuffer arg0) throws IOException {
    read(arg0);
  }

  /**
   * The fake buffer that emulates end of file, with footer length and magic. Given that these
   * can be generated based on the footer buffer itself, we don't cache them.
   */
  private final static class FooterEndBuffer implements MemoryBuffer {
    private final ByteBuffer bb;
    public FooterEndBuffer(int footerLength) {
      byte[] b = new byte[8];
      b[0] = (byte) ((footerLength >>>  0) & 0xFF);
      b[1] = (byte) ((footerLength >>>  8) & 0xFF);
      b[2] = (byte) ((footerLength >>> 16) & 0xFF);
      b[3] = (byte) ((footerLength >>> 24) & 0xFF);
      for (int i = 0; i < ParquetFileWriter.MAGIC.length; ++i) {
        b[4 + i] = ParquetFileWriter.MAGIC[i];
      }
      bb = ByteBuffer.wrap(b);
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