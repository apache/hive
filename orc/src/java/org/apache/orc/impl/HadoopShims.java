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

package org.apache.orc.impl;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.VersionInfo;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public interface HadoopShims {

  enum DirectCompressionType {
    NONE,
    ZLIB_NOHEADER,
    ZLIB,
    SNAPPY,
  }

  interface DirectDecompressor {
    void decompress(ByteBuffer var1, ByteBuffer var2) throws IOException;
  }

  /**
   * Get a direct decompressor codec, if it is available
   * @param codec
   * @return
   */
  DirectDecompressor getDirectDecompressor(DirectCompressionType codec);

  /**
   * a hadoop.io ByteBufferPool shim.
   */
  public interface ByteBufferPoolShim {
    /**
     * Get a new ByteBuffer from the pool.  The pool can provide this from
     * removing a buffer from its internal cache, or by allocating a
     * new buffer.
     *
     * @param direct     Whether the buffer should be direct.
     * @param length     The minimum length the buffer will have.
     * @return           A new ByteBuffer. Its capacity can be less
     *                   than what was requested, but must be at
     *                   least 1 byte.
     */
    ByteBuffer getBuffer(boolean direct, int length);

    /**
     * Release a buffer back to the pool.
     * The pool may choose to put this buffer into its cache/free it.
     *
     * @param buffer    a direct bytebuffer
     */
    void putBuffer(ByteBuffer buffer);
  }

  /**
   * Provides an HDFS ZeroCopyReader shim.
   * @param in FSDataInputStream to read from (where the cached/mmap buffers are tied to)
   * @param in ByteBufferPoolShim to allocate fallback buffers with
   *
   * @return returns null if not supported
   */
  public ZeroCopyReaderShim getZeroCopyReader(FSDataInputStream in, ByteBufferPoolShim pool) throws IOException;

  public interface ZeroCopyReaderShim extends Closeable {
    /**
     * Get a ByteBuffer from the FSDataInputStream - this can be either a HeapByteBuffer or an MappedByteBuffer.
     * Also move the in stream by that amount. The data read can be small than maxLength.
     *
     * @return ByteBuffer read from the stream,
     */
    public ByteBuffer readBuffer(int maxLength, boolean verifyChecksums) throws IOException;
    /**
     * Release a ByteBuffer obtained from a read on the
     * Also move the in stream by that amount. The data read can be small than maxLength.
     *
     */
    public void releaseBuffer(ByteBuffer buffer);

    /**
     * Close the underlying stream.
     * @throws IOException
     */
    public void close() throws IOException;
  }
  /**
   * Read data into a Text object in the fastest way possible
   */
  public interface TextReaderShim {
    /**
     * @param txt
     * @param size
     * @return bytes read
     * @throws IOException
     */
    void read(Text txt, int size) throws IOException;
  }

  /**
   * Wrap a TextReaderShim around an input stream. The reader shim will not
   * buffer any reads from the underlying stream and will only consume bytes
   * which are required for TextReaderShim.read() input.
   */
  public TextReaderShim getTextReaderShim(InputStream input) throws IOException;

  class Factory {
    private static HadoopShims SHIMS = null;

    public static synchronized HadoopShims get() {
      if (SHIMS == null) {
        String[] versionParts = VersionInfo.getVersion().split("[.]");
        int major = Integer.parseInt(versionParts[0]);
        int minor = Integer.parseInt(versionParts[1]);
        if (major < 2 || (major == 2 && minor < 3)) {
          SHIMS = new HadoopShims_2_2();
        } else {
          SHIMS = new HadoopShimsCurrent();
        }
      }
      return SHIMS;
    }
  }
}
