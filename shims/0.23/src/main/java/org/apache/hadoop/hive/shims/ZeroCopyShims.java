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
package org.apache.hadoop.hive.shims;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.hive.shims.HadoopShims.DirectCompressionType;
import org.apache.hadoop.hive.shims.HadoopShims.DirectDecompressorShim;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.compress.DirectDecompressor;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor.SnappyDirectDecompressor;
import org.apache.hadoop.io.compress.zlib.ZlibDecompressor.CompressionHeader;
import org.apache.hadoop.io.compress.zlib.ZlibDecompressor.ZlibDirectDecompressor;

import org.apache.hadoop.hive.shims.HadoopShims.ByteBufferPoolShim;
import org.apache.hadoop.hive.shims.HadoopShims.ZeroCopyReaderShim;

class ZeroCopyShims {
  private static final class ByteBufferPoolAdapter implements ByteBufferPool {
    private ByteBufferPoolShim pool;

    public ByteBufferPoolAdapter(ByteBufferPoolShim pool) {
      this.pool = pool;
    }

    @Override
    public final ByteBuffer getBuffer(boolean direct, int length) {
      return this.pool.getBuffer(direct, length);
    }

    @Override
    public final void putBuffer(ByteBuffer buffer) {
      this.pool.putBuffer(buffer);
    }
  }

  private static final class ZeroCopyAdapter implements ZeroCopyReaderShim {
    private final FSDataInputStream in;
    private final ByteBufferPoolAdapter pool;
    private final static EnumSet<ReadOption> CHECK_SUM = EnumSet
        .noneOf(ReadOption.class);
    private final static EnumSet<ReadOption> NO_CHECK_SUM = EnumSet
        .of(ReadOption.SKIP_CHECKSUMS);

    public ZeroCopyAdapter(FSDataInputStream in, ByteBufferPoolShim poolshim) {
      this.in = in;
      if (poolshim != null) {
        pool = new ByteBufferPoolAdapter(poolshim);
      } else {
        pool = null;
      }
    }

    public final ByteBuffer readBuffer(int maxLength, boolean verifyChecksums)
        throws IOException {
      EnumSet<ReadOption> options = NO_CHECK_SUM;
      if (verifyChecksums) {
        options = CHECK_SUM;
      }
      return this.in.read(this.pool, maxLength, options);
    }

    public final void releaseBuffer(ByteBuffer buffer) {
      this.in.releaseBuffer(buffer);
    }
  }

  public static ZeroCopyReaderShim getZeroCopyReader(FSDataInputStream in,
      ByteBufferPoolShim pool) throws IOException {
    return new ZeroCopyAdapter(in, pool);
  }

  private static final class DirectDecompressorAdapter implements
      DirectDecompressorShim {
    private final DirectDecompressor decompressor;

    public DirectDecompressorAdapter(DirectDecompressor decompressor) {
      this.decompressor = decompressor;
    }

    public void decompress(ByteBuffer src, ByteBuffer dst) throws IOException {
      this.decompressor.decompress(src, dst);
    }
  }

  public static DirectDecompressorShim getDirectDecompressor(
      DirectCompressionType codec) {
    DirectDecompressor decompressor = null;
    switch (codec) {
    case ZLIB: {
      decompressor = new ZlibDirectDecompressor();
    }
      break;
    case ZLIB_NOHEADER: {
      decompressor = new ZlibDirectDecompressor(CompressionHeader.NO_HEADER, 0);
    }
      break;
    case SNAPPY: {
      decompressor = new SnappyDirectDecompressor();
    }
      break;
    }
    if (decompressor != null) {
      return new DirectDecompressorAdapter(decompressor);
    }
    /* not supported */
    return null;
  }
}
