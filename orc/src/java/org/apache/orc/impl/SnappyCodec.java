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

import org.apache.orc.CompressionCodec;
import org.iq80.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;

public class SnappyCodec implements CompressionCodec, DirectDecompressionCodec {
  private static final HadoopShims SHIMS = HadoopShims.Factory.get();

  Boolean direct = null;

  @Override
  public boolean compress(ByteBuffer in, ByteBuffer out,
                          ByteBuffer overflow) throws IOException {
    int inBytes = in.remaining();
    // I should work on a patch for Snappy to support an overflow buffer
    // to prevent the extra buffer copy.
    byte[] compressed = new byte[Snappy.maxCompressedLength(inBytes)];
    int outBytes =
        Snappy.compress(in.array(), in.arrayOffset() + in.position(), inBytes,
            compressed, 0);
    if (outBytes < inBytes) {
      int remaining = out.remaining();
      if (remaining >= outBytes) {
        System.arraycopy(compressed, 0, out.array(), out.arrayOffset() +
            out.position(), outBytes);
        out.position(out.position() + outBytes);
      } else {
        System.arraycopy(compressed, 0, out.array(), out.arrayOffset() +
            out.position(), remaining);
        out.position(out.limit());
        System.arraycopy(compressed, remaining, overflow.array(),
            overflow.arrayOffset(), outBytes - remaining);
        overflow.position(outBytes - remaining);
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void decompress(ByteBuffer in, ByteBuffer out) throws IOException {
    if(in.isDirect() && out.isDirect()) {
      directDecompress(in, out);
      return;
    }
    int inOffset = in.position();
    int uncompressLen =
        Snappy.uncompress(in.array(), in.arrayOffset() + inOffset,
        in.limit() - inOffset, out.array(), out.arrayOffset() + out.position());
    out.position(uncompressLen + out.position());
    out.flip();
  }

  @Override
  public boolean isAvailable() {
    if (direct == null) {
      try {
        if (SHIMS.getDirectDecompressor(
            HadoopShims.DirectCompressionType.SNAPPY) != null) {
          direct = Boolean.valueOf(true);
        } else {
          direct = Boolean.valueOf(false);
        }
      } catch (UnsatisfiedLinkError ule) {
        direct = Boolean.valueOf(false);
      }
    }
    return direct.booleanValue();
  }

  @Override
  public void directDecompress(ByteBuffer in, ByteBuffer out)
      throws IOException {
    HadoopShims.DirectDecompressor decompressShim =
        SHIMS.getDirectDecompressor(HadoopShims.DirectCompressionType.SNAPPY);
    decompressShim.decompress(in, out);
    out.flip(); // flip for read
  }

  @Override
  public CompressionCodec modify(EnumSet<Modifier> modifiers) {
    // snappy allows no modifications
    return this;
  }
}
