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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

class ZlibCodec implements CompressionCodec {

  @Override
  public boolean compress(ByteBuffer in, ByteBuffer out,
                          ByteBuffer overflow) throws IOException {
    Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
    int length = in.remaining();
    deflater.setInput(in.array(), in.arrayOffset() + in.position(), length);
    deflater.finish();
    int outSize = 0;
    int offset = out.arrayOffset() + out.position();
    while (!deflater.finished() && (length > outSize)) {
      int size = deflater.deflate(out.array(), offset, out.remaining());
      out.position(size + out.position());
      outSize += size;
      offset += size;
      // if we run out of space in the out buffer, use the overflow
      if (out.remaining() == 0) {
        if (overflow == null) {
          deflater.end();
          return false;
        }
        out = overflow;
        offset = out.arrayOffset() + out.position();
      }
    }
    deflater.end();
    return length > outSize;
  }

  @Override
  public void decompress(ByteBuffer in, ByteBuffer out) throws IOException {
    Inflater inflater = new Inflater(true);
    inflater.setInput(in.array(), in.arrayOffset() + in.position(),
                      in.remaining());
    while (!(inflater.finished() || inflater.needsDictionary() ||
             inflater.needsInput())) {
      try {
        int count = inflater.inflate(out.array(),
                                     out.arrayOffset() + out.position(),
                                     out.remaining());
        out.position(count + out.position());
      } catch (DataFormatException dfe) {
        throw new IOException("Bad compression data", dfe);
      }
    }
    out.flip();
    inflater.end();
    in.position(in.limit());
  }

}
