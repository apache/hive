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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;

/**
 * Shims for versions of Hadoop up to and including 2.2.x
 */
public class HadoopShims_2_2 implements HadoopShims {

  final boolean zeroCopy;
  final boolean fastRead;

  HadoopShims_2_2() {
    boolean zcr = false;
    try {
      Class.forName("org.apache.hadoop.fs.CacheFlag", false,
        HadoopShims_2_2.class.getClassLoader());
      zcr = true;
    } catch (ClassNotFoundException ce) {
    }
    zeroCopy = zcr;
    boolean fastRead = false;
    if (zcr) {
      for (Method m : Text.class.getMethods()) {
        if ("readWithKnownLength".equals(m.getName())) {
          fastRead = true;
        }
      }
    }
    this.fastRead = fastRead;
  }

  public DirectDecompressor getDirectDecompressor(
      DirectCompressionType codec) {
    return null;
  }

  @Override
  public ZeroCopyReaderShim getZeroCopyReader(FSDataInputStream in,
                                              ByteBufferPoolShim pool
                                              ) throws IOException {
    if(zeroCopy) {
      return ZeroCopyShims.getZeroCopyReader(in, pool);
    }
    /* not supported */
    return null;
  }

  private final class BasicTextReaderShim implements TextReaderShim {
    private final InputStream in;

    public BasicTextReaderShim(InputStream in) {
      this.in = in;
    }

    @Override
    public void read(Text txt, int len) throws IOException {
      int offset = 0;
      byte[] bytes = new byte[len];
      while (len > 0) {
        int written = in.read(bytes, offset, len);
        if (written < 0) {
          throw new EOFException("Can't finish read from " + in + " read "
              + (offset) + " bytes out of " + bytes.length);
        }
        len -= written;
        offset += written;
      }
      txt.set(bytes);
    }
  }

  @Override
  public TextReaderShim getTextReaderShim(InputStream in) throws IOException {
    return new BasicTextReaderShim(in);
  }
}
