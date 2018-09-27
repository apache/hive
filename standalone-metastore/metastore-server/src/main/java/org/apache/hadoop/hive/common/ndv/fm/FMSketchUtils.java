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
package org.apache.hadoop.hive.common.ndv.fm;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javolution.util.FastBitSet;

public class FMSketchUtils {

  static final Logger LOG = LoggerFactory.getLogger(FMSketch.class.getName());
  public static final byte[] MAGIC = new byte[] { 'F', 'M' };

  /*
   * Serializes a distinctValueEstimator object to Text for transport.
   *
   * <b>4 byte header</b> is encoded like below 2 bytes - FM magic string to
   * identify serialized stream 2 bytes - numbitvectors because
   * BIT_VECTOR_SIZE=31, 4 bytes are enough to hold positions of 0-31
   */
  public static void serializeFM(OutputStream out, FMSketch fm) throws IOException {
    out.write(MAGIC);

    // max of numBitVectors = 1024, 2 bytes is enough.
    byte[] nbv = new byte[2];
    nbv[0] = (byte) fm.getNumBitVectors();
    nbv[1] = (byte) (fm.getNumBitVectors() >>> 8);

    out.write(nbv);

    // original toString takes too much space
    // we compress a fastbitset to 4 bytes
    for (int i = 0; i < fm.getNumBitVectors(); i++) {
      writeBitVector(out, fm.getBitVector(i));
    }
  }

  // BIT_VECTOR_SIZE is 31, we can use 32 bits, i.e., 4 bytes to represent a
  // FastBitSet, rather than using 31 integers.
  private static void writeBitVector(OutputStream out, FastBitSet bit) throws IOException {
    int num = 0;
    for (int pos = 0; pos < FMSketch.BIT_VECTOR_SIZE; pos++) {
      if (bit.get(pos)) {
        num |= 1 << pos;
      }
    }
    byte[] i = new byte[4];
    for (int j = 0; j < 4; j++) {
      i[j] = (byte) ((num >>> (8 * j)) & 0xff);
    }
    out.write(i);
  }

  /*
   * Deserializes from string to FastBitSet; Creates a NumDistinctValueEstimator
   * object and returns it.
   */
  public static FMSketch deserializeFM(byte[] buf) throws IOException {
    InputStream is = new ByteArrayInputStream(buf);
    try {
      FMSketch sketch = deserializeFM(is);
      is.close();
      return sketch;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static FMSketch deserializeFM(InputStream in) throws IOException {
    checkMagicString(in);

    byte[] nbv = new byte[2];
    nbv[0] = (byte) in.read();
    nbv[1] = (byte) in.read();

    int numBitVectors = 0;
    numBitVectors |= (nbv[0] & 0xff);
    numBitVectors |= ((nbv[1] & 0xff) << 8);

    FMSketch sketch = new FMSketch(numBitVectors);
    for (int n = 0; n < numBitVectors; n++) {
      sketch.setBitVector(readBitVector(in), n);
    }
    return sketch;
  }

  private static FastBitSet readBitVector(InputStream in) throws IOException {
    FastBitSet fastBitSet = new FastBitSet();
    fastBitSet.clear();
    for (int i = 0; i < 4; i++) {
      byte b = (byte) in.read();
      for (int j = 0; j < 8; j++) {
        if ((b & (1 << j)) != 0) {
          fastBitSet.set(j + 8 * i);
        }
      }
    }
    return fastBitSet;
  }

  private static void checkMagicString(InputStream in) throws IOException {
    byte[] magic = new byte[2];
    magic[0] = (byte) in.read();
    magic[1] = (byte) in.read();

    if (!Arrays.equals(magic, MAGIC)) {
      throw new IllegalArgumentException("The input stream is not a FMSketch stream.");
    }
  }
}
