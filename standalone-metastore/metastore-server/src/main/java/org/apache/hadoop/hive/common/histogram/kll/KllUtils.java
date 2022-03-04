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

package org.apache.hadoop.hive.common.histogram.kll;

import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.hadoop.hive.common.histogram.HistogramEstimator;
import org.apache.hadoop.hive.common.histogram.KllHistogramEstimator;
import org.apache.hadoop.hive.ql.util.JavaDataModel;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * KLL serialization utilities.
 */
public class KllUtils {

  private KllUtils() {
    throw new AssertionError("Suppress default constructor for non instantiation");
  }

  public static final byte[] MAGIC = new byte[] { 'K', 'L', 'L' };

  /**
   * HyperLogLog is serialized using the following format
   *
   * <pre>
   * |-3 byte-|---variable KLL serialization---|
   * -----------------------------------------_
   * | header |            KLL sketch         |
   * ------------------------------------------
   *
   * the header consists of 3 bytes of KLL magic string, used to identify the serialized stream
   *
   * Followed by header is the KLL serialization as provided by datasketches library.
   * </pre>
   * @param out output stream to write to
   * @param kll KLL sketch that needs to be serialized
   * @throws IOException
   */
  public static void serializeKLL(OutputStream out, KllFloatsSketch kll) throws IOException {
    out.write(MAGIC);
    out.write(kll.toByteArray());
  }

  /**
   * This function deserializes the serialized KLL from a byte array.
   * @param buf to deserialize
   * @return KLL histogram estimator
   */
  public static HistogramEstimator deserializeKLL(final byte[] buf) {
    try {
      checkMagicString(buf);
      return new KllHistogramEstimator(KllFloatsSketch.heapify(Memory.wrap(buf)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Check if the specified input stream is actually a KLL stream
   * @param buf buffer
   * @throws IOException
   */
  private static void checkMagicString(byte[] buf) throws IOException {
    byte[] magic = new byte[3];
    System.arraycopy(buf, 0, magic, 0, 3);

    if (!Arrays.equals(magic, MAGIC)) {
      throw new IllegalArgumentException("The input stream is not a KLL stream.");
    }
  }

  /**
   * Return relative error between actual and estimated cardinality
   * @param actualCount actual count
   * @param estimatedCount estimated count
   * @return relative error
   */
  public static float getRelativeError(long actualCount, long estimatedCount) {
    return (1.0f - ((float) estimatedCount / (float) actualCount)) * 100.0f;
  }

  public static int lengthFor(JavaDataModel model, KllFloatsSketch kll) {
    // 3 bytes for header + KLL serialized
    return kll == null ? (int) model.lengthForByteArrayOfSize(3L
        + KllFloatsSketch.getMaxSerializedSizeBytes(200, (long) Math.pow(10, 6)))
        : (int) model.lengthForByteArrayOfSize(3L + kll.getSerializedSizeBytes());
  }
}
