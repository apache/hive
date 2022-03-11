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

/**
 * KLL serialization utilities.
 */
public class KllUtils {

  private KllUtils() {
    throw new AssertionError("Suppress default constructor for non instantiation");
  }

  /**
   * KLL is serialized according to what provided by data-sketches library
   * @param out output stream to write to
   * @param kll KLL sketch that needs to be serialized
   * @throws IOException
   */
  public static void serializeKLL(OutputStream out, KllFloatsSketch kll) throws IOException {
    out.write(kll.toByteArray());
  }

  /**
   * This function deserializes the serialized KLL from a byte array.
   * @param buf to deserialize
   * @return KLL histogram estimator
   */
  public static HistogramEstimator deserializeKLL(final byte[] buf) {
    return new KllHistogramEstimator(KllFloatsSketch.heapify(Memory.wrap(buf)));
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
    // KLL serialized
    return kll == null ? KllFloatsSketch.getMaxSerializedSizeBytes(200, (long) Math.pow(10, 6))
        : (int) model.lengthForByteArrayOfSize(kll.getSerializedSizeBytes());
  }
}
