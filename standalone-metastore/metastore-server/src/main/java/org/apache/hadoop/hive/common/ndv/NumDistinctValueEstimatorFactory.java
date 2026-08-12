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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.common.ndv;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hive.common.ndv.fm.FMSketch;
import org.apache.hadoop.hive.common.ndv.fm.FMSketchUtils;
import org.apache.hadoop.hive.common.ndv.hll.HyperLogLog;
import org.apache.hadoop.hive.common.ndv.hll.HyperLogLogUtils;

public class NumDistinctValueEstimatorFactory {

  private NumDistinctValueEstimatorFactory() {
  }

  private static boolean isFMSketch(byte[] buf) throws IOException {
    byte[] magic = new byte[2];
    magic[0] = (byte) buf[0];
    magic[1] = (byte) buf[1];
    return Arrays.equals(magic, FMSketchUtils.MAGIC);
  }

  public static NumDistinctValueEstimator getNumDistinctValueEstimator(byte[] buf) {
    // Right now we assume only FM and HLL are available.
    try {
      if (isFMSketch(buf)) {
        return FMSketchUtils.deserializeFM(buf);
      } else {
        return HyperLogLogUtils.deserializeHLL(buf);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static NumDistinctValueEstimator getEmptyNumDistinctValueEstimator(
      NumDistinctValueEstimator n) {
    if (n instanceof FMSketch) {
      return new FMSketch(((FMSketch) n).getNumBitVectors());
    } else {
      return HyperLogLog.builder().setSizeOptimized().build();
    }
  }

  public static NumDistinctValueEstimator getEmptyNumDistinctValueEstimator(String func,
      int numBitVectors) {
    if ("fm".equals(func.toLowerCase())) {
      return new FMSketch(numBitVectors);
    } else if ("hll".equals(func.toLowerCase())) {
      return HyperLogLog.builder().setSizeOptimized().build();
    } else {
      throw new RuntimeException("Can not recognize " + func);
    }
  }

}
