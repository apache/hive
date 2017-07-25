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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.common.ndv;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.common.ndv.fm.FMSketch;
import org.apache.hadoop.hive.common.ndv.fm.FMSketchUtils;
import org.apache.hadoop.hive.common.ndv.hll.HyperLogLog;

public class NumDistinctValueEstimatorFactory {

  private NumDistinctValueEstimatorFactory() {
  }

  private static boolean isFMSketch(String s) throws IOException {
    InputStream in = new ByteArrayInputStream(Base64.decodeBase64(s));
    byte[] magic = new byte[2];
    magic[0] = (byte) in.read();
    magic[1] = (byte) in.read();
    in.close();
    return Arrays.equals(magic, FMSketchUtils.MAGIC);
  }

  public static NumDistinctValueEstimator getNumDistinctValueEstimator(String s) {
    // Right now we assume only FM and HLL are available.
    try {
      if (isFMSketch(s)) {
        return FMSketchUtils.deserializeFM(s);
      } else {
        return HyperLogLog.builder().build().deserialize(s);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static NumDistinctValueEstimator getEmptyNumDistinctValueEstimator(
      NumDistinctValueEstimator n) {
    if (n instanceof FMSketch) {
      return new FMSketch(((FMSketch) n).getnumBitVectors());
    } else {
      return HyperLogLog.builder().build();
    }
  }

  public static NumDistinctValueEstimator getEmptyNumDistinctValueEstimator(String func,
      int numBitVectors) {
    if ("fm".equals(func.toLowerCase())) {
      return new FMSketch(numBitVectors);
    } else if ("hll".equals(func.toLowerCase())) {
      return HyperLogLog.builder().build();
    } else {
      throw new RuntimeException("Can not recognize " + func);
    }
  }

}
