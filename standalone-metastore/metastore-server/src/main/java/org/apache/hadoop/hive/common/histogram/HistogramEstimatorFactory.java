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

package org.apache.hadoop.hive.common.histogram;

import org.apache.hadoop.hive.common.histogram.kll.KllUtils;

import java.util.Arrays;

public class HistogramEstimatorFactory {

  private HistogramEstimatorFactory() {
  }

  private static boolean isKllSketch(byte[] buf) {
    byte[] magic = new byte[3];
    magic[0] = buf[0];
    magic[1] = buf[1];
    magic[2] = buf[2];
    return Arrays.equals(magic, KllUtils.MAGIC);
  }

  public static HistogramEstimator getHistogramEstimator(byte[] buf) {
    // Right now we assume only KLL is available.
    if (isKllSketch(buf)) {
      return KllUtils.deserializeKLL(buf);
    } else {
      throw new RuntimeException("Unknown histogram estimator with magic number: "
          + buf[0] + buf[1] + buf[2]);
    }
  }

  public static HistogramEstimator getEmptyHistogramEstimator(int k) {
    return new KllHistogramEstimator(k);
  }

  public static HistogramEstimator getEmptyHistogramEstimator(HistogramEstimator histogramEstimator) {
    return new KllHistogramEstimator(((KllHistogramEstimator) histogramEstimator).getKll());
  }
}
