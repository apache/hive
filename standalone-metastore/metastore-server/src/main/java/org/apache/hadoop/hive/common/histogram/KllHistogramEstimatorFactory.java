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

public class KllHistogramEstimatorFactory {

  private KllHistogramEstimatorFactory() {
    throw new AssertionError("Suppress default constructor for non instantiation");
  }

  /**
   * This function deserializes the serialized KLL histogram estimator from a byte array.
   * @param buf to deserialize
   * @return KLL histogram estimator
   */
  public static KllHistogramEstimator getKllHistogramEstimator(byte[] buf) {
    return new KllHistogramEstimator(KllUtils.deserializeKll(buf, 0, buf.length));
  }

  /**
   * This function deserializes the serialized KLL histogram estimator from a byte array.
   * @param buf to deserialize
   * @param start start index for deserialization
   * @param len start+len is deserialized
   * @return KLL histogram estimator
   */
  public static KllHistogramEstimator getKllHistogramEstimator(byte[] buf, int start, int len) {
    return new KllHistogramEstimator(KllUtils.deserializeKll(buf, start, len));
  }

  /**
   * This method creates an empty histogram estimator with a KLL sketch with k=200.
   * @return an empty histogram estimator with a KLL sketch with k=200
   */
  public static KllHistogramEstimator getEmptyHistogramEstimator() {
    return new KllHistogramEstimator();
  }

  /**
   * This method creates an empty histogram estimator with a KLL sketch of a given k parameter.
   * @param k the KLL parameter k for initializing the sketch
   * @return an empty histogram estimator with a KLL sketch of a given k parameter
   */
  public static KllHistogramEstimator getEmptyHistogramEstimator(int k) {
    return new KllHistogramEstimator(k);
  }

  /**
   * This method creates an empty KLL histogram estimator, using the k parameter from the given estimator.
   * @param kllHistogramEstimator the estimator used to build the new one
   * @return an empty KLL histogram estimator, using the k parameter from the given estimator
   */
  public static KllHistogramEstimator getEmptyHistogramEstimator(KllHistogramEstimator kllHistogramEstimator) {
    return kllHistogramEstimator == null ? getEmptyHistogramEstimator()
        : getEmptyHistogramEstimator(kllHistogramEstimator.getSketch().getK());
  }
}
