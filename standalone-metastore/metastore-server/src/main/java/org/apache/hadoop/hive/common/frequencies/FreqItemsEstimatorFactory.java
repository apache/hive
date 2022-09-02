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
package org.apache.hadoop.hive.common.frequencies;

import org.apache.hadoop.hive.common.frequencies.freqitems.FIUtils;

public class FreqItemsEstimatorFactory {

  private FreqItemsEstimatorFactory() {
    throw new AssertionError("Suppress default constructor for non instantiation");
  }

  /**
   * This function deserializes the serialized KLL histogram estimator from a byte array.
   * @param buf to deserialize
   * @return KLL histogram estimator
   */
  public static FreqItemsEstimator getFreqItemsEstimator(byte[] buf) {
    return new FreqItemsEstimator(FIUtils.deserializeFI(buf, 0, buf.length));
  }

  /**
   * This function deserializes the serialized KLL histogram estimator from a byte array.
   * @param buf to deserialize
   * @param start start index for deserialization
   * @param len start+len is deserialized
   * @return KLL histogram estimator
   */
  public static FreqItemsEstimator getFreqItemsEstimator(byte[] buf, int start, int len) {
    return new FreqItemsEstimator(FIUtils.deserializeFI(buf, start, len));
  }

  /**
   * This method creates an empty histogram estimator with a KLL sketch with k=200.
   * @return an empty histogram estimator with a KLL sketch with k=200
   */
  public static FreqItemsEstimator getEmptyFreqItemsEstimator() {
    return new FreqItemsEstimator();
  }

  /**
   * This method creates an empty frequent items estimator with ItemsSketch of a given maximum size.
   * @param sz the Freq Items parameter for capturing the maximum most frequent items.
   * @return an empty histogram estimator with a KLL sketch of a given k parameter
   */
  public static FreqItemsEstimator getEmptyFreqItemsEstimator(int sz) {
    return new FreqItemsEstimator(sz);
  }

  /**
   * This method creates an empty KLL histogram estimator, using the k parameter from the given estimator.
   * @param freqItemsEstimator the estimator used to build the new one
   * @return an empty Freq Items estimator, using the maxMapSize parameter from the given estimator
   */
  public static FreqItemsEstimator getEmptyFreqItemsEstimator(FreqItemsEstimator freqItemsEstimator) {
    //TODO: SB: Check getMaximumMapCap is equivalent to maxMapSize
    return getEmptyFreqItemsEstimator(freqItemsEstimator.getSketch().getMaximumMapCapacity());
  }
}
