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

package org.apache.hadoop.hive.ql.exec.vector.wrapper;

import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBase.HashContext;

public class VectorHashKeyWrapperFactory {

  public static VectorHashKeyWrapperBase allocate(HashContext ctx, int longValuesCount,
      int doubleValuesCount, int byteValuesCount, int decimalValuesCount, int timestampValuesCount,
      int intervalDayTimeValuesCount, int keyCount) {

    final int nonLongBytesCount =
        doubleValuesCount + decimalValuesCount +
        timestampValuesCount + intervalDayTimeValuesCount;

    /*
     * Add more special cases as desired.
     * FUTURE: Consider writing a init time "classifier" that returns an enum so we don't have to
     * FUTURE: analyze these counts over and over...
     */
    if (nonLongBytesCount == 0) {
      if (byteValuesCount == 0) {
        if (longValuesCount == 1) {
          return new VectorHashKeyWrapperSingleLong();
        } else if (longValuesCount == 2) {
          return new VectorHashKeyWrapperTwoLong();
        } else if (longValuesCount == 0) {
          return VectorHashKeyWrapperEmpty.EMPTY_KEY_WRAPPER;
        }
      }
    }

    // Fall through to use the general wrapper.
    return new VectorHashKeyWrapperGeneral(ctx, longValuesCount, doubleValuesCount, byteValuesCount,
        decimalValuesCount, timestampValuesCount, intervalDayTimeValuesCount,
        keyCount);
  }
}
