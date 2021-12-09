/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.common.util;

import org.apache.hadoop.hive.common.ValidWriteIdList;

public class TxnIdUtils {

  /**
   * Check if 2 ValidWriteIdLists are at an equivalent commit point.
   */
  public static boolean checkEquivalentWriteIds(ValidWriteIdList a, ValidWriteIdList b) {
    return compare(a, b) == 0;
  }

  /*** Compare the freshness of two ValidWriteIdList
   * @param a
   * @param b
   * @return 0, if a and b are equivalent
   * 1, if a is more recent
   * -1, if b is more recent
   ***/
  public static int compare(ValidWriteIdList a, ValidWriteIdList b) {
    if (!a.getTableName().equalsIgnoreCase(b.getTableName())) {
      return a.getTableName().toLowerCase().compareTo(b.getTableName().toLowerCase());
    }
    // The algorithm assumes invalidWriteIds are sorted and values are less or equal than hwm, here is how
    // the algorithm works:
    // 1. Compare two invalidWriteIds until one the list ends, difference means the mismatch writeid is
    //    committed in one ValidWriteIdList but not the other, the comparison end
    // 2. Every writeid from the last writeid in the short invalidWriteIds till its hwm should be committed
    //    in the other ValidWriteIdList, otherwise the comparison end
    // 3. Every writeid from lower hwm to higher hwm should be invalid, otherwise, the comparison end
    int minLen = Math.min(a.getInvalidWriteIds().length, b.getInvalidWriteIds().length);
    for (int i=0;i<minLen;i++) {
      if (a.getInvalidWriteIds()[i] == b.getInvalidWriteIds()[i]) {
        continue;
      }
      return a.getInvalidWriteIds()[i] > b.getInvalidWriteIds()[i]?1:-1;
    }
    if (a.getInvalidWriteIds().length == b.getInvalidWriteIds().length) {
      return Long.signum(a.getHighWatermark() - b.getHighWatermark());
    }
    if (a.getInvalidWriteIds().length == minLen) {
      if (a.getHighWatermark() != b.getInvalidWriteIds()[minLen] -1) {
        return Long.signum(a.getHighWatermark() - (b.getInvalidWriteIds()[minLen] -1));
      }
      if (allInvalidFrom(b.getInvalidWriteIds(), minLen, b.getHighWatermark())) {
        return 0;
      } else {
        return -1;
      }
    } else {
      if (b.getHighWatermark() != a.getInvalidWriteIds()[minLen] -1) {
        return Long.signum((a.getInvalidWriteIds()[minLen] - 1) - b.getHighWatermark());
      }
      if (allInvalidFrom(a.getInvalidWriteIds(), minLen, a.getHighWatermark())) {
        return 0;
      } else {
        return 1;
      }
    }
  }
  private static boolean allInvalidFrom(long[] invalidIds, int start, long hwm) {
    for (int i=start+1;i<invalidIds.length;i++) {
      if (invalidIds[i] != (invalidIds[i-1]+1)) {
        return false;
      }
    }
    return invalidIds[invalidIds.length-1] == hwm;
  }
}
