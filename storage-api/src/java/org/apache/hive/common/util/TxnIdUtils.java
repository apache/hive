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
    if (!a.getTableName().equalsIgnoreCase(b.getTableName())) {
      return false;
    }
    ValidWriteIdList newer = a;
    ValidWriteIdList older = b;
    if (a.getHighWatermark() < b.getHighWatermark()) {
      newer = b;
      older = a;
    }

    return checkEquivalentCommittedIds(
        older.getHighWatermark(), older.getInvalidWriteIds(),
        newer.getHighWatermark(), newer.getInvalidWriteIds());
  }

  /**
   * Check the min open ID/highwater mark/exceptions list to see if 2 ID lists are at the same commit point.
   * This can also be used for ValidTxnList as well as ValidWriteIdList.
   */
  private static boolean checkEquivalentCommittedIds(
      long oldHWM, long[] oldInvalidIds,
      long newHWM, long[] newInvalidIds) {

    // There should be no valid txns in newer list that are not also in older.
    // - All values in oldInvalidIds should also be in newInvalidIds.
    // - if oldHWM < newHWM, then all IDs between oldHWM .. newHWM should exist in newInvalidTxns.
    //   A Gap in the sequence means a committed txn in newer list (lists are not equivalent)

    if (newInvalidIds.length < oldInvalidIds.length) {
      return false;
    }

    // Check that the values in the older list are also in newer. Lists should already be sorted.
    for (int idx = 0; idx < oldInvalidIds.length; ++idx) {
      if (oldInvalidIds[idx] != newInvalidIds[idx]) {
        return false;
      }
    }

    // If older committed state is equivalent to newer state, then there should be no committed IDs
    // between oldHWM and newHWM, and newInvalidIds should have exactly (newHWM - oldHWM)
    // more entries than oldInvalidIds.
    long oldNewListSizeDifference = newInvalidIds.length - oldInvalidIds.length;
    long oldNewHWMDifference = newHWM - oldHWM;
    if (oldNewHWMDifference != oldNewListSizeDifference) {
      return false;
    }

    return true;
  }
}
