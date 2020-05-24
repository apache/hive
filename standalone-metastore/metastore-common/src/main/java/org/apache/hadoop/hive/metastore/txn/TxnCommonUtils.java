/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore.txn;

import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;

import java.util.BitSet;
import java.util.Collections;
import java.util.List;

public class TxnCommonUtils {
  /**
   * Transform a {@link org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse} to a
   * {@link org.apache.hadoop.hive.common.ValidTxnList}.  This assumes that the caller intends to
   * read the files, and thus treats both open and aborted transactions as invalid.
   * @param txns txn list from the metastore
   * @param currentTxn Current transaction that the user has open.  If this is greater than 0 it
   *                   will be removed from the exceptions list so that the user sees his own
   *                   transaction as valid.
   * @return a valid txn list.
   */
  public static ValidTxnList createValidReadTxnList(GetOpenTxnsResponse txns, long currentTxn) {
    assert currentTxn <= txns.getTxn_high_water_mark();
    /*
     * The highWaterMark should be min(currentTxn,txns.getTxn_high_water_mark()) assuming currentTxn>0
     * otherwise if currentTxn=7 and 8 commits before 7, then 7 will see result of 8 which
     * doesn't make sense for Snapshot Isolation. Of course for Read Committed, the list should
     * include the latest committed set.
     */
    long highWaterMark = (currentTxn > 0) ? Math.min(currentTxn, txns.getTxn_high_water_mark())
                                          : txns.getTxn_high_water_mark();

    // Open txns are already sorted in ascending order. This list may or may not include HWM
    // but it is guaranteed that list won't have txn > HWM. But, if we overwrite the HWM with currentTxn
    // then need to truncate the exceptions list accordingly.
    List<Long> openTxns = txns.getOpen_txns();

    // We care only about open/aborted txns below currentTxn and hence the size should be determined
    // for the exceptions list. The currentTxn will be missing in openTxns list only in rare case like
    // txn is read-only or aborted by AcidHouseKeeperService and compactor actually cleans up the aborted txns.
    // So, for such cases, we get negative value for sizeToHwm with found position for currentTxn, and so,
    // we just negate it to get the size.
    int sizeToHwm = (currentTxn > 0) ? Math.abs(Collections.binarySearch(openTxns, currentTxn)) : openTxns.size();
    sizeToHwm = Math.min(sizeToHwm, openTxns.size());
    long[] exceptions = new long[sizeToHwm];
    BitSet inAbortedBits = BitSet.valueOf(txns.getAbortedBits());
    BitSet outAbortedBits = new BitSet();
    long minOpenTxnId = Long.MAX_VALUE;
    int i = 0;
    for (long txn : openTxns) {
      // For snapshot isolation, we don't care about txns greater than current txn and so stop here.
      // Also, we need not include current txn to exceptions list.
      if ((currentTxn > 0) && (txn >= currentTxn)) {
        break;
      }
      if (inAbortedBits.get(i)) {
        outAbortedBits.set(i);
      } else if (minOpenTxnId == Long.MAX_VALUE) {
        minOpenTxnId = txn;
      }
      exceptions[i++] = txn;
    }
    return new ValidReadTxnList(exceptions, outAbortedBits, highWaterMark, minOpenTxnId);
  }

  /**
   * Transform a {@link org.apache.hadoop.hive.metastore.api.GetValidWriteIdsResponse} to a
   * {@link org.apache.hadoop.hive.common.ValidTxnWriteIdList}.  This assumes that the caller intends to
   * read the files, and thus treats both open and aborted transactions as invalid.
   * @param currentTxnId current txn ID for which we get the valid write ids list
   * @param validIds valid write ids list from the metastore
   * @return a valid write IDs list for the whole transaction.
   */
  public static ValidTxnWriteIdList createValidTxnWriteIdList(Long currentTxnId,
                                                              List<TableValidWriteIds> validIds) {
    ValidTxnWriteIdList validTxnWriteIdList = new ValidTxnWriteIdList(currentTxnId);
    for (TableValidWriteIds tableWriteIds : validIds) {
      validTxnWriteIdList.addTableValidWriteIdList(createValidReaderWriteIdList(tableWriteIds));
    }
    return validTxnWriteIdList;
  }

  /**
   * Transform a {@link TableValidWriteIds} to a
   * {@link org.apache.hadoop.hive.common.ValidReaderWriteIdList}.  This assumes that the caller intends to
   * read the files, and thus treats both open and aborted write ids as invalid.
   * @param tableWriteIds valid write ids for the given table from the metastore
   * @return a valid write IDs list for the input table
   */
  public static ValidReaderWriteIdList createValidReaderWriteIdList(TableValidWriteIds tableWriteIds) {
    String fullTableName = tableWriteIds.getFullTableName();
    long highWater = tableWriteIds.getWriteIdHighWaterMark();
    List<Long> invalids = tableWriteIds.getInvalidWriteIds();
    BitSet abortedBits = BitSet.valueOf(tableWriteIds.getAbortedBits());
    long[] exceptions = new long[invalids.size()];
    int i = 0;
    for (long writeId : invalids) {
      exceptions[i++] = writeId;
    }
    if (tableWriteIds.isSetMinOpenWriteId()) {
      return new ValidReaderWriteIdList(fullTableName, exceptions, abortedBits, highWater,
                                        tableWriteIds.getMinOpenWriteId());
    } else {
      return new ValidReaderWriteIdList(fullTableName, exceptions, abortedBits, highWater);
    }
  }
}
