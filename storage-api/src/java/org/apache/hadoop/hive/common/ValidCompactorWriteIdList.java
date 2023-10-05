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

package org.apache.hadoop.hive.common;

import java.util.Arrays;
import java.util.BitSet;

/**
 * An implementation of {@link ValidWriteIdList} for use by the compactor.
 *
 * Compaction should only include txns up to smallest open txn (exclusive).
 * There may be aborted write ids in the snapshot represented by this ValidCompactorWriteIdList.
 * Thus {@link #isWriteIdRangeValid(long, long)} returns NONE for any range that includes any unresolved
 * write ids.  Any write id above {@code highWatermark} is unresolved.
 * These produce the logic we need to assure that the compactor only sees records less than the lowest
 * open write ids when choosing which files to compact, but that it still ignores aborted
 * records when compacting.
 *
 * See org.apache.hadoop.hive.metastore.txn.TxnUtils#createValidCompactTxnList() for proper
 * way to construct this.
 */
public class ValidCompactorWriteIdList extends ValidReaderWriteIdList {
  public ValidCompactorWriteIdList() {
    super();
  }
  public ValidCompactorWriteIdList(String tableName, long[] abortedWriteIdList, BitSet abortedBits,
                                   long highWatermark) {
    this(tableName, abortedWriteIdList, abortedBits, highWatermark, Long.MAX_VALUE);
  }
  /**
   * @param tableName table which is under compaction. Full name of format &lt;db_name&gt;.&lt;table_name&gt;
   * @param abortedWriteIdList list of all aborted write ids
   * @param abortedBits bitset marking whether the corresponding write id is aborted
   * @param highWatermark highest committed write id to be considered for compaction,
   *                      equivalently (lowest_open_write_id - 1).
   * @param minOpenWriteId minimum write ID which maps to a open transaction
   */
  public ValidCompactorWriteIdList(String tableName, long[] abortedWriteIdList, BitSet abortedBits,
                                   long highWatermark, long minOpenWriteId) {
    // abortedBits should be all true as everything in exceptions are aborted txns
    super(tableName, abortedWriteIdList, abortedBits, highWatermark, minOpenWriteId);
    if(this.exceptions.length <= 0) {
      return;
    }
    //now that exceptions (aka abortedTxnList) is sorted
    int idx = Arrays.binarySearch(this.exceptions, highWatermark);
    int lastElementPos;
    if(idx < 0) {
      int insertionPoint = -idx - 1 ;//see Arrays.binarySearch() JavaDoc
      lastElementPos = insertionPoint - 1;
    }
    else {
      lastElementPos = idx;
    }
    /*
     * ensure that we throw out any exceptions above highWatermark to make
     * {@link #isWriteIdValid(long)} faster
     */
    this.exceptions = Arrays.copyOf(this.exceptions, lastElementPos + 1);
  }
  public ValidCompactorWriteIdList(String value) {
    super(value);
  }
  /**
   * Returns org.apache.hadoop.hive.common.ValidWriteIdList.RangeResponse.ALL if all write ids in
   * the range are resolved and RangeResponse.NONE otherwise
   * Streaming ingest may create something like delta_11_20.  Compactor cannot include such delta in
   * compaction until all transactions that write to it terminate.  (Otherwise compactor
   * will produce delta that doesn't satisfy (D1 intersect D2 is empty or D1 intersect D2 = D2).
   */
  @Override
  public RangeResponse isWriteIdRangeValid(long minWriteId, long maxWriteId) {
    return highWatermark >= maxWriteId ? RangeResponse.ALL : RangeResponse.NONE;
  }

  @Override
  public boolean isWriteIdAborted(long writeId) {
    return Arrays.binarySearch(exceptions, writeId) >= 0;
  }
}
