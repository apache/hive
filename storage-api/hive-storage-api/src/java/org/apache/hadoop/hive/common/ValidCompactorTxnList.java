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
 * An implementation of {@link org.apache.hadoop.hive.common.ValidTxnList} for use by the compactor.
 * 
 * Compaction should only include txns up to smallest open txn (exclussive).
 * There may be aborted txns in the snapshot represented by this ValidCompactorTxnList.
 * Thus {@link #isTxnRangeValid(long, long)} returns NONE for any range that inluces any unresolved
 * transactions.  Any txn above {@code highWatermark} is unresolved.
 * These produce the logic we need to assure that the compactor only sees records less than the lowest
 * open transaction when choosing which files to compact, but that it still ignores aborted
 * records when compacting.
 * 
 * See org.apache.hadoop.hive.metastore.txn.TxnUtils#createValidCompactTxnList() for proper
 * way to construct this.
 */
public class ValidCompactorTxnList extends ValidReadTxnList {
  public ValidCompactorTxnList() {
    super();
  }
  public ValidCompactorTxnList(long[] abortedTxnList, BitSet abortedBits, long highWatermark) {
    this(abortedTxnList, abortedBits, highWatermark, Long.MAX_VALUE);
  }
  /**
   * @param abortedTxnList list of all aborted transactions
   * @param abortedBits bitset marking whether the corresponding transaction is aborted
   * @param highWatermark highest committed transaction to be considered for compaction,
   *                      equivalently (lowest_open_txn - 1).
   */
  public ValidCompactorTxnList(long[] abortedTxnList, BitSet abortedBits, long highWatermark, long minOpenTxnId) {
    // abortedBits should be all true as everything in exceptions are aborted txns
    super(abortedTxnList, abortedBits, highWatermark, minOpenTxnId);
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
     * {@link #isTxnValid(long)} faster 
     */
    this.exceptions = Arrays.copyOf(this.exceptions, lastElementPos + 1);
  }
  public ValidCompactorTxnList(String value) {
    super(value);
  }
  /**
   * Returns org.apache.hadoop.hive.common.ValidTxnList.RangeResponse.ALL if all txns in
   * the range are resolved and RangeResponse.NONE otherwise
   */
  @Override
  public RangeResponse isTxnRangeValid(long minTxnId, long maxTxnId) {
    return highWatermark >= maxTxnId ? RangeResponse.ALL : RangeResponse.NONE;
  }

  @Override
  public boolean isTxnAborted(long txnid) {
    return Arrays.binarySearch(exceptions, txnid) >= 0;
  }
}
