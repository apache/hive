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

import org.apache.commons.lang3.StringUtils;
import org.apache.hive.common.util.SuppressFBWarnings;

import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/**
 * An implementation of {@link org.apache.hadoop.hive.common.ValidTxnList} for use by readers.
 * This class will view a transaction as valid only if it is committed.  Both open and aborted
 * transactions will be seen as invalid.
 */
public class ValidReadTxnList implements ValidTxnList {

  private static final int MIN_RANGE_LENGTH = 5;
  protected long[] exceptions;
  protected BitSet abortedBits; // BitSet for flagging aborted transactions. Bit is true if aborted, false if open
  //default value means there are no open txn in the snapshot
  private long minOpenTxn = Long.MAX_VALUE;
  protected long highWatermark;

  public ValidReadTxnList() {
    this(new long[0], new BitSet(), Long.MAX_VALUE, Long.MAX_VALUE);
  }

  /**
   * Used if there are no open transactions in the snapshot
   */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "Ref external obj for efficiency")
  public ValidReadTxnList(long[] exceptions, BitSet abortedBits, long highWatermark, long minOpenTxn) {
    if (exceptions.length > 0) {
      this.minOpenTxn = minOpenTxn;
    }
    this.exceptions = exceptions;
    this.abortedBits = abortedBits;
    this.highWatermark = highWatermark;
  }

  public ValidReadTxnList(String value) {
    readFromString(value);
  }

  @Override
  public void removeException(long txnId) {
    exceptions = ArrayUtils.remove(exceptions, Arrays.binarySearch(exceptions, txnId));
  }

  @Override
  public boolean isTxnValid(long txnid) {
    if (txnid > highWatermark) {
      return false;
    }
    return Arrays.binarySearch(exceptions, txnid) < 0;
  }

  @Override
  public RangeResponse isTxnRangeValid(long minTxnId, long maxTxnId) {
    // check the easy cases first
    if (minTxnId > highWatermark) {
      return RangeResponse.NONE;
    } else if (exceptions.length > 0 && exceptions[0] > maxTxnId) {
      return RangeResponse.ALL;
    }

    // since the exceptions and the range in question overlap, count the
    // exceptions in the range
    long count = Math.max(0, maxTxnId - highWatermark);
    for(long txn: exceptions) {
      if (minTxnId <= txn && txn <= maxTxnId) {
        count += 1;
      }
    }

    if (count == 0) {
      return RangeResponse.ALL;
    } else if (count == (maxTxnId - minTxnId + 1)) {
      return RangeResponse.NONE;
    } else {
      return RangeResponse.SOME;
    }
  }

  @Override
  public String toString() {
    return writeToString();
  }

  @Override
  public String writeToString() {
    StringBuilder buf = new StringBuilder();
    buf.append(highWatermark);
    buf.append(':');
    buf.append(minOpenTxn);
    if (exceptions.length == 0) {
      buf.append(':');  // separator for open txns
      buf.append(':');  // separator for aborted txns
    } else {
      StringBuilder open = new StringBuilder();
      StringBuilder abort = new StringBuilder();
      long abortedMin = -1;
      long abortedMax = -1;
      long openedMin = -1;
      long openedMax = -1;
      for (int i = 0; i < exceptions.length; i++) {
        if (abortedBits.get(i)) {
          if (abortedMax + 1 == exceptions[i]) {
            abortedMax++;
          } else {
            writeTxnRange(abort, abortedMin, abortedMax);
            abortedMin = abortedMax = exceptions[i];
          }
        } else {
          if (openedMax + 1 == exceptions[i]) {
            openedMax++;
          } else {
            writeTxnRange(open, openedMin, openedMax);
            openedMin = openedMax = exceptions[i];
          }
        }
      }
      writeTxnRange(abort, abortedMin, abortedMax);
      writeTxnRange(open, openedMin, openedMax);

      buf.append(':');
      buf.append(open);
      buf.append(':');
      buf.append(abort);
    }
    return buf.toString();
  }

  /**
   * txnlist is represented in ranges like this: 10-20,14,16-50
   * if the range is smaller then 5 it will not get merged, to avoid unnecessary overhead
   *
   */
  private void writeTxnRange(StringBuilder builder, long txnMin, long txnMax) {
    if (txnMax >= 0) {
      if (builder.length() > 0) {
        builder.append(',');
      }
      if (txnMin == txnMax) {
        builder.append(txnMin);
      } else if (txnMin + MIN_RANGE_LENGTH - 1 > txnMax) {
        // If the range is small the overhead is not worth it
        for (long txn = txnMin; txn <= txnMax; txn++) {
          builder.append(txn);
          if (txn != txnMax) {
            builder.append(',');
          }
        }
      } else {
        builder.append(txnMin).append('-').append(txnMax);
      }
    }
  }

  @Override
  public void readFromString(String src) {
    if (StringUtils.isEmpty(src)) {
      highWatermark = Long.MAX_VALUE;
      exceptions = new long[0];
      abortedBits = new BitSet();
      return;
    }

    String[] values = src.split(":");
    highWatermark = Long.parseLong(values[0]);
    minOpenTxn = Long.parseLong(values[1]);
    List<Long> openTxns = new ArrayList<>();
    List<Long> abortedTxns = new ArrayList<>();
    if (values.length == 3) {
      if (!values[2].isEmpty()) {
        openTxns = readTxnListFromRangeString(values[2]);
      }
    } else if (values.length > 3)  {
      if (!values[2].isEmpty()) {
        openTxns = readTxnListFromRangeString(values[2]);
      }
      if (!values[3].isEmpty()) {
        abortedTxns = readTxnListFromRangeString(values[3]);
      }
    }
    exceptions = new long[openTxns.size() + abortedTxns.size()];
    abortedBits = new BitSet(exceptions.length);

    int exceptionIndex = 0;
    int openIndex = 0;
    int abortIndex = 0;
    while (openIndex < openTxns.size() || abortIndex < abortedTxns.size()) {
      if (abortIndex == abortedTxns.size() ||
          (openIndex < openTxns.size() && openTxns.get(openIndex) < abortedTxns.get(abortIndex))) {
        exceptions[exceptionIndex++] = openTxns.get(openIndex++);
      } else {
        abortedBits.set(exceptionIndex);
        exceptions[exceptionIndex++] = abortedTxns.get(abortIndex++);
      }
    }
  }

  /**
   * txnlist is represented in ranges like this: 10-20,14,16-50
   * if the range is smaller then 5 it will not get merged, to avoid unnecessary overhead
   * @param txnListString string to parse from
   * @return txn list
   */
  private List<Long> readTxnListFromRangeString(String txnListString) {
    List<Long> txnList = new ArrayList<>();
    for (String txnRange : txnListString.split(",")) {
      if (txnRange.indexOf('-') < 0) {
        txnList.add(Long.parseLong(txnRange));
      } else {
        String[] parts = txnRange.split("-");
        long txn = Long.parseLong(parts[0]);
        long txnEnd = Long.parseLong(parts[1]);
        while (txn <= txnEnd) {
          txnList.add(txn++);
        }
      }
    }
    return txnList;
  }

  @Override
  public long getHighWatermark() {
    return highWatermark;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "Expose internal rep for efficiency")
  public long[] getInvalidTransactions() {
    return exceptions;
  }

  @Override
  public Long getMinOpenTxn() {
    return minOpenTxn == Long.MAX_VALUE ? null : minOpenTxn;
  }

  @Override
  public boolean isTxnAborted(long txnid) {
    int index = Arrays.binarySearch(exceptions, txnid);
    return index >= 0 && abortedBits.get(index);
  }

  @Override
  public RangeResponse isTxnRangeAborted(long minTxnId, long maxTxnId) {
    // check the easy cases first
    if (highWatermark < minTxnId) {
      return RangeResponse.NONE;
    }

    int count = 0;  // number of aborted txns found in exceptions

    // traverse the aborted txns list, starting at first aborted txn index
    for (int i = abortedBits.nextSetBit(0); i >= 0; i = abortedBits.nextSetBit(i + 1)) {
      long abortedTxnId = exceptions[i];
      if (abortedTxnId > maxTxnId) {  // we've already gone beyond the specified range
        break;
      }
      if (abortedTxnId >= minTxnId && abortedTxnId <= maxTxnId) {
        count++;
      }
    }

    if (count == 0) {
      return RangeResponse.NONE;
    } else if (count == (maxTxnId - minTxnId + 1)) {
      return RangeResponse.ALL;
    } else {
      return RangeResponse.SOME;
    }
  }
}

