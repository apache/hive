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
 * An implementation of {@link org.apache.hadoop.hive.common.ValidTxnList} for use by readers.
 * This class will view a transaction as valid only if it is committed.  Both open and aborted
 * transactions will be seen as invalid.
 */
public class ValidReadTxnList implements ValidTxnList {

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
  public ValidReadTxnList(long[] exceptions, BitSet abortedBits, long highWatermark) {
    this(exceptions, abortedBits, highWatermark, Long.MAX_VALUE);
  }
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
  public boolean isTxnValid(long txnid) {
    if (highWatermark < txnid) {
      return false;
    }
    return Arrays.binarySearch(exceptions, txnid) < 0;
  }

  /**
   * We cannot use a base file if its range contains an open txn.
   * @param txnid from base_xxxx
   */
  @Override
  public boolean isValidBase(long txnid) {
    return minOpenTxn > txnid && txnid <= highWatermark;
  }
  @Override
  public RangeResponse isTxnRangeValid(long minTxnId, long maxTxnId) {
    // check the easy cases first
    if (highWatermark < minTxnId) {
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
      for (int i = 0; i < exceptions.length; i++) {
        if (abortedBits.get(i)) {
          if (abort.length() > 0) {
            abort.append(',');
          }
          abort.append(exceptions[i]);
        } else {
          if (open.length() > 0) {
            open.append(',');
          }
          open.append(exceptions[i]);
        }
      }
      buf.append(':');
      buf.append(open);
      buf.append(':');
      buf.append(abort);
    }
    return buf.toString();
  }

  @Override
  public void readFromString(String src) {
    if (src == null || src.length() == 0) {
      highWatermark = Long.MAX_VALUE;
      exceptions = new long[0];
      abortedBits = new BitSet();
    } else {
      String[] values = src.split(":");
      highWatermark = Long.parseLong(values[0]);
      minOpenTxn = Long.parseLong(values[1]);
      String[] openTxns = new String[0];
      String[] abortedTxns = new String[0];
      if (values.length < 3) {
        openTxns = new String[0];
        abortedTxns = new String[0];
      } else if (values.length == 3) {
        if (!values[2].isEmpty()) {
          openTxns = values[2].split(",");
        }
      } else {
        if (!values[2].isEmpty()) {
          openTxns = values[2].split(",");
        }
        if (!values[3].isEmpty()) {
          abortedTxns = values[3].split(",");
        }
      }
      exceptions = new long[openTxns.length + abortedTxns.length];
      int i = 0;
      for (String open : openTxns) {
        exceptions[i++] = Long.parseLong(open);
      }
      for (String abort : abortedTxns) {
        exceptions[i++] = Long.parseLong(abort);
      }
      Arrays.sort(exceptions);
      abortedBits = new BitSet(exceptions.length);
      for (String abort : abortedTxns) {
        int index = Arrays.binarySearch(exceptions, Long.parseLong(abort));
        abortedBits.set(index);
      }
    }
  }

  @Override
  public long getHighWatermark() {
    return highWatermark;
  }

  @Override
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

