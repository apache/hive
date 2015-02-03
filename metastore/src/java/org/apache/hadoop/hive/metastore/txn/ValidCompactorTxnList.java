/**
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

package org.apache.hadoop.hive.metastore.txn;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.common.ValidReadTxnList;

import java.util.Arrays;

/**
 * And implmentation of {@link org.apache.hadoop.hive.common.ValidTxnList} for use by the compactor.
 * For the purposes of {@link #isTxnRangeValid} this class will view a transaction as valid if it
 * is committed or aborted.  Additionally it will return none if there are any open transactions
 * below the max transaction given, since we don't want to compact above open transactions.  For
 * {@link #isTxnValid} it will still view a transaction as valid only if it is committed.  These
 * produce the logic we need to assure that the compactor only sees records less than the lowest
 * open transaction when choosing which files to compact, but that it still ignores aborted
 * records when compacting.
 */
public class ValidCompactorTxnList extends ValidReadTxnList {

  // The minimum open transaction id
  private long minOpenTxn;

  public ValidCompactorTxnList() {
    super();
    minOpenTxn = -1;
  }

  /**
   *
   * @param exceptions list of all open and aborted transactions
   * @param minOpen lowest open transaction
   * @param highWatermark highest committed transaction
   */
  public ValidCompactorTxnList(long[] exceptions, long minOpen, long highWatermark) {
    super(exceptions, highWatermark);
    minOpenTxn = minOpen;
  }

  public ValidCompactorTxnList(String value) {
    super(value);
  }

  @Override
  public RangeResponse isTxnRangeValid(long minTxnId, long maxTxnId) {
    if (highWatermark < minTxnId) {
      return RangeResponse.NONE;
    } else if (minOpenTxn < 0) {
      return highWatermark >= maxTxnId ? RangeResponse.ALL : RangeResponse.NONE;
    } else {
      return minOpenTxn > maxTxnId ? RangeResponse.ALL : RangeResponse.NONE;
    }
  }

  @Override
  public String writeToString() {
    StringBuilder buf = new StringBuilder();
    buf.append(highWatermark);
    buf.append(':');
    buf.append(minOpenTxn);
    if (exceptions.length == 0) {
      buf.append(':');
    } else {
      for(long except: exceptions) {
        buf.append(':');
        buf.append(except);
      }
    }
    return buf.toString();
  }

  @Override
  public void readFromString(String src) {
    if (src == null) {
      highWatermark = Long.MAX_VALUE;
      exceptions = new long[0];
    } else {
      String[] values = src.split(":");
      highWatermark = Long.parseLong(values[0]);
      minOpenTxn = Long.parseLong(values[1]);
      exceptions = new long[values.length - 2];
      for(int i = 2; i < values.length; ++i) {
        exceptions[i-2] = Long.parseLong(values[i]);
      }
    }
  }

  @VisibleForTesting
  long getMinOpenTxn() {
    return minOpenTxn;
  }
}
