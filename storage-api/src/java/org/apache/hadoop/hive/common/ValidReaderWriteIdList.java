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

import org.apache.hive.common.util.SuppressFBWarnings;

import java.util.Arrays;
import java.util.BitSet;

/**
 * An implementation of {@link ValidWriteIdList} for use by readers.
 * This class will view a write id as valid only if it maps to committed transaction.
 * Write ids of both open and aborted transactions will be seen as invalid.
 */
public class ValidReaderWriteIdList implements ValidWriteIdList {

  private String tableName; // Full table name of format <db_name>.<table_name>
  protected long[] exceptions;
  protected BitSet abortedBits; // BitSet for flagging aborted write ids. Bit is true if aborted, false if open
  //default value means there are no open write ids in the snapshot
  private long minOpenWriteId = Long.MAX_VALUE;
  protected long highWatermark;

  /**
   * This seems like a bad c'tor.  It doesn't even have a table name in it and it's used every time
   * ValidWriteIdList.VALID_WRITEIDS_KEY is not found in Configuration.
   * But, if anything, that would indicate a bug if was done for an acid read since it
   * considers everything valid - this should not be assumed.
   */
  public ValidReaderWriteIdList() {
    this(null, new long[0], new BitSet(), Long.MAX_VALUE, Long.MAX_VALUE);
  }

  /**
   * Used if there are no open write ids in the snapshot.
   */
  public ValidReaderWriteIdList(String tableName, long[] exceptions, BitSet abortedBits, long highWatermark) {
    this(tableName, exceptions, abortedBits, highWatermark, Long.MAX_VALUE);
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "Ref external obj for efficiency")
  public ValidReaderWriteIdList(String tableName,
                                long[] exceptions, BitSet abortedBits, long highWatermark, long minOpenWriteId) {
    this.tableName = tableName;
    if (exceptions.length > 0) {
      this.minOpenWriteId = minOpenWriteId;
    }
    this.exceptions = exceptions;
    this.abortedBits = abortedBits;
    this.highWatermark = highWatermark;
  }

  public ValidReaderWriteIdList(String value) {
    readFromString(value);
  }

  @Override
  public boolean isWriteIdValid(long writeId) {
    if (writeId > highWatermark) {
      return false;
    }
    return Arrays.binarySearch(exceptions, writeId) < 0;
  }

  /**
   * We cannot use a base file if its range contains an open write id.
   * @param writeId from base_xxxx
   */
  @Override
  public boolean isValidBase(long writeId) {
    return (writeId < minOpenWriteId) && (writeId <= highWatermark);
  }
  @Override
  public RangeResponse isWriteIdRangeValid(long minWriteId, long maxWriteId) {
    // check the easy cases first
    if (minWriteId > highWatermark) {
      return RangeResponse.NONE;
    } else if (exceptions.length > 0 && exceptions[0] > maxWriteId) {
      return RangeResponse.ALL;
    }

    // since the exceptions and the range in question overlap, count the
    // exceptions in the range
    long count = Math.max(0, maxWriteId - highWatermark);
    for(long txn: exceptions) {
      if (minWriteId <= txn && txn <= maxWriteId) {
        count += 1;
      }
    }

    if (count == 0) {
      return RangeResponse.ALL;
    } else if (count == (maxWriteId - minWriteId + 1)) {
      return RangeResponse.NONE;
    } else {
      return RangeResponse.SOME;
    }
  }

  @Override
  public String toString() {
    return writeToString();
  }

  // Format is <table_name>:<hwm>:<minOpenWriteId>:<open_writeids>:<abort_writeids>
  @Override
  public String writeToString() {
    StringBuilder buf = new StringBuilder();
    if (tableName == null) {
      buf.append("null");
    } else {
      buf.append(tableName);
    }
    buf.append(':');
    buf.append(highWatermark);
    buf.append(':');
    buf.append(minOpenWriteId);
    if (exceptions.length == 0) {
      buf.append(':');  // separator for open write ids
      buf.append(':');  // separator for aborted write ids
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
      tableName = values[0];
      if (tableName.equalsIgnoreCase("null")) {
        tableName = null;
      }
      highWatermark = Long.parseLong(values[1]);
      minOpenWriteId = Long.parseLong(values[2]);
      String[] openWriteIds = new String[0];
      String[] abortedWriteIds = new String[0];
      if (values.length < 4) {
        openWriteIds = new String[0];
        abortedWriteIds = new String[0];
      } else if (values.length == 4) {
        if (!values[3].isEmpty()) {
          openWriteIds = values[3].split(",");
        }
      } else {
        if (!values[3].isEmpty()) {
          openWriteIds = values[3].split(",");
        }
        if (!values[4].isEmpty()) {
          abortedWriteIds = values[4].split(",");
        }
      }
      exceptions = new long[openWriteIds.length + abortedWriteIds.length];
      int i = 0;
      for (String open : openWriteIds) {
        exceptions[i++] = Long.parseLong(open);
      }
      for (String abort : abortedWriteIds) {
        exceptions[i++] = Long.parseLong(abort);
      }
      Arrays.sort(exceptions);
      abortedBits = new BitSet(exceptions.length);
      for (String abort : abortedWriteIds) {
        int index = Arrays.binarySearch(exceptions, Long.parseLong(abort));
        abortedBits.set(index);
      }
    }
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public long getHighWatermark() {
    return highWatermark;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "Expose internal rep for efficiency")
  public long[] getInvalidWriteIds() {
    return exceptions;
  }

  @Override
  public Long getMinOpenWriteId() {
    return minOpenWriteId == Long.MAX_VALUE ? null : minOpenWriteId;
  }

  @Override
  public boolean isWriteIdAborted(long writeId) {
    int index = Arrays.binarySearch(exceptions, writeId);
    return index >= 0 && abortedBits.get(index);
  }

  @Override
  public RangeResponse isWriteIdRangeAborted(long minWriteId, long maxWriteId) {
    // check the easy cases first
    if (highWatermark < minWriteId) {
      return RangeResponse.NONE;
    }

    int count = 0;  // number of aborted txns found in exceptions

    // traverse the aborted txns list, starting at first aborted txn index
    for (int i = abortedBits.nextSetBit(0); i >= 0; i = abortedBits.nextSetBit(i + 1)) {
      long abortedTxnId = exceptions[i];
      if (abortedTxnId > maxWriteId) {  // we've already gone beyond the specified range
        break;
      }
      if (abortedTxnId >= minWriteId && abortedTxnId <= maxWriteId) {
        count++;
      }
    }

    if (count == 0) {
      return RangeResponse.NONE;
    } else if (count == (maxWriteId - minWriteId + 1)) {
      return RangeResponse.ALL;
    } else {
      return RangeResponse.SOME;
    }
  }

  public void setHighWatermark(long value) {
    this.highWatermark = value;
  }
}

