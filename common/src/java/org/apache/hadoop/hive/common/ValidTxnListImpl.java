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

package org.apache.hadoop.hive.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class ValidTxnListImpl implements ValidTxnList {

  static final private Log LOG = LogFactory.getLog(ValidTxnListImpl.class.getName());
  final static private int MAX_UNCOMPRESSED_LENGTH = 256;
  final static private char COMPRESSION_MARKER = 'C';
  final static private String STRING_ENCODING = "ISO-8859-1";

  private long[] exceptions;
  private long highWatermark;

  public ValidTxnListImpl() {
    this(new long[0], Long.MAX_VALUE);
  }

  public ValidTxnListImpl(long[] exceptions, long highWatermark) {
    if (exceptions.length == 0) {
      this.exceptions = exceptions;
    } else {
      this.exceptions = exceptions.clone();
      Arrays.sort(this.exceptions);
    }
    this.highWatermark = highWatermark;
  }

  public ValidTxnListImpl(String value) {
    readFromString(value);
  }

  @Override
  public boolean isTxnCommitted(long txnid) {
    if (highWatermark < txnid) {
      return false;
    }
    return Arrays.binarySearch(exceptions, txnid) < 0;
  }

  @Override
  public RangeResponse isTxnRangeCommitted(long minTxnId, long maxTxnId) {
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
    if (exceptions.length == 0) {
      buf.append(':');
    } else {
      for(long except: exceptions) {
        buf.append(':');
        buf.append(except);
      }
    }
    if (buf.length() > MAX_UNCOMPRESSED_LENGTH) {
      try {
        ByteArrayOutputStream byteBuf = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(byteBuf);
        gzip.write(buf.toString().getBytes());
        gzip.close();
        StringBuilder buf2 = new StringBuilder();
        buf2.append(COMPRESSION_MARKER);
        buf2.append(buf.length());
        buf2.append(':');
        buf2.append(byteBuf.toString(STRING_ENCODING));
        return buf2.toString();
      } catch (IOException e) {
        LOG.error("Unable to compress transaction list, " + e.getMessage());
        throw new RuntimeException(e);
      }
    } else {
      return buf.toString();
    }
  }

  @Override
  public void readFromString(String src) {
    if (src == null) {
      highWatermark = Long.MAX_VALUE;
      exceptions = new long[0];
    } else {
      String[] values;
      if (src.charAt(0) == COMPRESSION_MARKER) {
        try {
          int colon = src.indexOf(':');
          int len = Integer.valueOf(src.substring(1, colon));
          ByteArrayInputStream byteBuf =
              new ByteArrayInputStream(src.substring(colon + 1).getBytes(STRING_ENCODING));
          GZIPInputStream gzip = new GZIPInputStream(byteBuf);
          byte[] buf = new byte[len];
          int bytesRead = 0;
          int offset = 0;
          int maxReadLen = len;
          do {
            bytesRead = gzip.read(buf, offset, maxReadLen);
            offset += bytesRead;
            maxReadLen -= bytesRead;
          } while (maxReadLen > 0);
          values = new String(buf).split(":");
        } catch (IOException e) {
          LOG.error("Unable to decode compressed transaction list, " + e.getMessage());
          throw new RuntimeException(e);
        }

      } else {
        values = src.split(":");
      }
      highWatermark = Long.parseLong(values[0]);
      exceptions = new long[values.length - 1];
      for (int i = 1; i < values.length; ++i) {
        exceptions[i - 1] = Long.parseLong(values[i]);
      }
    }
  }

  @Override
  public long getHighWatermark() {
    return highWatermark;
  }

  @Override
  public long[] getOpenTransactions() {
    return exceptions;
  }
}

