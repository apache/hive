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
package org.apache.hadoop.hive.ql.udf;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFConv.
 *
 */
@Description(name = "conv",
    value = "_FUNC_(num, from_base, to_base) - convert num from from_base to"
    + " to_base",
    extended = "If to_base is negative, treat num as a signed integer,"
    + "otherwise, treat it as an unsigned integer.\n"
    + "Example:\n"
    + "  > SELECT _FUNC_('100', 2, 10) FROM src LIMIT 1;\n"
    + "  '4'\n"
    + "  > SELECT _FUNC_(-10, 16, -10) FROM src LIMIT 1;\n" + "  '16'")
public class UDFConv extends UDF {
  private final Text result = new Text();
  private final byte[] value = new byte[64];

  /**
   * Divide x by m as if x is an unsigned 64-bit integer. Examples:
   * unsignedLongDiv(-1, 2) == Long.MAX_VALUE unsignedLongDiv(6, 3) == 2
   * unsignedLongDiv(0, 5) == 0
   *
   * @param x
   *          is treated as unsigned
   * @param m
   *          is treated as signed
   */
  private long unsignedLongDiv(long x, int m) {
    if (x >= 0) {
      return x / m;
    }

    // Let uval be the value of the unsigned long with the same bits as x
    // Two's complement => x = uval - 2*MAX - 2
    // => uval = x + 2*MAX + 2
    // Now, use the fact: (a+b)/c = a/c + b/c + (a%c+b%c)/c
    return x / m + 2 * (Long.MAX_VALUE / m) + 2 / m
        + (x % m + 2 * (Long.MAX_VALUE % m) + 2 % m) / m;
  }

  /**
   * Decode val into value[].
   *
   * @param val
   *          is treated as an unsigned 64-bit integer
   * @param radix
   *          must be between MIN_RADIX and MAX_RADIX
   */
  private void decode(long val, int radix) {
    Arrays.fill(value, (byte) 0);
    for (int i = value.length - 1; val != 0; i--) {
      long q = unsignedLongDiv(val, radix);
      value[i] = (byte) (val - q * radix);
      val = q;
    }
  }

  /**
   * Convert value[] into a long. On overflow, return -1 (as mySQL does). If a
   * negative digit is found, ignore the suffix starting there.
   *
   * @param radix
   *          must be between MIN_RADIX and MAX_RADIX
   * @param fromPos
   *          is the first element that should be conisdered
   * @return the result should be treated as an unsigned 64-bit integer.
   */
  private long encode(int radix, int fromPos) {
    long val = 0;
    long bound = unsignedLongDiv(-1 - radix, radix); // Possible overflow once
    // val
    // exceeds this value
    for (int i = fromPos; i < value.length && value[i] >= 0; i++) {
      if (val >= bound) {
        // Check for overflow
        if (unsignedLongDiv(-1 - value[i], radix) < val) {
          return -1;
        }
      }
      val = val * radix + value[i];
    }
    return val;
  }

  /**
   * Convert the bytes in value[] to the corresponding chars.
   *
   * @param radix
   *          must be between MIN_RADIX and MAX_RADIX
   * @param fromPos
   *          is the first nonzero element
   */
  private void byte2char(int radix, int fromPos) {
    for (int i = fromPos; i < value.length; i++) {
      value[i] = (byte) Character.toUpperCase(Character.forDigit(value[i],
          radix));
    }
  }

  /**
   * Convert the chars in value[] to the corresponding integers. Convert invalid
   * characters to -1.
   *
   * @param radix
   *          must be between MIN_RADIX and MAX_RADIX
   * @param fromPos
   *          is the first nonzero element
   */
  private void char2byte(int radix, int fromPos) {
    for (int i = fromPos; i < value.length; i++) {
      value[i] = (byte) Character.digit(value[i], radix);
    }
  }

  /**
   * Convert numbers between different number bases. If toBase>0 the result is
   * unsigned, otherwise it is signed.
   *
   */
  public Text evaluate(Text n, IntWritable fromBase, IntWritable toBase) {
    if (n == null || fromBase == null || toBase == null) {
      return null;
    }

    int fromBs = fromBase.get();
    int toBs = toBase.get();
    if (fromBs < Character.MIN_RADIX || fromBs > Character.MAX_RADIX
        || Math.abs(toBs) < Character.MIN_RADIX
        || Math.abs(toBs) > Character.MAX_RADIX) {
      return null;
    }

    byte[] num = n.getBytes();
    boolean negative = (num[0] == '-');
    int first = 0;
    if (negative) {
      first = 1;
    }

    // Copy the digits in the right side of the array
    for (int i = 1; i <= n.getLength() - first; i++) {
      value[value.length - i] = num[n.getLength() - i];
    }
    char2byte(fromBs, value.length - n.getLength() + first);

    // Do the conversion by going through a 64 bit integer
    long val = encode(fromBs, value.length - n.getLength() + first);
    if (negative && toBs > 0) {
      if (val < 0) {
        val = -1;
      } else {
        val = -val;
      }
    }
    if (toBs < 0 && val < 0) {
      val = -val;
      negative = true;
    }
    decode(val, Math.abs(toBs));

    // Find the first non-zero digit or the last digits if all are zero.
    for (first = 0; first < value.length - 1 && value[first] == 0; first++) {
      ;
    }

    byte2char(Math.abs(toBs), first);

    if (negative && toBs < 0) {
      value[--first] = '-';
    }

    result.set(value, first, value.length - first);
    return result;
  }
}
