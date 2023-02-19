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

import java.math.BigInteger;
import java.util.Locale;

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

  /**
   * Convert numbers between different number bases. If toBase&gt;0 the result is
   * unsigned, otherwise it is signed.
   *
   */
  public Text evaluate(Text n, IntWritable fromBase, IntWritable toBase) {
    if (n == null || fromBase == null || toBase == null) {
      return null;
    }

    int fromBs = fromBase.get();
    int toBs = toBase.get();
    int toBaseAbsoluteValue = Math.abs(toBs);
    if (fromBs < Character.MIN_RADIX || fromBs > Character.MAX_RADIX
        || toBaseAbsoluteValue < Character.MIN_RADIX
        || toBaseAbsoluteValue > Character.MAX_RADIX) {
      return null;
    }

    String num = n.toString();
    if (num.isEmpty()) {
      return null;
    }

    String validNum = getLongestValidPrefix(num, fromBs);
    if (validNum == null || validNum.isEmpty()) {
      return null;
    }

    BigInteger bigInteger = new BigInteger(validNum, fromBs);
    BigInteger bigIntegerResolved = toBs > 0 ? getUnsignedBigInt(bigInteger) : bigInteger;
    String convertedValue = bigIntegerResolved
            .toString(toBaseAbsoluteValue)
            .toUpperCase(Locale.ROOT);
    result.set(convertedValue);
    return result;
  }

  private boolean isSign(char c) {
    return c == '-' || c == '+';
  }

  private String getLongestValidPrefix(String num, int radix) {
    boolean isSigned = isSign(num.charAt(0));
    StringBuilder builder = new StringBuilder();
    int startIndex = 0;
    if (isSigned) {
      builder.append(num.charAt(0));
      startIndex = 1;
    }

    char[] charNumbers = num.toCharArray();
    for (int i = startIndex; i < charNumbers.length; i++) {
      char charNumber = charNumbers[i];
      if (Character.digit(charNumber, radix) >= 0) {
        builder.append(charNumber);
      } else {
        break;
      }
    }

    if (isSigned && builder.length() == 1) {
      return null;
    }

    return builder.toString();
  }

  private BigInteger getUnsignedBigInt(BigInteger bigInteger) {
    boolean isValueSigned = bigInteger.signum() < 0;
    if (isValueSigned) {
      BigInteger shiftedOne = BigInteger.ONE.shiftLeft(Math.max(bigInteger.bitLength(), 64));
      return bigInteger.add(shiftedOne);
    } else {
      return bigInteger;
    }
  }
}
