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
package org.apache.hadoop.hive.serde2.lazy;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.LongWritable;

/**
 * LazyObject for storing a value of Long.
 * 
 * <p>
 * Part of the code is adapted from Apache Harmony Project.
 * 
 * As with the specification, this implementation relied on code laid out in <a
 * href="http://www.hackersdelight.org/">Henry S. Warren, Jr.'s Hacker's
 * Delight, (Addison Wesley, 2002)</a> as well as <a
 * href="http://aggregate.org/MAGIC/">The Aggregate's Magic Algorithms</a>.
 * </p>
 * 
 */
public class LazyLong extends LazyPrimitive<LongWritable> {


  public LazyLong() {
    data = new LongWritable();
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    try {
      data.set(parseLong(bytes.getData(), start, length, 10));
      isNull = false;
    } catch (NumberFormatException e) {
      isNull = true;
    }
  }

  /**
   * Parses the string argument as if it was a long value and returns the
   * result. Throws NumberFormatException if the string does not represent a
   * long quantity.
   * 
   * @param bytes
   * @param start
   * @param length
   *            a UTF-8 encoded string representation of a long quantity.
   * @return long the value represented by the argument
   * @exception NumberFormatException
   *                if the argument could not be parsed as a long quantity.
   */
  public static long parseLong(byte[] bytes, int start, int length) throws NumberFormatException {
    return parseLong(bytes, start, length, 10);
  }

  /**
   * Parses the string argument as if it was an long value and returns the
   * result. Throws NumberFormatException if the string does not represent an
   * long quantity. The second argument specifies the radix to use when
   * parsing the value.
   * 
   * @param bytes
   * @param start
   * @param length
   *            a UTF-8 encoded string representation of a long quantity.
   * @param radix
   *            the base to use for conversion.
   * @return the value represented by the argument
   * @exception NumberFormatException
   *                if the argument could not be parsed as an long quantity.
   */
  public static long parseLong(byte[] bytes, int start, int length, int radix)
      throws NumberFormatException {
    if (bytes == null) {
      throw new NumberFormatException("String is null");
    }
    if (radix < Character.MIN_RADIX ||
        radix > Character.MAX_RADIX) {
      throw new NumberFormatException("Invalid radix: " + radix);
    }
    if (length == 0) {
      throw new NumberFormatException("Empty string!");
    }
    int offset = start;
    boolean negative = bytes[start] == '-';
    if (negative || bytes[start] == '+') {
      offset ++;
      if (length == 1) {
        throw new NumberFormatException(LazyUtils.convertToString(bytes, start, length));
      }
    }

    return parse(bytes, start, length, offset, radix, negative);
  }

  /**
  /**
   * Parses the string argument as if it was an long value and returns the
   * result. Throws NumberFormatException if the string does not represent an
   * long quantity. The second argument specifies the radix to use when
   * parsing the value.
   * 
   * @param bytes
   * @param start
   * @param length
   *            a UTF-8 encoded string representation of a long quantity.
   * @param offset
   *            the starting position after the sign (if exists)
   * @param radix
   *            the base to use for conversion.
   * @param negative
   *            whether the number is negative.
   * @return the value represented by the argument
   * @exception NumberFormatException
   *                if the argument could not be parsed as an long quantity.
   */
  private static long parse(byte[] bytes, int start, int length, int offset, int radix,
      boolean negative) {
    long max = Long.MIN_VALUE / radix;
    long result = 0, end = start + length;
    while (offset < end) {
      int digit = LazyUtils.digit(bytes[offset++], radix);
      if (digit == -1 || max > result) {
        throw new NumberFormatException(LazyUtils.convertToString(bytes, start, length));
      }
      long next = result * radix - digit;
      if (next > result) {
        throw new NumberFormatException(LazyUtils.convertToString(bytes, start, length));
      }
      result = next;
    }
    if (!negative) {
      result = -result;
      if (result < 0) {
        throw new NumberFormatException(LazyUtils.convertToString(bytes, start, length));
      }
    }
    return result;
  }

  /**
   * Writes out the text representation of an integer using base 10 to an 
   * OutputStream in UTF-8 encoding.
   *
   * Note: division by a constant (like 10) is much faster than division by
   * a variable.  That's one of the reasons that we don't make radix a 
   * parameter here.
   *  
   * @param out the outputstream to write to
   * @param i   an int to write out
   * @throws IOException 
   */
  public static void writeUTF8(OutputStream out, long i) throws IOException {
    if (i == 0) {
      out.write('0');
      return;
    }

    boolean negative = i < 0;
    if (negative) {
      out.write('-');
    } else {
      // negative range is bigger than positive range, so there is no risk
      // of overflow here.
      i = -i;
    }
    
    long start = 1000000000000000000L;
    while (i/start == 0) {
      start /= 10;
    }
    
    while (start > 0) {
      out.write('0' - (int)((i / start) % 10));
      start /= 10;
    }
  }

  public static void writeUTF8NoException(OutputStream out, long i) {
    try {
      writeUTF8(out, i);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
