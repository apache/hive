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

package org.apache.hadoop.hive.ql.io.orc;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;

final class SerializationUtils {

  // unused
  private SerializationUtils() {}

  static void writeVulong(OutputStream output, long value) throws IOException {
    while (true) {
      if ((value & ~0x7f) == 0) {
        output.write((byte) value);
        return;
      } else {
        output.write((byte) (0x80 | (value & 0x7f)));
        value >>>= 7;
      }
    }
  }

  static void writeVslong(OutputStream output, long value) throws IOException {
    writeVulong(output, (value << 1) ^ (value >> 63));
  }


  static long readVulong(InputStream in) throws IOException {
    long result = 0;
    long b;
    int offset = 0;
    do {
      b = in.read();
      if (b == -1) {
        throw new EOFException("Reading Vulong past EOF");
      }
      result |= (0x7f & b) << offset;
      offset += 7;
    } while (b >= 0x80);
    return result;
  }

  static long readVslong(InputStream in) throws IOException {
    long result = readVulong(in);
    return (result >>> 1) ^ -(result & 1);
  }

  static float readFloat(InputStream in) throws IOException {
    int ser = in.read() | (in.read() << 8) | (in.read() << 16) |
      (in.read() << 24);
    return Float.intBitsToFloat(ser);
  }

  static void writeFloat(OutputStream output, float value) throws IOException {
    int ser = Float.floatToIntBits(value);
    output.write(ser & 0xff);
    output.write((ser >> 8) & 0xff);
    output.write((ser >> 16) & 0xff);
    output.write((ser >> 24) & 0xff);
  }

  static double readDouble(InputStream in) throws IOException {
  long ser = (long) in.read() |
             ((long) in.read() << 8) |
             ((long) in.read() << 16) |
             ((long) in.read() << 24) |
             ((long) in.read() << 32) |
             ((long) in.read() << 40) |
             ((long) in.read() << 48) |
             ((long) in.read() << 56);
    return Double.longBitsToDouble(ser);
  }

  static void writeDouble(OutputStream output,
                          double value) throws IOException {
    long ser = Double.doubleToLongBits(value);
    output.write(((int) ser) & 0xff);
    output.write(((int) (ser >> 8)) & 0xff);
    output.write(((int) (ser >> 16)) & 0xff);
    output.write(((int) (ser >> 24)) & 0xff);
    output.write(((int) (ser >> 32)) & 0xff);
    output.write(((int) (ser >> 40)) & 0xff);
    output.write(((int) (ser >> 48)) & 0xff);
    output.write(((int) (ser >> 56)) & 0xff);
  }

  /**
   * Write the arbitrarily sized signed BigInteger in vint format.
   *
   * Signed integers are encoded using the low bit as the sign bit using zigzag
   * encoding.
   *
   * Each byte uses the low 7 bits for data and the high bit for stop/continue.
   *
   * Bytes are stored LSB first.
   * @param output the stream to write to
   * @param value the value to output
   * @throws IOException
   */
  static void writeBigInteger(OutputStream output,
                              BigInteger value) throws IOException {
    // encode the signed number as a positive integer
    value = value.shiftLeft(1);
    int sign = value.signum();
    if (sign < 0) {
      value = value.negate();
      value = value.subtract(BigInteger.ONE);
    }
    int length = value.bitLength();
    while (true) {
      long lowBits = value.longValue() & 0x7fffffffffffffffL;
      length -= 63;
      // write out the next 63 bits worth of data
      for(int i=0; i < 9; ++i) {
        // if this is the last byte, leave the high bit off
        if (length <= 0 && (lowBits & ~0x7f) == 0) {
          output.write((byte) lowBits);
          return;
        } else {
          output.write((byte) (0x80 | (lowBits & 0x7f)));
          lowBits >>>= 7;
        }
      }
      value = value.shiftRight(63);
    }
  }

  /**
   * Read the signed arbitrary sized BigInteger BigInteger in vint format
   * @param input the stream to read from
   * @return the read BigInteger
   * @throws IOException
   */
  static BigInteger readBigInteger(InputStream input) throws IOException {
    BigInteger result = BigInteger.ZERO;
    long work = 0;
    int offset = 0;
    long b;
    do {
      b = input.read();
      if (b == -1) {
        throw new EOFException("Reading BigInteger past EOF from " + input);
      }
      work |= (0x7f & b) << (offset % 63);
      offset += 7;
      // if we've read 63 bits, roll them into the result
      if (offset == 63) {
        result = BigInteger.valueOf(work);
        work = 0;
      } else if (offset % 63 == 0) {
        result = result.or(BigInteger.valueOf(work).shiftLeft(offset-63));
        work = 0;
      }
    } while (b >= 0x80);
    if (work != 0) {
      result = result.or(BigInteger.valueOf(work).shiftLeft((offset/63)*63));
    }
    // convert back to a signed number
    boolean isNegative = result.testBit(0);
    if (isNegative) {
      result = result.add(BigInteger.ONE);
      result = result.negate();
    }
    result = result.shiftRight(1);
    return result;
  }
}
