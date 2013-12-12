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

  enum FixedBitSizes {
    ONE, TWO, THREE, FOUR, FIVE, SIX, SEVEN, EIGHT, NINE, TEN, ELEVEN, TWELVE,
    THIRTEEN, FOURTEEN, FIFTEEN, SIXTEEN, SEVENTEEN, EIGHTEEN, NINETEEN,
    TWENTY, TWENTYONE, TWENTYTWO, TWENTYTHREE, TWENTYFOUR, TWENTYSIX,
    TWENTYEIGHT, THIRTY, THIRTYTWO, FORTY, FORTYEIGHT, FIFTYSIX, SIXTYFOUR;
  }

  /**
   * Count the number of bits required to encode the given value
   * @param value
   * @return bits required to store value
   */
  static int findClosestNumBits(long value) {
    int count = 0;
    while (value != 0) {
      count++;
      value = value >>> 1;
    }
    return getClosestFixedBits(count);
  }

  /**
   * zigzag encode the given value
   * @param val
   * @return zigzag encoded value
   */
  static long zigzagEncode(long val) {
    return (val << 1) ^ (val >> 63);
  }

  /**
   * zigzag decode the given value
   * @param val
   * @return zizag decoded value
   */
  static long zigzagDecode(long val) {
    return (val >>> 1) ^ -(val & 1);
  }

  /**
   * Compute the bits required to represent pth percentile value
   * @param data - array
   * @param p - percentile value (>=0.0 to <=1.0)
   * @return pth percentile bits
   */
  static int percentileBits(long[] data, double p) {
    if ((p > 1.0) || (p <= 0.0)) {
      return -1;
    }

    // histogram that store the encoded bit requirement for each values.
    // maximum number of bits that can encoded is 32 (refer FixedBitSizes)
    int[] hist = new int[32];

    // compute the histogram
    for(long l : data) {
      int idx = encodeBitWidth(findClosestNumBits(l));
      hist[idx] += 1;
    }

    int len = data.length;
    int perLen = (int) (len * (1.0 - p));

    // return the bits required by pth percentile length
    for(int i = hist.length - 1; i >= 0; i--) {
      perLen -= hist[i];
      if (perLen < 0) {
        return decodeBitWidth(i);
      }
    }

    return 0;
  }

  /**
   * Read n bytes in big endian order and convert to long
   * @param b - byte array
   * @return long value
   */
  static long bytesToLongBE(InStream input, int n) throws IOException {
    long out = 0;
    long val = 0;
    while (n > 0) {
      n--;
      // store it in a long and then shift else integer overflow will occur
      val = input.read();
      out |= (val << (n * 8));
    }
    return out;
  }

  /**
   * Calculate the number of bytes required
   * @param n - number of values
   * @param numBits - bit width
   * @return number of bytes required
   */
  static int getTotalBytesRequired(int n, int numBits) {
    return (n * numBits + 7) / 8;
  }

  /**
   * For a given fixed bit this function will return the closest available fixed
   * bit
   * @param n
   * @return closest valid fixed bit
   */
  static int getClosestFixedBits(int n) {
    if (n == 0) {
      return 1;
    }

    if (n >= 1 && n <= 24) {
      return n;
    } else if (n > 24 && n <= 26) {
      return 26;
    } else if (n > 26 && n <= 28) {
      return 28;
    } else if (n > 28 && n <= 30) {
      return 30;
    } else if (n > 30 && n <= 32) {
      return 32;
    } else if (n > 32 && n <= 40) {
      return 40;
    } else if (n > 40 && n <= 48) {
      return 48;
    } else if (n > 48 && n <= 56) {
      return 56;
    } else {
      return 64;
    }
  }

  /**
   * Finds the closest available fixed bit width match and returns its encoded
   * value (ordinal)
   * @param n - fixed bit width to encode
   * @return encoded fixed bit width
   */
  static int encodeBitWidth(int n) {
    n = getClosestFixedBits(n);

    if (n >= 1 && n <= 24) {
      return n - 1;
    } else if (n > 24 && n <= 26) {
      return FixedBitSizes.TWENTYSIX.ordinal();
    } else if (n > 26 && n <= 28) {
      return FixedBitSizes.TWENTYEIGHT.ordinal();
    } else if (n > 28 && n <= 30) {
      return FixedBitSizes.THIRTY.ordinal();
    } else if (n > 30 && n <= 32) {
      return FixedBitSizes.THIRTYTWO.ordinal();
    } else if (n > 32 && n <= 40) {
      return FixedBitSizes.FORTY.ordinal();
    } else if (n > 40 && n <= 48) {
      return FixedBitSizes.FORTYEIGHT.ordinal();
    } else if (n > 48 && n <= 56) {
      return FixedBitSizes.FIFTYSIX.ordinal();
    } else {
      return FixedBitSizes.SIXTYFOUR.ordinal();
    }
  }

  /**
   * Decodes the ordinal fixed bit value to actual fixed bit width value
   * @param n - encoded fixed bit width
   * @return decoded fixed bit width
   */
  static int decodeBitWidth(int n) {
    if (n >= FixedBitSizes.ONE.ordinal()
        && n <= FixedBitSizes.TWENTYFOUR.ordinal()) {
      return n + 1;
    } else if (n == FixedBitSizes.TWENTYSIX.ordinal()) {
      return 26;
    } else if (n == FixedBitSizes.TWENTYEIGHT.ordinal()) {
      return 28;
    } else if (n == FixedBitSizes.THIRTY.ordinal()) {
      return 30;
    } else if (n == FixedBitSizes.THIRTYTWO.ordinal()) {
      return 32;
    } else if (n == FixedBitSizes.FORTY.ordinal()) {
      return 40;
    } else if (n == FixedBitSizes.FORTYEIGHT.ordinal()) {
      return 48;
    } else if (n == FixedBitSizes.FIFTYSIX.ordinal()) {
      return 56;
    } else {
      return 64;
    }
  }

  /**
   * Bitpack and write the input values to underlying output stream
   * @param input - values to write
   * @param offset - offset
   * @param len - length
   * @param bitSize - bit width
   * @param output - output stream
   * @throws IOException
   */
  static void writeInts(long[] input, int offset, int len, int bitSize,
                        OutputStream output) throws IOException {
    if (input == null || input.length < 1 || offset < 0 || len < 1
        || bitSize < 1) {
      return;
    }

    int bitsLeft = 8;
    byte current = 0;
    for(int i = offset; i < (offset + len); i++) {
      long value = input[i];
      int bitsToWrite = bitSize;
      while (bitsToWrite > bitsLeft) {
        // add the bits to the bottom of the current word
        current |= value >>> (bitsToWrite - bitsLeft);
        // subtract out the bits we just added
        bitsToWrite -= bitsLeft;
        // zero out the bits above bitsToWrite
        value &= (1L << bitsToWrite) - 1;
        output.write(current);
        current = 0;
        bitsLeft = 8;
      }
      bitsLeft -= bitsToWrite;
      current |= value << bitsLeft;
      if (bitsLeft == 0) {
        output.write(current);
        current = 0;
        bitsLeft = 8;
      }
    }

    // flush
    if (bitsLeft != 8) {
      output.write(current);
      current = 0;
      bitsLeft = 8;
    }
  }

  /**
   * Read bitpacked integers from input stream
   * @param buffer - input buffer
   * @param offset - offset
   * @param len - length
   * @param bitSize - bit width
   * @param input - input stream
   * @throws IOException
   */
  static void readInts(long[] buffer, int offset, int len, int bitSize,
                       InStream input) throws IOException {
    int bitsLeft = 0;
    int current = 0;

    for(int i = offset; i < (offset + len); i++) {
      long result = 0;
      int bitsLeftToRead = bitSize;
      while (bitsLeftToRead > bitsLeft) {
        result <<= bitsLeft;
        result |= current & ((1 << bitsLeft) - 1);
        bitsLeftToRead -= bitsLeft;
        current = input.read();
        bitsLeft = 8;
      }

      // handle the left over bits
      if (bitsLeftToRead > 0) {
        result <<= bitsLeftToRead;
        bitsLeft -= bitsLeftToRead;
        result |= (current >> bitsLeft) & ((1 << bitsLeftToRead) - 1);
      }
      buffer[i] = result;
    }
  }
}
