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

package org.apache.hadoop.hive.common.type;

import org.apache.hive.common.util.SuppressFBWarnings;

import java.util.Arrays;

/**
 * This code was based on code from Microsoft's PolyBase.
 *
 * Misc utilities used in this package.
 */
@SuppressFBWarnings(value = "MS_PKGPROTECT", justification = "Intended exposure of fields")
public final class SqlMathUtil {

  /** Mask to convert a long to a negative long. */
  public static final long NEGATIVE_LONG_MASK = 0x8000000000000000L;

  /** Mask to convert a long to an unsigned long. */
  public static final long FULLBITS_63 = 0x7FFFFFFFFFFFFFFFL;

  /** Mask to convert an int to a negative int. */
  public static final int NEGATIVE_INT_MASK = 0x80000000;

  /** Mask to convert signed integer to unsigned long. */
  public static final long LONG_MASK = 0xFFFFFFFFL;

  /** Mask to convert an int to an unsigned int. */
  public static final int FULLBITS_31 = 0x7FFFFFFF;

  /** Max unsigned integer. */
  public static final int FULLBITS_32 = 0xFFFFFFFF;

  /** 5^13 fits in 2^31. */
  public static final int MAX_POWER_FIVE_INT31 = 13;

  /** 5^x. All unsigned values. */
  protected static final int[] POWER_FIVES_INT31 = new int[MAX_POWER_FIVE_INT31 + 1];

  /** 5^27 fits in 2^63. */
  public static final int MAX_POWER_FIVE_INT63 = 27;

  /** 5^x. All unsigned values. */
  protected static final long[] POWER_FIVES_INT63 = new long[MAX_POWER_FIVE_INT63 + 1];

  /** 5^55 fits in 2^128. */
  public static final int MAX_POWER_FIVE_INT128 = 55;

  /** 5^x. */
  protected static final UnsignedInt128[] POWER_FIVES_INT128 = new UnsignedInt128[MAX_POWER_FIVE_INT128 + 1];

  /**
   * 1/5^x, scaled to 128bits (in other words, 2^128/5^x). Because of flooring,
   * this is same or smaller than real value.
   */
  protected static final UnsignedInt128[] INVERSE_POWER_FIVES_INT128 = new UnsignedInt128[MAX_POWER_FIVE_INT128 + 1];

  /** 10^9 fits in 2^31. */
  public static final int MAX_POWER_TEN_INT31 = 9;

  /** 10^x. All unsigned values. */
  protected static final int[] POWER_TENS_INT31 = new int[MAX_POWER_TEN_INT31 + 1];

  /** 5 * 10^(x-1). */
  protected static final int[] ROUND_POWER_TENS_INT31 = new int[MAX_POWER_TEN_INT31 + 1];

  /** 10^38 fits in UnsignedInt128. */
  public static final int MAX_POWER_TEN_INT128 = 38;

  /** 10^x. */
  protected static final UnsignedInt128[] POWER_TENS_INT128 = new UnsignedInt128[MAX_POWER_TEN_INT128 + 1];

  /** 5 * 10^(x-1). */
  protected static final UnsignedInt128[] ROUND_POWER_TENS_INT128 = new UnsignedInt128[MAX_POWER_TEN_INT128 + 1];

  /**
   * 1/10^x, scaled to 128bits, also word-shifted for better accuracy. Because
   * of flooring, this is same or smaller than real value.
   */
  protected static final UnsignedInt128[] INVERSE_POWER_TENS_INT128 = new UnsignedInt128[MAX_POWER_TEN_INT128 + 1];

  /** number of words shifted up in each INVERSE_POWER_TENS_INT128. */
  protected static final int[] INVERSE_POWER_TENS_INT128_WORD_SHIFTS = new int[MAX_POWER_TEN_INT128 + 1];

  /** To quickly calculate bit length for up to 256. */
  private static final byte[] BIT_LENGTH;

  /** Used in division. */
  private static final long BASE = (1L << 32);

  /**
   * Turn on or off the highest bit of an int value.
   *
   * @param val
   *          the value to modify
   * @param positive
   *          whether to turn off (positive) or on (negative).
   * @return unsigned int value
   */
  public static int setSignBitInt(int val, boolean positive) {
    if (positive) {
      return val & FULLBITS_31;
    }
    return val | NEGATIVE_INT_MASK;
  }

  /**
   * Turn on or off the highest bit of a long value.
   *
   * @param val
   *          the value to modify
   * @param positive
   *          whether to turn off (positive) or on (negative).
   * @return unsigned long value
   */
  public static long setSignBitLong(long val, boolean positive) {
    if (positive) {
      return val & FULLBITS_63;
    }
    return val | NEGATIVE_LONG_MASK;
  }

  /**
   * Returns the minimal number of bits to represent the given integer value.
   *
   * @param word
   *          int32 value
   * @return the minimal number of bits to represent the given integer value
   */
  public static short bitLengthInWord(int word) {
    if (word < 0) {
      return 32;
    }
    if (word < (1 << 16)) {
      if (word < 1 << 8) {
        return BIT_LENGTH[word];
      } else {
        return (short) (BIT_LENGTH[word >>> 8] + 8);
      }
    } else {
      if (word < (1 << 24)) {
        return (short) (BIT_LENGTH[word >>> 16] + 16);
      } else {
        return (short) (BIT_LENGTH[word >>> 24] + 24);
      }
    }
  }

  /**
   * Returns the minimal number of bits to represent the words.
   *
   * @param v0
   *          v0
   * @param v1
   *          v1
   * @param v2
   *          v2
   * @param v3
   *          v3
   * @return the minimal number of bits to represent the words
   */
  public static short bitLength(int v0, int v1, int v2, int v3) {
    if (v3 != 0) {
      return (short) (bitLengthInWord(v3) + 96);
    }
    if (v2 != 0) {
      return (short) (bitLengthInWord(v2) + 64);
    }
    if (v1 != 0) {
      return (short) (bitLengthInWord(v1) + 32);
    }
    return bitLengthInWord(v0);
  }

  /**
   * If we can assume JDK 1.8, this should use
   * java.lang.Integer.compareUnsigned(), which will be replaced with intrinsics
   * in JVM.
   *
   * @param x
   *          the first {@code int} to compare
   * @param y
   *          the second {@code int} to compare
   * @return the value {@code 0} if {@code x == y}; a value less than {@code 0}
   *         if {@code x < y} as unsigned values; and a value greater than
   *         {@code 0} if {@code x > y} as unsigned values
   * @see "http://hg.openjdk.java.net/jdk8/tl/jdk/rev/71200c517524"
   */
  public static int compareUnsignedInt(int x, int y) {

    // Can't assume JDK 1.8, so implementing this explicitly.
    // return Integer.compare(x + Integer.MIN_VALUE, y + Integer.MIN_VALUE);
    if (x == y) {
      return 0;
    }
    if (x + Integer.MIN_VALUE < y + Integer.MIN_VALUE) {
      return -1;
    } else {
      return 1;
    }
  }

  /**
   * If we can assume JDK 1.8, this should use java.lang.Long.compareUnsigned(),
   * which will be replaced with intrinsics in JVM.
   *
   * @param x
   *          the first {@code int} to compare
   * @param y
   *          the second {@code int} to compare
   * @return the value {@code 0} if {@code x == y}; a value less than {@code 0}
   *         if {@code x < y} as unsigned values; and a value greater than
   *         {@code 0} if {@code x > y} as unsigned values
   * @see "http://hg.openjdk.java.net/jdk8/tl/jdk/rev/71200c517524"
   */
  public static int compareUnsignedLong(long x, long y) {

    // Can't assume JDK 1.8, so implementing this explicitly.
    // return Long.compare(x + Long.MIN_VALUE, y + Long.MIN_VALUE);
    if (x == y) {
      return 0;
    }
    if (x + Long.MIN_VALUE < y + Long.MIN_VALUE) {
      return -1;
    } else {
      return 1;
    }
  }

  /**
   * If we can assume JDK 1.8, this should use java.lang.Long.divideUnsigned(),
   * which will be replaced with intrinsics in JVM.
   *
   * @param dividend
   *          the value to be divided
   * @param divisor
   *          the value doing the dividing
   * @return the unsigned quotient of the first argument divided by the second
   *         argument
   * @see "http://hg.openjdk.java.net/jdk8/tl/jdk/rev/71200c517524"
   */
  public static long divideUnsignedLong(long dividend, long divisor) {
    if (divisor < 0L) {

      // Answer must be 0 or 1 depending on relative magnitude
      // of dividend and divisor.
      return (compareUnsignedLong(dividend, divisor)) < 0 ? 0L : 1L;
    }

    if (dividend >= 0) { // Both inputs non-negative
      return dividend / divisor;
    } else {

      // simple division.
      // Yes, we should do something like this:
      // http://www.hackersdelight.org/divcMore.pdf
      // but later... (anyway this will be eventually replaced by
      // intrinsics in Java 8)

      // an equivalent algorithm exists in
      // com.google.common.primitives.UnsignedLongs
      long quotient = ((dividend >>> 1L) / divisor) << 1L;
      long remainder = dividend - quotient * divisor;
      if (compareUnsignedLong(remainder, divisor) >= 0) {
        return quotient + 1;
      }
      return quotient;
    }
  }

  /**
   * If we can assume JDK 1.8, this should use
   * java.lang.Long.remainderUnsigned(), which will be replaced with intrinsics
   * in JVM.
   *
   * @param dividend
   *          the value to be divided
   * @param divisor
   *          the value doing the dividing
   * @return the unsigned remainder of the first argument divided by the second
   *         argument
   * @see "http://hg.openjdk.java.net/jdk8/tl/jdk/rev/71200c517524"
   */
  public static long remainderUnsignedLong(long dividend, long divisor) {
    if (divisor < 0L) {

      // because divisor is negative, quotient is at most 1.
      // remainder must be dividend itself (quotient=0), or dividend -
      // divisor
      return (compareUnsignedLong(dividend, divisor)) < 0 ? dividend : dividend
          - divisor;
    }

    if (dividend >= 0L) { // signed comparisons
      return dividend % divisor;
    } else {
      // same above
      long quotient = ((dividend >>> 1L) / divisor) << 1L;
      long remainder = dividend - quotient * divisor;
      if (compareUnsignedLong(remainder, divisor) >= 0) {
        return remainder - divisor;
      }
      return remainder;
    }
  }

  /**
   * @param lo
   *          low 32bit
   * @param hi
   *          high 32bit
   * @return long value that combines the two integers
   */
  public static long combineInts(int lo, int hi) {
    return ((hi & LONG_MASK) << 32L) | (lo & LONG_MASK);
  }

  /**
   * @param val
   *          long value
   * @return high 32bit of the given value
   */
  public static int extractHiInt(long val) {
    return (int) (val >> 32);
  }

  /**
   * @param val
   *          long value
   * @return low 32bit of the given value
   */
  public static int extractLowInt(long val) {
    return (int) val;
  }

  /** Throws an overflow exception. */
  static void throwOverflowException() {
    throw new ArithmeticException("Overflow");
  }

  /** Throws a divide-by-zero exception. */
  static void throwZeroDivisionException() {
    throw new ArithmeticException("Divide by zero");
  }

  /**
   * Multi-precision one super-digit multiply in place.
   *
   * @param inOut
   * @param multiplier
   */
  private static void multiplyMultiPrecision(int[] inOut, int multiplier) {
    long multiplierUnsigned = multiplier & SqlMathUtil.LONG_MASK;
    long product = 0L;
    for (int i = 0; i < inOut.length; ++i) {
      product = (inOut[i] & SqlMathUtil.LONG_MASK) * multiplierUnsigned
          + (product >>> 32);
      inOut[i] = (int) product;
    }
    if ((product >> 32) != 0) {
      SqlMathUtil.throwOverflowException();
    }
  }

  /**
   * Multi-precision one super-digit divide in place.
   *
   * @param inOut
   * @param divisor
   * @return
   */
  private static int divideMultiPrecision(int[] inOut, int divisor) {
    long divisorUnsigned = divisor & SqlMathUtil.LONG_MASK;
    long quotient;
    long remainder = 0;
    for (int i = inOut.length - 1; i >= 0; --i) {
      remainder = (inOut[i] & SqlMathUtil.LONG_MASK) + (remainder << 32);
      quotient = remainder / divisorUnsigned;
      inOut[i] = (int) quotient;
      remainder %= divisorUnsigned;
    }
    return (int) remainder;
  }

  /**
   * Returns length of the array discounting the trailing elements with zero value.
   */
  private static int arrayValidLength(int[] array) {
    int len = array.length;
    while (len > 0 && array[len - 1] == 0) {
      --len;
    }
    return len <= 0 ? 0 : len;
  }

  /**
   * Multi-precision divide. dividend and divisor not changed. Assumes that
   * there is enough room in quotient for results. Drawbacks of this
   * implementation: 1) Need one extra super-digit in R 2) As it modifies D
   * during work, then it restores it back (this is necessary because the caller
   * doesn't expect D to change) 3) Always get Q and R - if R is unnecessary,
   * can be slightly faster.
   *
   * @param dividend
   *          dividend. in.
   * @param divisor
   *          divisor. in.
   * @param quotient
   *          quotient. out.
   * @return remainder
   */
  public static int[] divideMultiPrecision(int[] dividend, int[] divisor,
      int[] quotient) {
    final int dividendLength = arrayValidLength(dividend);
    final int divisorLength = arrayValidLength(divisor);
    Arrays.fill(quotient, 0);

    // Remainder := Dividend
    int[] remainder = new int[dividend.length + 1];
    System.arraycopy(dividend, 0, remainder, 0, dividend.length);
    remainder[remainder.length - 1] = 0;

    if (divisorLength == 0) {
      throwZeroDivisionException();
    }
    if (dividendLength < divisorLength) {
      return remainder;
    }
    if (divisorLength == 1) {
      int rem = divideMultiPrecision(remainder, divisor[0]);
      System.arraycopy(remainder, 0, quotient, 0, quotient.length);
      Arrays.fill(remainder, 0);
      remainder[0] = rem;
      return remainder;
    }

    // Knuth, "The Art of Computer Programming", 3rd edition, vol.II, Alg.D,
    // pg 272
    // D1. Normalize so high digit of D >= BASE/2 - that guarantee
    // that QH will not be too far from the correct digit later in D3
    int d1 = (int) (BASE / ((divisor[divisorLength - 1] & LONG_MASK) + 1L));
    if (d1 > 1) {

      // We are modifying divisor here, so make a local copy.
      int[] newDivisor = new int[divisorLength];
      System.arraycopy(divisor, 0, newDivisor, 0, divisorLength);
      multiplyMultiPrecision(newDivisor, d1);
      divisor = newDivisor;
      multiplyMultiPrecision(remainder, d1);
    }

    // only 32bits, but long to behave as unsigned
    long dHigh = (divisor[divisorLength - 1] & LONG_MASK);
    long dLow = (divisor[divisorLength - 2] & LONG_MASK);

    // D2 already done - iulRindex initialized before normalization of R.
    // D3-D7. Loop on iulRindex - obtaining digits one-by-one, as "in paper"
    for (int rIndex = remainder.length - 1; rIndex >= divisorLength; --rIndex) {

      // D3. Calculate Q hat - estimation of the next digit
      long accum = combineInts(remainder[rIndex - 1], remainder[rIndex]);
      int qhat;
      if (dHigh == (remainder[rIndex] & LONG_MASK)) {
        qhat = (int) (BASE - 1);
      } else {
        qhat = (int) divideUnsignedLong(accum, dHigh);
      }

      int rhat = (int) (accum - (qhat & LONG_MASK) * dHigh);
      while (compareUnsignedLong(dLow * (qhat & LONG_MASK),
          combineInts(remainder[rIndex - 2], rhat)) > 0) {
        qhat--;
        if ((rhat & LONG_MASK) >= -((int) dHigh)) {
          break;
        }
        rhat += dHigh;
      }

      // D4. Multiply and subtract: (some digits of) R -= D * QH
      long dwlMulAccum = 0;
      accum = BASE;
      int iulRwork = rIndex - divisorLength;
      for (int dIndex = 0; dIndex < divisorLength; dIndex++, iulRwork++) {
        dwlMulAccum += (qhat & LONG_MASK) * (divisor[dIndex] & LONG_MASK);
        accum += (remainder[iulRwork] & LONG_MASK)
            - (extractLowInt(dwlMulAccum) & LONG_MASK);
        dwlMulAccum = (extractHiInt(dwlMulAccum) & LONG_MASK);
        remainder[iulRwork] = extractLowInt(accum);
        accum = (extractHiInt(accum) & LONG_MASK) + BASE - 1;
      }
      accum += (remainder[iulRwork] & LONG_MASK) - dwlMulAccum;
      remainder[iulRwork] = extractLowInt(accum);
      quotient[rIndex - divisorLength] = qhat;

      // D5. Test remainder. Carry indicates result<0, therefore QH 1 too
      // large
      if (extractHiInt(accum) == 0) {

        // D6. Add back - probability is 2**(-31). R += D. Q[digit] -= 1
        quotient[rIndex - divisorLength] = qhat - 1;
        int carry = 0;
        int dIndex = 0;
        for (iulRwork = rIndex - divisorLength; dIndex < divisorLength; dIndex++, iulRwork++) {
          long accum2 = (divisor[dIndex] & LONG_MASK)
              + (remainder[iulRwork] & LONG_MASK) + (carry & LONG_MASK);
          carry = extractHiInt(accum2);
          remainder[iulRwork] = extractLowInt(accum2);
        }
        remainder[iulRwork] += carry;
      }
    }

    // D8. Unnormalize: Divide R to get result
    if (d1 > 1) {
      divideMultiPrecision(remainder, d1);
    }

    return remainder;
  }

  static {
    BIT_LENGTH = new byte[256];
    BIT_LENGTH[0] = 0;
    for (int i = 1; i < 8; ++i) {
      for (int j = 1 << (i - 1); j < 1 << i; ++j) {
        BIT_LENGTH[j] = (byte) i;
      }
    }

    POWER_FIVES_INT31[0] = 1;
    for (int i = 1; i < POWER_FIVES_INT31.length; ++i) {
      POWER_FIVES_INT31[i] = POWER_FIVES_INT31[i - 1] * 5;
      assert (POWER_FIVES_INT31[i] > 0);
    }

    POWER_FIVES_INT63[0] = 1L;
    for (int i = 1; i < POWER_FIVES_INT63.length; ++i) {
      POWER_FIVES_INT63[i] = POWER_FIVES_INT63[i - 1] * 5L;
      assert (POWER_FIVES_INT63[i] > 0L);
    }

    POWER_TENS_INT31[0] = 1;
    ROUND_POWER_TENS_INT31[0] = 0;
    for (int i = 1; i < POWER_TENS_INT31.length; ++i) {
      POWER_TENS_INT31[i] = POWER_TENS_INT31[i - 1] * 10;
      assert (POWER_TENS_INT31[i] > 0);
      ROUND_POWER_TENS_INT31[i] = POWER_TENS_INT31[i] >> 1;
    }

    POWER_FIVES_INT128[0] = new UnsignedInt128(1);
    INVERSE_POWER_FIVES_INT128[0] = new UnsignedInt128(0xFFFFFFFF, 0xFFFFFFFF,
        0xFFFFFFFF, 0xFFFFFFFF);
    for (int i = 1; i < POWER_FIVES_INT128.length; ++i) {
      POWER_FIVES_INT128[i] = new UnsignedInt128(POWER_FIVES_INT128[i - 1]);
      POWER_FIVES_INT128[i].multiplyDestructive(5);
      INVERSE_POWER_FIVES_INT128[i] = new UnsignedInt128(
          INVERSE_POWER_FIVES_INT128[i - 1]);
      INVERSE_POWER_FIVES_INT128[i].divideDestructive(5);
    }

    POWER_TENS_INT128[0] = new UnsignedInt128(1);
    ROUND_POWER_TENS_INT128[0] = new UnsignedInt128(0);
    INVERSE_POWER_TENS_INT128[0] = new UnsignedInt128(0xFFFFFFFF, 0xFFFFFFFF,
        0xFFFFFFFF, 0xFFFFFFFF);
    INVERSE_POWER_TENS_INT128_WORD_SHIFTS[0] = 0;
    int[] inverseTens = new int[8];
    Arrays.fill(inverseTens, 0xFFFFFFFF);
    for (int i = 1; i < POWER_TENS_INT128.length; ++i) {
      final int divisor = 10;
      POWER_TENS_INT128[i] = new UnsignedInt128(POWER_TENS_INT128[i - 1]);
      POWER_TENS_INT128[i].multiplyDestructive(divisor);
      ROUND_POWER_TENS_INT128[i] = POWER_TENS_INT128[i].shiftRightConstructive(
          1, false);

      long quotient;
      long remainder = 0;
      for (int j = inverseTens.length - 1; j >= 0; --j) {
        quotient = ((inverseTens[j] & SqlMathUtil.LONG_MASK) + (remainder << 32))
            / divisor;
        remainder = ((inverseTens[j] & SqlMathUtil.LONG_MASK) + (remainder << 32))
            % divisor;
        inverseTens[j] = (int) quotient;
      }
      int wordShifts = 0;
      for (int j = inverseTens.length - 1; j >= 4 && inverseTens[j] == 0; --j) {
        ++wordShifts;
      }
      INVERSE_POWER_TENS_INT128_WORD_SHIFTS[i] = wordShifts;
      INVERSE_POWER_TENS_INT128[i] = new UnsignedInt128(
          inverseTens[inverseTens.length - 4 - wordShifts],
          inverseTens[inverseTens.length - 3 - wordShifts],
          inverseTens[inverseTens.length - 2 - wordShifts],
          inverseTens[inverseTens.length - 1 - wordShifts]);
    }
  }

  private SqlMathUtil() {
  }
}
