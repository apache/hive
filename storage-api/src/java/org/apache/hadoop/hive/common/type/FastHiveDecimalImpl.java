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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

/**
 *    This class is a companion to the FastHiveDecimal class that separates the essential of code
 *    out of FastHiveDecimal into static methods in this class so that they can be used directly
 *    by vectorization to implement decimals by storing the fast0, fast1, and fast2 longs and
 *    the fastSignum, fastScale, etc ints in the DecimalColumnVector class.
 */
public class FastHiveDecimalImpl extends FastHiveDecimal {

  /**
   * Representation of fast decimals.
   *
   * We use 3 long words to store the 38 digits of fast decimals and and 3 integers for sign,
   * integer digit count, and scale.
   *
   * The lower and middle long words store 16 decimal digits each; the high long word has
   * 6 decimal digits; total 38 decimal digits.
   *
   * We do not try and represent fast decimal value as an unsigned 128 bit binary number in 2 longs.
   * There are several important reasons for this.
   *
   * The effort to represent an unsigned 128 integer in 2 Java signed longs is very difficult,
   * error prone, hard to debug, and not worth the effort.
   *
   * The focus here is on reusing memory (i.e. with HiveDecimalWritable) as often as possible.
   * Reusing memory is good for grouping of fast decimal objects and related objects in CPU cache
   * lines for fast memory access and eliminating the cost of allocating temporary objects and
   * reducing the global cost of garbage collection.
   *
   * In other words, we are focused on avoiding the poor performance of Java general immutable
   * objects.
   *
   * Reducing memory size or being concerned about the memory size of using 3 longs vs. 2 longs
   * for 128 unsigned bits is not the focus here.
   *
   * Besides focusing on reusing memory, storing a limited number (16) decimal digits in the longs
   * rather than compacting the value into all binary bits of 2 longs has a surprising benefit.
   *
   * One big part of implementing decimals turns out to be manipulating decimal digits.
   *
   * For example, rounding a decimal involves trimming off lower digits or clearing lower digits.
   * Since radix 10 digits cannot be masked with binary masks, we use division and multiplication
   * using powers of 10.  We can easily manipulate the decimal digits in a long word using simple
   * integer multiplication / division without doing emulated 128 binary bit multiplication /
   * division (e.g. the defunct Decimal128 class).
   *
   * For example, say we want to scale (round) down the fraction digits of a decimal.
   *
   *      final long divideFactor = powerOfTenTable[scaleDown];
   *      final long multiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - scaleDown];
   *
   *      result0 =
   *          fast0 / divideFactor
   *        + ((fast1 % divideFactor) * multiplyFactor);
   *      result1 =
   *          fast1 / divideFactor
   *        + ((fast2 % divideFactor) * multiplyFactor);
   *      result2 =
   *          fast2 / divideFactor;
   *
   * It also turns out to do addition and subtraction of decimals with different scales can involve
   * overlap using more than 3 long words.  Manipulating extra words is a natural extension of
   * the existing techniques.
   *
   * Why is the decimal digits representation easier to debug?  You can see the decimal digits in
   * the 3 long words and do not have to convert binary words to decimal to see the value.
   *
   * 16 decimal digits for a long was choose so that an int can have 1/2 or 8 decimal digits during
   * multiplication of int half words so intermediate multiplication results do not overflow a long.
   * And, so addition overflow is well below the sign bit of a long.
   */

  // Code Sections:
  //   Initialize (fastSetFrom*).
  //   Take Integer or Fractional Portion.
  //   Binary to Decimal Conversion.
  //   Decimal to Binary Conversion.r
  //   Emulate SerializationUtils Deserialization used by ORC.
  //   Emulate SerializationUtils Serialization used by ORC.
  //   Emulate BigInteger Deserialization used by LazyBinary and others.
  //   Emulate BigInteger Serialization used by LazyBinary and others.
  //   Decimal to Integer Conversion.
  //   Decimal to Non-Integer Conversion.
  //   Decimal Comparison.
  //   Decimal Rounding.
  //   Decimal Scale Up/Down.
  //   Decimal Precision / Trailing Zeroes.
  //   Decimal Addition / Subtraction.
  //   Decimal Multiply.
  //   Decimal Division / Remainder.
  //   Decimal String Formatting.
  //   Decimal Validation.
  //   Decimal Debugging.

  private static final long[] powerOfTenTable = {
    1L,                   // 0
    10L,
    100L,
    1000L,
    10000L,
    100000L,
    1000000L,
    10000000L,
    100000000L,           // 8
    1000000000L,
    10000000000L,
    100000000000L,
    1000000000000L,
    10000000000000L,
    100000000000000L,
    1000000000000000L,
    10000000000000000L,   // 16
    100000000000000000L,
    1000000000000000000L, // 18
  };

  public static final int MAX_DECIMAL_DIGITS = 38;

  /**
   * Int: 8 decimal digits.  An even number and 1/2 of MAX_LONGWORD_DECIMAL.
   */
  private static final int INTWORD_DECIMAL_DIGITS = 8;
  private static final int MULTIPLER_INTWORD_DECIMAL = (int) powerOfTenTable[INTWORD_DECIMAL_DIGITS];

  /**
   * Long: 16 decimal digits.  An even number and twice MAX_INTWORD_DECIMAL.
   */
  private static final int LONGWORD_DECIMAL_DIGITS = 16;
  private static final long MAX_LONGWORD_DECIMAL = powerOfTenTable[LONGWORD_DECIMAL_DIGITS] - 1;
  private static final long MULTIPLER_LONGWORD_DECIMAL = powerOfTenTable[LONGWORD_DECIMAL_DIGITS];

  public static final int DECIMAL64_DECIMAL_DIGITS = 18;
  public static final long MAX_ABS_DECIMAL64 = 999999999999999999L;  // 18 9's -- quite reliable!

  private static final int TWO_X_LONGWORD_DECIMAL_DIGITS = 2 * LONGWORD_DECIMAL_DIGITS;
  private static final int THREE_X_LONGWORD_DECIMAL_DIGITS = 3 * LONGWORD_DECIMAL_DIGITS;
  private static final int FOUR_X_LONGWORD_DECIMAL_DIGITS = 4 * LONGWORD_DECIMAL_DIGITS;

  // 38 decimal maximum - 32 digits in 2 lower longs (6 digits here).
  private static final int HIGHWORD_DECIMAL_DIGITS = MAX_DECIMAL_DIGITS - TWO_X_LONGWORD_DECIMAL_DIGITS;
  private static final long MAX_HIGHWORD_DECIMAL =
      powerOfTenTable[HIGHWORD_DECIMAL_DIGITS] - 1;

  // 38 * 2 or 76 full decimal maximum - (64 + 8) digits in 4 lower longs (4 digits here).
  private static final long FULL_MAX_HIGHWORD_DECIMAL =
      powerOfTenTable[MAX_DECIMAL_DIGITS * 2 - (FOUR_X_LONGWORD_DECIMAL_DIGITS + INTWORD_DECIMAL_DIGITS)] - 1;

  /**
   * BigInteger constants.
   */

  private static final BigInteger BIG_INTEGER_TWO = BigInteger.valueOf(2);
  private static final BigInteger BIG_INTEGER_FIVE = BigInteger.valueOf(5);
  private static final BigInteger BIG_INTEGER_TEN = BigInteger.valueOf(10);

  public static final BigInteger BIG_INTEGER_MAX_DECIMAL =
      BIG_INTEGER_TEN.pow(MAX_DECIMAL_DIGITS).subtract(BigInteger.ONE);

  private static final BigInteger BIG_INTEGER_MAX_LONGWORD_DECIMAL =
      BigInteger.valueOf(MAX_LONGWORD_DECIMAL);

  private static final BigInteger BIG_INTEGER_LONGWORD_MULTIPLIER =
      BigInteger.ONE.add(BIG_INTEGER_MAX_LONGWORD_DECIMAL);
  private static final BigInteger BIG_INTEGER_LONGWORD_MULTIPLIER_2X =
      BIG_INTEGER_LONGWORD_MULTIPLIER.multiply(BIG_INTEGER_LONGWORD_MULTIPLIER);
  private static final BigInteger BIG_INTEGER_MAX_HIGHWORD_DECIMAL =
      BigInteger.valueOf(MAX_HIGHWORD_DECIMAL);
  private static final BigInteger BIG_INTEGER_HIGHWORD_MULTIPLIER =
      BigInteger.ONE.add(BIG_INTEGER_MAX_HIGHWORD_DECIMAL);

  // UTF-8 byte constants used by string/UTF-8 bytes to decimal and decimal to String/UTF-8 byte
  // conversion.
  private static final byte BYTE_DIGIT_ZERO = (byte) '0';
  private static final byte BYTE_DIGIT_NINE = (byte) '9';

  // Decimal point.
  private static final byte BYTE_DOT = (byte) '.';

  // Sign.
  private static final byte BYTE_MINUS = (byte) '-';
  private static final byte BYTE_PLUS = (byte) '+';

  // Exponent E or e.
  private static final byte BYTE_EXPONENT_LOWER = (byte) 'e';
  private static final byte BYTE_EXPONENT_UPPER = (byte) 'E';

  //************************************************************************************************
  // Initialize (fastSetFrom*).

  /*
   * All of the fastSetFrom* methods require the caller to pass a fastResult parameter has been
   * reset for better performance.
   */

  private static void doRaiseSetFromBytesInvalid(
      byte[] bytes, int offset, int length,
      FastHiveDecimal fastResult) {
    final int end = offset + length;
    throw new RuntimeException(
        "Invalid fast decimal \"" +
            new String(bytes, offset, end, StandardCharsets.UTF_8) + "\"" +
        " fastSignum " + fastResult.fastSignum + " fast0 " + fastResult.fast0 + " fast1 " + fastResult.fast1 + " fast2 " + fastResult.fast2 +
            " fastIntegerDigitCount " + fastResult.fastIntegerDigitCount +" fastScale " + fastResult.fastScale +
        " stack trace: " + getStackTraceAsSingleLine(Thread.currentThread().getStackTrace()));
  }

  /**
   * Scan a byte array slice for a decimal number in UTF-8 bytes.
   *
   * Syntax:
   *   [+|-][integerPortion][.[fractionalDigits]][{E|e}[+|-]exponent]
   *                                                  // Where at least one integer or fractional
   *                                                  // digit is required...
   *
   * We handle too many fractional digits by doing rounding ROUND_HALF_UP.
   *
   * NOTE: The fastSetFromBytes method requires the caller to pass a fastResult parameter has been
   * reset for better performance.
   *
   * @param bytes the bytes to copy from
   * @param offset the starting location in bytes
   * @param length the number of bytes to use from bytes
   * @param trimBlanks should spaces be trimmed?
   * @param fastResult  True if the byte array slice was successfully converted to a decimal.
   * @return Was a valid number found?
   */
  public static boolean fastSetFromBytes(byte[] bytes, int offset, int length, boolean trimBlanks,
      FastHiveDecimal fastResult) {

    final int bytesLength = bytes.length;

    if (offset < 0 || offset >= bytesLength) {
      return false;
    }
    final int end = offset + length;
    if (end <= offset || end > bytesLength) {
      return false;
    }

    // We start here with at least one byte.
    int index = offset;

    if (trimBlanks) {
      while (isValidSpecialCharacter(bytes[index])) {
        if (++index >= end) {
          return false;
        }
      }
    }

    // Started with a few ideas from BigDecimal(char[] in, int offset, int len) constructor...
    // But soon became very fast decimal specific.

    boolean isNegative = false;
    if (bytes[index] == BYTE_MINUS) {
      isNegative = true;
      if (++index >= end) {
        return false;
      }
    } else if (bytes[index] == BYTE_PLUS) {
      if (++index >= end) {
        return false;
      }
    }

    int precision = 0;

    // We fill starting with highest digit in highest longword (HIGHWORD_DECIMAL_DIGITS) and
    // move down.  At end will will shift everything down if necessary.

    int longWordIndex = 0;   // Where 0 is the highest longword; 1 is middle longword, etc.

    int digitNum = HIGHWORD_DECIMAL_DIGITS;
    long multiplier = powerOfTenTable[HIGHWORD_DECIMAL_DIGITS - 1];

    int digitValue = 0;
    long longWord = 0;

    long fast0 = 0;
    long fast1 = 0;
    long fast2 = 0;

    byte work;

    // Parse integer portion.

    boolean haveInteger = false;
    while (true) {
      work = bytes[index];
      if (work < BYTE_DIGIT_ZERO || work > BYTE_DIGIT_NINE) {
        break;
      }
      haveInteger = true;
      if (precision == 0 && work == BYTE_DIGIT_ZERO) {
        // Ignore leading zeroes.
        if (++index >= end) {
          break;
        }
        continue;
      }
      digitValue = work - BYTE_DIGIT_ZERO;
      if (digitNum == 0) {

        // Integer parsing move to next lower longword.

        // Save previous longword.
        if (longWordIndex == 0) {
          fast2 = longWord;
        } else if (longWordIndex == 1) {
          fast1 = longWord;
        } else if (longWordIndex == 2) {

          // We have filled HiveDecimal.MAX_PRECISION digits and have no more room in our limit precision
          // fast decimal.
          return false;
        }
        longWordIndex++;

        // The middle and lowest longwords highest digit number is LONGWORD_DECIMAL_DIGITS.
        digitNum = LONGWORD_DECIMAL_DIGITS;
        multiplier = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - 1];
        longWord = 0;
      }
      longWord += digitValue * multiplier;
      multiplier /= 10;
      digitNum--;
      precision++;
      if (++index >= end) {
        break;
      }
    }

    // At this point we may have parsed an integer.

    // Try to eat a dot now since it could be the end.  We remember if we saw a dot so we can
    // do error checking later and detect just a dot.
    boolean sawDot = false;
    if (index < end && bytes[index] == BYTE_DOT) {
      sawDot = true;
      index++;
    }

    // Try to eat trailing blank padding.
    if (trimBlanks && index < end && isValidSpecialCharacter(bytes[index])) {
      index++;
      while (index < end && isValidSpecialCharacter(bytes[index])) {
        index++;
      }
      if (index < end) {
        // Junk after trailing blank padding.
        return false;
      }
      // Otherwise, fall through and process the what we saw before possible trailing blanks.
    }

    // Any more input?
    if (index >= end) {

      // We hit the end after getting optional integer and optional dot and optional blank padding.

      if (!haveInteger) {
        return false;
      }

      if (precision == 0) {

        // We just had leading zeroes (and possibly a dot and trailing blanks).
        // Value is 0.
        return true;
      }
      // Save last longword.
      if (longWordIndex == 0) {
        fast2 = longWord;
      } else if (longWordIndex == 1) {
        fast1 = longWord;
      } else {
        fast0 = longWord;
      }
      fastResult.fastSignum = (isNegative ? -1 : 1);
      fastResult.fastIntegerDigitCount = precision;
      fastResult.fastScale = 0;
      final int scaleDown = HiveDecimal.MAX_PRECISION - precision;
      if (scaleDown > 0) {
        doFastScaleDown(fast0, fast1, fast2, scaleDown, fastResult);
      } else {
        fastResult.fast0 = fast0;
        fastResult.fast1 = fast1;
        fastResult.fast2 = fast2;
      }
      return true;
    }

    // We have more input but did we start with something valid?
    if (!haveInteger && !sawDot) {

      // Must have one of those at this point.
      return false;
    }

    int integerDigitCount = precision;

    int nonTrailingZeroScale = 0;
    boolean roundingNecessary = false;
    if (sawDot) {

      // Parse fraction portion.

      while (true) {
        work = bytes[index];
        if (work < BYTE_DIGIT_ZERO || work > BYTE_DIGIT_NINE) {
          if (!haveInteger) {

            // Naked dot.
            return false;
          }
          break;
        }
        haveInteger = true;
        digitValue = work - BYTE_DIGIT_ZERO;
        if (digitNum == 0) {

          // Fraction digit parsing move to next lower longword.

          // Save previous longword.
          if (longWordIndex == 0) {
            fast2 = longWord;
          } else if (longWordIndex == 1) {
            fast1 = longWord;
          } else if (longWordIndex == 2) {

            // We have filled HiveDecimal.MAX_PRECISION digits and have no more room in our limit precision
            // fast decimal.  However, since we are processing fractional digits, we do rounding.
            // away.
            if (digitValue >= 5) {
              roundingNecessary = true;
            }

            // Scan through any remaining digits...
            while (++index < end) {
              work = bytes[index];
              if (work < BYTE_DIGIT_ZERO || work > BYTE_DIGIT_NINE) {
                break;
              }
            }
            break;
          }
          longWordIndex++;
          digitNum = LONGWORD_DECIMAL_DIGITS;
          multiplier = powerOfTenTable[digitNum - 1];
          longWord = 0;
        }
        longWord += digitValue * multiplier;
        multiplier /= 10;
        digitNum--;
        precision++;
        if (digitValue != 0) {
          nonTrailingZeroScale = precision - integerDigitCount;
        }
        if (++index >= end) {
          break;
        }
      }
    }

    boolean haveExponent = false;
    if (index < end &&
        (bytes[index] == BYTE_EXPONENT_UPPER || bytes[index] == BYTE_EXPONENT_LOWER)) {
      haveExponent = true;
      index++;
      if (index >= end) {
        // More required.
        return false;
      }
    }

    // At this point we have a number.  Save it in fastResult.  Round it.  If we have an exponent,
    // we will do a power 10 operation on fastResult.

    // Save last longword.
    if (longWordIndex == 0) {
      fast2 = longWord;
    } else if (longWordIndex == 1) {
      fast1 = longWord;
    } else {
      fast0 = longWord;
    }

    int trailingZeroesScale = precision - integerDigitCount;
    if (integerDigitCount == 0 && nonTrailingZeroScale == 0) {
      // Zero(es).
    } else {
      fastResult.fastSignum = (isNegative ? -1 : 1);
      fastResult.fastIntegerDigitCount = integerDigitCount;
      fastResult.fastScale = nonTrailingZeroScale;
      final int trailingZeroCount = trailingZeroesScale - fastResult.fastScale;
      final int scaleDown = HiveDecimal.MAX_PRECISION - precision + trailingZeroCount;
      if (scaleDown > 0) {
        doFastScaleDown(fast0, fast1, fast2, scaleDown, fastResult);
      } else {
        fastResult.fast0 = fast0;
        fastResult.fast1 = fast1;
        fastResult.fast2 = fast2;
      }
    }

    if (roundingNecessary) {

      if (fastResult.fastSignum == 0) {
        fastResult.fastSignum = (isNegative ? -1 : 1);
        fastResult.fast0 = 1;
        fastResult.fastIntegerDigitCount = 0;
        fastResult.fastScale = HiveDecimal.MAX_SCALE;
      } else {
        if (!fastAdd(
          fastResult.fastSignum, fastResult.fast0, fastResult.fast1, fastResult.fast2,
          fastResult.fastIntegerDigitCount, fastResult.fastScale,
          fastResult.fastSignum, 1, 0, 0, 0, trailingZeroesScale,
          fastResult)) {
          return false;
        }
      }
    }

    if (!haveExponent) {

      // Try to eat trailing blank padding.
      if (trimBlanks && index < end && isValidSpecialCharacter(bytes[index])) {
        index++;
        while (index < end && isValidSpecialCharacter(bytes[index])) {
          index++;
        }
      }
      if (index < end) {
        // Junk after trailing blank padding.
        return false;
      }
      return true;
    }

    // At this point, we have seen the exponent letter E or e and have decimal information as:
    //     isNegative, precision, integerDigitCount, nonTrailingZeroScale, and
    //     fast0, fast1, fast2.
    //
    // After we determine the exponent, we will do appropriate scaling and fill in fastResult.

    boolean isExponentNegative = false;
    if (bytes[index] == BYTE_MINUS) {
      isExponentNegative = true;
      if (++index >= end) {
        return false;
      }
    } else if (bytes[index] == BYTE_PLUS) {
      if (++index >= end) {
        return false;
      }
    }

    long exponent = 0;
    multiplier = 1;
    while (true) {
      work = bytes[index];
      if (work < BYTE_DIGIT_ZERO || work > BYTE_DIGIT_NINE) {
        break;
      }
      if (multiplier > 10) {
        // Power of ten way beyond our precision/scale...
        return false;
      }
      digitValue = work - BYTE_DIGIT_ZERO;
      if (digitValue != 0 || exponent != 0) {
        exponent = exponent * 10 + digitValue;
        multiplier *= 10;
      }
      if (++index >= end) {
        break;
      }
    }
    if (isExponentNegative) {
      exponent = -exponent;
    }

    // Try to eat trailing blank padding.
    if (trimBlanks && index < end && isValidSpecialCharacter(bytes[index])) {
      index++;
      while (index < end && isValidSpecialCharacter(bytes[index])) {
        index++;
      }
    }
    if (index < end) {
      // Junk after exponent.
      return false;
    }


    if (integerDigitCount == 0 && nonTrailingZeroScale == 0) {
      // Zero(es).
      return true;
    }

    if (exponent == 0) {

      // No effect since 10^0 = 1.

    } else {

      // We for these input with exponents, we have at this point an intermediate decimal,
      // an exponent power, and a result:
      //
      //                     intermediate
      //   input               decimal      exponent        result
      // 701E+1            701 scale 0        +1            7010 scale 0
      // 3E+4              3 scale 0          +4               3 scale 0
      // 3.223E+9          3.223 scale 3      +9      3223000000 scale 0
      // 0.009E+10         0.009 scale 4      +10       90000000 scale 0
      // 0.3221E-2         0.3221 scale 4     -2               0.003221 scale 6
      // 0.00223E-20       0.00223 scale 5    -20              0.0000000000000000000000223 scale 25
      //

      if (!fastScaleByPowerOfTen(
          fastResult,
          (int) exponent,
          fastResult)) {
        return false;
      }
    }

    final int trailingZeroCount =
        fastTrailingDecimalZeroCount(
            fastResult.fast0, fastResult.fast1, fastResult.fast2,
            fastResult.fastIntegerDigitCount, fastResult.fastScale);
    if (trailingZeroCount > 0) {
      doFastScaleDown(
          fastResult,
          trailingZeroCount,
          fastResult);
      fastResult.fastScale -= trailingZeroCount;
    }

    return true;
  }

  /**
   * Scans a byte array slice for UNSIGNED RAW DIGITS ONLY in UTF-8 (ASCII) characters
   * and forms a decimal from the digits and a sign and scale.
   *
   * Designed for BinarySortable serialization format that separates the sign and scale
   * from the raw digits.
   *
   * NOTE: The fastSetFromDigitsOnlyBytesAndScale method requires the caller to pass a fastResult
   * parameter has been reset for better performance.
   *
   * @param isNegative is the number negative
   * @param bytes the bytes to read from
   * @param offset the position to start at
   * @param length the number of bytes to read
   * @param scale the scale of the number
   * @param fastResult an object it into
   * @return True if the sign, digits, and scale were successfully converted to a decimal.
   */
  public static boolean fastSetFromDigitsOnlyBytesAndScale(
      boolean isNegative, byte[] bytes, int offset, int length, int scale,
      FastHiveDecimal fastResult) {

    final int bytesLength = bytes.length;

    if (offset < 0 || offset >= bytesLength) {
      return false;
    }
    final int end = offset + length;
    if (end <= offset || end > bytesLength) {
      return false;
    }

    // We start here with at least one byte.
    int index = offset;

    // A stripped down version of fastSetFromBytes.

    int precision = 0;

    // We fill starting with highest digit in highest longword (HIGHWORD_DECIMAL_DIGITS) and
    // move down.  At end will will shift everything down if necessary.

    int longWordIndex = 0;   // Where 0 is the highest longword; 1 is middle longword, etc.

    int digitNum = HIGHWORD_DECIMAL_DIGITS;
    long multiplier = powerOfTenTable[HIGHWORD_DECIMAL_DIGITS - 1];

    int digitValue;
    long longWord = 0;

    long fast0 = 0;
    long fast1 = 0;
    long fast2 = 0;

    byte work;

    // Parse digits.

    boolean haveInteger = false;
    while (true) {
      work = bytes[index];
      if (work < BYTE_DIGIT_ZERO || work > BYTE_DIGIT_NINE) {
        if (!haveInteger) {
          return false;
        }
        break;
      }
      haveInteger = true;
      if (precision == 0 && work == BYTE_DIGIT_ZERO) {
        // Ignore leading zeroes.
        if (++index >= end) {
          break;
        }
        continue;
      }
      digitValue = work - BYTE_DIGIT_ZERO;
      if (digitNum == 0) {

        // Integer parsing move to next lower longword.

        // Save previous longword.
        if (longWordIndex == 0) {
          fast2 = longWord;
        } else if (longWordIndex == 1) {
          fast1 = longWord;
        } else if (longWordIndex == 2) {

          // We have filled HiveDecimal.MAX_PRECISION digits and have no more room in our limit precision
          // fast decimal.
          return false;
        }
        longWordIndex++;

        // The middle and lowest longwords highest digit number is LONGWORD_DECIMAL_DIGITS.
        digitNum = LONGWORD_DECIMAL_DIGITS;
        multiplier = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - 1];
        longWord = 0;
      }
      longWord += digitValue * multiplier;
      multiplier /= 10;
      digitNum--;
      precision++;
      if (++index >= end) {
        break;
      }
    }

    // Just an digits?
    if (index < end) {
      return false;
    }

    if (precision == 0) {
      // We just had leading zeroes.
      // Value is 0.
      return true;
    }

    // Save last longword.
    if (longWordIndex == 0) {
      fast2 = longWord;
    } else if (longWordIndex == 1) {
      fast1 = longWord;
    } else {
      fast0 = longWord;
    }
    fastResult.fastSignum = (isNegative ? -1 : 1);
    fastResult.fastIntegerDigitCount = Math.max(0, precision - scale);
    fastResult.fastScale = scale;
    final int scaleDown = HiveDecimal.MAX_PRECISION - precision;
    if (scaleDown > 0) {
      doFastScaleDown(fast0, fast1, fast2, scaleDown, fastResult);
    } else {
      fastResult.fast0 = fast0;
      fastResult.fast1 = fast1;
      fastResult.fast2 = fast2;
    }
    return true;

  }

  /**
   * Scale down a BigInteger by a power of 10 and round off if necessary using ROUND_HALF_UP.
   * @return The scaled and rounded BigInteger.
   */
  private static BigInteger doBigIntegerScaleDown(BigInteger unscaledValue, int scaleDown) {
    BigInteger[] quotientAndRemainder = unscaledValue.divideAndRemainder(BigInteger.TEN.pow(scaleDown));
    BigInteger quotient = quotientAndRemainder[0];
    BigInteger round = quotientAndRemainder[1].divide(BigInteger.TEN.pow(scaleDown - 1));
    if (round.compareTo(BIG_INTEGER_FIVE) >= 0) {
      quotient = quotient.add(BigInteger.ONE);
    }
    return quotient;
  }

  /**
   * Create a fast decimal from a BigDecimal.
   *
   * NOTE: The fastSetFromBigDecimal method requires the caller to pass a fastResult
   * parameter has been reset for better performance.
   *
   * @param bigDecimal the big decimal to copy
   * @param allowRounding is rounding allowed?
   * @param fastResult an object to reuse
   * @return True if the BigDecimal could be converted to our decimal representation.
   */
  public static boolean fastSetFromBigDecimal(
      BigDecimal bigDecimal, boolean allowRounding, FastHiveDecimal fastResult) {

    // We trim the trailing zero fraction digits so we don't cause unnecessary precision
    // overflow later.
    if (bigDecimal.signum() == 0) {
      if (bigDecimal.scale() != 0) {

        // For some strange reason BigDecimal 0 can have a scale.  We do not support that.
        bigDecimal = BigDecimal.ZERO;
      }
    }

    if (!allowRounding) {
      if (bigDecimal.signum() != 0) {
        BigDecimal bigDecimalStripped = bigDecimal.stripTrailingZeros();
        int stripTrailingZerosScale = bigDecimalStripped.scale();
        // System.out.println("FAST_SET_FROM_BIG_DECIMAL bigDecimal " + bigDecimal);
        // System.out.println("FAST_SET_FROM_BIG_DECIMAL bigDecimalStripped " + bigDecimalStripped);
        // System.out.println("FAST_SET_FROM_BIG_DECIMAL stripTrailingZerosScale " + stripTrailingZerosScale);
        if (stripTrailingZerosScale < 0) {

          // The trailing zeroes extend into the integer part -- we only want to eliminate the
          // fractional zero digits.

          bigDecimal = bigDecimal.setScale(0);
        } else {

          // Ok, use result with some or all fractional digits stripped.

          bigDecimal = bigDecimalStripped;
        }
      }
      int scale = bigDecimal.scale();
      if (scale < 0 || scale > HiveDecimal.MAX_SCALE) {
        return false;
      }
      // The digits must fit without rounding.
      if (!fastSetFromBigInteger(bigDecimal.unscaledValue(), fastResult)) {
        return false;
      }
      if (fastResult.fastSignum != 0) {
        fastResult.fastIntegerDigitCount = Math.max(0, fastResult.fastIntegerDigitCount - scale);
        fastResult.fastScale = scale;
      }
      return true;
    }
    // This method will scale down and round to fit, if necessary.
    if (!fastSetFromBigInteger(bigDecimal.unscaledValue(), bigDecimal.scale(),
        bigDecimal.precision(), fastResult)) {
      return false;
    }
    return true;
  }

  /**
   * Scan a String for a decimal number in UTF-8 characters.
   *
   * NOTE: The fastSetFromString method requires the caller to pass a fastResult
   * parameter has been reset for better performance.
   *
   * @param string the string to parse
   * @param trimBlanks should the blanks be trimmed
   * @param result an object to reuse
   * @return True if the String was successfully converted to a decimal.
   */
  public static boolean fastSetFromString(
      String string, boolean trimBlanks, FastHiveDecimal result) {
    byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
    return fastSetFromBytes(bytes, 0, bytes.length, trimBlanks, result);
  }

  /**
   * Creates a scale 0 fast decimal from an int.
   *
   * NOTE: The fastSetFromString method requires the caller to pass a fastResult
   * parameter has been reset for better performance.
   * @param intValue the value to set
   * @param fastResult an object to reuse
   */
  public static void fastSetFromInt(int intValue, FastHiveDecimal fastResult) {
    if (intValue == 0) {
      // Zero special case.
      return;
    }
    if (intValue > 0) {
      fastResult.fastSignum = 1;
    } else {
      fastResult.fastSignum = -1;
      intValue = Math.abs(intValue);
    }
    // 10 digit int is all in lowest 16 decimal digit longword.
    // Since we are creating with scale 0, no fraction digits to zero trim.
    fastResult.fast0 = intValue & 0xFFFFFFFFL;
    fastResult.fastIntegerDigitCount =
        fastLongWordPrecision(fastResult.fast0);
  }

  /**
   * Creates a scale 0 fast decimal from a long.
   *
   * NOTE: The fastSetFromLong method requires the caller to pass a fastResult
   * parameter has been reset for better performance.
   *
   * @param longValue the value to set
   * @param fastResult an object to reuse
   */
  public static void fastSetFromLong(
      long longValue, FastHiveDecimal fastResult) {
    if (longValue == 0) {
      // Zero special case.
      return;
    }
    // Handle minimum integer case that doesn't have abs().
    if (longValue == Long.MIN_VALUE) {
      // Split -9,223,372,036,854,775,808 into 16 digit middle and lowest longwords by hand.
      fastResult.fastSignum = -1;
      fastResult.fast1 = 922L;
      fastResult.fast0 = 3372036854775808L;
      fastResult.fastIntegerDigitCount = 19;
    } else {
      if (longValue > 0) {
        fastResult.fastSignum = 1;
      } else {
        fastResult.fastSignum = -1;
        longValue = Math.abs(longValue);
      }
      // Split into 16 digit middle and lowest longwords remainder / division.
      fastResult.fast1 = longValue / MULTIPLER_LONGWORD_DECIMAL;
      fastResult.fast0 = longValue % MULTIPLER_LONGWORD_DECIMAL;
      if (fastResult.fast1 != 0) {
        fastResult.fastIntegerDigitCount =
            LONGWORD_DECIMAL_DIGITS + fastLongWordPrecision(fastResult.fast1);
      } else {
        fastResult.fastIntegerDigitCount =
            fastLongWordPrecision(fastResult.fast0);
      }
    }
    return;
  }

  /**
   * Creates a fast decimal from a long with a specified scale.
   *
   * NOTE: The fastSetFromLongAndScale method requires the caller to pass a fastResult
   * parameter has been reset for better performance.
   *
   * @param longValue the value to set as a long
   * @param scale the scale to use
   * @param fastResult an object to reuse
   * @return was the conversion successful?
   */
  public static boolean fastSetFromLongAndScale(
      long longValue, int scale, FastHiveDecimal fastResult) {

    if (scale < 0 || scale > HiveDecimal.MAX_SCALE) {
      return false;
    }

    fastSetFromLong(longValue, fastResult);
    if (scale == 0) {
      return true;
    }

    if (!fastScaleByPowerOfTen(
        fastResult,
        -scale,
        fastResult)) {
      return false;
    }
    return true;
  }

  /**
   * Creates fast decimal from a float.
   *
   * NOTE: The fastSetFromFloat method requires the caller to pass a fastResult
   * parameter has been reset for better performance.
   *
   * @param floatValue the value to set
   * @param fastResult an object to reuse
   * @return was the conversion successful?
   */
  public static boolean fastSetFromFloat(
      float floatValue, FastHiveDecimal fastResult) {

    String floatString = Float.toString(floatValue);
    return fastSetFromString(floatString, false, fastResult);

  }

  /**
   * Creates fast decimal from a double.
   *
   * NOTE: The fastSetFromDouble method requires the caller to pass a fastResult
   * parameter has been reset for better performance.
   *
   * @param doubleValue the value to set
   * @param fastResult an object to reuse
   * @return was the conversion successful?
   */
  public static boolean fastSetFromDouble(
      double doubleValue, FastHiveDecimal fastResult) {

    String doubleString = Double.toString(doubleValue);
    return fastSetFromString(doubleString, false, fastResult);

  }

  /**
   * Creates a fast decimal from a BigInteger with scale 0.
   *
   * For efficiency, we assume that fastResult is fastReset.  This method does not set the
   * fastScale field.
   *
   * NOTE: The fastSetFromBigInteger method requires the caller to pass a fastResult
   * parameter has been reset for better performance.
   *
   * @param bigInteger the value to set
   * @param fastResult an object to reuse
   * @return Return true if the BigInteger value fit within HiveDecimal.MAX_PRECISION.  Otherwise,
   *         false for overflow.
   */
  public static boolean fastSetFromBigInteger(
      BigInteger bigInteger, FastHiveDecimal fastResult) {

    final int signum = bigInteger.signum();
    if (signum == 0) {
      // Zero special case.
      return true;
    }
    fastResult.fastSignum = signum;
    if (signum == -1) {
      bigInteger = bigInteger.negate();
    }
    if (bigInteger.compareTo(BIG_INTEGER_LONGWORD_MULTIPLIER) < 0) {

      // Fits in one longword.
      fastResult.fast0 = bigInteger.longValue();
      if (fastResult.fast0 == 0) {
        fastResult.fastSignum = 0;
      } else {
        fastResult.fastIntegerDigitCount = fastLongWordPrecision(fastResult.fast0);
      }
      return true;
    }
    BigInteger[] quotientAndRemainder =
        bigInteger.divideAndRemainder(BIG_INTEGER_LONGWORD_MULTIPLIER);
    fastResult.fast0 = quotientAndRemainder[1].longValue();
    BigInteger quotient = quotientAndRemainder[0];
    if (quotient.compareTo(BIG_INTEGER_LONGWORD_MULTIPLIER) < 0) {

      // Fits in two longwords.
      fastResult.fast1 = quotient.longValue();
      if (fastResult.fast0 == 0 && fastResult.fast1 == 0) {
        // The special case zero logic at the beginning should have caught this.
        throw new RuntimeException("Unexpected");
      } else {
        fastResult.fastIntegerDigitCount =
            LONGWORD_DECIMAL_DIGITS + fastLongWordPrecision(fastResult.fast1);
      }
      return true;
    }

    // Uses all 3 decimal longs.
    quotientAndRemainder =
        quotient.divideAndRemainder(BIG_INTEGER_LONGWORD_MULTIPLIER);
    fastResult.fast1 = quotientAndRemainder[1].longValue();
    quotient = quotientAndRemainder[0];
    if (quotient.compareTo(BIG_INTEGER_HIGHWORD_MULTIPLIER) >= 0) {
      // Overflow.
      return false;
    }
    fastResult.fast2 = quotient.longValue();
    if (fastResult.fast0 == 0 && fastResult.fast1 == 0 && fastResult.fast2 == 0) {
      fastResult.fastSignum = 0;
    } else {
      fastResult.fastIntegerDigitCount =
          TWO_X_LONGWORD_DECIMAL_DIGITS + fastHighWordPrecision(fastResult.fast2);
    }
    return true;
  }

  /**
   * Creates a fast decimal from a BigInteger with a specified scale.
   *
   * NOTE: The fastSetFromBigInteger method requires the caller to pass a fastResult
   * parameter has been reset for better performance.
   *
   * @param bigInteger the value to set as an integer
   * @param scale the scale to use
   * @param fastResult an object to reuse
   * @return True if the BigInteger and scale were successfully converted to a decimal.
   */
  public static boolean fastSetFromBigInteger(
      BigInteger bigInteger, int scale, FastHiveDecimal fastResult) {
    // Poor performance, because the precision will be calculated by bigInteger.toString()
    return fastSetFromBigInteger(bigInteger, scale, -1, fastResult);
  }

  /**
   * Creates a fast decimal from a BigInteger with a specified scale.
   *
   * NOTE: The fastSetFromBigInteger method requires the caller to pass a fastResult
   * parameter has been reset for better performance.
   *
   * @param bigInteger the value to set as an integer
   * @param scale the scale to use
   * @param precision the precision to use
   * @param fastResult an object to reuse
   * @return True if the BigInteger and scale were successfully converted to a decimal.
   */
  public static boolean fastSetFromBigInteger(
      BigInteger bigInteger, int scale, int precision, FastHiveDecimal fastResult) {

    if (scale < 0) {

      // Multiply by 10^(-scale) to normalize.  We do not use negative scale in our representation.
      //
      // Example:
      //    4.172529E+20 has a negative scale -20 since scale is number of digits below the dot.
      //    417252900000000000000 normalized as scale 0.
      //
      bigInteger = bigInteger.multiply(BIG_INTEGER_TEN.pow(-scale));
      scale = 0;
    }

    int signum = bigInteger.signum();
    if (signum == 0) {
      // Zero.
      return true;
    } else if (signum == -1) {
      // Normalize to positive.
      bigInteger = bigInteger.negate();
    }

    if (precision < 0) {
      // A slow way to get the number of decimal digits.
      precision = bigInteger.toString().length();
    }

    // System.out.println("FAST_SET_FROM_BIG_INTEGER adjusted bigInteger " + bigInteger + " precision " + precision);

    int integerDigitCount = precision - scale;
    // System.out.println("FAST_SET_FROM_BIG_INTEGER integerDigitCount " + integerDigitCount + " scale " + scale);
    int maxScale;
    if (integerDigitCount >= 0) {
      if (integerDigitCount > HiveDecimal.MAX_PRECISION) {
        return false;
      }
      maxScale = HiveDecimal.MAX_SCALE - integerDigitCount;
    } else {
      maxScale = HiveDecimal.MAX_SCALE;
    }
    // System.out.println("FAST_SET_FROM_BIG_INTEGER maxScale " + maxScale);

    if (scale > maxScale) {

      // A larger scale is ok -- we will knock off lower digits and round.

      final int trimAwayCount = scale - maxScale;
      // System.out.println("FAST_SET_FROM_BIG_INTEGER trimAwayCount " + trimAwayCount);
      if (trimAwayCount > 1) {
        // First, throw away digits below round digit.
        BigInteger bigIntegerThrowAwayBelowRoundDigitDivisor = BIG_INTEGER_TEN.pow(trimAwayCount - 1);
        bigInteger = bigInteger.divide(bigIntegerThrowAwayBelowRoundDigitDivisor);
      }
      // System.out.println("FAST_SET_FROM_BIG_INTEGER with round digit bigInteger " + bigInteger + " length " + bigInteger.toString().length());

      BigInteger[] quotientAndRemainder = bigInteger.divideAndRemainder(BIG_INTEGER_TEN);
      // System.out.println("FAST_SET_FROM_BIG_INTEGER quotientAndRemainder " + Arrays.toString(quotientAndRemainder));

      BigInteger quotient = quotientAndRemainder[0];
      if (quotientAndRemainder[1].intValue() >= 5) {
        if (quotient.equals(BIG_INTEGER_MAX_DECIMAL)) {

          // 38 9's digits.
          // System.out.println("FAST_SET_FROM_BIG_INTEGER quotient is BIG_INTEGER_MAX_DECIMAL");

          if (maxScale == 0) {
            // No room above for rounding.
            return false;
          }

          // System.out.println("FAST_SET_FROM_BIG_INTEGER reached here... scale " + scale + " maxScale " + maxScale);
          // Rounding results in 10^N.
          bigInteger = BIG_INTEGER_TEN.pow(integerDigitCount);
          maxScale = 0;
        } else {

          // Round up.
          bigInteger = quotient.add(BigInteger.ONE);
        }
      } else {

        // No rounding.
        bigInteger = quotient;
      }
      scale = maxScale;
    }
    if (!fastSetFromBigInteger(bigInteger, fastResult)) {
      return false;
    }

    if (fastResult.fast0 == 0 && fastResult.fast1 == 0 && fastResult.fast2 == 0) {
      fastResult.fastSignum = 0;
    } else {
      fastResult.fastSignum = signum;
      fastResult.fastIntegerDigitCount = Math.max(0, fastResult.fastIntegerDigitCount - scale);
      fastResult.fastScale = scale;

      final int trailingZeroCount =
          fastTrailingDecimalZeroCount(
              fastResult.fast0, fastResult.fast1, fastResult.fast2,
              fastResult.fastIntegerDigitCount, scale);
      if (trailingZeroCount > 0) {
        doFastScaleDown(
            fastResult,
            trailingZeroCount,
            fastResult);
        fastResult.fastScale -= trailingZeroCount;
      }
    }

    return true;
  }

  //************************************************************************************************
  // Take Integer or Fractional Portion.

  /**
   * Creates fast decimal from the fraction portion of a fast decimal.
   *
   * NOTE: The fastFractionPortion method requires the caller to pass a fastResult
   * parameter has been reset for better performance.
   *
   * @param fastSignum the sign of the number (1, 0, or -1)
   * @param fast0 high bits
   * @param fast1 second word bits
   * @param fast2 third word bits
   * @param fastScale the scale
   * @param fastResult an object to reuse
   */
  public static void fastFractionPortion(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastScale,
      FastHiveDecimal fastResult) {

    if (fastSignum == 0 || fastScale == 0) {
      fastResult.fastReset();
      return;
    }

    // Clear integer portion; keep fraction.

    // Adjust all longs using power 10 division/remainder.
    long result0;
    long result1;
    long result2;
    if (fastScale < LONGWORD_DECIMAL_DIGITS) {

      // Part of lowest word survives.

      final long clearFactor = powerOfTenTable[fastScale];

      result0 = fast0 % clearFactor;
      result1 = 0;
      result2 = 0;

    } else if (fastScale < TWO_X_LONGWORD_DECIMAL_DIGITS) {

      // Throw away lowest word.

      final int adjustedScaleDown = fastScale - LONGWORD_DECIMAL_DIGITS;

      final long clearFactor = powerOfTenTable[adjustedScaleDown];

      result0 = fast0;
      result1 = fast1 % clearFactor;
      result2 = 0;

    } else {

      // Throw away middle and lowest words.

      final int adjustedScaleDown = fastScale - 2*LONGWORD_DECIMAL_DIGITS;

      final long clearFactor = powerOfTenTable[adjustedScaleDown];

      result0 = fast0;
      result1 = fast1;
      result2 = fast2 % clearFactor;

    }
    if (result0 == 0 && result1 == 0 && result2 == 0) {
      fastResult.fastReset();
    } else {
      fastResult.fastSet(fastSignum, result0, result1, result2, /* fastIntegerDigitCount */ 0, fastScale);
    }
  }

  /**
   * Creates fast decimal from the integer portion.
   *
   * NOTE: The fastFractionPortion method requires the caller to pass a fastResult
   * parameter has been reset for better performance.
   *
   * @param fastSignum the sign of the number (1, 0, or -1)
   * @param fast0 high bits
   * @param fast1 second word bits
   * @param fast2 third word bits
   * @param fastIntegerDigitCount the number of integer digits
   * @param fastScale the scale
   * @param fastResult an object to reuse
   */
  public static void fastIntegerPortion(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale,
      FastHiveDecimal fastResult) {

    if (fastSignum == 0) {
      fastResult.fastReset();
      return;
    }
    if (fastScale == 0) {
      fastResult.fastSet(fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale);
    }

    // Scale down no rounding to clear fraction.
    fastResult.fastSignum = fastSignum;
    doFastScaleDown(
        fast0, fast1, fast2,
        fastScale,
        fastResult);
    fastResult.fastIntegerDigitCount = fastIntegerDigitCount;
    fastResult.fastScale = 0;
  }

  //************************************************************************************************
  // Binary to Decimal Conversion.

  /**
   * Convert 3 binary words of N bits each to a fast decimal (scale 0).
   *
   * The 3 binary words highWord, middleWord, and lowerWord form a large binary value:
   *
   *    highWord * 2^(M+L) + middleWord * 2^L + lowerWord.
   *
   * Where L is the number of bits in the lower word; M is the number of bits in the middle word.
   * We let L and M be different to support the SerializationUtil serialization where the lower
   * word is 62 bits and the remaining words are 63 bits...
   *
   * @param lowerWord the lower internal representation
   * @param middleWord the middle internal representation
   * @param highWord the high internal representation
   * @param middleWordMultiplier 2^L
   * @param highWordMultiplier 2^(M+L)
   * @param fastResult an object to reuse
   * @return True if the conversion of the 3 binary words to decimal was successful.
   */
  public static boolean doBinaryToDecimalConversion(
      long lowerWord, long middleWord, long highWord,
      FastHiveDecimal middleWordMultiplier,
      FastHiveDecimal highWordMultiplier,
      FastHiveDecimal fastResult) {

    /*
     * Challenge: How to do the math to get this raw binary back to our decimal form.
     *
     * Briefly, for the middle and upper binary words, convert the middle/upper word into a decimal
     * long words and then multiply those by the binary word's power of 2.
     *
     * And, add the multiply results into the result decimal longwords.
     *
     */
    long result0 =
        lowerWord % MULTIPLER_LONGWORD_DECIMAL;
    long result1 =
        lowerWord / MULTIPLER_LONGWORD_DECIMAL;
    long result2 = 0;

    if (middleWord != 0 || highWord != 0) {

      if (highWord == 0) {
  
        // Form result from lower and middle words.

        if (!fastMultiply5x5HalfWords(
            middleWord % MULTIPLER_LONGWORD_DECIMAL,
            middleWord / MULTIPLER_LONGWORD_DECIMAL,
            0,
            middleWordMultiplier.fast0, middleWordMultiplier.fast1, middleWordMultiplier.fast2,
            fastResult)) {
          return false;
        }

        final long calc0 =
            result0
          + fastResult.fast0;
        result0 =
            calc0 % MULTIPLER_LONGWORD_DECIMAL;
        final long calc1 =
            calc0 / MULTIPLER_LONGWORD_DECIMAL
          + result1
          + fastResult.fast1;
        result1 =
            calc1 % MULTIPLER_LONGWORD_DECIMAL;
        result2 =
            calc1 / MULTIPLER_LONGWORD_DECIMAL
          + fastResult.fast2;

      } else if (middleWord == 0) {

        // Form result from lower and high words.

        if (!fastMultiply5x5HalfWords(
            highWord % MULTIPLER_LONGWORD_DECIMAL,
            highWord / MULTIPLER_LONGWORD_DECIMAL,
            0,
            highWordMultiplier.fast0, highWordMultiplier.fast1, highWordMultiplier.fast2,
            fastResult)) {
          return false;
        }

        final long calc0 =
            result0
          + fastResult.fast0;
        result0 =
            calc0 % MULTIPLER_LONGWORD_DECIMAL;
        final long calc1 =
            calc0 / MULTIPLER_LONGWORD_DECIMAL
          + result1
          + fastResult.fast1;
        result1 =
            calc1 % MULTIPLER_LONGWORD_DECIMAL;
        result2 =
            calc1 / MULTIPLER_LONGWORD_DECIMAL
          + fastResult.fast2;

      } else {

        // Form result from lower, middle, and middle words.

        if (!fastMultiply5x5HalfWords(
            middleWord % MULTIPLER_LONGWORD_DECIMAL,
            middleWord / MULTIPLER_LONGWORD_DECIMAL,
            0,
            middleWordMultiplier.fast0, middleWordMultiplier.fast1, middleWordMultiplier.fast2,
            fastResult)) {
          return false;
        }

        long middleResult0 = fastResult.fast0;
        long middleResult1 = fastResult.fast1;
        long middleResult2 = fastResult.fast2;

        if (!fastMultiply5x5HalfWords(
            highWord % MULTIPLER_LONGWORD_DECIMAL,
            highWord / MULTIPLER_LONGWORD_DECIMAL,
            0,
            highWordMultiplier.fast0, highWordMultiplier.fast1, highWordMultiplier.fast2,
            fastResult)) {
          return false;
        }

        long calc0 =
            result0
          + middleResult0
          + fastResult.fast0;
        result0 =
            calc0 % MULTIPLER_LONGWORD_DECIMAL;
        long calc1 =
            calc0 / MULTIPLER_LONGWORD_DECIMAL
          + result1
          + middleResult1
          + fastResult.fast1;
        result1 =
            calc1 % MULTIPLER_LONGWORD_DECIMAL;
        result2 =
            calc1 / MULTIPLER_LONGWORD_DECIMAL
          + middleResult2
          + fastResult.fast2;
      }
    }

    // Let caller set negative sign if necessary.
    if (result2 != 0) {
      fastResult.fastIntegerDigitCount = TWO_X_LONGWORD_DECIMAL_DIGITS + fastHighWordPrecision(result2);
      fastResult.fastSignum = 1;
    } else if (result1 != 0) {
      fastResult.fastIntegerDigitCount = LONGWORD_DECIMAL_DIGITS + fastHighWordPrecision(result1);
      fastResult.fastSignum = 1;
    } else if (result0 != 0) {
      fastResult.fastIntegerDigitCount = fastHighWordPrecision(result0);
      fastResult.fastSignum = 1;
    } else {
      fastResult.fastIntegerDigitCount = 0;
      fastResult.fastSignum = 0;
    }

    fastResult.fast0 = result0;
    fastResult.fast1 = result1;
    fastResult.fast2 = result2;

    return true;
  }

  //************************************************************************************************
  // Decimal to Binary Conversion.

  /**
   * A helper method that produces a single binary word remainder from a fast decimal (and
   * quotient).
   *
   * The fast decimal is longwords of 16 digits each and we need binary words of 2^N.  Since
   * we are in decimal form, we have do work to get to convert to binary form.
   *
   * We effectively need to produce on big binary value (i.e. greater than 64 bits since
   * HiveDecimal needs 128 bits of binary which Java does not provide primitive support for)
   * from the decimal long words and get the lower N binary bit remainder.
   *
   * We could try and do decimal division by 2^N to get the (integer) quotient, multiply the
   * quotient by 2^N decimal, and finally do a decimal subtract that from the original decimal.
   * The resulting decimal can be used to easily get the binary remainder.
   *
   * However, currently, we do not have fast decimal division.
   *
   * The "trick" we do here is to remember from your Algebra in school than multiplication and
   * division are inverses of each other.
   *
   * So instead of doing decimal division by 2^N we multiply by the inverse: 2^-N.
   *
   * We produce 1 binary word (remainder) and a decimal quotient for the higher portion.
   *
   * @param dividendFast0 The input decimal that will produce a
   *                      single binary word remainder and decimal quotient.
   * @param dividendFast1 second word
   * @param dividendFast2 third word
   * @param fastInverseConst the fast decimal inverse of 2^N = 2^-N
   * @param quotientIntegerWordNum the word in the inverse multiplication result
   *                               to find the quotient integer decimal portion
   * @param quotientIntegerDigitNum the digit in the result to find the quotient
   *                                integer decimal portion
   * @param fastMultiplierConst The fast decimal multiplier for converting the
   *                            quotient integer to the larger number to
   *                            subtract from the input decimal to get the
   *                            remainder.
   * @param scratchLongs where to store the result remainder word (index 3) and
   *                     result quotient decimal longwords (indices 0 .. 2)
   * @return True if the results were produced without overflow.
   */
  public static boolean doDecimalToBinaryDivisionRemainder(
      long dividendFast0, long dividendFast1, long dividendFast2,
      FastHiveDecimal fastInverseConst,
      int quotientIntegerWordNum,
      int quotientIntegerDigitNum,
      FastHiveDecimal fastMultiplierConst,
      long[] scratchLongs) {

    // Multiply by inverse (2^-N) to do the 2^N division.
    if (!fastMultiply5x6HalfWords(
        dividendFast0, dividendFast1, dividendFast2,
        fastInverseConst.fast0, fastInverseConst.fast1, fastInverseConst.fast2,
        scratchLongs)) {
      // Overflow.
      return false;
    }

    final long divideFactor = powerOfTenTable[quotientIntegerDigitNum];
    final long multiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - quotientIntegerDigitNum];

    // Extract the integer portion to get the quotient.
    long quotientFast0 =
        scratchLongs[quotientIntegerWordNum] / divideFactor
      + ((scratchLongs[quotientIntegerWordNum + 1] % divideFactor) * multiplyFactor);
    long quotientFast1 =
        scratchLongs[quotientIntegerWordNum + 1] / divideFactor
      + ((scratchLongs[quotientIntegerWordNum + 2] % divideFactor) * multiplyFactor);
    long quotientFast2 =
        scratchLongs[quotientIntegerWordNum + 2] / divideFactor;

    // Multiply the integer quotient back out so we can subtract it from the original to get
    // the remainder.
    if (!fastMultiply5x6HalfWords(
        quotientFast0, quotientFast1, quotientFast2,
        fastMultiplierConst.fast0, fastMultiplierConst.fast1, fastMultiplierConst.fast2,
        scratchLongs)) {
      return false;
    }

    long quotientMultiplied0 = scratchLongs[0];
    long quotientMultiplied1 = scratchLongs[1];
    long quotientMultiplied2 = scratchLongs[2];

    if (!doSubtractSameScaleNoUnderflow(
        dividendFast0, dividendFast1, dividendFast2,
        quotientMultiplied0, quotientMultiplied1, quotientMultiplied2,
        scratchLongs)) {
      // Underflow.
      return false;
    }

    long remainderBinaryWord =
        scratchLongs[1] * MULTIPLER_LONGWORD_DECIMAL
      + scratchLongs[0];

    // Pack the output into the scratch longs.
    scratchLongs[0] = quotientFast0;
    scratchLongs[1] = quotientFast1;
    scratchLongs[2] = quotientFast2;

    scratchLongs[3] = remainderBinaryWord;

    return true;
  }

  /**
   * Convert a fast decimal into 3 binary words of N bits each.
   * 
   * The 3 binary words will form a large binary value that is the unsigned unscaled decimal value:
   *
   *    highWord * 2^(M+L) + middleWord * 2^L + lowerWord.
   *
   * Where L is the number of bits in the lower word; M is the number of bits in the middle word.
   * We let L and M be different to support the SerializationUtil serialization where the lower
   * word is 62 bits and the remaining words are 63 bits...
   *
   * The fast decimal is longwords of 16 digits each and we need binary words of 2^N.  Since
   * we are in decimal form, we have do work to get to convert to binary form.
   *
   * See the comments for doDecimalToBinaryDivisionRemainder for details on the parameters.
   *
   * The lowerWord is produced by calling doDecimalToBinaryDivisionRemainder.  The quotient from
   * that is passed to doDecimalToBinaryDivisionRemainder to produce the middleWord.  The final
   * quotient is used to produce the highWord.
   *
   * @return True if the 3 binary words were produced without overflow.  Overflow is not expected.
   */
  private static boolean doDecimalToBinaryConversion(
      long fast0, long fast1, long fast2,
      FastHiveDecimal fastInverseConst,
      int quotientIntegerWordNum,
      int quotientIntegerDigitNum,
      FastHiveDecimal fastMultiplierConst,
      long[] scratchLongs) {

    long lowerBinaryWord;
    long middleBinaryWord = 0;
    long highBinaryWord = 0;

    if (fastCompareTo(
            1,
            fast0, fast1, fast2, 0,
            1,
            fastMultiplierConst.fast0, fastMultiplierConst.fast1, fastMultiplierConst.fast2, 0) < 0) {

      // Optimize: whole decimal fits in one binary word.

      lowerBinaryWord =
          fast1 * MULTIPLER_LONGWORD_DECIMAL
        + fast0;

    } else {

      // Do division/remainder to get lower binary word; quotient will either be middle decimal
      // or be both high and middle decimal that requires another division/remainder.

      if (!doDecimalToBinaryDivisionRemainder(
          fast0, fast1, fast2,
          fastInverseConst,
          quotientIntegerWordNum,
          quotientIntegerDigitNum,
          fastMultiplierConst,
          scratchLongs)) {
        // Overflow.
        return false;
      }

      // Unpack the output.
      long quotientFast0 = scratchLongs[0];
      long quotientFast1 = scratchLongs[1];
      long quotientFast2 = scratchLongs[2];

      lowerBinaryWord = scratchLongs[3];

      if (fastCompareTo(
          1,
          quotientFast0, quotientFast1, quotientFast2, 0,
          1,
          fastMultiplierConst.fast0, fastMultiplierConst.fast1, fastMultiplierConst.fast2, 0) < 0) {

        // Optimize: whole decimal fits in two binary words.

        middleBinaryWord =
            quotientFast1 * MULTIPLER_LONGWORD_DECIMAL
          + quotientFast0;

      } else {
        if (!doDecimalToBinaryDivisionRemainder(
            quotientFast0, quotientFast1, quotientFast2,
            fastInverseConst,
            quotientIntegerWordNum,
            quotientIntegerDigitNum,
            fastMultiplierConst,
            scratchLongs)) {
          // Overflow.
          return false;
        }

        highBinaryWord =
            scratchLongs[1] * MULTIPLER_LONGWORD_DECIMAL
          + scratchLongs[0];

        middleBinaryWord = scratchLongs[3];

      }
    }

    scratchLongs[0] = lowerBinaryWord;
    scratchLongs[1] = middleBinaryWord;
    scratchLongs[2] = highBinaryWord;

    return true;
  }

  //************************************************************************************************
  // Emulate SerializationUtils Deserialization used by ORC.

  /*
   * fastSerializationUtilsRead lower word is 62 bits (the lower bit is used as the sign and is
   * removed).  So, we need a multiplier 2^62
   *
   *    2^62 =
   *      4611686018427387904 or
   *      4,611,686,018,427,387,904 or
   *      461,1686018427387904 (16 digit comma'd)
   */
  private static final FastHiveDecimal FAST_HIVE_DECIMAL_TWO_POWER_62 =
      new FastHiveDecimal(1, 1686018427387904L, 461L, 0, 19, 0);

  /*
   * fastSerializationUtilsRead middle word is 63 bits. So, we need a multiplier 2^63 
   *
   *    2^63 =
   *      9223372036854775808 (Long.MAX_VALUE) or
   *      9,223,372,036,854,775,808 or
   *      922,3372036854775808 (16 digit comma'd)
   */
  private static final FastHiveDecimal FAST_HIVE_DECIMAL_TWO_POWER_63 =
      new FastHiveDecimal(1, 3372036854775808L, 922L, 0, 19, 0);

  /*
   * fastSerializationUtilsRead high word multiplier:
   *
   *    Multiply by 2^(62 + 63)                      -- 38 digits or 3 decimal words.
   *
   *    (2^62)*(2^63) =
   *      42535295865117307932921825928971026432 or
   *     (12345678901234567890123456789012345678)
   *     (         1         2         3        )
   *      42,535,295,865,117,307,932,921,825,928,971,026,432 or
   *      425352,9586511730793292,1825928971026432  (16 digit comma'd)
   */
  private static final FastHiveDecimal FAST_HIVE_DECIMAL_TWO_POWER_125 =
      new FastHiveDecimal(1, 1825928971026432L, 9586511730793292L, 425352L, 38, 0);

  /*
   * Inverse of 2^63 = 2^-63.  Please see comments for doDecimalToBinaryDivisionRemainder.
   *
   * Multiply by 1/2^63 = 1.08420217248550443400745280086994171142578125e-19 to divide by 2^63.
   * As 16 digit comma'd 1084202172485,5044340074528008,6994171142578125
   *
   * Scale down: 63 = 44 fraction digits + 19 (negative exponent or number of zeros after dot).
   *
   * 3*16 (48) + 15 --> 63 down shift.
   */
  private static final FastHiveDecimal FAST_HIVE_DECIMAL_TWO_POWER_63_INVERSE =
      new FastHiveDecimal(1, 6994171142578125L, 5044340074528008L, 1084202172485L, 45, 0);

  /*
   * Where in the inverse multiplication result to find the quotient integer decimal portion.
   *
   * Please see comments for doDecimalToBinaryDivisionRemainder.
   */
  private static final int SERIALIZATION_UTILS_WRITE_QUOTIENT_INTEGER_WORD_NUM = 3;
  private static final int SERIALIZATION_UTILS_WRITE_QUOTIENT_INTEGER_DIGIT_NUM = 15;

  /**
   * Deserialize data written in the format used by the SerializationUtils methods
   * readBigInteger/writeBigInteger and create a decimal using the supplied scale.
   *
   * ORC uses those SerializationUtils methods for its serialization.
   *
   * A scratch bytes array is necessary to do the binary to decimal conversion for better
   * performance.  Pass a FAST_SCRATCH_BUFFER_LEN_SERIALIZATION_UTILS_READ byte array for
   * scratchBytes.
   *
   * @param inputStream the stream to read from
   * @param scale the scale of the number
   * @param scratchBytes  An array for the binary to decimal conversion for better
   *                      performance.  Must have length of
   *                      FAST_SCRATCH_BUFFER_LEN_SERIALIZATION_UTILS_READ.
   * @param fastResult an object to reuse
   * @return The deserialized decimal or null if the conversion failed.
   * @throws IOException failures in reading the stream
   */
  public static boolean fastSerializationUtilsRead(InputStream inputStream, int scale,
      byte[] scratchBytes,
      FastHiveDecimal fastResult) throws IOException {

    // Following a suggestion from Gopal, quickly read in the bytes from the stream.
    // CONSIDER: Have ORC read the whole input stream into a big byte array with one call to
    // the read(byte[] b, int off, int len) method and then let this method read from the big
    // byte array.
    int readCount = 0;
    int input;
    do {
      input = inputStream.read();
      if (input == -1) {
        throw new EOFException("Reading BigInteger past EOF from " + inputStream);
      }
      scratchBytes[readCount++] = (byte) input;
    } while (input >= 0x80);

    /*
     * Determine the 3 binary words like what SerializationUtils.readBigInteger does.
     */

    long lowerWord63 = 0;
    long middleWord63 = 0;
    long highWord63 = 0;

    long work = 0;
    int offset = 0;
    int readIndex = 0;
    long b;
    do {
      b = scratchBytes[readIndex++];
      work |= (0x7f & b) << (offset % 63);
      offset += 7;
      // if we've read 63 bits, roll them into the result
      if (offset == 63) {
        lowerWord63 = work;
        work = 0;
      } else if (offset % 63 == 0) {
        if (offset == 126) {
          middleWord63 = work;
        } else if (offset == 189) {
          highWord63 = work;
        } else {
          throw new EOFException("Reading more than 3 words of BigInteger");
        }
        work = 0;
      }
    } while (readIndex < readCount);

    if (work != 0) {
      if (offset < 63) {
        lowerWord63 = work;
      } else if (offset < 126) {
        middleWord63 = work;
      } else if (offset < 189) {
        highWord63 =work;
      } else {
        throw new EOFException("Reading more than 3 words of BigInteger");
      }
    }

    // Grab sign bit and shift it away.
    boolean isNegative = ((lowerWord63 & 0x1) != 0);
    lowerWord63 >>= 1;

    /*
     * Use common binary to decimal conversion method we share with fastSetFromBigIntegerBytes.
     */
    if (!doBinaryToDecimalConversion(
            lowerWord63, middleWord63, highWord63,
            FAST_HIVE_DECIMAL_TWO_POWER_62,
            FAST_HIVE_DECIMAL_TWO_POWER_125,    // 2^(62 + 63)
            fastResult)) {
      return false;
    }

    if (isNegative) {

      // Adjust negative result, again doing what SerializationUtils.readBigInteger does.
      if (!doAddSameScaleSameSign(
          /* resultSignum */ 1,
          fastResult.fast0, fastResult.fast1, fastResult.fast2,
          1, 0, 0,
          fastResult)) {
        return false;
      }
    }

    if (fastResult.fast0 == 0 && fastResult.fast1 == 0 && fastResult.fast2 == 0) {
      fastResult.fastSignum = 0;
    } else {
      fastResult.fastSignum = (isNegative ? -1 : 1);
      final int rawPrecision = fastRawPrecision(fastResult);
      fastResult.fastIntegerDigitCount = Math.max(0, rawPrecision - scale);
      fastResult.fastScale = scale;

      /*
       * Just in case we deserialize a decimal with trailing zeroes...
       */
      final int resultTrailingZeroCount =
          fastTrailingDecimalZeroCount(
              fastResult.fast0, fastResult.fast1, fastResult.fast2,
              fastResult.fastIntegerDigitCount, fastResult.fastScale);
      if (resultTrailingZeroCount > 0) {
        doFastScaleDown(
            fastResult,
            resultTrailingZeroCount,
            fastResult);

        fastResult.fastScale -= resultTrailingZeroCount;
      }
    }

    return true;
  }

  //************************************************************************************************
  // Emulate SerializationUtils Serialization used by ORC.

  /**
   * Write the value of this decimal just like SerializationUtils.writeBigInteger.  It header
   * comments are:
   *
   *     Write the arbitrarily sized signed BigInteger in vint format.
   *
   *     Signed integers are encoded using the low bit as the sign bit using zigzag
   *     encoding.
   *
   *     Each byte uses the low 7 bits for data and the high bit for stop/continue.
   *
   *     Bytes are stored LSB first.
   *
   * NOTE:
   *    SerializationUtils.writeBigInteger sometimes pads the result with extra zeroes due to
   *    BigInteger.bitLength -- we do not emulate that.  SerializationUtils.readBigInteger will
   *    produce the same result for both.
   *
   * @param outputStream the stream to write to
   * @param fastSignum the sign digit (-1, 0, or +1)
   * @param fast0 word 0 of the internal representation
   * @param fast1 word 1 of the internal representation
   * @param fast2 word 2 of the internal representation
   * @param fastIntegerDigitCount unused
   * @param fastScale unused
   * @param scratchLongs scratch space
   * @return True if the decimal was successfully serialized into the output stream.
   * @throws IOException for problems in writing
   */
  public static boolean fastSerializationUtilsWrite(OutputStream outputStream,
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale,
      long[] scratchLongs)
          throws IOException {

    boolean isNegative = (fastSignum == -1);

    /*
     * The sign is encoded as the least significant bit.
     *
     * We need to adjust our decimal before conversion to binary.
     *
     * Positive:
     *   Multiply by 2.
     *
     * Negative:
     *   Logic in SerializationUtils.writeBigInteger does a negate on the BigInteger. We
     *   do not have to since FastHiveDecimal stores the numbers unsigned in fast0, fast1,
     *   and fast2.  We do need to subtract one though.
     *
     *   And then multiply by 2 and add in the 1 sign bit.
     *
     *   CONSIDER: This could be combined.
     */
    long adjust0;
    long adjust1;
    long adjust2;

    if (isNegative) {

      // Subtract 1.
      long r0 = fast0 - 1;
      long r1;
      if (r0 < 0) {
        adjust0 = r0 + MULTIPLER_LONGWORD_DECIMAL;
        r1 = fast1 - 1;
      } else {
        adjust0 = r0;
        r1 = fast1;
      }
      if (r1 < 0) {
        adjust1 = r1 + MULTIPLER_LONGWORD_DECIMAL;
        adjust2 = fast2 - 1;
      } else {
        adjust1 = r1;
        adjust2 = fast2;
      }
      if (adjust2 < 0) {
        return false;
      }

      // Now multiply by 2 and add 1 sign bit.
      r0 = adjust0 * 2 + 1;
      adjust0 =
          r0 % MULTIPLER_LONGWORD_DECIMAL;
      r1 =
          adjust1 * 2
        + r0 / MULTIPLER_LONGWORD_DECIMAL;
      adjust1 =
          r1 % MULTIPLER_LONGWORD_DECIMAL;
      adjust2 =
          adjust2 * 2
        + r1 / MULTIPLER_LONGWORD_DECIMAL;

    } else {

      // Multiply by 2 to make room for 0 sign bit.
      long r0 = fast0 * 2;
      adjust0 =
          r0 % MULTIPLER_LONGWORD_DECIMAL;
      final long r1 =
          fast1 * 2
        + r0 / MULTIPLER_LONGWORD_DECIMAL;
      adjust1 =
          r1 % MULTIPLER_LONGWORD_DECIMAL;
      adjust2 =
          fast2 * 2
        + r1 / MULTIPLER_LONGWORD_DECIMAL;

    }

    /*
     * Use common decimal to binary conversion method we share with fastBigIntegerBytes.
     */
    if (!doDecimalToBinaryConversion(
        adjust0, adjust1, adjust2,
        FAST_HIVE_DECIMAL_TWO_POWER_63_INVERSE,
        SERIALIZATION_UTILS_WRITE_QUOTIENT_INTEGER_WORD_NUM,
        SERIALIZATION_UTILS_WRITE_QUOTIENT_INTEGER_DIGIT_NUM,
        FAST_HIVE_DECIMAL_TWO_POWER_63,
        scratchLongs)) {
      // Overflow.
      return false;
    }

    long lowerWord63 = scratchLongs[0];
    long middleWord63 = scratchLongs[1];
    long highWord63 = scratchLongs[2];

    int wordCount;
    if (highWord63 != 0) {
      wordCount = 3;
    } else if (middleWord63 != 0) {
      wordCount = 2;
    } else {
      wordCount = 1;
    }

    // Write out the first 63 bits worth of data.
    long lowBits = lowerWord63;
    for(int i=0; i < 9; ++i) {
      // If this is the last byte, leave the high bit off
      if (wordCount == 1 && (lowBits & ~0x7f) == 0) {
        outputStream.write((byte) lowBits);
        return true;
      } else {
        outputStream.write((byte) (0x80 | (lowBits & 0x7f)));
        lowBits >>>= 7;
      }
    }
    if (wordCount <= 1) {
      throw new RuntimeException("Expecting write word count > 1");
    }

    lowBits = middleWord63;
    for(int i=0; i < 9; ++i) {
      // If this is the last byte, leave the high bit off
      if (wordCount == 2 && (lowBits & ~0x7f) == 0) {
        outputStream.write((byte) lowBits);
        return true;
      } else {
        outputStream.write((byte) (0x80 | (lowBits & 0x7f)));
        lowBits >>>= 7;
      }
    }

    lowBits = highWord63;
    for(int i=0; i < 9; ++i) {
      // If this is the last byte, leave the high bit off
      if ((lowBits & ~0x7f) == 0) {
        outputStream.write((byte) lowBits);
        return true;
      } else {
        outputStream.write((byte) (0x80 | (lowBits & 0x7f)));
        lowBits >>>= 7;
      }
    }

    // Should not get here.
    throw new RuntimeException("Unexpected");
  }

  public static long getDecimal64AbsMax(int precision) {
    return powerOfTenTable[precision] - 1;
  }

  /*
   * Deserializes 64-bit decimals up to the maximum 64-bit precision (18 decimal digits).
   *
   * NOTE: Major assumption: the input decimal64 has already been bounds checked and a least
   * has a precision <= DECIMAL64_DECIMAL_DIGITS.  We do not bounds check here for better
   * performance.
   */
  public static void fastDeserialize64(
      final long inputDecimal64Long, final int inputScale,
      FastHiveDecimal fastResult) {

    long decimal64Long;
    if (inputDecimal64Long == 0) {
      fastResult.fastReset();
      return;
    } else if (inputDecimal64Long > 0) {
      fastResult.fastSignum = 1;
      decimal64Long = inputDecimal64Long;
    } else {
      fastResult.fastSignum = -1;
      decimal64Long = -inputDecimal64Long;
    }

    // Trim trailing zeroes -- but only below the decimal point.
    int trimScale = inputScale;
    while (trimScale > 0 && decimal64Long % 10 == 0) {
      decimal64Long /= 10;
      trimScale--;
    }

    fastResult.fast2 = 0;
    fastResult.fast1 = decimal64Long / MULTIPLER_LONGWORD_DECIMAL;
    fastResult.fast0 = decimal64Long % MULTIPLER_LONGWORD_DECIMAL;

    fastResult.fastScale = trimScale;

    fastResult.fastIntegerDigitCount =
        Math.max(0, fastRawPrecision(fastResult) - fastResult.fastScale);
  }

  /*
   * Serializes decimal64 up to the maximum 64-bit precision (18 decimal digits).
   *
   * NOTE: Major assumption: the fast decimal has already been bounds checked and a least
   * has a precision <= DECIMAL64_DECIMAL_DIGITS.  We do not bounds check here for better
   * performance.
   */
  public static long fastSerialize64(
      int scale,
      int fastSignum, long fast1, long fast0, int fastScale) {

    if (fastSignum == 0) {
      return 0;
    } else if (fastSignum == 1) {
      return (fast1 * MULTIPLER_LONGWORD_DECIMAL + fast0) * powerOfTenTable[scale - fastScale];
    } else {
      return -(fast1 * MULTIPLER_LONGWORD_DECIMAL + fast0) * powerOfTenTable[scale - fastScale];
    }
  }

  //************************************************************************************************
  // Emulate BigInteger deserialization used by LazyBinary and others.

  /*
   * fastSetFromBigIntegerBytes word size we choose is 56 bits to stay below the 64 bit sign bit:
   * So, we need a multiplier 2^56
   *
   *    2^56 =
   *      72057594037927936 or
   *      72,057,594,037,927,936 or
   *      7,2057594037927936  (16 digit comma'd)
   */
  private static final FastHiveDecimal FAST_HIVE_DECIMAL_TWO_POWER_56 =
      new FastHiveDecimal(1, 2057594037927936L, 7L, 0, 17, 0);

  /*
   * fastSetFromBigIntegerBytes high word multiplier is 2^(56 + 56)
   *
   *    (2^56)*(2^56) =
   *      5192296858534827628530496329220096 or
   *     (1234567890123456789012345678901234)
   *     (         1         2         3    )
   *      5,192,296,858,534,827,628,530,496,329,220,096 or
   *      51,9229685853482762,8530496329220096  (16 digit comma'd)
   */
  private static final FastHiveDecimal FAST_HIVE_DECIMAL_TWO_POWER_112 =
      new FastHiveDecimal(1, 8530496329220096L, 9229685853482762L, 51L, 34, 0);

  // Multiply by 1/2^56 or 1.387778780781445675529539585113525390625e-17 to divide by 2^56.
  // As 16 digit comma'd 13877787,8078144567552953,9585113525390625
  //
  // Scale down: 56 = 39 fraction digits + 17 (negative exponent or number of zeros after dot).
  //
  // 3*16 (48) + 8 --> 56 down shift.
  //
  private static final FastHiveDecimal FAST_HIVE_DECIMAL_TWO_POWER_56_INVERSE =
      new FastHiveDecimal(1, 9585113525390625L, 8078144567552953L, 13877787L, 40, 0);

  /*
   * Where in the inverse multiplication result to find the quotient integer decimal portion.
   *
   * Please see comments for doDecimalToBinaryDivisionRemainder.
   */
  private static final int BIG_INTEGER_BYTES_QUOTIENT_INTEGER_WORD_NUM = 3;
  private static final int BIG_INTEGER_BYTES_QUOTIENT_INTEGER_DIGIT_NUM = 8;

  private static final int INITIAL_SHIFT = 48;   // 56 bits minus 1 byte.

  // Long masks and values.
  private static final long LONG_56_BIT_MASK = 0xFFFFFFFFFFFFFFL;
  private static final long LONG_TWO_TO_56_POWER = LONG_56_BIT_MASK + 1L;
  private static final long LONG_BYTE_MASK = 0xFFL;
  private static final long LONG_BYTE_HIGH_BIT_MASK = 0x80L;

  // Byte values.
  private static final byte BYTE_ALL_BITS = (byte) 0xFF;

  /**
   * Convert bytes in the format used by BigInteger's toByteArray format (and accepted by its
   * constructor) into a decimal using the specified scale.
   *
   * Our bigIntegerBytes methods create bytes in this format, too.
   *
   * This method is designed for high performance and does not create an actual BigInteger during
   * binary to decimal conversion.
   *
   * @param bytes the bytes to read from
   * @param offset the starting position in the bytes array
   * @param length the number of bytes to read from the bytes array
   * @param scale the scale of the number
   * @param fastResult an object to reused
   * @return did the conversion succeed?
   */
  public static boolean fastSetFromBigIntegerBytesAndScale(
      byte[] bytes, int offset, int length, int scale,
      FastHiveDecimal fastResult) {

    final int bytesLength = bytes.length;

    if (offset < 0 || offset >= bytesLength) {
      return false;
    }
    final int end = offset + length;
    if (end <= offset || end > bytesLength) {
      return false;
    }

    final int startOffset = offset;

    // Roughly based on BigInteger code.

    boolean isNegative = (bytes[offset] < 0);
    if (isNegative) {

      // Find first non-sign (0xff) byte of input.
      while (offset < end) {
        if (bytes[offset] != -1) {
          break;
        }
        offset++;
      }
      if (offset > end) {
        return false;
      }
    } else {

      // Strip leading zeroes -- although there shouldn't be any for a decimal.

      while (offset < end && bytes[offset] == 0) {
        offset++;
      }
      if (offset >= end) {
        // Zero.
        return true;
      }
    }

    long lowerWord56 = 0;
    long middleWord56 = 0;
    long highWord56 = 0;

    int reverseIndex = end;

    long work;
    int shift;

    final int lowestCount = Math.min(reverseIndex - offset, 7);
    shift = 0;
    for (int i = 0; i < lowestCount; i++) {
      work = bytes[--reverseIndex] & 0xFF;
      lowerWord56 |= work << shift;
      shift += 8;
    }

    if (reverseIndex <= offset) {
      if (isNegative) {
        lowerWord56 = ~lowerWord56 & ((1L << shift) - 1);
      }
    } else {

      // Go on to middle word.

      final int middleCount = Math.min(reverseIndex - offset, 7);
      shift = 0;
      for (int i = 0; i < middleCount; i++) {
        work = bytes[--reverseIndex] & 0xFF;
        middleWord56 |= work << shift;
        shift += 8;
      }
      if (reverseIndex <= offset) {
        if (isNegative) {
          lowerWord56 = ~lowerWord56 & LONG_56_BIT_MASK;
          middleWord56 = ~middleWord56 & ((1L << shift) - 1);
        }
      } else {

        // Go on to high word.

        final int highCount = Math.min(reverseIndex - offset, 7);
        shift = 0;
        for (int i = 0; i < highCount; i++) {
          work = bytes[--reverseIndex] & 0xFF;
          highWord56 |= work << shift;
          shift += 8;
        }
        if (isNegative) {
          // We only need to apply negation to all 3 words when there are 3 words, etc.
          lowerWord56 = ~lowerWord56 & LONG_56_BIT_MASK;
          middleWord56 = ~middleWord56 & LONG_56_BIT_MASK;
          highWord56 = ~highWord56 & ((1L << shift) - 1);
        }
      }
    }

    if (!doBinaryToDecimalConversion(
          lowerWord56, middleWord56, highWord56,
          FAST_HIVE_DECIMAL_TWO_POWER_56,
          FAST_HIVE_DECIMAL_TWO_POWER_112,    // 2^(56 + 56)
          fastResult)) {
      // Overflow.  Use slower alternate.
      return doAlternateSetFromBigIntegerBytesAndScale(
          bytes, startOffset, length, scale,
          fastResult);
    }

    // System.out.println("fastSetFromBigIntegerBytesAndScale fast0 " + fastResult.fast0 + " fast1 " + fastResult.fast1 + " fast2 " + fastResult.fast2);
    if (isNegative) {
      if (!doAddSameScaleSameSign(
          /* resultSignum */ 1,
          fastResult.fast0, fastResult.fast1, fastResult.fast2,
          1, 0, 0,
          fastResult)) {
        // Overflow.  Use slower alternate.
        return doAlternateSetFromBigIntegerBytesAndScale(
            bytes, startOffset, length, scale,
            fastResult);
      }
    }

    if (fastResult.fast0 == 0 && fastResult.fast1 == 0 && fastResult.fast2 == 0) {
      fastResult.fastSignum = 0;
    } else {
      fastResult.fastSignum = (isNegative ? -1 : 1);
      fastResult.fastScale = scale;
      final int rawPrecision = fastRawPrecision(fastResult);
      fastResult.fastIntegerDigitCount = Math.max(0, rawPrecision - scale);

      /*
       * Just in case we deserialize a decimal with trailing zeroes...
       */
      final int resultTrailingZeroCount =
          fastTrailingDecimalZeroCount(
              fastResult.fast0, fastResult.fast1, fastResult.fast2,
              fastResult.fastIntegerDigitCount, fastResult.fastScale);
      if (resultTrailingZeroCount > 0) {
        doFastScaleDown(
            fastResult,
            resultTrailingZeroCount,
            fastResult);

        fastResult.fastScale -= resultTrailingZeroCount;
      }
    }

    return true;
  }

  /**
   * When fastSetFromBigIntegerBytesAndScale can handle the input because it is too large,
   * we fall back to this.
   */
  private static boolean doAlternateSetFromBigIntegerBytesAndScale(
      byte[] bytes, int offset, int length, int scale,
      FastHiveDecimal fastResult) {

    byte[] byteArray = Arrays.copyOfRange(bytes, offset, offset + length);

    BigInteger bigInteger = new BigInteger(byteArray);
    // System.out.println("doAlternateSetFromBigIntegerBytesAndScale bigInteger " + bigInteger);
    BigDecimal bigDecimal = new BigDecimal(bigInteger, scale);
    // System.out.println("doAlternateSetFromBigIntegerBytesAndScale bigDecimal " + bigDecimal);
    fastResult.fastReset();
    return fastSetFromBigDecimal(bigDecimal, true, fastResult);
  }

  //************************************************************************************************
  // Emulate BigInteger serialization used by LazyBinary, Avro, Parquet, and possibly others.

  public static int fastBigIntegerBytes(
      final int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale,
      int fastSerializeScale,
      long[] scratchLongs, byte[] buffer) {
    if (fastSerializeScale != -1) {
      return
          fastBigIntegerBytesScaled(
              fastSignum, fast0, fast1, fast2,
              fastIntegerDigitCount, fastScale,
              fastSerializeScale,
              scratchLongs, buffer);
    } else {
      return
          fastBigIntegerBytesUnscaled(
              fastSignum, fast0, fast1, fast2,
              scratchLongs, buffer);
    }
  }

  /**
   * Return binary representation of this decimal's BigInteger equivalent unscaled value using
   * the format that the BigInteger's toByteArray method returns (and the BigInteger constructor
   * accepts).
   *
   * Used by LazyBinary, Avro, and Parquet serialization.
   *
   * Scratch objects necessary to do the decimal to binary conversion without actually creating a
   * BigInteger object are passed for better performance.
   *
   * @param fastSignum the sign (-1, 0, or +1)
   * @param fast0 word 0 of the internal representation
   * @param fast1 word 1
   * @param fast2 word 2
   * @param scratchLongs scratch array of SCRATCH_LONGS_LEN longs
   * @param buffer scratch array of SCRATCH_BUFFER_LEN_BIG_INTEGER_BYTES bytes
   * @return The number of bytes used for the binary result in buffer.  Otherwise, 0 if the
   *         conversion failed.
   */
  public static int fastBigIntegerBytesUnscaled(
      final int fastSignum, long fast0, long fast1, long fast2,
      long[] scratchLongs, byte[] buffer) {

    /*
     * Algorithm:
     * 1) Convert decimal to three 56-bit words (three is enough for the decimal since we
     *    represent the decimal with trailing zeroes trimmed).
     * 2) Skip leading zeroes in the words.
     * 3) Once we find real data (i.e. a non-zero byte), add a sign byte to buffer if necessary.
     * 4) Add bytes from the (rest of) 56-bit words.
     * 5) Return byte count.
     */

    if (fastSignum == 0) {
      buffer[0] = 0;
      return 1;
    }

    boolean isNegative = (fastSignum == -1);

    /*
     * Use common conversion method we share with fastSerializationUtilsWrite.
     */
    if (!doDecimalToBinaryConversion(
        fast0, fast1, fast2,
        FAST_HIVE_DECIMAL_TWO_POWER_56_INVERSE,
        BIG_INTEGER_BYTES_QUOTIENT_INTEGER_WORD_NUM,
        BIG_INTEGER_BYTES_QUOTIENT_INTEGER_DIGIT_NUM,
        FAST_HIVE_DECIMAL_TWO_POWER_56,
        scratchLongs)) {
      // Overflow.  This is not expected.
      return 0;
    }

    int byteIndex = 0;

    long word0 = scratchLongs[0];
    long word1 = scratchLongs[1];
    long word2 = scratchLongs[2];

    if (!isNegative) {

      // Positive number.

      long longWork = 0;

      int shift = INITIAL_SHIFT;

      if (word2 != 0L) {

        // Skip leading zeroes in word2.

        while (true) {
          longWork = (word2 >> shift) & LONG_BYTE_MASK;
          if (longWork != 0) {
            break;
          }
          if (shift == 0) {
            throw new RuntimeException("Unexpected #1");
          }
          shift -= Byte.SIZE;
        }

        // Now that we have found real data, emit sign byte if necessary.
        if ((longWork & LONG_BYTE_HIGH_BIT_MASK) != 0) {
          // Add sign byte since high bit is on.
          buffer[byteIndex++] = (byte) 0;
        }

        // Emit the rest of word2
        while (true) {
          buffer[byteIndex++] = (byte) longWork;
          if (shift == 0) {
            break;
          }
          shift -= Byte.SIZE;
          longWork = (word2 >> shift) & LONG_BYTE_MASK;
        }

        shift = INITIAL_SHIFT;
      }

      if (byteIndex == 0 && word1 == 0L) {

        // Skip word1, also.

      } else {

        if (byteIndex == 0) {

          // Skip leading zeroes in word1.

          while (true) {
            longWork = (word1 >> shift) & LONG_BYTE_MASK;
            if (longWork != 0) {
              break;
            }
            if (shift == 0) {
              throw new RuntimeException("Unexpected #2");
            }
            shift -= Byte.SIZE;
          }

          // Now that we have found real data, emit sign byte if necessary.
          if ((longWork & LONG_BYTE_HIGH_BIT_MASK) != 0) {
            // Add sign byte since high bit is on.
            buffer[byteIndex++] = (byte) 0;
          }

        } else {
          longWork = (word1 >> shift) & LONG_BYTE_MASK;
        }

        // Emit the rest of word1

        while (true) {
          buffer[byteIndex++] = (byte) longWork;
          if (shift == 0) {
            break;
          }
          shift -= Byte.SIZE;
          longWork = (word1 >> shift) & LONG_BYTE_MASK;
        }

        shift = INITIAL_SHIFT;
      }

      if (byteIndex == 0) {

        // Skip leading zeroes in word0.

        while (true) {
          longWork = (word0 >> shift) & LONG_BYTE_MASK;
          if (longWork != 0) {
            break;
          }
          if (shift == 0) {

            // All zeroes -- we should have handled this earlier.
            throw new RuntimeException("Unexpected #3");
          }
          shift -= Byte.SIZE;
        }

        // Now that we have found real data, emit sign byte if necessary.
        if ((longWork & LONG_BYTE_HIGH_BIT_MASK) != 0) {
          // Add sign byte since high bit is on.
          buffer[byteIndex++] = (byte) 0;
        }

      } else {
        longWork = (word0 >> shift) & LONG_BYTE_MASK;
      }

      // Emit the rest of word0.
      while (true) {
        buffer[byteIndex++] = (byte) longWork;
        if (shift == 0) {
          break;
        }
        shift -= Byte.SIZE;
        longWork = (word0 >> shift) & LONG_BYTE_MASK;
      }

    } else {

      // Negative number.

      // Subtract 1 for two's compliment adjustment.
      word0--;
      if (word0 < 0) {
        word0 += LONG_TWO_TO_56_POWER;
        word1--;
        if (word1 < 0) {
          word1 += LONG_TWO_TO_56_POWER;
          word2--;
          if (word2 < 0) {
            // Underflow.
            return 0;
          }
        }
      }

      long longWork = 0;

      int shift = INITIAL_SHIFT;

      if (word2 != 0L) {

        // Skip leading zeroes in word2.

        while (true) {
          longWork = (word2 >> shift) & LONG_BYTE_MASK;
          if (longWork != 0) {
            break;
          }
          if (shift == 0) {
            throw new RuntimeException("Unexpected #1");
          }
          shift -= Byte.SIZE;
        }

        // Now that we have found real data, emit sign byte if necessary and do negative fixup.

        longWork = (~longWork & LONG_BYTE_MASK);
        if (((longWork) & LONG_BYTE_HIGH_BIT_MASK) == 0) {
          // Add sign byte since high bit is off.
          buffer[byteIndex++] = BYTE_ALL_BITS;
        }

        // Invert words.
        word2 = ~word2;
        word1 = ~word1;
        word0 = ~word0;

        // Emit the rest of word2
        while (true) {
          buffer[byteIndex++] = (byte) longWork;
          if (shift == 0) {
            break;
          }
          shift -= Byte.SIZE;
          longWork = (word2 >> shift) & LONG_BYTE_MASK;
        }

        shift = INITIAL_SHIFT;
      }

      if (byteIndex == 0 && word1 == 0L) {

        // Skip word1, also.

      } else {

        if (byteIndex == 0) {

          // Skip leading zeroes in word1.

          while (true) {
            longWork = (word1 >> shift) & LONG_BYTE_MASK;
            if (longWork != 0) {
              break;
            }
            if (shift == 0) {
              throw new RuntimeException("Unexpected #2");
            }
            shift -= Byte.SIZE;
          }

          // Now that we have found real data, emit sign byte if necessary and do negative fixup.

          longWork = (~longWork & LONG_BYTE_MASK);
          if ((longWork & LONG_BYTE_HIGH_BIT_MASK) == 0) {
            // Add sign byte since high bit is off.
            buffer[byteIndex++] = BYTE_ALL_BITS;
          }

          // Invert words.
          word1 = ~word1;
          word0 = ~word0;

        } else {
          longWork = (word1 >> shift) & LONG_BYTE_MASK;
        }

        // Emit the rest of word1

        while (true) {
          buffer[byteIndex++] = (byte) longWork;
          if (shift == 0) {
            break;
          }
          shift -= Byte.SIZE;
          longWork = (word1 >> shift) & LONG_BYTE_MASK;
        }

        shift = INITIAL_SHIFT;
      }

      if (byteIndex == 0) {

        // Skip leading zeroes in word0.

        while (true) {
          longWork = (word0 >> shift) & LONG_BYTE_MASK;
          if (longWork != 0) {
            break;
          }
          if (shift == 0) {

            // All zeroes.

            // -1 special case.  Unsigned magnitude 1 - two's compliment adjustment 1 = 0.
            buffer[0] = BYTE_ALL_BITS;
            return 1;
          }
          shift -= Byte.SIZE;
        }

        // Now that we have found real data, emit sign byte if necessary and do negative fixup.

        longWork = (~longWork & LONG_BYTE_MASK);
        if ((longWork & LONG_BYTE_HIGH_BIT_MASK) == 0) {
          // Add sign byte since high bit is off.
          buffer[byteIndex++] = BYTE_ALL_BITS;
        }

        // Invert words.
        word0 = ~word0;

      } else {
        longWork = (word0 >> shift) & LONG_BYTE_MASK;
      }

      // Emit the rest of word0.
      while (true) {
        buffer[byteIndex++] = (byte) longWork;
        if (shift == 0) {
          break;
        }
        shift -= Byte.SIZE;
        longWork = (word0 >> shift) & LONG_BYTE_MASK;
      }
    }

    return byteIndex;
  }

  /**
   * Convert decimal to BigInteger binary bytes with a serialize scale, similar to the formatScale
   * for toFormatString.  It adds trailing zeroes when a serializeScale is greater than current
   * scale.  Or, rounds if scale is less than current scale.
   *
   * Used by Avro and Parquet serialization.
   *
   * This emulates the OldHiveDecimal setScale / OldHiveDecimal getInternalStorage() behavior.
   *
   * @param fastSignum the sign number (-1, 0, or +1)
   * @param fast0 word 0 of the internal representation
   * @param fast1 word 1
   * @param fast2 word 2
   * @param fastIntegerDigitCount the number of integer digits
   * @param fastScale the scale
   * @param serializeScale the scale to serialize
   * @param scratchLongs a scratch array of longs
   * @param buffer the buffer to serialize into
   * @return the number of bytes used to serialize the number
   */
  public static int fastBigIntegerBytesScaled(
      final int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale,
      int serializeScale,
      long[] scratchLongs, byte[] buffer) {

    // Normally, trailing fractional digits are removed.  But to emulate the
    // OldHiveDecimal setScale and OldHiveDecimalWritable internalStorage, we need to trailing zeroes
    // here.
    //
    // NOTE: This can cause a decimal that has too many decimal digits (because of trailing zeroes)
    //       for us to represent.  In that case, we punt and convert with a BigInteger alternate
    //       code.

    if (fastSignum == 0 || serializeScale == fastScale) {
      return
          fastBigIntegerBytesUnscaled(
              fastSignum, fast0, fast1, fast2,
              scratchLongs, buffer);
    } else if (serializeScale > fastScale) {

      final int scaleUp = serializeScale - fastScale;
      final int maxScale = HiveDecimal.MAX_SCALE - fastIntegerDigitCount;
      if (serializeScale > maxScale) {

        // We cannot to scaled up decimals that cannot be represented.
        // Instead, we use a BigInteger instead.

        BigInteger bigInteger =
            fastBigIntegerValueUnscaled(
                fastSignum, fast0, fast1, fast2);

        BigInteger bigIntegerScaled = bigInteger.multiply(BIG_INTEGER_TEN.pow(scaleUp));
        byte[] bigIntegerBytesScaled = bigIntegerScaled.toByteArray();
        final int length = bigIntegerBytesScaled.length;
        System.arraycopy(bigIntegerBytesScaled, 0, buffer, 0, length);
        return length;
      }

      FastHiveDecimal fastTemp = new FastHiveDecimal();
      if (!fastScaleUp(
          fast0, fast1, fast2,
          scaleUp,
          fastTemp)) {
        throw new RuntimeException("Unexpected");
      }
      return
          fastBigIntegerBytesUnscaled(
              fastSignum, fastTemp.fast0, fastTemp.fast1, fastTemp.fast2,
              scratchLongs, buffer);
    } else {

      // serializeScale < fastScale.

      FastHiveDecimal fastTemp = new FastHiveDecimal();
      if (!fastRound(
          fastSignum, fast0, fast1, fast2,
          fastIntegerDigitCount, fastScale,
          serializeScale, BigDecimal.ROUND_HALF_UP,
          fastTemp)) {
        return 0;
      }
      return
          fastBigIntegerBytesUnscaled(
              fastSignum, fastTemp.fast0, fastTemp.fast1, fastTemp.fast2,
              scratchLongs, buffer);
    }
  }

  //************************************************************************************************
  // Decimal to Integer conversion.

  private static final int MAX_BYTE_DIGITS = 3;
  private static final FastHiveDecimal FASTHIVEDECIMAL_MIN_BYTE_VALUE_MINUS_ONE =
      new FastHiveDecimal((long) Byte.MIN_VALUE - 1L);
  private static final FastHiveDecimal FASTHIVEDECIMAL_MAX_BYTE_VALUE_PLUS_ONE =
      new FastHiveDecimal((long) Byte.MAX_VALUE + 1L);

  private static final int MAX_SHORT_DIGITS = 5;
  private static final FastHiveDecimal FASTHIVEDECIMAL_MIN_SHORT_VALUE_MINUS_ONE =
      new FastHiveDecimal((long) Short.MIN_VALUE - 1L);
  private static final FastHiveDecimal FASTHIVEDECIMAL_MAX_SHORT_VALUE_PLUS_ONE =
      new FastHiveDecimal((long) Short.MAX_VALUE + 1L);

  private static final int MAX_INT_DIGITS = 10;
  private static final FastHiveDecimal FASTHIVEDECIMAL_MIN_INT_VALUE_MINUS_ONE =
      new FastHiveDecimal((long) Integer.MIN_VALUE - 1L);
  private static final FastHiveDecimal FASTHIVEDECIMAL_MAX_INT_VALUE_PLUS_ONE =
      new FastHiveDecimal((long) Integer.MAX_VALUE + 1L);

  private static final FastHiveDecimal FASTHIVEDECIMAL_MIN_LONG_VALUE =
      new FastHiveDecimal(Long.MIN_VALUE);
  private static final FastHiveDecimal FASTHIVEDECIMAL_MAX_LONG_VALUE =
      new FastHiveDecimal(Long.MAX_VALUE);
  private static final int MAX_LONG_DIGITS =
      FASTHIVEDECIMAL_MAX_LONG_VALUE.fastIntegerDigitCount;
  private static final FastHiveDecimal FASTHIVEDECIMAL_MIN_LONG_VALUE_MINUS_ONE =
      new FastHiveDecimal("-9223372036854775809");
  private static final FastHiveDecimal FASTHIVEDECIMAL_MAX_LONG_VALUE_PLUS_ONE =
      new FastHiveDecimal("9223372036854775808");

  private static final BigInteger BIG_INTEGER_UNSIGNED_BYTE_MAX_VALUE = BIG_INTEGER_TWO.pow(Byte.SIZE).subtract(BigInteger.ONE);
  private static final BigInteger BIG_INTEGER_UNSIGNED_SHORT_MAX_VALUE = BIG_INTEGER_TWO.pow(Short.SIZE).subtract(BigInteger.ONE);
  private static final BigInteger BIG_INTEGER_UNSIGNED_INT_MAX_VALUE = BIG_INTEGER_TWO.pow(Integer.SIZE).subtract(BigInteger.ONE);
  private static final BigInteger BIG_INTEGER_UNSIGNED_LONG_MAX_VALUE = BIG_INTEGER_TWO.pow(Long.SIZE).subtract(BigInteger.ONE);

  /**
   * Is the decimal value a byte? Range -128            to      127.
   *                                    Byte.MIN_VALUE          Byte.MAX_VALUE
   *
   * Emulates testing for no value corruption:
   *      bigDecimalValue().setScale(0).equals(BigDecimal.valueOf(bigDecimalValue().byteValue()))
   *
   * NOTE: Fractional digits are ignored in the test since fastByteValueClip() will
   *       remove them (round down).
   *
   * @param fastSignum the sign (-1, 0, or +1)
   * @param fast0 word 0 of the internal representation
   * @param fast1 word 1
   * @param fast2 word 2
   * @param fastIntegerDigitCount the number of integer digits
   * @param fastScale the scale of the number
   * @return True when fastByteValueClip() will return a correct byte.
   */
  public static boolean fastIsByte(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale) {

    if (fastIntegerDigitCount < MAX_BYTE_DIGITS) {

      // Definitely a byte; most bytes fall here
     return true;

    } else if (fastIntegerDigitCount > MAX_BYTE_DIGITS) {

      // Definitely not a byte.
      return false;

    } else if (fastScale == 0) {
      if (fast1 != 0 || fast2 != 0) {
        return false;
      }
      if (fastSignum == 1) {
        return (fast0 <= Byte.MAX_VALUE);
      } else {
        return (-fast0 >= Byte.MIN_VALUE);
      }
    } else {

      // We need to work a little harder for our comparison.  Note we round down for
      // integer conversion so anything below the next min/max will work.

      if (fastSignum == 1) {
        return
            (fastCompareTo(
                fastSignum, fast0, fast1, fast2, fastScale,
                FASTHIVEDECIMAL_MAX_BYTE_VALUE_PLUS_ONE) < 0);
      } else {
        return
            (fastCompareTo(
                fastSignum, fast0, fast1, fast2, fastScale,
                FASTHIVEDECIMAL_MIN_BYTE_VALUE_MINUS_ONE) > 0);
      }
    }
  }

  // We use "Clip" in the name because this method will return a corrupted value when
  // fastIsByte returns false.
  public static byte fastByteValueClip(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale) {

    if (fastScale == 0) {
      if (fast1 == 0 && fast2 == 0) {
        if (fastSignum == 1) {
          if (fast0 <= Byte.MAX_VALUE) {
            return (byte) fast0;
          }
        } else {
          if (-fast0 >= Byte.MIN_VALUE) {
            return (byte) -fast0;
          };
        }
      }
      // SLOW: Do remainder with BigInteger.
      BigInteger bigInteger =
          fastBigIntegerValueUnscaled(
              fastSignum, fast0, fast1, fast2);
      return bigInteger.remainder(BIG_INTEGER_UNSIGNED_BYTE_MAX_VALUE).byteValue();
    } else {

      // Adjust all longs using power 10 division/remainder.
      long result0;
      long result1;
      long result2;
      if (fastScale < LONGWORD_DECIMAL_DIGITS) {

        // Part of lowest word survives.

        final long divideFactor = powerOfTenTable[fastScale];
        final long multiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - fastScale];

        result0 =
            fast0 / divideFactor
          + ((fast1 % divideFactor) * multiplyFactor);
        result1 =
            fast1 / divideFactor
          + ((fast2 % divideFactor) * multiplyFactor);
        result2 =
            fast2 / divideFactor;

      } else if (fastScale < TWO_X_LONGWORD_DECIMAL_DIGITS) {

        // Throw away lowest word.

        final int adjustedScaleDown = fastScale - LONGWORD_DECIMAL_DIGITS;

        final long divideFactor = powerOfTenTable[adjustedScaleDown];
        final long multiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - adjustedScaleDown];

        result0 =
            fast1 / divideFactor
          + ((fast2 % divideFactor) * multiplyFactor);
        result1 =
            fast2 / divideFactor;
        result2 = 0;

      } else {

        // Throw away middle and lowest words.

        final int adjustedScaleDown = fastScale - 2*LONGWORD_DECIMAL_DIGITS;

        result0 =
            fast2 / powerOfTenTable[adjustedScaleDown];
        result1 = 0;
        result2 = 0;

      }

      if (result1 == 0 && result2 == 0) {
        if (fastSignum == 1) {
          if (result0 <= Byte.MAX_VALUE) {
            return (byte) result0;
          }
        } else {
          if (-result0 >= Byte.MIN_VALUE) {
            return (byte) -result0;
          };
        }
      }
      // SLOW: Do remainder with BigInteger.
      BigInteger bigInteger =
          fastBigIntegerValueUnscaled(
              fastSignum, result0, result1, result2);
      return bigInteger.remainder(BIG_INTEGER_UNSIGNED_BYTE_MAX_VALUE).byteValue();
    }
  }

  /**

   * @return True when shortValue() will return a correct short.
   */

  /**
   * Is the decimal value a short? Range -32,768         to     32,767.
   *                                     Short.MIN_VALUE        Short.MAX_VALUE
   *
   * Emulates testing for no value corruption:
   *      bigDecimalValue().setScale(0).equals(BigDecimal.valueOf(bigDecimalValue().shortValue()))
   *
   * NOTE: Fractional digits are ignored in the test since fastShortValueClip() will
   *       remove them (round down).
   *
   * @param fastSignum the sign (-1, 0, or +1)
   * @param fast0 word 0 of the internal representation
   * @param fast1 word 1
   * @param fast2 word 2
   * @param fastIntegerDigitCount the number of integer digits
   * @param fastScale the scale of the number
   * @return True when fastShortValueClip() will return a correct short.
   */
  public static boolean fastIsShort(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale) {

    if (fastIntegerDigitCount < MAX_SHORT_DIGITS) {

      // Definitely a short; most shorts fall here
      return true;

    } else if (fastIntegerDigitCount > MAX_SHORT_DIGITS) {

      // Definitely not a short.
      return false;

    } else if (fastScale == 0) {
      if (fast1 != 0 || fast2 != 0) {
        return false;
      }
      if (fastSignum == 1) {
        return (fast0 <= Short.MAX_VALUE);
      } else {
        return (-fast0 >= Short.MIN_VALUE);
      }
    } else {

      // We need to work a little harder for our comparison.  Note we round down for
      // integer conversion so anything below the next min/max will work.

      if (fastSignum == 1) {
        return
            (fastCompareTo(
                fastSignum, fast0, fast1, fast2, fastScale,
                FASTHIVEDECIMAL_MAX_SHORT_VALUE_PLUS_ONE) < 0);
      } else {
        return
            (fastCompareTo(
                fastSignum, fast0, fast1, fast2, fastScale,
                FASTHIVEDECIMAL_MIN_SHORT_VALUE_MINUS_ONE) > 0);
      }
    }
  }

  // We use "Clip" in the name because this method will return a corrupted value when
  // fastIsShort returns false.
  public static short fastShortValueClip(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale) {

    if (fastScale == 0) {
      if (fast1 == 0 && fast2 == 0) {
        if (fastSignum == 1) {
          if (fast0 <= Short.MAX_VALUE) {
            return (short) fast0;
          }
        } else {
          if (-fast0 >= Short.MIN_VALUE) {
            return (short) -fast0;
          };
        }
      }
      // SLOW: Do remainder with BigInteger.
      BigInteger bigInteger =
          fastBigIntegerValueUnscaled(
              fastSignum, fast0, fast1, fast2);
      return bigInteger.remainder(BIG_INTEGER_UNSIGNED_SHORT_MAX_VALUE).shortValue();
    } else {

      // Adjust all longs using power 10 division/remainder.
      long result0;
      long result1;
      long result2;
      if (fastScale < LONGWORD_DECIMAL_DIGITS) {

        // Part of lowest word survives.

        final long divideFactor = powerOfTenTable[fastScale];
        final long multiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - fastScale];

        result0 =
            fast0 / divideFactor
          + ((fast1 % divideFactor) * multiplyFactor);
        result1 =
            fast1 / divideFactor
          + ((fast2 % divideFactor) * multiplyFactor);
        result2 =
            fast2 / divideFactor;

      } else if (fastScale < TWO_X_LONGWORD_DECIMAL_DIGITS) {

        // Throw away lowest word.

        final int adjustedScaleDown = fastScale - LONGWORD_DECIMAL_DIGITS;

        final long divideFactor = powerOfTenTable[adjustedScaleDown];
        final long multiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - adjustedScaleDown];

        result0 =
            fast1 / divideFactor
          + ((fast2 % divideFactor) * multiplyFactor);
        result1 =
            fast2 / divideFactor;
        result2 = 0;

      } else {

        // Throw away middle and lowest words.

        final int adjustedScaleDown = fastScale - 2*LONGWORD_DECIMAL_DIGITS;

        result0 =
            fast2 / powerOfTenTable[adjustedScaleDown];
        result1 = 0;
        result2 = 0;

      }

      if (result1 == 0 && result2 == 0) {
        if (fastSignum == 1) {
          if (result0 <= Short.MAX_VALUE) {
            return (short) result0;
          }
        } else {
          if (-result0 >= Short.MIN_VALUE) {
            return (short) -result0;
          };
        }
      }
      // SLOW: Do remainder with BigInteger.
      BigInteger bigInteger =
          fastBigIntegerValueUnscaled(
              fastSignum, result0, result1, result2);
      return bigInteger.remainder(BIG_INTEGER_UNSIGNED_SHORT_MAX_VALUE).shortValue();
    }
  }

  /**
   * Is the decimal value a int? Range -2,147,483,648     to   2,147,483,647.
   *                                   Integer.MIN_VALUE       Integer.MAX_VALUE
   *
   * Emulates testing for no value corruption:
   *      bigDecimalValue().setScale(0).equals(BigDecimal.valueOf(bigDecimalValue().intValue()))
   *
   * NOTE: Fractional digits are ignored in the test since fastIntValueClip() will
   *       remove them (round down).
   *
   * @param fastSignum the sign (-1, 0, or +1)
   * @param fast0 word 0 of the internal representation
   * @param fast1 word 1
   * @param fast2 word 2
   * @param fastIntegerDigitCount the number of integer digits
   * @param fastScale the scale of the number
   * @return True when fastIntValueClip() will return a correct int.
   */
  public static boolean fastIsInt(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale) {

    if (fastIntegerDigitCount < MAX_INT_DIGITS) {

      // Definitely a int; most ints fall here
      return true;

    } else if (fastIntegerDigitCount > MAX_INT_DIGITS) {

      // Definitely not an int.
      return false;

    } else if (fastScale == 0) {
      if (fast1 != 0 || fast2 != 0) {
        return false;
      }
      if (fastSignum == 1) {
        return (fast0 <= Integer.MAX_VALUE);
      } else {
        return (-fast0 >= Integer.MIN_VALUE);
      }
    } else {

      // We need to work a little harder for our comparison.  Note we round down for
      // integer conversion so anything below the next min/max will work.

      if (fastSignum == 1) {
        return
            (fastCompareTo(
                fastSignum, fast0, fast1, fast2, fastScale,
                FASTHIVEDECIMAL_MAX_INT_VALUE_PLUS_ONE) < 0);
      } else {
        return
            (fastCompareTo(
                fastSignum, fast0, fast1, fast2, fastScale,
                FASTHIVEDECIMAL_MIN_INT_VALUE_MINUS_ONE) > 0);
      }
    }
  }

  // We use "Clip" in the name because this method will return a corrupted value when
  // fastIsInt returns false.
  public static int fastIntValueClip(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale) {

    if (fastScale == 0) {
      if (fast1 == 0 && fast2 == 0) {
        if (fastSignum == 1) {
          if (fast0 <= Integer.MAX_VALUE) {
            return (int) fast0;
          }
        } else {
          if (-fast0 >= Integer.MIN_VALUE) {
            return (int) -fast0;
          };
        }
      }
      // SLOW: Do remainder with BigInteger.
      BigInteger bigInteger =
          fastBigIntegerValueUnscaled(
              fastSignum, fast0, fast1, fast2);
      return bigInteger.remainder(BIG_INTEGER_UNSIGNED_INT_MAX_VALUE).intValue();
    } else {

      // Adjust all longs using power 10 division/remainder.
      long result0;
      long result1;
      long result2;
      if (fastScale < LONGWORD_DECIMAL_DIGITS) {

        // Part of lowest word survives.

        final long divideFactor = powerOfTenTable[fastScale];
        final long multiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - fastScale];

        result0 =
            fast0 / divideFactor
          + ((fast1 % divideFactor) * multiplyFactor);
        result1 =
            fast1 / divideFactor
          + ((fast2 % divideFactor) * multiplyFactor);
        result2 =
            fast2 / divideFactor;

      } else if (fastScale < TWO_X_LONGWORD_DECIMAL_DIGITS) {

        // Throw away lowest word.

        final int adjustedScaleDown = fastScale - LONGWORD_DECIMAL_DIGITS;

        final long divideFactor = powerOfTenTable[adjustedScaleDown];
        final long multiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - adjustedScaleDown];

        result0 =
            fast1 / divideFactor
          + ((fast2 % divideFactor) * multiplyFactor);
        result1 =
            fast2 / divideFactor;
        result2 = 0;

      } else {

        // Throw away middle and lowest words.

        final int adjustedScaleDown = fastScale - 2*LONGWORD_DECIMAL_DIGITS;

        result0 =
            fast2 / powerOfTenTable[adjustedScaleDown];
        result1 = 0;
        result2 = 0;

      }

      if (result1 == 0 && result2 == 0) {
        if (fastSignum == 1) {
          if (result0 <= Integer.MAX_VALUE) {
            return (int) result0;
          }
        } else {
          if (-result0 >= Integer.MIN_VALUE) {
            return (int) -result0;
          };
        }
      }
      // SLOW: Do remainder with BigInteger.
      BigInteger bigInteger =
          fastBigIntegerValueUnscaled(
              fastSignum, result0, result1, result2);
      return bigInteger.remainder(BIG_INTEGER_UNSIGNED_INT_MAX_VALUE).intValue();
    }
  }

  /**
   * Is the decimal value a long? Range -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807.
   *                                    Long.MIN_VALUE                Long.MAX_VALUE
   *
   * Emulates testing for no value corruption:
   *      bigDecimalValue().setScale(0).equals(BigDecimal.valueOf(bigDecimalValue().longValue()))
   *
   * NOTE: Fractional digits are ignored in the test since fastLongValueClip() will
   *       remove them (round down).
   *
   * @param fastSignum the sign (-1, 0, or +1)
   * @param fast0 word 0 of the internal representation
   * @param fast1 word 1
   * @param fast2 word 2
   * @param fastIntegerDigitCount the number of integer digits
   * @param fastScale the scale of the number
   * @return True when fastLongValueClip() will return a correct long.
   */
  public static boolean fastIsLong(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale) {

    if (fastIntegerDigitCount < MAX_LONG_DIGITS) {

      // Definitely a long; most longs fall here
      return true;

    } else if (fastIntegerDigitCount > MAX_LONG_DIGITS) {

      // Definitely not a long.
      return false;

    } else if (fastScale == 0) {

      // From the above checks, we know fast2 is zero.

      if (fastSignum == 1) {
        FastHiveDecimal max = FASTHIVEDECIMAL_MAX_LONG_VALUE;
        if (fast1 > max.fast1 || (fast1 == max.fast1 && fast0 > max.fast0)) {
          return false;
        }
        return true;
      } else {
        FastHiveDecimal min = FASTHIVEDECIMAL_MIN_LONG_VALUE;
        if (fast1 > min.fast1 || (fast1 == min.fast1 && fast0 > min.fast0)) {
          return false;
        }
        return true;
      }

    } else {

      // We need to work a little harder for our comparison.  Note we round down for
      // integer conversion so anything below the next min/max will work.

      if (fastSignum == 1) {
        return
            (fastCompareTo(
                fastSignum, fast0, fast1, fast2, fastScale,
                FASTHIVEDECIMAL_MAX_LONG_VALUE_PLUS_ONE) < 0);
      } else {
        return
            (fastCompareTo(
                fastSignum, fast0, fast1, fast2, fastScale,
                FASTHIVEDECIMAL_MIN_LONG_VALUE_MINUS_ONE) > 0);
      }
    }
  }

  // We use "Clip" in the name because this method will return a corrupted value when
  // fastIsLong returns false.
  public static long fastLongValueClip(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale) {
    if (fastSignum == 0) {
      return 0;
    }

    if (fastScale == 0) {
      // Do first comparison as unsigned.
      if (fastCompareTo(
          1, fast0, fast1, fast2, fastScale,
          FASTHIVEDECIMAL_MAX_LONG_VALUE) <= 0) {
        if (fastSignum == 1) {
          return
              fast1 * MULTIPLER_LONGWORD_DECIMAL
            + fast0;
        } else {
          return
             -(fast1 * MULTIPLER_LONGWORD_DECIMAL
             + fast0);
        }
      } if (fastEquals(
          fastSignum, fast0, fast1, fast2, fastScale,
          FASTHIVEDECIMAL_MIN_LONG_VALUE)) {
        return Long.MIN_VALUE;
      } else {
        // SLOW: Do remainder with BigInteger.
        BigInteger bigInteger =
            fastBigIntegerValueUnscaled(
                fastSignum, fast0, fast1, fast2);
        return bigInteger.remainder(BIG_INTEGER_UNSIGNED_LONG_MAX_VALUE).longValue();
      }
    } else {

      // Adjust all longs using power 10 division/remainder.
      long result0;
      long result1;
      long result2;
      if (fastScale < LONGWORD_DECIMAL_DIGITS) {

        // Part of lowest word survives.

        final long divideFactor = powerOfTenTable[fastScale];
        final long multiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - fastScale];

        result0 =
            fast0 / divideFactor
          + ((fast1 % divideFactor) * multiplyFactor);
        result1 =
            fast1 / divideFactor
          + ((fast2 % divideFactor) * multiplyFactor);
        result2 =
            fast2 / divideFactor;

      } else if (fastScale < TWO_X_LONGWORD_DECIMAL_DIGITS) {

        // Throw away lowest word.

        final int adjustedScaleDown = fastScale - LONGWORD_DECIMAL_DIGITS;

        final long divideFactor = powerOfTenTable[adjustedScaleDown];
        final long multiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - adjustedScaleDown];

        result0 =
            fast1 / divideFactor
          + ((fast2 % divideFactor) * multiplyFactor);
        result1 =
            fast2 / divideFactor;
        result2 = 0;

      } else {

        // Throw away middle and lowest words.

        final int adjustedScaleDown = fastScale - 2*LONGWORD_DECIMAL_DIGITS;

        result0 =
            fast2 / powerOfTenTable[adjustedScaleDown];
        result1 = 0;
        result2 = 0;

      }

      // Do first comparison as UNSIGNED.
      if (fastCompareTo(
          1, result0, result1, result2, /* fastScale */ 0,
          FASTHIVEDECIMAL_MAX_LONG_VALUE) <= 0) {
        if (fastSignum == 1) {
          return
              result1 * MULTIPLER_LONGWORD_DECIMAL
            + result0;
        } else {
          return
             -(result1 * MULTIPLER_LONGWORD_DECIMAL
             + result0);
        }
      } if (fastEquals(
          fastSignum, result0, result1, result2, /* fastScale */ 0,
          FASTHIVEDECIMAL_MIN_LONG_VALUE)) {

        // SIGNED comparison to Long.MIN_VALUE decimal.
        return Long.MIN_VALUE;
      } else {
        // SLOW: Do remainder with BigInteger.
        BigInteger bigInteger =
            fastBigIntegerValueUnscaled(
                fastSignum, result0, result1, result2);
        return bigInteger.remainder(BIG_INTEGER_UNSIGNED_LONG_MAX_VALUE).longValue();
      }
    }
  }

  //************************************************************************************************
  // Decimal to Non-Integer conversion.

  public static float fastFloatValue(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale) {
    if (fastSignum == 0) {
      return 0;
    }
    BigDecimal bigDecimal = fastBigDecimalValue(
        fastSignum, fast0, fast1, fast2,
        fastIntegerDigitCount, fastScale);
    return bigDecimal.floatValue();
  }

  public static double fastDoubleValue(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale) {
    if (fastSignum == 0) {
      return 0;
    }

    // CONSIDER: Looked at the possibility of faster decimal to double conversion by using some
    //           of their lower level logic that extracts the various parts out of a double.
    //           The difficulty is Java's rounding rules are byzantine.

    BigDecimal bigDecimal = fastBigDecimalValue(
        fastSignum, fast0, fast1, fast2,
        fastIntegerDigitCount, fastScale);
    return bigDecimal.doubleValue();
  }

  /**
   * Get a BigInteger representing the decimal's digits without a dot.
   *
   * @param fastSignum the sign (-1, 0, or +1)
   * @param fast0 word 0 of the internal representation
   * @param fast1 word 1
   * @param fast2 word 2
   * @param fastIntegerDigitCount the number of integer digits
   * @param fastScale the scale of the number
   * @param fastSerializationScale the scale to serialize
   * @return Returns a signed BigInteger.
   */
  public static BigInteger fastBigIntegerValue(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale,
      int fastSerializationScale) {
    if (fastSerializationScale != -1) {
      return
          fastBigIntegerValueScaled(
              fastSignum, fast0, fast1, fast2,
              fastIntegerDigitCount, fastScale,
              fastSerializationScale);
    } else {
      return
          fastBigIntegerValueUnscaled(
              fastSignum, fast0, fast1, fast2);
    }
  }

  public static BigInteger fastBigIntegerValueUnscaled(
      int fastSignum, long fast0, long fast1, long fast2) {

    if (fastSignum == 0) {
      return BigInteger.ZERO;
    }
    BigInteger result;
    if (fast2 == 0) {
      if (fast1 == 0) {
        result =
            BigInteger.valueOf(fast0);
      } else {
        result =
            BigInteger.valueOf(fast0).add(
                BigInteger.valueOf(fast1).multiply(BIG_INTEGER_LONGWORD_MULTIPLIER));
      }
    } else {
      result =
          BigInteger.valueOf(fast0).add(
              BigInteger.valueOf(fast1).multiply(BIG_INTEGER_LONGWORD_MULTIPLIER)).add(
                  BigInteger.valueOf(fast2).multiply(BIG_INTEGER_LONGWORD_MULTIPLIER_2X));
    }

    return (fastSignum == 1 ? result : result.negate());
  }

  public static BigInteger fastBigIntegerValueScaled(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale,
      int fastSerializationScale) {

    // Use the serialization scale and create a BigInteger with trailing zeroes (or
    // round the decimal) if necessary.
    //
    // Since we are emulating old behavior and recommending the use of HiveDecimal.bigIntegerBytesScaled
    // instead just do it the slow way.  Get the BigDecimal.setScale value and return the
    // BigInteger.
    //
    BigDecimal bigDecimal =
        fastBigDecimalValue(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale);
    bigDecimal = bigDecimal.setScale(fastSerializationScale, RoundingMode.HALF_UP);
    return bigDecimal.unscaledValue();
  }

  /**
   * Return a BigDecimal representing the decimal.  The BigDecimal class is able to accurately
   * represent the decimal.
   *
   * NOTE: We are not representing our decimal as BigDecimal now as OldHiveDecimal did, so this
   * is now slower.
   *
   * @param fastSignum the sign (-1, 0, or +1)
   * @param fast0 word 0 of the internal representation
   * @param fast1 word 1
   * @param fast2 word 2
   * @param fastIntegerDigitCount the number of integer digits
   * @param fastScale the scale of the number
   * @return the BigDecimal equivalent
   */
  public static BigDecimal fastBigDecimalValue(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale) {
    BigInteger unscaledValue =
        fastBigIntegerValueUnscaled(
            fastSignum, fast0, fast1, fast2);
    return new BigDecimal(unscaledValue, fastScale);
  }

  //************************************************************************************************
  // Decimal Comparison.

  public static int fastCompareTo(
      int leftSignum, long leftFast0, long leftFast1, long leftFast2,
      int leftScale,
      FastHiveDecimal fastRight) {

    return
        fastCompareTo(
            leftSignum, leftFast0, leftFast1, leftFast2,
            leftScale,
            fastRight.fastSignum, fastRight.fast0, fastRight.fast1, fastRight.fast2,
            fastRight.fastScale);
  }

  private static int doCompareToSameScale(
      int signum,
      long leftFast0, long leftFast1, long leftFast2,
      long rightFast0, long rightFast1, long rightFast2) {

    if (leftFast0 == rightFast0 && leftFast1 == rightFast1 && leftFast2 == rightFast2) {
      return 0;
    }
    if (leftFast2 < rightFast2) {
      return -signum;
    } else if (leftFast2 > rightFast2) {
      return signum;
    }
    if (leftFast1 < rightFast1) {
      return -signum;
    } else if (leftFast1 > rightFast1){
      return signum;
    }
    return (leftFast0 < rightFast0 ? -signum : signum);
  }

  public static int fastCompareTo(
      int leftSignum, long leftFast0, long leftFast1, long leftFast2,
      int leftScale,
      int rightSignum, long rightFast0, long rightFast1, long rightFast2,
      int rightScale) {

    if (leftSignum == 0 && rightSignum == 0) {
      return 0;
    }

    // Optimization copied from BigDecimal.
    int signDiff = leftSignum - rightSignum;
    if (signDiff != 0) {
      return (signDiff > 0 ? 1 : -1);
    }

    // We are here when the left and right are non-zero and have the same sign.

    if (leftScale == rightScale) {

      return doCompareToSameScale(
          leftSignum,
          leftFast0, leftFast1, leftFast2,
          rightFast0, rightFast1, rightFast2);

    } else {

      // How do we handle different scales?

      // We at least know they are not equal.  The one with the larger scale has non-zero digits
      // below the other's scale (since the scale does not include trailing zeroes).

      // For comparison purposes, we can scale away those digits.  And, we can not scale up since
      // that could overflow.

      // Use modified portions of doFastScaleDown code here since we do not want to allocate a
      // temporary FastHiveDecimal object.

      long compare0;
      long compare1;
      long compare2;
      int scaleDown;
      if (leftScale < rightScale) {

        // Scale down right and compare.
        scaleDown = rightScale - leftScale;

        // Adjust all longs using power 10 division/remainder.

        if (scaleDown < LONGWORD_DECIMAL_DIGITS) {
          // Part of lowest word survives.

          final long divideFactor = powerOfTenTable[scaleDown];
          final long multiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - scaleDown];

          compare0 =
              rightFast0 / divideFactor
            + ((rightFast1 % divideFactor) * multiplyFactor);
          compare1 =
              rightFast1 / divideFactor
            + ((rightFast2 % divideFactor) * multiplyFactor);
          compare2 =
              rightFast2 / divideFactor;
        } else if (scaleDown < TWO_X_LONGWORD_DECIMAL_DIGITS) {
          // Throw away lowest word.

          final int adjustedScaleDown = scaleDown - LONGWORD_DECIMAL_DIGITS;

          final long divideFactor = powerOfTenTable[adjustedScaleDown];
          final long multiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - adjustedScaleDown];

          compare0 =
              rightFast1 / divideFactor
            + ((rightFast2 % divideFactor) * multiplyFactor);
          compare1 =
              rightFast2 / divideFactor;
          compare2 = 0;
        } else {
          // Throw away middle and lowest words.

          final int adjustedScaleDown = scaleDown - TWO_X_LONGWORD_DECIMAL_DIGITS;

          compare0 =
              rightFast2 / powerOfTenTable[adjustedScaleDown];
          compare1 = 0;
          compare2 = 0;
        }

        if (leftFast0 == compare0 && leftFast1 == compare1 && leftFast2 == compare2) {
          // Return less than because of right's digits below left's scale.
          return -leftSignum;
        }
        if (leftFast2 < compare2) {
          return -leftSignum;
        } else if (leftFast2 > compare2) {
          return leftSignum;
        }
        if (leftFast1 < compare1) {
          return -leftSignum;
        } else if (leftFast1 > compare1){
          return leftSignum;
        }
        return (leftFast0 < compare0 ? -leftSignum : leftSignum);

      } else {

        // Scale down left and compare.
        scaleDown = leftScale - rightScale;

        // Adjust all longs using power 10 division/remainder.

        if (scaleDown < LONGWORD_DECIMAL_DIGITS) {
          // Part of lowest word survives.

          final long divideFactor = powerOfTenTable[scaleDown];
          final long multiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - scaleDown];

          compare1 =
              leftFast1 / divideFactor
            + ((leftFast2 % divideFactor) * multiplyFactor);
          compare0 =
              leftFast0 / divideFactor
            + ((leftFast1 % divideFactor) * multiplyFactor);
          compare2 =
              leftFast2 / divideFactor;
        } else if (scaleDown < TWO_X_LONGWORD_DECIMAL_DIGITS) {
          // Throw away lowest word.

          final int adjustedScaleDown = scaleDown - LONGWORD_DECIMAL_DIGITS;

          final long divideFactor = powerOfTenTable[adjustedScaleDown];
          final long multiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - adjustedScaleDown];

          compare0 =
              leftFast1 / divideFactor
            + ((leftFast2 % divideFactor) * multiplyFactor);
          compare1 =
              leftFast2 / divideFactor;
          compare2 = 0;
        } else {
          // Throw away middle and lowest words.

          final int adjustedScaleDown = scaleDown - 2*LONGWORD_DECIMAL_DIGITS;

          compare0 =
              leftFast2 / powerOfTenTable[adjustedScaleDown];
          compare1 = 0;
          compare2 = 0;
        }

        if (compare0 == rightFast0 && compare1 == rightFast1 && compare2 == rightFast2) {
          // Return greater than because of left's digits below right's scale.
          return leftSignum;
        }
        if (compare2 < rightFast2) {
          return -leftSignum;
        } else if (compare2 > rightFast2) {
          return leftSignum;
        }
        if (compare1 < rightFast1) {
          return -leftSignum;
        } else if (compare1 > rightFast1){
          return leftSignum;
        }
        return (compare0 < rightFast0 ? -leftSignum : leftSignum);

      }
    }
  }

  public static boolean fastEquals(
      int leftSignum, long leftFast0, long leftFast1, long leftFast2,
      int leftScale,
      FastHiveDecimal fastRight) {

    if (leftSignum == 0) {
      return (fastRight.fastSignum == 0);
    }
    if (leftSignum != fastRight.fastSignum) {
      return false;
    }
    if (leftScale != fastRight.fastScale) {
      // We know they are not equal because the one with the larger scale has non-zero digits
      // below the other's scale (since the scale does not include trailing zeroes).
      return false;
    }
    return (
        leftFast0 == fastRight.fast0 && leftFast1 == fastRight.fast1 && leftFast2 == fastRight.fast2);
  }

  public static boolean fastEquals(
      int leftSignum, long leftFast0, long leftFast1, long leftFast2,
      int leftScale,
      int rightSignum, long rightFast0, long rightFast1, long rightFast2,
      int rightScale) {

    if (leftSignum == 0) {
      return (rightSignum == 0);
    }
    if (leftSignum != rightSignum) {
      return false;
    }
    if (leftScale != rightScale) {
      // We know they are not equal because the one with the larger scale has non-zero digits
      // below the other's scale (since the scale does not include trailing zeroes).
      return false;
    }
    return (
        leftFast0 == rightFast0 && leftFast1 == rightFast1 && leftFast2 == rightFast2);
  }

  private static int doCalculateNewFasterHashCode(
      int fastSignum, long fast0, long fast1, long fast2, int fastIntegerDigitCount, int fastScale) {

    long longHashCode;

    long key = fast0;

    // Hash code logic from original calculateLongHashCode

    key = (~key) + (key << 21); // key = (key << 21) - key - 1;
    key = key ^ (key >>> 24);
    key = (key + (key << 3)) + (key << 8); // key * 265
    key = key ^ (key >>> 14);
    key = (key + (key << 2)) + (key << 4); // key * 21
    key = key ^ (key >>> 28);
    key = key + (key << 31);

    longHashCode = key;

    key = fast1;

    key = (~key) + (key << 21); // key = (key << 21) - key - 1;
    key = key ^ (key >>> 24);
    key = (key + (key << 3)) + (key << 8); // key * 265
    key = key ^ (key >>> 14);
    key = (key + (key << 2)) + (key << 4); // key * 21
    key = key ^ (key >>> 28);
    key = key + (key << 31);

    longHashCode ^= key;

    key = fast2;

    key = (~key) + (key << 21); // key = (key << 21) - key - 1;
    key = key ^ (key >>> 24);
    key = (key + (key << 3)) + (key << 8); // key * 265
    key = key ^ (key >>> 14);
    key = (key + (key << 2)) + (key << 4); // key * 21
    key = key ^ (key >>> 28);
    key = key + (key << 31);

    longHashCode ^= key;

    key = fastSignum;

    key = (~key) + (key << 21); // key = (key << 21) - key - 1;
    key = key ^ (key >>> 24);
    key = (key + (key << 3)) + (key << 8); // key * 265
    key = key ^ (key >>> 14);
    key = (key + (key << 2)) + (key << 4); // key * 21
    key = key ^ (key >>> 28);
    key = key + (key << 31);

    longHashCode ^= key;

    key = fastIntegerDigitCount;

    key = (~key) + (key << 21); // key = (key << 21) - key - 1;
    key = key ^ (key >>> 24);
    key = (key + (key << 3)) + (key << 8); // key * 265
    key = key ^ (key >>> 14);
    key = (key + (key << 2)) + (key << 4); // key * 21
    key = key ^ (key >>> 28);
    key = key + (key << 31);

    longHashCode ^= key;

    key = fastScale;

    key = (~key) + (key << 21); // key = (key << 21) - key - 1;
    key = key ^ (key >>> 24);
    key = (key + (key << 3)) + (key << 8); // key * 265
    key = key ^ (key >>> 14);
    key = (key + (key << 2)) + (key << 4); // key * 21
    key = key ^ (key >>> 28);
    key = key + (key << 31);

    longHashCode ^= key;

    return (int) longHashCode;
  }

  private static final int ZERO_NEW_FASTER_HASH_CODE = doCalculateNewFasterHashCode(0, 0, 0, 0, 0, 0);

  /**
   * Hash code based on (new) decimal representation.
   *
   * Faster than fastHashCode().
   *
   * Used by map join and other Hive internal purposes where performance is important.
   *
   * IMPORTANT: See comments for fastHashCode(), too.
   * @param fastSignum the sign (-1, 0, or +1)
   * @param fast0 word 0 of the internal representation
   * @param fast1 word 1
   * @param fast2 word 2
   * @param fastIntegerDigitCount the number of integer digits
   * @param fastScale the scale of the number
   * @return the hash code
   */
  public static int fastNewFasterHashCode(
      int fastSignum, long fast0, long fast1, long fast2, int fastIntegerDigitCount, int fastScale) {
    if (fastSignum == 0) {
      return ZERO_NEW_FASTER_HASH_CODE;
    }
    int hashCode = doCalculateNewFasterHashCode(fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale);
    return hashCode;
  }

  /**
   * This is the original hash code as returned by OldHiveDecimal.
   *
   * We need this when the OldHiveDecimal hash code has been exposed and and written or affected
   * how data is written.
   *
   * This method supports compatibility.
   *
   * Examples: bucketing and the Hive hash() function.
   *
   * NOTE: It is necessary to create a BigDecimal object and use its hash code, so this method is
   *       slow.
   *
   * @param fastSignum the sign (-1, 0, or +1)
   * @param fast0 word 0 of the internal representation
   * @param fast1 word 1
   * @param fast2 word 2
   * @param fastIntegerDigitCount the number of integer digits
   * @param fastScale the scale of the number
   * @return the hash code
   */
  public static int fastHashCode(
      int fastSignum, long fast0, long fast1, long fast2, int fastIntegerDigitCount, int fastScale) {

    // OldHiveDecimal returns the hash code of its internal BigDecimal.  Our TestHiveDecimal
    // verifies the OldHiveDecimal.bigDecimalValue() matches (new) HiveDecimal.bigDecimalValue().

    BigDecimal bigDecimal =
        fastBigDecimalValue(
            fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale);

    return bigDecimal.hashCode();
  }

  //************************************************************************************************
  // Decimal Math.

  public static boolean fastScaleByPowerOfTen(
      FastHiveDecimal fastDec,
      int power,
      FastHiveDecimal fastResult) {
    return fastScaleByPowerOfTen(
        fastDec.fastSignum, fastDec.fast0, fastDec.fast1, fastDec.fast2,
        fastDec.fastIntegerDigitCount, fastDec.fastScale,
        power,
        fastResult);
  }

  // NOTE: power can be positive or negative.
  // NOTE: e.g. power = 2 is effectively multiply by 10^2
  // NOTE:      and power = -3 is multiply by 10^-3
  public static boolean fastScaleByPowerOfTen(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale,
      int power,
      FastHiveDecimal fastResult) {

    if (fastSignum == 0) {
      fastResult.fastReset();
      return true;
    }
    if (power == 0) {
      fastResult.fastSet(fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale);
      /*
      if (!fastResult.fastIsValid()) {
        fastResult.fastRaiseInvalidException();
      }
      */
      return true;
    }

    final int absPower = Math.abs(power);

    if (power > 0) {

      int integerRoom;
      int fractionalRoom;
      if (fastIntegerDigitCount > 0) {

        // Is there integer room above?

        integerRoom = HiveDecimal.MAX_PRECISION - fastIntegerDigitCount;
        if (integerRoom < power) {
          return false;
        }
        fastResult.fastSignum = fastSignum;
        if (fastScale <= power) {

          // All fractional digits become integer digits.
          final int scaleUp = power - fastScale;
          if (scaleUp > 0) {
            if (!fastScaleUp(
                fast0, fast1, fast2,
                scaleUp,
                fastResult)) {
              throw new RuntimeException("Unexpected");
            }
          } else {
            fastResult.fast0 = fast0;
            fastResult.fast1 = fast1;
            fastResult.fast2 = fast2;
          }
          fastResult.fastIntegerDigitCount = fastIntegerDigitCount + fastScale + scaleUp;
          fastResult.fastScale = 0;

        } else {

          // Only a scale adjustment is needed.
          fastResult.fastSet(
              fastSignum, fast0, fast1, fast2,
              fastIntegerDigitCount + power, fastScale - power);
        }
      } else {

        // How much can the fraction be moved up?

        final int rawPrecision = fastRawPrecision(fastSignum, fast0, fast1, fast2);
        final int zeroesBelowDot = fastScale - rawPrecision;

        // Our limit is max precision integer digits + "leading" zeros below the dot.
        // E.g. 0.00021 has 3 zeroes below the dot.
        //
        if (power > HiveDecimal.MAX_PRECISION + zeroesBelowDot) {

          // Fractional part powered up too high.
          return false;
        }

        final int newIntegerDigitCount = Math.max(0, power - zeroesBelowDot);
        if (newIntegerDigitCount > rawPrecision) {

          fastResult.fastSignum = fastSignum;
          final int scaleUp = newIntegerDigitCount - rawPrecision;
          if (!fastScaleUp(
              fast0, fast1, fast2,
              scaleUp,
              fastResult)) {
            throw new RuntimeException("Unexpected");
          }
          fastResult.fastIntegerDigitCount = newIntegerDigitCount;
          fastResult.fastScale = 0;
        } else {
          final int newScale = Math.max(0, fastScale - power);
          fastResult.fastSet(fastSignum, fast0, fast1, fast2, newIntegerDigitCount, newScale);
        }
      }

    } else if (fastScale + absPower <= HiveDecimal.MAX_SCALE) {

      // Negative power with range -- adjust the scale.

      final int newScale = fastScale + absPower;
      final int newIntegerDigitCount = Math.max(0, fastIntegerDigitCount - absPower);

      final int trailingZeroCount =
          fastTrailingDecimalZeroCount(
              fast0, fast1, fast2,
              newIntegerDigitCount, newScale);
      if (trailingZeroCount > 0) {
        fastResult.fastSignum = fastSignum;
        doFastScaleDown(
            fast0, fast1, fast2,
            trailingZeroCount, fastResult);
        fastResult.fastScale = newScale - trailingZeroCount;
        fastResult.fastIntegerDigitCount = newIntegerDigitCount;
      } else {
        fastResult.fastSet(fastSignum, fast0, fast1, fast2,
            newIntegerDigitCount, newScale);
      }
    } else {

      // fastScale + absPower > HiveDecimal.MAX_SCALE

      // Look at getting rid of fractional digits that will now be below HiveDecimal.MAX_SCALE.

      final int scaleDown = fastScale + absPower - HiveDecimal.MAX_SCALE;

      if (scaleDown < HiveDecimal.MAX_SCALE) {
        if (!fastRoundFractionalHalfUp(
            fastSignum, fast0, fast1, fast2,
            scaleDown,
            fastResult)) {
          // Overflow.
          return false;
        }
        if (fastResult.fastSignum != 0) {

          fastResult.fastScale = HiveDecimal.MAX_SCALE;
          fastResult.fastIntegerDigitCount =
              Math.max(0, fastRawPrecision(fastResult) - fastResult.fastScale);

          final int trailingZeroCount =
              fastTrailingDecimalZeroCount(
                  fastResult.fast0, fastResult.fast1, fastResult.fast2,
                  fastResult.fastIntegerDigitCount, fastResult.fastScale);
          if (trailingZeroCount > 0) {
            doFastScaleDown(
                fastResult,
                trailingZeroCount,
                fastResult);
            fastResult.fastScale -= trailingZeroCount;
          }
        }
      } else {
        // All precision has been lost -- result is 0.
        fastResult.fastReset();
      }
    }

    /*
    if (!fastResult.fastIsValid()) {
      fastResult.fastRaiseInvalidException();
    }
    */

    return true;
  }

  //************************************************************************************************
  // Decimal Rounding.

  public static boolean doFastRound(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale,
      int roundPower,
      int roundingMode,
      FastHiveDecimal fastResult) {

    if (fastSignum == 0) {

      // Zero result.
      fastResult.fastReset();
      return true;
    } else if (fastScale == roundPower) {

      // The roundPower same as scale means all zeroes below round point.

      fastResult.fastSet(fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale);
      /*
      if (!fastResult.fastIsValid()) {
        fastResult.fastRaiseInvalidException();
      }
      */
      return true;
    }

    if (roundPower > fastScale) {

      // We pretend to add trailing zeroes, EVEN WHEN it would exceed the HiveDecimal.MAX_PRECISION.

      // Copy current value; do not change current scale.
      fastResult.fastSet(fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale);
      /*
      if (!fastResult.fastIsValid()) {
        fastResult.fastRaiseInvalidException();
      }
      */
    } else if (roundPower < 0) {

      // roundPower < 0
      //
      // Negative scale means we start rounding integer digits.
      //
      // The result will integer result will have at least abs(roundPower) trailing digits.
      //
      // Examples where the 'r's show the rounding digits:
      //
      //      round(12500, -3) = 13000           // BigDecimal.ROUND_HALF_UP
      //              rrr
      //
      // Or,  ceiling(12400.8302, -2) = 12500     // BigDecimal.ROUND_CEILING
      //                 rr rrrr
      //
      // Notice that any fractional digits will be gone in the result.
      //
     switch (roundingMode) {
      case BigDecimal.ROUND_DOWN:
        if (!fastRoundIntegerDown(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale,
            roundPower,
            fastResult)) {
          return false;
        }
        break;
      case BigDecimal.ROUND_UP:
        if (!fastRoundIntegerUp(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale,
            roundPower,
            fastResult)) {
          return false;
        }
        break;
      case BigDecimal.ROUND_FLOOR:
        // Round towards negative infinity.
        if (fastSignum == 1) {
          if (!fastRoundIntegerDown(
              fastSignum, fast0, fast1, fast2,
              fastIntegerDigitCount, fastScale,
              roundPower,
              fastResult)) {
            return false;
          }
        } else {
          if (!fastRoundIntegerUp(
              fastSignum, fast0, fast1, fast2,
              fastIntegerDigitCount, fastScale,
              roundPower,
              fastResult)) {
            return false;
          }
          if (fastResult.fast2 > MAX_HIGHWORD_DECIMAL) {
            return false;
          }
        }
        break;
      case BigDecimal.ROUND_CEILING:
        // Round towards positive infinity.
        if (fastSignum == 1) {
          if (!fastRoundIntegerUp(
              fastSignum, fast0, fast1, fast2,
              fastIntegerDigitCount, fastScale,
              roundPower,
              fastResult)) {
            return false;
          }
          if (fastResult.fast2 > MAX_HIGHWORD_DECIMAL) {
            return false;
          }
        } else {
          if (!fastRoundIntegerDown(
              fastSignum, fast0, fast1, fast2,
              fastIntegerDigitCount, fastScale,
              roundPower,
              fastResult)) {
            return false;
          }
        }
        break;
      case BigDecimal.ROUND_HALF_UP:
        if (!fastRoundIntegerHalfUp(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale,
            roundPower,
            fastResult)) {
          return false;
        }
        if (fastResult.fast2 > MAX_HIGHWORD_DECIMAL) {
          return false;
        }
        break;
      case BigDecimal.ROUND_HALF_EVEN:
        if (!fastRoundIntegerHalfEven(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale,
            roundPower,
            fastResult)) {
          return false;
        }
        if (fastResult.fast2 > MAX_HIGHWORD_DECIMAL) {
          return false;
        }
        break;
      default:
        throw new RuntimeException("Unsupported rounding mode " + roundingMode);
      }

      // The fastRoundInteger* methods remove all fractional digits, set fastIntegerDigitCount, and
      // set fastScale to 0.
      return true;

    } else {

      // roundPower < fastScale

      // Do rounding of fractional digits.
      final int scaleDown = fastScale - roundPower;
      switch (roundingMode) {
      case BigDecimal.ROUND_DOWN:
        fastRoundFractionalDown(
            fastSignum, fast0, fast1, fast2,
            scaleDown,
            fastResult);
        break;
      case BigDecimal.ROUND_UP:
        if (!fastRoundFractionalUp(
            fastSignum, fast0, fast1, fast2,
            scaleDown,
            fastResult)) {
          return false;
        }
        break;
      case BigDecimal.ROUND_FLOOR:
        // Round towards negative infinity.
        if (fastSignum == 1) {
          fastRoundFractionalDown(
              fastSignum, fast0, fast1, fast2,
              scaleDown,
              fastResult);
        } else {
          if (!fastRoundFractionalUp(
              fastSignum, fast0, fast1, fast2,
              scaleDown,
              fastResult)) {
            return false;
          }
        }
        break;
      case BigDecimal.ROUND_CEILING:
        // Round towards positive infinity.
        if (fastSignum == 1) {
          if (!fastRoundFractionalUp(
              fastSignum, fast0, fast1, fast2,
              scaleDown,
              fastResult)) {
            return false;
          }
        } else {
          fastRoundFractionalDown(
              fastSignum, fast0, fast1, fast2,
              scaleDown,
              fastResult);
        }
        break;
      case BigDecimal.ROUND_HALF_UP:
        if (!fastRoundFractionalHalfUp(
            fastSignum, fast0, fast1, fast2,
            scaleDown,
            fastResult)) {
          return false;
        }
        break;
      case BigDecimal.ROUND_HALF_EVEN:
        if (!fastRoundFractionalHalfEven(
            fastSignum, fast0, fast1, fast2,
            scaleDown,
            fastResult)) {
          return false;
        }
        break;
      default:
        throw new RuntimeException("Unsupported rounding mode " + roundingMode);
      }
      if (fastResult.fastSignum == 0) {
        fastResult.fastScale = 0;
        /*
        if (!fastResult.fastIsValid()) {
          fastResult.fastRaiseInvalidException();
        }
        */
      } else {
        final int rawPrecision = fastRawPrecision(fastResult);
        fastResult.fastIntegerDigitCount = Math.max(0, rawPrecision - roundPower);
        fastResult.fastScale = roundPower;

        // Trim trailing zeroes and re-adjust scale.
        final int trailingZeroCount =
            fastTrailingDecimalZeroCount(
                fastResult.fast0, fastResult.fast1, fastResult.fast2,
                fastResult.fastIntegerDigitCount, fastResult.fastScale);
        if (trailingZeroCount > 0) {
          doFastScaleDown(
              fastResult,
              trailingZeroCount,
              fastResult);
          fastResult.fastScale -= trailingZeroCount;
          /*
          if (!fastResult.fastIsValid()) {
            fastResult.fastRaiseInvalidException();
          }
          */
        }
      }
    }

    return true;
  }

  public static boolean fastRound(
      FastHiveDecimal fastDec,
      int newScale, int roundingMode,
      FastHiveDecimal fastResult) {
    return fastRound(
        fastDec.fastSignum, fastDec.fast0, fastDec.fast1, fastDec.fast2,
        fastDec.fastIntegerDigitCount, fastDec.fastScale,
        newScale, roundingMode,
        fastResult);
  }

  public static boolean fastRound(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale,
      int newScale, int roundingMode,
      FastHiveDecimal fastResult) {
    return doFastRound(
        fastSignum, fast0, fast1, fast2,
        fastIntegerDigitCount, fastScale,
        newScale, roundingMode,
        fastResult);
  }

  private static boolean isRoundPortionAllZeroes(
      long fast0, long fast1, long fast2,
      int roundingPoint) {

    boolean isRoundPortionAllZeroes;
    if (roundingPoint < LONGWORD_DECIMAL_DIGITS) {

      // Lowest word gets integer rounding.

      // Factor includes scale.
      final long roundPointFactor = powerOfTenTable[roundingPoint];

      isRoundPortionAllZeroes = (fast0 % roundPointFactor == 0);

    } else if (roundingPoint < TWO_X_LONGWORD_DECIMAL_DIGITS) {

      // Middle word gets integer rounding.

      final int adjustedRoundingPoint = roundingPoint - LONGWORD_DECIMAL_DIGITS;

      if (adjustedRoundingPoint == 0) {
        isRoundPortionAllZeroes = (fast0 == 0);
      } else {

        // Factor includes scale.
        final long roundPointFactor = powerOfTenTable[adjustedRoundingPoint];

        final long roundPortion = fast1 % roundPointFactor;
        isRoundPortionAllZeroes = (fast0 == 0 && roundPortion == 0);
      }

    } else {

      // High word gets integer rounding.

      final int adjustedRoundingPoint = roundingPoint - TWO_X_LONGWORD_DECIMAL_DIGITS;

      if (adjustedRoundingPoint == 0) {
        isRoundPortionAllZeroes = (fast0 == 0 && fast1 == 0);
      } else {

        // Factor includes scale.
        final long roundPointFactor = powerOfTenTable[adjustedRoundingPoint];

        final long roundPortion = fast2 % roundPointFactor;
        isRoundPortionAllZeroes = (fast0 == 0 && fast1 == 0 && roundPortion == 0);
      }
    }
    return isRoundPortionAllZeroes;
  }

  private static boolean isRoundPortionHalfUp(
      long fast0, long fast1, long fast2,
      int roundingPoint) {

    boolean isRoundPortionHalfUp;
    if (roundingPoint < LONGWORD_DECIMAL_DIGITS) {

      // Lowest word gets integer rounding.

      // Divide down just before round point to get round digit.
      final long withRoundDigit = fast0 / powerOfTenTable[roundingPoint - 1];
      final long roundDigit = withRoundDigit % 10;

      isRoundPortionHalfUp = (roundDigit >= 5);

    } else if (roundingPoint < TWO_X_LONGWORD_DECIMAL_DIGITS) {

      // Middle word gets integer rounding.

      final int adjustedRoundingPoint = roundingPoint - LONGWORD_DECIMAL_DIGITS;

      long roundDigit;
      if (adjustedRoundingPoint == 0) {
        // Grab round digit from lowest word.
        roundDigit = fast0 / (MULTIPLER_LONGWORD_DECIMAL / 10);
      } else {
        // Divide down just before scaleDown to get round digit.
        final long withRoundDigit = fast1 / powerOfTenTable[adjustedRoundingPoint - 1];
        roundDigit = withRoundDigit % 10;
      }

      isRoundPortionHalfUp = (roundDigit >= 5);

    } else {

      // High word gets integer rounding.

      final int adjustedRoundingPoint = roundingPoint - TWO_X_LONGWORD_DECIMAL_DIGITS;

      long roundDigit;
      if (adjustedRoundingPoint == 0) {
        // Grab round digit from middle word.
        roundDigit = fast1 / (MULTIPLER_LONGWORD_DECIMAL / 10);
      } else {
        // Divide down just before scaleDown to get round digit.
        final long withRoundDigit = fast2 / powerOfTenTable[adjustedRoundingPoint - 1];
        roundDigit = withRoundDigit % 10;
      }

      isRoundPortionHalfUp = (roundDigit >= 5);

    }
    return isRoundPortionHalfUp;
  }

  private static boolean isRoundPortionHalfEven(
      long fast0, long fast1, long fast2,
      int roundingPoint) {

    boolean isRoundPortionHalfEven;
    if (roundingPoint < LONGWORD_DECIMAL_DIGITS) {

      // Lowest word gets integer rounding.

      // Divide down just before scaleDown to get round digit.
      final long roundDivisor = powerOfTenTable[roundingPoint - 1];
      final long withRoundDigit = fast0 / roundDivisor;
      final long roundDigit = withRoundDigit % 10;
      final long fast0Scaled = withRoundDigit / 10;

      if (roundDigit > 5) {
        isRoundPortionHalfEven = true;
      } else if (roundDigit == 5) {
        boolean exactlyOneHalf;
        if (roundingPoint - 1 == 0) {
          // Fraction below 0.5 is implicitly 0.
          exactlyOneHalf = true;
        } else {
          exactlyOneHalf = (fast0 % roundDivisor == 0);
        }

        // When fraction is exactly 0.5 and lowest new digit is odd, go towards even.
        if (exactlyOneHalf) {
          isRoundPortionHalfEven = (fast0Scaled % 2 == 1);
        } else {
          isRoundPortionHalfEven = true;
        }
      } else {
        isRoundPortionHalfEven = false;
      }

    } else if (roundingPoint < TWO_X_LONGWORD_DECIMAL_DIGITS) {

      // Middle word gets integer rounding.

      final int adjustedRoundingPoint = roundingPoint - LONGWORD_DECIMAL_DIGITS;

      long roundDigit;
      long fast1Scaled;
      if (adjustedRoundingPoint == 0) {
        // Grab round digit from lowest word.
        final long roundDivisor = MULTIPLER_LONGWORD_DECIMAL / 10;
        roundDigit = fast0 / roundDivisor;
        fast1Scaled = fast1;
        if (roundDigit > 5) {
          isRoundPortionHalfEven = true;
        } else if (roundDigit == 5) {
          boolean exactlyOneHalf = (fast0 % roundDivisor == 0);

          // When fraction is exactly 0.5 and lowest new digit is odd, go towards even.
          if (exactlyOneHalf) {
            isRoundPortionHalfEven = (fast1Scaled % 2 == 1);
          } else {
            isRoundPortionHalfEven = true;
          }
        } else {
          isRoundPortionHalfEven = false;
        }
      } else {
        // Divide down just before scaleDown to get round digit.
        final long roundDivisor = powerOfTenTable[adjustedRoundingPoint - 1];
        final long withRoundDigit = fast1 / roundDivisor;
        roundDigit = withRoundDigit % 10;
        fast1Scaled = withRoundDigit / 10;
        if (roundDigit > 5) {
          isRoundPortionHalfEven = true;
        } else if (roundDigit == 5) {
          boolean exactlyOneHalf;
          if (adjustedRoundingPoint - 1 == 0) {
            // Just examine the lower word.
            exactlyOneHalf = (fast0 == 0);
          } else {
            exactlyOneHalf = (fast0 == 0 && fast1 % roundDivisor == 0);
          }

          // When fraction is exactly 0.5 and lowest new digit is odd, go towards even.
          if (exactlyOneHalf) {
            isRoundPortionHalfEven = (fast1Scaled % 2 == 1);
          } else {
            isRoundPortionHalfEven = true;
          }
        } else {
          isRoundPortionHalfEven = false;
        }
      }

    } else {

      // High word gets integer rounding.

      final int adjustedRoundingPoint = roundingPoint - TWO_X_LONGWORD_DECIMAL_DIGITS;

      long roundDigit;
      long fast2Scaled;
      if (adjustedRoundingPoint == 0) {
        // Grab round digit from middle word.
        final long roundDivisor = MULTIPLER_LONGWORD_DECIMAL / 10;
        roundDigit = fast1 / roundDivisor;
        fast2Scaled = fast2;
        if (roundDigit > 5) {
          isRoundPortionHalfEven = true;
        } else if (roundDigit == 5) {
          boolean exactlyOneHalf = (fast1 % roundDivisor == 0 && fast0 == 0);

          // When fraction is exactly 0.5 and lowest new digit is odd, go towards even.
          if (exactlyOneHalf) {
            isRoundPortionHalfEven = (fast2Scaled % 2 == 1);
          } else {
            isRoundPortionHalfEven = true;
          }
        } else {
          isRoundPortionHalfEven = false;
        }
      } else {
        // Divide down just before scaleDown to get round digit.
        final long roundDivisor = powerOfTenTable[adjustedRoundingPoint - 1];
        final long withRoundDigit = fast2 / roundDivisor;
        roundDigit = withRoundDigit % 10;
        fast2Scaled = withRoundDigit / 10;
        if (roundDigit > 5) {
          isRoundPortionHalfEven = true;
        } else if (roundDigit == 5) {
          boolean exactlyOneHalf;
          if (adjustedRoundingPoint - 1 == 0) {
            // Just examine the middle and lower words.
            exactlyOneHalf = (fast1 == 0 && fast0 == 0);
          } else {
            exactlyOneHalf = (fast2 % roundDivisor == 0 && fast1 == 0 && fast0 == 0);
          }

          // When fraction is exactly 0.5 and lowest new digit is odd, go towards even.
          if (exactlyOneHalf) {
            isRoundPortionHalfEven = (fast2Scaled % 2 == 1);
          } else {
            isRoundPortionHalfEven = true;
          }
        } else {
          isRoundPortionHalfEven = false;
        }
      }
    }
    return isRoundPortionHalfEven;
  }

  private static void doClearRoundIntegerPortionAndAddOne(
      long fast0, long fast1, long fast2,
      int absRoundPower,
      FastHiveDecimal fastResult) {

    long result0;
    long result1;
    long result2;

    if (absRoundPower < LONGWORD_DECIMAL_DIGITS) {

      // Lowest word gets integer rounding.

      // Clear rounding portion in lower longword and add 1 at right scale (roundMultiplyFactor).

      final long roundFactor = powerOfTenTable[absRoundPower];

      final long r0 =
          ((fast0 / roundFactor) * roundFactor)
        + roundFactor;
      result0 = r0 % MULTIPLER_LONGWORD_DECIMAL;
      final long r1 =
          fast1
        + r0 / MULTIPLER_LONGWORD_DECIMAL;
      result1 = r1 % MULTIPLER_LONGWORD_DECIMAL;
      result2 =
          fast2
        + r1 / MULTIPLER_LONGWORD_DECIMAL;

    } else if (absRoundPower < TWO_X_LONGWORD_DECIMAL_DIGITS) {

      // Middle word gets integer rounding; lower longword is cleared.

      final int adjustedAbsPower = absRoundPower - LONGWORD_DECIMAL_DIGITS;

      // Clear rounding portion in middle longword and add 1 at right scale (roundMultiplyFactor);
      // lower longword result is 0;

      final long roundFactor = powerOfTenTable[adjustedAbsPower];

      result0 = 0;
      final long r1 =
          ((fast1 / roundFactor) * roundFactor)
        + roundFactor;
      result1 = r1 % MULTIPLER_LONGWORD_DECIMAL;
      result2 =
          fast2
        + r1 / MULTIPLER_LONGWORD_DECIMAL;

    } else {

      // High word gets integer rounding; middle and lower longwords are cleared.

      final int adjustedAbsPower = absRoundPower - TWO_X_LONGWORD_DECIMAL_DIGITS;

      // Clear rounding portion in high longword and add 1 at right scale (roundMultiplyFactor);
      // middle and lower longwords result is 0;

      final long roundFactor = powerOfTenTable[adjustedAbsPower];

      result0 = 0;
      result1 = 0;
      result2 =
          ((fast2 / roundFactor) * roundFactor)
        + roundFactor;

    }

    fastResult.fast0 = result0;
    fastResult.fast1 = result1;
    fastResult.fast2 = result2;
  }

  private static void doClearRoundIntegerPortion(
      long fast0, long fast1, long fast2,
      int absRoundPower,
      FastHiveDecimal fastResult) {

    long result0;
    long result1;
    long result2;

    if (absRoundPower < LONGWORD_DECIMAL_DIGITS) {

      // Lowest word gets integer rounding.

      // Clear rounding portion in lower longword and add 1 at right scale (roundMultiplyFactor).

      final long roundFactor = powerOfTenTable[absRoundPower];
      // final long roundMultiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - absRoundPower];

      result0 =
          ((fast0 / roundFactor) * roundFactor);
      result1 = fast1;
      result2 = fast2;

    } else if (absRoundPower < TWO_X_LONGWORD_DECIMAL_DIGITS) {

      // Middle word gets integer rounding; lower longword is cleared.

      final int adjustedAbsPower = absRoundPower - LONGWORD_DECIMAL_DIGITS;

      // Clear rounding portion in middle longword and add 1 at right scale (roundMultiplyFactor);
      // lower longword result is 0;

      final long roundFactor = powerOfTenTable[adjustedAbsPower];

      result0 = 0;
      result1 =
          ((fast1 / roundFactor) * roundFactor);
      result2 = fast2;

    } else {

      // High word gets integer rounding; middle and lower longwords are cleared.

      final int adjustedAbsPower = absRoundPower - TWO_X_LONGWORD_DECIMAL_DIGITS;

      // Clear rounding portion in high longword and add 1 at right scale (roundMultiplyFactor);
      // middle and lower longwords result is 0;

      final long roundFactor = powerOfTenTable[adjustedAbsPower];

      result0 = 0;
      result1 = 0;
      result2 =
          ((fast2 / roundFactor) * roundFactor);

    }

    fastResult.fast0 = result0;
    fastResult.fast1 = result1;
    fastResult.fast2 = result2;
  }

  /**
   * Fast decimal integer part rounding ROUND_UP.
   *
   * ceiling(12400.8302, -2) = 12500     // E.g. Positive case FAST_ROUND_CEILING
   *            rr rrrr
   *
   * @param fastSignum the sign (-1, 0, or +1)
   * @param fast0 word 0 of the internal representation
   * @param fast1 word 1
   * @param fast2 word 2
   * @param fastIntegerDigitCount the number of integer digits
   * @param fastScale the scale of the number
   * @param roundPower the power to round to
   * @param fastResult an object to reuse
   * @return was the operation successful
   */
  public static boolean fastRoundIntegerUp(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale,
      int roundPower,
      FastHiveDecimal fastResult) {

    /*
     * Basic algorithm:
     *
     * 1. Determine if rounding part is non-zero for rounding.
     * 2. Scale away fractional digits if present.
     * 3. If rounding, clear integer rounding portion and add 1.
     *
     */

    if (roundPower >= 0) {
      throw new IllegalArgumentException("Expecting roundPower < 0 (roundPower " + roundPower + ")");
    }

    final int absRoundPower = -roundPower;
    if (fastIntegerDigitCount < absRoundPower) {

      // Above decimal.
      return false;
    }

    final int roundingPoint = absRoundPower + fastScale;
    if (roundingPoint > HiveDecimal.MAX_PRECISION) {

      // Value becomes null for rounding beyond.
      return false;
    }

    // First, determine whether rounding is necessary based on rounding point, which is inside
    // integer part.  And, get rid of any fractional digits.  The result scale will be 0.
    //
    boolean isRoundPortionAllZeroes =
        isRoundPortionAllZeroes(
            fast0, fast1, fast2,
            roundingPoint);

    // If necessary, divide and multiply to get rid of fractional digits.
    if (fastScale == 0) {
      fastResult.fast0 = fast0;
      fastResult.fast1 = fast1;
      fastResult.fast2 = fast2;
    } else {
      doFastScaleDown(
          fast0, fast1, fast2,
          /* scaleDown */ fastScale,
          fastResult);
    }

    // The fractional digits are gone; when rounding, clear remaining round digits and add 1.
    if (!isRoundPortionAllZeroes) {

      doClearRoundIntegerPortionAndAddOne(
          fastResult.fast0, fastResult.fast1, fastResult.fast2,
          absRoundPower,
          fastResult);
    }

    if (fastResult.fast0 == 0 && fastResult.fast1 == 0 && fastResult.fast2 == 0) {
      fastResult.fastSignum = 0;
      fastResult.fastIntegerDigitCount = 0;
      fastResult.fastScale = 0;
    } else {
      fastResult.fastSignum = fastSignum;
      fastResult.fastIntegerDigitCount = fastRawPrecision(fastResult);
      fastResult.fastScale = 0;
    }

    return true;
  }

  /**
   * Fast decimal scale down by factor of 10 with rounding ROUND_DOWN.
   *
   * The fraction being scaled away is thrown away.
   *
   * The signum will be updated if the result is 0, otherwise the original sign is unchanged.
   *
   * @param fastSignum the sign (-1, 0, or +1)
   * @param fast0 word 0 of the internal representation
   * @param fast1 word 1
   * @param fast2 word 2
   * @param fastIntegerDigitCount the number of integer digits
   * @param fastScale the scale of the number
   * @param roundPower the power to round to
   * @param fastResult an object to reuse
   * @return was the operation successful?
   */
  public static boolean fastRoundIntegerDown(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale,
      int roundPower,
      FastHiveDecimal fastResult) {

    /*
     * Basic algorithm:
     *
     * 1. Scale away fractional digits if present.
     * 2. Clear integer rounding portion.
     *
     */

    if (roundPower >= 0) {
      throw new IllegalArgumentException("Expecting roundPower < 0 (roundPower " + roundPower + ")");
    }

    final int absRoundPower = -roundPower;
    if (fastIntegerDigitCount < absRoundPower) {

      // Zero result.
      fastResult.fastReset();
      return true;
    }

    final int roundingPoint = absRoundPower + fastScale;
    if (roundingPoint > HiveDecimal.MAX_PRECISION) {

      // Value becomes zero for rounding beyond.
      fastResult.fastReset();
      return true;
    }

    // If necessary, divide and multiply to get rid of fractional digits.
    if (fastScale == 0) {
      fastResult.fast0 = fast0;
      fastResult.fast1 = fast1;
      fastResult.fast2 = fast2;
    } else {
      doFastScaleDown(
          fast0, fast1, fast2,
          /* scaleDown */ fastScale,
          fastResult);
    }

    // The fractional digits are gone; clear remaining round digits.
    doClearRoundIntegerPortion(
        fastResult.fast0, fastResult.fast1, fastResult.fast2,
        absRoundPower,
        fastResult);

    if (fastResult.fast0 == 0 && fastResult.fast1 == 0 && fastResult.fast2 == 0) {
      fastResult.fastIntegerDigitCount = 0;
      fastResult.fastScale = 0;
    } else {
      fastResult.fastSignum = fastSignum;
      fastResult.fastIntegerDigitCount = fastRawPrecision(fastResult);
      fastResult.fastScale = 0;
    }

    return true;
  }

  /**
   * Fast decimal scale down by factor of 10 with rounding ROUND_HALF_UP.
   *
   * When the fraction being scaled away is &gt;= 0.5, the add 1.
   *
   * @param fastSignum the sign (-1, 0, or +1)
   * @param fast0 word 0 of the internal representation
   * @param fast1 word 1
   * @param fast2 word 2
   * @param fastIntegerDigitCount the number of integer digits
   * @param fastScale the scale of the number
   * @param roundPower the power to round to
   * @param fastResult an object to reuse
   * @return was the operation successful?
   */
  public static boolean fastRoundIntegerHalfUp(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale,
      int roundPower,
      FastHiveDecimal fastResult) {

    /*
     * Basic algorithm:
     *
     * 1. Determine if rounding digit is >= 5 for rounding.
     * 2. Scale away fractional digits if present.
     * 3. If rounding, clear integer rounding portion and add 1.
     *
     */

    if (roundPower >= 0) {
      throw new IllegalArgumentException("Expecting roundPower < 0 (roundPower " + roundPower + ")");
    }

    final int absRoundPower = -roundPower;
    if (fastIntegerDigitCount < absRoundPower) {

      // Zero result.
      fastResult.fastReset();
      return true;
    }

    final int roundingPoint = absRoundPower + fastScale;
    if (roundingPoint > HiveDecimal.MAX_PRECISION) {

      // Value becomes zero for rounding beyond.
      fastResult.fastReset();
      return true;
    }

    // First, determine whether rounding is necessary based on rounding point, which is inside
    // integer part.  And, get rid of any fractional digits.  The result scale will be 0.
    //
    boolean isRoundPortionHalfUp =
        isRoundPortionHalfUp(
            fast0, fast1, fast2,
            roundingPoint);

    // If necessary, divide and multiply to get rid of fractional digits.
    if (fastScale == 0) {
      fastResult.fast0 = fast0;
      fastResult.fast1 = fast1;
      fastResult.fast2 = fast2;
    } else {
      doFastScaleDown(
          fast0, fast1, fast2,
          /* scaleDown */ fastScale,
          fastResult);
    }

    // The fractional digits are gone; when rounding, clear remaining round digits and add 1.
    if (isRoundPortionHalfUp) {

      doClearRoundIntegerPortionAndAddOne(
          fastResult.fast0, fastResult.fast1, fastResult.fast2,
          absRoundPower,
          fastResult);
    } else {

      doClearRoundIntegerPortion(
          fastResult.fast0, fastResult.fast1, fastResult.fast2,
          absRoundPower,
          fastResult);
    }

    if (fastResult.fast0 == 0 && fastResult.fast1 == 0 && fastResult.fast2 == 0) {
      fastResult.fastSignum = 0;
      fastResult.fastIntegerDigitCount = 0;
      fastResult.fastScale = 0;
    } else {
      fastResult.fastSignum = fastSignum;
      fastResult.fastIntegerDigitCount = fastRawPrecision(fastResult);
      fastResult.fastScale = 0;
    }

    return true;
  }

  /**
   * Fast decimal scale down by factor of 10 with rounding ROUND_HALF_EVEN.
   *
   * When the fraction being scaled away is exactly 0.5, then round and add 1 only if aaa.
   * When fraction is not exactly 0.5, then if fraction &gt; 0.5 then add 1.
   * Otherwise, throw away fraction.
   *
   * The signum will be updated if the result is 0, otherwise the original sign is unchanged.
   *
   * @param fastSignum the sign (-1, 0, or +1)
   * @param fast0 word 0 of the internal representation
   * @param fast1 word 1
   * @param fast2 word 2
   * @param fastIntegerDigitCount the number of integer digits
   * @param fastScale the scale of the number
   * @param roundPower the power to round to
   * @param fastResult an object to reuse
   * @return was the operation successful?
   */
  public static boolean fastRoundIntegerHalfEven(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale,
      int roundPower,
      FastHiveDecimal fastResult) {

    /*
     * Basic algorithm:
     *
     * 1. Determine if rounding part meets banker's rounding rules for rounding.
     * 2. Scale away fractional digits if present.
     * 3. If rounding, clear integer rounding portion and add 1.
     *
     */

    if (roundPower >= 0) {
      throw new IllegalArgumentException("Expecting roundPower < 0 (roundPower " + roundPower + ")");
    }

    final int absRoundPower = -roundPower;
    if (fastIntegerDigitCount < absRoundPower) {

      // Zero result.
      fastResult.fastReset();
    }

    final int roundingPoint = absRoundPower + fastScale;
    if (roundingPoint > HiveDecimal.MAX_PRECISION) {

      // Value becomes zero for rounding beyond.
      fastResult.fastReset();
      return true;
    }

    // First, determine whether rounding is necessary based on rounding point, which is inside
    // integer part.  And, get rid of any fractional digits.  The result scale will be 0.
    //
    boolean isRoundPortionHalfEven =
        isRoundPortionHalfEven(
            fast0, fast1, fast2,
            roundingPoint);

    // If necessary, divide and multiply to get rid of fractional digits.
    if (fastScale == 0) {
      fastResult.fast0 = fast0;
      fastResult.fast1 = fast1;
      fastResult.fast2 = fast2;
    } else {
      doFastScaleDown(
          fast0, fast1, fast2,
          /* scaleDown */ fastScale,
          fastResult);
    }

    // The fractional digits are gone; when rounding, clear remaining round digits and add 1.
    if (isRoundPortionHalfEven) {

      doClearRoundIntegerPortionAndAddOne(
          fastResult.fast0, fastResult.fast1, fastResult.fast2,
          absRoundPower,
          fastResult);
    } else {

      doClearRoundIntegerPortion(
          fastResult.fast0, fastResult.fast1, fastResult.fast2,
          absRoundPower,
          fastResult);
    }

    if (fastResult.fast0 == 0 && fastResult.fast1 == 0 && fastResult.fast2 == 0) {
      fastResult.fastSignum = 0;
      fastResult.fastIntegerDigitCount = 0;
      fastResult.fastScale = 0;
    } else {
      fastResult.fastSignum = fastSignum;
      fastResult.fastIntegerDigitCount = fastRawPrecision(fastResult);
      fastResult.fastScale = 0;
    }

    return true;
  }

  /**
   * Fast decimal scale down by factor of 10 and do not allow rounding.
   *
   * When the fraction being scaled away is non-zero, return false.
   *
   * The signum will be updated if the result is 0, otherwise the original sign
   * is unchanged.
   *
   * @param fastSignum the sign (-1, 0, or +1)
   * @param fast0 word 0 of the internal representation
   * @param fast1 word 1
   * @param fast2 word 2
   * @param scaleDown the digits to scale down by
   * @param fastResult an object to reuse
   * @return was the operation successful?
   */
  public static boolean fastScaleDownNoRound(
      int fastSignum, long fast0, long fast1, long fast2,
      int scaleDown,
      FastHiveDecimal fastResult) {
    if (scaleDown < 1 || scaleDown >= THREE_X_LONGWORD_DECIMAL_DIGITS - 1) {
      throw new IllegalArgumentException("Expecting scaleDown > 0 and scaleDown < 3*16 - 1 (scaleDown " + scaleDown + ")");
    }

    // Adjust all longs using power 10 division/remainder.
    long result0;
    long result1;
    long result2;
    if (scaleDown < LONGWORD_DECIMAL_DIGITS) {

      // Part of lowest word survives.

      final long divideFactor = powerOfTenTable[scaleDown];
      final long multiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - scaleDown];

      final long throwAwayFraction = fast0 % divideFactor;

      if (throwAwayFraction != 0) {
        return false;
      }
      result0 =
          fast0 / divideFactor
        + ((fast1 % divideFactor) * multiplyFactor);
      result1 =
          fast1 / divideFactor
        + ((fast2 % divideFactor) * multiplyFactor);
      result2 =
          fast2 / divideFactor;

    } else if (scaleDown < TWO_X_LONGWORD_DECIMAL_DIGITS) {

      // Throw away lowest word.

      final int adjustedScaleDown = scaleDown - LONGWORD_DECIMAL_DIGITS;

      final long divideFactor = powerOfTenTable[adjustedScaleDown];
      final long multiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - adjustedScaleDown];

      boolean isThrowAwayFractionZero;
      if (adjustedScaleDown == 0) {
        isThrowAwayFractionZero = (fast0 == 0);
      } else {
        final long throwAwayFraction = fast1 % divideFactor;
        isThrowAwayFractionZero = (throwAwayFraction == 0 && fast0 == 0);
      }

      if (!isThrowAwayFractionZero) {
        return false;
      }
      result0 =
          fast1 / divideFactor
        + ((fast2 % divideFactor) * multiplyFactor);
      result1 =
          fast2 / divideFactor;
      result2 = 0;

    } else {

      // Throw away middle and lowest words.

      final int adjustedScaleDown = scaleDown - 2*LONGWORD_DECIMAL_DIGITS;

      final long divideFactor = powerOfTenTable[adjustedScaleDown];

      boolean isThrowAwayFractionZero;
      if (adjustedScaleDown == 0) {
        isThrowAwayFractionZero = (fast0 == 0 && fast1 == 0);
      } else {
        final long throwAwayFraction = fast2 % divideFactor;
        isThrowAwayFractionZero = (throwAwayFraction == 0 && fast0 == 0 && fast1 == 0);
      }

      if (!isThrowAwayFractionZero) {
        return false;
      }
      result0 =
          fast2 / divideFactor;
      result1 = 0;
      result2 = 0;
    }

    if (result0 == 0 && result1 == 0 && result2 == 0) {
      fastResult.fastReset();
    } else {
      fastResult.fastSignum = fastSignum;
      fastResult.fast0 = result0;
      fastResult.fast1 = result1;
      fastResult.fast2 = result2;
    }

    return true;
  }

  /**
   * Fast decimal scale down by factor of 10 with rounding ROUND_UP.
   *
   * When the fraction being scaled away is non-zero, the add 1.
   *
   * @param fastSignum the sign (-1, 0, or +1)
   * @param fast0 word 0 of the internal representation
   * @param fast1 word 1
   * @param fast2 word 2
   * @param scaleDown the number of integer digits to scale
   * @param fastResult an object to reuse
   * @return was the operation successfule?
   */
  public static boolean fastRoundFractionalUp(
      int fastSignum, long fast0, long fast1, long fast2,
      int scaleDown,
      FastHiveDecimal fastResult) {
    if (scaleDown < 1 || scaleDown > HiveDecimal.MAX_SCALE) {
      throw new IllegalArgumentException("Expecting scaleDown > 0 and scaleDown < " + HiveDecimal.MAX_SCALE + " (scaleDown " + scaleDown + ")");
    }

    if (scaleDown == HiveDecimal.MAX_SCALE) {

      // Examine all digits being thrown away to determine if result is 0 or 1.
      if (fast0 == 0 && fast1 == 0 && fast2 == 0) {

        // Zero result.
        fastResult.fastReset();
      } else {
        fastResult.fastSet(fastSignum, /* fast0 */ 1, 0, 0, /* fastIntegerDigitCount */ 1, 0);
      }
      return true;
    }

    boolean isRoundPortionAllZeroes =
        isRoundPortionAllZeroes(
            fast0, fast1, fast2,
            scaleDown);

    doFastScaleDown(
        fast0, fast1, fast2,
        scaleDown,
        fastResult);

    if (!isRoundPortionAllZeroes) {
      final long r0 = fastResult.fast0 + 1;
      fastResult.fast0 =
          r0 % MULTIPLER_LONGWORD_DECIMAL;
      final long r1 =
          fastResult.fast1
        + r0 / MULTIPLER_LONGWORD_DECIMAL;
      fastResult.fast1 =
          r1 % MULTIPLER_LONGWORD_DECIMAL;
      fastResult.fast2 =
          fastResult.fast2
        + r1 / MULTIPLER_LONGWORD_DECIMAL;
    }

    if (fastResult.fast0 == 0 && fastResult.fast1 == 0 && fastResult.fast2 == 0) {
      fastResult.fastSignum = 0;
      fastResult.fastIntegerDigitCount = 0;
      fastResult.fastScale = 0;
    } else {
      fastResult.fastSignum = fastSignum;
    }

    return (fastResult.fast2 <= MAX_HIGHWORD_DECIMAL);
  }

  /**
   * Fast decimal scale down by factor of 10 with rounding ROUND_DOWN.
   *
   * The fraction being scaled away is thrown away.
   *
   * @param fastSignum the sign (-1, 0, or +1)
   * @param fast0 word 0 of the internal representation
   * @param fast1 word 1
   * @param fast2 word 2
   * @param scaleDown the number of integer digits to scale
   * @param fastResult an object to reuse
   */
  public static void fastRoundFractionalDown(
      int fastSignum, long fast0, long fast1, long fast2,
      int scaleDown,
      FastHiveDecimal fastResult) {
    if (scaleDown < 1 || scaleDown > HiveDecimal.MAX_SCALE) {
      throw new IllegalArgumentException("Expecting scaleDown > 0 and scaleDown < 38 (scaleDown " + scaleDown + ")");
    }

    if (scaleDown == HiveDecimal.MAX_SCALE) {

      // Complete fractional digits shear off.  Zero result.
      fastResult.fastReset();
      return;
    }

    doFastScaleDown(
        fast0, fast1, fast2,
        scaleDown,
        fastResult);

    if (fastResult.fast0 == 0 && fastResult.fast1 == 0 && fastResult.fast2 == 0) {
      fastResult.fastSignum = 0;
      fastResult.fastIntegerDigitCount = 0;
      fastResult.fastScale = 0;
    } else {
      fastResult.fastSignum = fastSignum;
    }
  }

  /**
   * Fast decimal scale down by factor of 10 with rounding ROUND_HALF_UP.
   *
   * When the fraction being scaled away is &gt;= 0.5, the add 1.
   *
   * @param fastSignum the sign (-1, 0, or +1)
   * @param fast0 word 0 of the internal representation
   * @param fast1 word 1
   * @param fast2 word 2
   * @param scaleDown the number of integer digits to scale
   * @param fastResult an object to reuse
   * @return was the operation successfule?
   */
  public static boolean fastRoundFractionalHalfUp(
      int fastSignum, long fast0, long fast1, long fast2,
      int scaleDown,
      FastHiveDecimal fastResult) {
    if (fastSignum == 0) {
      throw new IllegalArgumentException("Unexpected zero value");
    }
    if (scaleDown < 1 || scaleDown > HiveDecimal.MAX_SCALE) {
      throw new IllegalArgumentException("Expecting scaleDown > 0 and scaleDown < 38 (scaleDown " + scaleDown + ")");
    }

    if (scaleDown == HiveDecimal.MAX_SCALE) {

      // Check highest digit for rounding.
      final long roundDigit = fast2 / powerOfTenTable[HIGHWORD_DECIMAL_DIGITS - 1];
      if (roundDigit < 5) {

        // Zero result.
        fastResult.fastReset();
      } else {
        fastResult.fastSet(fastSignum, /* fast0 */ 1, 0, 0, /* fastIntegerDigitCount */ 1, 0);
      }
      return true;
    }

    boolean isRoundPortionHalfUp =
        isRoundPortionHalfUp(
            fast0, fast1, fast2,
            scaleDown);

    doFastScaleDown(
        fast0, fast1, fast2,
        scaleDown,
        fastResult);

    if (isRoundPortionHalfUp) {
      final long r0 = fastResult.fast0 + 1;
      fastResult.fast0 =
          r0 % MULTIPLER_LONGWORD_DECIMAL;
      final long r1 =
          fastResult.fast1
        + r0 / MULTIPLER_LONGWORD_DECIMAL;
      fastResult.fast1 =
          r1 % MULTIPLER_LONGWORD_DECIMAL;
      fastResult.fast2 =
          fastResult.fast2
        + r1 / MULTIPLER_LONGWORD_DECIMAL;
    }

    if (fastResult.fast0 == 0 && fastResult.fast1 == 0 && fastResult.fast2 == 0) {
      fastResult.fastSignum = 0;
      fastResult.fastIntegerDigitCount = 0;
      fastResult.fastScale = 0;
    } else {
      fastResult.fastSignum = fastSignum;
    }

    return (fastResult.fast2 <= MAX_HIGHWORD_DECIMAL);
  }

  /**
   * Fast decimal scale down by factor of 10 with rounding ROUND_HALF_UP.
   *
   * When the fraction being scaled away is &gt;= 0.5, the add 1.
   *
   * @param fastSignum the sign (-1, 0, or +1)
   * @param fast0 word 0 of the internal representation
   * @param fast1 word 1
   * @param fast2 word 2
   * @param fast3 word 3
   * @param fast4 word 4
   * @param scaleDown the number of integer digits to scale
   * @param fastResult an object to reuse
   * @return was the operation successfule?
   */
  public static boolean fastRoundFractionalHalfUp5Words(
      int fastSignum, long fast0, long fast1, long fast2, long fast3, long fast4,
      int scaleDown,
      FastHiveDecimal fastResult) {

    // Adjust all longs using power 10 division/remainder.
    long result0;
    long result1;
    long result2;
    long result3;
    long result4;
    if (scaleDown < LONGWORD_DECIMAL_DIGITS) {

      // Part of lowest word survives.

      // Divide down just before scaleDown to get round digit.
      final long withRoundDigit = fast0 / powerOfTenTable[scaleDown - 1];
      final long roundDigit = withRoundDigit % 10;

      final long divideFactor = powerOfTenTable[scaleDown];
      final long multiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - scaleDown];

      if (roundDigit < 5) {
        result0 =
            withRoundDigit / 10
          + ((fast1 % divideFactor) * multiplyFactor);
        result1 =
            fast1 / divideFactor
          + ((fast2 % divideFactor) * multiplyFactor);
        result2 =
          + fast2 / divideFactor
          + ((fast3 % divideFactor) * multiplyFactor);
        result3 =
            fast3 / divideFactor
          + ((fast4 % divideFactor) * multiplyFactor);
        result4 =
            fast4 / divideFactor;
      } else {
        // Add rounding and handle carry.
        final long r0 =
            withRoundDigit / 10
          + ((fast1 % divideFactor) * multiplyFactor)
          + 1;
        result0 = r0 % MULTIPLER_LONGWORD_DECIMAL;
        final long r1 =
            fast1 / divideFactor
          + ((fast2 % divideFactor) * multiplyFactor)
          + r0 / MULTIPLER_LONGWORD_DECIMAL;
        result1 = r1 % MULTIPLER_LONGWORD_DECIMAL;
        final long r2 =
            fast2 / divideFactor +
          + ((fast3 % divideFactor) * multiplyFactor)
          + r1 / MULTIPLER_LONGWORD_DECIMAL;
        result2 = r2 % MULTIPLER_LONGWORD_DECIMAL;
        final long r3 =
            fast3 / divideFactor
          + ((fast4 % divideFactor) * multiplyFactor)
          + r2 / MULTIPLER_LONGWORD_DECIMAL;
        result3 = r3 % MULTIPLER_LONGWORD_DECIMAL;
        result4 =
            fast4 / divideFactor +
            r3 % MULTIPLER_LONGWORD_DECIMAL;
      }
    } else if (scaleDown < TWO_X_LONGWORD_DECIMAL_DIGITS) {

      // Throw away lowest word.

      final int adjustedScaleDown = scaleDown - LONGWORD_DECIMAL_DIGITS;

      long roundDigit;
      long fast1Scaled;
      if (adjustedScaleDown == 0) {
        // Grab round digit from lowest word.
        roundDigit = fast0 / (MULTIPLER_LONGWORD_DECIMAL / 10);
        fast1Scaled = fast1;
      } else {
        // Divide down just before scaleDown to get round digit.
        final long withRoundDigit = fast1 / powerOfTenTable[adjustedScaleDown - 1];
        roundDigit = withRoundDigit % 10;
        fast1Scaled = withRoundDigit / 10;
      }

      final long divideFactor = powerOfTenTable[adjustedScaleDown];
      final long multiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - adjustedScaleDown];

      if (roundDigit < 5) {
        result0 =
            fast1Scaled
          + ((fast2 % divideFactor) * multiplyFactor);
        result1 =
            fast2 / divideFactor
          + ((fast3 % divideFactor) * multiplyFactor);
        result2 =
            fast3 / divideFactor
          + ((fast4 % divideFactor) * multiplyFactor);
        result3 =
            fast4 / divideFactor;
      } else {
        // Add rounding and handle carry.
        final long r0 =
            fast1Scaled
          + ((fast2 % divideFactor) * multiplyFactor)
          + 1;
        result0 = r0 % MULTIPLER_LONGWORD_DECIMAL;
        final long r1 =
            fast2 / divideFactor
          + ((fast3 % divideFactor) * multiplyFactor)
          + r0 / MULTIPLER_LONGWORD_DECIMAL;
        result1 = r1 % MULTIPLER_LONGWORD_DECIMAL;
        final long r2 =
            fast3 / divideFactor
          + ((fast4 % divideFactor) * multiplyFactor)
          + r1 / MULTIPLER_LONGWORD_DECIMAL;
        result2 = r2 % MULTIPLER_LONGWORD_DECIMAL;
        result3 =
            fast4 / divideFactor
          + r2 / MULTIPLER_LONGWORD_DECIMAL;
      }
      result4 = 0;
    } else {

      // Throw away middle and lowest words.

      final int adjustedScaleDown = scaleDown - 2*LONGWORD_DECIMAL_DIGITS;

      long roundDigit;
      long fast2Scaled;
      if (adjustedScaleDown == 0) {
        // Grab round digit from middle word.
        roundDigit = fast1 / (MULTIPLER_LONGWORD_DECIMAL / 10);
        fast2Scaled = fast2;
      } else {
        // Divide down just before scaleDown to get round digit.
        final long withRoundDigit = fast2 / powerOfTenTable[adjustedScaleDown - 1];
        roundDigit = withRoundDigit % 10;
        fast2Scaled = withRoundDigit / 10;
      }

      final long divideFactor = powerOfTenTable[adjustedScaleDown];
      final long multiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - adjustedScaleDown];

      if (roundDigit < 5) {
        result0 =
            fast2Scaled
          + ((fast3 % divideFactor) * multiplyFactor);
        result1 =
            fast3 / divideFactor
          + ((fast4 % divideFactor) * multiplyFactor);
        result2 =
            fast4 / divideFactor;
      } else {
        // Add rounding.
        final long r0 =
            fast2Scaled
          + ((fast3 % divideFactor) * multiplyFactor)
          + 1;
        result0 = r0 % MULTIPLER_LONGWORD_DECIMAL;
        final long r1 =
            fast3 / divideFactor
          + ((fast4 % divideFactor) * multiplyFactor)
          + r0 / MULTIPLER_LONGWORD_DECIMAL;
        result1 = r1 % MULTIPLER_LONGWORD_DECIMAL;
        result2 =
            fast4 / divideFactor
          + r1 / MULTIPLER_LONGWORD_DECIMAL;
      }
      result3 = 0;
      result4 = 0;
    }

    if (result4 != 0 || result3 != 0) {
      throw new RuntimeException("Unexpected overflow into result3 or result4");
    }
    if (result0 == 0 && result1 == 0 && result2 == 0) {
      fastResult.fastReset();
    }
    fastResult.fastSignum = fastSignum;
    fastResult.fast0 = result0;
    fastResult.fast1 = result1;
    fastResult.fast2 = result2;

    return (result2 <= MAX_HIGHWORD_DECIMAL);
  }

  /**
   * Fast decimal scale down by factor of 10 with rounding ROUND_HALF_EVEN.
   *
   * When the fraction being scaled away is exactly 0.5, then round and add 1 only if aaa.
   * When fraction is not exactly 0.5, then if fraction &gt; 0.5 then add 1.
   * Otherwise, throw away fraction.
   *
   * @param fastSignum the sign (-1, 0, or +1)
   * @param fast0 word 0 of the internal representation
   * @param fast1 word 1
   * @param fast2 word 2
   * @param scaleDown the number of integer digits to scale
   * @param fastResult an object to reuse
   * @return was the operation successfule?
   */
  public static boolean fastRoundFractionalHalfEven(
      int fastSignum, long fast0, long fast1, long fast2,
      int scaleDown,
      FastHiveDecimal fastResult) {
    if (scaleDown < 1 || scaleDown > HiveDecimal.MAX_SCALE) {
      throw new IllegalArgumentException("Expecting scaleDown > 0 and scaleDown < 38 (scaleDown " + scaleDown + ")");
    }

    if (scaleDown == HiveDecimal.MAX_SCALE) {

      // Check for rounding.
      final long roundDivisor = powerOfTenTable[HIGHWORD_DECIMAL_DIGITS - 1];
      final long withRoundDigit = fast2 / roundDivisor;
      final long roundDigit = withRoundDigit % 10;
      final long fast2Scaled = withRoundDigit / 10;
      boolean shouldRound;
      if (roundDigit > 5) {
        shouldRound = true;
      } else if (roundDigit == 5) {
        boolean exactlyOneHalf = (fast2Scaled == 0 && fast1 == 0 && fast0 == 0);
        if (exactlyOneHalf) {
          // Round to even 0.
          shouldRound = false;
        } else {
          shouldRound = true;
        }
      } else {
        shouldRound = false;
      }
      if (!shouldRound) {

        // Zero result.
        fastResult.fastReset();
      } else {
        fastResult.fastSet(fastSignum, /* fast0 */ 1, 0, 0, /* fastIntegerDigitCount */ 1, 0);
      }
      return true;
    }

    boolean isRoundPortionHalfEven =
        isRoundPortionHalfEven(
            fast0, fast1, fast2,
            scaleDown);

    doFastScaleDown(
        fast0, fast1, fast2,
        scaleDown,
        fastResult);

    if (isRoundPortionHalfEven) {
      final long r0 = fastResult.fast0 + 1;
      fastResult.fast0 =
          r0 % MULTIPLER_LONGWORD_DECIMAL;
      final long r1 =
          fastResult.fast1
        + r0 / MULTIPLER_LONGWORD_DECIMAL;
      fastResult.fast1 =
          r1 % MULTIPLER_LONGWORD_DECIMAL;
      fastResult.fast2 =
          fastResult.fast2
        + r1 / MULTIPLER_LONGWORD_DECIMAL;
    }

    if (fastResult.fast0 == 0 && fastResult.fast1 == 0 && fastResult.fast2 == 0) {
      fastResult.fastSignum = 0;
      fastResult.fastIntegerDigitCount = 0;
      fastResult.fastScale = 0;
    } else {
      fastResult.fastSignum = fastSignum;
    }

    return (fastResult.fast2 <= MAX_HIGHWORD_DECIMAL);
  }

  public static void doFastScaleDown(
      FastHiveDecimal fastDec,
      int scaleDown,
      FastHiveDecimal fastResult) {
    doFastScaleDown(
        fastDec.fast0, fastDec.fast1, fastDec.fast2,
        scaleDown,
        fastResult);
  }

  //************************************************************************************************
  // Decimal Scale Up/Down.

  /**
   * Fast decimal scale down by factor of 10 with NO rounding.
   *
   * The signum will be updated if the result is 0, otherwise the original sign is unchanged.
   *
   * @param fast0 word 0 of the internal representation
   * @param fast1 word 1
   * @param fast2 word 2
   * @param scaleDown the number of integer digits to scale
   * @param fastResult an object to reuse
   */
  public static void doFastScaleDown(
      long fast0, long fast1, long fast2,
      int scaleDown,
      FastHiveDecimal fastResult) {

    // Adjust all longs using power 10 division/remainder.
    long result0;
    long result1;
    long result2;
    if (scaleDown < LONGWORD_DECIMAL_DIGITS) {

      // Part of lowest word survives.

      final long divideFactor = powerOfTenTable[scaleDown];
      final long multiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - scaleDown];

      result0 =
          fast0 / divideFactor
        + ((fast1 % divideFactor) * multiplyFactor);
      result1 =
          fast1 / divideFactor
        + ((fast2 % divideFactor) * multiplyFactor);
      result2 =
          fast2 / divideFactor;

    } else if (scaleDown < TWO_X_LONGWORD_DECIMAL_DIGITS) {

      // Throw away lowest word.

      final int adjustedScaleDown = scaleDown - LONGWORD_DECIMAL_DIGITS;

      final long divideFactor = powerOfTenTable[adjustedScaleDown];
      final long multiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - adjustedScaleDown];

      result0 =
          fast1 / divideFactor
        + ((fast2 % divideFactor) * multiplyFactor);
      result1 =
          fast2 / divideFactor;
      result2 = 0;

    } else {

      // Throw away middle and lowest words.

      final int adjustedScaleDown = scaleDown - 2*LONGWORD_DECIMAL_DIGITS;

      result0 =
          fast2 / powerOfTenTable[adjustedScaleDown];
      result1 = 0;
      result2 = 0;

    }

    if (result0 == 0 && result1 == 0 && result2 == 0) {
      fastResult.fastSignum = 0;
    }
    fastResult.fast0 = result0;
    fastResult.fast1 = result1;
    fastResult.fast2 = result2;
  }

  public static boolean fastScaleUp(
      FastHiveDecimal fastDec,
      int scaleUp,
      FastHiveDecimal fastResult) {
    return
        fastScaleUp(
            fastDec.fast0, fastDec.fast1, fastDec.fast2,
            scaleUp,
            fastResult);
  }

  /**
   * Fast decimal scale up by factor of 10.
   * @param fast0 word 0 of the internal representation
   * @param fast1 word 1
   * @param fast2 word 2
   * @param scaleUp the number of integer digits to scale up by
   * @param fastResult an object to reuse
   * @return was the operation successfule?
   */
  public static boolean fastScaleUp(
      long fast0, long fast1, long fast2,
      int scaleUp,
      FastHiveDecimal fastResult) {
    if (scaleUp < 1 || scaleUp >= HiveDecimal.MAX_SCALE) {
      throw new IllegalArgumentException("Expecting scaleUp > 0 and scaleUp < 38");
    }

    long result0;
    long result1;
    long result2;

    // Each range checks for overflow first, then moves digits.
    if (scaleUp < HIGHWORD_DECIMAL_DIGITS) {
      // Need to check if there are overflow digits in the high word.

      final long overflowFactor = powerOfTenTable[HIGHWORD_DECIMAL_DIGITS - scaleUp];
      if (fast2 / overflowFactor != 0) {
        return false;
      }

      final long divideFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - scaleUp];
      final long multiplyFactor = powerOfTenTable[scaleUp];

      result2 =
          fast2 * multiplyFactor
        + fast1 / divideFactor;
      result1 =
          (fast1 % divideFactor) * multiplyFactor
        + fast0 / divideFactor;
      result0 =
          (fast0 % divideFactor) * multiplyFactor;
    } else if (scaleUp < HIGHWORD_DECIMAL_DIGITS + LONGWORD_DECIMAL_DIGITS) {
      // High word must be zero.  Check for overflow digits in middle word.

      if (fast2 != 0) {
        return false;
      }

      final int adjustedScaleUp = scaleUp - HIGHWORD_DECIMAL_DIGITS;

      final int middleDigits = LONGWORD_DECIMAL_DIGITS - adjustedScaleUp;
      final long overflowFactor = powerOfTenTable[middleDigits];
      if (fast1 / overflowFactor != 0) {
        return false;
      }

      if (middleDigits < HIGHWORD_DECIMAL_DIGITS) {
        // Must fill high word from both middle and lower longs.

        final int highWordMoreDigits = HIGHWORD_DECIMAL_DIGITS - middleDigits;
        final long multiplyFactor = powerOfTenTable[highWordMoreDigits];

        final long divideFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - highWordMoreDigits];

        result2 =
            fast1 * multiplyFactor
          + fast0 / divideFactor;
        result1 =
            (fast0 % divideFactor) * multiplyFactor;
        result0 = 0;
      } else if (middleDigits == HIGHWORD_DECIMAL_DIGITS) {
        // Fill high long from middle long, and middle long from lower long.

        result2 = fast1;
        result1 = fast0;
        result0 = 0;
      } else {
        // Fill high long from some of middle long.

        final int keepMiddleDigits = middleDigits - HIGHWORD_DECIMAL_DIGITS;
        final long divideFactor = powerOfTenTable[keepMiddleDigits];
        final long multiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - keepMiddleDigits];

        result2 =
            fast1 / divideFactor;
        result1 =
            (fast1 % divideFactor) * multiplyFactor
          + fast0 / divideFactor;
        result0 =
            (fast0 % divideFactor) * multiplyFactor;
      }
    } else {
      // High and middle word must be zero.  Check for overflow digits in lower word.

      if (fast2 != 0 || fast1 != 0) {
        return false;
      }

      final int adjustedScaleUp = scaleUp - HIGHWORD_DECIMAL_DIGITS - LONGWORD_DECIMAL_DIGITS;

      final int lowerDigits = LONGWORD_DECIMAL_DIGITS - adjustedScaleUp;
      final long overflowFactor = powerOfTenTable[lowerDigits];
      if (fast0 / overflowFactor != 0) {
        return false;
      }

      if (lowerDigits < HIGHWORD_DECIMAL_DIGITS) {
        // Must fill high word from both middle and lower longs.

        final int highWordMoreDigits = HIGHWORD_DECIMAL_DIGITS - lowerDigits;
        final long multiplyFactor = powerOfTenTable[highWordMoreDigits];

        final long divideFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - highWordMoreDigits];

        result2 =
            fast0 * multiplyFactor;
        result1 = 0;
        result0 = 0;
      } else if (lowerDigits == HIGHWORD_DECIMAL_DIGITS) {
        // Fill high long from lower long.

        result2 = fast0;
        result1 = 0;
        result0 = 0;
      } else {
        // Fill high long and middle from some of lower long.

        final int keepLowerDigits = lowerDigits - HIGHWORD_DECIMAL_DIGITS;
        final long keepLowerDivideFactor = powerOfTenTable[keepLowerDigits];
        final long keepLowerMultiplyFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - keepLowerDigits];

        result2 =
            fast0 / keepLowerDivideFactor;
        result1 =
            (fast0 % keepLowerDivideFactor) * keepLowerMultiplyFactor;
        result0 =  0;
      }
    }

    if (result0 == 0 && result1 == 0 && result2 == 0) {
      fastResult.fastSignum = 0;
    }
    fastResult.fast0 = result0;
    fastResult.fast1 = result1;
    fastResult.fast2 = result2;

    return true;
  }

  //************************************************************************************************
  // Decimal Precision / Trailing Zeroes.

  public static int fastLongWordTrailingZeroCount(
      long longWord) {

    if (longWord == 0) {
      return LONGWORD_DECIMAL_DIGITS;
    }

    long factor = 10;
    for (int i = 0; i < LONGWORD_DECIMAL_DIGITS; i++) {
      if (longWord % factor != 0) {
        return i;
      }
      factor *= 10;
    }
    return 0;
  }

  public static int fastHighWordTrailingZeroCount(
      long longWord) {

    if (longWord == 0) {
      return HIGHWORD_DECIMAL_DIGITS;
    }

    long factor = 10;
    for (int i = 0; i < HIGHWORD_DECIMAL_DIGITS; i++) {
      if (longWord % factor != 0) {
        return i;
      }
      factor *= 10;
    }
    return 0;
  }

  public static int fastLongWordPrecision(
      long longWord) {

    if (longWord == 0) {
      return 0;
    }
    // Search like binary search to minimize comparisons.
    if (longWord > 99999999L) {
      if (longWord > 999999999999L) {
        if (longWord > 99999999999999L) {
          if (longWord > 999999999999999L) {
            return 16;
          } else {
            return 15;
          }
        } else {
          if (longWord > 9999999999999L) {
            return 14;
          } else {
            return 13;
          }
        }
      } else {
        if (longWord > 9999999999L) {
          if (longWord > 99999999999L) {
            return 12;
          } else {
            return 11;
          }
        } else {
          if (longWord > 999999999L) {
            return 10;
          } else {
            return 9;
          }
        }
      }
    } else {
      if (longWord > 9999L) {
        if (longWord > 999999L) {
          if (longWord > 9999999L) {
            return 8;
          } else {
            return 7;
          }
        } else {
          if (longWord > 99999L) {
            return 6;
          } else {
            return 5;
          }
        }
      } else {
        if (longWord > 99L) {
          if (longWord > 999L) {
            return 4;
          } else {
            return 3;
          }
        } else {
          if (longWord > 9L) {
            return 2;
          } else {
            return 1;
          }
        }
      }
    }
  }

  public static int fastHighWordPrecision(
      long longWord) {

    if (longWord == 0) {
      return 0;
    }
    // 6 highword digits.
    if (longWord > 999L) {
      if (longWord > 9999L) {
        if (longWord > 99999L) {
          return 6;
        } else {
          return 5;
        }
      } else {
        return 4;
      }
    } else {
      if (longWord > 99L) {
        return 3;
      } else {
        if (longWord > 9L) {
          return 2;
        } else {
          return 1;
        }
      }
    }
  }

  public static int fastSqlPrecision(
      FastHiveDecimal fastDec) {
    return
        fastSqlPrecision(
            fastDec.fastSignum, fastDec.fast0, fastDec.fast1, fastDec.fast2,
            fastDec.fastIntegerDigitCount, fastDec.fastScale);
  }

  public static int fastSqlPrecision(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale) {

    if (fastSignum == 0) {
      return 1;
    }

    final int rawPrecision =
        fastRawPrecision(fastSignum, fast0, fast1, fast2);

    if (rawPrecision < fastScale) {
      // This can happen for numbers less than 0.1
      // For 0.001234: rawPrecision=4, scale=6
      // In this case, we'll set the type to have the same precision as the scale.
      return fastScale;
    }
    return rawPrecision;
  }

  public static int fastRawPrecision(
      FastHiveDecimal fastDec) {
    return
        fastRawPrecision(
            fastDec.fastSignum, fastDec.fast0, fastDec.fast1, fastDec.fast2);
  }

  public static int fastRawPrecision(
      int fastSignum, long fast0, long fast1, long fast2) {

    if (fastSignum == 0) {
      return 0;
    }

    int precision;

    if (fast2 != 0) {

      // 6 highword digits.
      precision = TWO_X_LONGWORD_DECIMAL_DIGITS + fastHighWordPrecision(fast2);

    } else if (fast1 != 0) {

      // Check fast1.
      precision = LONGWORD_DECIMAL_DIGITS + fastLongWordPrecision(fast1);


    } else {

      // Check fast0.
      precision = fastLongWordPrecision(fast0);

    }

    return precision;
  }

  // Determine if all digits below a power is zero.
  // The lowest digit is power = 0.
  public static boolean isAllZeroesBelow(
      int fastSignum, long fast0, long fast1, long fast2,
      int power) {
    if (power < 0 || power > HiveDecimal.MAX_SCALE) {
      throw new IllegalArgumentException("Expecting power >= 0 and power <= 38");
    }

    if (fastSignum == 0) {
      return true;
    }

    if (power >= TWO_X_LONGWORD_DECIMAL_DIGITS) {
      if (fast0 != 0 || fast1 != 0) {
        return false;
      }
      final int adjustedPower = power - TWO_X_LONGWORD_DECIMAL_DIGITS;
      if (adjustedPower == 0) {
        return true;
      }
      long remainder = fast2 % powerOfTenTable[adjustedPower];
      return (remainder == 0);
    } else if (power >= LONGWORD_DECIMAL_DIGITS) {
      if (fast0 != 0) {
        return false;
      }
      final int adjustedPower = power - LONGWORD_DECIMAL_DIGITS;
      if (adjustedPower == 0) {
        return true;
      }
      long remainder = fast1 % powerOfTenTable[adjustedPower];
      return (remainder == 0);
    } else {
      if (power == 0) {
        return true;
      }
      long remainder = fast0 % powerOfTenTable[power];
      return (remainder == 0);
    }
  }

  public static boolean fastExceedsPrecision(
      long fast0, long fast1, long fast2,
      int precision) {

    if (precision <= 0) {
      return true;
    } else if (precision >= HiveDecimal.MAX_PRECISION) {
      return false;
    }
    final int precisionLessOne = precision - 1;

    // 0 (lowest), 1 (middle), or 2 (high).
    int wordNum = precisionLessOne / LONGWORD_DECIMAL_DIGITS;

    int digitInWord = precisionLessOne % LONGWORD_DECIMAL_DIGITS;

    final long overLimitInWord = powerOfTenTable[digitInWord + 1] - 1;

    if (wordNum == 0) {
      if (digitInWord < LONGWORD_DECIMAL_DIGITS - 1) {
        if (fast0 > overLimitInWord) {
          return true;
        }
      }
      return (fast1 != 0 || fast2 != 0);
    } else if (wordNum == 1) {
      if (digitInWord < LONGWORD_DECIMAL_DIGITS - 1) {
        if (fast1 > overLimitInWord) {
          return true;
        }
      }
      return (fast2 != 0);
    } else {
      // We've eliminated the highest digit already with HiveDecimal.MAX_PRECISION check above.
      return (fast2 > overLimitInWord);
    }
  }

  public static int fastTrailingDecimalZeroCount(
      long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale) {
    if (fastScale < 0 || fastScale > HiveDecimal.MAX_SCALE) {
      throw new IllegalArgumentException("Expecting scale >= 0 and scale <= 38");
    }
    if (fastScale == 0) {
      return 0;
    }

    final int lowerLongwordDigits = Math.min(fastScale, LONGWORD_DECIMAL_DIGITS);
    if (lowerLongwordDigits < LONGWORD_DECIMAL_DIGITS || fast0 != 0) {
      long factor = 10;
      for (int i = 0; i < lowerLongwordDigits; i++) {
        if (fast0 % factor != 0) {
          return i;
        }
        factor *= 10;
      }
      if (lowerLongwordDigits < LONGWORD_DECIMAL_DIGITS) {
        return fastScale;
      }
    }
    if (fastScale == LONGWORD_DECIMAL_DIGITS) {
      return fastScale;
    }
    final int middleLongwordDigits = Math.min(fastScale - LONGWORD_DECIMAL_DIGITS, LONGWORD_DECIMAL_DIGITS);
    if (middleLongwordDigits < LONGWORD_DECIMAL_DIGITS || fast1 != 0) {
      long factor = 10;
      for (int i = 0; i < middleLongwordDigits; i++) {
        if (fast1 % factor != 0) {
          return LONGWORD_DECIMAL_DIGITS + i;
        }
        factor *= 10;
      }
      if (middleLongwordDigits < LONGWORD_DECIMAL_DIGITS) {
        return fastScale;
      }
    }
    if (fastScale == TWO_X_LONGWORD_DECIMAL_DIGITS) {
      return fastScale;
    }
    final int highLongwordDigits = fastScale - TWO_X_LONGWORD_DECIMAL_DIGITS;
    if (highLongwordDigits < HIGHWORD_DECIMAL_DIGITS || fast2 != 0) {
      long factor = 10;
      for (int i = 0; i < highLongwordDigits; i++) {
        if (fast2 % factor != 0) {
          return TWO_X_LONGWORD_DECIMAL_DIGITS + i;
        }
        factor *= 10;
      }
    }
    return fastScale;
  }

  public static FastCheckPrecisionScaleStatus fastCheckPrecisionScale(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale,
      int maxPrecision, int maxScale) {

    if (fastSignum == 0) {
      return FastCheckPrecisionScaleStatus.NO_CHANGE;
    }
    final int maxIntegerDigitCount = maxPrecision - maxScale;
    if (fastIntegerDigitCount > maxIntegerDigitCount) {
      return FastCheckPrecisionScaleStatus.OVERFLOW;
    }
    if (fastScale > maxScale) {
      return FastCheckPrecisionScaleStatus.UPDATE_SCALE_DOWN;
    }
    return FastCheckPrecisionScaleStatus.NO_CHANGE;
  }

  public static boolean fastUpdatePrecisionScale(
      final int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale,
      int maxPrecision, int maxScale, FastCheckPrecisionScaleStatus status,
      FastHiveDecimal fastResult) {

    switch (status) {
    case UPDATE_SCALE_DOWN:
      {
        fastResult.fastSignum = fastSignum;

        // Throw away lower digits.
        if (!fastRoundFractionalHalfUp(
          fastSignum, fast0, fast1, fast2,
          fastScale - maxScale,
          fastResult)) {
          return false;
        }

        fastResult.fastScale = maxScale;

        // CONSIDER: For now, recompute integerDigitCount...
        fastResult.fastIntegerDigitCount =
            Math.max(0, fastRawPrecision(fastResult) - fastResult.fastScale);

        // And, round up may cause us to exceed our precision/scale...
        final int maxIntegerDigitCount = maxPrecision - maxScale;
        if (fastResult.fastIntegerDigitCount > maxIntegerDigitCount) {
          return false;
        }

        // Scaling down may have opened up trailing zeroes...
        final int trailingZeroCount =
            fastTrailingDecimalZeroCount(
                fastResult.fast0, fastResult.fast1, fastResult.fast2,
                fastResult.fastIntegerDigitCount, fastResult.fastScale);
        if (trailingZeroCount > 0) {
          // Scale down again.
          doFastScaleDown(
              fastResult,
              trailingZeroCount,
              fastResult);
          fastResult.fastScale -= trailingZeroCount;
        }
      }
      break;
    default:
      throw new RuntimeException("Unexpected fast check precision scale status " + status);
    }

    return true;
  }

  //************************************************************************************************
  // Decimal Addition / Subtraction.

  public static boolean doAddSameScaleSameSign(
      FastHiveDecimal fastLeft,
      FastHiveDecimal fastRight,
      FastHiveDecimal fastResult) {
    return
        doAddSameScaleSameSign(
            /* resultSignum */ fastLeft.fastSignum,
            fastLeft.fast0, fastLeft.fast1, fastLeft.fast2,
            fastRight.fast0, fastRight.fast1, fastRight.fast2,
            fastResult);
  }

  public static boolean doAddSameScaleSameSign(
      int resultSignum,
      long left0, long left1, long left2,
      long right0, long right1, long right2,
      FastHiveDecimal fastResult) {

    long result0;
    long result1;
    long result2;

    final long r0 = left0 + right0;
    result0 =
        r0 % MULTIPLER_LONGWORD_DECIMAL;
    final long r1 =
        left1
      + right1
      + r0 / MULTIPLER_LONGWORD_DECIMAL;
    result1 =
        r1 % MULTIPLER_LONGWORD_DECIMAL;
    result2 =
        left2
      + right2
      + r1 / MULTIPLER_LONGWORD_DECIMAL;

    if (result0 == 0 && result1 == 0 && result2 == 0) {
      fastResult.fastReset();
    } else {
      fastResult.fastSignum = resultSignum;
      fastResult.fast0 = result0;
      fastResult.fast1 = result1;
      fastResult.fast2 = result2;
    }

    return (result2 <= MAX_HIGHWORD_DECIMAL);
  }

  public static boolean doSubtractSameScaleNoUnderflow(
      int resultSignum,
      FastHiveDecimal fastLeft,
      FastHiveDecimal fastRight,
      FastHiveDecimal fastResult) {
    return
        doSubtractSameScaleNoUnderflow(
            resultSignum,
            fastLeft.fast0, fastLeft.fast1, fastLeft.fast2,
            fastRight.fast0, fastRight.fast1, fastRight.fast2,
            fastResult);
  }

  public static boolean doSubtractSameScaleNoUnderflow(
      int resultSignum,
      long left0, long left1, long left2,
      long right0, long right1, long right2,
      FastHiveDecimal fastResult) {

    long result0;
    long result1;
    long result2;

    final long r0 = left0 - right0;
    long r1;
    if (r0 < 0) {
      result0 = r0 + MULTIPLER_LONGWORD_DECIMAL;
      r1 = left1 - right1 - 1;
    } else {
      result0 = r0;
      r1 = left1 - right1;
    }
    if (r1 < 0) {
      result1 = r1 + MULTIPLER_LONGWORD_DECIMAL;
      result2 = left2 - right2 - 1;
    } else {
      result1 = r1;
      result2 = left2 - right2;
    }
    if (result2 < 0) {
      return false;
    }

    if (result0 == 0 && result1 == 0 && result2 == 0) {
      fastResult.fastReset();
    } else {
      fastResult.fastSignum = resultSignum;
      fastResult.fast0 = result0;
      fastResult.fast1 = result1;
      fastResult.fast2 = result2;
    }

    return true;
  }

  public static boolean doSubtractSameScaleNoUnderflow(
      long left0, long left1, long left2,
      long right0, long right1, long right2,
      long[] result) {

    long result0;
    long result1;
    long result2;

    final long r0 = left0 - right0;
    long r1;
    if (r0 < 0) {
      result0 = r0 + MULTIPLER_LONGWORD_DECIMAL;
      r1 = left1 - right1 - 1;
    } else {
      result0 = r0;
      r1 = left1 - right1;
    }
    if (r1 < 0) {
      result1 = r1 + MULTIPLER_LONGWORD_DECIMAL;
      result2 = left2 - right2 - 1;
    } else {
      result1 = r1;
      result2 = left2 - right2;
    }
    if (result2 < 0) {
      return false;
    }

    result[0] = result0;
    result[1] = result1;
    result[2] = result2;

    return true;
  }

  private static boolean doAddSameScale(
      int leftSignum, long leftFast0, long leftFast1, long leftFast2,
      int rightSignum, long rightFast0, long rightFast1, long rightFast2,
      int scale,
      FastHiveDecimal fastResult) {

    if (leftSignum == rightSignum) {
      if (!doAddSameScaleSameSign(
          /* resultSignum */ leftSignum,
          leftFast0, leftFast1, leftFast2,
          rightFast0, rightFast1, rightFast2,
          fastResult)) {
        // Handle overflow precision issue.
        if (scale > 0) {
          if (!fastRoundFractionalHalfUp(
              fastResult.fastSignum, fastResult.fast0, fastResult.fast1, fastResult.fast2,
              1,
              fastResult)) {
            return false;
          }
          scale--;
        } else {
          // Overflow.
          return false;
        }
      }
      fastResult.fastScale = scale;
    } else {
      // Just compare the magnitudes (i.e. signums set to 1).
      int compareTo =
          fastCompareTo(
              1,
              leftFast0, leftFast1, leftFast2, 0,
              1,
              rightFast0, rightFast1, rightFast2, 0);
      if (compareTo == 0) {
        // They cancel each other.
        fastResult.fastReset();
        return true;
      }

      if (compareTo == 1) {
        if (!doSubtractSameScaleNoUnderflow(
                /* resultSignum */ leftSignum,
                leftFast0, leftFast1, leftFast2,
                rightFast0, rightFast1, rightFast2,
                fastResult)) {
          throw new RuntimeException("Unexpected underflow");
        }
      } else {
        if (!doSubtractSameScaleNoUnderflow(
                /* resultSignum */ rightSignum,
                rightFast0, rightFast1, rightFast2,
                leftFast0, leftFast1, leftFast2,
                fastResult)) {
          throw new RuntimeException("Unexpected underflow");
        }
      }
      fastResult.fastScale = scale;
    }

    if (fastResult.fastSignum != 0) {
      final int precision = fastRawPrecision(fastResult);
      fastResult.fastIntegerDigitCount = Math.max(0, precision - fastResult.fastScale);
    }

    final int resultTrailingZeroCount =
        fastTrailingDecimalZeroCount(
            fastResult.fast0, fastResult.fast1, fastResult.fast2,
            fastResult.fastIntegerDigitCount, fastResult.fastScale);
    if (resultTrailingZeroCount > 0) {
      doFastScaleDown(
          fastResult,
          resultTrailingZeroCount,
          fastResult);
      if (fastResult.fastSignum == 0) {
        fastResult.fastScale = 0;
      } else {
        fastResult.fastScale -= resultTrailingZeroCount;
      }
    }

    return true;
  }

  /**
   * Handle the common logic at the end of fastAddDifferentScale / fastSubtractDifferentScale that
   * takes the 5 intermediate result words and fits them into the 3 longword fast result.
   *
   */
  private static boolean doFinishAddSubtractDifferentScale(
      long result0, long result1, long result2, long result3, long result4,
      int resultScale,
      FastHiveDecimal fastResult) {

    int precision;
    if (result4 != 0) {
      precision = FOUR_X_LONGWORD_DECIMAL_DIGITS + fastLongWordPrecision(result4);
    } else if (result3 != 0) {
      precision = THREE_X_LONGWORD_DECIMAL_DIGITS + fastLongWordPrecision(result3);
    } else if (result2 != 0) {
      precision = TWO_X_LONGWORD_DECIMAL_DIGITS + fastLongWordPrecision(result2);
    } else if (result1 != 0) {
      precision = LONGWORD_DECIMAL_DIGITS + fastLongWordPrecision(result1);
    } else {
      precision = fastLongWordPrecision(result0);
    }

    if (precision > HiveDecimal.MAX_PRECISION){
      final int scaleDown = precision - HiveDecimal.MAX_PRECISION;

      resultScale -= scaleDown;
      if (resultScale < 0) {
        // No room.
        return false;
      }
      if (!fastRoundFractionalHalfUp5Words(
            1, result0, result1, result2, result3, result4,
            scaleDown,
            fastResult)) {
        // Handle overflow precision issue.
        if (resultScale > 0) {
          if (!fastRoundFractionalHalfUp(
              fastResult.fastSignum, fastResult.fast0, fastResult.fast1, fastResult.fast2,
              1,
              fastResult)) {
            throw new RuntimeException("Unexpected overflow");
          }
          if (fastResult.fastSignum == 0) {
            return true;
          }
          resultScale--;
        } else {
          return false;
        }
      }

      precision = fastRawPrecision(1, fastResult.fast0, fastResult.fast1, fastResult.fast2);

      // Stick back into result variables...
      result0 = fastResult.fast0;
      result1 = fastResult.fast1;
      result2 = fastResult.fast2;
    }

    // Caller will set signum.
    fastResult.fastSignum = 1;
    fastResult.fast0 = result0;
    fastResult.fast1 = result1;
    fastResult.fast2 = result2;
    fastResult.fastIntegerDigitCount = Math.max(0, precision - resultScale);
    fastResult.fastScale = resultScale;

    final int resultTrailingZeroCount =
        fastTrailingDecimalZeroCount(
            fastResult.fast0, fastResult.fast1, fastResult.fast2,
            fastResult.fastIntegerDigitCount, fastResult.fastScale);
    if (resultTrailingZeroCount > 0) {
      doFastScaleDown(
          fastResult,
          resultTrailingZeroCount,
          fastResult);
      if (fastResult.fastSignum == 0) {
        fastResult.fastScale = 0;
      } else {
        fastResult.fastScale -= resultTrailingZeroCount;
      }
    }

    return true;
  }

  /**
   * Handle decimal subtraction when the values have different scales.
   */
  private static boolean fastSubtractDifferentScale(
      long leftFast0, long leftFast1, long leftFast2,
      int leftIntegerDigitCount, int leftScale,
      long rightFast0, long rightFast1, long rightFast2,
      int rightIntegerDigitCount, int rightScale,
      FastHiveDecimal fastResult) {

    int diffScale;
    int resultScale;

    long result0 = 0;
    long result1 = 0;
    long result2 = 0;
    long result3 = 0;
    long result4 = 0;

    // Since subtraction is not commutative, we can must subtract in the order passed in.
    if (leftScale > rightScale) {

      // Since left has a longer digit tail and it doesn't move; we will shift the right digits
      // as we do our addition into the result.

      diffScale = leftScale - rightScale;
      resultScale = leftScale;

      if (diffScale < LONGWORD_DECIMAL_DIGITS) {

        final long divideFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - diffScale];
        final long multiplyFactor = powerOfTenTable[diffScale];

        final long r0 =
            leftFast0
          - (rightFast0 % divideFactor) * multiplyFactor;
        long r1;
        if (r0 < 0) {
          result0 = r0 + MULTIPLER_LONGWORD_DECIMAL;
          r1 =
              leftFast1
            - rightFast0 / divideFactor
            - (rightFast1 % divideFactor) * multiplyFactor
            - 1;
        } else {
          result0 = r0;
          r1 =
              leftFast1
            - rightFast0 / divideFactor
            - (rightFast1 % divideFactor) * multiplyFactor;
        }
        long r2;
        if (r1 < 0) {
          result1 = r1 + MULTIPLER_LONGWORD_DECIMAL;
          r2 =
              leftFast2
            - rightFast1 / divideFactor
            - (rightFast2 % divideFactor) * multiplyFactor
            - 1;
        } else {
          result1 = r1;
          r2 =
              leftFast2
            - rightFast1 / divideFactor
            - (rightFast2 % divideFactor) * multiplyFactor;
        }
        long r3;
        if (r2 < 0) {
          result2 = r2 + MULTIPLER_LONGWORD_DECIMAL;
          r3 =
             -(rightFast2 / divideFactor)
            - 1;
        } else {
          result2 = r2;
          r3 =
             -(rightFast2 / divideFactor);
        }
        long r4;
        if (r3 < 0) {
          result3 = r3 + MULTIPLER_LONGWORD_DECIMAL;
          r4 =  - 1;
        } else {
          result3 = r3;
          r4 = 0;
        }
        if (r4 != 0) {
          throw new RuntimeException("Unexpected underflow");
        }

      } else if (diffScale == LONGWORD_DECIMAL_DIGITS){

        result0 = leftFast0;
        final long r1 =
            leftFast1
          - rightFast0;
        long r2;
        if (r1 < 0) {
          result1 = r1 + MULTIPLER_LONGWORD_DECIMAL;
          r2 =
              leftFast2
            - rightFast1
            - 1;
        } else {
          result1 = r1;
          r2 =
              leftFast2
            - rightFast1;
        }
        long r3;
        if (r2 < 0) {
          result2 = r2 + MULTIPLER_LONGWORD_DECIMAL;
          r3 =
             -rightFast2
            - 1;
        } else {
          result2 = r2;
          r3 =
             -rightFast2;
        }
        if (r3 != 0) {
          throw new RuntimeException("Unexpected underflow");
        }


      } else if (diffScale < TWO_X_LONGWORD_DECIMAL_DIGITS) {

        final long divideFactor = powerOfTenTable[TWO_X_LONGWORD_DECIMAL_DIGITS - diffScale];
        final long multiplyFactor = powerOfTenTable[diffScale - LONGWORD_DECIMAL_DIGITS];

        result0 = leftFast0;
        final long r1 =
            leftFast1
           -(rightFast0 % divideFactor) * multiplyFactor;
        long r2;
        if (r1 < 0) {
          result1 = r1 + MULTIPLER_LONGWORD_DECIMAL;
          r2 =
              leftFast2
            - rightFast0 / divideFactor
            - (rightFast1 % divideFactor) * multiplyFactor
            - 1;
        } else {
          result1 = r1;
          r2 =
              leftFast2
            - rightFast0 / divideFactor
            - (rightFast1 % divideFactor) * multiplyFactor;
        }
        long r3;
        if (r2 < 0) {
          result2 = r2 + MULTIPLER_LONGWORD_DECIMAL;
          r3 =
            - rightFast1 / divideFactor
            - (rightFast2 % divideFactor) * multiplyFactor
            - 1;
        } else {
          result2 = r2;
          r3 =
            - rightFast1 / divideFactor
            - (rightFast2 % divideFactor) * multiplyFactor;
        }
        long r4;
        if (r3 < 0) {
          result3 = r3 + MULTIPLER_LONGWORD_DECIMAL;
          r4 =
            - rightFast2 / divideFactor
            - 1;
        } else {
          result3 = r3;
          r4 =
            - rightFast2 / divideFactor;
        }
        long r5;
        if (r4 < 0) {
          result4 = r4 + MULTIPLER_LONGWORD_DECIMAL;
          r5 = - 1;
        } else {
          result4 = r4;
          r5 = 0;
        }
        if (r5 != 0) {
         throw new RuntimeException("Unexpected underflow");
        }

      } else if (diffScale == TWO_X_LONGWORD_DECIMAL_DIGITS) {

        result0 = leftFast0;
        result1 = leftFast1;
        final long r2 =
            leftFast2
          - rightFast0;
        long r3;
        if (r2 < 0) {
          result2 = r2 + MULTIPLER_LONGWORD_DECIMAL;
          r3 =
            - rightFast1
            - 1;
        } else {
          result2 = r2;
          r3 =
            - rightFast1;
        }
        long r4;
        if (r3 < 0) {
          result3 = r3 + MULTIPLER_LONGWORD_DECIMAL;
          r4 =
             -rightFast2
            - 1;
        } else {
          result3 = r3;
          r4 =
             -rightFast2;
        }
        long r5;
        if (r4 < 0) {
          result4 = r4 + MULTIPLER_LONGWORD_DECIMAL;
          r5 = - 1;
        } else {
          result4 = r4;
          r5 = 0;
        }
        if (r5 != 0) {
          throw new RuntimeException("Unexpected underflow");
        }

      } else {

        final long divideFactor = powerOfTenTable[THREE_X_LONGWORD_DECIMAL_DIGITS - diffScale];
        final long multiplyFactor = powerOfTenTable[diffScale - TWO_X_LONGWORD_DECIMAL_DIGITS];

        result0 = leftFast0;
        result1 = leftFast1;
        final long r2 =
            leftFast2
          - (rightFast0 % divideFactor) * multiplyFactor;
        long r3;
        if (r2 < 0) {
          result2 = r2 + MULTIPLER_LONGWORD_DECIMAL;
          r3 =
            - (rightFast0 / divideFactor)
            - (rightFast1 % divideFactor) * multiplyFactor
            - 1;
        } else {
          result2 = r2;
          r3 =
            - (rightFast0 / divideFactor)
            - (rightFast1 % divideFactor) * multiplyFactor;
        }
        long r4;
        if (r3 < 0) {
          result3 = r3 + MULTIPLER_LONGWORD_DECIMAL;
          r4 =
              - (rightFast1 / divideFactor)
              - (rightFast2 % divideFactor) * multiplyFactor
              - 1;
        } else {
          result3 = r3;
          r4 =
              - (rightFast1 / divideFactor)
              - (rightFast2 % divideFactor) * multiplyFactor;
        }
        long r5;
        if (r4 < 0) {
          result4 = r4 + MULTIPLER_LONGWORD_DECIMAL;
          r5 =
              - (rightFast2 / divideFactor)
              - 1;
        } else {
          result4 = r4;
          r5 =
              - (rightFast2 / divideFactor);
        }
        if (r5 != 0) {
          throw new RuntimeException("Unexpected underflow");
        }
      }
    } else {

      // Since right has a longer digit tail and it doesn't move; we will shift the left digits
      // as we do our addition into the result.

      diffScale = rightScale - leftScale;
      resultScale = rightScale;

      if (diffScale < LONGWORD_DECIMAL_DIGITS) {

        final long divideFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - diffScale];
        final long multiplyFactor = powerOfTenTable[diffScale];

        final long r0 =
            (leftFast0 % divideFactor) * multiplyFactor
          - rightFast0;
        long r1;
        if (r0 < 0) {
          result0 = r0 + MULTIPLER_LONGWORD_DECIMAL;
          r1 =
              leftFast0 / divideFactor
            + (leftFast1 % divideFactor) * multiplyFactor
            - rightFast1
            - 1;
        } else {
          result0 = r0;
          r1 =
              leftFast0 / divideFactor
            + (leftFast1 % divideFactor) * multiplyFactor
            - rightFast1;
        }
        long r2;
        if (r1 < 0) {
          result1 = r1 + MULTIPLER_LONGWORD_DECIMAL;
          r2 =
              leftFast1 / divideFactor
            + (leftFast2 % divideFactor) * multiplyFactor
            - rightFast2
            - 1;
        } else {
          result1 = r1;
          r2 =
              leftFast1 / divideFactor
            + (leftFast2 % divideFactor) * multiplyFactor
            - rightFast2;
        }
        long r3;
        if (r2 < 0) {
          result2 = r2 + MULTIPLER_LONGWORD_DECIMAL;
          r3 =
              leftFast2 / divideFactor
            - 1;
        } else {
          result2 = r2;
          r3 =
              leftFast2 / divideFactor;
        }
        long r4;
        if (r3 < 0) {
          result3 = r3 + MULTIPLER_LONGWORD_DECIMAL;
          r4 = - 1;
        } else {
          result3 = r3;
          r4 = 0;
        }
        if (r4 != 0) {
          throw new RuntimeException("Unexpected underflow");
        }

      } else if (diffScale == LONGWORD_DECIMAL_DIGITS){

        final long r0 =
          - rightFast0;
        long r1;
        if (r0 < 0) {
          result0 = r0 + MULTIPLER_LONGWORD_DECIMAL;
          r1 =
              leftFast0
            - rightFast1
            - 1;
        } else {
          result0 = r0;
          r1 =
              leftFast0
            - rightFast1;
        }
        long r2;
        if (r1 < 0) {
          result1 = r1 + MULTIPLER_LONGWORD_DECIMAL;
          r2 =
              leftFast1
            - rightFast2
            - 1;
        } else {
          result1 = r1;
          r2 =
              leftFast1
            - rightFast2;
        }
        long r3;
        if (r2 < 0) {
          result2 = r2 + MULTIPLER_LONGWORD_DECIMAL;
          r3 =
              leftFast2
            - 1;
        } else {
          result2 = r2;
          r3 =
              leftFast2;
        }
        long r4;
        if (r3 < 0) {
          result3 = r3 + MULTIPLER_LONGWORD_DECIMAL;
          r4 = - 1;
        } else {
          result3 = r3;
          r4 = 0;
        }
        if (r4 != 0) {
          throw new RuntimeException("Unexpected underflow");
        }

      } else if (diffScale < TWO_X_LONGWORD_DECIMAL_DIGITS) {

        final long divideFactor = powerOfTenTable[TWO_X_LONGWORD_DECIMAL_DIGITS - diffScale];
        final long multiplyFactor = powerOfTenTable[diffScale - LONGWORD_DECIMAL_DIGITS];

        final long r0 =
          - rightFast0;
        long r1;
        if (r0 < 0) {
          result0 = r0 + MULTIPLER_LONGWORD_DECIMAL;
          r1 =
              (leftFast0 % divideFactor) * multiplyFactor
            - rightFast1
            - 1;
        } else {
          result0 = r0;
          r1 =
              (leftFast0 % divideFactor) * multiplyFactor
            - rightFast1;
        }
        long r2;
        if (r1 < 0) {
          result1 = r1 + MULTIPLER_LONGWORD_DECIMAL;
          r2 =
              leftFast0 / divideFactor
            + (leftFast1 % divideFactor) * multiplyFactor
            - rightFast2
            - 1;
        } else {
          result1 = r1;
          r2 =
              leftFast0 / divideFactor
            + (leftFast1 % divideFactor) * multiplyFactor
            - rightFast2;
        }
        long r3;
        if (r2 < 0) {
          result2 = r2 + MULTIPLER_LONGWORD_DECIMAL;
          r3 =
              leftFast1 / divideFactor
            + (leftFast2 % divideFactor) * multiplyFactor
            - 1;
        } else {
          result2 = r2;
          r3 =
              leftFast1 / divideFactor
            + (leftFast2 % divideFactor) * multiplyFactor;
        }
        long r4;
        if (r3 < 0) {
          result3 = r3 + MULTIPLER_LONGWORD_DECIMAL;
          r4 =
              leftFast2 / divideFactor
            - 1;
        } else {
          result3 = r3;
          r4 =
              leftFast2 / divideFactor;
        }
        if (r4 < 0) {
          result4 = r4 + MULTIPLER_LONGWORD_DECIMAL;
        } else {
          result4 = r4;
        }

      } else if (diffScale == TWO_X_LONGWORD_DECIMAL_DIGITS) {

        final long r0 =
            - rightFast0;
        long r1;
        if (r0 < 0) {
          result0 = r0 + MULTIPLER_LONGWORD_DECIMAL;
          r1 =
            - rightFast1
            - 1;
        } else {
          result0 = r0;
          r1 =
            - rightFast1;
        }
        long r2;
        if (r1 < 0) {
          result1 = r1 + MULTIPLER_LONGWORD_DECIMAL;
          r2 =
              leftFast0
            - rightFast2
            - 1;
        } else {
          result1 = r1;
          r2 =
              leftFast0
            - rightFast2;
        }
        long r3;
        if (r2 < 0) {
          result2 = r2 + MULTIPLER_LONGWORD_DECIMAL;
          r3 =
              leftFast1
            - 1;
        } else {
          result2 = r2;
          r3 =
              leftFast1;
        }
        long r4;
        if (r3 < 0) {
          result3 = r3 + MULTIPLER_LONGWORD_DECIMAL;
          r4 =
              leftFast2
            - 1;
        } else {
          result3 = r3;
          r4 =
              leftFast2;
        }
        long r5;
        if (r4 < 0) {
          result4 = r4 + MULTIPLER_LONGWORD_DECIMAL;
          r5 = - 1;
        } else {
          result4 = r4;
          r5 = 0;
        }
        if (r5 != 0) {
          throw new RuntimeException("Unexpected underflow");
        }

      } else {

        final long divideFactor = powerOfTenTable[THREE_X_LONGWORD_DECIMAL_DIGITS - diffScale];
        final long multiplyFactor = powerOfTenTable[diffScale - TWO_X_LONGWORD_DECIMAL_DIGITS];

        final long r0 =
            - rightFast0;
        long r1;
        if (r0 < 0) {
          result0 = r0 + MULTIPLER_LONGWORD_DECIMAL;
          r1 =
            - rightFast1
            - 1;
        } else {
          result0 = r0;
          r1 =
            - rightFast1;
        }
        long r2;
        if (r1 < 0) {
          result1 = r1 + MULTIPLER_LONGWORD_DECIMAL;
          r2 =
              (leftFast0 % divideFactor) * multiplyFactor
            - rightFast2
            - 1;
        } else {
          result1 = r1;
          r2 =
              (leftFast0 % divideFactor) * multiplyFactor
            - rightFast2;
        }
        long r3;
        if (r2 < 0) {
          result2 = r2 + MULTIPLER_LONGWORD_DECIMAL;
          r3 =
              leftFast0 / divideFactor
            + (leftFast1 % divideFactor) * multiplyFactor
            - 1;
        } else {
          result2 = r2;
          r3 =
              leftFast0 / divideFactor
            + (leftFast1 % divideFactor) * multiplyFactor;
        }
        long r4;
        if (r3 < 0) {
          result3 = r3 + MULTIPLER_LONGWORD_DECIMAL;
          r4 =
              leftFast1 / divideFactor
            + (leftFast2 % divideFactor) * multiplyFactor
            - 1;
        } else {
          result3 = r3;
          r4 =
              leftFast1 / divideFactor
            + (leftFast2 % divideFactor) * multiplyFactor;
        }
        long r5;
        if (r4 < 0) {
          result4 = r4 + MULTIPLER_LONGWORD_DECIMAL;
          r5 =
              leftFast2 / divideFactor
            - 1;
        } else {
          result4 = r4;
          r5 =
              leftFast2 / divideFactor;
        }
        if (r5 != 0) {
          throw new RuntimeException("Unexpected underflow");
        }
      }
    }

    return
        doFinishAddSubtractDifferentScale(
            result0, result1, result2, result3, result4,
            resultScale,
            fastResult);
  }

  /**
   * Handle decimal addition when the values have different scales.
   */
  private static boolean fastAddDifferentScale(
      long leftFast0, long leftFast1, long leftFast2,
      int leftIntegerDigitCount, int leftScale,
      long rightFast0, long rightFast1, long rightFast2,
      int rightIntegerDigitCount, int rightScale,
      FastHiveDecimal fastResult) {

    // Arrange so result* has a longer digit tail and it lines up; we will shift the shift* digits
    // as we do our addition and them into the result.
    long result0;
    long result1;
    long result2;

    long shift0;
    long shift1;
    long shift2;

    int diffScale;
    int resultScale;

    // Since addition is commutative, we can add in any order.
    if (leftScale > rightScale) {

      result0 = leftFast0;
      result1 = leftFast1;
      result2 = leftFast2;

      shift0 = rightFast0;
      shift1 = rightFast1;
      shift2 = rightFast2;

      diffScale = leftScale - rightScale;
      resultScale = leftScale;
    } else {

      result0 = rightFast0;
      result1 = rightFast1;
      result2 = rightFast2;

      shift0 = leftFast0;
      shift1 = leftFast1;
      shift2 = leftFast2;

      diffScale = rightScale - leftScale;
      resultScale = rightScale;
    }

    long result3 = 0;
    long result4 = 0;

    if (diffScale < LONGWORD_DECIMAL_DIGITS) {

      final long divideFactor = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - diffScale];
      final long multiplyFactor = powerOfTenTable[diffScale];

      final long r0 =
          result0
        + (shift0 % divideFactor) * multiplyFactor;
      result0 =
          r0 % MULTIPLER_LONGWORD_DECIMAL;
      final long r1 =
          result1
        + shift0 / divideFactor
        + (shift1 % divideFactor) * multiplyFactor
        + r0 / MULTIPLER_LONGWORD_DECIMAL;
      result1 =
          r1 % MULTIPLER_LONGWORD_DECIMAL;
      final long r2 =
          result2
        + shift1 / divideFactor
        + (shift2 % divideFactor) * multiplyFactor
        + r1 / MULTIPLER_LONGWORD_DECIMAL;
      result2 =
          r2 % MULTIPLER_LONGWORD_DECIMAL;
      final long r3 =
          shift2 / divideFactor
        + r2 / MULTIPLER_LONGWORD_DECIMAL;
      result3 =
          r3 % MULTIPLER_LONGWORD_DECIMAL;

    } else if (diffScale == LONGWORD_DECIMAL_DIGITS){

      final long r1 =
          result1
        + shift0;
      result1 =
          r1 % MULTIPLER_LONGWORD_DECIMAL;
      final long r2 =
          result2
        + shift1
        + r1 / MULTIPLER_LONGWORD_DECIMAL;
      result2 =
          r2 % MULTIPLER_LONGWORD_DECIMAL;
      final long r3 =
          shift2
        + r2 / MULTIPLER_LONGWORD_DECIMAL;
      result3 =
          r3 % MULTIPLER_LONGWORD_DECIMAL;
      result4 = r3 / MULTIPLER_LONGWORD_DECIMAL;

    } else if (diffScale < TWO_X_LONGWORD_DECIMAL_DIGITS) {

      final long divideFactor = powerOfTenTable[TWO_X_LONGWORD_DECIMAL_DIGITS - diffScale];
      final long multiplyFactor = powerOfTenTable[diffScale - LONGWORD_DECIMAL_DIGITS];

      final long r1 =
          result1
        + (shift0 % divideFactor) * multiplyFactor;
      result1 =
          r1 % MULTIPLER_LONGWORD_DECIMAL;
      final long r2 =
          result2
        + shift0 / divideFactor
        + (shift1 % divideFactor) * multiplyFactor
        + r1 / MULTIPLER_LONGWORD_DECIMAL;
      result2 =
          r2 % MULTIPLER_LONGWORD_DECIMAL;
      final long r3 =
          shift1 / divideFactor
        + (shift2 % divideFactor) * multiplyFactor
        + r2 / MULTIPLER_LONGWORD_DECIMAL;
      result3 =
          r3 % MULTIPLER_LONGWORD_DECIMAL;
      final long r4 =
          shift2 / divideFactor
        + r3 / MULTIPLER_LONGWORD_DECIMAL;
      result4 =
          r4 % MULTIPLER_LONGWORD_DECIMAL;

    } else if (diffScale == TWO_X_LONGWORD_DECIMAL_DIGITS) {

      final long r2 =
          result2
        + shift0;
      result2 =
          r2 % MULTIPLER_LONGWORD_DECIMAL;
      final long r3 =
          shift1
        + r2 / MULTIPLER_LONGWORD_DECIMAL;
      result3 =
          r3 % MULTIPLER_LONGWORD_DECIMAL;
      final long r4 =
          shift2
        + r3 / MULTIPLER_LONGWORD_DECIMAL;
      result4 =
          r4 % MULTIPLER_LONGWORD_DECIMAL;

    } else {

      final long divideFactor = powerOfTenTable[THREE_X_LONGWORD_DECIMAL_DIGITS - diffScale];
      final long multiplyFactor = powerOfTenTable[diffScale - TWO_X_LONGWORD_DECIMAL_DIGITS];

      final long r2 =
          result2
        + (shift0 % divideFactor) * multiplyFactor;
      result2 =
          r2 % MULTIPLER_LONGWORD_DECIMAL;
      final long r3 =
          shift0 / divideFactor
        + (shift1 % divideFactor) * multiplyFactor
        + r2 / MULTIPLER_LONGWORD_DECIMAL;
      result3 =
          r3 % MULTIPLER_LONGWORD_DECIMAL;
      final long r4 =
          shift1 / divideFactor
        + (shift2 % divideFactor) * multiplyFactor
        + r3 / MULTIPLER_LONGWORD_DECIMAL;
      result4 =
          r4 % MULTIPLER_LONGWORD_DECIMAL;
      if (shift2 / divideFactor != 0) {
        throw new RuntimeException("Unexpected overflow");
      }

    }

    return
        doFinishAddSubtractDifferentScale(
            result0, result1, result2, result3, result4,
            resultScale,
            fastResult);
  }

  private static boolean doAddDifferentScale(
      int leftSignum, long leftFast0, long leftFast1, long leftFast2,
      int leftIntegerDigitCount, int leftScale,
      int rightSignum, long rightFast0, long rightFast1, long rightFast2,
      int rightIntegerDigitCount, int rightScale,
      FastHiveDecimal fastResult) {

    if (leftSignum == rightSignum) {
      if (!fastAddDifferentScale(
          leftFast0, leftFast1, leftFast2,
          leftIntegerDigitCount, leftScale,
          rightFast0, rightFast1, rightFast2,
          rightIntegerDigitCount, rightScale,
          fastResult)) {
        return false;
      }
      // Sign stays the same.
      fastResult.fastSignum = leftSignum;
    } else {

      // Just compare the magnitudes (i.e. signums set to 1).
      int compareTo =
          fastCompareTo(
              1,
              leftFast0, leftFast1, leftFast2, leftScale,
              1,
              rightFast0, rightFast1, rightFast2, rightScale);
      if (compareTo == 0) {
        // They cancel each other.
        fastResult.fastSignum = 0;
        fastResult.fast0 = 0;
        fastResult.fast1 = 0;
        fastResult.fast2 = 0;
        fastResult.fastScale = 0;
        return true;
      }

      if (compareTo == 1) {
        if (!fastSubtractDifferentScale(
                leftFast0, leftFast1, leftFast2,
                leftIntegerDigitCount, leftScale,
                rightFast0, rightFast1, rightFast2,
                rightIntegerDigitCount, rightScale,
                fastResult)) {
          throw new RuntimeException("Unexpected overflow");
        }
        fastResult.fastSignum = leftSignum;
      } else {
        if (!fastSubtractDifferentScale(
                rightFast0, rightFast1, rightFast2,
                rightIntegerDigitCount, rightScale,
                leftFast0, leftFast1, leftFast2,
                leftIntegerDigitCount, leftScale,
                fastResult)) {
          throw new RuntimeException("Unexpected overflow");
        }
        fastResult.fastSignum = rightSignum;
      }
    }

    final int resultTrailingZeroCount =
        fastTrailingDecimalZeroCount(
            fastResult.fast0, fastResult.fast1, fastResult.fast2,
            fastResult.fastIntegerDigitCount, fastResult.fastScale);
    if (resultTrailingZeroCount > 0) {
      doFastScaleDown(
          fastResult,
          resultTrailingZeroCount,
          fastResult);
      if (fastResult.fastSignum == 0) {
        fastResult.fastScale = 0;
      } else {
        fastResult.fastScale -= resultTrailingZeroCount;
      }
    }

    return true;
  }

  public static boolean fastAdd(
      FastHiveDecimal fastLeft,
      FastHiveDecimal fastRight,
      FastHiveDecimal fastResult) {
    return fastAdd(
        fastLeft.fastSignum, fastLeft.fast0, fastLeft.fast1, fastLeft.fast2,
        fastLeft.fastIntegerDigitCount, fastLeft.fastScale,
        fastRight.fastSignum, fastRight.fast0, fastRight.fast1, fastRight.fast2,
        fastRight.fastIntegerDigitCount, fastRight.fastScale,
        fastResult);
  }

  /**
   * Add the two decimals.
   *
   * NOTE: Scale Determination for Addition/Subtraction
   *
   * One could take the Math.min of the scales and adjust the operand with the lower scale have a
   * scale = higher scale.
   *
   * But this does not seem to work with decimals with widely varying scales as these:
   *
   *     598575157855521918987423259.94094                            dec1 (int digits 27,scale 5)
   *  +                            0.0000000000006711991169422033     dec2 (int digits 0, scale 28)
   *
   * Trying to make dec1 to have a scale of 28 (i.e. by adding trailing zeroes) would exceed
   * MAX_PRECISION (int digits 27 + 28 &gt; 38).
   *
   * In this example we need to make sure we have enough integer digit room in the result to
   * handle dec1's digits.  In order to maintain that, we will need to get rid of lower
   * fractional digits of dec2.  But when do we do that?
   *
   * OldHiveDecimal.add does the full arithmetic add with all the digits using BigDecimal and
   * then adjusts the result to fit in MAX_PRECISION, etc.
   *
   * If we try to do pre-rounding dec2 it is problematic.  We'd need to know if there is a carry in
   * the arithmetic in order to know at which scale to do the rounding.  This gets complicated.
   *
   * So, the simplest thing is to emulate what OldHiveDecimal does and do the full digit addition
   * and then fit the result afterwards.
   *
   * @param leftSignum The left sign (-1, 0, or +1)
   * @param leftFast0 The left word 0 of reprentation
   * @param leftFast1 word 1
   * @param leftFast2 word 2
   * @param leftIntegerDigitCount The left number of integer digits
   * @param leftScale the left scale
   * @param rightSignum The right sign (-1, 0, or +1)
   * @param rightFast0 The right word 0 of reprentation
   * @param rightFast1 word 1
   * @param rightFast2 word 2
   * @param rightIntegerDigitCount The right number of integer digits
   * @param rightScale the right scale
   * @param fastResult an object to reuse
   * @return True if the addition was successful; Otherwise, false is returned on overflow.
   */
  public static boolean fastAdd(
      int leftSignum, long leftFast0, long leftFast1, long leftFast2,
      int leftIntegerDigitCount, int leftScale,
      int rightSignum, long rightFast0, long rightFast1, long rightFast2,
      int rightIntegerDigitCount, int rightScale,
      FastHiveDecimal fastResult) {

    if (rightSignum == 0) {
      fastResult.fastSet(leftSignum, leftFast0, leftFast1, leftFast2, leftIntegerDigitCount, leftScale);
      return true;
    }
    if (leftSignum == 0) {
      fastResult.fastSet(rightSignum, rightFast0, rightFast1, rightFast2, rightIntegerDigitCount, rightScale);
      return true;
    }

    if (leftScale == rightScale) {
      return doAddSameScale(
          leftSignum, leftFast0, leftFast1, leftFast2,
          rightSignum, rightFast0, rightFast1, rightFast2,
          leftScale,
          fastResult);
    } else {
      return doAddDifferentScale(
          leftSignum, leftFast0, leftFast1, leftFast2,
          leftIntegerDigitCount, leftScale,
          rightSignum, rightFast0, rightFast1, rightFast2,
          rightIntegerDigitCount, rightScale,
          fastResult);
    }
  }

  public static boolean fastSubtract(
      FastHiveDecimal fastLeft,
      FastHiveDecimal fastRight,
      FastHiveDecimal fastResult) {
    return fastSubtract(
        fastLeft.fastSignum, fastLeft.fast0, fastLeft.fast1, fastLeft.fast2,
        fastLeft.fastIntegerDigitCount, fastLeft.fastScale,
        fastRight.fastSignum, fastRight.fast0, fastRight.fast1, fastRight.fast2,
        fastRight.fastIntegerDigitCount, fastRight.fastScale,
        fastResult);
  }

  public static boolean fastSubtract(
      int leftSignum, long leftFast0, long leftFast1, long leftFast2,
      int leftIntegerDigitCount, int leftScale,
      int rightSignum, long rightFast0, long rightFast1, long rightFast2,
      int rightIntegerDigitCount, int rightScale,
      FastHiveDecimal fastResult) {

    if (rightSignum == 0) {
      fastResult.fastSet(leftSignum, leftFast0, leftFast1, leftFast2, leftIntegerDigitCount, leftScale);
      return true;
    }
    final int flippedDecSignum = (rightSignum == 1 ? -1 : 1);
    if (leftSignum == 0) {
      fastResult.fastSet(flippedDecSignum, rightFast0, rightFast1, rightFast2, rightIntegerDigitCount, rightScale);
      return true;
    }

    if (leftScale == rightScale) {
      return doAddSameScale(
          leftSignum, leftFast0, leftFast1, leftFast2,
          flippedDecSignum, rightFast0, rightFast1, rightFast2,
          leftScale,
          fastResult);
    } else {
      return doAddDifferentScale(
          leftSignum, leftFast0, leftFast1, leftFast2,
          leftIntegerDigitCount, leftScale,
          flippedDecSignum, rightFast0, rightFast1, rightFast2,
          rightIntegerDigitCount, rightScale,
          fastResult);
    }
  }

  //************************************************************************************************
  // Decimal Multiply.

  private static boolean doMultiply(
      int leftSignum, long leftFast0, long leftFast1, long leftFast2,
      int leftIntegerDigitCount, int leftScale,
      int rightSignum, long rightFast0, long rightFast1, long rightFast2,
      int rightIntegerDigitCount, int rightScale,
      FastHiveDecimal fastResult) {

    // Set signum before; if result is zero, fastMultiply will set signum to 0.
    fastResult.fastSignum = (leftSignum == rightSignum ? 1 : -1);
    int resultScale = leftScale + rightScale;

    /*
     * For multiplicands with scale 0, trim trailing zeroes.
     */
    if (leftScale == 0) {

      // Pretend like it has fractional digits so we can get the trailing zero count.
      final int leftTrailingZeroCount =
          fastTrailingDecimalZeroCount(
              leftFast0, leftFast1, leftFast2,
              0, leftIntegerDigitCount);
      if (leftTrailingZeroCount > 0) {
        doFastScaleDown(
            leftFast0, leftFast1, leftFast2, leftTrailingZeroCount, fastResult);
        resultScale -= leftTrailingZeroCount;
        leftFast0 = fastResult.fast0;
        leftFast1 = fastResult.fast1;
        leftFast2 = fastResult.fast2;
      }
    }
    if (rightScale == 0) {

      // Pretend like it has fractional digits so we can get the trailing zero count.
      final int rightTrailingZeroCount =
          fastTrailingDecimalZeroCount(
              rightFast0, rightFast1, rightFast2,
              0, rightIntegerDigitCount);
      if (rightTrailingZeroCount > 0) {
        doFastScaleDown(
            rightFast0, rightFast1, rightFast2, rightTrailingZeroCount, fastResult);
        resultScale -= rightTrailingZeroCount;
        rightFast0 = fastResult.fast0;
        rightFast1 = fastResult.fast1;
        rightFast2 = fastResult.fast2;
      }
    }

    boolean largeOverflow =
        !fastMultiply5x5HalfWords(
            leftFast0, leftFast1, leftFast2,
            rightFast0, rightFast1, rightFast2,
            fastResult);
    if (largeOverflow) {
      return false;
    }

    if (fastResult.fastSignum == 0) {
      fastResult.fastScale = 0;
      return true;
    }

    if (resultScale < 0) {
      if (-resultScale >= HiveDecimal.MAX_SCALE) {
        return false;
      }
      if (!fastScaleUp(
          fastResult.fast0, fastResult.fast1, fastResult.fast2, -resultScale,
          fastResult)) {
        return false;
      }
      resultScale = 0;
    }

    int precision;
    if (fastResult.fast2 != 0) {
      precision = TWO_X_LONGWORD_DECIMAL_DIGITS + fastLongWordPrecision(fastResult.fast2);
    } else if (fastResult.fast1 != 0) {
      precision = LONGWORD_DECIMAL_DIGITS + fastLongWordPrecision(fastResult.fast1);
    } else {
      precision = fastLongWordPrecision(fastResult.fast0);
    }

    int integerDigitCount = Math.max(0, precision - resultScale);
    if (integerDigitCount > HiveDecimal.MAX_PRECISION) {
      // Integer is too large -- cannot recover by trimming fractional digits.
      return false;
    }

    if (precision > HiveDecimal.MAX_PRECISION || resultScale > HiveDecimal.MAX_SCALE) {

      // Trim off lower fractional digits but with NO ROUNDING.

      final int maxScale = HiveDecimal.MAX_SCALE - integerDigitCount;
      final int scaleDown = resultScale - maxScale;
      if (!fastScaleDownNoRound(
          fastResult.fastSignum, fastResult.fast0, fastResult.fast1, fastResult.fast2,
          scaleDown,
          fastResult)) {
        // Round fractional must be 0.  Not allowed to throw away digits.
        return false;
      }
      resultScale -= scaleDown;
    }
    fastResult.fastScale = resultScale;

    // This assume no round up...
    fastResult.fastIntegerDigitCount = integerDigitCount;

    if (fastResult.fastScale > HiveDecimal.MAX_SCALE) {
      // We are not allowed to lose digits in multiply to be compatible with OldHiveDecimal
      // behavior, so overflow.
      // CONSIDER: Does it make sense to be so restrictive.  If we just did repeated addition,
      //           it would succeed...
      return false;
    }
    final int resultTrailingZeroCount =
        fastTrailingDecimalZeroCount(
            fastResult.fast0, fastResult.fast1, fastResult.fast2,
            fastResult.fastIntegerDigitCount, fastResult.fastScale);
    if (resultTrailingZeroCount > 0) {
      doFastScaleDown(
          fastResult,
          resultTrailingZeroCount,
          fastResult);
      if (fastResult.fastSignum == 0) {
        fastResult.fastScale = 0;
      } else {
        fastResult.fastScale -= resultTrailingZeroCount;
      }
    }

    return true;
  }

  public static boolean fastMultiply5x5HalfWords(
      FastHiveDecimal fastLeft,
      FastHiveDecimal fastRight,
      FastHiveDecimal fastResult) {
    return
        fastMultiply5x5HalfWords(
            fastLeft.fast0, fastLeft.fast1, fastLeft.fast2,
            fastRight.fast0, fastRight.fast1, fastRight.fast2,
            fastResult);
  }

  /**
   * Fast decimal multiplication on two decimals that have been already scaled and whose results
   * will fit in 38 digits.
   *
   * The caller is responsible checking for overflow within the highword and determining
   * if scale down appropriate.
   *
   * @return  Returns false if the multiplication resulted in large overflow.  Values in result are
   *          undefined in that case.
   */
  public static boolean fastMultiply5x5HalfWords(
      long left0, long left1, long left2,
      long right0, long right1, long right2,
      FastHiveDecimal fastResult) {

    long product;

    final long halfRight0 = right0 % MULTIPLER_INTWORD_DECIMAL;
    final long halfRight1 = right0 / MULTIPLER_INTWORD_DECIMAL;
    final long halfRight2 = right1 % MULTIPLER_INTWORD_DECIMAL;
    final long halfRight3 = right1 / MULTIPLER_INTWORD_DECIMAL;
    final long halfRight4 = right2 % MULTIPLER_INTWORD_DECIMAL;

    final long halfLeft0 = left0 % MULTIPLER_INTWORD_DECIMAL;
    final long halfLeft1 = left0 / MULTIPLER_INTWORD_DECIMAL;
    final long halfLeft2 = left1 % MULTIPLER_INTWORD_DECIMAL;
    final long halfLeft3 = left1 / MULTIPLER_INTWORD_DECIMAL;
    final long halfLeft4 = left2 % MULTIPLER_INTWORD_DECIMAL;

    // v[0]
    product =
        halfRight0 * halfLeft0;
    final int z0 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[1] where (product % MULTIPLER_INTWORD_DECIMAL) is the carry from v[0]. 
    product =
        halfRight0
      * halfLeft1
      + halfRight1
      * halfLeft0
      + (product / MULTIPLER_INTWORD_DECIMAL);
    final int z1 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[2]
    product =
        halfRight0
      * halfLeft2
      + halfRight1
      * halfLeft1
      + halfRight2
      * halfLeft0
      + (product / MULTIPLER_INTWORD_DECIMAL);
    final int z2 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[3]
    product =
        halfRight0
      * halfLeft3
      + halfRight1
      * halfLeft2
      + halfRight2
      * halfLeft1
      + halfRight3
      * halfLeft0
      + (product / MULTIPLER_INTWORD_DECIMAL);
    final int z3 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[4]
    product =
        halfRight0
      * halfLeft4
      + halfRight1
      * halfLeft3
      + halfRight2
      * halfLeft2
      + halfRight3
      * halfLeft1
      + halfRight4
      * halfLeft0
      + (product / MULTIPLER_INTWORD_DECIMAL);

    // v[5] is not calculated since high integer is always 0 for our decimals.

    // These remaining combinations below definitely result in overflow.
    if ((halfRight4 != 0 && (halfLeft4 != 0 || halfLeft3 != 0 || halfLeft2 != 0 || halfLeft1 != 0))
        || (halfRight3 != 0 && (halfLeft4 != 0 || halfLeft3 != 0 || halfLeft2 != 0))
        || (halfRight2 != 0 && (halfLeft4 != 0 || halfLeft3 != 0))
        || (halfRight1 != 0 && halfLeft4 != 0)) {
      return false;
    }


    final long result0 = (long) z1 * MULTIPLER_INTWORD_DECIMAL + (long) z0;
    final long result1 = (long) z3 * MULTIPLER_INTWORD_DECIMAL + (long) z2;
    final long result2 = product;

    if (result0 == 0 && result1 == 0 && result2 == 0) {
      fastResult.fastSignum = 0;
    }
    fastResult.fast0 = result0;
    fastResult.fast1 = result1;
    fastResult.fast2 = result2;

    return true;
  }

  public static boolean fastMultiplyFullInternal(
      FastHiveDecimal fastLeft,
      FastHiveDecimal fastRight,
      long[] result) {
    return
        fastMultiplyFullInternal(
            fastLeft.fast0, fastLeft.fast1, fastLeft.fast2,
            fastRight.fast0, fastRight.fast1, fastRight.fast2,
            result);
  }

  /**
   * Fast decimal multiplication on two decimals that have been already scaled and whose results
   * will fit in 38 digits.
   *
   * The caller is responsible checking for overflow within the highword and determining
   * if scale down appropriate.
   *
   * @return  Returns false if the multiplication resulted in large overflow.  Values in result are
   *          undefined in that case.
   */
  public static boolean fastMultiply5x5HalfWords(
      long left0, long left1, long left2,
      long right0, long right1, long right2,
      long[] result) {

    long product;

    final long halfRight0 = right0 % MULTIPLER_INTWORD_DECIMAL;
    final long halfRight1 = right0 / MULTIPLER_INTWORD_DECIMAL;
    final long halfRight2 = right1 % MULTIPLER_INTWORD_DECIMAL;
    final long halfRight3 = right1 / MULTIPLER_INTWORD_DECIMAL;
    final long halfRight4 = right2 % MULTIPLER_INTWORD_DECIMAL;

    final long halfLeft0 = left0 % MULTIPLER_INTWORD_DECIMAL;
    final long halfLeft1 = left0 / MULTIPLER_INTWORD_DECIMAL;
    final long halfLeft2 = left1 % MULTIPLER_INTWORD_DECIMAL;
    final long halfLeft3 = left1 / MULTIPLER_INTWORD_DECIMAL;
    final long halfLeft4 = left2 % MULTIPLER_INTWORD_DECIMAL;

    // v[0]
    product =
        halfRight0 * halfLeft0;
    final int z0 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[1] where (product % MULTIPLER_INTWORD_DECIMAL) is the carry from v[0].
    product =
        halfRight0
      * halfLeft1
      + halfRight1
      * halfLeft0
      + (product / MULTIPLER_INTWORD_DECIMAL);
    final int z1 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[2]
    product =
        halfRight0
      * halfLeft2
      + halfRight1
      * halfLeft1
      + halfRight2
      * halfLeft0
      + (product / MULTIPLER_INTWORD_DECIMAL);
    final int z2 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[3]
    product =
        halfRight0
      * halfLeft3
      + halfRight1
      * halfLeft2
      + halfRight2
      * halfLeft1
      + halfRight3
      * halfLeft0
      + (product / MULTIPLER_INTWORD_DECIMAL);
    final int z3 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[4]
    product =
        halfRight0
      * halfLeft4
      + halfRight1
      * halfLeft3
      + halfRight2
      * halfLeft2
      + halfRight3
      * halfLeft1
      + halfRight4
      * halfLeft0
      + (product / MULTIPLER_INTWORD_DECIMAL);

    // v[5] is not calculated since high integer is always 0 for our decimals.

    // These remaining combinations below definitely result in overflow.
    if ((halfRight4 != 0 && (halfLeft4 != 0 || halfLeft3 != 0 || halfLeft2 != 0 || halfLeft1 != 0))
        || (halfRight3 != 0 && (halfLeft4 != 0 || halfLeft3 != 0 || halfLeft2 != 0))
        || (halfRight2 != 0 && (halfLeft4 != 0 || halfLeft3 != 0))
        || (halfRight1 != 0 && halfLeft4 != 0)) {
      return false;
    }

    result[0] = (long) z1 * MULTIPLER_INTWORD_DECIMAL + (long) z0;
    result[1] = (long) z3 * MULTIPLER_INTWORD_DECIMAL + (long) z2;
    result[2] = product;

    return true;
  }

  /**
   * Fast decimal multiplication on two decimals whose results are permitted to go beyond
   * 38 digits to the maximum possible 76 digits.  The caller is responsible for scaling and
   * rounding the results back to 38 or fewer digits.
   *
   * The caller is responsible for determining the signum.
   *
   * @param left0
   * @param left1
   * @param left2
   * @param right0
   * @param right1
   * @param right2
   * @param result  This full result has 5 longs.
   * @return  Returns false if the multiplication resulted in an overflow.  Values in result are
   *          undefined in that case.
   */
  public static boolean fastMultiplyFullInternal(
      long left0, long left1, long left2,
      long right0, long right1, long right2,
      long[] result) {
    if (result.length != 5) {
      throw new IllegalArgumentException("Expecting result array length = 5");
    }

    long product;

    final long halfRight0 = right0 % MULTIPLER_INTWORD_DECIMAL;
    final long halfRight1 = right0 / MULTIPLER_INTWORD_DECIMAL;
    final long halfRight2 = right1 % MULTIPLER_INTWORD_DECIMAL;
    final long halfRight3 = right1 / MULTIPLER_INTWORD_DECIMAL;
    final long halfRight4 = right2 % MULTIPLER_INTWORD_DECIMAL;

    final long halfLeft0 = left0 % MULTIPLER_INTWORD_DECIMAL;
    final long halfLeft1 = left0 / MULTIPLER_INTWORD_DECIMAL;
    final long halfLeft2 = left1 % MULTIPLER_INTWORD_DECIMAL;
    final long halfLeft3 = left1 / MULTIPLER_INTWORD_DECIMAL;
    final long halfLeft4 = left2 % MULTIPLER_INTWORD_DECIMAL;

    // v[0]
    product =
        halfRight0 * halfLeft0;
    final int z0 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[1] where (product % MULTIPLER_INTWORD_DECIMAL) is the carry from v[0].
    product =
        halfRight0
      * halfLeft1
      + halfRight1
      * halfLeft0
      + (product / MULTIPLER_INTWORD_DECIMAL);
    final int z1 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[2]
    product =
        halfRight0
      * halfLeft2
      + halfRight1
      * halfLeft1
      + halfRight2
      * halfLeft0
      + (product / MULTIPLER_INTWORD_DECIMAL);
    final int z2 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[3]
    product =
        halfRight0
      * halfLeft3
      + halfRight1
      * halfLeft2
      + halfRight2
      * halfLeft1
      + halfRight3
      * halfLeft0
      + (product / MULTIPLER_INTWORD_DECIMAL);
    final int z3 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[4]
    product =
        halfRight0
      * halfLeft4
      + halfRight1
      * halfLeft3
      + halfRight2
      * halfLeft2
      + halfRight3
      * halfLeft1
      + halfRight4
      * halfLeft0
      + (product / MULTIPLER_INTWORD_DECIMAL);
    final int z4 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[5] -- since integer #5 is always 0, some products here are not included.
    product =
        halfRight1
      * halfLeft4
      + halfRight2
      * halfLeft3
      + halfRight3
      * halfLeft2
      + halfRight4
      * halfLeft1
      + (product / MULTIPLER_INTWORD_DECIMAL);
    final int z5 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[6] -- since integer #5 is always 0, some products here are not included.
    product =
        halfRight2
      * halfLeft4
      + halfRight3
      * halfLeft3
      + halfRight4
      * halfLeft2
      + (product / MULTIPLER_INTWORD_DECIMAL);
    final int z6 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[7] -- since integer #5 is always 0, some products here are not included.
    product =
        halfRight3
      * halfLeft4
      + halfRight4
      * halfLeft3
      + (product / MULTIPLER_INTWORD_DECIMAL);
    final int z7 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[8] -- since integer #5 is always 0, some products here are not included.
    product =
        halfRight4
      * halfLeft4
      + (product / MULTIPLER_INTWORD_DECIMAL);
    final int z8 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[9] -- since integer #5 is always 0, some products here are not included.
    product =
        (product / MULTIPLER_INTWORD_DECIMAL);
    if (product > FULL_MAX_HIGHWORD_DECIMAL) {
      return false;
    }

    result[0] = (long) z1 * MULTIPLER_INTWORD_DECIMAL + (long) z0;
    result[1] = (long) z3 * MULTIPLER_INTWORD_DECIMAL + (long) z2;
    result[2] = (long) z5 * MULTIPLER_INTWORD_DECIMAL + (long) z4;
    result[3] = (long) z7 * MULTIPLER_INTWORD_DECIMAL + (long) z6;
    result[4] = product * MULTIPLER_INTWORD_DECIMAL + (long) z8;

    return true;
  }

  /**
   * Fast decimal multiplication on two decimals whose results are permitted to go beyond
   * 38 digits to the maximum possible 76 digits.  The caller is responsible for scaling and
   * rounding the results back to 38 or fewer digits.
   *
   * The caller is responsible for determining the signum.
   *
   * @param result  This full result has 5 longs.
   * @return  Returns false if the multiplication resulted in an overflow.  Values in result are
   *          undefined in that case.
   */
  public static boolean fastMultiply5x6HalfWords(
      long left0, long left1, long left2,
      long right0, long right1, long right2,
      long[] result) {

    if (result.length != 6) {
      throw new RuntimeException("Expecting result array length = 6");
    }

    long product;

    final long halfRight0 = right0 % MULTIPLER_INTWORD_DECIMAL;
    final long halfRight1 = right0 / MULTIPLER_INTWORD_DECIMAL;
    final long halfRight2 = right1 % MULTIPLER_INTWORD_DECIMAL;
    final long halfRight3 = right1 / MULTIPLER_INTWORD_DECIMAL;
    final long halfRight4 = right2 % MULTIPLER_INTWORD_DECIMAL;
    final long halfRight5 = right2 / MULTIPLER_INTWORD_DECIMAL;

    final long halfLeft0 = left0 % MULTIPLER_INTWORD_DECIMAL;
    final long halfLeft1 = left0 / MULTIPLER_INTWORD_DECIMAL;
    final long halfLeft2 = left1 % MULTIPLER_INTWORD_DECIMAL;
    final long halfLeft3 = left1 / MULTIPLER_INTWORD_DECIMAL;
    final long halfLeft4 = left2 % MULTIPLER_INTWORD_DECIMAL;

    // v[0]
    product =
        halfRight0 * halfLeft0;
    final int z0 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[1] where (product % MULTIPLER_INTWORD_DECIMAL) is the carry from v[0].
    product =
        halfRight0
      * halfLeft1
      + halfRight1
      * halfLeft0
      + (product / MULTIPLER_INTWORD_DECIMAL);
    final int z1 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[2]
    product =
        halfRight0
      * halfLeft2
      + halfRight1
      * halfLeft1
      + halfRight2
      * halfLeft0
      + (product / MULTIPLER_INTWORD_DECIMAL);
    final int z2 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[3]
    product =
        halfRight0
      * halfLeft3
      + halfRight1
      * halfLeft2
      + halfRight2
      * halfLeft1
      + halfRight3
      * halfLeft0
      + (product / MULTIPLER_INTWORD_DECIMAL);
    final int z3 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[4]
    product =
        halfRight0
      * halfLeft4
      + halfRight1
      * halfLeft3
      + halfRight2
      * halfLeft2
      + halfRight3
      * halfLeft1
      + halfRight4
      * halfLeft0
      + (product / MULTIPLER_INTWORD_DECIMAL);
    final int z4 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[5] -- since left integer #5 is always 0, some products here are not included.
    product =
        halfRight1
      * halfLeft4
      + halfRight2
      * halfLeft3
      + halfRight3
      * halfLeft2
      + halfRight4
      * halfLeft1
      + halfRight5
      * halfLeft0
      + (product / MULTIPLER_INTWORD_DECIMAL);
    final int z5 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[6] -- since left integer #5 is always 0, some products here are not included.
    product =
        halfRight2
      * halfLeft4
      + halfRight3
      * halfLeft3
      + halfRight4
      * halfLeft2
      + halfRight5
      * halfLeft1
      + (product / MULTIPLER_INTWORD_DECIMAL);
    final int z6 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[7] -- since left integer #5 is always 0, some products here are not included.
    product =
        halfRight3
      * halfLeft4
      + halfRight4
      * halfLeft3
      + halfRight5
      * halfLeft2
      + (product / MULTIPLER_INTWORD_DECIMAL);
    final int z7 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[8] -- since left integer #5 is always 0, some products here are not included.
    product =
        halfRight4
      * halfLeft4
      + halfRight5
      * halfLeft3
      + (product / MULTIPLER_INTWORD_DECIMAL);
    final int z8 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[9] -- since left integer #5 is always 0, some products here are not included.
    product =
        halfRight5
      * halfLeft4
      + (product / MULTIPLER_INTWORD_DECIMAL);
    final int z9 = (int) (product % MULTIPLER_INTWORD_DECIMAL);

    // v[10] -- since left integer #5 is always 0, some products here are not included.
    product =
      + (product / MULTIPLER_INTWORD_DECIMAL);
    if (product > MULTIPLER_INTWORD_DECIMAL) {
      return false;
    }

    result[0] = (long) z1 * MULTIPLER_INTWORD_DECIMAL + (long) z0;
    result[1] = (long) z3 * MULTIPLER_INTWORD_DECIMAL + (long) z2;
    result[2] = (long) z5 * MULTIPLER_INTWORD_DECIMAL + (long) z4;
    result[3] = (long) z7 * MULTIPLER_INTWORD_DECIMAL + (long) z6;
    result[4] = (long) z9 * MULTIPLER_INTWORD_DECIMAL + (long) z8;
    result[5] = product;

    return true;
  }

  public static boolean fastMultiply(
      FastHiveDecimal fastLeft,
      FastHiveDecimal fastRight,
      FastHiveDecimal fastResult) {
    return fastMultiply(
        fastLeft.fastSignum, fastLeft.fast0, fastLeft.fast1, fastLeft.fast2,
        fastLeft.fastIntegerDigitCount, fastLeft.fastScale,
        fastRight.fastSignum, fastRight.fast0, fastRight.fast1, fastRight.fast2,
        fastRight.fastIntegerDigitCount, fastRight.fastScale,
        fastResult);
  }

  public static boolean fastMultiply(
      int leftSignum, long leftFast0, long leftFast1, long leftFast2,
      int leftIntegerDigitCount, int leftScale,
      int rightSignum, long rightFast0, long rightFast1, long rightFast2,
      int rightIntegerDigitCount, int rightScale,
      FastHiveDecimal fastResult) {

    if (leftSignum == 0 || rightSignum == 0) {
      fastResult.fastReset();
      return true;
    }

    return doMultiply(
        leftSignum, leftFast0, leftFast1, leftFast2,
        leftIntegerDigitCount, leftScale,
        rightSignum, rightFast0, rightFast1, rightFast2,
        rightIntegerDigitCount, rightScale,
        fastResult);
  }

  //************************************************************************************************
  // Decimal Division / Remainder.

  /**
   * EXPERMIMENTAL: Division when divisor fits in a single decimal longword.
   *
   * @return  remainderSubexpr2
   */
  private static long doSingleWordQuotient(
      long leftFast0, long leftFast1, long leftFast2,
      long rightFast0,
      FastHiveDecimal fastResult) {

    long quotient2;
    long quotient1;
    long quotient0;

    long remainderSubexpr2;

    if (leftFast2 == 0 && leftFast1 == 0) {
      quotient2 = 0;
      quotient1 = 0;
      quotient0 =
          leftFast0 / rightFast0;
      final long k0 =
          leftFast0 - quotient0 * rightFast0;
      remainderSubexpr2 =
          k0 * MULTIPLER_LONGWORD_DECIMAL;
    } else if (leftFast2 == 0) {
      // leftFast1 != 0.
      quotient2 = 0;
      quotient1 =
          leftFast1 / rightFast0;
      final long k1 =
          leftFast1 - quotient1 * rightFast0;
      final long quotientSubexpr0 =
          k1 * MULTIPLER_LONGWORD_DECIMAL
        + leftFast0;
      quotient0 =
          quotientSubexpr0 / rightFast0;
      final long k0 =
          quotientSubexpr0 - quotient0 * rightFast0;
      remainderSubexpr2 =
          k0 * MULTIPLER_LONGWORD_DECIMAL;
    } else if (leftFast1 == 0){
      // leftFast2 != 0 && leftFast1 == 0.
      quotient2 =
          leftFast2 / rightFast0;
      quotient1 = 0;
      quotient0 =
          leftFast0 / rightFast0;
      final long k0 =
          leftFast0 - quotient0 * rightFast0;
      remainderSubexpr2 =
          k0 * MULTIPLER_LONGWORD_DECIMAL;
    } else {
      quotient2 =
          leftFast2 / rightFast0;
      final long k2 =
          leftFast2 - quotient2 * rightFast0;
      final long quotientSubexpr1 =
          k2 * MULTIPLER_LONGWORD_DECIMAL
        + leftFast1;
      quotient1 =
          quotientSubexpr1 / rightFast0;
      final long k1 =
          quotientSubexpr1 - quotient1 * rightFast0;
      final long quotientSubexpr0 =
          k1 * MULTIPLER_LONGWORD_DECIMAL;
      quotient0 =
          quotientSubexpr0 / rightFast0;
      final long k0 =
          quotientSubexpr0 - quotient0 * rightFast0;
      remainderSubexpr2 =
          k0 * MULTIPLER_LONGWORD_DECIMAL;
    }

    fastResult.fast0 = quotient0;
    fastResult.fast1 = quotient1;
    fastResult.fast2 = quotient2;

    return remainderSubexpr2;
  }

  private static int doSingleWordRemainder(
      long leftFast0, long leftFast1, long leftFast2,
      long rightFast0,
      long remainderSubexpr2,
      FastHiveDecimal fastResult) {

    int remainderDigitCount;

    long remainder2;
    long remainder1;
    long remainder0;

    if (remainderSubexpr2 == 0) {
      remainder2 = 0;
      remainder1 = 0;
      remainder0 = 0;
      remainderDigitCount = 0;
    } else {
      remainder2 =
          remainderSubexpr2 / rightFast0;
      final long k2 =
          remainderSubexpr2 - remainder2 * rightFast0;
      if (k2 == 0) {
        remainder1 = 0;
        remainder0 = 0;
        remainderDigitCount =
            LONGWORD_DECIMAL_DIGITS - fastLongWordTrailingZeroCount(remainder2);
      } else {
        final long remainderSubexpr1 =
            k2 * MULTIPLER_LONGWORD_DECIMAL;
        long remainderSubexpr0;
        remainder1 =
            remainderSubexpr1 / rightFast0;
        final long k1 =
            remainderSubexpr1 - remainder1 * rightFast0;
        if (k1 == 0) {
          remainder0 = 0;
          remainderDigitCount =
              LONGWORD_DECIMAL_DIGITS
            + LONGWORD_DECIMAL_DIGITS - fastLongWordTrailingZeroCount(remainder1);
        } else {
          remainderSubexpr0 =
              k2 * MULTIPLER_LONGWORD_DECIMAL;

          remainder0 =
              remainderSubexpr0 / rightFast0;
          remainderDigitCount =
              TWO_X_LONGWORD_DECIMAL_DIGITS
            + LONGWORD_DECIMAL_DIGITS - fastLongWordTrailingZeroCount(remainder0);
        }
      }
    }

    fastResult.fast0 = remainder0;
    fastResult.fast1 = remainder1;
    fastResult.fast2 = remainder2;

    return remainderDigitCount;
  }

  // EXPERIMENT
  private static boolean fastSingleWordDivision(
      int leftSignum, long leftFast0, long leftFast1, long leftFast2, int leftScale,
      int rightSignum, long rightFast0, int rightScale,
      FastHiveDecimal fastResult) {

    long remainderSubexpr2 =
        doSingleWordQuotient(
            leftFast0, leftFast1, leftFast2,
            rightFast0,
            fastResult);

    long quotient0 = fastResult.fast0;
    long quotient1 = fastResult.fast1;
    long quotient2 = fastResult.fast2;

    int quotientDigitCount;
    if (quotient2 != 0) {
      quotientDigitCount = fastLongWordPrecision(quotient2);
    } else if (quotient1 != 0) {
      quotientDigitCount = fastLongWordPrecision(quotient1);
    } else {
      quotientDigitCount = fastLongWordPrecision(quotient0);
    }

    int remainderDigitCount =
        doSingleWordRemainder(
            leftFast0, leftFast1, leftFast2,
            rightFast0,
            remainderSubexpr2,
            fastResult);

    long remainder0 = fastResult.fast0;
    long remainder1 = fastResult.fast1;
    long remainder2 = fastResult.fast2;

    fastResult.fast0 = quotient0;
    fastResult.fast1 = quotient1;
    fastResult.fast2 = quotient2;

    final int quotientScale = leftScale + rightScale;

    if (remainderDigitCount == 0) {
      fastResult.fastScale = quotientScale;
    } else {
      int resultScale = quotientScale + remainderDigitCount;

      int adjustedQuotientDigitCount;
      if (quotientScale > 0) {
        adjustedQuotientDigitCount = Math.max(0, quotientDigitCount - quotientScale);
      } else {
        adjustedQuotientDigitCount = quotientDigitCount;
      }
      final int maxScale = HiveDecimal.MAX_SCALE - adjustedQuotientDigitCount;

      int scale = Math.min(resultScale, maxScale);

      int remainderScale;
      remainderScale = Math.min(remainderDigitCount, maxScale - quotientScale);
      if (remainderScale > 0) {
        if (quotientDigitCount > 0) {
          // Make room for remainder.
          fastScaleUp(
              fastResult,
              remainderScale,
              fastResult);
        }
        // Copy in remainder digits... which start at the top of remainder2.
        if (remainderScale < LONGWORD_DECIMAL_DIGITS) {
          final long remainderDivisor2 = powerOfTenTable[LONGWORD_DECIMAL_DIGITS - remainderScale];
          fastResult.fast0 += (remainder2 / remainderDivisor2);
        } else if (remainderScale == LONGWORD_DECIMAL_DIGITS) {
          fastResult.fast0 = remainder2;
        } else if (remainderScale < TWO_X_LONGWORD_DECIMAL_DIGITS) {
          final long remainderDivisor2 = powerOfTenTable[remainderScale - LONGWORD_DECIMAL_DIGITS];
          fastResult.fast1 += (remainder2 / remainderDivisor2);
          fastResult.fast0 = remainder1;
        } else if (remainderScale == TWO_X_LONGWORD_DECIMAL_DIGITS) {
          fastResult.fast1 = remainder2;
          fastResult.fast0 = remainder1;
        }
      }

      // UNDONE: Method is still under development.
      fastResult.fastScale = scale;

      // UNDONE: Trim trailing zeroes...
    }

    return true;
  }

  public static boolean fastDivide(
      FastHiveDecimal fastLeft,
      FastHiveDecimal fastRight,
      FastHiveDecimal fastResult) {
    return fastDivide(
        fastLeft.fastSignum, fastLeft.fast0, fastLeft.fast1, fastLeft.fast2,
        fastLeft.fastIntegerDigitCount, fastLeft.fastScale,
        fastRight.fastSignum, fastRight.fast0, fastRight.fast1, fastRight.fast2,
        fastRight.fastIntegerDigitCount, fastRight.fastScale,
        fastResult);
  }

  public static boolean fastDivide(
      int leftSignum, long leftFast0, long leftFast1, long leftFast2,
      int leftIntegerDigitCount, int leftScale,
      int rightSignum, long rightFast0, long rightFast1, long rightFast2,
      int rightIntegerDigitCount, int rightScale,
      FastHiveDecimal fastResult) {

    // Arithmetic operations reset the results.
    fastResult.fastReset();

    if (rightSignum == 0) {
      // Division by 0.
      return false;
    }
    if (leftSignum == 0) {
      // Zero result.
      return true;
    }

    /*
    if (rightFast1 == 0 && rightFast2 == 0) {
      return fastSingleWordDivision(
          leftSignum, leftFast0, leftFast1, leftFast2, leftScale,
          rightSignum, rightFast0, rightScale,
          fastResult);
    }
    */

    BigDecimal denominator =
        fastBigDecimalValue(
            leftSignum, leftFast0, leftFast1, leftFast2, leftIntegerDigitCount, leftScale);
    BigDecimal divisor =
        fastBigDecimalValue(
            rightSignum, rightFast0, rightFast1, rightFast2, rightIntegerDigitCount, rightScale);
    BigDecimal quotient =
        denominator.divide(divisor, HiveDecimal.MAX_SCALE, BigDecimal.ROUND_HALF_UP);

    if (!fastSetFromBigDecimal(
        quotient,
        true,
        fastResult)) {
      return false;
    }

    return true;
  }

  public static boolean fastRemainder(
      int leftSignum, long leftFast0, long leftFast1, long leftFast2,
      int leftIntegerDigitCount, int leftScale,
      int rightSignum, long rightFast0, long rightFast1, long rightFast2,
      int rightIntegerDigitCount, int rightScale,
      FastHiveDecimal fastResult) {

    // Arithmetic operations reset the results.
    fastResult.fastReset();

    if (rightSignum == 0) {
     // Division by 0.
      return false;
    }
    if (leftSignum == 0) {
      // Zero result.
      return true;
    }

    BigDecimal denominator =
        fastBigDecimalValue(
            leftSignum, leftFast0, leftFast1, leftFast2, leftIntegerDigitCount, leftScale);
    BigDecimal divisor =
        fastBigDecimalValue(
            rightSignum, rightFast0, rightFast1, rightFast2, rightIntegerDigitCount, rightScale);
    BigDecimal remainder =
        denominator.remainder(divisor);
    fastResult.fastReset();
    if (!fastSetFromBigDecimal(
        remainder,
        true,
        fastResult)) {
      return false;
    }

    return true;
  }

  public static boolean fastPow(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale,
      int exponent,
      FastHiveDecimal fastResult) {

    // Arithmetic operations (re)set the results.
    fastResult.fastSet(fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale);

    if (exponent < 0) {
      // UNDONE: Currently, negative exponent is not supported.
      return false;
    }

    for (int e = 1; e < exponent; e++) {
      if (!doMultiply(
          fastResult.fastSignum, fastResult.fast0, fastResult.fast1, fastResult.fast2,
          fastResult.fastIntegerDigitCount, fastResult.fastScale,
          fastResult.fastSignum, fastResult.fast0, fastResult.fast1, fastResult.fast2,
          fastResult.fastIntegerDigitCount, fastResult.fastScale,
          fastResult)) {
        return false;
      }
    }
    return true;
  }

  //************************************************************************************************
  // Decimal String Formatting.

  public static String fastToFormatString(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale,
      int formatScale) {
    byte[] scratchBuffer = new byte[FAST_SCRATCH_BUFFER_LEN_TO_BYTES];
    final int index =
        doFastToFormatBytes(
          fastSignum, fast0, fast1, fast2,
          fastIntegerDigitCount, fastScale,
          formatScale,
          scratchBuffer);
    return
        new String(scratchBuffer, index, FAST_SCRATCH_BUFFER_LEN_TO_BYTES - index, StandardCharsets.UTF_8);
  }

  //************************************************************************************************
  // Decimal String Formatting.

  public static String fastToFormatString(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale,
      int formatScale,
      byte[] scratchBuffer) {
    final int index =
        doFastToBytes(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale, formatScale,
            scratchBuffer);
    return new String(scratchBuffer, index, scratchBuffer.length - index, StandardCharsets.UTF_8);
  }

  public static int fastToFormatBytes(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale,
      int formatScale,
      byte[] scratchBuffer) {
   return
       doFastToFormatBytes(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale,
            formatScale,
            scratchBuffer);
  }

  public static int doFastToFormatBytes(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale,
      int formatScale,
      byte[] scratchBuffer) {

    // NOTE: OldHiveDecimal.toFormatString returns decimal strings with more than > 38 digits!

    if (formatScale >= fastScale) {
      return
          doFastToBytes(
              fastSignum, fast0, fast1, fast2,
              fastIntegerDigitCount, fastScale, formatScale,
              scratchBuffer);
    } else {
      FastHiveDecimal fastTemp = new FastHiveDecimal();
      if (!fastRound(
          fastSignum, fast0, fast1, fast2,
          fastIntegerDigitCount, fastScale,
          formatScale, BigDecimal.ROUND_HALF_UP,
          fastTemp)) {
        return 0;
      }
      return
          doFastToBytes(
              fastTemp.fastSignum, fastTemp.fast0, fastTemp.fast1, fastTemp.fast2,
              fastTemp.fastIntegerDigitCount, fastTemp.fastScale, formatScale,
              scratchBuffer);
    }
  }

  public static String fastToString(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale, int fastTrailingZeroesScale) {
    return doFastToString(
        fastSignum, fast0, fast1, fast2,
        fastIntegerDigitCount, fastScale, fastTrailingZeroesScale);
  }

  public static String fastToString(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale, int fastTrailingZeroesScale,
      byte[] scratchBuffer) {
    return doFastToString(
        fastSignum, fast0, fast1, fast2,
        fastIntegerDigitCount, fastScale, fastTrailingZeroesScale,
        scratchBuffer);
  }

  public static String fastToDigitsOnlyString(
      long fast0, long fast1, long fast2,
      int fastIntegerDigitCount) {
    byte[] scratchBuffer = new byte[FAST_SCRATCH_BUFFER_LEN_TO_BYTES];
    final int index =
        doFastToDigitsOnlyBytes(
            fast0, fast1, fast2,
            fastIntegerDigitCount,
            scratchBuffer);
    return
        new String(scratchBuffer, index, FAST_SCRATCH_BUFFER_LEN_TO_BYTES - index, StandardCharsets.UTF_8);
  }

  public static int fastToBytes(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale, int fastTrailingZeroesScale,
      byte[] scratchBuffer) {
    return doFastToBytes(
        fastSignum, fast0, fast1, fast2,
        fastIntegerDigitCount, fastScale, fastTrailingZeroesScale,
        scratchBuffer);
  }

  private static String doFastToString(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale, int fastTrailingZeroesScale) {
    byte[] scratchBuffer = new byte[FAST_SCRATCH_BUFFER_LEN_TO_BYTES];
    final int index =
        doFastToBytes(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale, fastTrailingZeroesScale,
            scratchBuffer);
    return
        new String(
            scratchBuffer, index, FAST_SCRATCH_BUFFER_LEN_TO_BYTES - index, StandardCharsets.UTF_8);
  }

  private static String doFastToString(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale, int fastTrailingZeroesScale,
      byte[] scratchBuffer) {
    final int index =
        doFastToBytes(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale, fastTrailingZeroesScale,
            scratchBuffer);
    return new String(scratchBuffer, index, scratchBuffer.length - index, StandardCharsets.UTF_8);
  }

  private static int doFastToBytes(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale, int fastTrailingZeroesScale,
      byte[] scratchBuffer) {

    int index = scratchBuffer.length - 1;

    int trailingZeroCount =
        (fastTrailingZeroesScale != -1 ? fastTrailingZeroesScale - fastScale : 0);
    // Virtual trailing zeroes.
    if (trailingZeroCount > 0) {
      for (int i = 0; i < trailingZeroCount; i++) {
        scratchBuffer[index--] = BYTE_DIGIT_ZERO;
      }
    }

    // Scale fractional digits, dot, integer digits.

    final int scale = fastScale;

    final boolean isZeroFast1AndFast2 = (fast1 == 0 && fast2 == 0);
    final boolean isZeroFast2 = (fast2 == 0);

    int lowerLongwordScale = 0;
    int middleLongwordScale = 0;
    int highLongwordScale = 0;
    long longWord = fast0;
    if (scale > 0) {

      // Fraction digits from lower longword.

      lowerLongwordScale = Math.min(scale, LONGWORD_DECIMAL_DIGITS);

      for (int i = 0; i < lowerLongwordScale; i++) {
        scratchBuffer[index--] = (byte) (BYTE_DIGIT_ZERO + longWord % 10);
        longWord /= 10;
      }
      if (lowerLongwordScale == LONGWORD_DECIMAL_DIGITS) {
        longWord = fast1;
      }

      if (scale > LONGWORD_DECIMAL_DIGITS) {

        // Fraction digits continue into middle longword.

        middleLongwordScale = Math.min(scale - LONGWORD_DECIMAL_DIGITS, LONGWORD_DECIMAL_DIGITS);
        for (int i = 0; i < middleLongwordScale; i++) {
          scratchBuffer[index--] = (byte) (BYTE_DIGIT_ZERO + longWord % 10);
          longWord /= 10;
        }
        if (middleLongwordScale == LONGWORD_DECIMAL_DIGITS) {
          longWord = fast2;
        }

        if (scale > TWO_X_LONGWORD_DECIMAL_DIGITS) {

          // Fraction digit continue into highest longword.

          highLongwordScale = scale - TWO_X_LONGWORD_DECIMAL_DIGITS;
          for (int i = 0; i < highLongwordScale; i++) {
            scratchBuffer[index--] = (byte) (BYTE_DIGIT_ZERO + longWord % 10);
            longWord /= 10;
          }
        }
      }
      scratchBuffer[index--] = BYTE_DOT;
    } else if (trailingZeroCount > 0) {
      scratchBuffer[index--] = BYTE_DOT;
    }

    // Integer digits; stop on zeroes above.

    boolean atLeastOneIntegerDigit = false;
    if (scale <= LONGWORD_DECIMAL_DIGITS) {

      // Handle remaining lower long word digits as integer digits.

      final int remainingLowerLongwordDigits = LONGWORD_DECIMAL_DIGITS - lowerLongwordScale;
      for (int i = 0; i < remainingLowerLongwordDigits; i++) {
        scratchBuffer[index--] = (byte) (BYTE_DIGIT_ZERO + longWord % 10);
        atLeastOneIntegerDigit = true;
        longWord /= 10;
        if (longWord == 0 && isZeroFast1AndFast2) {
          // Suppress leading zeroes.
          break;
        }
      }
      if (isZeroFast1AndFast2) {
        if (!atLeastOneIntegerDigit) {
          scratchBuffer[index--] = BYTE_DIGIT_ZERO;
        }
        if (fastSignum == -1) {
          scratchBuffer[index--] = BYTE_MINUS;
        }
        return index + 1;
      }
      longWord = fast1;
    }

    if (scale <= TWO_X_LONGWORD_DECIMAL_DIGITS) {

      // Handle remaining middle long word digits.

      final int remainingMiddleLongwordDigits = LONGWORD_DECIMAL_DIGITS - middleLongwordScale;

      for (int i = 0; i < remainingMiddleLongwordDigits; i++) {
        scratchBuffer[index--] = (byte) (BYTE_DIGIT_ZERO + longWord % 10);
        atLeastOneIntegerDigit = true;
        longWord /= 10;
        if (longWord == 0 && isZeroFast2) {
          // Suppress leading zeroes.
          break;
        }
      }
      if (isZeroFast2) {
        if (!atLeastOneIntegerDigit) {
          scratchBuffer[index--] = BYTE_DIGIT_ZERO;
        }
        if (fastSignum == -1) {
          scratchBuffer[index--] = BYTE_MINUS;
        }
        return index + 1;
      }
      longWord = fast2;
    }

    final int remainingHighwordDigits = HIGHWORD_DECIMAL_DIGITS - highLongwordScale;

    for (int i = 0; i < remainingHighwordDigits; i++) {
      scratchBuffer[index--] = (byte) (BYTE_DIGIT_ZERO + longWord % 10);
      atLeastOneIntegerDigit = true;
      longWord /= 10;
      if (longWord == 0) {
        // Suppress leading zeroes.
        break;
      }
    }
    if (!atLeastOneIntegerDigit) {
      scratchBuffer[index--] = BYTE_DIGIT_ZERO;
    }
    if (fastSignum == -1) {
      scratchBuffer[index--] = BYTE_MINUS;
    }
    return index + 1;
  }

  public static int fastToDigitsOnlyBytes(
      long fast0, long fast1, long fast2,
      int fastIntegerDigitCount,
      byte[] scratchBuffer) {
    return doFastToDigitsOnlyBytes(
        fast0, fast1, fast2,
        fastIntegerDigitCount,
        scratchBuffer);
  }

  private static int doFastToDigitsOnlyBytes(
      long fast0, long fast1, long fast2,
      int fastIntegerDigitCount,
      byte[] scratchBuffer) {

    int index = scratchBuffer.length - 1;

    // Just digits.

    final boolean isZeroFast1AndFast2 = (fast1 == 0 && fast2 == 0);
    final boolean isZeroFast2 = (fast2 == 0);

    boolean atLeastOneIntegerDigit = false;
    long longWord = fast0;
    for (int i = 0; i < LONGWORD_DECIMAL_DIGITS; i++) {
      scratchBuffer[index--] = (byte) (BYTE_DIGIT_ZERO + longWord % 10);
      atLeastOneIntegerDigit = true;
      longWord /= 10;
      if (longWord == 0 && isZeroFast1AndFast2) {
        // Suppress leading zeroes.
        break;
      }
    }
    if (isZeroFast1AndFast2) {
      if (!atLeastOneIntegerDigit) {
        scratchBuffer[index--] = BYTE_DIGIT_ZERO;
      }
      return index + 1;
    }

    longWord = fast1;

    for (int i = 0; i < LONGWORD_DECIMAL_DIGITS; i++) {
      scratchBuffer[index--] = (byte) (BYTE_DIGIT_ZERO + longWord % 10);
      atLeastOneIntegerDigit = true;
      longWord /= 10;
      if (longWord == 0 && isZeroFast2) {
        // Suppress leading zeroes.
        break;
      }
    }
    if (isZeroFast2) {
      if (!atLeastOneIntegerDigit) {
        scratchBuffer[index--] = BYTE_DIGIT_ZERO;
      }
      return index + 1;
    }

    longWord = fast2;

    for (int i = 0; i < HIGHWORD_DECIMAL_DIGITS; i++) {
      scratchBuffer[index--] = (byte) (BYTE_DIGIT_ZERO + longWord % 10);
      atLeastOneIntegerDigit = true;
      longWord /= 10;
      if (longWord == 0) {
        // Suppress leading zeroes.
        break;
      }
    }
    if (!atLeastOneIntegerDigit) {
      scratchBuffer[index--] = BYTE_DIGIT_ZERO;
    }
    return index + 1;
  }

  //************************************************************************************************
  // Decimal Validation.

  public static boolean fastIsValid(FastHiveDecimal fastDec) {
    return fastIsValid(
        fastDec.fastSignum, fastDec.fast0, fastDec.fast1, fastDec.fast2,
        fastDec.fastIntegerDigitCount, fastDec.fastScale);
  }

  public static boolean fastIsValid(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale) {
    boolean isValid;
    if (fastSignum == 0) {
      isValid = (fast0 == 0 && fast1 == 0 && fast2 == 0 && fastIntegerDigitCount == 0 && fastScale == 0);
      if (!isValid) {
        System.out.println("FAST_IS_VALID signum 0 but other fields not");
      }
    } else {
      isValid = (
          (fast0 >= 0 && fast0 <= MAX_LONGWORD_DECIMAL) &&
          (fast1 >= 0 && fast1 <= MAX_LONGWORD_DECIMAL) &&
          (fast2 >= 0 && fast2 <= MAX_HIGHWORD_DECIMAL));
      if (!isValid) {
        System.out.println("FAST_IS_VALID fast0 .. fast2 out of range");
      } else {
        if (fastScale < 0 || fastScale > HiveDecimal.MAX_SCALE) {
          System.out.println("FAST_IS_VALID fastScale " + fastScale + " out of range");
          isValid = false;
        } else if (fastIntegerDigitCount < 0 || fastIntegerDigitCount > HiveDecimal.MAX_PRECISION) {
          System.out.println("FAST_IS_VALID fastIntegerDigitCount " + fastIntegerDigitCount + " out of range");
          isValid = false;
        } else if (fastIntegerDigitCount + fastScale > HiveDecimal.MAX_PRECISION) {
          System.out.println("FAST_IS_VALID exceeds max precision: fastIntegerDigitCount " + fastIntegerDigitCount + " and fastScale " + fastScale);
          isValid = false;
        } else {
          // Verify integerDigitCount given fastScale.
          final int rawPrecision = fastRawPrecision(fastSignum, fast0, fast1, fast2);
          if (fastIntegerDigitCount > 0) {
            if (rawPrecision != fastIntegerDigitCount + fastScale) {
              System.out.println("FAST_IS_VALID integer case: rawPrecision " + rawPrecision +
                  " fastIntegerDigitCount " + fastIntegerDigitCount +
                  " fastScale " + fastScale);
              isValid = false;
            }
          } else {
            if (rawPrecision > fastScale) {
              System.out.println("FAST_IS_VALID fraction only case: rawPrecision " + rawPrecision +
                  " fastIntegerDigitCount " + fastIntegerDigitCount +
                  " fastScale " + fastScale);
              isValid = false;
            }
          }
          if (isValid) {
            final int trailingZeroCount =
                fastTrailingDecimalZeroCount(
                    fast0, fast1, fast2,
                    fastIntegerDigitCount, fastScale);
            if (trailingZeroCount != 0) {
              System.out.println("FAST_IS_VALID exceeds max precision: trailingZeroCount != 0");
              isValid = false;
            }
          }
        }
      }
    }

    if (!isValid) {
      System.out.println("FAST_IS_VALID fast0 " + fast0);
      System.out.println("FAST_IS_VALID fast1 " + fast1);
      System.out.println("FAST_IS_VALID fast2 " + fast2);
      System.out.println("FAST_IS_VALID fastIntegerDigitCount " + fastIntegerDigitCount);
      System.out.println("FAST_IS_VALID fastScale " + fastScale);
    }
    return isValid;
  }

  public static void fastRaiseInvalidException(
      FastHiveDecimal fastResult) {
    throw new RuntimeException(
        "Invalid fast decimal " +
        " fastSignum " + fastResult.fastSignum + " fast0 " + fastResult.fast0 + " fast1 " + fastResult.fast1 + " fast2 " + fastResult.fast2 +
            " fastIntegerDigitCount " + fastResult.fastIntegerDigitCount + " fastScale " + fastResult.fastScale +
        " stack trace: " + getStackTraceAsSingleLine(Thread.currentThread().getStackTrace()));
  }

  public static void fastRaiseInvalidException(
      FastHiveDecimal fastResult,
      String parameters) {
    throw new RuntimeException(
        "Parameters: " + parameters + " --> " +
        "Invalid fast decimal " +
        " fastSignum " + fastResult.fastSignum + " fast0 " + fastResult.fast0 + " fast1 " + fastResult.fast1 + " fast2 " + fastResult.fast2 +
            " fastIntegerDigitCount " + fastResult.fastIntegerDigitCount + " fastScale " + fastResult.fastScale +
        " stack trace: " + getStackTraceAsSingleLine(Thread.currentThread().getStackTrace()));
  }

  /**
   * Determines if the specified character can be treated as valid while handling decimals
   *
   * @param b the character to be tested.
   * @return returns true if the character is one of the characters listed below
   * List of characters that are supported in regular RDBMS databases(MySQL, PostgreSQL) while handling decimals
   * are considered valid in Hive as well. The list include
   * '\u0009' - HORIZONTAL TABULATION (\t)
   * '\u000B' - VERTICAL TABULATION
   * '\u000C' - FORM FEED
   * '\u0020' - SPACE SEPARATOR
   */
  public static boolean isValidSpecialCharacter(byte b) {
    return Arrays.stream(SpecialCharacters.values()).anyMatch(splCharacters -> splCharacters.getValue() == b);
  }

  private enum SpecialCharacters {

    HORIZONTAL_TABULATION('\u0009'),
    VERTICAL_TABULATION('\u000B'),
    FORM_FEED('\u000C'),
    SPACE_SEPARATOR('\u0020');

    private char character;

    SpecialCharacters(char c) {
      this.character = c;
    }

    public byte getValue() {
      return (byte) this.character;
    }
  }

  //************************************************************************************************
  // Decimal Debugging.

  static final int STACK_LENGTH_LIMIT = 20;
  public static String getStackTraceAsSingleLine(StackTraceElement[] stackTrace) {
    StringBuilder sb = new StringBuilder();
    sb.append("Stack trace: ");
    int length = stackTrace.length;
    boolean isTruncated = false;
    if (length > STACK_LENGTH_LIMIT) {
      length = STACK_LENGTH_LIMIT;
      isTruncated = true;
    }
    for (int i = 0; i < length; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(stackTrace[i]);
    }
    if (isTruncated) {
      sb.append(", ...");
    }

    return sb.toString();
  }

  public static String displayBytes(byte[] bytes, int start, int length) {
    StringBuilder sb = new StringBuilder();
    for (int i = start; i < start + length; i++) {
      sb.append(String.format("\\%03d", (int) (bytes[i] & 0xff)));
    }
    return sb.toString();
  }
}