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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import org.apache.hive.common.util.Decimal128FastBuffer;

/**
 * This code was based on code from Microsoft's PolyBase.
 * <p>
 * A 128-bit fixed-length Decimal value in the ANSI SQL Numeric semantics,
 * representing unscaledValue / 10**scale where scale is 0 or positive.
 * </p>
 * <p>
 * This class is similar to {@link java.math.BigDecimal}, but a few things
 * differ to conform to the SQL Numeric semantics.
 * </p>
 * <p>
 * Scale of this object is specified by the user, not automatically determined
 * like {@link java.math.BigDecimal}. This means that underflow is possible
 * depending on the scale. {@link java.math.BigDecimal} controls rounding
 * behaviors by MathContext, possibly throwing errors. But, underflow is NOT an
 * error in ANSI SQL Numeric. "CAST(0.000000000....0001 AS DECIMAL(38,1))" is
 * "0.0" without an error.
 * </p>
 * <p>
 * Because this object is fixed-length, overflow is also possible. Overflow IS
 * an error in ANSI SQL Numeric. "CAST(10000 AS DECIMAL(38,38))" throws overflow
 * error.
 * </p>
 * <p>
 * Each arithmetic operator takes scale as a parameter to control its behavior.
 * It's user's (or query optimizer's) responsibility to give an appropriate
 * scale parameter.
 * </p>
 * <p>
 * Finally, this class performs MUCH faster than java.math.BigDecimal for a few
 * reasons. Its behavior is simple because of the designs above. This class is
 * fixed-length without array expansion and re-allocation. This class is
 * mutable, allowing reuse of the same object without re-allocation. This class
 * and {@link UnsignedInt128} are designed such that minimal heap-object
 * allocations are required for most operations. The only exception is division.
 * Even this class requires a few object allocations for division, though much
 * fewer than BigDecimal.
 * </p>
 */
public final class Decimal128 extends Number implements Comparable<Decimal128> {
  /** Maximum value for #scale. */
  public static final short MAX_SCALE = 38;

  /** Minimum value for #scale. */
  public static final short MIN_SCALE = 0;

  public static final Decimal128 ONE = new Decimal128().update(1);

  /** Maximum value that can be represented in this class. */
  public static final Decimal128 MAX_VALUE = new Decimal128(
      UnsignedInt128.TEN_TO_THIRTYEIGHT, (short) 0, false).subtractDestructive(
          Decimal128.ONE, (short) 0);

  /** Minimum value that can be represented in this class. */
  public static final Decimal128 MIN_VALUE = new Decimal128(
      UnsignedInt128.TEN_TO_THIRTYEIGHT, (short) 0, true).addDestructive(
          Decimal128.ONE, (short) 0);

  /** For Serializable. */
  private static final long serialVersionUID = 1L;

  /**
   * The unscaled value of this Decimal128, as returned by
   * {@link #getUnscaledValue()}.
   *
   * @serial
   * @see #getUnscaledValue()
   */
  private UnsignedInt128 unscaledValue;

  /**
   * The scale of this Decimal128, as returned by {@link #getScale()}. Unlike
   * java.math.BigDecimal, the scale is always zero or positive. The possible
   * value range is 0 to 38.
   *
   * @serial
   * @see #getScale()
   */
  private short scale;

  /**
   * -1 means negative, 0 means zero, 1 means positive.
   *
   * @serial
   * @see #getSignum()
   */
  private byte signum;

  /**
   * Determines the number of ints to store one value.
   *
   * @param precision
   *          precision (0-38)
   * @return the number of ints to store one value
   */
  public static int getIntsPerElement(int precision) {
    return UnsignedInt128.getIntsPerElement(precision) + 1; // +1 for
                                                            // scale/signum
  }

  /** Construct a zero. */
  public Decimal128() {
    this.unscaledValue = new UnsignedInt128();
    this.scale = 0;
    this.signum = 0;
  }

  /**
   * Copy constructor.
   *
   * @param o
   *          object to copy from
   */
  public Decimal128(Decimal128 o) {
    this.unscaledValue = new UnsignedInt128(o.unscaledValue);
    this.scale = o.scale;
    this.signum = o.signum;
  }

  /**
   * Translates a {@code double} into a {@code Decimal128} in the given scaling.
   * Note that, unlike java.math.BigDecimal, the scaling is given as the
   * parameter, not automatically detected. This is one of the differences
   * between ANSI SQL Numeric and Java's BigDecimal. See class comments for more
   * details.
   * <p>
   * Unchecked exceptions: ArithmeticException if {@code val} overflows in the
   * scaling. NumberFormatException if {@code val} is infinite or NaN.
   * </p>
   *
   * @param val
   *          {@code double} value to be converted to {@code Decimal128}.
   * @param scale
   *          scale of the {@code Decimal128}.
   */
  public Decimal128(double val, short scale) {
    this();
    update(val, scale);
  }

  /**
   * Translates a {@code UnsignedInt128} unscaled value, an {@code int} scale,
   * and sign flag into a {@code Decimal128} . The value of the
   * {@code Decimal128} is <tt>(unscaledVal &times; 10<sup>-scale</sup>)</tt>.
   *
   * @param unscaledVal
   *          unscaled value of the {@code Decimal128}.
   * @param scale
   *          scale of the {@code Decimal128}.
   * @param negative
   *          whether the value is negative
   */
  public Decimal128(UnsignedInt128 unscaledVal, short scale, boolean negative) {
    checkScaleRange(scale);
    this.unscaledValue = new UnsignedInt128(unscaledVal);
    this.scale = scale;
    if (unscaledValue.isZero()) {
      this.signum = 0;
    } else {
      this.signum = negative ? (byte) -1 : (byte) 1;
    }
    this.unscaledValue.throwIfExceedsTenToThirtyEight();
  }

  /**
   * Translates a {@code long} into a {@code Decimal128}. The scale of the
   * {@code Decimal128} is zero.
   *
   * @param val
   *          {@code long} value to be converted to {@code Decimal128}.
   */
  public Decimal128(long val) {
    this(val, (short) 0);
  }

  /**
   * Translates a {@code long} into a {@code Decimal128} with the given scaling.
   *
   * @param val
   *          {@code long} value to be converted to {@code Decimal128}.
   * @param scale
   *          scale of the {@code Decimal128}.
   */
  public Decimal128(long val, short scale) {
    this();
    update(val, scale);
  }

  /**
   * Constructs from the given string.
   *
   * @param str
   *          string
   * @param scale
   *          scale of the {@code Decimal128}.
   */
  public Decimal128(String str, short scale) {
    this();
    update(str, scale);
  }

  /**
   * Constructs from the given string with given offset and length.
   *
   * @param str
   *          string
   * @param offset
   *          offset
   * @param length
   *          length
   * @param scale
   *          scale of the {@code Decimal128}.
   */
  public Decimal128(char[] str, int offset, int length, short scale) {
    this();
    update(str, offset, length, scale);
  }

  /** Reset the value of this object to zero. */
  public Decimal128 zeroClear() {
    this.unscaledValue.zeroClear();
    this.signum = 0;
    return this;
  }

  /** @return whether this value represents zero. */
  public boolean isZero() {
    assert ((this.signum == 0 && this.unscaledValue.isZero()) || (this.signum != 0 && !this.unscaledValue
        .isZero()));
    return this.signum == 0;
  }

  /**
   * Copy the value of given object.
   *
   * @param o
   *          object to copy from
   */
  public Decimal128 update(Decimal128 o) {
    this.unscaledValue.update(o.unscaledValue);
    this.scale = o.scale;
    this.signum = o.signum;
    return this;
  }

  /**
   * Copy the value of given object and assigns a custom scale.
   *
   * @param o
   *          object to copy from
   */
  public Decimal128 update(Decimal128 o, short scale) {
    update(o);
    this.changeScaleDestructive(scale);
    return this;
  }

  /**
   * Update the value of this object with the given {@code long}. The scale of
   * the {@code Decimal128} is zero.
   *
   * @param val
   *          {@code long} value to be set to {@code Decimal128}.
   */
  public Decimal128 update(long val) {
    return update(val, (short) 0);
  }

  /**
   * Update the value of this object with the given {@code long} with the given
   * scal.
   *
   * @param val
   *          {@code long} value to be set to {@code Decimal128}.
   * @param scale
   *          scale of the {@code Decimal128}.
   */
  public Decimal128 update(long val, short scale) {
    this.scale = 0;
    if (val < 0L) {
      this.unscaledValue.update(-val);
      this.signum = -1;
    } else if (val == 0L) {
      zeroClear();
    } else {
      this.unscaledValue.update(val);
      this.signum = 1;
    }

    if (scale != 0) {
      changeScaleDestructive(scale);
    }
    return this;
  }

  /**
   * Update the value of this object with the given {@code double}. in the given
   * scaling. Note that, unlike java.math.BigDecimal, the scaling is given as
   * the parameter, not automatically detected. This is one of the differences
   * between ANSI SQL Numeric and Java's BigDecimal. See class comments for more
   * details.
   * <p>
   * Unchecked exceptions: ArithmeticException if {@code val} overflows in the
   * scaling. NumberFormatException if {@code val} is infinite or NaN.
   * </p>
   *
   * @param val
   *          {@code double} value to be converted to {@code Decimal128}.
   * @param scale
   *          scale of the {@code Decimal128}.
   */
  public Decimal128 update(double val, short scale) {
    if (Double.isInfinite(val) || Double.isNaN(val)) {
      throw new NumberFormatException("Infinite or NaN");
    }

    checkScaleRange(scale);
    this.scale = scale;

    // Translate the double into sign, exponent and significand, according
    // to the formulae in JLS, Section 20.10.22.
    long valBits = Double.doubleToLongBits(val);
    byte sign = ((valBits >> 63) == 0 ? (byte) 1 : (byte) -1);
    short exponent = (short) ((valBits >> 52) & 0x7ffL);
    long significand = (exponent == 0 ? (valBits & ((1L << 52) - 1)) << 1
        : (valBits & ((1L << 52) - 1)) | (1L << 52));
    exponent -= 1075;

    // zero check
    if (significand == 0) {
      zeroClear();
      return this;
    }

    this.signum = sign;

    // Normalize
    while ((significand & 1) == 0) { // i.e., significand is even
      significand >>= 1;
      exponent++;
    }

    // so far same as java.math.BigDecimal, but the scaling below is
    // specific to ANSI SQL Numeric.

    // first, underflow is NOT an error in ANSI SQL Numeric.
    // CAST(0.000000000....0001 AS DECIMAL(38,1)) is "0.0" without an error.

    // second, overflow IS an error in ANSI SQL Numeric.
    // CAST(10000 AS DECIMAL(38,38)) throws overflow error.

    // val == sign * significand * 2**exponent.
    // this == sign * unscaledValue / 10**scale.
    // so, to make val==this, we need to scale it up/down such that:
    // unscaledValue = significand * 2**exponent * 10**scale
    // Notice that we must do the scaling carefully to check overflow and
    // preserve precision.
    this.unscaledValue.update(significand);
    if (exponent >= 0) {

      // both parts are scaling up. easy. Just check overflow.
      this.unscaledValue.shiftLeftDestructiveCheckOverflow(exponent);
      this.unscaledValue.scaleUpTenDestructive(scale);
    } else {

      // 2**exponent part is scaling down while 10**scale is scaling up.
      // Now it's tricky.
      // unscaledValue = significand * 10**scale / 2**twoScaleDown
      short twoScaleDown = (short) -exponent;
      if (scale >= twoScaleDown) {

        // make both scaling up as follows
        // unscaledValue = significand * 5**(scale) *
        // 2**(scale-twoScaleDown)
        this.unscaledValue.shiftLeftDestructiveCheckOverflow(scale
            - twoScaleDown);
        this.unscaledValue.scaleUpFiveDestructive(scale);
      } else {

        // Gosh, really both scaling up and down.
        // unscaledValue = significand * 5**(scale) /
        // 2**(twoScaleDown-scale)
        // To check overflow while preserving precision, we need to do a
        // real multiplication
        this.unscaledValue.multiplyShiftDestructive(
            SqlMathUtil.POWER_FIVES_INT128[scale],
            (short) (twoScaleDown - scale));
      }
    }
    return this;
  }

  /**
   * Updates the value of this object by reading from ByteBuffer, using the
   * required number of ints for the given precision.
   *
   * @param buf
   *          ByteBuffer to read values from
   * @param precision
   *          0 to 38. Decimal digits.
   */
  public Decimal128 update(IntBuffer buf, int precision) {
    int scaleAndSignum = buf.get();
    this.scale = (short) (scaleAndSignum >> 16);
    this.signum = (byte) (scaleAndSignum & 0xFF);
    this.unscaledValue.update(buf, precision);
    assert ((signum == 0) == unscaledValue.isZero());
    return this;
  }

  /**
   * Updates the value of this object by reading from ByteBuffer, receiving
   * 128+32 bits data (full ranges).
   *
   * @param buf
   *          ByteBuffer to read values from
   */
  public Decimal128 update128(IntBuffer buf) {
    int scaleAndSignum = buf.get();
    this.scale = (short) (scaleAndSignum >> 16);
    this.signum = (byte) (scaleAndSignum & 0xFF);
    this.unscaledValue.update128(buf);
    assert ((signum == 0) == unscaledValue.isZero());
    return this;
  }

  /**
   * Updates the value of this object by reading from ByteBuffer, receiving only
   * 96+32 bits data.
   *
   * @param buf
   *          ByteBuffer to read values from
   */
  public Decimal128 update96(IntBuffer buf) {
    int scaleAndSignum = buf.get();
    this.scale = (short) (scaleAndSignum >> 16);
    this.signum = (byte) (scaleAndSignum & 0xFF);
    this.unscaledValue.update96(buf);
    assert ((signum == 0) == unscaledValue.isZero());
    return this;
  }

  /**
   * Updates the value of this object by reading from ByteBuffer, receiving only
   * 64+32 bits data.
   *
   * @param buf
   *          ByteBuffer to read values from
   */
  public Decimal128 update64(IntBuffer buf) {
    int scaleAndSignum = buf.get();
    this.scale = (short) (scaleAndSignum >> 16);
    this.signum = (byte) (scaleAndSignum & 0xFF);
    this.unscaledValue.update64(buf);
    assert ((signum == 0) == unscaledValue.isZero());
    return this;
  }

  /**
   * Updates the value of this object by reading from ByteBuffer, receiving only
   * 32+32 bits data.
   *
   * @param buf
   *          ByteBuffer to read values from
   */
  public Decimal128 update32(IntBuffer buf) {
    int scaleAndSignum = buf.get();
    this.scale = (short) (scaleAndSignum >> 16);
    this.signum = (byte) (scaleAndSignum & 0xFF);
    this.unscaledValue.update32(buf);
    assert ((signum == 0) == unscaledValue.isZero());
    return this;
  }

  /**
   * Updates the value of this object by reading from the given array, using the
   * required number of ints for the given precision.
   *
   * @param array
   *          array to read values from
   * @param offset
   *          offset of the long array
   * @param precision
   *          0 to 38. Decimal digits.
   */
  public Decimal128 update(int[] array, int offset, int precision) {
    int scaleAndSignum = array[offset];
    this.scale = (short) (scaleAndSignum >> 16);
    this.signum = (byte) (scaleAndSignum & 0xFF);
    this.unscaledValue.update(array, offset + 1, precision);
    return this;
  }

  /**
   * Updates the value of this object by reading from the given integers,
   * receiving 128+32 bits of data (full range).
   *
   * @param array
   *          array to read from
   * @param offset
   *          offset of the int array
   */
  public Decimal128 update128(int[] array, int offset) {
    int scaleAndSignum = array[offset];
    this.scale = (short) (scaleAndSignum >> 16);
    this.signum = (byte) (scaleAndSignum & 0xFF);
    this.unscaledValue.update128(array, offset + 1);
    return this;
  }

  /**
   * Updates the value of this object by reading from the given integers,
   * receiving only 96+32 bits data.
   *
   * @param array
   *          array to read from
   * @param offset
   *          offset of the int array
   */
  public Decimal128 update96(int[] array, int offset) {
    int scaleAndSignum = array[offset];
    this.scale = (short) (scaleAndSignum >> 16);
    this.signum = (byte) (scaleAndSignum & 0xFF);
    this.unscaledValue.update96(array, offset + 1);
    return this;
  }

  /**
   * Updates the value of this object by reading from the given integers,
   * receiving only 64+32 bits data.
   *
   * @param array
   *          array to read from
   * @param offset
   *          offset of the int array
   */
  public Decimal128 update64(int[] array, int offset) {
    int scaleAndSignum = array[offset];
    this.scale = (short) (scaleAndSignum >> 16);
    this.signum = (byte) (scaleAndSignum & 0xFF);
    this.unscaledValue.update64(array, offset + 1);
    return this;
  }

  /**
   * Updates the value of this object by reading from the given integers,
   * receiving only 32+32 bits data.
   *
   * @param array
   *          array to read from
   * @param offset
   *          offset of the int array
   */
  public Decimal128 update32(int[] array, int offset) {
    int scaleAndSignum = array[offset];
    this.scale = (short) (scaleAndSignum >> 16);
    this.signum = (byte) (scaleAndSignum & 0xFF);
    this.unscaledValue.update32(array, offset + 1);
    return this;
  }

  /**
   * Updates the value of this object with the given {@link BigDecimal}. 
   * @param bigDecimal
   *    {@link java.math.BigDecimal}
   */
   public Decimal128 update(BigDecimal bigDecimal) {
     return update(bigDecimal.unscaledValue(), (short) bigDecimal.scale());
   }

  /**
   * Updates the value of this object with the given {@link BigInteger} and scale.
   *
   * @param bigInt
   *          {@link java.math.BigInteger}
   * @param scale
   */
  public Decimal128 update(BigInteger bigInt, short scale) {
    this.scale = scale;
    this.signum = (byte) bigInt.compareTo(BigInteger.ZERO);
    if (signum == 0) {
      update(0);
    } else if (signum < 0) {
      unscaledValue.update(bigInt.negate());
    } else {
      unscaledValue.update(bigInt);
    }
    return this;
  }

  /**
   * Updates the value of this object with the given string.
   *
   * @param str
   *          string
   * @param scale
   *          scale of the {@code Decimal128}.
   */
  public Decimal128 update(String str, short scale) {
    return update(str.toCharArray(), 0, str.length(), scale);
  }

  /**
   * Updates the value of this object from the given string with given offset
   * and length.
   *
   * @param str
   *          string
   * @param offset
   *          offset
   * @param length
   *          length
   * @param scale
   *          scale of the {@code Decimal128}.
   */
  public Decimal128 update(char[] str, int offset, int length, short scale) {
    final int end = offset + length;
    assert (end <= str.length);
    int cursor = offset;

    // sign mark
    boolean negative = false;
    if (str[cursor] == '+') {
      ++cursor;
    } else if (str[cursor] == '-') {
      negative = true;
      ++cursor;
    }

    // Skip leading zeros and compute number of digits in magnitude
    while (cursor < end && str[cursor] == '0') {
      ++cursor;
    }

    this.scale = scale;
    zeroClear();
    if (cursor == end) {
      return this;
    }

    // "1234567" => unscaledValue=1234567, negative=false,
    // fractionalDigits=0
    // "-1234567.89" => unscaledValue=123456789, negative=true,
    // fractionalDigits=2
    // "12.3E7" => unscaledValue=123, negative=false, fractionalDigits=1,
    // exponent=7
    // ".123E-7" => unscaledValue=123, negative=false, fractionalDigits=3,
    // exponent=-7
    int accumulated = 0;
    int accumulatedCount = 0;
    boolean fractional = false; // after "."?
    int fractionalDigits = 0;
    int exponent = 0;
    while (cursor < end) {
      if (str[cursor] == '.') {
        if (fractional) {

          // two dots??
          throw new NumberFormatException("Invalid string:"
              + new String(str, offset, length));
        }
        fractional = true;
      } else if (str[cursor] >= '0' && str[cursor] <= '9') {
        if (accumulatedCount == 9) {
          this.unscaledValue.scaleUpTenDestructive((short) accumulatedCount);
          this.unscaledValue.addDestructive(accumulated);
          accumulated = 0;
          accumulatedCount = 0;
        }
        int digit = str[cursor] - '0';
        accumulated = accumulated * 10 + digit;
        ++accumulatedCount;
        if (fractional) {
          ++fractionalDigits;
        }
      } else if (str[cursor] == 'e' || str[cursor] == 'E') {
        // exponent part
        ++cursor;
        boolean exponentNegative = false;
        if (str[cursor] == '+') {
          ++cursor;
        } else if (str[cursor] == '-') {
          exponentNegative = true;
          ++cursor;
        }
        while (cursor < end) {
          if (str[cursor] >= '0' && str[cursor] <= '9') {
            int exponentDigit = str[cursor] - '0';
            exponent *= 10;
            exponent += exponentDigit;
          }
          ++cursor;
        }
        if (exponentNegative) {
          exponent = -exponent;
        }
      } else {
        throw new NumberFormatException("Invalid string:"
            + new String(str, offset, length));
      }

      ++cursor;
    }

    if (accumulatedCount > 0) {
      this.unscaledValue.scaleUpTenDestructive((short) accumulatedCount);
      this.unscaledValue.addDestructive(accumulated);
    }

    int scaleAdjust = scale - fractionalDigits + exponent;
    if (scaleAdjust > 0) {
      this.unscaledValue.scaleUpTenDestructive((short) scaleAdjust);
    } else if (scaleAdjust < 0) {
      this.unscaledValue.scaleDownTenDestructive((short) -scaleAdjust);
    }
    this.signum = (byte) (this.unscaledValue.isZero() ? 0 : (negative ? -1 : 1));
    return this;
  }

  /**
   * Serializes the value in a format compatible with the BigDecimal's own representation
   * @param scratch
   */
  public int fastSerializeForHiveDecimal( Decimal128FastBuffer scratch) {
    return this.unscaledValue.fastSerializeForHiveDecimal(scratch, this.signum);
  }

  /**
   * Serialize this object to the given array, putting the required number of
   * ints for the given precision.
   *
   * @param array
   *          array to write values to
   * @param offset
   *          offset of the int array
   * @param precision
   *          0 to 38. Decimal digits.
   */
  public void serializeTo(int[] array, int offset, int precision) {
    array[offset] = ((scale << 16) | (signum & 0xFF));
    this.unscaledValue.serializeTo(array, offset + 1, precision);
  }

  /**
   * Serialize this object to the given integers, putting 128+32 bits of data
   * (full range).
   *
   * @param array
   *          array to write values to
   * @param offset
   *          offset of the int array
   */
  public void serializeTo128(int[] array, int offset) {
    array[offset] = ((scale << 16) | (signum & 0xFF));
    this.unscaledValue.serializeTo128(array, offset + 1);
  }

  /**
   * Serialize this object to the given integers, putting only 96+32 bits of
   * data.
   *
   * @param array
   *          array to write values to
   * @param offset
   *          offset of the int array
   */
  public void serializeTo96(int[] array, int offset) {
    array[offset] = ((scale << 16) | (signum & 0xFF));
    this.unscaledValue.serializeTo96(array, offset + 1);
  }

  /**
   * Serialize this object to the given integers, putting only 64+32 bits of
   * data.
   *
   * @param array
   *          array to write values to
   * @param offset
   *          offset of the int array
   */
  public void serializeTo64(int[] array, int offset) {
    array[offset] = ((scale << 16) | (signum & 0xFF));
    this.unscaledValue.serializeTo64(array, offset + 1);
  }

  /**
   * Serialize this object to the given integers, putting only 32+32 bits of
   * data.
   *
   * @param array
   *          array to write values to
   * @param offset
   *          offset of the int array
   */
  public void serializeTo32(int[] array, int offset) {
    array[offset] = ((scale << 16) | (signum & 0xFF));
    this.unscaledValue.serializeTo32(array, offset + 1);
  }

  /**
   * Serialize this object to the given ByteBuffer, putting the required number
   * of ints for the given precision.
   *
   * @param buf
   *          ByteBuffer to write values to
   * @param precision
   *          0 to 38. Decimal digits.
   */
  public void serializeTo(IntBuffer buf, int precision) {
    buf.put((scale << 16) | (signum & 0xFF));
    this.unscaledValue.serializeTo(buf, precision);
  }

  /**
   * Serialize this object to the given ByteBuffer, putting 128+32 bits of data
   * (full range).
   *
   * @param buf
   *          ByteBuffer to write values to
   */
  public void serializeTo128(IntBuffer buf) {
    buf.put((scale << 16) | (signum & 0xFF));
    this.unscaledValue.serializeTo128(buf);
  }

  /**
   * Serialize this object to the given ByteBuffer, putting only 96+32 bits of
   * data.
   *
   * @param buf
   *          ByteBuffer to write values to
   */
  public void serializeTo96(IntBuffer buf) {
    buf.put((scale << 16) | (signum & 0xFF));
    this.unscaledValue.serializeTo96(buf);
  }

  /**
   * Serialize this object to the given ByteBuffer, putting only 64+32 bits of
   * data.
   *
   * @param buf
   *          ByteBuffer to write values to
   */
  public void serializeTo64(IntBuffer buf) {
    buf.put((scale << 16) | (signum & 0xFF));
    this.unscaledValue.serializeTo64(buf);
  }

  /**
   * Serialize this object to the given ByteBuffer, putting only 32+32 bits of
   * data.
   *
   * @param buf
   *          ByteBuffer to write values to
   */
  public void serializeTo32(IntBuffer buf) {
    buf.put((scale << 16) | (signum & 0xFF));
    this.unscaledValue.serializeTo32(buf);
  }

  /**
   * Changes the scaling of this {@code Decimal128}, preserving the represented
   * value. This method is destructive.
   * <p>
   * This method is NOT just a setter for #scale. It also adjusts the unscaled
   * value to preserve the represented value.
   * </p>
   * <p>
   * When the given scaling is larger than the current scaling, this method
   * shrinks the unscaled value accordingly. It will NOT throw any error even if
   * underflow happens.
   * </p>
   * <p>
   * When the given scaling is smaller than the current scaling, this method
   * expands the unscaled value accordingly. It does throw an error if overflow
   * happens.
   * </p>
   * <p>
   * Unchecked exceptions: ArithmeticException a negative value is specified or
   * overflow.
   * </p>
   *
   * @param scale
   *          new scale. must be 0 or positive.
   */
  public void changeScaleDestructive(short scale) {
    if (scale == this.scale) {
      return;
    }

    checkScaleRange(scale);
    short scaleDown = (short) (this.scale - scale);
    if (scaleDown > 0) {
      this.unscaledValue.scaleDownTenDestructive(scaleDown);
      if (this.unscaledValue.isZero()) {
        this.signum = 0;
      }
    } else if (scaleDown < 0) {
      this.unscaledValue.scaleUpTenDestructive((short) -scaleDown);
    }
    this.scale = scale;

    this.unscaledValue.throwIfExceedsTenToThirtyEight();
  }

  /**
   * Calculates addition and puts the result into the given object. Both
   * operands are first scaled up/down to the specified scale, then this method
   * performs the operation. This method is static and not destructive (except
   * the result object).
   * <p>
   * Unchecked exceptions: ArithmeticException an invalid scale is specified or
   * overflow.
   * </p>
   *
   * @param left
   *          left operand
   * @param right
   *          right operand
   * @param scale
   *          scale of the result. must be 0 or positive.
   * @param result
   *          object to receive the calculation result
   */
  public static void add(Decimal128 left, Decimal128 right, Decimal128 result,
      short scale) {
    result.update(left);
    result.addDestructive(right, scale);
  }

  /**
   * Calculates addition and stores the result into this object. This method is
   * destructive.
   * <p>
   * Unchecked exceptions: ArithmeticException an invalid scale is specified or
   * overflow.
   * </p>
   *
   * @param right
   *          right operand
   * @param scale
   *          scale of the result. must be 0 or positive.
   */
  public Decimal128 addDestructive(Decimal128 right, short scale) {
    this.changeScaleDestructive(scale);
    if (right.signum == 0) {
      return this;
    }
    if (this.signum == 0) {
      this.update(right);
      this.changeScaleDestructive(scale);
      return this;
    }

    short rightScaleTen = (short) (scale - right.scale);
    if (this.signum == right.signum) {

      // if same sign, just add up the absolute values
      this.unscaledValue.addDestructiveScaleTen(right.unscaledValue,
          rightScaleTen);
    } else {
      byte cmp = UnsignedInt128.differenceScaleTen(this.unscaledValue,
          right.unscaledValue, this.unscaledValue, rightScaleTen);
      if (cmp == 0) {
        this.signum = 0;
      } else if (cmp < 0) {

        // right's signum wins
        this.signum = right.signum;
      }

      // if left's signum wins, we don't need to do anything
    }

    this.unscaledValue.throwIfExceedsTenToThirtyEight();
    return this;
  }

  /**
   * Calculates subtraction and puts the result into the given object. Both
   * operands are first scaled up/down to the specified scale, then this method
   * performs the operation. This method is static and not destructive (except
   * the result object).
   * <p>
   * Unchecked exceptions: ArithmeticException an invalid scale is specified or
   * overflow.
   * </p>
   *
   * @param left
   *          left operand
   * @param right
   *          right operand
   * @param result
   *          object to receive the calculation result
   * @param scale
   *          scale of the result. must be 0 or positive.
   */
  public static void subtract(Decimal128 left, Decimal128 right,
      Decimal128 result, short scale) {
    result.update(left);
    result.subtractDestructive(right, scale);
  }

  /**
   * Calculates subtraction and stores the result into this object. This method
   * is destructive.
   * <p>
   * Unchecked exceptions: ArithmeticException an invalid scale is specified or
   * overflow.
   * </p>
   *
   * @param right
   *          right operand
   * @param scale
   *          scale of the result. must be 0 or positive.
   */
  public Decimal128 subtractDestructive(Decimal128 right, short scale) {
    this.changeScaleDestructive(scale);
    if (right.signum == 0) {
      return this;
    }
    if (this.signum == 0) {
      this.update(right);
      this.changeScaleDestructive(scale);
      this.negateDestructive();
      return this;
    }

    short rightScaleTen = (short) (scale - right.scale);
    if (this.signum != right.signum) {

      // if different sign, just add up the absolute values
      this.unscaledValue.addDestructiveScaleTen(right.unscaledValue,
          rightScaleTen);
    } else {
      byte cmp = UnsignedInt128.differenceScaleTen(this.unscaledValue,
          right.unscaledValue, this.unscaledValue, rightScaleTen);
      if (cmp == 0) {
        this.signum = 0;
      } else if (cmp < 0) {

        // right's signum wins (notice the negation, because we are
        // subtracting right)
        this.signum = (byte) -right.signum;
      }

      // if left's signum wins, we don't need to do anything
    }

    this.unscaledValue.throwIfExceedsTenToThirtyEight();
    return this;
  }

  /**
   * Calculates multiplication and puts the result into the given object. Both
   * operands are first scaled up/down to the specified scale, then this method
   * performs the operation. This method is static and not destructive (except
   * the result object).
   * <p>
   * Unchecked exceptions: ArithmeticException an invalid scale is specified or
   * overflow.
   * </p>
   *
   * @param left
   *          left operand
   * @param right
   *          right operand
   * @param result
   *          object to receive the calculation result
   * @param scale
   *          scale of the result. must be 0 or positive.
   */
  public static void multiply(Decimal128 left, Decimal128 right,
      Decimal128 result, short scale) {
    if (result == left || result == right) {
      throw new IllegalArgumentException(
          "result object cannot be left or right operand");
    }

    result.update(left);
    result.multiplyDestructive(right, scale);
  }

  /**
   * As of 2/11/2014 this has a known bug in multiplication. See
   * TestDecimal128.testKnownPriorErrors(). Keeping this source
   * code available since eventually it is better to fix this.
   * The fix employed now is to replace this code with code that
   * uses Java BigDecimal multiply.
   *
   * Performs multiplication, changing the scale of this object to the specified
   * value.
   *
   * @param right
   *          right operand. this object is not modified.
   * @param newScale
   *          scale of the result. must be 0 or positive.
   */
  public void multiplyDestructiveNativeDecimal128(Decimal128 right, short newScale) {
    if (this.signum == 0 || right.signum == 0) {
      this.zeroClear();
      this.scale = newScale;
      return;
    }

    // this = this.mag / 10**this.scale
    // right = right.mag / 10**right.scale
    // this * right = this.mag * right.mag / 10**(this.scale + right.scale)
    // so, we need to scale down (this.scale + right.scale - newScale)
    short currentTotalScale = (short) (this.scale + right.scale);
    short scaleBack = (short) (currentTotalScale - newScale);

    if (scaleBack > 0) {

      // we do the scaling down _during_ multiplication to avoid
      // unnecessary overflow.
      // note that even this could overflow if newScale is too small.
      this.unscaledValue.multiplyScaleDownTenDestructive(right.unscaledValue,
          scaleBack);
    } else {

      // in this case, we are actually scaling up.
      // we don't have to do complicated things because doing scaling-up
      // after
      // multiplication doesn't affect overflow (it doesn't happen or
      // happens anyways).
      this.unscaledValue.multiplyDestructive(right.unscaledValue);
      this.unscaledValue.scaleUpTenDestructive((short) -scaleBack);
    }

    this.scale = newScale;
    this.signum = (byte) (this.signum * right.signum);
    if (this.unscaledValue.isZero()) {
      this.signum = 0; // because of scaling down, this could happen
    }
    this.unscaledValue.throwIfExceedsTenToThirtyEight();
  }

  /**
   * Performs multiplication, changing the scale of this object to the specified
   * value.
   *
   * @param right
   *          right operand. this object is not modified.
   * @param newScale
   *          scale of the result. must be 0 or positive.
   */
  public void multiplyDestructive(Decimal128 right, short newScale) {
    HiveDecimal rightHD = HiveDecimal.create(right.toBigDecimal());
    HiveDecimal thisHD = HiveDecimal.create(this.toBigDecimal());
    HiveDecimal result = thisHD.multiply(rightHD);

    /* If the result is null, throw an exception. This can be caught
     * by calling code in the vectorized code path and made to yield
     * a SQL NULL value.
     */
    if (result == null) {
      throw new ArithmeticException("null multiply result");
    }
    this.update(result.bigDecimalValue().toPlainString(), newScale);
    this.unscaledValue.throwIfExceedsTenToThirtyEight();
  }

  /**
   * Performs division and puts the result into the given object. Both operands
   * are first scaled up/down to the specified scale, then this method performs
   * the operation. This method is static and not destructive (except the result
   * object).
   * <p>
   * Unchecked exceptions: ArithmeticException an invalid scale is specified or
   * overflow.
   * </p>
   *
   * @param left
   *          left operand
   * @param right
   *          right operand
   * @param quotient
   *          result object to receive the calculation result
   * @param scale
   *          scale of the result. must be 0 or positive.
   */
  public static void divide(Decimal128 left, Decimal128 right,
      Decimal128 quotient, short scale) {
    if (quotient == left || quotient == right) {
      throw new IllegalArgumentException(
          "result object cannot be left or right operand");
    }

    quotient.update(left);
    quotient.divideDestructive(right, scale);
  }

  /**
   * As of 1/20/2014 this has a known bug in division. See
   * TestDecimal128.testKnownPriorErrors(). Keeping this source
   * code available since eventually it is better to fix this.
   * The fix employed now is to replace this code with code that
   * uses Java BigDecimal divide.
   *
   * Performs division, changing the scale of this object to the specified
   * value.
   * <p>
   * Unchecked exceptions: ArithmeticException an invalid scale is specified or
   * overflow.
   * </p>
   *
   * @param right
   *          right operand. this object is not modified.
   * @param newScale
   *          scale of the result. must be 0 or positive.
   * @param remainder
   *          object to receive remainder
   */
  public void divideDestructiveNativeDecimal128(Decimal128 right, short newScale,
      Decimal128 remainder) {
    if (right.signum == 0) {
      SqlMathUtil.throwZeroDivisionException();
    }
    if (this.signum == 0) {
      this.scale = newScale;
      remainder.update(this);
      return;
    }

    // this = this.mag / 10**this.scale
    // right = right.mag / 10**right.scale
    // this / right = (this.mag / right.mag) / 10**(this.scale -
    // right.scale)
    // so, we need to scale down (this.scale - right.scale - newScale)
    short scaleBack = (short) (this.scale - right.scale - newScale);
    if (scaleBack >= 0) {

      // it's easier then because we simply do division and then scale
      // down.
      this.unscaledValue.divideDestructive(right.unscaledValue,
          remainder.unscaledValue);
      this.unscaledValue.scaleDownTenDestructive(scaleBack);
      remainder.unscaledValue.scaleDownTenDestructive(scaleBack);
    } else {

      // in this case, we have to scale up _BEFORE_ division. otherwise we
      // might lose precision. this is costly, but inevitable.
      this.unscaledValue.divideScaleUpTenDestructive(right.unscaledValue,
          (short) -scaleBack, remainder.unscaledValue);
    }

    this.scale = newScale;
    this.signum = (byte) (this.unscaledValue.isZero() ? 0
        : (this.signum * right.signum));
    remainder.scale = scale;
    remainder.signum = (byte) (remainder.unscaledValue.isZero() ? 0 : 1); // remainder
                                                                          // is
                                                                          // always
                                                                          // positive

    this.unscaledValue.throwIfExceedsTenToThirtyEight();
  }

  /**
   * Divide the target object by right, and scale the result to newScale.
   *
   * This uses HiveDecimal to get a correct answer with the same rounding
   * behavior as HiveDecimal, but it is expensive.
   *
   * In the future, a native implementation could be faster.
   */
  public void divideDestructive(Decimal128 right, short newScale) {
    HiveDecimal rightHD = HiveDecimal.create(right.toBigDecimal());
    HiveDecimal thisHD = HiveDecimal.create(this.toBigDecimal());
    HiveDecimal result = thisHD.divide(rightHD);

    /* If the result is null, throw an exception. This can be caught
     * by calling code in the vectorized code path and made to yield
     * a SQL NULL value.
     */
    if (result == null) {
      throw new ArithmeticException("null divide result");
    }
    this.update(result.bigDecimalValue().toPlainString(), newScale);
    this.unscaledValue.throwIfExceedsTenToThirtyEight();
  }

  /**
    * Performs decimal modulo
    * <p>
    * The definition of modulo (x % p) is:
    *   x - IntegerPart(x / p, resultScale) * p
    * </p>
    *
    * @param left
    *    is x
    * @param right
    *    is p
    * @param result
    *    receives the result
    * @param scale
    *    scale of result
    */
   public static void modulo(Decimal128 left, Decimal128 right, Decimal128 result,
       short scale) {

     // set result to x / p (the quotient)
     Decimal128.divide(left, right, result, scale);

     // take integer part of it
     result.zeroFractionPart();

     // multiply by p
     result.multiplyDestructive(right, scale);

     // negate it
     result.negateDestructive();

     // add x to it
     result.addDestructive(left, scale);
   }

  /**
   * Makes this {@code Decimal128} a positive number. Unlike
   * java.math.BigDecimal, this method is destructive.
   */
  public void absDestructive() {
    if (this.signum < 0) {
      this.signum = 1;
    }
  }

  /**
   * Reverses the sign of this {@code Decimal128}. Unlike java.math.BigDecimal,
   * this method is destructive.
   */
  public void negateDestructive() {
    this.signum = (byte) (-this.signum);
  }

  /**
   * Calculate the square root of this value in double precision.
   * <p>
   * Note that this does NOT perform the calculation in infinite accuracy.
   * Although this sounds weird, it's at least how SQLServer does it. The SQL
   * below demonstrates that SQLServer actually converts it into FLOAT (Java's
   * double).
   * </p>
   * <code>
   * create table bb (col1 DECIMAL(38,36), col2 DECIMAL (38,36));
   * insert into bb values(1.00435134913958923485982394892384,1.00345982739817298323423423);
   * select sqrt(col1) as cola,sqrt(col2) colb into asdasd from bb;
   * sp_columns asdasd;
   * </code>
   * <p>
   * This SQL tells that the result is in FLOAT data type. One justification is
   * that, because DECIMAL is at most 38 digits, double precision is enough for
   * sqrt/pow.
   * </p>
   * <p>
   * This method does not modify this object.
   * </p>
   * <p>
   * This method throws exception if this value is negative rather than
   * returning NaN. This is the SQL semantics (at least in SQLServer).
   * </p>
   *
   * @return square root of this value
   */
  public double sqrtAsDouble() {
    if (this.signum == 0) {
      return 0;
    } else if (this.signum < 0) {
      throw new ArithmeticException("sqrt will not be a real number");
    }
    double val = doubleValue();
    return Math.sqrt(val);
  }

  /**
   * <p>
   * Calculate the power of this value in double precision. Note that this does
   * NOT perform the calculation in infinite accuracy. Although this sounds
   * weird, it's at least how SQLServer does it. The SQL below demonstrates that
   * SQLServer actually converts it into FLOAT (Java's double).
   * </p>
   * <code>
   * create table bb (col1 DECIMAL(38,36), col2 DECIMAL (38,36));
   * insert into bb values(1.00435134913958923485982394892384,1.00345982739817298323423423);
   * select power(col1, col2);
   * </code>
   * <p>
   * This SQL returns '1.004366436877081000000000000000000000', which is merely
   * a power calculated in double precision (
   * "java.lang.Math.pow(1.00435134913958923485982394892384d, 1.00345982739817298323423423d)"
   * returns 1.0043664368770813), and then scaled into DECIMAL. The same thing
   * happens even when "n" is a positive integer. One justification is that,
   * because DECIMAL is at most 38 digits, double precision is enough for
   * sqrt/pow.
   * </p>
   * <p>
   * This method does not modify this object.
   * </p>
   * <p>
   * This method throws exception if the calculated value is Infinite. This is
   * the SQL semantics (at least in SQLServer).
   * </p>
   *
   * @param n
   *          power to raise this object. Unlike
   *          {@link java.math.BigDecimal#pow(int)}, this can receive a
   *          fractional number. Instead, this method calculates the value in
   *          double precision.
   * @return power of this value
   */
  public double powAsDouble(double n) {
    if (this.signum == 0) {
      return 0;
    }
    double val = doubleValue();
    double result = Math.pow(val, n);
    if (Double.isInfinite(result) || Double.isNaN(result)) {
      SqlMathUtil.throwOverflowException();
    }
    return result;
  }

  /**
   * Returns the signum of this {@code Decimal128}.
   *
   * @return -1, 0, or 1 as the value of this {@code Decimal128} is negative,
   *         zero, or positive.
   */
  public byte getSignum() {
    return signum;
  }

  /**
   * Returns the <i>scale</i> of this {@code Decimal128}. The scale is the
   * positive number of digits to the right of the decimal point.
   *
   * @return the scale of this {@code Decimal128}.
   */
  public short getScale() {
    return scale;
  }

  /**
   * Returns unscaled value of this {@code Decimal128}. Be careful because
   * changes on the returned object will affect the object. It is not
   * recommended to modify the object this method returns.
   *
   * @return the unscaled value of this {@code Decimal128}.
   */
  public UnsignedInt128 getUnscaledValue() {
    return unscaledValue;
  }

  // Comparison Operations

  /**
   * Compares this {@code Decimal128} with the specified {@code Decimal128}. Two
   * {@code Decimal128} objects that are equal in value but have a different
   * scale (like 2.0 and 2.00) are considered equal by this method. This method
   * is provided in preference to individual methods for each of the six boolean
   * comparison operators ({@literal <}, ==, {@literal >}, {@literal >=}, !=,
   * {@literal <=}). The suggested idiom for performing these comparisons is:
   * {@code (x.compareTo(y)} &lt;<i>op</i>&gt; {@code 0)}, where
   * &lt;<i>op</i>&gt; is one of the six comparison operators.
   *
   * @param val
   *          {@code Decimal128} to which this {@code Decimal128} is to be
   *          compared.
   * @return a negative integer, zero, or a positive integer as this
   *         {@code Decimal128} is numerically less than, equal to, or greater
   *         than {@code val}.
   */
  @Override
  public int compareTo(Decimal128 val) {
    if (val == this) {
      return 0;
    }

    if (this.signum != val.signum) {
      return this.signum - val.signum;
    }

    int cmp;
    if (this.scale >= val.scale) {
      cmp = this.unscaledValue.compareToScaleTen(val.unscaledValue,
          (short) (this.scale - val.scale));
    } else {
      cmp = - val.unscaledValue.compareToScaleTen(this.unscaledValue,
          (short) (val.scale - this.scale));
    }
    return cmp * this.signum;
  }

  /**
   * Compares this {@code Decimal128} with the specified {@code Object} for
   * equality. Unlike {@link #compareTo(Decimal128) compareTo}, this method
   * considers two {@code Decimal128} objects equal only if they are equal in
   * value and scale (thus 2.0 is not equal to 2.00 when compared by this
   * method). This somewhat confusing behavior is, however, same as
   * java.math.BigDecimal.
   *
   * @param x
   *          {@code Object} to which this {@code Decimal128} is to be compared.
   * @return {@code true} if and only if the specified {@code Object} is a
   *         {@code Decimal128} whose value and scale are equal to this
   *         {@code Decimal128}'s.
   * @see #compareTo(Decimal128)
   * @see #hashCode
   */
  @Override
  public boolean equals(Object x) {
    if (x == this) {
      return true;
    }
    if (!(x instanceof Decimal128)) {
      return false;
    }

    Decimal128 xDec = (Decimal128) x;
    if (scale != xDec.scale) {
      return false;
    }
    if (signum != xDec.signum) {
      return false;
    }

    return unscaledValue.equals(xDec.unscaledValue);
  }

  /**
   * Returns the hash code for this {@code Decimal128}. Note that two
   * {@code Decimal128} objects that are numerically equal but differ in scale
   * (like 2.0 and 2.00) will generally <i>not</i> have the same hash code. This
   * somewhat confusing behavior is, however, same as java.math.BigDecimal.
   *
   * @return hash code for this {@code Decimal128}.
   * @see #equals(Object)
   */
  @Override
  public int hashCode() {
    if (signum == 0) {
      return 0;
    }
    return signum * (scale * 31 + unscaledValue.hashCode());
  }

  /**
   * Converts this {@code Decimal128} to a {@code long}. This conversion is
   * analogous to the <i>narrowing primitive conversion</i> from {@code double}
   * to {@code short} as defined in section 5.1.3 of <cite>The Java&trade;
   * Language Specification</cite>: any fractional part of this
   * {@code Decimal128} will be discarded, and if the resulting "
   * {@code UnsignedInt128}" is too big to fit in a {@code long}, only the
   * low-order 64 bits are returned. Note that this conversion can lose
   * information about the overall magnitude and precision of this
   * {@code Decimal128} value.
   *
   * @return this {@code Decimal128} converted to a {@code long}.
   */
  @Override
  public long longValue() {

    // Avoid allocating temporary variables for special cases: signum or scale is zero
    if (signum == 0) {
      return 0L;
    }
    if (scale == 0) {
      long ret;
      ret = this.unscaledValue.getV1();
      ret <<= 32L;
      ret |= SqlMathUtil.LONG_MASK & this.unscaledValue.getV0();
      if (signum >= 0) {
        return ret;
      } else {
        return -ret;
      }
    } else {
      HiveDecimal hd = HiveDecimal.create(this.toBigDecimal());
      return hd.longValue();
    }
  }

  /**
   * Converts this {@code Decimal128} to an {@code int}. This conversion is
   * analogous to the <i>narrowing primitive conversion</i> from {@code double}
   * to {@code short} as defined in section 5.1.3 of <cite>The Java&trade;
   * Language Specification</cite>: any fractional part of this
   * {@code Decimal128} will be discarded, and if the resulting "
   * {@code UnsignedInt128}" is too big to fit in an {@code int}, only the
   * low-order 32 bits are returned. Note that this conversion can lose
   * information about the overall magnitude and precision of this
   * {@code Decimal128} value.
   *
   * @return this {@code Decimal128} converted to an {@code int}.
   */
  @Override
  public int intValue() {
    if (signum == 0) {
      return 0;
    }

    int ret;
    if (scale == 0) {
      ret = this.unscaledValue.getV0();
    } else {
      UnsignedInt128 tmp = new UnsignedInt128(this.unscaledValue);
      tmp.scaleDownTenDestructive(scale);
      ret = tmp.getV0();
    }

    return SqlMathUtil.setSignBitInt(ret, signum > 0);
  }

  /**
   * Converts this {@code Decimal128} to a {@code float}. This conversion is
   * similar to the <i>narrowing primitive conversion</i> from {@code double} to
   * {@code float} as defined in section 5.1.3 of <cite>The Java&trade; Language
   * Specification</cite>: if this {@code Decimal128} has too great a magnitude
   * to represent as a {@code float}, it will be converted to
   * {@link Float#NEGATIVE_INFINITY} or {@link Float#POSITIVE_INFINITY} as
   * appropriate. Note that even when the return value is finite, this
   * conversion can lose information about the precision of the
   * {@code Decimal128} value.
   *
   * @return this {@code Decimal128} converted to a {@code float}.
   */
  @Override
  public float floatValue() {

    // if this function is frequently used, we need to optimize this.
    return Float.parseFloat(toFormalString());
  }

  /**
   * Converts this {@code Decimal128} to a {@code double}. This conversion is
   * similar to the <i>narrowing primitive conversion</i> from {@code double} to
   * {@code float} as defined in section 5.1.3 of <cite>The Java&trade; Language
   * Specification</cite>: if this {@code Decimal128} has too great a magnitude
   * represent as a {@code double}, it will be converted to
   * {@link Double#NEGATIVE_INFINITY} or {@link Double#POSITIVE_INFINITY} as
   * appropriate. Note that even when the return value is finite, this
   * conversion can lose information about the precision of the
   * {@code Decimal128} value.
   *
   * @return this {@code Decimal128} converted to a {@code double}.
   */
  @Override
  public double doubleValue() {

    // if this function is frequently used, we need to optimize this.
    return Double.parseDouble(toFormalString());
  }

  /**
   * Converts this object to {@link BigDecimal}. This method is not supposed to
   * be used in a performance-sensitive place.
   *
   * @return {@link BigDecimal} object equivalent to this object.
   */
  public BigDecimal toBigDecimal() {

    // if this function is frequently used, we need to optimize this.
    return new BigDecimal(toFormalString());
  }

  /**
   * Throws an exception if the value of this object exceeds the maximum value
   * for the given precision. Remember that underflow is not an error, but
   * overflow is an immediate error.
   *
   * @param precision
   *          maximum precision
   */
  public void checkPrecisionOverflow(int precision) {
    if (precision <= 0 || precision > 38) {
      throw new IllegalArgumentException("Invalid precision " + precision);
    }

    if (this.unscaledValue.compareTo(SqlMathUtil.POWER_TENS_INT128[precision]) >= 0) {
      SqlMathUtil.throwOverflowException();
    }
  }

  /**
   * Throws an exception if the given scale is invalid for this object.
   *
   * @param scale
   *          scale value
   */
  private static void checkScaleRange(short scale) {
    if (scale < MIN_SCALE) {
      throw new ArithmeticException(
          "Decimal128 does not support negative scaling");
    }
    if (scale > MAX_SCALE) {
      throw new ArithmeticException("Beyond possible Decimal128 scaling");
    }
  }

  /**
   * Temporary array used in {@link #getHiveDecimalString}
   */
  private int [] tmpArray = new int[2];

  /**
   * Returns the string representation of this value. It discards the trailing zeros
   * in the fractional part to match the HiveDecimal's string representation. However,
   * don't use this string representation for the reconstruction of the object.
   *
   * @return string representation of this value
   */
  public String getHiveDecimalString() {
    if (this.signum == 0) {
      return "0";
    }

    StringBuilder buf = new StringBuilder(50);
    if (this.signum < 0) {
      buf.append('-');
    }

    char [] unscaled = this.unscaledValue.getDigitsArray(tmpArray);
    int unscaledLength = tmpArray[0];
    int trailingZeros = tmpArray[1];
    int numIntegerDigits = unscaledLength - this.scale;
    if (numIntegerDigits > 0) {

      // write out integer part first
      // then write out fractional part
      for (int i=0; i < numIntegerDigits; i++) {
        buf.append(unscaled[i]);
      }

      if (this.scale > trailingZeros) {
        buf.append('.');
        for (int i = numIntegerDigits; i < (unscaledLength - trailingZeros); i++) {
          buf.append(unscaled[i]);
        }
      }
    } else {

      // no integer part
      buf.append('0');

      if (this.scale > trailingZeros) {

        // fractional part has, starting with zeros
        buf.append('.');
        for (int i = unscaledLength; i < this.scale; ++i) {
          buf.append('0');
        }
        for (int i = 0; i < (unscaledLength - trailingZeros); i++) {
          buf.append(unscaled[i]);
        }
      }
    }

    return new String(buf);
  }

  /**
   * Returns the formal string representation of this value. Unlike the debug
   * string returned by {@link #toString()}, this method returns a string that
   * can be used to re-construct this object. Remember, toString() is only for
   * debugging.
   *
   * @return string representation of this value
   */
  public String toFormalString() {
    if (this.signum == 0) {
      return "0";
    }

    StringBuilder buf = new StringBuilder(50);
    if (this.signum < 0) {
      buf.append('-');
    }

    String unscaled = this.unscaledValue.toFormalString();
    if (unscaled.length() > this.scale) {

      // write out integer part first
      // then write out fractional part
      buf.append(unscaled, 0, unscaled.length() - this.scale);

      if (this.scale > 0) {
        buf.append('.');
        buf.append(unscaled, unscaled.length() - this.scale, unscaled.length());
      }
    } else {

      // no integer part
      buf.append('0');

      if (this.scale > 0) {

        // fractional part has, starting with zeros
        buf.append('.');
        for (int i = unscaled.length(); i < this.scale; ++i) {
          buf.append('0');
        }
        buf.append(unscaled);
      }
    }

    return new String(buf);
  }

  @Override
  public String toString() {
    return toFormalString() + "(Decimal128: scale=" + scale + ", signum="
        + signum + ", BigDecimal.toString=" + toBigDecimal().toString()
        + ", unscaledValue=[" + unscaledValue.toString() + "])";
  }

  /**
   * Vectorized execution uses the smallest possible positive non-zero
   * value to prevent possible later zero-divide exceptions. Set the field
   * to this value (1 in the internal unsigned 128 bit int).
   */
  public void setNullDataValue() {
    unscaledValue.update(1, 0, 0, 0);
  }

  /**
   * Update the value to a decimal value with the decimal point equal to
   * val but with the decimal point inserted scale
   * digits from the right. Behavior is undefined if scale is &gt; 38 or &lt; 0.
   *
   * For example, updateFixedPoint(123456789L, (short) 3) changes the target
   * to the value 123456.789 with scale 3.
   */
  public void updateFixedPoint(long val, short scale) {
    this.scale = scale;
    if (val < 0L) {
      this.unscaledValue.update(-val);
      this.signum = -1;
    } else if (val == 0L) {
      zeroClear();
    } else {
      this.unscaledValue.update(val);
      this.signum = 1;
    }
  }

  /**
   * Zero the fractional part of value.
   *
   * Argument scratch is needed to hold unused remainder output, to avoid need to
   * create a new object.
   */
  public void zeroFractionPart(UnsignedInt128 scratch) {
    short placesToRemove = this.getScale();

    // If there's no fraction part, return immediately to avoid the cost of a divide.
    if (placesToRemove == 0) {
      return;
    }

    /* Divide by a power of 10 equal to 10**scale to logically shift the digits
     * places right by "scale" positions to eliminate them.
     */
    UnsignedInt128 powerTenDivisor = SqlMathUtil.POWER_TENS_INT128[placesToRemove];

    this.getUnscaledValue().divideDestructive(powerTenDivisor, scratch);

    /* Multiply by the same power of ten to shift the decimal point back to
     * the original place. Places to the right of the decimal will be zero.
     */
    this.getUnscaledValue().scaleUpTenDestructive(placesToRemove);

    if (this.unscaledValue.isZero()) {
      this.signum = 0;
    }
  }

  public void zeroFractionPart() {
    /* A scratch variable is created here. This could be optimized in the future
     * by perhaps using thread-local storage to allocate this scratch field.
     */
    UnsignedInt128 scratch = new UnsignedInt128();
    zeroFractionPart(scratch);
  }

  /**
   * Multiplies this with this, updating this
   * 
   * @return self
   */
  public Decimal128 squareDestructive() {
     this.multiplyDestructive(this, this.getScale());
     return this;
  }

  /**
   * For UDAF variance we use the algorithm described by Chan, Golub, and LeVeque in
   * "Algorithms for computing the sample variance: analysis and recommendations"
   * The American Statistician, 37 (1983) pp. 242--247.
   *
   * variance = variance1 + variance2 + n/(m*(m+n)) * pow(((m/n)*t1 - t2),2)
   *
   * where: - variance is sum[x-avg^2] (this is actually n times the variance)
   * and is updated at every step. - n is the count of elements in chunk1 - m is
   * the count of elements in chunk2 - t1 = sum of elements in chunk1, t2 =
   * sum of elements in chunk2.
   *
   * This is a helper function doing the intermediate computation:
   *   t = myagg.count*value - myagg.sum;
   *   myagg.variance += (t*t) / ((double)myagg.count*(myagg.count-1));
   *
   * @return self
   */
  public Decimal128 updateVarianceDestructive(
          Decimal128 scratch, Decimal128 value, Decimal128 sum, long count) {
      scratch.update(count);
      scratch.multiplyDestructive(value, value.getScale());
      scratch.subtractDestructive(sum, sum.getScale());
      scratch.squareDestructive();
      scratch.unscaledValue.divideDestructive(count * (count-1));
      this.addDestructive(scratch, getScale());
      return this;
  }

  /**
   * Fats update from BigInteger two's complement representation
   * @param internalStorage BigInteger two's complement representation of the unscaled value
   * @param scale
   */
  public Decimal128 fastUpdateFromInternalStorage(byte[] internalStorage, short scale) {
    this.scale = scale;
    this.signum = this.unscaledValue.fastUpdateFromInternalStorage(internalStorage);

    return this;
  }

  /**
   * This setter is only for de-serialization, should not be used otherwise.
   */
  public void setUnscaledValue(UnsignedInt128 unscaledValue) {
    this.unscaledValue = unscaledValue;
  }

  /**
   * This setter is only for de-serialization, should not be used otherwise.
   */
  public void setScale(short scale) {
    this.scale = scale;
  }

  /**
   * This setter is only for de-serialization, should not be used otherwise.
   */
  public void setSignum(byte signum) {
    this.signum = signum;
  }
}

