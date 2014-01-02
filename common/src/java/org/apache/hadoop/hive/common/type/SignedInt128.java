/**
 * Copyright (c) Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.common.type;

import java.math.BigInteger;
import java.nio.IntBuffer;

/**
 * This code was originally written for Microsoft PolyBase.
 *
 * Represents a signed 128-bit integer. This object is much faster and more
 * compact than BigInteger, but has many limitations explained in
 * {@link UnsignedInt128}. In short, this class is a thin wrapper for
 * {@link UnsignedInt128} to make it signed. This object can be used to
 * represent a few SQL data types, such as DATETIMEOFFSET in SQLServer.
 */
public final class SignedInt128 extends Number implements
    Comparable<SignedInt128> {

  /** Maximum value that can be represented in this class. */
  public static final SignedInt128 MAX_VALUE = new SignedInt128(0xFFFFFFFF,
      0xFFFFFFFF, 0xFFFFFFFF, 0x7FFFFFFF);

  /** Minimum value that can be represented in this class. */
  public static final SignedInt128 MIN_VALUE = new SignedInt128(0xFFFFFFFF,
      0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF);

  /** For Serializable. */
  private static final long serialVersionUID = 1L;

  /** Magnitude. Core implementation of this object. */
  private final UnsignedInt128 mag;

  /**
   * Whether the value is negative (zero is NOT negative). When serialized, this
   * flag is combined into the most significant integer in mag. In other words,
   * this object can use only 127 bits in mag. UnsignedInt128 itself can handle
   * 128 bits data.
   */
  private boolean negative;

  /**
   * Determines the number of ints to store one value.
   *
   * @param precision
   *          precision (0-38)
   * @return the number of ints to store one value
   */
  public static int getIntsPerElement(int precision) {
    return UnsignedInt128.getIntsPerElement(precision);
  }

  /**
   * Empty constructor to construct zero.
   */
  public SignedInt128() {
    this.negative = false;
    this.mag = new UnsignedInt128(0, 0, 0, 0);
  }

  /**
   * Construct this object from a long value.
   *
   * @param v
   *          long value
   */
  public SignedInt128(long v) {
    this.negative = v < 0L;
    this.mag = new UnsignedInt128(v < 0 ? -v : v);
  }

  /**
   * Construct this object from UnsignedInt128. The highest bit of the
   * UnsignedInt128 is converted as the sign bit just like conversion between
   * int/uint in C++.
   *
   * @param mag
   *          UnsignedInt128 object
   */
  public SignedInt128(UnsignedInt128 mag) {
    this.negative = mag.getV3() < 0;
    this.mag = new UnsignedInt128(mag.getV0(), mag.getV1(), mag.getV2(),
        mag.getV3() & SqlMathUtil.FULLBITS_31);
  }

  /**
   * Copy Constructor.
   *
   * @param o
   *          object to copy from
   */
  public SignedInt128(SignedInt128 o) {
    this.negative = o.negative;
    this.mag = new UnsignedInt128(o.mag);
  }

  /**
   * Construct this object from the given integers. The highest bit of v3 is
   * converted as the sign bit just like conversion between int/uint in C++.
   *
   * @param v0
   *          v0
   * @param v1
   *          v1
   * @param v2
   *          v2
   * @param v3
   *          v3
   */
  public SignedInt128(int v0, int v1, int v2, int v3) {
    this.negative = v3 < 0;
    this.mag = new UnsignedInt128(v0, v1, v2, v3 & SqlMathUtil.FULLBITS_31);
  }

  /**
   * Constructs from the given string.
   *
   * @param str
   *          string
   */
  public SignedInt128(String str) {
    this();
    update(str);
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
   */
  public SignedInt128(char[] str, int offset, int length) {
    this();
    update(str, offset, length);
  }

  /** @return v[0] */
  public int getV0() {
    return this.mag.getV0();
  }

  /** @return v[1] */
  public int getV1() {
    return this.mag.getV1();
  }

  /** @return v[2] */
  public int getV2() {
    return this.mag.getV2();
  }

  /** @return v[3] */
  public int getV3() {
    return this.mag.getV3();
  }

  /** Make the value to zero. */
  public void zeroClear() {
    this.mag.zeroClear();
    this.negative = false;
  }

  /**
   * Update this object with the given long value.
   *
   * @param v
   *          long value
   */
  public void update(long v) {
    this.negative = v < 0L;
    this.mag.update(v < 0 ? -v : v);
  }

  /**
   * Update this object with the value of the given object.
   *
   * @param o
   *          object to copy from
   */
  public void update(SignedInt128 o) {
    this.negative = o.negative;
    this.mag.update(o.mag);
  }

  /**
   * Updates the value of this object with the given string.
   *
   * @param str
   *          string
   */
  public void update(String str) {
    update(str.toCharArray(), 0, str.length());
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
   */
  public void update(char[] str, int offset, int length) {
    if (length == 0) {
      this.zeroClear();
      return;
    }
    this.negative = false;
    if (str[offset] == '-') {
      this.negative = true;
      ++offset;
      --length;
    } else if (str[offset] == '+') {
      ++offset;
      --length;
    }
    this.mag.update(str, offset, length);
    if (this.mag.isZero()) {
      this.negative = false;
    }
  }

  /**
   * Update this object with the given integers, receiving 128 bits data (full
   * ranges).
   *
   * @param v0
   *          v0
   * @param v1
   *          v1
   * @param v2
   *          v2
   * @param v3
   *          v3
   */
  public void update128(int v0, int v1, int v2, int v3) {
    this.negative = (v3 < 0);
    this.mag.update(v0, v1, v2, v3 & SqlMathUtil.FULLBITS_31);
  }

  /**
   * Update this object with the given integers, receiving only 96 bits data.
   *
   * @param v0
   *          v0
   * @param v1
   *          v1
   * @param v2
   *          v2
   */
  public void update96(int v0, int v1, int v2) {
    this.negative = (v2 < 0);
    this.mag.update(v0, v1, v2 & SqlMathUtil.FULLBITS_31, 0);
  }

  /**
   * Update this object with the given integers, receiving only 64 bits data.
   *
   * @param v0
   *          v0
   * @param v1
   *          v1
   */
  public void update64(int v0, int v1) {
    this.negative = (v1 < 0);
    this.mag.update(v0, v1 & SqlMathUtil.FULLBITS_31, 0, 0);
  }

  /**
   * Update this object with the given integers, receiving only 32 bits data.
   *
   * @param v0
   *          v0
   */
  public void update32(int v0) {
    this.negative = (v0 < 0);
    this.mag.update(v0 & SqlMathUtil.FULLBITS_31, 0, 0, 0);
  }

  /**
   * Updates the value of this object by reading from the given array, receiving
   * 128 bits data (full ranges).
   *
   * @param array
   *          array to read values from
   * @param offset
   *          offset of the int array
   */
  public void update128(int[] array, int offset) {
    update128(array[offset], array[offset + 1], array[offset + 2],
        array[offset + 3]);
  }

  /**
   * Updates the value of this object by reading from the given array, receiving
   * only 96 bits data.
   *
   * @param array
   *          array to read values from
   * @param offset
   *          offset of the int array
   */
  public void update96(int[] array, int offset) {
    update96(array[offset], array[offset + 1], array[offset + 2]);
  }

  /**
   * Updates the value of this object by reading from the given array, receiving
   * only 64 bits data.
   *
   * @param array
   *          array to read values from
   * @param offset
   *          offset of the int array
   */
  public void update64(int[] array, int offset) {
    update64(array[offset], array[offset + 1]);
  }

  /**
   * Updates the value of this object by reading from the given array, receiving
   * only 32 bits data.
   *
   * @param array
   *          array to read values from
   * @param offset
   *          offset of the int array
   */
  public void update32(int[] array, int offset) {
    update32(array[offset]);
  }

  /**
   * Updates the value of this object by reading from ByteBuffer, receiving 128
   * bits data (full ranges).
   *
   * @param buf
   *          ByteBuffer to read values from
   */
  public void update128(IntBuffer buf) {
    update128(buf.get(), buf.get(), buf.get(), buf.get());
  }

  /**
   * Updates the value of this object by reading from ByteBuffer, receiving only
   * 96 bits data.
   *
   * @param buf
   *          ByteBuffer to read values from
   */
  public void update96(IntBuffer buf) {
    update96(buf.get(), buf.get(), buf.get());
  }

  /**
   * Updates the value of this object by reading from ByteBuffer, receiving only
   * 64 bits data.
   *
   * @param buf
   *          ByteBuffer to read values from
   */
  public void update64(IntBuffer buf) {
    update64(buf.get(), buf.get());
  }

  /**
   * Updates the value of this object by reading from ByteBuffer, receiving only
   * 32 bits data.
   *
   * @param buf
   *          ByteBuffer to read values from
   */
  public void update32(IntBuffer buf) {
    update32(buf.get());
  }

  /**
   * Serializes the value of this object to the given array, putting 128 bits
   * data (full ranges).
   *
   * @param array
   *          array to use
   * @param offset
   *          offset of the int array
   */
  public void serializeTo128(int[] array, int offset) {
    assert (this.mag.getV3() >= 0);
    array[offset] = this.mag.getV0();
    array[offset + 1] = this.mag.getV1();
    array[offset + 2] = this.mag.getV2();
    array[offset + 3] = this.mag.getV3()
        | (this.negative ? SqlMathUtil.NEGATIVE_INT_MASK : 0);
  }

  /**
   * Serializes the value of this object to the given array, putting only 96
   * bits data.
   *
   * @param array
   *          array to use
   * @param offset
   *          offset of the int array
   */
  public void serializeTo96(int[] array, int offset) {
    assert (this.mag.getV3() == 0 && this.mag.getV2() >= 0);
    array[offset] = this.mag.getV0();
    array[offset + 1] = this.mag.getV1();
    array[offset + 2] = this.mag.getV2()
        | (this.negative ? SqlMathUtil.NEGATIVE_INT_MASK : 0);
  }

  /**
   * Serializes the value of this object to the given array, putting only 64
   * bits data.
   *
   * @param array
   *          array to use
   * @param offset
   *          offset of the int array
   */
  public void serializeTo64(int[] array, int offset) {
    assert (this.mag.getV3() == 0 && this.mag.getV2() == 0 && this.mag.getV1() >= 0);
    array[offset] = this.mag.getV0();
    array[offset + 1] = this.mag.getV1()
        | (this.negative ? SqlMathUtil.NEGATIVE_INT_MASK : 0);
  }

  /**
   * Serializes the value of this object to the given array, putting only 32
   * bits data.
   *
   * @param array
   *          array to use
   * @param offset
   *          offset of the int array
   */
  public void serializeTo32(int[] array, int offset) {
    assert (this.mag.getV3() == 0 && this.mag.getV2() == 0
        && this.mag.getV1() == 0 && this.mag.getV0() >= 0);
    array[offset] = this.mag.getV0()
        | (this.negative ? SqlMathUtil.NEGATIVE_INT_MASK : 0);
  }

  /**
   * Serializes the value of this object to ByteBuffer, putting 128 bits data
   * (full ranges).
   *
   * @param buf
   *          ByteBuffer to use
   */
  public void serializeTo128(IntBuffer buf) {
    assert (this.mag.getV3() >= 0);
    buf.put(this.mag.getV0());
    buf.put(this.mag.getV1());
    buf.put(this.mag.getV2());
    buf.put(this.mag.getV3()
        | (this.negative ? SqlMathUtil.NEGATIVE_INT_MASK : 0));
  }

  /**
   * Serializes the value of this object to ByteBuffer, putting only 96 bits
   * data.
   *
   * @param buf
   *          ByteBuffer to use
   */
  public void serializeTo96(IntBuffer buf) {
    assert (this.mag.getV3() == 0 && this.mag.getV2() >= 0);
    buf.put(this.mag.getV0());
    buf.put(this.mag.getV1());
    buf.put(this.mag.getV2()
        | (this.negative ? SqlMathUtil.NEGATIVE_INT_MASK : 0));
  }

  /**
   * Serializes the value of this object to ByteBuffer, putting only 64 bits
   * data.
   *
   * @param buf
   *          ByteBuffer to use
   */
  public void serializeTo64(IntBuffer buf) {
    assert (this.mag.getV3() == 0 && this.mag.getV2() == 0 && this.mag.getV1() >= 0);
    buf.put(this.mag.getV0());
    buf.put(this.mag.getV1()
        | (this.negative ? SqlMathUtil.NEGATIVE_INT_MASK : 0));
  }

  /**
   * Serializes the value of this object to ByteBuffer, putting only 32 bits
   * data.
   *
   * @param buf
   *          ByteBuffer to use
   */
  public void serializeTo32(IntBuffer buf) {
    assert (this.mag.getV3() == 0 && this.mag.getV2() == 0
        && this.mag.getV1() == 0 && this.mag.getV0() >= 0);
    buf.put(this.mag.getV0()
        | (this.negative ? SqlMathUtil.NEGATIVE_INT_MASK : 0));
  }

  /**
   * @return Whether this object represents zero.
   */
  public boolean isZero() {
    return this.mag.isZero();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SignedInt128) {
      SignedInt128 o = (SignedInt128) obj;
      return this.negative == o.negative && mag.equals(o.mag);
    } else {
      return false;
    }
  }

  /**
   * Specialized version.
   *
   * @param o
   *          the object to compare
   * @return whether the object is equal to this object
   */
  public boolean equals(SignedInt128 o) {
    return this.negative == o.negative && mag.equals(o.mag);
  }

  @Override
  public int hashCode() {
    return this.negative ? -mag.hashCode() : mag.hashCode();
  }

  @Override
  public int compareTo(SignedInt128 o) {
    if (negative) {
      if (o.negative) {
        return o.mag.compareTo(mag);
      } else {
        return -1;
      }
    } else {
      if (o.negative) {
        return 1;
      } else {
        return mag.compareTo(o.mag);
      }
    }
  }

  @Override
  public int intValue() {
    int unsigned = this.mag.getV0() & SqlMathUtil.FULLBITS_31;
    return this.negative ? -unsigned : unsigned;
  }

  @Override
  public long longValue() {
    long unsigned = SqlMathUtil.combineInts(this.mag.getV0(), this.mag.getV1())
        & SqlMathUtil.FULLBITS_63;
    return this.negative ? -unsigned : unsigned;
  }

  @Override
  public float floatValue() {
    return intValue();
  }

  @Override
  public double doubleValue() {
    return longValue();
  }

  /**
   * Calculates addition and puts the result into the given object. This method
   * is static and not destructive (except the result object).
   *
   * @param left
   *          left operand
   * @param right
   *          right operand
   * @param result
   *          object to receive the calculation result
   */
  public static void add(SignedInt128 left, SignedInt128 right,
      SignedInt128 result) {
    result.update(left);
    result.addDestructive(right);
  }

  /**
   * Calculates addition and stores the result into this object. This method is
   * destructive.
   *
   * @param right
   *          right operand
   */
  public void addDestructive(SignedInt128 right) {
    if (this.negative == right.negative) {
      this.mag.addDestructive(right.mag);
      if (this.mag.getV3() < 0) {
        SqlMathUtil.throwOverflowException();
      }
      return;
    }

    byte signum = UnsignedInt128.difference(this.mag, right.mag, this.mag);
    this.negative = (signum > 0 ? this.negative : right.negative);
  }

  /**
   * Calculates subtraction and puts the result into the given object. This
   * method is static and not destructive (except the result object).
   *
   * @param left
   *          left operand
   * @param right
   *          right operand
   * @param result
   *          object to receive the calculation result
   */
  public static void subtract(SignedInt128 left, SignedInt128 right,
      SignedInt128 result) {
    result.update(left);
    result.subtractDestructive(right);
  }

  /**
   * Calculates subtraction and stores the result into this object. This method
   * is destructive.
   *
   * @param right
   *          right operand
   */
  public void subtractDestructive(SignedInt128 right) {
    if (this.negative != right.negative) {
      this.mag.addDestructive(right.mag);
      if (this.mag.getV3() < 0) {
        SqlMathUtil.throwOverflowException();
      }
      return;
    }

    byte signum = UnsignedInt128.difference(this.mag, right.mag, this.mag);
    this.negative = (signum > 0 ? this.negative : !this.negative);
  }

  /**
   * Calculates multiplication and puts the result into the given object. This
   * method is static and not destructive (except the result object).
   *
   * @param left
   *          left operand
   * @param right
   *          right operand
   * @param result
   *          object to receive the calculation result
   */
  public static void multiply(SignedInt128 left, SignedInt128 right,
      SignedInt128 result) {
    if (result == left || result == right) {
      throw new IllegalArgumentException(
          "result object cannot be left or right operand");
    }

    result.update(left);
    result.multiplyDestructive(right);
  }

  /**
   * Performs multiplication.
   *
   * @param right
   *          right operand. this object is not modified.
   */
  public void multiplyDestructive(SignedInt128 right) {
    this.mag.multiplyDestructive(right.mag);
    this.negative = this.negative ^ right.negative;
    if (this.mag.getV3() < 0) {
      SqlMathUtil.throwOverflowException();
    }
  }

  /**
   * Performs multiplication.
   *
   * @param right
   *          right operand.
   */
  public void multiplyDestructive(int right) {
    if (right < 0) {
      this.mag.multiplyDestructive(-right);
      this.negative = !this.negative;
    } else {
      this.mag.multiplyDestructive(right);
    }
    if (this.mag.isZero()) {
      this.negative = false;
    }
    if (this.mag.getV3() < 0) {
      SqlMathUtil.throwOverflowException();
    }
  }

  /**
   * Divides this value with the given value. This version is destructive,
   * meaning it modifies this object.
   *
   * @param right
   *          the value to divide
   * @return remainder
   */
  public int divideDestructive(int right) {
    int ret;
    if (right < 0) {
      ret = this.mag.divideDestructive(-right);
      this.negative = !this.negative;
    } else {
      ret = this.mag.divideDestructive(right);
    }
    ret = ret & SqlMathUtil.FULLBITS_31;
    if (this.negative) {
      ret = -ret;
    }
    if (this.mag.isZero()) {
      this.negative = false;
    }
    return ret;
  }

  /**
   * Performs division and puts the quotient into the given object. This method
   * is static and not destructive (except the result object).
   *
   * @param left
   *          left operand
   * @param right
   *          right operand
   * @param quotient
   *          result object to receive the calculation result
   * @param remainder
   *          result object to receive the calculation result
   */
  public static void divide(SignedInt128 left, SignedInt128 right,
      SignedInt128 quotient, SignedInt128 remainder) {
    if (quotient == left || quotient == right) {
      throw new IllegalArgumentException(
          "result object cannot be left or right operand");
    }

    quotient.update(left);
    quotient.divideDestructive(right, remainder);
  }

  /**
   * Performs division and puts the quotient into this object.
   *
   * @param right
   *          right operand. this object is not modified.
   * @param remainder
   *          result object to receive the calculation result
   */
  public void divideDestructive(SignedInt128 right, SignedInt128 remainder) {
    this.mag.divideDestructive(right.mag, remainder.mag);
    remainder.negative = false; // remainder is always positive
    this.negative = this.negative ^ right.negative;
  }

  /**
   * Reverses the sign of this object. This method is destructive.
   */
  public void negateDestructive() {
    this.negative = !this.negative;
  }

  /**
   * Makes this object positive. This method is destructive.
   */
  public void absDestructive() {
    this.negative = false;
  }

  /**
   * This version returns the result as a new object, not modifying the give
   * objects.
   *
   * @param left
   *          left operand
   * @param result
   *          object to receive the calculation result
   */
  public static void negate(SignedInt128 left, SignedInt128 result) {
    result.update(left);
    result.negateDestructive();
  }

  /**
   * This version returns the result as a new object, not modifying the give
   * objects.
   *
   * @param left
   *          left operand
   * @param result
   *          object to receive the calculation result
   */
  public static void abs(SignedInt128 left, SignedInt128 result) {
    result.update(left);
    result.absDestructive();
  }

  /**
   * Adds one to this value. This version is destructive, meaning it modifies
   * this object.
   */
  public void incrementDestructive() {
    if (!this.negative) {
      if (this.mag.equals(SqlMathUtil.FULLBITS_32, SqlMathUtil.FULLBITS_32,
          SqlMathUtil.FULLBITS_32, SqlMathUtil.FULLBITS_31)) {
        SqlMathUtil.throwOverflowException();
      }
      this.mag.incrementDestructive();
      assert (this.mag.getV3() >= 0);
    } else {
      assert (!this.mag.isZero());
      this.mag.decrementDestructive();
      if (this.mag.isZero()) {
        this.negative = false;
      }
    }
  }

  /**
   * Subtracts one from this value. This version is destructive, meaning it
   * modifies this object.
   */
  public void decrementDestructive() {
    if (this.negative) {
      if (this.mag.equals(SqlMathUtil.FULLBITS_32, SqlMathUtil.FULLBITS_32,
          SqlMathUtil.FULLBITS_32, SqlMathUtil.FULLBITS_31)) {
        SqlMathUtil.throwOverflowException();
      }
      this.mag.incrementDestructive();
      assert (this.mag.getV3() >= 0);
    } else {
      if (this.mag.isZero()) {
        this.negative = true;
        this.mag.incrementDestructive();
      } else {
        this.mag.decrementDestructive();
      }
    }
  }

  /**
   * This version returns the result as a new object, not modifying the give
   * objects.
   *
   * @param left
   *          left operand
   * @param result
   *          object to receive the calculation result
   */
  public static void increment(SignedInt128 left, SignedInt128 result) {
    result.update(left);
    result.incrementDestructive();
  }

  /**
   * This version returns the result as a new object, not modifying the give
   * objects.
   *
   * @param left
   *          left operand
   * @param result
   *          object to receive the calculation result
   */
  public static void decrement(SignedInt128 left, SignedInt128 result) {
    result.update(left);
    result.decrementDestructive();
  }

  /**
   * Right-shift for the given number of bits. This bit-shift is equivalent to
   * Java's signed bit shift "&gt;&gt;". This version is destructive, meaning it
   * modifies this object. NOTE: So far we don't provide an equivalent of the
   * unsigned right bit shift "&gt;&gt;&gt;" because we recommend to simply use
   * {@link UnsignedInt128} for unsigned use.
   *
   * @param bits
   *          the number of bits. must be positive
   * @param roundUp
   *          whether to round up the most significant bit that was discarded
   */
  public void shiftRightDestructive(int bits, boolean roundUp) {
    this.mag.shiftRightDestructive(bits, roundUp);
    if (this.mag.isZero() && this.negative) {
      this.negative = false;
    }
  }

  /**
   * Left-shift for the given number of bits. This bit-shift is equivalent to
   * Java's signed bit shift "&lt;&lt;". This method does not throw an error
   * even if overflow happens. This version is destructive, meaning it modifies
   * this object.
   *
   * @param bits
   *          the number of bits. must be positive
   */
  public void shiftLeftDestructive(int bits) {
    this.mag.shiftLeftDestructive(bits);
    if (this.mag.getV3() < 0) {
      SqlMathUtil.throwOverflowException();
    }
    assert (this.mag.getV3() >= 0);
  }

  /**
   * Scale down the value for 10**tenScale (this := this / 10**tenScale). This
   * method rounds-up, eg 44/10=4, 45/10=5. This version is destructive, meaning
   * it modifies this object.
   *
   * @param tenScale
   *          scaling. must be positive
   */
  public void scaleDownTenDestructive(short tenScale) {
    this.mag.scaleDownTenDestructive(tenScale);
    if (this.mag.isZero() && this.negative) {
      this.negative = false;
    }
  }

  /**
   * Scale up the value for 10**tenScale (this := this * 10**tenScale). Scaling
   * up DOES throw an error when an overflow occurs. For example, 42.scaleUp(1)
   * = 420, 42.scaleUp(40) = ArithmeticException. This version is destructive,
   * meaning it modifies this object.
   *
   * @param tenScale
   *          scaling. must be positive
   */
  public void scaleUpTenDestructive(short tenScale) {
    this.mag.scaleUpTenDestructive(tenScale);
    if (this.mag.getV3() < 0) {
      SqlMathUtil.throwOverflowException();
    }
  }

  /**
   * This version returns the result as a new object, not modifying the give
   * objects.
   *
   * @param left
   *          left operand
   * @param result
   *          object to receive the calculation result
   * @param bits
   *          the number of bits. must be positive
   * @param roundUp
   *          whether to round up the most significant bit that was discarded
   */
  public static void shiftRight(SignedInt128 left, SignedInt128 result,
      int bits, boolean roundUp) {
    result.update(left);
    result.shiftRightDestructive(bits, roundUp);
  }

  /**
   * This version returns the result as a new object, not modifying the give
   * objects.
   *
   * @param left
   *          left operand
   * @param result
   *          object to receive the calculation result
   * @param bits
   *          the number of bits. must be positive
   */
  public static void shiftLeft(SignedInt128 left, SignedInt128 result, int bits) {
    result.update(left);
    result.shiftLeftDestructive(bits);
  }

  /**
   * This version returns the result as a new object, not modifying the give
   * objects.
   *
   * @param left
   *          left operand
   * @param result
   *          object to receive the calculation result
   * @param tenScale
   *          scaling. must be positive
   */
  public static void scaleDownTen(SignedInt128 left, SignedInt128 result,
      short tenScale) {
    result.update(left);
    result.scaleDownTenDestructive(tenScale);
  }

  /**
   * This version returns the result as a new object, not modifying the give
   * objects.
   *
   * @param left
   *          left operand
   * @param result
   *          object to receive the calculation result
   * @param tenScale
   *          scaling. must be positive
   */
  public static void scaleUpTen(SignedInt128 left, SignedInt128 result,
      short tenScale) {
    result.update(left);
    result.scaleUpTenDestructive(tenScale);
  }

  /**
   * Convert this object to {@link BigInteger}. Do not use this method in a
   * performance sensitive place.
   *
   * @return BigInteger to represent this object
   */
  public BigInteger toBigIntegerSlow() {
    BigInteger bigInt = this.mag.toBigIntegerSlow();
    return this.negative ? bigInt.negate() : bigInt;
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
    if (this.negative) {
      return "-" + this.mag.toFormalString();
    }
    return this.mag.toFormalString();
  }

  @Override
  public String toString() {
    return "SignedInt128 (" + (this.negative ? "negative" : "positive")
        + "). mag=" + this.mag.toString();
  }
}
