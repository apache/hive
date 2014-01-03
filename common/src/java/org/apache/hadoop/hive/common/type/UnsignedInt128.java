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
import java.util.Arrays;
import org.apache.hadoop.hive.common.type.SqlMathUtil;

/**
 * This code was originally written for Microsoft PolyBase.
 *
 * Represents an unsigned 128-bit integer. This is the basis for
 * {@link Decimal128} and {@link SignedInt128}. This object is much faster and
 * more compact than BigInteger, but has many limitations below.
 * <ul>
 * <li>Always consumes 128 bits (4 int32) even if the values is, say, 3.</li>
 * <li>Cannot handle values larger than 10**38.</li>
 * <li>Does not support some of arithmetic operations that is not required in
 * SQL (e.g., exact POWER/SQRT).</li>
 * </ul>
 */
public final class UnsignedInt128 implements Comparable<UnsignedInt128> {

  /** Number of ints to store this object. */
  public static final int INT_COUNT = 4;

  /** Number of bytes to store this object. */
  public static final int BYTE_SIZE = 4 * INT_COUNT;

  /** Can hold up to 10^38. */
  public static final int MAX_DIGITS = 38;

  /** Maximum value that can be represented in this class. */
  public static final UnsignedInt128 MAX_VALUE = new UnsignedInt128(0xFFFFFFFF,
      0xFFFFFFFF, 0xFFFFFFFF, 0xFFFFFFFF);

  /** Minimum value that can be represented in this class. */
  public static final UnsignedInt128 MIN_VALUE = new UnsignedInt128(0);

  /** A special value representing 10**38. */
  public static final UnsignedInt128 TEN_TO_THIRTYEIGHT = new UnsignedInt128(0,
      0x98a2240, 0x5a86c47a, 0x4b3b4ca8);

  /**
   * Int32 elements as little-endian (v[0] is least significant) unsigned
   * integers.
   */
  private final int[] v = new int[INT_COUNT];

  /**
   * Number of leading non-zero elements in {@link #v}. For example, if the
   * value of this object is 123 (v0=123, v1=v2=v3=0), then 1. 0 to 4. 0 means
   * that this object represents zero.
   *
   * @see #updateCount()
   */
  private transient byte count;

  /**
   * Determines the number of ints to store one value.
   *
   * @param precision
   *          precision (0-38)
   * @return the number of ints to store one value
   */
  public static int getIntsPerElement(int precision) {
    assert (precision >= 0 && precision <= 38);
    if (precision <= 9) {
      return 1;
    } else if (precision <= 19) {
      return 2;
    } else if (precision <= 28) {
      return 3;
    }
    return 4;
  }

  /** Creates an instance that represents zero. */
  public UnsignedInt128() {
    zeroClear();
  }

  /**
   * Copy constructor.
   *
   * @param o
   *          The instance to copy from
   */
  public UnsignedInt128(UnsignedInt128 o) {
    update(o);
  }

  /**
   * Creates an instance that has the given values.
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
  public UnsignedInt128(int v0, int v1, int v2, int v3) {
    this.v[0] = v0;
    this.v[1] = v1;
    this.v[2] = v2;
    this.v[3] = v3;
    updateCount();
  }

  /**
   * Constructs from the given long value.
   *
   * @param v
   *          long value
   */
  public UnsignedInt128(long v) {
    update(v);
  }

  /**
   * Constructs from the given string.
   *
   * @param str
   *          string
   */
  public UnsignedInt128(String str) {
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
  public UnsignedInt128(char[] str, int offset, int length) {
    update(str, offset, length);
  }

  /** @return v[0] */
  public int getV0() {
    return v[0];
  }

  /** @return v[1] */
  public int getV1() {
    return v[1];
  }

  /** @return v[2] */
  public int getV2() {
    return v[2];
  }

  /** @return v[3] */
  public int getV3() {
    return v[3];
  }

  /**
   * Setter for v0.
   *
   * @param val
   *          value to set
   */
  public void setV0(int val) {
    v[0] = val;
    updateCount();
  }

  /**
   * Setter for v1.
   *
   * @param val
   *          value to set
   */
  public void setV1(int val) {
    v[1] = val;
    updateCount();
  }

  /**
   * Setter for v2.
   *
   * @param val
   *          value to set
   */
  public void setV2(int val) {
    v[2] = val;
    updateCount();
  }

  /**
   * Setter for v3.
   *
   * @param val
   *          value to set
   */
  public void setV3(int val) {
    v[3] = val;
    updateCount();
  }

  /**
   * Returns if we overflowed 10**38, but not 2**128. This code is equivalent to
   * CSsNumeric::FGt10_38 in SQLServer (numeric.cpp). However, be aware that the
   * elements are signed ints, not UI4 in SQLServer.
   *
   * @return whether this value is equal to or larger than 10**38
   */
  public boolean exceedsTenToThirtyEight() {

    // 10**38=
    // v[0]=0(0),v[1]=160047680(98a2240),v[2]=1518781562(5a86c47a),v[3]=1262177448(4b3b4ca8)

    // check most significant part first
    if (v[3] != 0x4b3b4ca8) {
      return (v[3] < 0 || v[3] > 0x4b3b4ca8);
    }

    // check second most significant part
    if (v[2] != 0x5a86c47a) {
      return (v[2] < 0 || v[2] > 0x5a86c47a);
    }

    return (v[1] < 0 || v[1] > 0x098a2240);
  }

  /**
   * Used to check overflows. This is NOT used in {@link UnsignedInt128} itself
   * because this overflow semantics is Decimal's. (throws - but not a checked
   * exception) ArithmeticException if this value is equal to or exceed 10**38.
   */
  public void throwIfExceedsTenToThirtyEight() {
    if (exceedsTenToThirtyEight()) {
      SqlMathUtil.throwOverflowException();
    }
  }

  /**
   * Returns the value of this object as long, throwing error if the value
   * exceeds long.
   *
   * @return the value this object represents
   */
  public long asLong() {
    if (this.count > 2 || v[1] < 0) {
      SqlMathUtil.throwOverflowException();
    }
    return (((long) v[1]) << 32L) | v[0];
  }

  /** Make the value to zero. */
  public void zeroClear() {
    this.v[0] = 0;
    this.v[1] = 0;
    this.v[2] = 0;
    this.v[3] = 0;
    this.count = 0;
  }

  /** @return whether the value is zero */
  public boolean isZero() {
    return this.count == 0;
  }

  /** @return whether the value is one */
  public boolean isOne() {
    return this.v[0] == 1 && this.count == 1;
  }

  /** @return whether 32bits int is enough to represent this value */
  public boolean fitsInt32() {
    return this.count <= 1;
  }

  /**
   * Copy from the given object.
   *
   * @param o
   *          The instance to copy from
   */
  public void update(UnsignedInt128 o) {
    update(o.v[0], o.v[1], o.v[2], o.v[3]);
  }

  /**
   * Updates the value of this object with the given long value.
   *
   * @param v
   *          long value
   */
  public void update(long v) {
    assert (v >= 0);
    update((int) v, (int) (v >> 32), 0, 0);
  }

  /**
   * Updates the value of this object with the given values.
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
  public void update(int v0, int v1, int v2, int v3) {
    this.v[0] = v0;
    this.v[1] = v1;
    this.v[2] = v2;
    this.v[3] = v3;
    updateCount();
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
  public void update(IntBuffer buf, int precision) {
    switch (getIntsPerElement(precision)) {
    case 1:
      update32(buf);
      break;
    case 2:
      update64(buf);
      break;
    case 3:
      update96(buf);
      break;
    case 4:
      update128(buf);
      break;
    default:
      throw new RuntimeException();
    }
  }

  /**
   * Updates the value of this object by reading from ByteBuffer, receiving 128
   * bits data (full ranges).
   *
   * @param buf
   *          ByteBuffer to read values from
   */
  public void update128(IntBuffer buf) {
    buf.get(v, 0, INT_COUNT);
    updateCount();
  }

  /**
   * Updates the value of this object by reading from ByteBuffer, receiving only
   * 96 bits data.
   *
   * @param buf
   *          ByteBuffer to read values from
   */
  public void update96(IntBuffer buf) {
    buf.get(v, 0, 3);
    v[3] = 0;
    updateCount();
  }

  /**
   * Updates the value of this object by reading from ByteBuffer, receiving only
   * 64 bits data.
   *
   * @param buf
   *          ByteBuffer to read values from
   */
  public void update64(IntBuffer buf) {
    buf.get(v, 0, 2);
    v[2] = 0;
    v[3] = 0;
    updateCount();
  }

  /**
   * Updates the value of this object by reading from ByteBuffer, receiving only
   * 32 bits data.
   *
   * @param buf
   *          ByteBuffer to read values from
   */
  public void update32(IntBuffer buf) {
    v[0] = buf.get();
    v[1] = 0;
    v[2] = 0;
    v[3] = 0;
    updateCount();
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
  public void update(int[] array, int offset, int precision) {
    switch (getIntsPerElement(precision)) {
    case 1:
      update32(array, offset);
      break;
    case 2:
      update64(array, offset);
      break;
    case 3:
      update96(array, offset);
      break;
    case 4:
      update128(array, offset);
      break;
    default:
      throw new RuntimeException();
    }
  }

  /**
   * Updates the value of this object by reading from the given array, receiving
   * 128 bits data (full ranges).
   *
   * @param array
   *          array to read values from
   * @param offset
   *          offset of the long array
   */
  public void update128(int[] array, int offset) {
    System.arraycopy(array, offset, v, 0, 4);
    updateCount();
  }

  /**
   * Updates the value of this object by reading from the given array, receiving
   * only 96 bits data.
   *
   * @param array
   *          array to read values from
   * @param offset
   *          offset of the long array
   */
  public void update96(int[] array, int offset) {
    System.arraycopy(array, offset, v, 0, 3);
    v[3] = 0;
    updateCount();
  }

  /**
   * Updates the value of this object by reading from the given array, receiving
   * only 64 bits data.
   *
   * @param array
   *          array to read values from
   * @param offset
   *          offset of the long array
   */
  public void update64(int[] array, int offset) {
    System.arraycopy(array, offset, v, 0, 2);
    v[2] = 0;
    v[3] = 0;
    updateCount();
  }

  /**
   * Updates the value of this object by reading from the given array, receiving
   * only 32 bits data.
   *
   * @param array
   *          array to read values from
   * @param offset
   *          offset of the long array
   */
  public void update32(int[] array, int offset) {
    v[0] = array[offset];
    v[1] = 0;
    v[2] = 0;
    v[3] = 0;
    updateCount();
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

    // Skip leading zeros and compute number of digits in magnitude
    final int end = offset + length;
    assert (end <= str.length);
    int cursor = offset;
    while (cursor < end && str[cursor] == '0') {
      ++cursor;
    }

    if (cursor == end) {
      zeroClear();
      return;
    }
    if (end - cursor > MAX_DIGITS) {
      SqlMathUtil.throwOverflowException();
    }

    int accumulated = 0;
    int accumulatedCount = 0;
    while (cursor < end) {
      if (str[cursor] < '0' || str[cursor] > '9') {
        throw new NumberFormatException("Invalid string:"
            + new String(str, offset, length));
      }

      if (accumulatedCount == 9) {
        scaleUpTenDestructive((short) accumulatedCount);
        addDestructive(accumulated);
        accumulated = 0;
        accumulatedCount = 0;
      }
      int digit = str[cursor] - '0';
      accumulated = accumulated * 10 + digit;
      ++accumulatedCount;
      ++cursor;
    }

    if (accumulatedCount > 0) {
      scaleUpTenDestructive((short) accumulatedCount);
      addDestructive(accumulated);
    }
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
    buf.put(v, 0, getIntsPerElement(precision));
  }

  /**
   * Serialize this object to the given ByteBuffer, putting 128 bits data (full
   * ranges).
   *
   * @param buf
   *          ByteBuffer to write values to
   */
  public void serializeTo128(IntBuffer buf) {
    buf.put(v, 0, 4);
  }

  /**
   * Serialize this object to the given ByteBuffer, putting only 96 bits data.
   *
   * @param buf
   *          ByteBuffer to write values to
   */
  public void serializeTo96(IntBuffer buf) {
    assert (v[3] == 0);
    buf.put(v, 0, 3);
  }

  /**
   * Serialize this object to the given ByteBuffer, putting only 64 bits data.
   *
   * @param buf
   *          ByteBuffer to write values to
   */
  public void serializeTo64(IntBuffer buf) {
    assert (v[2] == 0);
    assert (v[3] == 0);
    buf.put(v, 0, 2);
  }

  /**
   * Serialize this object to the given ByteBuffer, putting only 32 bits data.
   *
   * @param buf
   *          ByteBuffer to write values to
   */
  public void serializeTo32(IntBuffer buf) {
    assert (v[1] == 0);
    assert (v[2] == 0);
    assert (v[3] == 0);
    buf.put(v[0]);
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
    System.arraycopy(v, 0, array, offset, getIntsPerElement(precision));
  }

  /**
   * Serialize this object to the given array, putting 128 bits data (full
   * ranges).
   *
   * @param array
   *          array to write values to
   * @param offset
   *          offset of the int array
   */
  public void serializeTo128(int[] array, int offset) {
    System.arraycopy(v, 0, array, offset, 4);
  }

  /**
   * Serialize this object to the given array, putting only 96 bits data.
   *
   * @param array
   *          array to write values to
   * @param offset
   *          offset of the int array
   */
  public void serializeTo96(int[] array, int offset) {
    assert (v[3] == 0);
    System.arraycopy(v, 0, array, offset, 3);
  }

  /**
   * Serialize this object to the given array, putting only 64 bits data.
   *
   * @param array
   *          array to write values to
   * @param offset
   *          offset of the int array
   */
  public void serializeTo64(int[] array, int offset) {
    assert (v[2] == 0);
    assert (v[3] == 0);
    System.arraycopy(v, 0, array, offset, 2);
  }

  /**
   * Serialize this object to the given array, putting only 32 bits data.
   *
   * @param array
   *          array to write values to
   * @param offset
   *          offset of the int array
   */
  public void serializeTo32(int[] array, int offset) {
    assert (v[1] == 0);
    assert (v[2] == 0);
    assert (v[3] == 0);
    array[0] = v[0];
  }

  @Override
  public int compareTo(UnsignedInt128 o) {
    return compareTo(o.v);
  }

  /**
   * @see #compareTo(UnsignedInt128)
   * @param o
   *          the object to be compared.
   * @return a negative integer, zero, or a positive integer as this object is
   *         less than, equal to, or greater than the specified object.
   */
  public int compareTo(int[] o) {
    return compareTo(o[0], o[1], o[2], o[3]);
  }

  /**
   * @see #compareTo(UnsignedInt128)
   * @param o0
   *          o0
   * @param o1
   *          o1
   * @param o2
   *          o2
   * @param o3
   *          o3
   * @return a negative integer, zero, or a positive integer as this object is
   *         less than, equal to, or greater than the specified object.
   */
  public int compareTo(int o0, int o1, int o2, int o3) {
    if (v[3] != o3) {
      return SqlMathUtil.compareUnsignedInt(v[3], o3);
    } else if (v[2] != o2) {
      return SqlMathUtil.compareUnsignedInt(v[2], o2);
    } else if (v[1] != o1) {
      return SqlMathUtil.compareUnsignedInt(v[1], o1);
    } else {
      return SqlMathUtil.compareUnsignedInt(v[0], o0);
    }
  }

  /**
   * Compares with the given object after scaling up/down it for 10**scaleUp.
   * This method is not destructive. Used from {@link Decimal128}.
   *
   * @param o
   *          the object to compare with
   * @param tenScale
   *          power of 10 to scale up (if positive) or down (if negative) the
   *          given object.
   * @return a negative integer, zero, or a positive integer as this object is
   *         less than, equal to, or greater than the specified object.
   */
  public int compareToScaleTen(UnsignedInt128 o, short tenScale) {

    // easier case. take a quick path
    if (tenScale == 0) {
      return compareTo(o);
    }

    // if o is zero, easy.
    if (o.isZero()) {
      return this.isZero() ? 0 : 1;
    } else if (this.isZero()) {

      // if this is zero, we need to check if o might become zero after
      // scaling down.
      // this is not easy because there might be rounding.
      if (tenScale > 0) {

        // scaling up, so o is definitely larger
        return -1;
      }
      if (tenScale < -SqlMathUtil.MAX_POWER_TEN_INT128) {

        // any value will become zero. even no possibility of rounding
        return 0;
      } else {

        // compare with 5 * 10**-tenScale
        // example: tenScale=-1. o will be zero after scaling if o>=5.
        boolean oZero = o
            .compareTo(SqlMathUtil.ROUND_POWER_TENS_INT128[-tenScale]) < 0;
        return oZero ? 0 : -1;
      }
    }

    // another quick path
    if (this.fitsInt32() && o.fitsInt32()
        && tenScale <= SqlMathUtil.MAX_POWER_TEN_INT31) {
      long v0Long = this.v[0] & SqlMathUtil.LONG_MASK;
      long o0;
      if (tenScale < 0) {
        if (tenScale < -SqlMathUtil.MAX_POWER_TEN_INT31) {

          // this scales down o.v[0] to 0 because 2^32 = 4.2E9. No
          // possibility of rounding.
          o0 = 0L;
        } else {

          // divide by 10**-tenScale. check for rounding.
          o0 = (o.v[0] & SqlMathUtil.LONG_MASK)
              / SqlMathUtil.POWER_TENS_INT31[-tenScale];
          long remainder = (o.v[0] & SqlMathUtil.LONG_MASK)
              % SqlMathUtil.POWER_TENS_INT31[-tenScale];
          if (remainder >= SqlMathUtil.ROUND_POWER_TENS_INT31[-tenScale]) {
            assert (o0 >= 0);
            ++o0; // this is safe because o0 is positive
          }
        }
      } else {

        // tenScale <= SqlMathUtil.MAX_POWER_TEN_INT31
        // so, we can make this as a long comparison
        o0 = (o.v[0] & SqlMathUtil.LONG_MASK)
            * (SqlMathUtil.POWER_TENS_INT31[tenScale] & SqlMathUtil.LONG_MASK);
      }
      return SqlMathUtil.compareUnsignedLong(v0Long, o0);
    }

    // unfortunately no quick path. let's do scale up/down
    int[] ov = o.v.clone();
    if (tenScale < 0) {

      // scale down. does rounding
      scaleDownTenArray4RoundUp(ov, (short) -tenScale);
    } else {

      // scale up
      boolean overflow = scaleUpTenArray(ov, tenScale);
      if (overflow) {

        // overflow is not an error here. it just means "this" is
        // smaller
        return -1;
      }
    }

    return compareTo(ov);
  }

  @Override
  public int hashCode() {

    // note: v[0] ^ v[1] ^ v[2] ^ v[3] would cause too many hash collisions
    return (v[0] * 0x2AB19E23) + (v[1] * 0x4918EACB) + (v[2] * 0xF03051A7)
        + v[3];
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof UnsignedInt128)) {
      return false;
    }
    return equals((UnsignedInt128) obj);
  }

  /**
   * Specialized version.
   *
   * @see #equals(Object)
   * @param o
   *          the object to compare with
   * @return whether this object is equal to the given object
   */
  public boolean equals(UnsignedInt128 o) {
    return this.v[0] == o.v[0] && this.v[1] == o.v[1] && this.v[2] == o.v[2]
        && this.v[3] == o.v[3];
  }

  /**
   * Specialized version.
   *
   * @see #equals(Object)
   * @param o0
   *          o0
   * @param o1
   *          o1
   * @param o2
   *          o2
   * @param o3
   *          o3
   * @return whether this object is equal to the given object
   */
  public boolean equals(int o0, int o1, int o2, int o3) {
    return this.v[0] == o0 && this.v[1] == o1 && this.v[2] == o2
        && this.v[3] == o3;
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return new UnsignedInt128(this);
  }

  /**
   * Convert this object to {@link BigInteger}. Do not use this method in a
   * performance sensitive place.
   *
   * @return BigInteger to represent this object
   */
  public BigInteger toBigIntegerSlow() {
    BigInteger bigInt = BigInteger.valueOf(v[3] & SqlMathUtil.LONG_MASK);
    bigInt = bigInt.shiftLeft(32);
    bigInt = bigInt.add(BigInteger.valueOf(v[2] & SqlMathUtil.LONG_MASK));
    bigInt = bigInt.shiftLeft(32);
    bigInt = bigInt.add(BigInteger.valueOf(v[1] & SqlMathUtil.LONG_MASK));
    bigInt = bigInt.shiftLeft(32);
    bigInt = bigInt.add(BigInteger.valueOf(v[0] & SqlMathUtil.LONG_MASK));
    return bigInt;
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
    char[] buf = new char[MAX_DIGITS + 1];
    int bufCount = 0;
    int nonZeroBufCount = 0;

    final int tenScale = SqlMathUtil.MAX_POWER_TEN_INT31;
    final int tenPower = SqlMathUtil.POWER_TENS_INT31[tenScale];
    UnsignedInt128 tmp = new UnsignedInt128(this);

    while (!tmp.isZero()) {
      int remainder = tmp.divideDestructive(tenPower);
      for (int i = 0; i < tenScale && bufCount < buf.length; ++i) {
        int digit = remainder % 10;
        remainder /= 10;
        buf[bufCount] = (char) (digit + '0');
        ++bufCount;
        if (digit != 0) {
          nonZeroBufCount = bufCount;
        }
      }
    }

    if (bufCount == 0) {
      return "0";
    } else {
      char[] reversed = new char[nonZeroBufCount];
      for (int i = 0; i < nonZeroBufCount; ++i) {
        reversed[i] = buf[nonZeroBufCount - i - 1];
      }
      return new String(reversed);
    }
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    str.append("Int128: count=" + count + ",");
    str.append("v[0]=" + v[0] + "(0x" + Integer.toHexString(v[0]) + "), ");
    str.append("v[1]=" + v[1] + "(0x" + Integer.toHexString(v[1]) + "), ");
    str.append("v[2]=" + v[2] + "(0x" + Integer.toHexString(v[2]) + "), ");
    str.append("v[3]=" + v[3] + "(0x" + Integer.toHexString(v[3]) + "), ");
    str.append("BigInteger#toString=" + toBigIntegerSlow().toString());
    return new String(str);
  }

  /**
   * Adds the given value to this value. This version is destructive, meaning it
   * modifies this object.
   *
   * @param right
   *          the value to add
   */
  public void addDestructive(UnsignedInt128 right) {
    addDestructive(right.v);
  }

  /**
   * Adds the given value to this value. This version is destructive, meaning it
   * modifies this object.
   *
   * @param r
   *          the value to add
   */
  public void addDestructive(int[] r) {
    long sum = 0L;
    for (int i = 0; i < INT_COUNT; ++i) {
      sum = (this.v[i] & SqlMathUtil.LONG_MASK)
          + (r[i] & SqlMathUtil.LONG_MASK) + (sum >>> 32);
      this.v[i] = (int) sum;
    }
    updateCount();

    if ((sum >> 32) != 0) {
      SqlMathUtil.throwOverflowException();
    }
  }

  /**
   * Adds the given value to this value. This version is destructive, meaning it
   * modifies this object.
   *
   * @param r
   *          the value to add
   */
  public void addDestructive(int r) {
    if ((this.v[0] & SqlMathUtil.LONG_MASK) + (r & SqlMathUtil.LONG_MASK) >= (1L << 32L)) {
      this.v[0] += r;

      if (this.v[1] == SqlMathUtil.FULLBITS_32) {
        this.v[1] = 0;
        if (this.v[2] == SqlMathUtil.FULLBITS_32) {
          this.v[2] = 0;
          if (this.v[3] == SqlMathUtil.FULLBITS_32) {
            SqlMathUtil.throwOverflowException();
          } else {
            ++this.v[3];
          }
        } else {
          ++this.v[2];
        }
      } else {
        ++this.v[1];
      }
    } else {
      this.v[0] += r;
    }
    updateCount();
  }

  /**
   * Adds one to this value. This version is destructive, meaning it modifies
   * this object.
   */
  public void incrementDestructive() {
    incrementArray(v);
    updateCount();
  }

  /**
   * Subtracts one from this value. This version is destructive, meaning it
   * modifies this object.
   */
  public void decrementDestructive() {
    decrementArray(v);
    updateCount();
  }

  /**
   * Adds the given value after scaling to this value. this := this + (right *
   * 10**tenScale). This version is destructive, meaning it modifies this
   * object.
   *
   * @param right
   *          the value to add
   * @param tenScale
   *          number of ten-based scaling. could be either positive or negative.
   */
  public void addDestructiveScaleTen(UnsignedInt128 right, short tenScale) {
    if (tenScale == 0) {
      addDestructive(right);
      return;
    }

    // scale up/down
    final int[] r = right.v.clone();
    if (tenScale < 0) {

      // scale down
      scaleDownTenArray4RoundUp(r, (short) -tenScale);
    } else if (tenScale > 0) {

      // scale up
      boolean overflow = scaleUpTenArray(r, tenScale);
      if (overflow) {
        SqlMathUtil.throwOverflowException();
      }
    }

    addDestructive(r);
  }

  /**
   * Subtracts the given value from this value. In other words, this := this -
   * right. This method will throw overflow exception if right operand is larger
   * than this value. This version is destructive, meaning it modifies this
   * object.
   *
   * @param right
   *          the value to subtract
   */
  public void subtractDestructive(UnsignedInt128 right) {
    subtractDestructive(right.v);
  }

  /**
   * Subtracts the given value from this value. In other words, this := this -
   * right. This method doesn't work if right operand is larger than this value.
   * This version is destructive, meaning it modifies this object.
   *
   * @param r
   *          the value to subtract
   */
  public void subtractDestructive(int[] r) {
    long sum = 0L;
    for (int i = 0; i < INT_COUNT; ++i) {
      sum = (this.v[i] & SqlMathUtil.LONG_MASK)
          - (r[i] & SqlMathUtil.LONG_MASK) - ((int) -(sum >> 32));
      this.v[i] = (int) sum;
    }
    updateCount();

    if ((sum >> 32) != 0) {
      SqlMathUtil.throwOverflowException();
    }
  }

  /**
   * Calculates absolute difference (remember that this is unsigned) of left and
   * right operator. <code>result := abs (left - right)</code> This is the core
   * implementation of subtract and signed add.
   *
   * @param left
   *          left operand
   * @param right
   *          right operand
   * @param result
   *          the object to receive the result. can be same object as left or
   *          right.
   * @return signum of the result. -1 if left was smaller than right, 1 if
   *         larger, 0 if same (result is zero).
   */
  public static byte difference(UnsignedInt128 left, UnsignedInt128 right,
      UnsignedInt128 result) {
    return differenceInternal(left, right.v, result);
  }

  /**
   * Calculates absolute difference of left and right operator after ten-based
   * scaling on right.
   * <code>result := abs (left - (right * 10**tenScale))</code> This is the core
   * implementation of subtract.
   *
   * @param left
   *          left operand
   * @param right
   *          right operand
   * @param result
   *          the object to receive the result. can be same object as left or
   *          right.
   * @param tenScale
   *          number of ten-based scaling. could be either positive or negative.
   * @return signum of the result. -1 if left was smaller than right, 1 if
   *         larger, 0 if same (result is zero).
   */
  public static byte differenceScaleTen(UnsignedInt128 left,
      UnsignedInt128 right, UnsignedInt128 result, short tenScale) {
    if (tenScale == 0) {
      return difference(left, right, result);
    }

    // scale up/down
    int[] r = right.v.clone();
    if (tenScale < 0) {
      // scale down
      scaleDownTenArray4RoundUp(r, (short) -tenScale);
    } else {
      // scale up
      boolean overflow = scaleUpTenArray(r, tenScale);
      if (overflow) {
        SqlMathUtil.throwOverflowException();
      }
    }

    return differenceInternal(left, r, result);
  }

  /**
   * Multiplies this value with the given integer value. This version is
   * destructive, meaning it modifies this object.
   *
   * @param right
   *          the value to multiply
   */
  public void multiplyDestructive(int right) {
    if (right == 0) {
      zeroClear();
      return;
    } else if (right == 1) {
      return;
    }

    long sum = 0L;
    long rightUnsigned = right & SqlMathUtil.LONG_MASK;
    for (int i = 0; i < INT_COUNT; ++i) {
      sum = (this.v[i] & SqlMathUtil.LONG_MASK) * rightUnsigned + (sum >>> 32);
      this.v[i] = (int) sum;
    }
    updateCount();

    if ((sum >> 32) != 0) {
      SqlMathUtil.throwOverflowException();
    }
  }

  /**
   * Multiplies this value with the given value. This version is destructive,
   * meaning it modifies this object.
   *
   * @param right
   *          the value to multiply
   */
  public void multiplyDestructive(UnsignedInt128 right) {
    if (this.fitsInt32() && right.fitsInt32()) {
      multiplyDestructiveFitsInt32(right, (short) 0, (short) 0);
      return;
    }
    multiplyArrays4And4To4NoOverflow(this.v, right.v);
    updateCount();
  }

  /**
   * Multiplies this value with the given value, followed by right bit shifts to
   * scale it back to this object. This is used from division. This version is
   * destructive, meaning it modifies this object.
   *
   * @param right
   *          the value to multiply
   * @param rightShifts
   *          the number of right-shifts after multiplication
   */
  public void multiplyShiftDestructive(UnsignedInt128 right, short rightShifts) {
    if (this.fitsInt32() && right.fitsInt32()) {
      multiplyDestructiveFitsInt32(right, rightShifts, (short) 0);
      return;
    }

    int[] z = multiplyArrays4And4To8(this.v, right.v);
    shiftRightArray(rightShifts, z, this.v, true);
    updateCount();
  }

  /**
   * Multiply this value with the given value, followed by ten-based scale down.
   * This method does the two operations without cutting off, so it preserves
   * accuracy without throwing a wrong overflow exception.
   *
   * @param right
   *          right operand
   * @param tenScale
   *          distance to scale down
   */
  public void multiplyScaleDownTenDestructive(UnsignedInt128 right,
      short tenScale) {
    assert (tenScale >= 0);
    if (this.fitsInt32() && right.fitsInt32()) {
      multiplyDestructiveFitsInt32(right, (short) 0, tenScale);
      return;
    }
    int[] z = multiplyArrays4And4To8(this.v, right.v);

    // Then, scale back.
    scaleDownTenArray8RoundUp(z, tenScale);
    update(z[0], z[1], z[2], z[3]);
  }

  /**
   * Divides this value with the given value. This version is destructive,
   * meaning it modifies this object.
   *
   * @param right
   *          the value to divide
   * @param remainder
   *          object to receive remainder
   */
  public void divideDestructive(UnsignedInt128 right, UnsignedInt128 remainder) {
    if (right.isZero()) {
      assert (right.isZero());
      SqlMathUtil.throwZeroDivisionException();
    }
    if (right.count == 1) {
      assert (right.v[1] == 0);
      assert (right.v[2] == 0);
      assert (right.v[3] == 0);
      int rem = divideDestructive(right.v[0]);
      remainder.update(rem);
      return;
    }

    int[] quotient = new int[5];
    int[] rem = SqlMathUtil.divideMultiPrecision(this.v, right.v, quotient);

    update(quotient[0], quotient[1], quotient[2], quotient[3]);
    remainder.update(rem[0], rem[1], rem[2], rem[3]);
  }

  /**
   * Scale up this object for 10**tenScale and then divides this value with the
   * given value. This version is destructive, meaning it modifies this object.
   *
   * @param right
   *          the value to divide
   * @param tenScale
   *          ten-based scale up distance
   * @param remainder
   *          object to receive remainder
   */
  public void divideScaleUpTenDestructive(UnsignedInt128 right, short tenScale,
      UnsignedInt128 remainder) {
    // in this case, we have to scale up _BEFORE_ division. otherwise we
    // might lose precision.
    if (tenScale > SqlMathUtil.MAX_POWER_TEN_INT128) {
      // in this case, the result will be surely more than 128 bit even
      // after division
      SqlMathUtil.throwOverflowException();
    }
    int[] scaledUp = multiplyConstructive256(SqlMathUtil.POWER_TENS_INT128[tenScale]);
    int[] quotient = new int[5];
    int[] rem = SqlMathUtil.divideMultiPrecision(scaledUp, right.v, quotient);
    update(quotient[0], quotient[1], quotient[2], quotient[3]);
    remainder.update(rem[0], rem[1], rem[2], rem[3]);
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
    assert (right >= 0);
    long rightUnsigned = right & SqlMathUtil.LONG_MASK;

    long quotient;
    long remainder = 0;

    for (int i = INT_COUNT - 1; i >= 0; --i) {
      remainder = ((this.v[i] & SqlMathUtil.LONG_MASK) + (remainder << 32));
      quotient = remainder / rightUnsigned;
      remainder %= rightUnsigned;
      this.v[i] = (int) quotient;
    }
    updateCount();
    return (int) remainder;
  }

  /**
   * Right-shift for the given number of bits. This version is destructive,
   * meaning it modifies this object.
   *
   * @param bits
   *          the number of bits. must be positive
   * @param roundUp
   *          whether to round up the most significant bit that was discarded
   */
  public void shiftRightDestructive(int bits, boolean roundUp) {
    assert (bits >= 0);
    shiftRightDestructive(bits / 32, bits % 32, roundUp);
  }

  /**
   * Left-shift for the given number of bits. This method does not throw an
   * error even if overflow happens. This version is destructive, meaning it
   * modifies this object.
   *
   * @param bits
   *          the number of bits. must be positive
   */
  public void shiftLeftDestructive(int bits) {
    assert (bits >= 0);
    shiftLeftDestructive(bits / 32, bits % 32);
  }

  /**
   * Left-shift for the given number of bits. This method throws an error even
   * if overflow happens. This version is destructive, meaning it modifies this
   * object.
   *
   * @param bits
   *          the number of bits. must be positive
   */
  public void shiftLeftDestructiveCheckOverflow(int bits) {
    if (bitLength() + bits >= 128) {
      SqlMathUtil.throwOverflowException();
    }
    shiftLeftDestructive(bits);
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
    if (tenScale == 0) {
      return;
    }

    if (tenScale < 0) {
      throw new IllegalArgumentException();
    }

    if (isZero()) {
      return;
    }

    scaleDownTenArray4RoundUp(v, tenScale);
    updateCount();
  }

  /**
   * Scale down the value for 5**tenScale (this := this / 5**tenScale). This
   * method rounds-up, eg 42/5=8, 43/5=9. This version is destructive, meaning
   * it modifies this object.
   *
   * @param fiveScale
   *          scaling. must be positive
   */
  public void scaleDownFiveDestructive(short fiveScale) {
    if (fiveScale == 0) {
      return;
    }

    if (fiveScale < 0) {
      throw new IllegalArgumentException();
    }

    if (isZero()) {
      return;
    }

    scaleDownFiveArrayRoundUp(v, fiveScale);
    updateCount();
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
    if (tenScale == 0) {
      return;
    }

    if (tenScale < 0) {
      throw new IllegalArgumentException();
    }

    if (isZero()) {
      return;
    }

    // First, scale up with 2. Check overflow
    shiftLeftDestructiveCheckOverflow(tenScale);

    // Then, scale up with 5
    scaleUpFiveDestructive(tenScale);
  }

  /**
   * Scale up the value for 5**tenScale (this := this * 5**tenScale). Scaling up
   * DOES throw an error when an overflow occurs. This version is destructive,
   * meaning it modifies this object.
   *
   * @param fiveScale
   *          scaling. must be positive
   */
  public void scaleUpFiveDestructive(short fiveScale) {
    if (fiveScale == 0) {
      return;
    }

    if (fiveScale < 0) {
      throw new IllegalArgumentException();
    }

    if (isZero()) {
      return;
    }

    // Scale up with 5. This is done via #multiplyDestructive(int).
    while (fiveScale > 0) {
      int powerFive = Math.min(fiveScale, SqlMathUtil.MAX_POWER_FIVE_INT31);
      multiplyDestructive(SqlMathUtil.POWER_FIVES_INT31[powerFive]);
      fiveScale -= powerFive;
    }
  }

  /**
   * This version returns the result as a new object, not modifying the give
   * objects.
   *
   * @param right
   *          right operand
   * @return operation result as a new object
   */
  public UnsignedInt128 addConstructive(UnsignedInt128 right) {
    UnsignedInt128 ret = new UnsignedInt128(this);
    ret.addDestructive(right);
    return ret;
  }

  /**
   * This version returns the result as a new object, not modifying the give
   * objects.
   *
   * @return operation result as a new object
   */
  public UnsignedInt128 incrementConstructive() {
    UnsignedInt128 ret = new UnsignedInt128(this);
    ret.incrementDestructive();
    return ret;
  }

  /**
   * This version returns the result as a new object, not modifying the give
   * objects. This method doesn't work if right operand is larger than this
   * value.
   *
   * @param right
   *          right operand
   * @return operation result as a new object
   */
  public UnsignedInt128 subtractConstructive(UnsignedInt128 right) {
    UnsignedInt128 ret = new UnsignedInt128(this);
    ret.subtractDestructive(right);
    return ret;
  }

  /**
   * This version returns the result as a new object, not modifying the give
   * objects. This method doesn't work if right operand is larger than this
   * value.
   *
   * @return operation result as a new object
   */
  public UnsignedInt128 decrementConstructive() {
    UnsignedInt128 ret = new UnsignedInt128(this);
    ret.decrementDestructive();
    return ret;
  }

  /**
   * This version returns the result as a new object, not modifying the give
   * objects.
   *
   * @param right
   *          right operand
   * @return operation result as a new object
   */
  public UnsignedInt128 multiplyConstructive(int right) {
    UnsignedInt128 ret = new UnsignedInt128(this);
    ret.multiplyDestructive(right);
    return ret;
  }

  /**
   * This version returns the result as a new object, not modifying the give
   * objects.
   *
   * @param right
   *          right operand
   * @return operation result as a new object
   */
  public UnsignedInt128 multiplyConstructive(UnsignedInt128 right) {
    UnsignedInt128 ret = new UnsignedInt128(this);
    ret.multiplyDestructive(right);
    return ret;
  }

  /**
   * This version returns the result of multiplication as 256bit data.
   *
   * @param right
   *          right operand
   * @return operation result as 256bit data
   */
  public int[] multiplyConstructive256(UnsignedInt128 right) {
    return multiplyArrays4And4To8(this.v, right.v);
  }

  /**
   * This version returns the result as a new object, not modifying the give
   * objects. Note that this method cannot receive remainder. Use destructive
   * version for it.
   *
   * @param right
   *          right operand
   * @return operation result as a new object
   */
  public UnsignedInt128 divideConstructive(int right) {
    UnsignedInt128 ret = new UnsignedInt128(this);
    ret.divideDestructive(right);
    return ret;
  }

  /**
   * This version returns the result as a new object, not modifying the give
   * objects.
   *
   * @param right
   *          right operand
   * @param remainder
   *          object to receive remainder
   * @return operation result as a new object
   */
  public UnsignedInt128 divideConstructive(UnsignedInt128 right,
      UnsignedInt128 remainder) {
    UnsignedInt128 ret = new UnsignedInt128(this);
    ret.divideDestructive(right, remainder);
    return ret;
  }

  /**
   * This version returns the result as a new object, not modifying the give
   * objects.
   *
   * @param bits
   *          the number of bits. must be positive
   * @param roundUp
   *          whether to round up the most significant bit that was discarded
   * @return operation result as a new object
   */
  public UnsignedInt128 shiftRightConstructive(int bits, boolean roundUp) {
    UnsignedInt128 ret = new UnsignedInt128(this);
    ret.shiftRightDestructive(bits, roundUp);
    return ret;
  }

  /**
   * This version returns the result as a new object, not modifying the give
   * objects.
   *
   * @param bits
   *          the number of bits. must be positive
   * @return operation result as a new object
   */
  public UnsignedInt128 shiftLeftConstructive(int bits) {
    UnsignedInt128 ret = new UnsignedInt128(this);
    ret.shiftLeftDestructive(bits);
    return ret;
  }

  private short bitLength() {
    return SqlMathUtil.bitLength(v[0], v[1], v[2], v[3]);
  }

  private void shiftRightDestructive(int wordShifts, int bitShiftsInWord,
      boolean roundUp) {
    if (wordShifts == 0 && bitShiftsInWord == 0) {
      return;
    }

    assert (wordShifts >= 0);
    assert (bitShiftsInWord >= 0);
    assert (bitShiftsInWord < 32);
    if (wordShifts >= 4) {
      zeroClear();
      return;
    }

    final int shiftRestore = 32 - bitShiftsInWord;

    // check this because "123 << 32" will be 123.
    final boolean noRestore = bitShiftsInWord == 0;
    final int roundCarryNoRestoreMask = 1 << 31;
    final int roundCarryMask = (1 << (bitShiftsInWord - 1));
    boolean roundCarry;
    int z0 = 0, z1 = 0, z2 = 0, z3 = 0;

    switch (wordShifts) {
    case 3:
      roundCarry = (noRestore ? (this.v[2] & roundCarryNoRestoreMask)
          : (this.v[3] & roundCarryMask)) != 0;
      z0 = this.v[3] >>> bitShiftsInWord;
      break;
    case 2:
      roundCarry = (noRestore ? (this.v[1] & roundCarryNoRestoreMask)
          : (this.v[2] & roundCarryMask)) != 0;
      z1 = this.v[3] >>> bitShiftsInWord;
      z0 = (noRestore ? 0 : this.v[3] << shiftRestore)
          | (this.v[2] >>> bitShiftsInWord);
      break;
    case 1:
      roundCarry = (noRestore ? (this.v[0] & roundCarryNoRestoreMask)
          : (this.v[1] & roundCarryMask)) != 0;
      z2 = this.v[3] >>> bitShiftsInWord;
      z1 = (noRestore ? 0 : this.v[3] << shiftRestore)
          | (this.v[2] >>> bitShiftsInWord);
      z0 = (noRestore ? 0 : this.v[2] << shiftRestore)
          | (this.v[1] >>> bitShiftsInWord);
      break;
    case 0:
      roundCarry = (noRestore ? 0 : (this.v[0] & roundCarryMask)) != 0;
      z3 = this.v[3] >>> bitShiftsInWord;
      z2 = (noRestore ? 0 : this.v[3] << shiftRestore)
          | (this.v[2] >>> bitShiftsInWord);
      z1 = (noRestore ? 0 : this.v[2] << shiftRestore)
          | (this.v[1] >>> bitShiftsInWord);
      z0 = (noRestore ? 0 : this.v[1] << shiftRestore)
          | (this.v[0] >>> bitShiftsInWord);
      break;
    default:
      assert (false);
      throw new RuntimeException();
    }

    update(z0, z1, z2, z3);

    // round up
    if (roundUp && roundCarry) {
      incrementDestructive();
    }
  }

  private void shiftLeftDestructive(int wordShifts, int bitShiftsInWord) {
    if (wordShifts == 0 && bitShiftsInWord == 0) {
      return;
    }
    assert (wordShifts >= 0);
    assert (bitShiftsInWord >= 0);
    assert (bitShiftsInWord < 32);
    if (wordShifts >= 4) {
      zeroClear();
      return;
    }

    final int shiftRestore = 32 - bitShiftsInWord;
    // check this because "123 << 32" will be 123.
    final boolean noRestore = bitShiftsInWord == 0;
    int z0 = 0, z1 = 0, z2 = 0, z3 = 0;

    switch (wordShifts) {
    case 3:
      z3 = this.v[0] << bitShiftsInWord;
      break;
    case 2:
      z2 = (this.v[0] << bitShiftsInWord);
      z3 = (noRestore ? 0 : this.v[0] >>> shiftRestore)
          | this.v[1] << bitShiftsInWord;
      break;
    case 1:
      z1 = (this.v[0] << bitShiftsInWord);
      z2 = (noRestore ? 0 : this.v[0] >>> shiftRestore)
          | (this.v[1] << bitShiftsInWord);
      z3 = (noRestore ? 0 : this.v[1] >>> shiftRestore)
          | this.v[2] << bitShiftsInWord;
      break;
    case 0:
      z0 = (this.v[0] << bitShiftsInWord);
      z1 = (noRestore ? 0 : this.v[0] >>> shiftRestore)
          | (this.v[1] << bitShiftsInWord);
      z2 = (noRestore ? 0 : this.v[1] >>> shiftRestore)
          | (this.v[2] << bitShiftsInWord);
      z3 = (noRestore ? 0 : this.v[2] >>> shiftRestore)
          | this.v[3] << bitShiftsInWord;
      break;
    default:
      assert (false);
    }

    update(z0, z1, z2, z3);
  }

  /**
   * Multiplies this value with the given value.
   *
   * @param left
   *          the value to multiply. in AND out.
   * @param right
   *          the value to multiply. in
   */
  private static void multiplyArrays4And4To4NoOverflow(int[] left, int[] right) {
    assert (left.length == 4);
    assert (right.length == 4);
    long product;

    product = (right[0] & SqlMathUtil.LONG_MASK)
        * (left[0] & SqlMathUtil.LONG_MASK);
    int z0 = (int) product;

    product = (right[0] & SqlMathUtil.LONG_MASK)
        * (left[1] & SqlMathUtil.LONG_MASK)
        + (right[1] & SqlMathUtil.LONG_MASK)
        * (left[0] & SqlMathUtil.LONG_MASK) + (product >>> 32);
    int z1 = (int) product;

    product = (right[0] & SqlMathUtil.LONG_MASK)
        * (left[2] & SqlMathUtil.LONG_MASK)
        + (right[1] & SqlMathUtil.LONG_MASK)
        * (left[1] & SqlMathUtil.LONG_MASK)
        + (right[2] & SqlMathUtil.LONG_MASK)
        * (left[0] & SqlMathUtil.LONG_MASK) + (product >>> 32);
    int z2 = (int) product;

    // v[3]
    product = (right[0] & SqlMathUtil.LONG_MASK)
        * (left[3] & SqlMathUtil.LONG_MASK)
        + (right[1] & SqlMathUtil.LONG_MASK)
        * (left[2] & SqlMathUtil.LONG_MASK)
        + (right[2] & SqlMathUtil.LONG_MASK)
        * (left[1] & SqlMathUtil.LONG_MASK)
        + (right[3] & SqlMathUtil.LONG_MASK)
        * (left[0] & SqlMathUtil.LONG_MASK) + (product >>> 32);
    int z3 = (int) product;
    if ((product >>> 32) != 0) {
      SqlMathUtil.throwOverflowException();
    }

    // the combinations below definitely result in overflow
    if ((right[3] != 0 && (left[3] != 0 || left[2] != 0 || left[1] != 0))
        || (right[2] != 0 && (left[3] != 0 || left[2] != 0))
        || (right[1] != 0 && left[3] != 0)) {
      SqlMathUtil.throwOverflowException();
    }

    left[0] = z0;
    left[1] = z1;
    left[2] = z2;
    left[3] = z3;
  }

  private static int[] multiplyArrays4And4To8(int[] left, int[] right) {
    assert (left.length == 4);
    assert (right.length == 4);
    long product;

    // this method could go beyond the integer ranges until we scale back
    // so, we need twice more variables.
    int[] z = new int[8];

    product = (right[0] & SqlMathUtil.LONG_MASK)
        * (left[0] & SqlMathUtil.LONG_MASK);
    z[0] = (int) product;

    product = (right[0] & SqlMathUtil.LONG_MASK)
        * (left[1] & SqlMathUtil.LONG_MASK)
        + (right[1] & SqlMathUtil.LONG_MASK)
        * (left[0] & SqlMathUtil.LONG_MASK) + (product >>> 32);
    z[1] = (int) product;

    product = (right[0] & SqlMathUtil.LONG_MASK)
        * (left[2] & SqlMathUtil.LONG_MASK)
        + (right[1] & SqlMathUtil.LONG_MASK)
        * (left[1] & SqlMathUtil.LONG_MASK)
        + (right[2] & SqlMathUtil.LONG_MASK)
        * (left[0] & SqlMathUtil.LONG_MASK) + (product >>> 32);
    z[2] = (int) product;

    product = (right[0] & SqlMathUtil.LONG_MASK)
        * (left[3] & SqlMathUtil.LONG_MASK)
        + (right[1] & SqlMathUtil.LONG_MASK)
        * (left[2] & SqlMathUtil.LONG_MASK)
        + (right[2] & SqlMathUtil.LONG_MASK)
        * (left[1] & SqlMathUtil.LONG_MASK)
        + (right[3] & SqlMathUtil.LONG_MASK)
        * (left[0] & SqlMathUtil.LONG_MASK) + (product >>> 32);
    z[3] = (int) product;

    product = (right[1] & SqlMathUtil.LONG_MASK)
        * (left[3] & SqlMathUtil.LONG_MASK)
        + (right[2] & SqlMathUtil.LONG_MASK)
        * (left[2] & SqlMathUtil.LONG_MASK)
        + (right[3] & SqlMathUtil.LONG_MASK)
        * (left[1] & SqlMathUtil.LONG_MASK) + (product >>> 32);
    z[4] = (int) product;

    product = (right[2] & SqlMathUtil.LONG_MASK)
        * (left[3] & SqlMathUtil.LONG_MASK)
        + (right[3] & SqlMathUtil.LONG_MASK)
        * (left[2] & SqlMathUtil.LONG_MASK) + (product >>> 32);
    z[5] = (int) product;

    // v[1], v[0]
    product = (right[3] & SqlMathUtil.LONG_MASK)
        * (left[3] & SqlMathUtil.LONG_MASK) + (product >>> 32);
    z[6] = (int) product;
    z[7] = (int) (product >>> 32);

    return z;
  }

  private static void incrementArray(int[] array) {
    for (int i = 0; i < INT_COUNT; ++i) {
      if (array[i] != SqlMathUtil.FULLBITS_32) {
        array[i] = (int) ((array[i] & SqlMathUtil.LONG_MASK) + 1L);
        break;
      }
      array[i] = 0;
      if (i == INT_COUNT - 1) {
        SqlMathUtil.throwOverflowException();
      }
    }
  }

  private static void decrementArray(int[] array) {
    for (int i = 0; i < INT_COUNT; ++i) {
      if (array[i] != 0) {
        array[i] = (int) ((array[i] & SqlMathUtil.LONG_MASK) - 1L);
        break;
      }
      array[i] = SqlMathUtil.FULLBITS_32;
      if (i == INT_COUNT - 1) {
        SqlMathUtil.throwOverflowException();
      }
    }
  }

  /** common implementation of difference. */
  private static byte differenceInternal(UnsignedInt128 left, int[] r,
      UnsignedInt128 result) {
    int cmp = left.compareTo(r);
    if (cmp == 0) {
      result.zeroClear();
      return 0;
    }

    long sum = 0L;
    if (cmp > 0) {

      // left is larger
      for (int i = 0; i < INT_COUNT; ++i) {
        sum = (left.v[i] & SqlMathUtil.LONG_MASK)
            - (r[i] & SqlMathUtil.LONG_MASK) - ((int) -(sum >> 32));
        result.v[i] = (int) sum;
      }
    } else {

      // right is larger
      for (int i = 0; i < INT_COUNT; ++i) {
        sum = (r[i] & SqlMathUtil.LONG_MASK)
            - (left.v[i] & SqlMathUtil.LONG_MASK) - ((int) -(sum >> 32));
        result.v[i] = (int) sum;
      }
    }

    if ((sum >> 32) != 0) {
      SqlMathUtil.throwOverflowException();
    }

    return cmp > 0 ? (byte) 1 : (byte) -1;
  }

  /**
   * @see #compareTo(UnsignedInt128)
   */
  private static int compareTo(int l0, int l1, int l2, int l3, int r0, int r1,
      int r2, int r3) {
    if (l3 != r3) {
      return SqlMathUtil.compareUnsignedInt(l3, r3);
    }
    if (l2 != r2) {
      return SqlMathUtil.compareUnsignedInt(l2, r2);
    }
    if (l1 != r1) {
      return SqlMathUtil.compareUnsignedInt(l1, r1);
    }
    if (l0 != r0) {
      return SqlMathUtil.compareUnsignedInt(l0, r0);
    }
    return 0;
  }

  /**
   * @param array
   * @param tenScale
   * @return Whether it overflowed
   */
  private static boolean scaleUpTenArray(int[] array, short tenScale) {
    while (tenScale > 0) {
      long sum = 0L;
      int powerTen = Math.min(tenScale, SqlMathUtil.MAX_POWER_TEN_INT31);
      tenScale -= powerTen;

      final long rightUnsigned = SqlMathUtil.POWER_TENS_INT31[powerTen]
          & SqlMathUtil.LONG_MASK;
      for (int i = 0; i < INT_COUNT; ++i) {
        sum = (array[i] & SqlMathUtil.LONG_MASK) * rightUnsigned + (sum >>> 32);
        array[i] = (int) sum;
      }

      if ((sum >> 32) != 0) {

        // overflow means this is smaller
        return true;
      }
    }
    return false;
  }

  /**
   * Scales down the given array (4 elements) for 10**tenScale. This method is
   * only used from add/subtract/compare, and most likely with smaller tenScale.
   * So, this method is not as optimized as
   * {@link #scaleDownTenArray8RoundUp(int[], short)}.
   *
   * @param array
   *          array to scale down. in AND out. length must be 4.
   * @param tenScale
   *          distance to scale down
   */
  private static void scaleDownTenArray4RoundUp(int[] array, short tenScale) {
    scaleDownFiveArray(array, tenScale);
    shiftRightArray(tenScale, array, array, true);
  }

  /**
   * Scales down the given array (8 elements) for 10**tenScale. This method is
   * frequently called from multiply(), so highly optimized. It's lengthy, but
   * this is inevitable to avoid array/object creation. <h2>Summary of this
   * method</h2>
   * <p>
   * This method employs division by inverse multiplication (except easy cases).
   * because all inverses of powers of tens are pre-calculated, this is much
   * faster than doing divisions. The matrix multiplication benchmark hit 3x
   * performance with this. because the inverse is a little bit smaller than
   * real value, the result is same or smaller than accurate answer, not larger.
   * we take care of it after the first multiplication.
   * </p>
   * <p>
   * Then, multiply it with power of tens (which is accurate) to correct +1
   * error and rounding up. let D = array - z * power: if D >= power/2, then
   * ++z. Otherwise, do nothing.
   * </p>
   *
   * @param array
   *          array to scale down. in AND out. length must be 8.
   * @param tenScale
   *          distance to scale down
   */
  private static void scaleDownTenArray8RoundUp(int[] array, short tenScale) {
    assert (array.length == 8);
    if (tenScale > MAX_DIGITS) {

      // then definitely this will end up zero.
      Arrays.fill(array, 0);
      return;
    }
    if (tenScale <= SqlMathUtil.MAX_POWER_TEN_INT31) {
      int divisor = SqlMathUtil.POWER_TENS_INT31[tenScale];
      assert (divisor > 0);
      boolean round = divideCheckRound(array, divisor);
      if (round) {
        incrementArray(array);
      }
      return;
    }

    // division by inverse multiplication.
    final int[] inverse = SqlMathUtil.INVERSE_POWER_TENS_INT128[tenScale].v;
    final int inverseWordShift = SqlMathUtil.INVERSE_POWER_TENS_INT128_WORD_SHIFTS[tenScale];
    assert (inverseWordShift <= 3);
    assert (inverse[3] != 0);
    for (int i = 5 + inverseWordShift; i < 8; ++i) {
      if (array[i] != 0) {
        SqlMathUtil.throwOverflowException(); // because inverse[3] is
                                              // not zero
      }
    }

    int z4 = 0, z5 = 0, z6 = 0, z7 = 0; // because inverse is scaled 2^128,
                                        // these will become v0-v3
    int z8 = 0, z9 = 0, z10 = 0; // for wordshift
    long product = 0L;

    product += (inverse[0] & SqlMathUtil.LONG_MASK)
        * (array[4] & SqlMathUtil.LONG_MASK)
        + (inverse[1] & SqlMathUtil.LONG_MASK)
        * (array[3] & SqlMathUtil.LONG_MASK)
        + (inverse[2] & SqlMathUtil.LONG_MASK)
        * (array[2] & SqlMathUtil.LONG_MASK)
        + (inverse[3] & SqlMathUtil.LONG_MASK)
        * (array[1] & SqlMathUtil.LONG_MASK);
    z4 = (int) product;
    product >>>= 32;

    product += (inverse[0] & SqlMathUtil.LONG_MASK)
        * (array[5] & SqlMathUtil.LONG_MASK)
        + (inverse[1] & SqlMathUtil.LONG_MASK)
        * (array[4] & SqlMathUtil.LONG_MASK)
        + (inverse[2] & SqlMathUtil.LONG_MASK)
        * (array[3] & SqlMathUtil.LONG_MASK)
        + (inverse[3] & SqlMathUtil.LONG_MASK)
        * (array[2] & SqlMathUtil.LONG_MASK);
    z5 = (int) product;
    product >>>= 32;

    product += (inverse[0] & SqlMathUtil.LONG_MASK)
        * (array[6] & SqlMathUtil.LONG_MASK)
        + (inverse[1] & SqlMathUtil.LONG_MASK)
        * (array[5] & SqlMathUtil.LONG_MASK)
        + (inverse[2] & SqlMathUtil.LONG_MASK)
        * (array[4] & SqlMathUtil.LONG_MASK)
        + (inverse[3] & SqlMathUtil.LONG_MASK)
        * (array[3] & SqlMathUtil.LONG_MASK);
    z6 = (int) product;
    product >>>= 32;

    product += (inverse[0] & SqlMathUtil.LONG_MASK)
        * (array[7] & SqlMathUtil.LONG_MASK)
        + (inverse[1] & SqlMathUtil.LONG_MASK)
        * (array[6] & SqlMathUtil.LONG_MASK)
        + (inverse[2] & SqlMathUtil.LONG_MASK)
        * (array[5] & SqlMathUtil.LONG_MASK)
        + (inverse[3] & SqlMathUtil.LONG_MASK)
        * (array[4] & SqlMathUtil.LONG_MASK);
    z7 = (int) product;
    product >>>= 32;

    if (inverseWordShift >= 1) {
      product += (inverse[1] & SqlMathUtil.LONG_MASK)
          * (array[7] & SqlMathUtil.LONG_MASK)
          + (inverse[2] & SqlMathUtil.LONG_MASK)
          * (array[6] & SqlMathUtil.LONG_MASK)
          + (inverse[3] & SqlMathUtil.LONG_MASK)
          * (array[5] & SqlMathUtil.LONG_MASK);
      z8 = (int) product;
      product >>>= 32;

      if (inverseWordShift >= 2) {
        product += (inverse[2] & SqlMathUtil.LONG_MASK)
            * (array[7] & SqlMathUtil.LONG_MASK)
            + (inverse[3] & SqlMathUtil.LONG_MASK)
            * (array[6] & SqlMathUtil.LONG_MASK);
        z9 = (int) product;
        product >>>= 32;

        if (inverseWordShift >= 3) {
          product += (inverse[3] & SqlMathUtil.LONG_MASK)
              * (array[7] & SqlMathUtil.LONG_MASK);
          z10 = (int) product;
          product >>>= 32;
        }
      }
    }

    if (product != 0) {
      SqlMathUtil.throwOverflowException();
    }

    // if inverse is word-shifted for accuracy, shift it back here.
    switch (inverseWordShift) {
    case 1:
      z4 = z5;
      z5 = z6;
      z6 = z7;
      z7 = z8;
      break;
    case 2:
      z4 = z6;
      z5 = z7;
      z6 = z8;
      z7 = z9;
      break;
    case 3:
      z4 = z7;
      z5 = z8;
      z6 = z9;
      z7 = z10;
      break;
    default:
      break;
    }

    // now, correct +1 error and rounding up.
    final int[] power = SqlMathUtil.POWER_TENS_INT128[tenScale].v;
    final int[] half = SqlMathUtil.ROUND_POWER_TENS_INT128[tenScale].v;

    int d0, d1, d2, d3, d4;
    product = (array[0] & SqlMathUtil.LONG_MASK)
        - (power[0] & SqlMathUtil.LONG_MASK) * (z4 & SqlMathUtil.LONG_MASK);
    d0 = (int) product;

    product = (array[1] & SqlMathUtil.LONG_MASK)
        - (power[0] & SqlMathUtil.LONG_MASK) * (z5 & SqlMathUtil.LONG_MASK)
        - (power[1] & SqlMathUtil.LONG_MASK) * (z4 & SqlMathUtil.LONG_MASK)
        - ((int) -(product >> 32));
    d1 = (int) product;

    product = (array[2] & SqlMathUtil.LONG_MASK)
        - (power[0] & SqlMathUtil.LONG_MASK) * (z6 & SqlMathUtil.LONG_MASK)
        - (power[1] & SqlMathUtil.LONG_MASK) * (z5 & SqlMathUtil.LONG_MASK)
        - (power[2] & SqlMathUtil.LONG_MASK) * (z4 & SqlMathUtil.LONG_MASK)
        - ((int) -(product >> 32));
    d2 = (int) product;

    product = (array[3] & SqlMathUtil.LONG_MASK)
        - (power[0] & SqlMathUtil.LONG_MASK) * (z7 & SqlMathUtil.LONG_MASK)
        - (power[1] & SqlMathUtil.LONG_MASK) * (z6 & SqlMathUtil.LONG_MASK)
        - (power[2] & SqlMathUtil.LONG_MASK) * (z5 & SqlMathUtil.LONG_MASK)
        - (power[3] & SqlMathUtil.LONG_MASK) * (z4 & SqlMathUtil.LONG_MASK)
        - ((int) -(product >> 32));
    d3 = (int) product;

    product = (array[4] & SqlMathUtil.LONG_MASK)
        - (power[1] & SqlMathUtil.LONG_MASK) * (z7 & SqlMathUtil.LONG_MASK)
        - (power[2] & SqlMathUtil.LONG_MASK) * (z6 & SqlMathUtil.LONG_MASK)
        - (power[3] & SqlMathUtil.LONG_MASK) * (z5 & SqlMathUtil.LONG_MASK)
        - ((int) -(product >> 32));
    d4 = (int) product;

    // If the difference is larger than 2^128 (d4 != 0), then D is
    // definitely larger than power, so increment.
    // otherwise, compare it with power and half.
    boolean increment = (d4 != 0)
        || (compareTo(d0, d1, d2, d3, half[0], half[1], half[2], half[3]) >= 0);

    array[0] = z4;
    array[1] = z5;
    array[2] = z6;
    array[3] = z7;
    if (increment) {
      incrementArray(array);
    }
  }

  /**
   * Scales down the given array for 5**fiveScale.
   *
   * @param array
   *          array to scale down
   * @param fiveScale
   *          distance to scale down
   * @return Whether it requires incrementing if rounding
   */
  private static boolean scaleDownFiveArray(int[] array, short fiveScale) {
    while (true) {
      int powerFive = Math.min(fiveScale, SqlMathUtil.MAX_POWER_FIVE_INT31);
      fiveScale -= powerFive;

      int divisor = SqlMathUtil.POWER_FIVES_INT31[powerFive];
      assert (divisor > 0);
      if (fiveScale == 0) {
        return divideCheckRound(array, divisor);
      } else {
        divideCheckRound(array, divisor);
      }
    }
  }

  private static boolean divideCheckRound(int[] array, int divisor) {
    long remainder = 0;
    for (int i = array.length - 1; i >= 0; --i) {
      remainder = ((array[i] & SqlMathUtil.LONG_MASK) + (remainder << 32));
      array[i] = (int) (remainder / divisor);
      remainder %= divisor;
    }

    return (remainder >= (divisor >> 1));
  }

  private static void scaleDownFiveArrayRoundUp(int[] array, short tenScale) {
    boolean rounding = scaleDownFiveArray(array, tenScale);
    if (rounding) {
      incrementArray(array);
    }
  }

  /**
   * Internal method to apply the result of multiplication with right-shifting.
   * This method does round the value while right-shifting (SQL Numeric
   * semantics).
   *
   * @param rightShifts
   *          distance of right-shifts
   */
  private static void shiftRightArray(int rightShifts, int[] z, int[] result,
      boolean round) {
    assert (rightShifts >= 0);
    if (rightShifts == 0) {
      for (int i = 0; i < INT_COUNT; ++i) {
        if (z[i + INT_COUNT] != 0) {
          SqlMathUtil.throwOverflowException();
        }
      }
      result[0] = z[0];
      result[1] = z[1];
      result[2] = z[2];
      result[3] = z[3];
    } else {
      final int wordShifts = rightShifts / 32;
      final int bitShiftsInWord = rightShifts % 32;
      final int shiftRestore = 32 - bitShiftsInWord;

      // check this because "123 << 32" will be 123.
      final boolean noRestore = bitShiftsInWord == 0;

      // overflow checks
      if (z.length > INT_COUNT) {
        if (wordShifts + INT_COUNT < z.length
            && (z[wordShifts + INT_COUNT] >>> bitShiftsInWord) != 0) {
          SqlMathUtil.throwOverflowException();
        }

        for (int i = 1; i < INT_COUNT; ++i) {
          if (i + wordShifts < z.length - INT_COUNT
              && z[i + wordShifts + INT_COUNT] != 0) {
            SqlMathUtil.throwOverflowException();
          }
        }
      }

      // check round-ups before settings values to result.
      // be aware that result could be the same object as z.
      boolean roundCarry = false;
      if (round) {
        if (bitShiftsInWord == 0) {
          assert (wordShifts > 0);
          roundCarry = z[wordShifts - 1] < 0;
        } else {
          roundCarry = (z[wordShifts] & (1 << (bitShiftsInWord - 1))) != 0;
        }
      }

      // extract the values.
      for (int i = 0; i < INT_COUNT; ++i) {
        int val = 0;
        if (!noRestore && i + wordShifts + 1 < z.length) {
          val = z[i + wordShifts + 1] << shiftRestore;
        }
        if (i + wordShifts < z.length) {
          val |= (z[i + wordShifts] >>> bitShiftsInWord);
        }
        result[i] = val;
      }

      if (roundCarry) {
        incrementArray(result);
      }
    }
  }

  /**
   * helper method for multiplication. used when either left/right fits int32.
   */
  private void multiplyDestructiveFitsInt32(UnsignedInt128 right,
      short rightShifts, short tenScaleDown) {
    assert (this.fitsInt32() && right.fitsInt32());
    assert (rightShifts == 0 || tenScaleDown == 0); // only one of them
    if (this.isZero()) {
      return; // zero. no need to shift/scale
    } else if (right.isZero()) {
      zeroClear();
      return; // zero. no need to shift/scale
    } else if (this.isOne()) {
      this.update(right);
    } else {
      this.multiplyDestructive(right.v[0]);
    }

    if (rightShifts > 0) {
      this.shiftRightDestructive(rightShifts, true);
    } else if (tenScaleDown > 0) {
      this.scaleDownTenDestructive(tenScaleDown);
    }
  }

  /** Updates the value of {@link #cnt} by checking {@link #v}. */
  private void updateCount() {
    if (v[3] != 0) {
      this.count = (byte) 4;
    } else if (v[2] != 0) {
      this.count = (byte) 3;
    } else if (v[1] != 0) {
      this.count = (byte) 2;
    } else if (v[0] != 0) {
      this.count = (byte) 1;
    } else {
      this.count = (byte) 0;
    }
  }
}
