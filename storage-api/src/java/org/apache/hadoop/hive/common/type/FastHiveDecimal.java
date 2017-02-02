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
package org.apache.hadoop.hive.common.type;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 *    FastHiveDecimal is a mutable fast decimal object.  It is the base class for both the
 *    HiveDecimal and HiveDecimalWritable classes.  All fast* methods are protected so they
 *    cannot be accessed by clients of HiveDecimal and HiveDecimalWritable.  HiveDecimal ensures
 *    it creates new objects when the value changes since it provides immutable semantics;
 *    HiveDecimalWritable does not create new objects since it provides mutable semantics.
 *
 *    The methods in this class are shells that pickup the member variables from FastHiveDecimal
 *    parameters and pass them as individual parameters to static methods in the FastHiveDecimalImpl
 *    class that do the real work.
 *
 *    NOTE: The rationale for fast decimal is in FastHiveDecimalImpl.
 */
public class FastHiveDecimal {

  /*
   * We use protected for the fields so the FastHiveDecimalImpl class can access them.  Other
   * classes including HiveDecimal should not access these fields directly.
   */

  // See FastHiveDecimalImpl for more details on these fields.

  // -1 when negative; 0 when decimal is zero; 1 when positive.
  protected int fastSignum;

  // Decimal longwords.
  protected long fast2;
  protected long fast1;
  protected long fast0;

  // The number of integer digits in the decimal.  When the integer portion is zero, this is 0.
  protected int fastIntegerDigitCount;

  // The scale of the decimal.
  protected int fastScale;

  // Used for legacy HiveDecimalV1 setScale compatibility for binary / display serialization of
  // trailing zeroes (or rounding).
  protected int fastSerializationScale;

  protected FastHiveDecimal() {
    fastReset();
  }

  protected FastHiveDecimal(FastHiveDecimal fastDec) {
    this();
    fastSignum = fastDec.fastSignum;
    fast0 = fastDec.fast0;
    fast1 = fastDec.fast1;
    fast2 = fastDec.fast2;
    fastIntegerDigitCount = fastDec.fastIntegerDigitCount;
    fastScale = fastDec.fastScale;

    // Not propagated.
    fastSerializationScale = -1;
  }

  protected FastHiveDecimal(int fastSignum, FastHiveDecimal fastDec) {
    this();
    this.fastSignum = fastSignum;
    fast0 = fastDec.fast0;
    fast1 = fastDec.fast1;
    fast2 = fastDec.fast2;
    fastIntegerDigitCount = fastDec.fastIntegerDigitCount;
    fastScale = fastDec.fastScale;

    // Not propagated.
    fastSerializationScale = -1;
  }

  protected FastHiveDecimal(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale) {
    this();
    this.fastSignum = fastSignum;
    this.fast0 = fast0;
    this.fast1 = fast1;
    this.fast2 = fast2;
    this.fastIntegerDigitCount = fastIntegerDigitCount;
    this.fastScale = fastScale;

    fastSerializationScale = -1;
  }

  protected FastHiveDecimal(long longValue) {
    this();
    FastHiveDecimalImpl.fastSetFromLong(longValue, this);
  }

  protected FastHiveDecimal(String string) {
    this();
    FastHiveDecimalImpl.fastSetFromString(string, false, this);
  }

  protected void fastReset() {
    fastSignum = 0;
    fast0 = 0;
    fast1 = 0;
    fast2 = 0;
    fastIntegerDigitCount = 0;
    fastScale = 0;
    fastSerializationScale = -1;
  }

  protected void fastSet(FastHiveDecimal fastDec) {
    fastSignum = fastDec.fastSignum;
    fast0 = fastDec.fast0;
    fast1 = fastDec.fast1;
    fast2 = fastDec.fast2;
    fastIntegerDigitCount = fastDec.fastIntegerDigitCount;
    fastScale = fastDec.fastScale;
    fastSerializationScale = fastDec.fastSerializationScale;
  }

  protected void fastSet(
      int fastSignum, long fast0, long fast1, long fast2, int fastIntegerDigitCount, int fastScale) {
    this.fastSignum = fastSignum;
    this.fast0 = fast0;
    this.fast1 = fast1;
    this.fast2 = fast2;
    this.fastIntegerDigitCount = fastIntegerDigitCount;
    this.fastScale = fastScale;

    // Not specified.
    fastSerializationScale = -1;
  }

  protected void fastSetSerializationScale(int fastSerializationScale) {
    this.fastSerializationScale = fastSerializationScale;
  }

  protected int fastSerializationScale() {
    return fastSerializationScale;
  }

  protected static final String STRING_ENFORCE_PRECISION_OUT_OF_RANGE =
      "Decimal precision out of allowed range [1," + HiveDecimal.MAX_PRECISION + "]";
  protected static final String STRING_ENFORCE_SCALE_OUT_OF_RANGE =
      "Decimal scale out of allowed range [0," + HiveDecimal.MAX_SCALE + "]";
  protected static final String STRING_ENFORCE_SCALE_LESS_THAN_EQUAL_PRECISION =
      "Decimal scale must be less than or equal to precision";

  protected boolean fastSetFromBigDecimal(
      BigDecimal bigDecimal, boolean allowRounding) {
    return
        FastHiveDecimalImpl.fastSetFromBigDecimal(
            bigDecimal, allowRounding, this);
  }

  protected boolean fastSetFromBigInteger(
      BigInteger bigInteger) {
    return
        FastHiveDecimalImpl.fastSetFromBigInteger(
            bigInteger, this);
  }

  protected boolean fastSetFromBigIntegerAndScale(
      BigInteger bigInteger, int scale) {
    return
        FastHiveDecimalImpl.fastSetFromBigInteger(
            bigInteger, scale, this);
  }

  protected boolean fastSetFromString(String string, boolean trimBlanks) {
    byte[] bytes = string.getBytes();
    return
        fastSetFromBytes(
            bytes, 0, bytes.length, trimBlanks);
  }

  protected boolean fastSetFromBytes(byte[] bytes, int offset, int length, boolean trimBlanks) {
    return
        FastHiveDecimalImpl.fastSetFromBytes(
            bytes, offset, length, trimBlanks, this);
  }

  protected boolean fastSetFromDigitsOnlyBytesAndScale(
      boolean isNegative, byte[] bytes, int offset, int length, int scale) {
    return
        FastHiveDecimalImpl.fastSetFromDigitsOnlyBytesAndScale(
            isNegative, bytes, offset, length, scale, this);
  }

  protected void fastSetFromInt(int intValue) {
    FastHiveDecimalImpl.fastSetFromInt(intValue, this);
  }

  protected void fastSetFromLong(long longValue) {
    FastHiveDecimalImpl.fastSetFromLong(longValue, this);
  }

  protected boolean fastSetFromLongAndScale(long longValue, int scale) {
    return
        FastHiveDecimalImpl.fastSetFromLongAndScale(
            longValue, scale, this);
  }

  protected boolean fastSetFromFloat(float floatValue) {
    return
        FastHiveDecimalImpl.fastSetFromFloat(
            floatValue, this);
  }

  protected boolean fastSetFromDouble(double doubleValue) {
    return
        FastHiveDecimalImpl.fastSetFromDouble(
            doubleValue, this);
  }

  protected void fastFractionPortion() {
    FastHiveDecimalImpl.fastFractionPortion(
        fastSignum, fast0, fast1, fast2, fastScale,
        this);
  }

  protected void fastIntegerPortion() {
    FastHiveDecimalImpl.fastIntegerPortion(
        fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale,
        this);
  }

  protected static final int FAST_SCRATCH_BUFFER_LEN_SERIALIZATION_UTILS_READ = 8 * 3;

  protected boolean fastSerializationUtilsRead(
      InputStream inputStream, int scale,
      byte[] scratchBytes) throws IOException, EOFException {
    return
        FastHiveDecimalImpl.fastSerializationUtilsRead(
            inputStream, scale, scratchBytes, this);
  }

  protected boolean fastSetFromBigIntegerBytesAndScale(
      byte[] bytes, int offset, int length, int scale) {
    return FastHiveDecimalImpl.fastSetFromBigIntegerBytesAndScale(
        bytes, offset, length, scale, this);
  }

  protected static final int SCRATCH_LONGS_LEN_FAST_SERIALIZATION_UTILS_WRITE = 6;

  protected boolean fastSerializationUtilsWrite(OutputStream outputStream,
      long[] scratchLongs)
          throws IOException {
    return
        FastHiveDecimalImpl.fastSerializationUtilsWrite(
            outputStream,
            fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale,
            scratchLongs);
  }

  // The fastBigIntegerBytes method returns 3 56 bit (7 byte) words and a possible sign byte.
  // However, the fastBigIntegerBytes can take on trailing zeroes -- so make it larger.
  protected static final int FAST_SCRATCH_BUFFER_LEN_BIG_INTEGER_BYTES = 1 + 48;
  protected static final int FAST_SCRATCH_LONGS_LEN = 6;

  protected int fastBigIntegerBytes(
      long[] scratchLongs, byte[] buffer) {
    return
        FastHiveDecimalImpl.fastBigIntegerBytes(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale,
            fastSerializationScale,
            scratchLongs, buffer);
  }

  protected int fastBigIntegerBytesScaled(
      int serializationScale,
      long[] scratchLongs, byte[] buffer) {
    return
        FastHiveDecimalImpl.fastBigIntegerBytesScaled(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale,
            serializationScale,
            scratchLongs, buffer);
  }

  protected boolean fastIsByte() {
    return
        FastHiveDecimalImpl.fastIsByte(
            fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale);
  }

  protected byte fastByteValueClip() {
    return
        FastHiveDecimalImpl.fastByteValueClip(
            fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale);
  }

  protected boolean fastIsShort() {
    return
        FastHiveDecimalImpl.fastIsShort(
            fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale);
  }

  protected short fastShortValueClip() {
    return
        FastHiveDecimalImpl.fastShortValueClip(
            fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale);
  }

  protected boolean fastIsInt() {
    return
        FastHiveDecimalImpl.fastIsInt(
            fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale);
  }

  protected int fastIntValueClip() {
    return
        FastHiveDecimalImpl.fastIntValueClip(
            fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale);
  }

  protected boolean fastIsLong() {
    return
        FastHiveDecimalImpl.fastIsLong(
            fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale);
  }

  protected long fastLongValueClip() {
    return
        FastHiveDecimalImpl.fastLongValueClip(
            fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale);
  }

  protected float fastFloatValue() {
    return
        FastHiveDecimalImpl.fastFloatValue(
            fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale);
  }

  protected double fastDoubleValue() {
    return
        FastHiveDecimalImpl.fastDoubleValue(
            fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale);
  }

  protected BigInteger fastBigIntegerValue() {
    return
        FastHiveDecimalImpl.fastBigIntegerValue(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale,
            fastSerializationScale);
  }

  protected BigDecimal fastBigDecimalValue() {
    return
        FastHiveDecimalImpl.fastBigDecimalValue(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale);
  }

  protected int fastScale() {
    return fastScale;
  }

  protected int fastSignum() {
    return fastSignum;
  }

  protected int fastCompareTo(FastHiveDecimal right) {
    return
        FastHiveDecimalImpl.fastCompareTo(
            fastSignum, fast0, fast1, fast2,
            fastScale,
            right.fastSignum, right.fast0, right.fast1, right.fast2,
            right.fastScale);
  }

  protected static int fastCompareTo(FastHiveDecimal left, FastHiveDecimal right) {
    return
        FastHiveDecimalImpl.fastCompareTo(
            left.fastSignum, left.fast0, left.fast1, left.fast2,
            left.fastScale,
            right.fastSignum, right.fast0, right.fast1, right.fast2,
            right.fastScale);
  }

  protected boolean fastEquals(FastHiveDecimal that) {
    return
        FastHiveDecimalImpl.fastEquals(
          fastSignum, fast0, fast1, fast2,
          fastScale,
          that.fastSignum, that.fast0, that.fast1, that.fast2,
          that.fastScale);
  }

  protected void fastAbs() {
    fastSignum = 1;
  }

  protected void fastNegate() {
    if (fastSignum == 0) {
      return;
    }
    fastSignum = (fastSignum == 1 ? -1 : 1);
  }

  protected int fastNewFasterHashCode() {
    return
        FastHiveDecimalImpl.fastNewFasterHashCode(
            fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale);
  }

  protected int fastHashCode() {
    return
        FastHiveDecimalImpl.fastHashCode(
            fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale);
  }

  protected int fastIntegerDigitCount() {
    return fastIntegerDigitCount;
  }

  protected int fastSqlPrecision() {
    return
        FastHiveDecimalImpl.fastSqlPrecision(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale);
  }

  protected int fastRawPrecision() {
    return
        FastHiveDecimalImpl.fastRawPrecision(
            fastSignum, fast0, fast1, fast2);
  }

  protected boolean fastScaleByPowerOfTen(
      int n,
      FastHiveDecimal fastResult) {
    return
        FastHiveDecimalImpl.fastScaleByPowerOfTen(
            fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale,
            n,
            fastResult);
  }

  protected static String fastRoundingModeToString(int roundingMode) {
    String roundingModeString;
    switch (roundingMode) {
    case BigDecimal.ROUND_DOWN:
      roundingModeString = "ROUND_DOWN";
      break;
    case BigDecimal.ROUND_UP:
      roundingModeString = "ROUND_UP";
      break;
    case BigDecimal.ROUND_FLOOR:
      roundingModeString = "ROUND_FLOOR";
      break;
    case BigDecimal.ROUND_CEILING:
      roundingModeString = "ROUND_CEILING";
      break;
    case BigDecimal.ROUND_HALF_UP:
      roundingModeString = "ROUND_HALF_UP";
      break;
    case BigDecimal.ROUND_HALF_EVEN:
      roundingModeString = "ROUND_HALF_EVEN";
      break;
    default:
      roundingModeString = "Unknown";
    }
    return roundingModeString + " (" + roundingMode + ")";
  }

  protected boolean fastRound(
      int newScale, int roundingMode,
      FastHiveDecimal fastResult) {
    return
        FastHiveDecimalImpl.fastRound(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale,
            newScale, roundingMode,
            fastResult);
  }

  protected boolean isAllZeroesBelow(
      int power) {
    return
        FastHiveDecimalImpl.isAllZeroesBelow(
            fastSignum, fast0, fast1, fast2, power);
  }

  protected boolean fastEnforcePrecisionScale(
      int maxPrecision, int maxScale) {
    if (maxPrecision <= 0 || maxPrecision > HiveDecimal.MAX_PRECISION) {
      return false;
    }
    if (maxScale < 0 || maxScale > HiveDecimal.MAX_SCALE) {
      return false;
    }
    /*
    if (!fastIsValid()) {
      fastRaiseInvalidException();
    }
    */
    FastCheckPrecisionScaleStatus status =
        FastHiveDecimalImpl.fastCheckPrecisionScale(
          fastSignum, fast0, fast1, fast2,
          fastIntegerDigitCount, fastScale,
          maxPrecision, maxScale);
    switch (status) {
    case NO_CHANGE:
      return true;
    case OVERFLOW:
      return false;
    case UPDATE_SCALE_DOWN:
      {
        if (!FastHiveDecimalImpl.fastUpdatePrecisionScale(
          fastSignum, fast0, fast1, fast2,
          fastIntegerDigitCount, fastScale,
          maxPrecision, maxScale, status,
          this)) {
          return false;
        }
        /*
        if (!fastIsValid()) {
          fastRaiseInvalidException();
        }
        */
        return true;
      }
    default:
      throw new RuntimeException("Unknown fast decimal check precision and scale status " + status);
    }
  }

  protected FastCheckPrecisionScaleStatus fastCheckPrecisionScale(
      int maxPrecision, int maxScale) {
    return
        FastHiveDecimalImpl.fastCheckPrecisionScale(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale,
            maxPrecision, maxScale);
  }

  protected static enum FastCheckPrecisionScaleStatus {
    NO_CHANGE,
    OVERFLOW,
    UPDATE_SCALE_DOWN;
  }

  protected boolean fastUpdatePrecisionScale(
      int maxPrecision, int maxScale, FastCheckPrecisionScaleStatus status,
      FastHiveDecimal fastResult) {
    return
        FastHiveDecimalImpl.fastUpdatePrecisionScale(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale,
            maxPrecision, maxScale, status,
            fastResult);
  }

  protected boolean fastAdd(
      FastHiveDecimal fastRight,
      FastHiveDecimal fastResult) {
    return
        FastHiveDecimalImpl.fastAdd(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale,
            fastRight.fastSignum, fastRight.fast0, fastRight.fast1, fastRight.fast2,
            fastRight.fastIntegerDigitCount, fastRight.fastScale,
            fastResult);
  }

  protected boolean fastSubtract(
      FastHiveDecimal fastRight,
      FastHiveDecimal fastResult) {
    return
        FastHiveDecimalImpl.fastSubtract(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale,
            fastRight.fastSignum, fastRight.fast0, fastRight.fast1, fastRight.fast2,
            fastRight.fastIntegerDigitCount, fastRight.fastScale,
            fastResult);
  }

  protected boolean fastMultiply(
      FastHiveDecimal fastRight,
      FastHiveDecimal fastResult) {
    return
        FastHiveDecimalImpl.fastMultiply(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale,
            fastRight.fastSignum, fastRight.fast0, fastRight.fast1, fastRight.fast2,
            fastRight.fastIntegerDigitCount, fastRight.fastScale,
            fastResult);
  }

  protected boolean fastRemainder(
      FastHiveDecimal fastRight,
      FastHiveDecimal fastResult) {
    return
        FastHiveDecimalImpl.fastRemainder(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale,
            fastRight.fastSignum, fastRight.fast0, fastRight.fast1, fastRight.fast2,
            fastRight.fastIntegerDigitCount, fastRight.fastScale,
            fastResult);
  }

  protected boolean fastDivide(
      FastHiveDecimal fastRight,
      FastHiveDecimal fastResult) {
    return
        FastHiveDecimalImpl.fastDivide(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale,
            fastRight.fastSignum, fastRight.fast0, fastRight.fast1, fastRight.fast2,
            fastRight.fastIntegerDigitCount, fastRight.fastScale,
            fastResult);
  }

  protected boolean fastPow(
      int exponent,
      FastHiveDecimal fastResult) {
    return
        FastHiveDecimalImpl.fastPow(
            fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale,
            exponent,
            fastResult);
  }

  protected String fastToString(
      byte[] scratchBuffer) {
    return
        FastHiveDecimalImpl.fastToString(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale, -1,
            scratchBuffer);
  }

  protected String fastToString() {
    return
        FastHiveDecimalImpl.fastToString(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale, -1);
  }

  protected String fastToFormatString(int formatScale) {
    return
        FastHiveDecimalImpl.fastToFormatString(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale,
            formatScale);
  }

  protected String fastToFormatString(
      int formatScale,
      byte[] scratchBuffer) {
    return
        FastHiveDecimalImpl.fastToFormatString(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale,
            formatScale,
            scratchBuffer);
  }

  protected String fastToDigitsOnlyString() {
    return
        FastHiveDecimalImpl.fastToDigitsOnlyString(
            fast0, fast1, fast2,
            fastIntegerDigitCount);
  }

  // Sign, zero, dot, 2 * digits (to support toFormatString which can add a lot of trailing zeroes).
  protected final static int FAST_SCRATCH_BUFFER_LEN_TO_BYTES =
      1 + 1 + 1 + 2 * FastHiveDecimalImpl.MAX_DECIMAL_DIGITS;

  protected int fastToBytes(
      byte[] scratchBuffer) {
    return
        FastHiveDecimalImpl.fastToBytes(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale, -1,
            scratchBuffer);
  }

  protected int fastToFormatBytes(
      int formatScale,
      byte[] scratchBuffer) {
    return
        FastHiveDecimalImpl.fastToFormatBytes(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale,
            formatScale,
            scratchBuffer);
  }

  protected int fastToDigitsOnlyBytes(
      byte[] scratchBuffer) {
    return
        FastHiveDecimalImpl.fastToDigitsOnlyBytes(
            fast0, fast1, fast2,
            fastIntegerDigitCount,
            scratchBuffer);
  }

  @Override
  public String toString() {
    return
        FastHiveDecimalImpl.fastToString(
            fastSignum, fast0, fast1, fast2,
            fastIntegerDigitCount, fastScale, -1);
  }

  protected boolean fastIsValid() {
    return FastHiveDecimalImpl.fastIsValid(this);
  }

  protected void fastRaiseInvalidException() {
    FastHiveDecimalImpl.fastRaiseInvalidException(this);
  }

  protected void fastRaiseInvalidException(String parameters) {
    FastHiveDecimalImpl.fastRaiseInvalidException(this, parameters);
  }
}