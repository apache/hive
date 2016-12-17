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

import java.util.Arrays;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * HiveDecimal is a decimal data type with a maximum precision and scale.
 * <p>
 * It is the Hive DECIMAL data type.
 * <p>
 * The scale is the number of fractional decimal digits.  The digits after the dot.  It is limited
 * to 38 (MAX_SCALE).
 * <p>
 * The precision is the integer (or whole-number) decimal digits plus fractional decimal digits.
 * It is limited to a total of 38 digits (MAX_PRECISION).
 * <p>
 * Hive syntax for declaring DECIMAL has 3 forms:
 * <p>
 * {@code
 *       DECIMAL                            // Use the default precision/scale.}
 * <p>
 * {@code
 *       DECIMAL(precision)                 // Use the default scale.}
 * <p>
 * {@code
 *       DECIMAL(precision, scale)}
 * }
 * <p>
 * The declared scale must be &lt;= precision.
 * <p>
 * Use DECIMAL instead of DOUBLE when exact numeric accuracy is required.  Not all decimal numbers
 * (radix 10) are exactly representable in the binary (radix 2 based) floating point type DOUBLE and
 * cause accuracy anomalies (i.e. wrong results).  See the Internet for more details.
 * <p>
 * HiveDecimal is implemented as a classic Java immutable object.  All operations on HiveDecimal
 * that produce a different value will create a new HiveDecimal object.
 * <p>
 * Decimals are physically stored without any extra leading or trailing zeroes.  The scale of
 * a decimal is the number of non-trailing zero fractional digits.
 * <p>
 * Math operations on decimals typically cause the scale to change as a result of the math and
 * from trailing fractional digit elimination.
 * <p>
 * Typically, Hive, when it wants to make sure a result decimal fits in the column decimal's
 * precision/scale it calls enforcePrecisionScale.  That method will scale down or trim off
 * result fractional digits if necessary with rounding when the column has a smaller scale.
 * And, it will also indicate overflow when the decimal has exceeded the column's maximum precision.
 * <p>
 * NOTE: When Hive gets ready to serialize a decimal into text or binary, it usually sometimes
 * wants trailing fractional zeroes.  See the special notes for toFormatString and
 * bigIntegerBytesScaled for details.
 * <p>
 * ------------------------------------- Version 2 ------------------------------------------------
 * <p>
 * This is the 2nd major version of HiveDecimal called V2.  The previous version has been
 * renamed to HiveDecimalV1 and is kept as a test and behavior reference.
 * <p>
 * For good performance we do not represent the decimal using a BigDecimal object like the previous
 * version V1 did.  Using Java objects to represent our decimal incurs too high a penalty
 * for memory allocations and general logic.
 * <p>
 * The original V1 public methods and fields are annotated with @HiveDecimalVersionV1; new public
 * methods and fields are annotated with @HiveDecimalVersionV2.
 *
 */
public final class HiveDecimal extends FastHiveDecimal implements Comparable<HiveDecimal> {

  /*
   * IMPLEMENTATION NOTE:
   *    We implement HiveDecimal with the mutable FastHiveDecimal class.  That class uses
   *    protected on all its methods so they will not be visible in the HiveDecimal class.
   *
   *    So even if one casts to FastHiveDecimal, you shouldn't be able to violate the immutability
   *    of a HiveDecimal class.
   */

  @HiveDecimalVersionV1
  public static final int MAX_PRECISION = 38;
  @HiveDecimalVersionV1
  public static final int MAX_SCALE = 38;

  /**
   * Default precision/scale when user doesn't specify in the column metadata, such as
   * decimal and decimal(8).
   */
  @HiveDecimalVersionV1
  public static final int USER_DEFAULT_PRECISION = 10;
  @HiveDecimalVersionV1
  public static final int USER_DEFAULT_SCALE = 0;

  /**
   *  Default precision/scale when system is not able to determine them, such as in case
   *  of a non-generic udf.
   */
  @HiveDecimalVersionV1
  public static final int SYSTEM_DEFAULT_PRECISION = 38;
  @HiveDecimalVersionV1
  public static final int SYSTEM_DEFAULT_SCALE = 18;

  /**
   * Common values.
   */
  @HiveDecimalVersionV1
  public static final HiveDecimal ZERO = HiveDecimal.create(0);
  @HiveDecimalVersionV1
  public static final HiveDecimal ONE = HiveDecimal.create(1);

  /**
   * ROUND_FLOOR:
   * <p>
   *   Round towards negative infinity.
   * <p>
   *   The Hive function is FLOOR.
   * <p>
   *   Positive numbers: The round fraction is thrown away.
   * <p>
   *       (Example here rounds at scale 0)
   *       Value        FLOOR
   *        0.3           0
   *        2             2
   *        2.1           2
   * <p>
   *   Negative numbers: If there is a round fraction, throw it away and subtract 1.
   * <p>
   *       (Example here rounds at scale 0)
   *       Value        FLOOR
   *       -0.3           -1
   *       -2             -2
   *       -2.1           -3
   */
  @HiveDecimalVersionV1
  public static final int ROUND_FLOOR = BigDecimal.ROUND_FLOOR;

  /**
   * ROUND_CEILING:
   * <p>
   *   Round towards positive infinity.
   * <p>
   *   The Hive function is CEILING.
   * <p>
   *   Positive numbers: If there is a round fraction, throw it away and add 1
   * <p>
   *       (Example here rounds at scale 0)
   *       Value        CEILING
   *        0.3           1
   *        2             2
   *        2.1           3
   * <p>
   *   Negative numbers: The round fraction is thrown away.
   * <p>
   *       (Example here rounds at scale 0)
   *       Value        CEILING
   *       -0.3           0
   *       -2             -2
   *       -2.1           -2
   */
  @HiveDecimalVersionV1
  public static final int ROUND_CEILING = BigDecimal.ROUND_CEILING;

  /**
   * ROUND_HALF_UP:
   * <p>
   *   Round towards "nearest neighbor" unless both neighbors are equidistant then round up.
   * <p>
   *   The Hive function is ROUND.
   * <p>
   *   For result, throw away round fraction.  If the round fraction is &gt;= 0.5, then add 1 when
   *   positive and subtract 1 when negative.  So, the sign is irrelevant.
   * <p>
   *      (Example here rounds at scale 0)
   *       Value        ROUND                  Value        ROUND
   *       0.3           0                     -0.3           0
   *       2             2                     -2            -2
   *       2.1           2                     -2.1          -2
   *       2.49          2                     -2.49         -2
   *       2.5           3                     -2.5          -3
   *
   */
  @HiveDecimalVersionV1
  public static final int ROUND_HALF_UP = BigDecimal.ROUND_HALF_UP;

  /**
   * ROUND_HALF_EVEN:
   *   Round towards the "nearest neighbor" unless both neighbors are equidistant, then round
   *   towards the even neighbor.
   * <p>
   *   The Hive function is BROUND.
   * <p>
   *   Known as Banker’s Rounding.
   * <p>
   *   When you add values rounded with ROUND_HALF_UP you have a bias that grows as you add more
   *   numbers.  Banker's Rounding is a way to minimize that bias.  It rounds toward the nearest
   *   even number when the fraction is 0.5 exactly.  In table below, notice that 2.5 goes DOWN to
   *   2 (even) but 3.5 goes UP to 4 (even), etc.
   * <p>
   *   So, the sign is irrelevant.
   * <p>
   *       (Example here rounds at scale 0)
   *       Value        BROUND                  Value        BROUND
   *        0.49          0                     -0.49          0
   *        0.5           0                     -0.5           0
   *        0.51          1                     -0.51         -1
   *        1.5           2                     -1.5          -2
   *        2.5           2                     -2.5          -2
   *        2.51          3                     -2.51         -3
   *        3.5           4                     -3.5          -4
   *        4.5           4                     -4.5          -4
   *        4.51          5                     -4.51         -5
   *
   */
  @HiveDecimalVersionV1
  public static final int ROUND_HALF_EVEN = BigDecimal.ROUND_HALF_EVEN;

  //-----------------------------------------------------------------------------------------------
  // Constructors are marked private; use create methods.
  //-----------------------------------------------------------------------------------------------

  private HiveDecimal() {
    super();
  }

  private HiveDecimal(HiveDecimal dec) {
    super(dec);
  }

  private HiveDecimal(FastHiveDecimal fastDec) {
    super(fastDec);
  }

  private HiveDecimal(int fastSignum, FastHiveDecimal fastDec) {
    super(fastSignum, fastDec);
  }

  private HiveDecimal(
      int fastSignum, long fast0, long fast1, long fast2,
      int fastIntegerDigitCount, int fastScale) {
    super(fastSignum, fast0, fast1, fast2, fastIntegerDigitCount, fastScale);
  }

  //-----------------------------------------------------------------------------------------------
  // Create methods.
  //-----------------------------------------------------------------------------------------------

  /**
   * Create a HiveDecimal from a FastHiveDecimal object. Used by HiveDecimalWritable.
   * @param fastDec the value to set
   * @return new hive decimal
   */
  @HiveDecimalVersionV2
  public static HiveDecimal createFromFast(FastHiveDecimal fastDec) {
    return new HiveDecimal(fastDec);
  }

  /**
   * Create a HiveDecimal from BigDecimal object.
   *
   * A BigDecimal object has a decimal scale.
   *
   * We will have overflow if BigDecimal's integer part exceed MAX_PRECISION digits or
   * 99,999,999,999,999,999,999,999,999,999,999,999,999 or 10^38 - 1.
   *
   * When the BigDecimal value's precision exceeds MAX_PRECISION and there are fractional digits
   * because of scale &gt; 0, then lower digits are trimmed off with rounding to meet the
   * MAX_PRECISION requirement.
   *
   * Also, BigDecimal supports negative scale -- which means multiplying the value by 10^abs(scale).
   * And, BigDecimal allows for a non-zero scale for zero.  We normalize that so zero always has
   * scale 0.
   *
   * @param bigDecimal the value to set
   * @return  The HiveDecimal with the BigDecimal's value adjusted down to a maximum precision.
   *          Otherwise, null is returned for overflow.
   */
  @HiveDecimalVersionV1
  public static HiveDecimal create(BigDecimal bigDecimal) {
    return create(bigDecimal, true);
  }

  /**
   * Same as the above create method, except fractional digit rounding can be turned off.
   * @param bigDecimal the value to set
   * @param allowRounding  True requires all of the bigDecimal value be converted to the decimal
   *                       without loss of precision.
   * @return
   */
  @HiveDecimalVersionV1
  public static HiveDecimal create(BigDecimal bigDecimal, boolean allowRounding) {
    HiveDecimal result = new HiveDecimal();
    if (!result.fastSetFromBigDecimal(
        bigDecimal, allowRounding)) {
      return null;
    }
    return result;
  }

  /**
   * Creates a HiveDecimal from a BigInteger's value with a scale of 0.
   *
   * We will have overflow if BigInteger exceed MAX_PRECISION digits or
   * 99,999,999,999,999,999,999,999,999,999,999,999,999 or 10^38 - 1.
   *
   * @param bigInteger the value to set
   * @return  A HiveDecimal object with the exact BigInteger's value.
   *          Otherwise, null is returned on overflow.
   */
  @HiveDecimalVersionV1
  public static HiveDecimal create(BigInteger bigInteger) {
    HiveDecimal result = new HiveDecimal();
    if (!result.fastSetFromBigInteger(
        bigInteger)) {
      return null;
    }
    return result;
  }

  /**
   * Creates a HiveDecimal from a BigInteger's value with a specified scale.
   *
   * We will have overflow if BigInteger exceed MAX_PRECISION digits or
   * 99,999,999,999,999,999,999,999,999,999,999,999,999 or 10^38 - 1.
   *
   * The resulting decimal will have fractional digits when the specified scale is greater than 0.
   *
   * When the BigInteger's value's precision exceeds MAX_PRECISION and there are fractional digits
   * because of scale &gt; 0, then lower digits are trimmed off with rounding to meet the
   * MAX_PRECISION requirement.
   *
   * @param bigInteger the value to set
   * @param scale the scale to set
   * @return  A HiveDecimal object with the BigInteger's value adjusted for scale.
   *          Otherwise, null is returned on overflow.
   */
  @HiveDecimalVersionV1
  public static HiveDecimal create(BigInteger bigInteger, int scale) {
    HiveDecimal result = new HiveDecimal();
    if (!result.fastSetFromBigIntegerAndScale(
        bigInteger, scale)) {
      return null;
    }
    return result;
  }

  /**
   * Create a HiveDecimal by parsing a whole string.
   *
   * We support parsing a decimal with an exponent because the previous version
   * (i.e. OldHiveDecimal) uses the BigDecimal parser and was able to.
   *
   * @param string the string to parse
   * @return a new hive decimal
   */
  @HiveDecimalVersionV1
  public static HiveDecimal create(String string) {
    HiveDecimal result = new HiveDecimal();
    if (!result.fastSetFromString(
        string, true)) {
      return null;
    }
    return result;
  }

  /**
   * Same as the method above, except blanks before and after are tolerated.
   * @param string the string to parse
   * @param trimBlanks  True specifies leading and trailing blanks are to be ignored.
   * @return a new hive decimal
   */
  @HiveDecimalVersionV2
  public static HiveDecimal create(String string, boolean trimBlanks) {
    HiveDecimal result = new HiveDecimal();
    if (!result.fastSetFromString(
        string, trimBlanks)) {
      return null;
    }
    return result;
  }

  /**
   * Create a HiveDecimal by parsing the characters in a whole byte array.
   *
   * Same rules as create(String string) above.
   *
   */
  @HiveDecimalVersionV2
  public static HiveDecimal create(byte[] bytes) {
    HiveDecimal result = new HiveDecimal();
    if (!result.fastSetFromBytes(
        bytes, 0, bytes.length, false)) {
      return null;
    }
    return result;
  }

  /**
   * Same as the method above, except blanks before and after are tolerated.
   *
   */
  @HiveDecimalVersionV2
  public static HiveDecimal create(byte[] bytes, boolean trimBlanks) {
    HiveDecimal result = new HiveDecimal();
    if (!result.fastSetFromBytes(
        bytes, 0, bytes.length, trimBlanks)) {
      return null;
    }
    return result;
  }

  /**
   * This method takes in digits only UTF-8 characters, a sign flag, and a scale and returns
   * a decimal.
   */
  @HiveDecimalVersionV2
  public static HiveDecimal create(boolean isNegative, byte[] bytes, int scale) {
    HiveDecimal result = new HiveDecimal();
    if (!result.fastSetFromDigitsOnlyBytesAndScale(
        isNegative, bytes, 0, bytes.length, scale)) {
      return null;
    }
    if (isNegative) {
      result.fastNegate();
    }
    return result;
  }

  @HiveDecimalVersionV2
  public static HiveDecimal create(
      boolean isNegative, byte[] bytes, int offset, int length, int scale) {
    HiveDecimal result = new HiveDecimal();
    if (!result.fastSetFromDigitsOnlyBytesAndScale(
        isNegative, bytes, offset, length, scale)) {
      return null;
    }
    return result;
  }

  /**
   * Create a HiveDecimal by parsing the characters in a slice of a byte array.
   *
   * Same rules as create(String string) above.
   *
   */
  @HiveDecimalVersionV2
  public static HiveDecimal create(byte[] bytes, int offset, int length) {
    HiveDecimal result = new HiveDecimal();
    if (!result.fastSetFromBytes(
        bytes, offset, length, false)) {
      return null;
    }
    return result;
  }

  /**
   * Same as the method above, except blanks before and after are tolerated.
   *
   */
  @HiveDecimalVersionV2
  public static HiveDecimal create(
      byte[] bytes, int offset, int length, boolean trimBlanks) {
    HiveDecimal result = new HiveDecimal();
    if (!result.fastSetFromBytes(
        bytes, offset, length, trimBlanks)) {
      return null;
    }
    return result;
  }

  /**
   * Create a HiveDecimal object from an int.
   *
   */
  @HiveDecimalVersionV1
  public static HiveDecimal create(int intValue) {
    HiveDecimal result = new HiveDecimal();
    result.fastSetFromInt(intValue);
    return result;
  }

  /**
   * Create a HiveDecimal object from a long.
   *
   */
  @HiveDecimalVersionV1
  public static HiveDecimal create(long longValue) {
    HiveDecimal result = new HiveDecimal();
    result.fastSetFromLong(longValue);
    return result;
  }

  /**
   * Create a HiveDecimal object from a long with a specified scale.
   *
   */
  @HiveDecimalVersionV2
  public static HiveDecimal create(long longValue, int scale) {
    HiveDecimal result = new HiveDecimal();
    if (!result.fastSetFromLongAndScale(
        longValue, scale)) {
      return null;
    }
    return result;
  }

  /**
   * Create a HiveDecimal object from a float.
   * <p>
   * This method is equivalent to HiveDecimal.create(Float.toString(floatValue))
   */
  @HiveDecimalVersionV2
  public static HiveDecimal create(float floatValue) {
    HiveDecimal result = new HiveDecimal();
    if (!result.fastSetFromFloat(floatValue)) {
      return null;
    }
    return result;
  }

  /**
   * Create a HiveDecimal object from a double.
   * <p>
   * This method is equivalent to HiveDecimal.create(Double.toString(doubleValue))
   */
  @HiveDecimalVersionV2
  public static HiveDecimal create(double doubleValue) {
    HiveDecimal result = new HiveDecimal();
    if (!result.fastSetFromDouble(doubleValue)) {
      return null;
    }
    return result;
  }

  //-----------------------------------------------------------------------------------------------
  // Serialization methods.
  //-----------------------------------------------------------------------------------------------

  // The byte length of the scratch byte array that needs to be passed to serializationUtilsRead.
  @HiveDecimalVersionV2
  public static final int SCRATCH_BUFFER_LEN_SERIALIZATION_UTILS_READ =
      FAST_SCRATCH_BUFFER_LEN_SERIALIZATION_UTILS_READ;

  /**
   * Deserialize data written in the format used by the SerializationUtils methods
   * readBigInteger/writeBigInteger and create a decimal using the supplied scale.
   * <p>
   * ORC uses those SerializationUtils methods for its serialization.
   * <p>
   * A scratch bytes array is necessary to do the binary to decimal conversion for better
   * performance.  Pass a SCRATCH_BUFFER_LEN_SERIALIZATION_UTILS_READ byte array for scratchBytes.
   * <p>
   * @return The deserialized decimal or null if the conversion failed.
   */
  @HiveDecimalVersionV2
  public static HiveDecimal serializationUtilsRead(
      InputStream inputStream, int scale,
      byte[] scratchBytes)
      throws IOException {
    HiveDecimal result = new HiveDecimal();
    if (!result.fastSerializationUtilsRead(
        inputStream, scale,
        scratchBytes)) {
      return null;
    }
    return result;
  }

  /**
   * Convert bytes in the format used by BigInteger's toByteArray format (and accepted by its
   * constructor) into a decimal using the specified scale.
   * <p>
   * Our bigIntegerBytes methods create bytes in this format, too.
   * <p>
   * This method is designed for high performance and does not create an actual BigInteger during
   * binary to decimal conversion.
   *
   */
  @HiveDecimalVersionV2
  public static HiveDecimal createFromBigIntegerBytesAndScale(
      byte[] bytes, int scale) {
    HiveDecimal result = new HiveDecimal();
    if (!result.fastSetFromBigIntegerBytesAndScale(
        bytes, 0, bytes.length, scale)) {
      return null;
    }
    return result;
  }

  @HiveDecimalVersionV2
  public static HiveDecimal createFromBigIntegerBytesAndScale(
      byte[] bytes, int offset, int length, int scale) {
    HiveDecimal result = new HiveDecimal();
    if (!result.fastSetFromBigIntegerBytesAndScale(
        bytes, offset, length, scale)) {
      return null;
    }
    return result;
  }

  // The length of the long array that needs to be passed to serializationUtilsWrite.
  @HiveDecimalVersionV2
  public static final int SCRATCH_LONGS_LEN = FAST_SCRATCH_LONGS_LEN;

  /**
   * Serialize this decimal's BigInteger equivalent unscaled value using the format that the
   * SerializationUtils methods readBigInteger/writeBigInteger use.
   * <p>
   * ORC uses those SerializationUtils methods for its serialization.
   * <p>
   * Scratch objects necessary to do the decimal to binary conversion without actually creating a
   * BigInteger object are passed for better performance.
   * <p>
   * Allocate scratchLongs with SCRATCH_LONGS_LEN longs.
   *
   */
  @HiveDecimalVersionV2
  public boolean serializationUtilsWrite(
      OutputStream outputStream,
      long[] scratchLongs)
          throws IOException {
    return
        fastSerializationUtilsWrite(
            outputStream,
            scratchLongs);
  }

  // The length of the scratch byte array that needs to be passed to bigIntegerBytes, etc.
  @HiveDecimalVersionV2
  public static final int SCRATCH_BUFFER_LEN_BIG_INTEGER_BYTES =
      FAST_SCRATCH_BUFFER_LEN_BIG_INTEGER_BYTES;

  /**
   * Return binary representation of this decimal's BigInteger equivalent unscaled value using
   * the format that the BigInteger's toByteArray method returns (and the BigInteger constructor
   * accepts).
   * <p>
   * Used by LazyBinary, Avro, and Parquet serialization.
   * <p>
   * Scratch objects necessary to do the decimal to binary conversion without actually creating a
   * BigInteger object are passed for better performance.
   * <p>
   * Allocate scratchLongs with SCRATCH_LONGS_LEN longs.
   * And, allocate buffer with SCRATCH_BUFFER_LEN_BIG_INTEGER_BYTES bytes.
   * <p>
   * @param scratchLongs
   * @param buffer
   * @return The number of bytes used for the binary result in buffer.  Otherwise, 0 if the
   *         conversion failed.
   */
  @HiveDecimalVersionV2
  public int bigIntegerBytes(
      long[] scratchLongs, byte[] buffer) {
    return
        fastBigIntegerBytes(
            scratchLongs, buffer);
  }

  @HiveDecimalVersionV2
  public byte[] bigIntegerBytes() {
    long[] scratchLongs = new long[SCRATCH_LONGS_LEN];
    byte[] buffer = new byte[SCRATCH_BUFFER_LEN_BIG_INTEGER_BYTES];
    final int byteLength =
        fastBigIntegerBytes(
            scratchLongs, buffer);
    return Arrays.copyOfRange(buffer, 0, byteLength);
  }

  /**
   * Convert decimal to BigInteger binary bytes with a serialize scale, similar to the formatScale
   * for toFormatString.  It adds trailing zeroes the (emulated) BigInteger toByteArray result
   * when a serializeScale is greater than current scale.  Or, rounds if scale is less than
   * current scale.
   * <p>
   * Used by Avro and Parquet serialization.
   * <p>
   * This emulates the OldHiveDecimal setScale AND THEN OldHiveDecimal getInternalStorage() behavior.
   *
   */
  @HiveDecimalVersionV2
  public int bigIntegerBytesScaled(
      int serializeScale,
      long[] scratchLongs, byte[] buffer) {
    return
        fastBigIntegerBytesScaled(
            serializeScale,
            scratchLongs, buffer);
  }

  @HiveDecimalVersionV2
  public byte[] bigIntegerBytesScaled(int serializeScale) {
    long[] scratchLongs = new long[SCRATCH_LONGS_LEN];
    byte[] buffer = new byte[SCRATCH_BUFFER_LEN_BIG_INTEGER_BYTES];
    int byteLength =
        fastBigIntegerBytesScaled(
            serializeScale,
            scratchLongs, buffer);
    return Arrays.copyOfRange(buffer, 0, byteLength);
  }

  //-----------------------------------------------------------------------------------------------
  // Convert to string/UTF-8 ASCII bytes methods.
  //-----------------------------------------------------------------------------------------------

  /**
   * Return a string representation of the decimal.
   * <p>
   * It is the equivalent of calling bigDecimalValue().toPlainString -- it does not add exponent
   * notation -- but is much faster.
   * <p>
   * NOTE: If setScale(int serializationScale) was used to create the decimal object, then trailing
   * fractional digits will be added to display to the serializationScale.  Or, the display may
   * get rounded.  See the comments for that method.
   *
   */
  @HiveDecimalVersionV1
  @Override
  public String toString() {
    if (fastSerializationScale() != -1) {

      // Use the serialization scale and format the string with trailing zeroes (or
      // round the decimal) if necessary.
      return
          fastToFormatString(fastSerializationScale());
    } else {
      return
          fastToString();
    }
  }

  @HiveDecimalVersionV2
  public String toString(
      byte[] scratchBuffer) {
    if (fastSerializationScale() != -1) {

      // Use the serialization scale and format the string with trailing zeroes (or
      // round the decimal) if necessary.
      return
          fastToFormatString(
              fastSerializationScale(),
              scratchBuffer);
    } else {
      return
          fastToString(scratchBuffer);
    }
  }

  /**
   * Return a string representation of the decimal using the specified scale.
   * <p>
   * This method is designed to ALWAYS SUCCEED (unless the newScale parameter is out of range).
   * <p>
   * Is does the equivalent of a setScale(int newScale).  So, more than 38 digits may be returned.
   * See that method for more details on how this can happen.
   * <p>
   * @param formatScale The number of digits after the decimal point
   * @return The scaled decimal representation string representation.
   */
  @HiveDecimalVersionV1
  public String toFormatString(int formatScale) {
    return
        fastToFormatString(
            formatScale);
  }

  @HiveDecimalVersionV2
  public String toFormatString(int formatScale, byte[] scratchBuffer) {
    return
        fastToFormatString(
            formatScale,
            scratchBuffer);
  }

  @HiveDecimalVersionV2
  public String toDigitsOnlyString() {
    return
        fastToDigitsOnlyString();
  }

  // The length of the scratch buffer that needs to be passed to toBytes, toFormatBytes,
  // toDigitsOnlyBytes.
  @HiveDecimalVersionV2
  public final static int SCRATCH_BUFFER_LEN_TO_BYTES = FAST_SCRATCH_BUFFER_LEN_TO_BYTES;

  /**
   * Decimal to ASCII bytes conversion.
   * <p>
   * The scratch buffer will contain the result afterwards.  It should be
   * SCRATCH_BUFFER_LEN_TO_BYTES bytes long.
   * <p>
   * The result is produced at the end of the scratch buffer, so the return value is the byte
   * index of the first byte.  The byte slice is [byteIndex:SCRATCH_BUFFER_LEN_TO_BYTES-1].
   *
   */
  @HiveDecimalVersionV2
  public int toBytes(
      byte[] scratchBuffer) {
    return
        fastToBytes(
            scratchBuffer);
  }

  /**
   * This is the serialization version of decimal to string conversion.
   * <p>
   * It adds trailing zeroes when the formatScale is greater than the current scale.  Or, it
   * does round if the formatScale is less than the current scale.
   * <p>
   * Note that you can get more than 38 (MAX_PRECISION) digits in the output with this method.
   *
   */
  @HiveDecimalVersionV2
  public int toFormatBytes(
      int formatScale,
      byte[] scratchBuffer) {
    return
        fastToFormatBytes(
            formatScale,
            scratchBuffer);
  }

  /**
   * Convert decimal to just the digits -- no dot.
   * <p>
   * Currently used by BinarySortable serialization.
   * <p>
   * A faster way to get just the digits than calling unscaledValue.toString().getBytes().
   *
   */
  @HiveDecimalVersionV2
  public int toDigitsOnlyBytes(
      byte[] scratchBuffer) {
    return
        fastToDigitsOnlyBytes(
            scratchBuffer);
  }

  //-----------------------------------------------------------------------------------------------
  // Comparison methods.
  //-----------------------------------------------------------------------------------------------

  @HiveDecimalVersionV1
  @Override
  public int compareTo(HiveDecimal dec) {
    return fastCompareTo(dec);
  }

  /**
   * Hash code based on (new) decimal representation.
   * <p>
   * Faster than hashCode().
   * <p>
   * Used by map join and other Hive internal purposes where performance is important.
   * <p>
   * IMPORTANT: See comments for hashCode(), too.
   */
  @HiveDecimalVersionV2
  public int newFasterHashCode() {
    return fastNewFasterHashCode();
  }

  /**
   * This is returns original hash code as returned by HiveDecimalV1.
   * <p>
   * We need this when the HiveDecimalV1 hash code has been exposed and and written or affected
   * how data is written.
   * <p>
   * This method supports compatibility.
   * <p>
   * Examples: bucketing, Hive hash() function, and Hive statistics.
   * <p>
   * NOTE: It is necessary to create a BigDecimal object and use its hash code, so this method is
   *       slow.
   */
  @HiveDecimalVersionV1
  @Override
  public int hashCode() {
    return fastHashCode();
  }

  /**
   * Are two decimal content (values) equal?
   * <p>
   * @param obj   The 2nd decimal.
   * @return  When obj is null or not class HiveDecimal, the return is false.
   *          Otherwise, returns true when the decimal values are exactly equal.
   */
  @HiveDecimalVersionV1
  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != getClass()) {
      return false;
    }
    return fastEquals((HiveDecimal) obj);
  }


  //-----------------------------------------------------------------------------------------------
  // Attribute methods.
  //-----------------------------------------------------------------------------------------------

  /**
   * Returns the scale of the decimal.  Range 0 .. MAX_SCALE.
   *
   */
  @HiveDecimalVersionV1
  public int scale() {
    return fastScale();
  }

  /**
   * Returns the number of integer digits in the decimal.
   * <p>
   * When the integer portion is zero, this method returns 0.
   *
   */
  @HiveDecimalVersionV2
  public int integerDigitCount() {
    return fastIntegerDigitCount();
  }

  /**
   * Returns the number of digits (integer and fractional) in the number, which is equivalent
   * to SQL decimal precision.
   * <p>
   * Note that this method is different from rawPrecision(), which returns the number of digits
   * ignoring the scale.  Note that rawPrecision returns 0 when the value is 0.
   *
   *     Decimal            precision              rawPrecision
   *        0                    1                         0
   *        1                    1                         1
   *       -7                    1                         1
   *       0.1                   1                         1
   *       0.04                  2                         1
   *       0.00380               5                         3
   *     104.0009                7                         7
   * <p>
   * If you just want the actual number of digits, use rawPrecision().
   *
   */
  @HiveDecimalVersionV1
  public int precision() {
    return fastSqlPrecision();
  }

  // See comments for sqlPrecision.
  @HiveDecimalVersionV2
  public int rawPrecision() {
    return fastRawPrecision();
  }

  /**
   * Get the sign of the decimal.
   * <p>
   * @return 0 if the decimal is equal to 0, -1 if less than zero, and 1 if greater than 0
   */
  @HiveDecimalVersionV1
  public int signum() {
    return fastSignum();
  }

  //-----------------------------------------------------------------------------------------------
  // Value conversion methods.
  //-----------------------------------------------------------------------------------------------

  /**
   * Is the decimal value a byte? Range -128            to      127.
   *                                    Byte.MIN_VALUE          Byte.MAX_VALUE
   * <p>
   * Emulates testing for no value corruption:
   *      bigDecimalValue().setScale(0).equals(BigDecimal.valueOf(bigDecimalValue().byteValue()))
   * <p>
   * NOTE: Fractional digits are ignored in the test since byteValue() will
   *       remove them (round down).
   * <p>
   * @return True when byteValue() will return a correct byte.
   */
  @HiveDecimalVersionV2
  public boolean isByte() {
    return fastIsByte();
  }

  /**
   * A byte variation of longValue()
   * <p>
   * This method will return a corrupted value unless isByte() is true.
   */
  @HiveDecimalVersionV1
  public byte byteValue() {
    return fastByteValueClip();
  }

  /**
   * Is the decimal value a short? Range -32,768         to     32,767.
   *                                     Short.MIN_VALUE        Short.MAX_VALUE
   * <p>
   * Emulates testing for no value corruption:
   *      bigDecimalValue().setScale(0).equals(BigDecimal.valueOf(bigDecimalValue().shortValue()))
   * <p>
   * NOTE: Fractional digits are ignored in the test since shortValue() will
   *       remove them (round down).
   * <p>
   * @return True when shortValue() will return a correct short.
   */
  @HiveDecimalVersionV2
  public boolean isShort() {
    return fastIsShort();
  }

  /**
   * A short variation of longValue().
   * <p>
   * This method will return a corrupted value unless isShort() is true.
   */
  @HiveDecimalVersionV1
  public short shortValue() {
    return fastShortValueClip();
  }

  /**
   * Is the decimal value a int? Range -2,147,483,648     to   2,147,483,647.
   *                                   Integer.MIN_VALUE       Integer.MAX_VALUE
   * <p>
   * Emulates testing for no value corruption:
   *      bigDecimalValue().setScale(0).equals(BigDecimal.valueOf(bigDecimalValue().intValue()))
   * <p>
   * NOTE: Fractional digits are ignored in the test since intValue() will
   *       remove them (round down).
   * <p>
   * @return True when intValue() will return a correct int.
   */
  @HiveDecimalVersionV2
  public boolean isInt() {
    return fastIsInt();
  }

  /**
   * An int variation of longValue().
   * <p>
   * This method will return a corrupted value unless isInt() is true.
   */
  @HiveDecimalVersionV1
  public int intValue() {
    return fastIntValueClip();
  }

  /**
   * Is the decimal value a long? Range -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807.
   *                                    Long.MIN_VALUE                Long.MAX_VALUE
   * <p>
   * Emulates testing for no value corruption:
   *      bigDecimalValue().setScale(0).equals(BigDecimal.valueOf(bigDecimalValue().longValue()))
   * <p>
   * NOTE: Fractional digits are ignored in the test since longValue() will
   *       remove them (round down).
   * <p>
   * @return True when longValue() will return a correct long.
   */
  @HiveDecimalVersionV2
  public boolean isLong() {
    return fastIsLong();
  }

  /**
   * Return the long value of a decimal.
   * <p>
   * This method will return a corrupted value unless isLong() is true.
   */
  @HiveDecimalVersionV1
  public long longValue() {
    return fastLongValueClip();
  }

  @HiveDecimalVersionV1
  public long longValueExact() {
    if (!isLong()) {
      throw new ArithmeticException();
    }
    return fastLongValueClip();
  }

  /**
   * Return a float representing the decimal.  Due the limitations of float, some values will not
   * be accurate.
   *
   */
  @HiveDecimalVersionV1
  public float floatValue() {
    return fastFloatValue();
   }

  /**
   * Return a double representing the decimal.  Due the limitations of double, some values will not
   * be accurate.
   *
   */
  @HiveDecimalVersionV1
  public double doubleValue() {
    return fastDoubleValue();
  }

  /**
   * Return a BigDecimal representing the decimal.  The BigDecimal class is able to accurately
   * represent the decimal.
   *
   * NOTE: We are not representing our decimal as BigDecimal now as OldHiveDecimal did, so this
   * is now slower.
   *
   */
  @HiveDecimalVersionV1
  public BigDecimal bigDecimalValue() {
    return fastBigDecimalValue();
  }

  /**
   * Get a BigInteger representing the decimal's digits without a dot.
   * <p>
   * @return Returns a signed BigInteger.
   */
  @HiveDecimalVersionV1
  public BigInteger unscaledValue() {
    return fastBigIntegerValue();
  }

  /**
   * Return a decimal with only the fractional digits.
   * <p>
   * Zero is returned when there are no fractional digits (i.e. scale is 0).
   *
   */
  @HiveDecimalVersionV2
  public HiveDecimal fractionPortion() {
    HiveDecimal result = new HiveDecimal();
    result.fastFractionPortion();
    return result;
  }

  /**
   * Return a decimal with only the integer digits.
   * <p>
   * Any fractional digits are removed.  E.g. 2.083 scale 3 returns as 2 scale 0.
   *
   */
  @HiveDecimalVersionV2
  public HiveDecimal integerPortion() {
    HiveDecimal result = new HiveDecimal();
    result.fastIntegerPortion();
    return result;
  }

  //-----------------------------------------------------------------------------------------------
  // Math methods.
  //-----------------------------------------------------------------------------------------------

  /**
   * Add the current decimal and another decimal and return the result.
   *
   */
  @HiveDecimalVersionV1
  public HiveDecimal add(HiveDecimal dec) {
    HiveDecimal result = new HiveDecimal();
    if (!fastAdd(
        dec,
        result)) {
      return null;
    }
    return result;
  }

  /**
   * Subtract from the current decimal another decimal and return the result.
   *
   */
  @HiveDecimalVersionV1
  public HiveDecimal subtract(HiveDecimal dec) {
    HiveDecimal result = new HiveDecimal();
    if (!fastSubtract(
        dec,
        result)) {
      return null;
    }
    return result;
  }

  /**
   * Multiply two decimals.
   * <p>
   * NOTE: Overflow Determination for Multiply
   * <p>
   *   OldDecimal.multiply performs the multiply with BigDecimal but DOES NOT ALLOW ROUNDING
   *   (i.e. no throwing away lower fractional digits).
   * <p>
   *   CONSIDER: Allowing rounding.  This would eliminate cases today where we return null for
   *             the multiplication result.
   * <p>
   * IMPLEMENTATION NOTE: HiveDecimalV1 code does this:
   * <p>
   * return create(bd.multiply(dec.bd), false);
   */
  @HiveDecimalVersionV1
  public HiveDecimal multiply(HiveDecimal dec) {
    HiveDecimal result = new HiveDecimal();
    if (!fastMultiply(
        dec,
        result)) {
      return null;
    }
    return result;
  }

  /**
   * Multiplies a decimal by a power of 10.
   * <p>
   * The decimal 19350 scale 0 will return 193.5 scale 1 when power is -2 (negative).
   * <p>
   * The decimal 1.000923 scale 6 will return 10009.23 scale 2 when power is 4 (positive).
   * <p>
   * @param power
   * @return Returns a HiveDecimal whose value is value * 10^power.
   */
  @HiveDecimalVersionV1
  public HiveDecimal scaleByPowerOfTen(int power) {
    if (power == 0 || fastSignum() == 0) {
      // No change for multiply by 10^0 or value 0.
      return this;
    }
    HiveDecimal result = new HiveDecimal();
    if (!fastScaleByPowerOfTen(
        power,
        result)) {
      return null;
    }
    return result;
  }

  /**
   * Take the absolute value of a decimal.
   * <p>
   * @return When the decimal is negative, returns a new HiveDecimal with the positive value.
   *         Otherwise, returns the current 0 or positive value object;
   */
  @HiveDecimalVersionV1
  public HiveDecimal abs() {
    if (fastSignum() != -1) {
      return this;
    }
    HiveDecimal result = new HiveDecimal(this);
    result.fastAbs();
    return result;
  }

  /**
   * Reverse the sign of a decimal.
   * <p>
   * @return Returns a new decimal with the sign flipped.  When the value is 0, the current
   * object is returned.
   */
  @HiveDecimalVersionV1
  public HiveDecimal negate() {
    if (fastSignum() == 0) {
      return this;
    }
    HiveDecimal result = new HiveDecimal(this);
    result.fastNegate();
    return result;
  }

  //-----------------------------------------------------------------------------------------------
  // Rounding / setScale methods.
  //-----------------------------------------------------------------------------------------------

  /**
   * DEPRECATED for V2.
   * <p>
   * Create a decimal from another decimal whose only change is it is MARKED and will display /
   * serialize with a specified scale that will add trailing zeroes (or round) if necessary.
   * <p>
   * After display / serialization, the MARKED object is typically thrown away.
   * <p>
   * A MARKED decimal ONLY affects these 2 methods since these were the only ways setScale was
   * used in the old code.
   * <p>
   *    toString
   *    unscaleValue
   * <p>
   * This method has been deprecated because has poor performance by creating a throw away object.
   * <p>
   * For setScale(scale).toString() use toFormatString(scale) instead.
   * For setScale(scale).unscaledValue().toByteArray() use V2 bigIntegerBytesScaled(scale) instead.
   * <p>
   * For better performance, use the V2 form of toFormatString that takes a scratch buffer,
   * or even better use toFormatBytes.
   * <p>
   * And, use the form of bigIntegerBytesScaled that takes scratch objects for better performance.
   *
   */
  @Deprecated
  @HiveDecimalVersionV1
  public HiveDecimal setScale(int serializationScale) {
    HiveDecimal result = new HiveDecimal(this);
    result.fastSetSerializationScale(serializationScale);
    return result;
  }

  /**
   * Do decimal rounding and return the result.
   * <p>
   * When the roundingPoint is 0 or positive, we round away lower fractional digits if the
   * roundingPoint is less than current scale.  In this case, we will round the result using the
   * specified rounding mode.
   * <p>
   * When the roundingPoint is negative, the rounding will occur within the integer digits.  Integer
   * digits below the roundPoint will be cleared.  If the rounding occurred, a one will be added
   * just above the roundingPoint.  Note this may cause overflow.
   * <p>
   * No effect when the roundingPoint equals the current scale.  The current object is returned.
   * <p>
   * The name setScale is taken from BigDecimal.setScale -- a better name would have been round.
   *
   */
  @HiveDecimalVersionV1
  public HiveDecimal setScale(
      int roundingPoint, int roundingMode) {
    if (fastScale() == roundingPoint) {
      // No change.
      return this;
    }

    // Even if we are just setting the scale when newScale is greater than the current scale,
    // we need a new object to obey our immutable behavior.
    HiveDecimal result = new HiveDecimal();
    if (!fastRound(
        roundingPoint, roundingMode,
        result)) {
      return null;
    }
    return result;
  }

  /**
   * Return the result of decimal^exponent
   * <p>
   * CONSIDER: Currently, negative exponent is not supported.
   * CONSIDER: Does anybody use this method?
   *
   */
  @HiveDecimalVersionV1
  public HiveDecimal pow(int exponent) {
    HiveDecimal result = new HiveDecimal(this);
    if (!fastPow(
        exponent, result)) {
      return null;
    }
    return result;
  }

  /**
   * Divides this decimal by another decimal and returns a new decimal with the result.
   *
   */
  @HiveDecimalVersionV1
  public HiveDecimal divide(HiveDecimal divisor) {
    HiveDecimal result = new HiveDecimal();
    if (!fastDivide(
        divisor,
        result)) {
      return null;
    }
    return result;
  }

  /**
   * Divides this decimal by another decimal and returns a new decimal with the remainder of the
   * division.
   * <p>
   * value is (decimal % divisor)
   * <p>
   * The remainder is equivalent to BigDecimal:
   *    bigDecimalValue().subtract(bigDecimalValue().divideToIntegralValue(divisor).multiply(divisor))
   *
   */
  @HiveDecimalVersionV1
  public HiveDecimal remainder(HiveDecimal divisor) {
    HiveDecimal result = new HiveDecimal();
    if (!fastRemainder(
        divisor,
        result)) {
      return null;
    }
    return result;
  }

  //-----------------------------------------------------------------------------------------------
  // Precision/scale enforcement methods.
  //-----------------------------------------------------------------------------------------------

  /**
   * Determine if a decimal fits within a specified maxPrecision and maxScale, and round
   * off fractional digits if necessary to make the decimal fit.
   * <p>
   * The relationship between the enforcement maxPrecision and maxScale is restricted. The
   * specified maxScale must be less than or equal to the maxPrecision.
   * <p>
   * Normally, decimals that result from creation operation, arithmetic operations, etc are
   * "free range" up to MAX_PRECISION and MAX_SCALE.  Each operation checks if the result decimal
   * is beyond MAX_PRECISION and MAX_SCALE.  If so the result decimal is rounded off using
   * ROUND_HALF_UP.  If the round digit is 5 or more, one is added to the lowest remaining digit.
   * The round digit is the digit just below the round point. Result overflow can occur if a
   * result decimal's integer portion exceeds MAX_PRECISION.
   * <p>
   * This method supports enforcing to a declared Hive DECIMAL's precision/scale.
   * E.g. DECIMAL(10,4)
   * <p>
   * Here are the enforcement/rounding checks of this method:
   * <p>
   *   1) Maximum integer digits = maxPrecision - maxScale
   * <p>
   *      If the decimal's integer digit count exceeds this, the decimal does not fit (overflow).
   * <p>
   *   2) If decimal's scale is greater than maxScale, then excess fractional digits are
   *      rounded off.  When rounding increases the remaining decimal, it may exceed the
   *      limits and overflow.
   * <p>
   * @param dec
   * @param maxPrecision
   * @param maxScale
   * @return The original decimal if no adjustment is necessary.
   *         A rounded off decimal if adjustment was necessary.
   *         Otherwise, null if the decimal doesn't fit within maxPrecision / maxScale or rounding
   *         caused a result that exceeds the specified limits or MAX_PRECISION integer digits.
   */
  @HiveDecimalVersionV1
  public static HiveDecimal enforcePrecisionScale(
      HiveDecimal dec, int maxPrecision, int maxScale) {

    if (maxPrecision < 1 || maxPrecision > MAX_PRECISION) {
      throw new IllegalArgumentException(STRING_ENFORCE_PRECISION_OUT_OF_RANGE);
    }

    if (maxScale < 0 || maxScale > HiveDecimal.MAX_SCALE) {
      throw new IllegalArgumentException(STRING_ENFORCE_SCALE_OUT_OF_RANGE);
    }

    if (maxPrecision < maxScale) {
      throw new IllegalArgumentException(STRING_ENFORCE_SCALE_LESS_THAN_EQUAL_PRECISION);
    }

    if (dec == null) {
      return null;
    }

    FastCheckPrecisionScaleStatus status =
        dec.fastCheckPrecisionScale(
            maxPrecision, maxScale);
    switch (status) {
    case NO_CHANGE:
      return dec;
    case OVERFLOW:
      return null;
    case UPDATE_SCALE_DOWN:
      {
        HiveDecimal result = new HiveDecimal();
        if (!dec.fastUpdatePrecisionScale(
          maxPrecision, maxScale, status,
          result)) {
          return null;
        }
        return result;
      }
    default:
      throw new RuntimeException("Unknown fast decimal check precision and scale status " + status);
    }
  }

  //-----------------------------------------------------------------------------------------------
  // Validation methods.
  //-----------------------------------------------------------------------------------------------

  /**
   * Throws an exception if the current decimal value is invalid.
   */
  @HiveDecimalVersionV2
  public void validate() {
    if (!fastIsValid()) {
      fastRaiseInvalidException();
    }
  }
}