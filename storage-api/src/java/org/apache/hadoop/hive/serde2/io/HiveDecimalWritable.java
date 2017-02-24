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
package org.apache.hadoop.hive.serde2.io;

import java.util.Arrays;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.FastHiveDecimal;
import org.apache.hadoop.hive.common.type.FastHiveDecimalImpl;
import org.apache.hadoop.hive.common.type.HiveDecimalVersionV2;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * A mutable decimal.
 * <p>
 * ------------------------------------- Version 2 ------------------------------------------------
 * <p>
 * This is the 2nd major version of HiveDecimalWritable called V2.  The previous version has been
 * renamed to HiveDecimalWritableV1 and is kept as a test and behavior reference.
 * <p>
 * For good performance we do not represent the decimal using a byte array containing BigInteger
 * bytes like the previous version V1 did.  Instead V2 HiveDecimalWritable is is a private subclass
 * of the same fast decimal class also used by HiveDecimal.  So it stores the decimal value
 * directly.
 * <p>
 * Many of the methods of HiveDecimal have been added to HiveDecimalWritable in V2 so code can
 * modify the decimal instead of calling getHiveDecimal(), doing operations on HiveDecimal, and then
 * setting HiveDecimalWritable back to the result.
 *  <p>
 * Operations that modify have a prefix of "mutate" in their name.  For example mutateAdd is used
 * instead of the immutable operation add in HiveDecimal that returns a new decimal object.
 * <p>
 * This should have much better performance.
 * <p>
 * The original V1 public methods and fields are annotated with @HiveDecimalWritableVersionV1; new
 * public methods and fields are annotated with @HiveDecimalWritableVersionV2.
 *
 */
public final class HiveDecimalWritable extends FastHiveDecimal
    implements WritableComparable<HiveDecimalWritable> {

  // Is the decimal value currently valid?
  private boolean isSet;

  /*
   * Scratch arrays used in fastBigIntegerBytes calls for better performance.
   */

  // An optional long array of length FastHiveDecimal.FAST_SCRATCH_LONGS_LEN.
  private long[] internalScratchLongs;

  // An optional byte array of FastHiveDecimal.FAST_SCRATCH_BUFFER_LEN_BIG_INTEGER_BYTES.
  private byte[] internalScratchBuffer;

  /**
   * Create a decimal writable with no current value (i.e. isSet() will return false).
   */
  @HiveDecimalWritableVersionV1
  public HiveDecimalWritable() {
    super();
    isSet = false;
    internalScratchLongs = null;
    internalScratchBuffer = null;
  }

  /**
   * Create a decimal writable with an initial value from a String.
   * <p>
   * If the conversion from String to decimal is successful, isSet() will return true.
   *
   */
  @HiveDecimalWritableVersionV1
  public HiveDecimalWritable(String string) {
    super();
    isSet = fastSetFromString(string, false);
    if (!isSet) {
      fastReset();
    }
  }

  /**
   * Create a decimal writable with an initial value from BigInteger bytes and a specified scale.
   * <p>
   * If the conversion to decimal is successful, isSet() will return true.
   *
   */
  @HiveDecimalWritableVersionV1
  public HiveDecimalWritable(byte[] bigIntegerBytes, int scale) {
    super();
    setFromBigIntegerBytesAndScale(bigIntegerBytes, scale);
  }

  /**
   * Create a decimal writable with an initial value from another decimal writable.
   * <p>
   * If the supplied writable has a value, isSet() will return true.
   *
   */
  @HiveDecimalWritableVersionV1
  public HiveDecimalWritable(HiveDecimalWritable writable) {
    super();
    set(writable);
  }

  /**
   * Create a decimal writable with an initial value from a HiveDecimal.
   * <p>
   * Afterwards, the isSet() method will return true, unless value is null.
   *
   */
  @HiveDecimalWritableVersionV1
  public HiveDecimalWritable(HiveDecimal value) {
    super();
    set(value);
  }

  /**
   * Create a decimal writable with an initial value from a long with scale 0.
   * <p>
   * Afterwards, the isSet() method will return true.
   *
   */
  @HiveDecimalWritableVersionV1
  public HiveDecimalWritable(long longValue) {
    super();
    setFromLong(longValue);
  }

  /**
   * Set the writable's current value to a HiveDecimal's value.
   * <p>
   * Afterwards, the isSet() method will return true, unless value is null.
   *
   */
  @HiveDecimalWritableVersionV1
  public void set(HiveDecimal value) {
    if (value == null) {
      fastReset();
      isSet = false;
    } else {
      fastSet(value);
      isSet = true;
    }
  }

  /**
   * Set the writable's current value to a HiveDecimal's value with a specified precision / scale
   * enforced.
   * <p>
   * Afterwards, the isSet() method will return true, unless value is null or value didn't fit within
   * maxPrecision / maxScale.
   *
   */
  @HiveDecimalWritableVersionV1
  public void set(HiveDecimal value, int maxPrecision, int maxScale) {
    set(value);
    if (isSet) {
      isSet = fastEnforcePrecisionScale(maxPrecision, maxScale);
      if (!isSet) {
        fastReset();
      }
    }
  }

  /**
   * Set the writable's current value to the value in a another decimal writable.
   * <p>
   * If the supplied writable has a value, isSet() will return true.
   *
   */
  @HiveDecimalWritableVersionV1
  public void set(HiveDecimalWritable writable) {
    if (writable == null || !writable.isSet()) {
      fastReset();
      isSet = false;
    } else {
      fastSet(writable);
      isSet = true;
    }
  }

  /**
   * Set a decimal writable's value from BigInteger bytes and a specified scale.
   * <p>
   * If the conversion to decimal is successful, isSet() will return true.
   *
   */
  @HiveDecimalWritableVersionV1
  public void set(byte[] bigIntegerBytes, int scale) {
    setFromBigIntegerBytesAndScale(bigIntegerBytes, scale);
  }

  /**
   * Set the writable's current value to a writable's value with a specified precision / scale
   * enforced.
   * <p>
   * The isSet() method will return true, unless value is null or value didn't fit within
   * maxPrecision / maxScale.
   *
   */
  @HiveDecimalWritableVersionV2
  public void set(HiveDecimalWritable writable, int maxPrecision, int maxScale) {
    set(writable);
    if (isSet) {
      isSet = fastEnforcePrecisionScale(maxPrecision, maxScale);
      if (!isSet) {
        fastReset();
      }
    }
  }

  /**
   * Set a decimal writable's value to a long's value with scale 0.
   * <p>
   * Afterwards, the isSet() method will return true since all long values fit in a decimal.
   *
   */
  @HiveDecimalWritableVersionV2
  public void setFromLong(long longValue) {
    fastReset();
    fastSetFromLong(longValue);
    isSet = true;
  }

  /**
   * Set a decimal writable's value to a doubles value.
   * <p>
   * Afterwards, the isSet() method will return true if the double to decimal conversion was successful.
   *
   */
  @HiveDecimalWritableVersionV2
  public void setFromDouble(double doubleValue) {
    fastReset();
    isSet = fastSetFromDouble(doubleValue);
    if (!isSet) {
      fastReset();
    }
  }

  /**
   * Set the writable's current value to the decimal in a UTF-8 byte slice.
   * <p>
   * Afterwards, the isSet() method will return true, unless byte slice could not be converted.
   *
   */
  @HiveDecimalWritableVersionV2
  public void setFromBytes(byte[] bytes, int offset, int length) {
    fastReset();
    isSet = fastSetFromBytes(bytes, offset, length, false);
    if (!isSet) {
      fastReset();
    }
  }

  @HiveDecimalWritableVersionV2
  public void setFromBytes(byte[] bytes, int offset, int length, boolean trimBlanks) {
    fastReset();
    isSet = fastSetFromBytes(bytes, offset, length, trimBlanks);
    if (!isSet) {
      fastReset();
    }
  }

  /**
   * Set the writable's current value to the decimal digits only in a UTF-8 byte slice, a sign
   * flag, and a scale.
   * <p>
   * Afterwards, the isSet() method will return true, unless byte slice etc could not be converted.
   *
   */
  @HiveDecimalWritableVersionV2
  public void setFromDigitsOnlyBytesWithScale(
      boolean isNegative, byte[] bytes, int offset, int length, int scale) {
    fastReset();
    isSet = fastSetFromDigitsOnlyBytesAndScale(isNegative, bytes, offset, length, scale);
    if (!isSet) {
      fastReset();
    }
  }

  /**
   * Set the writable's current value to the signed value from BigInteger bytes and a specified
   * scale.
   * <p>
   * Afterwards, the isSet() method will return true, unless conversion failed.
   *
   */
  @HiveDecimalWritableVersionV2
  public void setFromBigIntegerBytesAndScale(byte[] bigIntegerBytes, int scale) {
    fastReset();
    isSet = fastSetFromBigIntegerBytesAndScale(bigIntegerBytes, 0, bigIntegerBytes.length, scale);
    if (!isSet) {
      fastReset();
    }
  }

  @HiveDecimalWritableVersionV2
  public void setFromBigIntegerBytesAndScale(
      byte[] bigIntegerBytes, int offset, int length, int scale) {
    fastReset();
    isSet = fastSetFromBigIntegerBytesAndScale(bigIntegerBytes, offset, length, scale);
    if (!isSet) {
      fastReset();
    }
  }

  /**
   * Set the writable's current value to the long's value at a specified
   * scale.
   * <p>
   * Afterwards, the isSet() method will return true, unless conversion failed.
   *
   */
  @HiveDecimalWritableVersionV2
  public void setFromLongAndScale(long longValue, int scale) {
    fastReset();
    isSet = fastSetFromLongAndScale(longValue, scale);
    if (!isSet) {
      fastReset();
    }
  }

  /**
   * Does this writable have a current value?
   * <p>
   * A return of false means a current value wasn't set, or an operation like mutateAdd overflowed,
   * or a set* method couldn't convert the input value, etc.
   *
   */
  @HiveDecimalWritableVersionV2
  public boolean isSet() {
    return isSet;
  }

  /**
   * Returns a HiveDecimal for the writable's current value.
   * <p>
   * Returns null if the writable isn't set.
   *
   */
  @HiveDecimalWritableVersionV1
  public HiveDecimal getHiveDecimal() {
    if (!isSet) {
      return null;
    }
    HiveDecimal result = HiveDecimal.createFromFast(this);
    return result;
  }

  /**
   * Get a HiveDecimal instance from the writable and constraint it with maximum precision/scale.
   * <p>
   * @param maxPrecision maximum precision
   * @param maxScale maximum scale
   * @return HiveDecimal instance
   */
  @HiveDecimalWritableVersionV1
  public HiveDecimal getHiveDecimal(int maxPrecision, int maxScale) {
    if (!isSet) {
      return null;
    }
    HiveDecimal dec = HiveDecimal.createFromFast(this);
    HiveDecimal result = HiveDecimal.enforcePrecisionScale(dec, maxPrecision, maxScale);
    return result;
  }

  /**
   * Standard Writable method that deserialize the fields of this object from a DataInput.
   * 
   */
  @HiveDecimalWritableVersionV1
  @Override
  public void readFields(DataInput in) throws IOException {
    int scale = WritableUtils.readVInt(in);
    int byteArrayLen = WritableUtils.readVInt(in);
    byte[] bytes = new byte[byteArrayLen];
    in.readFully(bytes);

    fastReset();
    if (!fastSetFromBigIntegerBytesAndScale(bytes, 0, bytes.length, scale)) {
      throw new IOException("Couldn't convert decimal");
    }
    isSet = true;
  }

  /**
   * Standard Writable method that serialize the fields of this object to a DataOutput.
   *
   */
  @HiveDecimalWritableVersionV1
  @Override
  public void write(DataOutput out) throws IOException {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }

    if (internalScratchLongs == null) {
      internalScratchLongs = new long[FastHiveDecimal.FAST_SCRATCH_LONGS_LEN];
      internalScratchBuffer = new byte[FastHiveDecimal.FAST_SCRATCH_BUFFER_LEN_BIG_INTEGER_BYTES];
    }

    write(out, internalScratchLongs, internalScratchBuffer);
  }


  /**
   * A variation of the standard Writable method that serialize the fields of this object to a
   * DataOutput with scratch buffers for good performance.
   * <p>
   * Allocate scratchLongs with HiveDecimal.SCRATCH_LONGS_LEN longs.
   * And, allocate scratch buffer with HiveDecimal.SCRATCH_BUFFER_LEN_BIG_INTEGER_BYTES bytes.
   *
   */
  @HiveDecimalWritableVersionV2
  public void write(
      DataOutput out,
      long[] scratchLongs, byte[] scratchBuffer) throws IOException {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }

    WritableUtils.writeVInt(out, fastScale());

    int byteLength =
        fastBigIntegerBytes(
            scratchLongs, scratchBuffer);
    if (byteLength == 0) {
      throw new RuntimeException("Couldn't convert decimal to binary");
    }

    WritableUtils.writeVInt(out, byteLength);
    out.write(scratchBuffer, 0, byteLength);
  }

  /**
   * See the comments for HiveDecimal.serializationUtilsRead.
   */
  @HiveDecimalWritableVersionV2
  public boolean serializationUtilsRead(
      InputStream inputStream, int scale,
      byte[] scratchBytes)
          throws IOException {
    fastReset();
    isSet =
        fastSerializationUtilsRead(
            inputStream,
            scale,
            scratchBytes);
    return isSet;
  }

  /**
   * See the comments for HiveDecimal.serializationUtilsWrite.
   */
  @HiveDecimalWritableVersionV2
  public boolean serializationUtilsWrite(
      OutputStream outputStream,
      long[] scratchLongs)
          throws IOException {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    return
        fastSerializationUtilsWrite(
            outputStream,
            scratchLongs);
  }

  /**
   * Returns the length of the decimal converted to bytes.
   * Call bigIntegerBytesBuffer() to get a reference to the converted bytes.
   *
   */
  @HiveDecimalWritableVersionV2
  public int bigIntegerBytesInternalScratch() {

    if (!isSet()) {
      throw new RuntimeException("no value set");
    }

    if (internalScratchLongs == null) {
      internalScratchLongs = new long[FastHiveDecimal.FAST_SCRATCH_LONGS_LEN];
      internalScratchBuffer = new byte[FastHiveDecimal.FAST_SCRATCH_BUFFER_LEN_BIG_INTEGER_BYTES];
    }

    int byteLength =
        fastBigIntegerBytes(
            internalScratchLongs, internalScratchBuffer);
    if (byteLength == 0) {
      throw new RuntimeException("Couldn't convert decimal to binary");
    }
    return byteLength;
  }

  /**
   * Returns the scratch array containing the result after a call to bigIntegerBytesInternalScratch.
   *
   */
  @HiveDecimalWritableVersionV2
  public byte[] bigIntegerBytesInternalScratchBuffer() {
    return internalScratchBuffer;
  }

  /**
  * Allocate scratchLongs with HiveDecimal.SCRATCH_LONGS_LEN longs.
  * And, allocate scratch scratchBuffer with HiveDecimal.SCRATCH_BUFFER_LEN_BIG_INTEGER_BYTES bytes.
  *
  */
  @HiveDecimalWritableVersionV2
  public byte[] bigIntegerBytesCopy(
      long[] scratchLongs, byte[] scratchBuffer) {

    if (!isSet()) {
      throw new RuntimeException("no value set");
    }

    int byteLength =
        fastBigIntegerBytes(
            scratchLongs, scratchBuffer);
    if (byteLength == 0) {
      throw new RuntimeException("Couldn't convert decimal to binary");
    }
    return Arrays.copyOf(scratchBuffer, byteLength);
  }

  @HiveDecimalWritableVersionV2
  public int bigIntegerBytes(
      long[] scratchLongs, byte[] scratchBuffer) {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    int byteLength =
        fastBigIntegerBytes(
            scratchLongs, scratchBuffer);
    return byteLength;
  }

  @HiveDecimalWritableVersionV2
  public int signum() {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    return fastSignum();
  }

  @HiveDecimalWritableVersionV2
  public int precision() {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    return fastSqlPrecision();
  }

  @HiveDecimalWritableVersionV2
  public int rawPrecision() {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    return fastRawPrecision();
  }

  @HiveDecimalWritableVersionV2
  public int scale() {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    return fastScale();
  }

  @HiveDecimalWritableVersionV2
  public boolean isByte() {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    return fastIsByte();
  }

  @HiveDecimalWritableVersionV2
  public byte byteValue() {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    return fastByteValueClip();
  }

  @HiveDecimalWritableVersionV2
  public boolean isShort() {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    return fastIsShort();
  }

  @HiveDecimalWritableVersionV2
  public short shortValue() {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    return fastShortValueClip();
  }

  @HiveDecimalWritableVersionV2
  public boolean isInt() {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    return fastIsInt();
  }

  @HiveDecimalWritableVersionV2
  public int intValue() {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    return fastIntValueClip();
  }

  @HiveDecimalWritableVersionV2
  public boolean isLong() {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    return fastIsLong();
  }

  @HiveDecimalWritableVersionV2
  public long longValue() {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    return fastLongValueClip();
  }

  @HiveDecimalWritableVersionV2
  public float floatValue() {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    return fastFloatValue();
   }

  @HiveDecimalWritableVersionV2
  public double doubleValue() {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    return fastDoubleValue();
  }

  //-----------------------------------------------------------------------------------------------
  // Mutate operations.
  //-----------------------------------------------------------------------------------------------

  @HiveDecimalWritableVersionV2
  public void mutateAbs() {
    if (!isSet) {
      return;
    }
    fastAbs();
  }

  @HiveDecimalWritableVersionV2
  public void mutateNegate() {
    if (!isSet) {
      return;
    }
    fastNegate();
  }

  @HiveDecimalWritableVersionV2
  public void mutateAdd(HiveDecimalWritable decWritable) {
    if (!isSet || !decWritable.isSet) {
      isSet = false;
      return;
    }
    isSet =
        fastAdd(decWritable, this);
  }

  @HiveDecimalWritableVersionV2
  public void mutateAdd(HiveDecimal dec) {
    if (!isSet) {
      return;
    }
    isSet =
        fastAdd(dec, this);
  }

  @HiveDecimalWritableVersionV2
  public void mutateSubtract(HiveDecimalWritable decWritable) {
    if (!isSet || !decWritable.isSet) {
      isSet = false;
      return;
    }
    isSet =
        fastSubtract(decWritable, this);
  }

  @HiveDecimalWritableVersionV2
  public void mutateSubtract(HiveDecimal dec) {
    if (!isSet) {
      return;
    }
    isSet =
        fastSubtract(dec, this);
  }

  @HiveDecimalWritableVersionV2
  public void mutateMultiply(HiveDecimalWritable decWritable) {
    if (!isSet || !decWritable.isSet) {
      isSet = false;
      return;
    }
    isSet =
        fastMultiply(decWritable, this);
  }

  @HiveDecimalWritableVersionV2
  public void mutateMultiply(HiveDecimal dec) {
    if (!isSet) {
      return;
    }
    isSet =
        fastMultiply(dec, this);
  }

  @HiveDecimalWritableVersionV2
  public void mutateDivide(HiveDecimalWritable decWritable) {
    if (!isSet || !decWritable.isSet) {
      isSet = false;
      return;
    }
    isSet =
        fastDivide(decWritable, this);
  }

  @HiveDecimalWritableVersionV2
  public void mutateDivide(HiveDecimal dec) {
    if (!isSet) {
      return;
    }
    isSet =
        fastDivide(dec, this);

  }

  @HiveDecimalWritableVersionV2
  public void mutateRemainder(HiveDecimalWritable decWritable) {
    if (!isSet || !decWritable.isSet) {
      isSet = false;
      return;
    }
    isSet =
        fastRemainder(decWritable, this);
  }

  @HiveDecimalWritableVersionV2
  public void mutateRemainder(HiveDecimal dec) {
    if (!isSet) {
      return;
    }
    isSet =
        fastRemainder(dec, this);
  }

  @HiveDecimalWritableVersionV2
  public void mutateScaleByPowerOfTen(int power) {
    if (!isSet) {
      return;
    }
    isSet = fastScaleByPowerOfTen(power, this);
  }

  @HiveDecimalWritableVersionV2
  public void mutateFractionPortion() {
    if (!isSet) {
      return;
    }
    fastFractionPortion();
  }

  @HiveDecimalWritableVersionV2
  public void mutateIntegerPortion() {
    if (!isSet) {
      return;
    }
    fastIntegerPortion();
  }

  //-----------------------------------------------------------------------------------------------
  // Standard overrides methods.
  //-----------------------------------------------------------------------------------------------

  @HiveDecimalWritableVersionV1
  @Override
  public int compareTo(HiveDecimalWritable writable) {
    if (!isSet() || writable == null || !writable.isSet()) {
      throw new RuntimeException("Invalid comparision operand(s)");
    }
    return fastCompareTo(writable);
  }

  @HiveDecimalWritableVersionV2
  public int compareTo(HiveDecimal dec) {
    if (!isSet() || dec == null) {
      throw new RuntimeException("Invalid comparision operand(s)");
    }
    return fastCompareTo(dec);
  }

  @HiveDecimalWritableVersionV2
  public static int compareTo(HiveDecimal dec, HiveDecimalWritable writable) {
    if (dec == null || !writable.isSet()) {
      throw new RuntimeException("Invalid comparision operand(s)");
    }
    return FastHiveDecimal.fastCompareTo(dec, writable);
  }

  @HiveDecimalWritableVersionV2
  public int toBytes(byte[] scratchBuffer) {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    return fastToBytes(scratchBuffer);
  }

  @HiveDecimalWritableVersionV1
  @Override
  public String toString() {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    return fastToString();
  }

  @HiveDecimalWritableVersionV2
  public String toString(
      byte[] scratchBuffer) {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
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

  @HiveDecimalWritableVersionV2
  public String toFormatString(
      int formatScale) {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    return
        fastToFormatString(
            formatScale);
  }

  @HiveDecimalWritableVersionV2
  public int toFormatBytes(
      int formatScale,
      byte[] scratchBuffer) {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    return
        fastToFormatBytes(
            formatScale,
            scratchBuffer);
  }

  @HiveDecimalWritableVersionV2
  public int toDigitsOnlyBytes(
      byte[] scratchBuffer) {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    return
        fastToDigitsOnlyBytes(
            scratchBuffer);
  }

  @HiveDecimalWritableVersionV1
  @Override
  public boolean equals(Object other) {
    if (!isSet) {
      return false;
    }
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    HiveDecimalWritable otherHiveDecWritable = (HiveDecimalWritable) other;
    if (!otherHiveDecWritable.isSet()) {
      return false;
    }
    return fastEquals((FastHiveDecimal) otherHiveDecWritable);

  }

  @HiveDecimalWritableVersionV2
  public int newFasterHashCode() {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    return fastNewFasterHashCode();
  }

  @HiveDecimalWritableVersionV1
  @Override
  public int hashCode() {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    return fastHashCode();
  }

  private static final byte[] EMPTY_ARRAY = new byte[0];

  @HiveDecimalWritableVersionV1
  public byte[] getInternalStorage() {
    if (!isSet()) {
      // don't break old callers that are trying to reuse storages
      return EMPTY_ARRAY;
    }

    if (internalScratchLongs == null) {
      internalScratchLongs = new long[FastHiveDecimal.FAST_SCRATCH_LONGS_LEN];
      internalScratchBuffer = new byte[FastHiveDecimal.FAST_SCRATCH_BUFFER_LEN_BIG_INTEGER_BYTES];
    }

    return bigIntegerBytesCopy(
        internalScratchLongs, internalScratchBuffer);
  }

  @HiveDecimalWritableVersionV1
  public int getScale() {
    if (!isSet()) {
      throw new RuntimeException("no value set");
    }
    return fastScale();
  }

  @HiveDecimalWritableVersionV2
  public void mutateSetScale(int roundingPoint, int roundingMode) {
    if (!isSet) {
      return;
    }
    isSet = fastRound(roundingPoint, roundingMode, this);
    if (!isSet) {
      fastReset();
    }
  }

  @HiveDecimalWritableVersionV2
  public boolean mutateEnforcePrecisionScale(int precision, int scale) {
    if (!isSet) {
      return false;
    }
    isSet = fastEnforcePrecisionScale(precision, scale);
    if (!isSet) {
      fastReset();
    }
    return isSet;
  }

  @HiveDecimalWritableVersionV1
  public static HiveDecimalWritable enforcePrecisionScale(HiveDecimalWritable writable, int precision, int scale) {
    if (!writable.isSet) {
      return null;
    }
    HiveDecimalWritable result = new HiveDecimalWritable(writable);
    result.mutateEnforcePrecisionScale(precision, scale);
    if (!result.isSet()) {
      return null;
    }
    return result;
  }
}
