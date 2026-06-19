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

package org.apache.hadoop.hive.ql.io.parquet.vector;

import org.apache.hadoop.hive.common.type.HiveBaseChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.io.parquet.convert.ETypeConverter;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.ParquetTimestampUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Optional;
import java.util.TimeZone;

import static com.google.common.base.MoreObjects.firstNonNull;

/**
 * Parquet file has self-describing schema which may differ from the user required schema (e.g.
 * schema evolution). This factory is used to retrieve user required typed data via corresponding
 * reader which reads the underlying data.
 */
public final class ParquetDataColumnReaderFactory {

  private ParquetDataColumnReaderFactory() {
  }

  /**
   * The default data column reader for existing Parquet page reader which works for both
   * dictionary or non dictionary types, Mirror from dictionary encoding path.
   *
   * HMS metadata can have a typename that is different from the type of the parquet data.
   * If the data is valid as per the definition of the type in HMS, the data will be returned
   * as part of hive query.  If the data is invalid, null will be returned.
   */
  public static class DefaultParquetDataColumnReader implements ParquetDataColumnReader {
    protected ValuesReader valuesReader;
    protected Dictionary dict;

    // After the data is read in the parquet type, isValid will be set to true if the data can be
    // returned in the type defined in HMS. Otherwise, isValid is set to false.
    boolean isValid = true;

    protected int hivePrecision = 0;
    protected int hiveScale = 0;
    protected final HiveDecimalWritable hiveDecimalWritable = new HiveDecimalWritable(0L);

    // Varchar or char length
    protected int length = -1;

    public DefaultParquetDataColumnReader(ValuesReader valuesReader, int length) {
      this.valuesReader = valuesReader;
      this.length = length;
    }

    public DefaultParquetDataColumnReader(Dictionary dict, int length) {
      this.dict = dict;
      this.length = length;
    }

    public DefaultParquetDataColumnReader(ValuesReader realReader, int length, int precision,
        int scale) {
      this(realReader, length);
      hivePrecision = precision;
      hiveScale = scale;
    }

    public DefaultParquetDataColumnReader(Dictionary dict, int length, int precision, int scale) {
      this(dict, length);
      this.hivePrecision = precision;
      this.hiveScale = scale;
    }

    @Override
    public void initFromPage(int i, ByteBufferInputStream in) throws IOException {
      valuesReader.initFromPage(i, in);
    }

    @Override
    public boolean readBoolean() {
      return valuesReader.readBoolean();
    }

    @Override
    public boolean readBoolean(int id) {
      return dict.decodeToBoolean(id);
    }

    @Override
    public byte[] readString(int id) {
      return dict.decodeToBinary(id).getBytesUnsafe();
    }

    @Override
    public byte[] readString() {
      return valuesReader.readBytes().getBytesUnsafe();
    }

    @Override
    public byte[] readVarchar() {
      // we need to enforce the size here even the types are the same
      return valuesReader.readBytes().getBytesUnsafe();
    }

    @Override
    public byte[] readVarchar(int id) {
      return dict.decodeToBinary(id).getBytesUnsafe();
    }

    @Override
    public byte[] readChar() {
      return valuesReader.readBytes().getBytesUnsafe();
    }

    @Override
    public byte[] readChar(int id) {
      return dict.decodeToBinary(id).getBytesUnsafe();
    }

    @Override
    public byte[] readBytes() {
      return valuesReader.readBytes().getBytesUnsafe();
    }

    @Override
    public byte[] readBytes(int id) {
      return dict.decodeToBinary(id).getBytesUnsafe();
    }

    @Override
    public byte[] readDecimal() {
      return valuesReader.readBytes().getBytesUnsafe();
    }

    @Override
    public byte[] readDecimal(int id) {
      return dict.decodeToBinary(id).getBytesUnsafe();
    }

    @Override
    public float readFloat() {
      return valuesReader.readFloat();
    }

    @Override
    public float readFloat(int id) {
      return dict.decodeToFloat(id);
    }

    @Override
    public double readDouble() {
      return valuesReader.readDouble();
    }

    @Override
    public double readDouble(int id) {
      return dict.decodeToDouble(id);
    }

    @Override
    public Timestamp readTimestamp() {
      throw new RuntimeException("Unsupported operation");
    }

    @Override
    public Timestamp readTimestamp(int id) {
      throw new RuntimeException("Unsupported operation");
    }

    @Override
    public long readInteger() {
      return valuesReader.readInteger();
    }

    @Override
    public long readInteger(int id) {
      return dict.decodeToInt(id);
    }

    @Override
    public boolean isValid() {
      return isValid;
    }

    @Override
    public long readLong(int id) {
      return dict.decodeToLong(id);
    }

    @Override
    public long readLong() {
      return valuesReader.readLong();
    }

    @Override
    public long readSmallInt() {
      return validatedLong(valuesReader.readInteger(), serdeConstants.SMALLINT_TYPE_NAME);
    }

    @Override
    public long readSmallInt(int id) {
      return validatedLong(dict.decodeToInt(id), serdeConstants.SMALLINT_TYPE_NAME);
    }

    @Override
    public long readTinyInt() {
      return validatedLong(valuesReader.readInteger(), serdeConstants.TINYINT_TYPE_NAME);
    }

    @Override
    public long readTinyInt(int id) {
      return validatedLong(dict.decodeToInt(id), serdeConstants.TINYINT_TYPE_NAME);
    }

    @Override
    public int readValueDictionaryId() {
      return valuesReader.readValueDictionaryId();
    }

    public void skip() {
      valuesReader.skip();
    }

    @Override
    public Dictionary getDictionary() {
      return dict;
    }

    /**
     * Enforce the max length of varchar or char.
     */
    protected String enforceMaxLength(String value) {
      return HiveBaseChar.enforceMaxLength(value, length);
    }

    /**
     * Enforce the char length.
     */
    protected String getPaddedString(String value) {
      return HiveBaseChar.getPaddedValue(value, length);
    }

    /**
     * Method to convert string to UTF-8 bytes.
     */
    protected static byte[] convertToBytes(String value) {
      return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Helper function to validate long data.  Sets the isValid to true if the data is valid
     * for the type it will be read in, otherwise false.
     * @param longValue input value of long type to be validated.
     * @param typeName the hivetype to be used to read the longValue
     * @param isUnSigned true when longValue is unsigned parquet type
     * @return 0 if the data is invalid, other longValue
     */
    long validatedLong(long longValue, String typeName, boolean isUnSigned) {
      switch (typeName) {
      case serdeConstants.BIGINT_TYPE_NAME:
        isValid = !isUnSigned || (longValue >= 0);
        break;
      case serdeConstants.INT_TYPE_NAME:
        isValid = ((longValue <= Integer.MAX_VALUE) &&
                   (longValue >= (isUnSigned ? 0 : Integer.MIN_VALUE)));
        break;
      case serdeConstants.SMALLINT_TYPE_NAME:
        isValid = ((longValue <= Short.MAX_VALUE) &&
                   (longValue >= (isUnSigned ? 0 : Short.MIN_VALUE)));
        break;
      case serdeConstants.TINYINT_TYPE_NAME:
        isValid = ((longValue <= Byte.MAX_VALUE) &&
                   (longValue >= (isUnSigned ? 0 : Byte.MIN_VALUE)));
        break;
      default:
        isValid = true;
      }

      if (isValid) {
        return longValue;
      } else {
        return 0;
      }
    }

    /**
     * Helper function to validate long data.  Sets the isValid to true if the data is valid
     * for the type it will be read in, otherwise false.
     * @param longValue input value of long type to be validated.
     * @param typeName the hivetype to be used to read the longValue
     * @return 0 if the data is invalid, other longValue
     */
    long validatedLong(long longValue, String typeName) {
      return validatedLong(longValue, typeName, false);
    }

    /**
     * Helper function to validate long data when it will be read as a decimal from hive.  Sets the
     * isValid to true if the data can be read as decimal, otherwise false.
     * @param longValue input value of long type to be validated.
     * @return null if the data is invalid, other a validated hivedecimalwritable
     */
    byte[] validatedDecimal(long longValue) {
      hiveDecimalWritable.setFromLong(longValue);
      return validatedDecimal();
    }

    /**
     * Helper function to validate double data when it will be read as a decimal from hive.  Sets
     * the isValid to true if the data can be read as decimal, otherwise false.
     * @param doubleValue input value of double type to be validated.
     * @return null if the data is invalid, other a validated hivedecimalwritable
     */
    byte[] validatedDecimal(double doubleValue) {
      hiveDecimalWritable.setFromDouble(doubleValue);
      return validatedDecimal();
    }

    /**
     * Helper function to validate decimal data in hiveDecimalWritable can be read as the decimal
     * type defined in HMS.  Sets the isValid to true if the data can be read as decimal, otherwise
     * false.
     * @return null if the data is invalid, other a validated hivedecimalwritable
     */
    byte[] validatedDecimal() {
      return validatedScaledDecimal(hiveScale);
    }

    byte[] validatedScaledDecimal(int inpScale) {
      hiveDecimalWritable.mutateEnforcePrecisionScale(hivePrecision, hiveScale);
      if (hiveDecimalWritable.isSet()) {
        this.isValid = true;
        return hiveDecimalWritable.getHiveDecimal().bigIntegerBytesScaled(inpScale);
      } else {
        this.isValid = false;
        return null;
      }
    }

    /**
     * Helper function to validate double data.  Sets the isValid to true if the data is valid
     * for the type it will be read in, otherwise false.
     * @param doubleValue input value of long type to be validated.
     * @param typeName the hivetype to be used to read the doubleValue
     * @return 0 if the data is invalid, other doubleValue
     */
    double validatedDouble(double doubleValue, String typeName) {
      switch (typeName) {
      case serdeConstants.FLOAT_TYPE_NAME:
        double absDoubleValue = (doubleValue < 0) ? (doubleValue * -1) : doubleValue;
        int exponent = Math.getExponent(doubleValue);
        isValid = ((absDoubleValue <= Float.MAX_VALUE) && (absDoubleValue >= Float.MIN_VALUE) &&
                   (exponent <= Float.MAX_EXPONENT) && (exponent >= Float.MIN_EXPONENT));
        break;
      case serdeConstants.BIGINT_TYPE_NAME:
        isValid = ((doubleValue <= Long.MAX_VALUE) && (doubleValue >= Long.MIN_VALUE) &&
                   (doubleValue % 1 == 0));
        break;
      case serdeConstants.INT_TYPE_NAME:
        isValid = ((doubleValue <= Integer.MAX_VALUE) && (doubleValue >= Integer.MIN_VALUE) &&
                   (doubleValue % 1 == 0));
        break;
      case serdeConstants.SMALLINT_TYPE_NAME:
        isValid = ((doubleValue <= Short.MAX_VALUE) && (doubleValue >= Short.MIN_VALUE) &&
                   (doubleValue % 1 == 0));
        break;
      case serdeConstants.TINYINT_TYPE_NAME:
        isValid = ((doubleValue <= Byte.MAX_VALUE) && (doubleValue >= Byte.MIN_VALUE) &&
                   (doubleValue % 1 == 0));
        break;
      default:
        isValid = true;
      }

      if (isValid) {
        return doubleValue;
      } else {
        return 0;
      }
    }
  }

  /**
   * The reader who reads from the underlying int64 value value. Implementation is in consist with
   * ETypeConverter EINT64_CONVERTER
   *
   * The data is read as long from the reader, then validated, converted and returned as per the
   * type defined in HMS.
   */
  public static class TypesFromInt64PageReader extends DefaultParquetDataColumnReader {

    private boolean isAdjustedToUTC;
    private TimeUnit timeUnit;

    public TypesFromInt64PageReader(ValuesReader realReader, int length, int precision, int scale) {
      super(realReader, length, precision, scale);
    }

    public TypesFromInt64PageReader(Dictionary dict, int length, int precision, int scale) {
      super(dict, length, precision, scale);
    }

    public TypesFromInt64PageReader(ValuesReader realReader, int length, boolean isAdjustedToUTC, TimeUnit timeUnit) {
      super(realReader, length);
      this.isAdjustedToUTC = isAdjustedToUTC;
      this.timeUnit = timeUnit;
    }

    public TypesFromInt64PageReader(Dictionary dict, int length, boolean isAdjustedToUTC, TimeUnit timeUnit) {
      super(dict, length);
      this.isAdjustedToUTC = isAdjustedToUTC;
      this.timeUnit = timeUnit;
    }

    @Override
    public long readInteger() {
      return super.validatedLong(valuesReader.readLong(), serdeConstants.INT_TYPE_NAME);
    }

    @Override
    public long readInteger(int id) {
      return super.validatedLong(dict.decodeToLong(id), serdeConstants.INT_TYPE_NAME);
    }

    @Override
    public long readSmallInt() {
      return super.validatedLong(valuesReader.readLong(), serdeConstants.SMALLINT_TYPE_NAME);
    }

    @Override
    public long readSmallInt(int id) {
      return super.validatedLong(dict.decodeToLong(id), serdeConstants.SMALLINT_TYPE_NAME);
    }

    @Override
    public long readTinyInt() {
      return super.validatedLong(valuesReader.readLong(), serdeConstants.TINYINT_TYPE_NAME);
    }

    @Override
    public long readTinyInt(int id) {
      return super.validatedLong(dict.decodeToLong(id), serdeConstants.TINYINT_TYPE_NAME);
    }

    @Override
    public float readFloat() {
      return valuesReader.readLong();
    }

    @Override
    public float readFloat(int id) {
      return dict.decodeToLong(id);
    }

    @Override
    public double readDouble() {
      return valuesReader.readLong();
    }

    @Override
    public double readDouble(int id) {
      return dict.decodeToLong(id);
    }

    @Override
    public byte[] readDecimal() {
      return super.validatedDecimal(valuesReader.readLong());
    }

    @Override
    public byte[] readDecimal(int id) {
      return super.validatedDecimal(dict.decodeToLong(id));
    }

    @Override
    public byte[] readString() {
      return convertToBytes(valuesReader.readLong());
    }

    @Override
    public byte[] readString(int id) {
      return convertToBytes(dict.decodeToLong(id));
    }

    @Override
    public byte[] readVarchar() {
      String value = enforceMaxLength(
          convertToString(valuesReader.readLong()));
      return convertToBytes(value);
    }

    @Override
    public byte[] readVarchar(int id) {
      String value = enforceMaxLength(
          convertToString(dict.decodeToLong(id)));
      return convertToBytes(value);
    }

    @Override
    public byte[] readChar() {
      String value = enforceMaxLength(
          convertToString(valuesReader.readLong()));
      return convertToBytes(value);
    }

    @Override
    public byte[] readChar(int id) {
      String value = enforceMaxLength(
          convertToString(dict.decodeToLong(id)));
      return convertToBytes(value);
    }

    private Timestamp convert(Long value) {
      Timestamp timestamp = ParquetTimestampUtils.getTimestamp(value, timeUnit, isAdjustedToUTC);
      return timestamp;
    }

    @Override
    public Timestamp readTimestamp(int id) {
      return convert(dict.decodeToLong(id));
    }

    @Override
    public Timestamp readTimestamp() {
      return convert(valuesReader.readLong());
    }

    private static String convertToString(long value) {
      return Long.toString(value);
    }

    private static byte[] convertToBytes(long value) {
      return convertToBytes(convertToString(value));
    }
  }

  /**
   * The reader who reads unsigned long data.
   *
   * The data is read as long from the reader, then validated, converted and returned as per the
   * type defined in HMS.  The data is unsigned long hence when read back it can't be negative.
   * The call to validate will indicate that the data is unsigned to do the appropriate
   * validation.
   */
  public static class TypesFromUInt64PageReader extends TypesFromInt64PageReader {

    public TypesFromUInt64PageReader(ValuesReader realReader, int length, int precision,
        int scale) {
      super(realReader, length, precision, scale);
    }

    public TypesFromUInt64PageReader(Dictionary dict, int length, int precision, int scale) {
      super(dict, length, precision, scale);
    }

    @Override
    public long readLong() {
      return super.validatedLong(valuesReader.readLong(), serdeConstants.BIGINT_TYPE_NAME, true);
    }

    @Override
    public long readLong(int id) {
      return super.validatedLong(dict.decodeToLong(id), serdeConstants.BIGINT_TYPE_NAME, true);
    }

    @Override
    public long readInteger() {
      return super.validatedLong(valuesReader.readLong(), serdeConstants.INT_TYPE_NAME, true);
    }

    @Override
    public long readInteger(int id) {
      return super.validatedLong(dict.decodeToLong(id), serdeConstants.INT_TYPE_NAME, true);
    }

    @Override
    public long readSmallInt() {
      return super.validatedLong(valuesReader.readLong(), serdeConstants.SMALLINT_TYPE_NAME, true);
    }

    @Override
    public long readSmallInt(int id) {
      return super.validatedLong(dict.decodeToLong(id), serdeConstants.SMALLINT_TYPE_NAME, true);
    }

    @Override
    public long readTinyInt() {
      return super.validatedLong(valuesReader.readLong(), serdeConstants.TINYINT_TYPE_NAME, true);
    }

    @Override
    public long readTinyInt(int id) {
      return super.validatedLong(dict.decodeToLong(id), serdeConstants.TINYINT_TYPE_NAME, true);
    }

    @Override
    public float readFloat() {
      return (float) super.validatedLong(valuesReader.readLong(), serdeConstants.BIGINT_TYPE_NAME,
                                         true);
    }

    @Override
    public float readFloat(int id) {
      return (float) super.validatedLong(dict.decodeToLong(id), serdeConstants.BIGINT_TYPE_NAME,
                                        true);
    }

    @Override
    public double readDouble() {
      return (double) super.validatedLong(valuesReader.readLong(), serdeConstants.BIGINT_TYPE_NAME,
                                          true);
    }

    @Override
    public double readDouble(int id) {
      return (double) super.validatedLong(dict.decodeToLong(id), serdeConstants.BIGINT_TYPE_NAME,
          true);
    }

    @Override
    public byte[] readDecimal() {
      long validatedLongValue = super.validatedLong(valuesReader.readLong(),
                                                    serdeConstants.BIGINT_TYPE_NAME, true);
      if (super.isValid) {
        return super.validatedDecimal(validatedLongValue);
      } else {
        return null;
      }
    }

    @Override
    public byte[] readDecimal(int id) {
      long validatedLongValue = super.validatedLong(dict.decodeToLong(id),
                                                    serdeConstants.BIGINT_TYPE_NAME, true);
      if (super.isValid) {
        return super.validatedDecimal(validatedLongValue);
      } else {
        return null;
      }
    }
  }

  /**
   * The reader who reads from the underlying int32 value value. Implementation is in consist with
   * ETypeConverter EINT32_CONVERTER
   *
   * The data is read as integer from the reader, then validated, converted and returned as per the
   * type defined in HMS.
   */
  public static class TypesFromInt32PageReader extends DefaultParquetDataColumnReader {

    public TypesFromInt32PageReader(ValuesReader realReader, int length, int precision,
        int scale) {
      super(realReader, length, precision, scale);
    }

    public TypesFromInt32PageReader(Dictionary dict, int length, int precision, int scale) {
      super(dict, length, precision, scale);
    }

    @Override
    public long readLong() {
      return valuesReader.readInteger();
    }

    @Override
    public long readLong(int id) {
      return dict.decodeToInt(id);
    }

    @Override
    public float readFloat() {
      return valuesReader.readInteger();
    }

    @Override
    public float readFloat(int id) {
      return dict.decodeToInt(id);
    }

    @Override
    public double readDouble() {
      return valuesReader.readInteger();
    }

    @Override
    public double readDouble(int id) {
      return dict.decodeToInt(id);
    }

    @Override
    public byte[] readString() {
      return convertToBytes(valuesReader.readInteger());
    }

    @Override
    public byte[] readString(int id) {
      return convertToBytes(dict.decodeToInt(id));
    }

    @Override
    public byte[] readVarchar() {
      String value = enforceMaxLength(
          convertToString(valuesReader.readInteger()));
      return convertToBytes(value);
    }

    @Override
    public byte[] readVarchar(int id) {
      String value = enforceMaxLength(
          convertToString(dict.decodeToInt(id)));
      return convertToBytes(value);
    }

    @Override
    public byte[] readChar() {
      String value = enforceMaxLength(
          convertToString(valuesReader.readInteger()));
      return convertToBytes(value);
    }

    @Override
    public byte[] readChar(int id) {
      String value = enforceMaxLength(
          convertToString(dict.decodeToInt(id)));
      return convertToBytes(value);
    }

    private static String convertToString(int value) {
      return Integer.toString(value);
    }

    private static byte[] convertToBytes(int value) {
      return convertToBytes(convertToString(value));
    }

    @Override
    public byte[] readDecimal() {
      return super.validatedDecimal(valuesReader.readInteger());
    }

    @Override
    public byte[] readDecimal(int id) {
      return super.validatedDecimal(dict.decodeToInt(id));
    }
  }

  /**
   * The reader who reads unsigned int data.
   *
   * The data is read as integer from the reader, then validated, converted and returned as per the
   * type defined in HMS.  The data is unsigned integer hence when read back it can't be negative.
   * The call to validate will indicate that the data is unsigned to do the appropriate
   * validation.
   */
  public static class TypesFromUInt32PageReader extends TypesFromInt32PageReader {

    public TypesFromUInt32PageReader(ValuesReader realReader, int length, int precision,
        int scale) {
      super(realReader, length, precision, scale);
    }

    public TypesFromUInt32PageReader(Dictionary dict, int length, int precision, int scale) {
      super(dict, length, precision, scale);
    }

    @Override
    public long readLong() {
      return super.validatedLong(valuesReader.readInteger(), serdeConstants.BIGINT_TYPE_NAME, true);
    }

    @Override
    public long readLong(int id) {
      return super.validatedLong(dict.decodeToLong(id), serdeConstants.BIGINT_TYPE_NAME, true);
    }

    @Override
    public long readInteger() {
      return super.validatedLong(valuesReader.readInteger(), serdeConstants.INT_TYPE_NAME, true);
    }

    @Override
    public long readInteger(int id) {
      return super.validatedLong(dict.decodeToInt(id), serdeConstants.INT_TYPE_NAME, true);
    }

    @Override
    public long readSmallInt() {
      return validatedLong(valuesReader.readInteger(), serdeConstants.SMALLINT_TYPE_NAME, true);
    }

    @Override
    public long readSmallInt(int id) {
      return validatedLong(dict.decodeToInt(id), serdeConstants.SMALLINT_TYPE_NAME, true);
    }

    @Override
    public long readTinyInt() {
      return validatedLong(valuesReader.readInteger(), serdeConstants.TINYINT_TYPE_NAME, true);
    }

    @Override
    public long readTinyInt(int id) {
      return validatedLong(dict.decodeToInt(id), serdeConstants.TINYINT_TYPE_NAME, true);
    }

    @Override
    public float readFloat() {
      return (float) super.validatedLong(valuesReader.readInteger(), serdeConstants.BIGINT_TYPE_NAME,
          true);
    }

    @Override
    public float readFloat(int id) {
      return (float) super.validatedLong(dict.decodeToLong(id), serdeConstants.BIGINT_TYPE_NAME,
          true);
    }

    @Override
    public double readDouble() {
      return (double) super.validatedLong(valuesReader.readInteger(),
          serdeConstants.BIGINT_TYPE_NAME, true);
    }

    @Override
    public double readDouble(int id) {
      return (double) super.validatedLong(dict.decodeToLong(id), serdeConstants.BIGINT_TYPE_NAME,
          true);
    }

    @Override
    public byte[] readDecimal() {
      long validatedIntValue = super.validatedLong(valuesReader.readInteger(),
                                                    serdeConstants.INT_TYPE_NAME, true);
      if (super.isValid) {
        return super.validatedDecimal(validatedIntValue);
      } else {
        return null;
      }
    }

    @Override
    public byte[] readDecimal(int id) {
      long validatedIntValue = super.validatedLong(dict.decodeToInt(id),
                                                    serdeConstants.INT_TYPE_NAME, true);
      if (super.isValid) {
        return super.validatedDecimal(validatedIntValue);
      } else {
        return null;
      }
    }
  }

  /**
   * The reader who reads from the underlying float value value. Implementation is in consist with
   * ETypeConverter EFLOAT_CONVERTER
   *
   * The data is read as float from the reader, then validated, converted and returned as per the
   * type defined in HMS.
   */
  public static class TypesFromFloatPageReader extends DefaultParquetDataColumnReader {

    public TypesFromFloatPageReader(ValuesReader realReader, int length, int precision, int scale) {
      super(realReader, length, precision, scale);
    }

    public TypesFromFloatPageReader(Dictionary dict, int length, int precision, int scale) {
      super(dict, length, precision, scale);
    }

    @Override
    public double readDouble() {
      return valuesReader.readFloat();
    }

    @Override
    public double readDouble(int id) {
      return dict.decodeToFloat(id);
    }

    @Override
    public byte[] readString() {
      return convertToBytes(valuesReader.readFloat());
    }

    @Override
    public byte[] readString(int id) {
      return convertToBytes(dict.decodeToFloat(id));
    }

    @Override
    public byte[] readVarchar() {
      String value = enforceMaxLength(
          convertToString(valuesReader.readFloat()));
      return convertToBytes(value);
    }

    @Override
    public byte[] readVarchar(int id) {
      String value = enforceMaxLength(
          convertToString(dict.decodeToFloat(id)));
      return convertToBytes(value);
    }

    @Override
    public byte[] readChar() {
      String value = enforceMaxLength(
          convertToString(valuesReader.readFloat()));
      return convertToBytes(value);
    }

    @Override
    public byte[] readChar(int id) {
      String value = enforceMaxLength(
          convertToString(dict.decodeToFloat(id)));
      return convertToBytes(value);
    }

    @Override
    public long readLong() {
      return (long)(super.validatedDouble(valuesReader.readFloat(),
          serdeConstants.BIGINT_TYPE_NAME));
    }

    @Override
    public long readLong(int id) {
      return (long)(super.validatedDouble(dict.decodeToFloat(id), serdeConstants.BIGINT_TYPE_NAME));
    }

    @Override
    public long readInteger() {
      return (long)(super.validatedDouble(valuesReader.readFloat(), serdeConstants.INT_TYPE_NAME));
    }

    @Override
    public long readInteger(int id) {
      return (long)(super.validatedDouble(dict.decodeToFloat(id), serdeConstants.INT_TYPE_NAME));
    }

    @Override
    public long readSmallInt() {
      return (long)super.validatedDouble(valuesReader.readFloat(),
                                         serdeConstants.SMALLINT_TYPE_NAME);
    }

    @Override
    public long readSmallInt(int id) {
      return (long)super.validatedDouble(dict.decodeToFloat(id), serdeConstants.SMALLINT_TYPE_NAME);
    }

    @Override
    public long readTinyInt() {
      return (long)super.validatedDouble(valuesReader.readFloat(),
                                         serdeConstants.TINYINT_TYPE_NAME);
    }

    @Override
    public long readTinyInt(int id) {
      return (long)super.validatedDouble(dict.decodeToFloat(id), serdeConstants.TINYINT_TYPE_NAME);
    }

    private static String convertToString(float value) {
      return Float.toString(value);
    }

    private static byte[] convertToBytes(float value) {
      return convertToBytes(convertToString(value));
    }

    @Override
    public byte[] readDecimal() {
      return super.validatedDecimal(valuesReader.readFloat());
    }

    @Override
    public byte[] readDecimal(int id) {
      return super.validatedDecimal(dict.decodeToFloat(id));
    }
  }

  /**
   * The reader who reads from the underlying double value value.
   *
   * The data is read as double from the reader, then validated, converted and returned as per the
   * type defined in HMS.
   */
  public static class TypesFromDoublePageReader extends DefaultParquetDataColumnReader {

    public TypesFromDoublePageReader(ValuesReader realReader, int length, int precision, int scale) {
      super(realReader, length, precision, scale);
    }

    public TypesFromDoublePageReader(Dictionary dict, int length, int precision, int scale) {
      super(dict, length, precision, scale);
    }

    @Override
    public byte[] readString() {
      return convertToBytes(valuesReader.readDouble());
    }

    @Override
    public byte[] readString(int id) {
      return convertToBytes(dict.decodeToDouble(id));
    }

    @Override
    public byte[] readVarchar() {
      String value = enforceMaxLength(
          convertToString(valuesReader.readDouble()));
      return convertToBytes(value);
    }

    @Override
    public byte[] readVarchar(int id) {
      String value = enforceMaxLength(
          convertToString(dict.decodeToDouble(id)));
      return convertToBytes(value);
    }

    @Override
    public byte[] readChar() {
      String value = enforceMaxLength(
          convertToString(valuesReader.readDouble()));
      return convertToBytes(value);
    }

    @Override
    public byte[] readChar(int id) {
      String value = enforceMaxLength(
          convertToString(dict.decodeToDouble(id)));
      return convertToBytes(value);
    }

    @Override
    public long readLong() {
      return (long)(super.validatedDouble(valuesReader.readDouble(),
          serdeConstants.BIGINT_TYPE_NAME));
    }

    @Override
    public long readLong(int id) {
      return (long)(super.validatedDouble(dict.decodeToDouble(id),
          serdeConstants.BIGINT_TYPE_NAME));
    }

    @Override
    public long readInteger() {
      return (long)(super.validatedDouble(valuesReader.readDouble(),
          serdeConstants.INT_TYPE_NAME));
    }

    @Override
    public long readInteger(int id) {
      return (long)(super.validatedDouble(dict.decodeToDouble(id), serdeConstants.INT_TYPE_NAME));
    }

    @Override
    public long readSmallInt() {
      return (long)super.validatedDouble(valuesReader.readDouble(), serdeConstants.SMALLINT_TYPE_NAME);
    }

    @Override
    public long readSmallInt(int id) {
      return (long)super.validatedDouble(dict.decodeToDouble(id), serdeConstants.SMALLINT_TYPE_NAME);
    }

    @Override
    public long readTinyInt() {
      return (long)super.validatedDouble(valuesReader.readDouble(), serdeConstants.TINYINT_TYPE_NAME);
    }

    @Override
    public long readTinyInt(int id) {
      return (long)super.validatedDouble(dict.decodeToDouble(id), serdeConstants.TINYINT_TYPE_NAME);
    }

    @Override
    public float readFloat() {
      return (float)(super.validatedDouble(valuesReader.readDouble(),
          serdeConstants.FLOAT_TYPE_NAME));
    }

    @Override
    public float readFloat(int id) {
      return (float)(super.validatedDouble(dict.decodeToDouble(id),
          serdeConstants.FLOAT_TYPE_NAME));
    }

    private static String convertToString(double value) {
      return Double.toString(value);
    }

    private static byte[] convertToBytes(double value) {
      return convertToBytes(convertToString(value));
    }

    @Override
    public byte[] readDecimal() {
      return super.validatedDecimal(valuesReader.readDouble());
    }

    @Override
    public byte[] readDecimal(int id) {
      return super.validatedDecimal(dict.decodeToDouble(id));
    }
  }

  /**
   * The reader who reads from the underlying boolean value value.
   */
  public static class TypesFromBooleanPageReader extends DefaultParquetDataColumnReader {

    public TypesFromBooleanPageReader(ValuesReader valuesReader, int length) {
      super(valuesReader, length);
    }

    public TypesFromBooleanPageReader(Dictionary dict, int length) {
      super(dict, length);
    }

    @Override
    public byte[] readString() {
      return convertToBytes(valuesReader.readBoolean());
    }

    @Override
    public byte[] readString(int id) {
      return convertToBytes(dict.decodeToBoolean(id));
    }

    @Override
    public byte[] readVarchar() {
      String value = enforceMaxLength(
          convertToString(valuesReader.readBoolean()));
      return convertToBytes(value);
    }

    @Override
    public byte[] readVarchar(int id) {
      String value = enforceMaxLength(
          convertToString(dict.decodeToBoolean(id)));
      return convertToBytes(value);
    }

    @Override
    public byte[] readChar() {
      String value = enforceMaxLength(
          convertToString(valuesReader.readBoolean()));
      return convertToBytes(value);
    }

    @Override
    public byte[] readChar(int id) {
      String value = enforceMaxLength(
          convertToString(dict.decodeToBoolean(id)));
      return convertToBytes(value);
    }

    private static String convertToString(boolean value) {
      return Boolean.toString(value);
    }

    private static byte[] convertToBytes(boolean value) {
      return convertToBytes(convertToString(value));
    }
  }

  /**
   * The reader who reads from the underlying Timestamp value value.
   */
  public static class TypesFromInt96PageReader extends DefaultParquetDataColumnReader {
    private final ZoneId targetZone;
    private boolean legacyConversionEnabled;

    public TypesFromInt96PageReader(ValuesReader realReader, int length, ZoneId targetZone,
        boolean legacyConversionEnabled) {
      super(realReader, length);
      this.targetZone = targetZone;
      this.legacyConversionEnabled = legacyConversionEnabled;
    }

    public TypesFromInt96PageReader(Dictionary dict, int length, ZoneId targetZone, boolean legacyConversionEnabled) {
      super(dict, length);
      this.targetZone = targetZone;
      this.legacyConversionEnabled = legacyConversionEnabled;
    }

    private Timestamp convert(Binary binary) {
      ByteBuffer buf = binary.toByteBuffer();
      buf.order(ByteOrder.LITTLE_ENDIAN);
      long timeOfDayNanos = buf.getLong();
      int julianDay = buf.getInt();
      NanoTime nt = new NanoTime(julianDay, timeOfDayNanos);
      return NanoTimeUtils.getTimestamp(nt, targetZone, legacyConversionEnabled);
    }

    @Override
    public Timestamp readTimestamp(int id) {
      return convert(dict.decodeToBinary(id));
    }

    @Override
    public Timestamp readTimestamp() {
      return convert(valuesReader.readBytes());
    }

    @Override
    public byte[] readString() {
      return convertToBytes(readTimestamp());
    }

    @Override
    public byte[] readString(int id) {
      return convertToBytes(readTimestamp(id));
    }

    @Override
    public byte[] readVarchar() {
      String value = enforceMaxLength(
          convertToString(readTimestamp()));
      return convertToBytes(value);
    }

    @Override
    public byte[] readVarchar(int id) {
      String value = enforceMaxLength(
          convertToString(readTimestamp(id)));
      return convertToBytes(value);
    }

    @Override
    public byte[] readChar() {
      String value = enforceMaxLength(
          convertToString(readTimestamp()));
      return convertToBytes(value);
    }

    @Override
    public byte[] readChar(int id) {
      String value = enforceMaxLength(
          convertToString(readTimestamp(id)));
      return convertToBytes(value);
    }

    private static String convertToString(Timestamp value) {
      return value.toString();
    }

    private static byte[] convertToBytes(Timestamp value) {
      return convertToBytes(convertToString(value));
    }
  }

  /**
   * The reader who reads from the underlying decimal value value.
   *
   * The data is read as binary from the reader treated as a decimal, then validated, converted
   * and returned as per the type defined in HMS.
   */
  public static class TypesFromDecimalPageReader extends DefaultParquetDataColumnReader {
    private short scale;

    public TypesFromDecimalPageReader(ValuesReader realReader, int length, short scale,
        int hivePrecision, int hiveScale) {
      super(realReader, length, hivePrecision, hiveScale);
      this.scale = scale;
    }

    public TypesFromDecimalPageReader(Dictionary dict, int length, short scale, int hivePrecision,
        int hiveScale) {
      super(dict, length, hivePrecision, hiveScale);
      this.scale = scale;
    }

    @Override
    public byte[] readString() {
      return convertToBytes(valuesReader.readBytes());
    }

    @Override
    public byte[] readString(int id) {
      return convertToBytes(dict.decodeToBinary(id));
    }

    @Override
    public byte[] readVarchar() {
      String value = enforceMaxLength(
          convertToString(valuesReader.readBytes()));
      return convertToBytes(value);
    }

    @Override
    public byte[] readVarchar(int id) {
      String value = enforceMaxLength(
          convertToString(dict.decodeToBinary(id)));
      return convertToBytes(value);
    }

    @Override
    public byte[] readChar() {
      String value = enforceMaxLength(
          convertToString(valuesReader.readBytes()));
      return convertToBytes(value);
    }

    @Override
    public byte[] readChar(int id) {
      String value = enforceMaxLength(
          convertToString(dict.decodeToBinary(id)));
      return convertToBytes(value);
    }

    @Override
    public float readFloat() {
      hiveDecimalWritable.set(valuesReader.readBytes().getBytesUnsafe(), scale);
      return (float)(super.validatedDouble(hiveDecimalWritable.doubleValue(),
          serdeConstants.FLOAT_TYPE_NAME));
    }

    @Override
    public float readFloat(int id) {
      hiveDecimalWritable.set(dict.decodeToBinary(id).getBytesUnsafe(), scale);
      return (float)(super.validatedDouble(hiveDecimalWritable.doubleValue(),
          serdeConstants.FLOAT_TYPE_NAME));
    }

    @Override
    public double readDouble() {
      hiveDecimalWritable.set(valuesReader.readBytes().getBytesUnsafe(), scale);
      return (super.validatedDouble(hiveDecimalWritable.doubleValue(),
          serdeConstants.DOUBLE_TYPE_NAME));
    }

    @Override
    public double readDouble(int id) {
      hiveDecimalWritable.set(dict.decodeToBinary(id).getBytesUnsafe(), scale);
      return (super.validatedDouble(hiveDecimalWritable.doubleValue(),
          serdeConstants.DOUBLE_TYPE_NAME));
    }

    @Override
    public long readLong() {
      hiveDecimalWritable.set(valuesReader.readBytes().getBytesUnsafe(), scale);
      return (long)(super.validatedDouble(hiveDecimalWritable.doubleValue(),
          serdeConstants.BIGINT_TYPE_NAME));
    }

    @Override
    public long readLong(int id) {
      hiveDecimalWritable.set(dict.decodeToBinary(id).getBytesUnsafe(), scale);
      return (long)(super.validatedDouble(hiveDecimalWritable.doubleValue(),
          serdeConstants.BIGINT_TYPE_NAME));
    }

    @Override
    public long readInteger() {
      hiveDecimalWritable.set(valuesReader.readBytes().getBytesUnsafe(), scale);
      return (long)(super.validatedDouble(hiveDecimalWritable.doubleValue(),
          serdeConstants.INT_TYPE_NAME));
    }

    @Override
    public long readInteger(int id) {
      hiveDecimalWritable.set(dict.decodeToBinary(id).getBytesUnsafe(), scale);
      return (long)(super.validatedDouble(hiveDecimalWritable.doubleValue(),
          serdeConstants.INT_TYPE_NAME));
    }

    @Override
    public long readSmallInt() {
      hiveDecimalWritable.set(valuesReader.readBytes().getBytesUnsafe(), scale);
      return (long)(super.validatedDouble(hiveDecimalWritable.doubleValue(),
          serdeConstants.SMALLINT_TYPE_NAME));
    }

    @Override
    public long readSmallInt(int id) {
      hiveDecimalWritable.set(dict.decodeToBinary(id).getBytesUnsafe(), scale);
      return (long)(super.validatedDouble(hiveDecimalWritable.doubleValue(),
          serdeConstants.SMALLINT_TYPE_NAME));
    }

    @Override
    public long readTinyInt() {
      hiveDecimalWritable.set(valuesReader.readBytes().getBytesUnsafe(), scale);
      return (long)(super.validatedDouble(hiveDecimalWritable.doubleValue(),
          serdeConstants.TINYINT_TYPE_NAME));
    }

    @Override
    public long readTinyInt(int id) {
      hiveDecimalWritable.set(dict.decodeToBinary(id).getBytesUnsafe(), scale);
      return (long)(super.validatedDouble(hiveDecimalWritable.doubleValue(),
          serdeConstants.TINYINT_TYPE_NAME));
    }

    private String convertToString(Binary value) {
      hiveDecimalWritable.set(value.getBytesUnsafe(), scale);
      return hiveDecimalWritable.toString();
    }

    private byte[] convertToBytes(Binary value) {
      return convertToBytes(convertToString(value));
    }

    @Override
    public byte[] readDecimal() {
      hiveDecimalWritable.set(valuesReader.readBytes().getBytesUnsafe(), scale);
      return super.validatedScaledDecimal(scale);
    }

    @Override
    public byte[] readDecimal(int id) {
      hiveDecimalWritable.set(dict.decodeToBinary(id).getBytesUnsafe(), scale);
      return super.validatedScaledDecimal(scale);
    }
  }

  /**
   * The reader who reads from the underlying decimal value which is stored in an INT32 physical type.
   *
   * The data is read as INT32 from the reader treated as a decimal, then validated, converted
   * and returned as per the type defined in HMS.
   */
  public static class TypesFromInt32DecimalPageReader extends DefaultParquetDataColumnReader {
    private short scale;

    public TypesFromInt32DecimalPageReader(ValuesReader realReader, int length, short scale, int hivePrecision,
        int hiveScale) {
      super(realReader, length, hivePrecision, hiveScale);
      this.scale = scale;
    }

    public TypesFromInt32DecimalPageReader(Dictionary dict, int length, short scale, int hivePrecision, int hiveScale) {
      super(dict, length, hivePrecision, hiveScale);
      this.scale = scale;
    }

    @Override
    public byte[] readString() {
      return convertToBytes(valuesReader.readInteger());
    }

    @Override
    public byte[] readString(int id) {
      return convertToBytes(dict.decodeToInt(id));
    }

    @Override
    public byte[] readVarchar() {
      String value = enforceMaxLength(convertToString(valuesReader.readInteger()));
      return convertToBytes(value);
    }

    @Override
    public byte[] readVarchar(int id) {
      String value = enforceMaxLength(convertToString(dict.decodeToInt(id)));
      return convertToBytes(value);
    }

    @Override
    public byte[] readChar() {
      String value = enforceMaxLength(convertToString(valuesReader.readInteger()));
      return convertToBytes(value);
    }

    @Override
    public byte[] readChar(int id) {
      String value = enforceMaxLength(convertToString(dict.decodeToInt(id)));
      return convertToBytes(value);
    }

    @Override
    public float readFloat() {
      HiveDecimal hiveDecimal = HiveDecimal.create(valuesReader.readInteger(), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (float) (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.FLOAT_TYPE_NAME));
    }

    @Override
    public float readFloat(int id) {
      HiveDecimal hiveDecimal = HiveDecimal.create(dict.decodeToInt(id), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (float) (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.FLOAT_TYPE_NAME));
    }

    @Override
    public double readDouble() {
      HiveDecimal hiveDecimal = HiveDecimal.create(valuesReader.readInteger(), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.DOUBLE_TYPE_NAME));
    }

    @Override
    public double readDouble(int id) {
      HiveDecimal hiveDecimal = HiveDecimal.create(dict.decodeToInt(id), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.DOUBLE_TYPE_NAME));
    }

    @Override
    public long readLong() {
      HiveDecimal hiveDecimal = HiveDecimal.create(valuesReader.readInteger(), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (long) (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.BIGINT_TYPE_NAME));
    }

    @Override
    public long readLong(int id) {
      HiveDecimal hiveDecimal = HiveDecimal.create(dict.decodeToInt(id), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (long) (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.BIGINT_TYPE_NAME));
    }

    @Override
    public long readInteger() {
      HiveDecimal hiveDecimal = HiveDecimal.create(valuesReader.readInteger(), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (long) (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.INT_TYPE_NAME));
    }

    @Override
    public long readInteger(int id) {
      HiveDecimal hiveDecimal = HiveDecimal.create(dict.decodeToInt(id), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (long) (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.INT_TYPE_NAME));
    }

    @Override
    public long readSmallInt() {
      HiveDecimal hiveDecimal = HiveDecimal.create(valuesReader.readInteger(), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (long) (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.SMALLINT_TYPE_NAME));
    }

    @Override
    public long readSmallInt(int id) {
      HiveDecimal hiveDecimal = HiveDecimal.create(dict.decodeToInt(id), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (long) (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.SMALLINT_TYPE_NAME));
    }

    @Override
    public long readTinyInt() {
      HiveDecimal hiveDecimal = HiveDecimal.create(valuesReader.readInteger(), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (long) (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.TINYINT_TYPE_NAME));
    }

    @Override
    public long readTinyInt(int id) {
      HiveDecimal hiveDecimal = HiveDecimal.create(dict.decodeToInt(id), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (long) (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.TINYINT_TYPE_NAME));
    }

    private String convertToString(int value) {
      HiveDecimal hiveDecimal = HiveDecimal.create(value, scale);
      return hiveDecimal.toString();
    }

    private byte[] convertToBytes(int value) {
      return convertToBytes(convertToString(value));
    }

    @Override
    public byte[] readDecimal() {
      HiveDecimal hiveDecimal = HiveDecimal.create(valuesReader.readInteger(), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return super.validatedScaledDecimal(scale);
    }

    @Override
    public byte[] readDecimal(int id) {
      HiveDecimal hiveDecimal = HiveDecimal.create(dict.decodeToInt(id), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return super.validatedScaledDecimal(scale);
    }
  }

  /**
   * The reader who reads from the underlying decimal value which is stored in an INT64 physical type.
   *
   * The data is read as INT64 from the reader treated as a decimal, then validated, converted
   * and returned as per the type defined in HMS.
   */
  public static class TypesFromInt64DecimalPageReader extends DefaultParquetDataColumnReader {
    private short scale;

    public TypesFromInt64DecimalPageReader(ValuesReader realReader, int length, short scale, int hivePrecision,
        int hiveScale) {
      super(realReader, length, hivePrecision, hiveScale);
      this.scale = scale;
    }

    public TypesFromInt64DecimalPageReader(Dictionary dict, int length, short scale, int hivePrecision, int hiveScale) {
      super(dict, length, hivePrecision, hiveScale);
      this.scale = scale;
    }

    @Override
    public byte[] readString() {
      return convertToBytes(valuesReader.readLong());
    }

    @Override
    public byte[] readString(int id) {
      return convertToBytes(dict.decodeToLong(id));
    }

    @Override
    public byte[] readVarchar() {
      String value = enforceMaxLength(convertToString(valuesReader.readLong()));
      return convertToBytes(value);
    }

    @Override
    public byte[] readVarchar(int id) {
      String value = enforceMaxLength(convertToString(dict.decodeToLong(id)));
      return convertToBytes(value);
    }

    @Override
    public byte[] readChar() {
      String value = enforceMaxLength(convertToString(valuesReader.readLong()));
      return convertToBytes(value);
    }

    @Override
    public byte[] readChar(int id) {
      String value = enforceMaxLength(convertToString(dict.decodeToLong(id)));
      return convertToBytes(value);
    }

    @Override
    public float readFloat() {
      HiveDecimal hiveDecimal = HiveDecimal.create(valuesReader.readLong(), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (float) (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.FLOAT_TYPE_NAME));
    }

    @Override
    public float readFloat(int id) {
      HiveDecimal hiveDecimal = HiveDecimal.create(dict.decodeToLong(id), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (float) (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.FLOAT_TYPE_NAME));
    }

    @Override
    public double readDouble() {
      HiveDecimal hiveDecimal = HiveDecimal.create(valuesReader.readLong(), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.DOUBLE_TYPE_NAME));
    }

    @Override
    public double readDouble(int id) {
      HiveDecimal hiveDecimal = HiveDecimal.create(dict.decodeToLong(id), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.DOUBLE_TYPE_NAME));
    }

    @Override
    public long readLong() {
      HiveDecimal hiveDecimal = HiveDecimal.create(valuesReader.readLong(), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (long) (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.BIGINT_TYPE_NAME));
    }

    @Override
    public long readLong(int id) {
      HiveDecimal hiveDecimal = HiveDecimal.create(dict.decodeToLong(id), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (long) (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.BIGINT_TYPE_NAME));
    }

    @Override
    public long readInteger() {
      HiveDecimal hiveDecimal = HiveDecimal.create(valuesReader.readLong(), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (long) (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.INT_TYPE_NAME));
    }

    @Override
    public long readInteger(int id) {
      HiveDecimal hiveDecimal = HiveDecimal.create(dict.decodeToLong(id), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (long) (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.INT_TYPE_NAME));
    }

    @Override
    public long readSmallInt() {
      HiveDecimal hiveDecimal = HiveDecimal.create(valuesReader.readLong(), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (long) (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.SMALLINT_TYPE_NAME));
    }

    @Override
    public long readSmallInt(int id) {
      HiveDecimal hiveDecimal = HiveDecimal.create(dict.decodeToLong(id), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (long) (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.SMALLINT_TYPE_NAME));
    }

    @Override
    public long readTinyInt() {
      HiveDecimal hiveDecimal = HiveDecimal.create(valuesReader.readLong(), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (long) (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.TINYINT_TYPE_NAME));
    }

    @Override
    public long readTinyInt(int id) {
      HiveDecimal hiveDecimal = HiveDecimal.create(dict.decodeToLong(id), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return (long) (super.validatedDouble(hiveDecimalWritable.doubleValue(), serdeConstants.TINYINT_TYPE_NAME));
    }

    private String convertToString(long value) {
      HiveDecimal hiveDecimal = HiveDecimal.create(value, scale);
      return hiveDecimal.toString();
    }

    private byte[] convertToBytes(long value) {
      return convertToBytes(convertToString(value));
    }

    @Override
    public byte[] readDecimal() {
      HiveDecimal hiveDecimal = HiveDecimal.create(valuesReader.readLong(), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return super.validatedScaledDecimal(scale);
    }

    @Override
    public byte[] readDecimal(int id) {
      HiveDecimal hiveDecimal = HiveDecimal.create(dict.decodeToLong(id), scale);
      hiveDecimalWritable.set(hiveDecimal);
      return super.validatedScaledDecimal(scale);
    }
  }

  /**
   * The reader who reads from the underlying UTF8 string.
   */
  public static class TypesFromStringPageReader extends DefaultParquetDataColumnReader {

    public TypesFromStringPageReader(ValuesReader realReader, int length) {
      super(realReader, length);
    }

    public TypesFromStringPageReader(Dictionary dict, int length) {
      super(dict, length);
    }

    @Override
    public byte[] readVarchar() {
      // check the character numbers with the length
      final byte[] value = valuesReader.readBytes().getBytesUnsafe();
      return truncateIfNecesssary(value);
    }

    @Override
    public byte[] readVarchar(int id) {
      // check the character numbers with the length
      final byte[] value = dict.decodeToBinary(id).getBytesUnsafe();
      return truncateIfNecesssary(value);
    }

    @Override
    public byte[] readChar() {
      // check the character numbers with the length
      final byte[] value = valuesReader.readBytes().getBytesUnsafe();
      return truncateIfNecesssary(value);
    }

    @Override
    public byte[] readChar(int id) {
      // check the character numbers with the length
      final byte[] value = dict.decodeToBinary(id).getBytesUnsafe();
      return truncateIfNecesssary(value);
    }

    private byte[] truncateIfNecesssary(byte[] bytes) {
      if (length <= 0 || bytes == null) {
        return bytes;
      }

      int len = bytes.length;
      int truncatedLength = StringExpr.truncate(bytes, 0, len, length);
      if (truncatedLength >= len) {
        return bytes;
      }

      return Arrays.copyOf(bytes, truncatedLength);
    }
  }

  private static ParquetDataColumnReader getDataColumnReaderByTypeHelper(boolean isDictionary,
                                                                         PrimitiveType parquetType,
                                                                         TypeInfo hiveType,
                                                                         Dictionary dictionary,
                                                                         ValuesReader valuesReader,
                                                                         boolean skipTimestampConversion,
                                                                         ZoneId writerTimezone,
                                                                         boolean legacyConversionEnabled)
      throws IOException {
    // max length for varchar and char cases
    int length = getVarcharLength(hiveType);
    TypeInfo realHiveType = (hiveType instanceof ListTypeInfo) ? ((ListTypeInfo) hiveType)
        .getListElementTypeInfo() : hiveType;

    String typeName = TypeInfoUtils.getBaseName(realHiveType.getTypeName());

    int hivePrecision = (typeName.equalsIgnoreCase(serdeConstants.DECIMAL_TYPE_NAME)) ?
        ((DecimalTypeInfo) realHiveType).getPrecision() : 0;
    int hiveScale = (typeName.equalsIgnoreCase(serdeConstants.DECIMAL_TYPE_NAME)) ?
        ((DecimalTypeInfo) realHiveType).getScale() : 0;

    switch (parquetType.getPrimitiveTypeName()) {
    case INT32:
      if (ETypeConverter.isUnsignedInteger(parquetType)) {
        return isDictionary ? new TypesFromUInt32PageReader(dictionary, length, hivePrecision,
            hiveScale) : new TypesFromUInt32PageReader(valuesReader, length, hivePrecision,
            hiveScale);
      } else if (parquetType.getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation) {
        DecimalLogicalTypeAnnotation logicalType = (DecimalLogicalTypeAnnotation) parquetType.getLogicalTypeAnnotation();
        final short scale = (short) logicalType.getScale();
        return isDictionary ? new TypesFromInt32DecimalPageReader(dictionary, length, scale, hivePrecision, hiveScale)
          : new TypesFromInt32DecimalPageReader(valuesReader, length, scale, hivePrecision, hiveScale);
      } else {
        return isDictionary ? new TypesFromInt32PageReader(dictionary, length, hivePrecision,
            hiveScale) : new TypesFromInt32PageReader(valuesReader, length, hivePrecision,
            hiveScale);
      }
    case INT64:
      LogicalTypeAnnotation logicalType = parquetType.getLogicalTypeAnnotation();
      if (logicalType instanceof TimestampLogicalTypeAnnotation) {
        TimestampLogicalTypeAnnotation timestampLogicalType = (TimestampLogicalTypeAnnotation) logicalType;
        boolean isAdjustedToUTC = timestampLogicalType.isAdjustedToUTC();
        TimeUnit timeUnit = timestampLogicalType.getUnit();
        return isDictionary ? new TypesFromInt64PageReader(dictionary, length, isAdjustedToUTC, timeUnit)
          : new TypesFromInt64PageReader(valuesReader, length, isAdjustedToUTC, timeUnit);
      }

      if (ETypeConverter.isUnsignedInteger(parquetType)) {
        return isDictionary ? new TypesFromUInt64PageReader(dictionary, length, hivePrecision, hiveScale)
          : new TypesFromUInt64PageReader(valuesReader, length, hivePrecision, hiveScale);
      }

      if (logicalType instanceof DecimalLogicalTypeAnnotation) {
        DecimalLogicalTypeAnnotation decimalLogicalType = (DecimalLogicalTypeAnnotation) logicalType;
        final short scale = (short) decimalLogicalType.getScale();
        return isDictionary ? new TypesFromInt64DecimalPageReader(dictionary, length, scale, hivePrecision, hiveScale)
          : new TypesFromInt64DecimalPageReader(valuesReader, length, scale, hivePrecision, hiveScale);
      }

      return isDictionary ? new TypesFromInt64PageReader(dictionary, length, hivePrecision, hiveScale)
        : new TypesFromInt64PageReader(valuesReader, length, hivePrecision, hiveScale);
    case FLOAT:
      return isDictionary ? new TypesFromFloatPageReader(dictionary, length, hivePrecision,
          hiveScale) : new TypesFromFloatPageReader(valuesReader, length, hivePrecision, hiveScale);
    case INT96:
      ZoneId targetZone =
          skipTimestampConversion ? ZoneOffset.UTC : firstNonNull(writerTimezone, TimeZone.getDefault().toZoneId());
      return isDictionary ?
          new TypesFromInt96PageReader(dictionary, length, targetZone, legacyConversionEnabled) :
          new TypesFromInt96PageReader(valuesReader, length, targetZone, legacyConversionEnabled);
    case BOOLEAN:
      return isDictionary ? new TypesFromBooleanPageReader(dictionary, length) : new
          TypesFromBooleanPageReader(valuesReader, length);
    case BINARY:
    case FIXED_LEN_BYTE_ARRAY:
      return getConvertorFromBinary(isDictionary, parquetType, hiveType, valuesReader, dictionary);
    case DOUBLE:
      return isDictionary ? new TypesFromDoublePageReader(dictionary, length, hivePrecision,
          hiveScale) : new TypesFromDoublePageReader(valuesReader, length, hivePrecision,
          hiveScale);
    default:
      return isDictionary ? new DefaultParquetDataColumnReader(dictionary, length, hivePrecision,
          hiveScale) : new DefaultParquetDataColumnReader(valuesReader, length, hivePrecision,
          hiveScale);
    }
  }

  private static ParquetDataColumnReader getConvertorFromBinary(boolean isDict,
                                                                PrimitiveType parquetType,
                                                                TypeInfo hiveType,
                                                                ValuesReader valuesReader,
                                                                Dictionary dictionary) {
    LogicalTypeAnnotation logicalType = parquetType.getLogicalTypeAnnotation();

    // max length for varchar and char cases
    int length = getVarcharLength(hiveType);
    TypeInfo realHiveType = (hiveType instanceof ListTypeInfo) ?
        ((ListTypeInfo) hiveType).getListElementTypeInfo() :
        (hiveType instanceof MapTypeInfo) ?
            ((MapTypeInfo) hiveType).getMapValueTypeInfo() : hiveType;

    String typeName = TypeInfoUtils.getBaseName(realHiveType.getTypeName());

    int hivePrecision = (typeName.equalsIgnoreCase(serdeConstants.DECIMAL_TYPE_NAME)) ?
        ((DecimalTypeInfo) realHiveType).getPrecision() : 0;
    int hiveScale = (typeName.equalsIgnoreCase(serdeConstants.DECIMAL_TYPE_NAME)) ?
        ((DecimalTypeInfo) realHiveType).getScale() : 0;

    if (logicalType == null) {
      return isDict ? new DefaultParquetDataColumnReader(dictionary, length) : new
          DefaultParquetDataColumnReader(valuesReader, length);
    }

    Optional<ParquetDataColumnReader> reader = parquetType.getLogicalTypeAnnotation()
        .accept(new LogicalTypeAnnotationVisitor<ParquetDataColumnReader>() {
          @Override public Optional<ParquetDataColumnReader> visit(
              DecimalLogicalTypeAnnotation logicalTypeAnnotation) {
            final short scale = (short) logicalTypeAnnotation.getScale();
            return isDict ? Optional
                .of(new TypesFromDecimalPageReader(dictionary, length, scale, hivePrecision,
                    hiveScale)) : Optional
                .of(new TypesFromDecimalPageReader(valuesReader, length, scale, hivePrecision,
                    hiveScale));
          }

          @Override public Optional<ParquetDataColumnReader> visit(
              StringLogicalTypeAnnotation logicalTypeAnnotation) {
            return isDict ? Optional
                .of(new TypesFromStringPageReader(dictionary, length)) : Optional
                .of(new TypesFromStringPageReader(valuesReader, length));
          }
        });

    if (reader.isPresent()) {
      return reader.get();
    }

    return isDict ? new DefaultParquetDataColumnReader(dictionary, length) : new
      DefaultParquetDataColumnReader(valuesReader, length);
  }

  public static ParquetDataColumnReader getDataColumnReaderByTypeOnDictionary(
      PrimitiveType parquetType,
      TypeInfo hiveType,
      Dictionary realReader,
      boolean skipTimestampConversion,
      ZoneId writerTimezone,
      boolean legacyConversionEnabled)
      throws IOException {
    return getDataColumnReaderByTypeHelper(true, parquetType, hiveType, realReader, null,
        skipTimestampConversion, writerTimezone, legacyConversionEnabled);
  }

  public static ParquetDataColumnReader getDataColumnReaderByType(PrimitiveType parquetType,
      TypeInfo hiveType, ValuesReader realReader, boolean skipTimestampConversion,
      ZoneId writerTimezone, boolean legacyConversionEnabled)
      throws IOException {
    return getDataColumnReaderByTypeHelper(false, parquetType, hiveType, null, realReader,
        skipTimestampConversion, writerTimezone, legacyConversionEnabled);
  }


  // For Varchar or char type, return the max length of the type
  private static int getVarcharLength(TypeInfo hiveType) {
    int length = -1;
    if (hiveType instanceof PrimitiveTypeInfo) {
      PrimitiveTypeInfo hivePrimitiveType = (PrimitiveTypeInfo) hiveType;
      switch (hivePrimitiveType.getPrimitiveCategory()) {
      case CHAR:
        length = ((CharTypeInfo) hivePrimitiveType).getLength();
        break;
      case VARCHAR:
        length = ((VarcharTypeInfo) hivePrimitiveType).getLength();
        break;
      default:
        break;
      }
    }

    return length;
  }
}
