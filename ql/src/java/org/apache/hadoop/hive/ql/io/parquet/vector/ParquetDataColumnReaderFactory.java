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
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

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
   */
  public static class DefaultParquetDataColumnReader implements ParquetDataColumnReader {
    protected ValuesReader valuesReader;
    protected Dictionary dict;

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
    public boolean isValid(long value) {
      return true;
    }

    @Override
    public boolean isValid(float value) {
      return true;
    }

    @Override
    public boolean isValid(double value) {
      return true;
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
     * Enforce the max legnth of varchar or char.
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
      try {
        // convert integer to string
        return value.getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException("Failed to encode string in UTF-8", e);
      }
    }
  }

  /**
   * The reader who reads from the underlying int32 value value. Implementation is in consist with
   * ETypeConverter EINT32_CONVERTER
   */
  public static class TypesFromInt32PageReader extends DefaultParquetDataColumnReader {

    public TypesFromInt32PageReader(ValuesReader realReader, int length) {
      super(realReader, length);
    }

    public TypesFromInt32PageReader(Dictionary dict, int length) {
      super(dict, length);
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
  }

  /**
   * The reader who reads from the underlying int64 value value. Implementation is in consist with
   * ETypeConverter EINT64_CONVERTER
   */
  public static class TypesFromInt64PageReader extends DefaultParquetDataColumnReader {

    public TypesFromInt64PageReader(ValuesReader realReader, int length) {
      super(realReader, length);
    }

    public TypesFromInt64PageReader(Dictionary dict, int length) {
      super(dict, length);
    }

    @Override
    public long readInteger() {
      return valuesReader.readLong();
    }

    @Override
    public long readInteger(int id) {
      return dict.decodeToLong(id);
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

    private static String convertToString(long value) {
      return Long.toString(value);
    }

    private static byte[] convertToBytes(long value) {
      return convertToBytes(convertToString(value));
    }
  }

  /**
   * The reader who reads long data using int type.
   */
  public static class Types64Int2IntPageReader extends TypesFromInt64PageReader {

    public Types64Int2IntPageReader(ValuesReader realReader, int length) {
      super(realReader, length);
    }

    public Types64Int2IntPageReader(Dictionary dict, int length) {
      super(dict, length);
    }

    @Override
    public boolean isValid(long value) {
      return ((value <= Integer.MAX_VALUE) && (value >= Integer.MIN_VALUE));
    }
  }

  /**
   * The reader who reads long data using smallint type.
   */
  public static class Types64Int2SmallintPageReader extends TypesFromInt64PageReader {
    public Types64Int2SmallintPageReader(ValuesReader realReader, int length) {
      super(realReader, length);
    }

    public Types64Int2SmallintPageReader(Dictionary dict, int length) {
      super(dict, length);
    }

    @Override
    public boolean isValid(long value) {
      return ((value <= Short.MAX_VALUE) && (value >= Short.MIN_VALUE));
    }
  }

  /**
   * The reader who reads long data using tinyint type.
   */
  public static class Types64Int2TinyintPageReader extends TypesFromInt64PageReader {
    public Types64Int2TinyintPageReader(ValuesReader realReader, int length) {
      super(realReader, length);
    }

    public Types64Int2TinyintPageReader(Dictionary dict, int length) {
      super(dict, length);
    }

    @Override
    public boolean isValid(long value) {
      return ((value <= Byte.MAX_VALUE) && (value >= Byte.MIN_VALUE));
    }
  }

  /**
   * The reader who reads long data using Decimal type.
   */
  public static class Types64Int2DecimalPageReader extends TypesFromInt64PageReader {
    private int precision = 0;
    private int scale = 0;
    private final HiveDecimalWritable hiveDecimalWritable = new HiveDecimalWritable(0L);

    public Types64Int2DecimalPageReader(ValuesReader realReader, int length, int precision,
        int scale) {
      super(realReader, length);
      this.precision = precision;
      this.scale = scale;
    }

    public Types64Int2DecimalPageReader(Dictionary dict, int length, int precision, int scale) {
      super(dict, length);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public boolean isValid(long value) {
      hiveDecimalWritable.setFromLong(value);
      hiveDecimalWritable.mutateEnforcePrecisionScale(precision, scale);
      return hiveDecimalWritable.isSet();
    }
  }

  /**
   * The reader who reads unsigned long data.
   */
  public static class TypesFromUInt64PageReader extends TypesFromInt64PageReader {

    public TypesFromUInt64PageReader(ValuesReader realReader, int length) {
      super(realReader, length);
    }

    public TypesFromUInt64PageReader(Dictionary dict, int length) {
      super(dict, length);
    }

    @Override
    public boolean isValid(long value) {
      return (value >= 0);
    }

    @Override
    public boolean isValid(float value) {
      return (value >= 0);
    }

    @Override
    public boolean isValid(double value) {
      return (value >= 0);
    }
  }

  /**
   * The reader who reads unsigned long data using int type.
   */
  public static class Types64UInt2IntPageReader extends TypesFromInt64PageReader {

    public Types64UInt2IntPageReader(ValuesReader realReader, int length) {
      super(realReader, length);
    }

    public Types64UInt2IntPageReader(Dictionary dict, int length) {
      super(dict, length);
    }

    @Override
    public boolean isValid(long value) {
      return ((value <= Integer.MAX_VALUE) && (value >= 0));
    }
  }

  /**
   * The reader who reads unsigned long data using smallint type.
   */
  public static class Types64UInt2SmallintPageReader extends TypesFromInt64PageReader {
    public Types64UInt2SmallintPageReader(ValuesReader realReader, int length) {
      super(realReader, length);
    }

    public Types64UInt2SmallintPageReader(Dictionary dict, int length) {
      super(dict, length);
    }

    @Override
    public boolean isValid(long value) {
      return ((value <= Short.MAX_VALUE) && (value >= 0));
    }
  }

  /**
   * The reader who reads unsigned long data using tinyint type.
   */
  public static class Types64UInt2TinyintPageReader extends TypesFromInt64PageReader {
    public Types64UInt2TinyintPageReader(ValuesReader realReader, int length) {
      super(realReader, length);
    }

    public Types64UInt2TinyintPageReader(Dictionary dict, int length) {
      super(dict, length);
    }

    @Override
    public boolean isValid(long value) {
      return ((value <= Byte.MAX_VALUE) && (value >= 0));
    }
  }

  /**
   * The reader who reads unsigned long data using Decimal type.
   */
  public static class Types64UInt2DecimalPageReader extends TypesFromInt64PageReader {
    private int precision = 0;
    private int scale = 0;
    private final HiveDecimalWritable hiveDecimalWritable = new HiveDecimalWritable(0L);

    public Types64UInt2DecimalPageReader(ValuesReader realReader, int length, int precision,
        int scale) {
      super(realReader, length);
      this.precision = precision;
      this.scale = scale;
    }

    public Types64UInt2DecimalPageReader(Dictionary dict, int length, int precision, int scale) {
      super(dict, length);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public boolean isValid(long value) {
      hiveDecimalWritable.setFromLong(value);
      hiveDecimalWritable.mutateEnforcePrecisionScale(precision, scale);
      return ((value >= 0) && hiveDecimalWritable.isSet());
    }
  }

  /**
   * The reader who reads int data using smallint type.
   */
  public static class Types32Int2SmallintPageReader extends TypesFromInt32PageReader {
    public Types32Int2SmallintPageReader(ValuesReader realReader, int length) {
      super(realReader, length);
    }

    public Types32Int2SmallintPageReader(Dictionary dict, int length) {
      super(dict, length);
    }

    @Override
    public boolean isValid(long value) {
      return ((value <= Short.MAX_VALUE) && (value >= Short.MIN_VALUE));
    }
  }

  /**
   * The reader who reads int data using tinyint type.
   */
  public static class Types32Int2TinyintPageReader extends TypesFromInt32PageReader {
    public Types32Int2TinyintPageReader(ValuesReader realReader, int length) {
      super(realReader, length);
    }

    public Types32Int2TinyintPageReader(Dictionary dict, int length) {
      super(dict, length);
    }

    @Override
    public boolean isValid(long value) {
      return ((value <= Byte.MAX_VALUE) && (value >= Byte.MIN_VALUE));
    }
  }

  /**
   * The reader who reads int data using Decimal type.
   */
  public static class Types32Int2DecimalPageReader extends TypesFromInt32PageReader {
    private int precision = 0;
    private int scale = 0;
    private final HiveDecimalWritable hiveDecimalWritable = new HiveDecimalWritable(0L);

    public Types32Int2DecimalPageReader(ValuesReader realReader, int length, int precision,
        int scale) {
      super(realReader, length);
      this.precision = precision;
      this.scale = scale;
    }

    public Types32Int2DecimalPageReader(Dictionary dict, int length, int precision, int scale) {
      super(dict, length);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public boolean isValid(long value) {
      hiveDecimalWritable.setFromLong(value);
      hiveDecimalWritable.mutateEnforcePrecisionScale(precision, scale);
      return hiveDecimalWritable.isSet();
    }
  }

  /**
   * The reader who reads unsigned int data.
   */
  public static class TypesFromUInt32PageReader extends TypesFromInt32PageReader {
    public TypesFromUInt32PageReader(ValuesReader realReader, int length) {
      super(realReader, length);
    }

    public TypesFromUInt32PageReader(Dictionary dict, int length) {
      super(dict, length);
    }

    @Override
    public boolean isValid(long value) {
      return (value >= 0);
    }

    @Override
    public boolean isValid(float value) {
      return (value >= 0);
    }

    @Override
    public boolean isValid(double value) {
      return (value >= 0);
    }
  }

  /**
   * The reader who reads unsigned int data using smallint type.
   */
  public static class Types32UInt2SmallintPageReader extends TypesFromInt32PageReader {
    public Types32UInt2SmallintPageReader(ValuesReader realReader, int length) {
      super(realReader, length);
    }

    public Types32UInt2SmallintPageReader(Dictionary dict, int length) {
      super(dict, length);
    }

    @Override
    public boolean isValid(long value) {
      return ((value <= Short.MAX_VALUE) && (value >= 0));
    }
  }

  /**
   * The reader who reads unsigned int data using tinyint type.
   */
  public static class Types32UInt2TinyintPageReader extends TypesFromInt32PageReader {
    public Types32UInt2TinyintPageReader(ValuesReader realReader, int length) {
      super(realReader, length);
    }

    public Types32UInt2TinyintPageReader(Dictionary dict, int length) {
      super(dict, length);
    }

    @Override
    public boolean isValid(long value) {
      return ((value <= Byte.MAX_VALUE) && (value >= 0));
    }
  }

  /**
   * The reader who reads unsigned int data using Decimal type.
   */
  public static class Types32UInt2DecimalPageReader extends TypesFromInt32PageReader {
    private int precision = 0;
    private int scale = 0;
    private final HiveDecimalWritable hiveDecimalWritable = new HiveDecimalWritable(0L);

    public Types32UInt2DecimalPageReader(ValuesReader realReader, int length, int precision,
        int scale) {
      super(realReader, length);
      this.precision = precision;
      this.scale = scale;
    }

    public Types32UInt2DecimalPageReader(Dictionary dict, int length, int precision, int scale) {
      super(dict, length);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public boolean isValid(long value) {
      hiveDecimalWritable.setFromLong(value);
      hiveDecimalWritable.mutateEnforcePrecisionScale(precision, scale);
      return ((value >= 0) && hiveDecimalWritable.isSet());
    }
  }

  /**
   * The reader who reads from the underlying float value value. Implementation is in consist with
   * ETypeConverter EFLOAT_CONVERTER
   */
  public static class TypesFromFloatPageReader extends DefaultParquetDataColumnReader {

    public TypesFromFloatPageReader(ValuesReader realReader, int length) {
      super(realReader, length);
    }

    public TypesFromFloatPageReader(Dictionary realReader, int length) {
      super(realReader, length);
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

    private static String convertToString(float value) {
      return Float.toString(value);
    }

    private static byte[] convertToBytes(float value) {
      return convertToBytes(convertToString(value));
    }
  }

  /**
   * The reader who reads from the underlying double value value.
   */
  public static class TypesFromDoublePageReader extends DefaultParquetDataColumnReader {

    public TypesFromDoublePageReader(ValuesReader realReader, int length) {
      super(realReader, length);
    }

    public TypesFromDoublePageReader(Dictionary dict, int length) {
      super(dict, length);
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

    private static String convertToString(double value) {
      return Double.toString(value);
    }

    private static byte[] convertToBytes(double value) {
      return convertToBytes(convertToString(value));
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
    private boolean skipTimestampConversion = false;

    public TypesFromInt96PageReader(ValuesReader realReader, int length,
                                    boolean skipTimestampConversion) {
      super(realReader, length);
      this.skipTimestampConversion = skipTimestampConversion;
    }

    public TypesFromInt96PageReader(Dictionary dict, int length, boolean skipTimestampConversion) {
      super(dict, length);
      this.skipTimestampConversion = skipTimestampConversion;
    }

    private Timestamp convert(Binary binary) {
      ByteBuffer buf = binary.toByteBuffer();
      buf.order(ByteOrder.LITTLE_ENDIAN);
      long timeOfDayNanos = buf.getLong();
      int julianDay = buf.getInt();
      NanoTime nt = new NanoTime(julianDay, timeOfDayNanos);
      return NanoTimeUtils.getTimestamp(nt, skipTimestampConversion);
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
   */
  public static class TypesFromDecimalPageReader extends DefaultParquetDataColumnReader {
    private HiveDecimalWritable tempDecimal = new HiveDecimalWritable();
    private short scale;

    public TypesFromDecimalPageReader(ValuesReader realReader, int length, short scale) {
      super(realReader, length);
      this.scale = scale;
    }

    public TypesFromDecimalPageReader(Dictionary dict, int length, short scale) {
      super(dict, length);
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

    private String convertToString(Binary value) {
      tempDecimal.set(value.getBytesUnsafe(), scale);
      return tempDecimal.toString();
    }

    private byte[] convertToBytes(Binary value) {
      return convertToBytes(convertToString(value));
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
                                                                         boolean
                                                                             skipTimestampConversion)
      throws IOException {
    // max length for varchar and char cases
    int length = getVarcharLength(hiveType);
    String typeName = TypeInfoUtils.getBaseName(hiveType.getTypeName());

    switch (parquetType.getPrimitiveTypeName()) {
    case INT32:
      if (OriginalType.UINT_8 == parquetType.getOriginalType() ||
          OriginalType.UINT_16 == parquetType.getOriginalType() ||
          OriginalType.UINT_32 == parquetType.getOriginalType() ||
          OriginalType.UINT_64 == parquetType.getOriginalType()) {
        switch (typeName) {
        case serdeConstants.SMALLINT_TYPE_NAME:
          return isDictionary ? new Types32UInt2SmallintPageReader(dictionary,
              length) : new Types32UInt2SmallintPageReader(valuesReader, length);
        case serdeConstants.TINYINT_TYPE_NAME:
          return isDictionary ? new Types32UInt2TinyintPageReader(dictionary,
              length) : new Types32UInt2TinyintPageReader(valuesReader, length);
        case serdeConstants.DECIMAL_TYPE_NAME:
          return isDictionary ?
              new Types32UInt2DecimalPageReader(dictionary, length,
                  ((DecimalTypeInfo) hiveType).getPrecision(),
                  ((DecimalTypeInfo) hiveType).getScale()) :
              new Types32UInt2DecimalPageReader(valuesReader, length,
                  ((DecimalTypeInfo) hiveType).getPrecision(),
                  ((DecimalTypeInfo) hiveType).getScale());
        default:
          return isDictionary ? new TypesFromUInt32PageReader(dictionary,
              length) : new TypesFromUInt32PageReader(valuesReader, length);
        }
      } else {
        switch (typeName) {
        case serdeConstants.SMALLINT_TYPE_NAME:
          return isDictionary ? new Types32Int2SmallintPageReader(dictionary,
              length) : new Types32Int2SmallintPageReader(valuesReader, length);
        case serdeConstants.TINYINT_TYPE_NAME:
          return isDictionary ? new Types32Int2TinyintPageReader(dictionary,
              length) : new Types32Int2TinyintPageReader(valuesReader, length);
        case serdeConstants.DECIMAL_TYPE_NAME:
          return isDictionary ?
              new Types32Int2DecimalPageReader(dictionary, length,
                  ((DecimalTypeInfo) hiveType).getPrecision(),
                  ((DecimalTypeInfo) hiveType).getScale()) :
              new Types32Int2DecimalPageReader(valuesReader, length,
                  ((DecimalTypeInfo) hiveType).getPrecision(),
                  ((DecimalTypeInfo) hiveType).getScale());
        default:
          return isDictionary ? new TypesFromInt32PageReader(dictionary,
              length) : new TypesFromInt32PageReader(valuesReader, length);
        }
      }
    case INT64:
      if (OriginalType.UINT_8 == parquetType.getOriginalType() ||
          OriginalType.UINT_16 == parquetType.getOriginalType() ||
          OriginalType.UINT_32 == parquetType.getOriginalType() ||
          OriginalType.UINT_64 == parquetType.getOriginalType()) {
        switch (typeName) {
        case serdeConstants.INT_TYPE_NAME:
          return isDictionary ? new Types64UInt2IntPageReader(dictionary,
              length) : new Types64UInt2IntPageReader(valuesReader, length);
        case serdeConstants.SMALLINT_TYPE_NAME:
          return isDictionary ? new Types64UInt2SmallintPageReader(dictionary,
              length) : new Types64UInt2SmallintPageReader(valuesReader, length);
        case serdeConstants.TINYINT_TYPE_NAME:
          return isDictionary ? new Types64UInt2TinyintPageReader(dictionary,
              length) : new Types64UInt2TinyintPageReader(valuesReader, length);
        case serdeConstants.DECIMAL_TYPE_NAME:
          return isDictionary ?
              new Types64UInt2DecimalPageReader(dictionary, length,
                  ((DecimalTypeInfo) hiveType).getPrecision(),
                  ((DecimalTypeInfo) hiveType).getScale()) :
              new Types64UInt2DecimalPageReader(valuesReader, length,
                  ((DecimalTypeInfo) hiveType).getPrecision(),
                  ((DecimalTypeInfo) hiveType).getScale());
        default:
          return isDictionary ? new TypesFromUInt64PageReader(dictionary,
              length) : new TypesFromUInt64PageReader(valuesReader, length);
        }
      } else {
        switch (typeName) {
        case serdeConstants.INT_TYPE_NAME:
          return isDictionary ? new Types64Int2IntPageReader(dictionary,
              length) : new Types64Int2IntPageReader(valuesReader, length);
        case serdeConstants.SMALLINT_TYPE_NAME:
          return isDictionary ? new Types64Int2SmallintPageReader(dictionary,
              length) : new Types64Int2SmallintPageReader(valuesReader, length);
        case serdeConstants.TINYINT_TYPE_NAME:
          return isDictionary ? new Types64Int2TinyintPageReader(dictionary,
              length) : new Types64Int2TinyintPageReader(valuesReader, length);
        case serdeConstants.DECIMAL_TYPE_NAME:
          return isDictionary ?
              new Types64Int2DecimalPageReader(dictionary, length,
                  ((DecimalTypeInfo) hiveType).getPrecision(),
                  ((DecimalTypeInfo) hiveType).getScale()) :
              new Types64Int2DecimalPageReader(valuesReader, length,
                  ((DecimalTypeInfo) hiveType).getPrecision(),
                  ((DecimalTypeInfo) hiveType).getScale());
        default:
          return isDictionary ? new TypesFromInt64PageReader(dictionary,
              length) : new TypesFromInt64PageReader(valuesReader, length);
        }
      }
    case FLOAT:
      return isDictionary ? new TypesFromFloatPageReader(dictionary, length) : new
          TypesFromFloatPageReader(valuesReader, length);
    case INT96:
      return isDictionary ? new TypesFromInt96PageReader(dictionary, length,
          skipTimestampConversion) : new
          TypesFromInt96PageReader(valuesReader, length, skipTimestampConversion);
    case BOOLEAN:
      return isDictionary ? new TypesFromBooleanPageReader(dictionary, length) : new
          TypesFromBooleanPageReader(valuesReader, length);
    case BINARY:
    case FIXED_LEN_BYTE_ARRAY:
      return getConvertorFromBinary(isDictionary, parquetType, hiveType, valuesReader, dictionary);
    case DOUBLE:
      return isDictionary ? new TypesFromDoublePageReader(dictionary, length) : new
          TypesFromDoublePageReader(valuesReader, length);
    default:
      return isDictionary ? new DefaultParquetDataColumnReader(dictionary, length) : new
          DefaultParquetDataColumnReader(valuesReader, length);
    }
  }

  private static ParquetDataColumnReader getConvertorFromBinary(boolean isDict,
                                                                PrimitiveType parquetType,
                                                                TypeInfo hiveType,
                                                                ValuesReader valuesReader,
                                                                Dictionary dictionary) {
    OriginalType originalType = parquetType.getOriginalType();

    // max length for varchar and char cases
    int length = getVarcharLength(hiveType);

    if (originalType == null) {
      return isDict ? new DefaultParquetDataColumnReader(dictionary, length) : new
          DefaultParquetDataColumnReader(valuesReader, length);
    }
    switch (originalType) {
    case DECIMAL:
      final short scale = (short) parquetType.asPrimitiveType().getDecimalMetadata().getScale();
      return isDict ? new TypesFromDecimalPageReader(dictionary, length, scale) : new
          TypesFromDecimalPageReader(valuesReader, length, scale);
    case UTF8:
      return isDict ? new TypesFromStringPageReader(dictionary, length) : new
          TypesFromStringPageReader(valuesReader, length);
    default:
      return isDict ? new DefaultParquetDataColumnReader(dictionary, length) : new
          DefaultParquetDataColumnReader(valuesReader, length);
    }
  }

  public static ParquetDataColumnReader getDataColumnReaderByTypeOnDictionary(
      PrimitiveType parquetType,
      TypeInfo hiveType,
      Dictionary realReader, boolean skipTimestampConversion)
      throws IOException {
    return getDataColumnReaderByTypeHelper(true, parquetType, hiveType, realReader, null,
        skipTimestampConversion);
  }

  public static ParquetDataColumnReader getDataColumnReaderByType(PrimitiveType parquetType,
                                                                  TypeInfo hiveType,
                                                                  ValuesReader realReader,
                                                                  boolean skipTimestampConversion)
      throws IOException {
    return getDataColumnReaderByTypeHelper(false, parquetType, hiveType, null, realReader,
        skipTimestampConversion);
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
