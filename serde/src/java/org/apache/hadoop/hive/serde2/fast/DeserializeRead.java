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

package org.apache.hadoop.hive.serde2.fast;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;

/*
 * Directly deserialize with the caller reading field-by-field a serialization format.
 *
 * The caller is responsible for calling the read method for the right type of each field
 * (after calling readNextField).
 *
 * Reading some fields require a results object to receive value information.  A separate
 * results object is created by the caller at initialization per different field even for the same
 * type.
 *
 * Some type values are by reference to either bytes in the deserialization buffer or to
 * other type specific buffers.  So, those references are only valid until the next time set is
 * called.
 */
public abstract class DeserializeRead {

  protected final TypeInfo[] typeInfos;

  // NOTE: Currently, read variations only apply to top level data types...
  protected DataTypePhysicalVariation[] dataTypePhysicalVariations;

  protected final boolean useExternalBuffer;

  protected final Category[] categories;
  protected final PrimitiveCategory[] primitiveCategories;

  /*
   * This class is used to read one field at a time.  Simple fields like long, double, int are read
   * into to primitive current* members; the non-simple field types like Date, Timestamp, etc, are
   * read into a current object that this method will allocate.
   *
   * This method handles complex type fields by recursively calling this method.
   */
  private void allocateCurrentWritable(TypeInfo typeInfo) {
    switch (typeInfo.getCategory()) {
    case PRIMITIVE:
      switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
      case DATE:
        if (currentDateWritable == null) {
          currentDateWritable = new DateWritableV2();
        }
        break;
      case TIMESTAMP:
        if (currentTimestampWritable == null) {
          currentTimestampWritable = new TimestampWritableV2();
        }
        break;
      case INTERVAL_YEAR_MONTH:
        if (currentHiveIntervalYearMonthWritable == null) {
          currentHiveIntervalYearMonthWritable = new HiveIntervalYearMonthWritable();
        }
        break;
      case INTERVAL_DAY_TIME:
        if (currentHiveIntervalDayTimeWritable == null) {
          currentHiveIntervalDayTimeWritable = new HiveIntervalDayTimeWritable();
        }
        break;
      case DECIMAL:
        if (currentHiveDecimalWritable == null) {
          currentHiveDecimalWritable = new HiveDecimalWritable();
        }
        break;
      default:
        // No writable needed for this data type.
      }
      break;
    case LIST:
      allocateCurrentWritable(((ListTypeInfo) typeInfo).getListElementTypeInfo());
      break;
    case MAP:
      allocateCurrentWritable(((MapTypeInfo) typeInfo).getMapKeyTypeInfo());
      allocateCurrentWritable(((MapTypeInfo) typeInfo).getMapValueTypeInfo());
      break;
    case STRUCT:
      for (TypeInfo fieldTypeInfo : ((StructTypeInfo) typeInfo).getAllStructFieldTypeInfos()) {
        allocateCurrentWritable(fieldTypeInfo);
      }
      break;
    case UNION:
      for (TypeInfo fieldTypeInfo : ((UnionTypeInfo) typeInfo).getAllUnionObjectTypeInfos()) {
        allocateCurrentWritable(fieldTypeInfo);
      }
      break;
    default:
      throw new RuntimeException("Unexpected category " + typeInfo.getCategory());
    }
  }

  /**
   * Constructor.
   *
   * When useExternalBuffer is specified true and readNextField reads a string/char/varchar/binary
   * field, it will request an external buffer to receive the data of format conversion.
   *
   * if (deserializeRead.readNextField()) {
   *   if (deserializeRead.currentExternalBufferNeeded) {
   *     &lt;Ensure external buffer is as least deserializeRead.currentExternalBufferNeededLen bytes&gt;
   *     deserializeRead.copyToExternalBuffer(externalBuffer, externalBufferStart);
   *   } else {
   *     &lt;Otherwise, field data is available in the currentBytes, currentBytesStart, and
   *      currentBytesLength of deserializeRead&gt;
   *   }
   *
   * @param typeInfos
   * @param dataTypePhysicalVariations
   *                            Specify for each corresponding TypeInfo a read variation. Can be
   *                            null.  dataTypePhysicalVariation.NONE is then assumed.
   * @param useExternalBuffer   Specify true when the caller is prepared to provide a bytes buffer
   *                            to receive a string/char/varchar/binary field that needs format
   *                            conversion.
   */
  public DeserializeRead(TypeInfo[] typeInfos, DataTypePhysicalVariation[] dataTypePhysicalVariations,
      boolean useExternalBuffer) {
    this.typeInfos = typeInfos;
    final int count = typeInfos.length;
    if (dataTypePhysicalVariations != null) {
      this.dataTypePhysicalVariations = dataTypePhysicalVariations;
    } else {
      this.dataTypePhysicalVariations = new DataTypePhysicalVariation[count];
      Arrays.fill(this.dataTypePhysicalVariations, DataTypePhysicalVariation.NONE);
    }
    categories = new Category[count];
    primitiveCategories = new PrimitiveCategory[count];
    for (int i = 0; i < count; i++) {
      TypeInfo typeInfo = typeInfos[i];
      Category category = typeInfo.getCategory();
      categories[i] = category;
      if (category == Category.PRIMITIVE) {
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
        PrimitiveCategory primitiveCategory = primitiveTypeInfo.getPrimitiveCategory();
        primitiveCategories[i] = primitiveCategory;
      }
      allocateCurrentWritable(typeInfo);
    }
    this.useExternalBuffer = useExternalBuffer;
  }

  public DeserializeRead(TypeInfo[] typeInfos, boolean useExternalBuffer) {
    this(typeInfos, null, useExternalBuffer);
  }

  // Don't allow for public.
  protected DeserializeRead() {
    // Initialize to satisfy compiler finals.
    typeInfos = null;
    useExternalBuffer = false;
    categories = null;
    primitiveCategories = null;
  }

  /*
   * The type information for all fields.
   */
  public TypeInfo[] typeInfos() {
    return typeInfos;
  }

  /*
   * Get optional read variations for fields.
   */
  public DataTypePhysicalVariation[] getDataTypePhysicalVariations() {
    return dataTypePhysicalVariations;
  }

  /*
   * Set the range of bytes to be deserialized.
   */
  public abstract void set(byte[] bytes, int offset, int length);

  /*
   * Reads the the next field.
   *
   * Afterwards, reading is positioned to the next field.
   *
   * @return  Return true when the field was not null and data is put in the appropriate
   *          current* member.
   *          Otherwise, false when the field is null.
   *
   */
  public abstract boolean readNextField() throws IOException;

  /*
   * Reads through an undesired field.
   *
   * No data values are valid after this call.
   * Designed for skipping columns that are not included.
   */
  public abstract void skipNextField() throws IOException;

  /*
   * Returns true if the readField method is supported;
   */
  public boolean isReadFieldSupported() {
    return false;
  }

  /*
   * When supported, read a field by field number (i.e. random access).
   *
   * Currently, only LazySimpleDeserializeRead supports this.
   *
   * @return  Return true when the field was not null and data is put in the appropriate
   *          current* member.
   *          Otherwise, false when the field is null.
   */
  public boolean readField(int fieldIndex) throws IOException {
    throw new RuntimeException("Not supported");
  }

  /*
   * Tests whether there is another List element or another Map key/value pair.
   */
  public abstract boolean isNextComplexMultiValue() throws IOException;

  /*
   * Read a field that is under a complex type.  It may be a primitive type or deeper complex type.
   */
  public abstract boolean readComplexField() throws IOException;

  /*
   * Used by Struct and Union complex type readers to indicate the (final) field has been fully
   * read and the current complex type is finished.
   */
  public abstract void finishComplexVariableFieldsType();

  /*
   * Call this method may be called after all the all fields have been read to check
   * for unread fields.
   *
   * Note that when optimizing reading to stop reading unneeded include columns, worrying
   * about whether all data is consumed is not appropriate (often we aren't reading it all by
   * design).
   *
   * Since LazySimpleDeserializeRead parses the line through the last desired column it does
   * support this function.
   */
  public abstract boolean isEndOfInputReached();

  /*
   * Get detailed read position information to help diagnose exceptions.
   */
  public abstract String getDetailedReadPositionString();

  /*
   * These members hold the current value that was read when readNextField return false.
   */

  /*
   * BOOLEAN.
   */
  public boolean currentBoolean;

  /*
   * BYTE.
   */
  public byte currentByte;

  /*
   * SHORT.
   */
  public short currentShort;

  /*
   * INT.
   */
  public int currentInt;

  /*
   * LONG.
   */
  public long currentLong;

  /*
   * FLOAT.
   */
  public float currentFloat;

  /*
   * DOUBLE.
   */
  public double currentDouble;

  /*
   * STRING, CHAR, VARCHAR, and BINARY.
   *
   * For CHAR and VARCHAR when the caller takes responsibility for
   * truncation/padding issues.
   *
   * When currentExternalBufferNeeded is true, conversion is needed into an external buffer of
   * at least currentExternalBufferNeededLen bytes.  Use copyToExternalBuffer to get the result.
   *
   * Otherwise, currentBytes, currentBytesStart, and currentBytesLength are the result.
   */
  public boolean currentExternalBufferNeeded;
  public int currentExternalBufferNeededLen;

  public void copyToExternalBuffer(byte[] externalBuffer, int externalBufferStart) throws IOException {
    throw new RuntimeException("Not implemented");
  }

  public byte[] currentBytes;
  public int currentBytesStart;
  public int currentBytesLength;

  /*
   * DATE.
   */
  public DateWritableV2 currentDateWritable;

  /*
   * TIMESTAMP.
   */
  public TimestampWritableV2 currentTimestampWritable;

  /*
   * INTERVAL_YEAR_MONTH.
   */
  public HiveIntervalYearMonthWritable currentHiveIntervalYearMonthWritable;

  /*
   * INTERVAL_DAY_TIME.
   */
  public HiveIntervalDayTimeWritable currentHiveIntervalDayTimeWritable;

  /*
   * DECIMAL.
   */
  public HiveDecimalWritable currentHiveDecimalWritable;

  /*
   * DECIMAL_64.
   */
  public long currentDecimal64;
}