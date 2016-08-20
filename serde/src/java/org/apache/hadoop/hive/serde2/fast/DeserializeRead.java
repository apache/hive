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

package org.apache.hadoop.hive.serde2.fast;

import java.io.IOException;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/*
 * Directly deserialize with the caller reading field-by-field a serialization format.
 *
 * The caller is responsible for calling the read method for the right type of each field
 * (after calling readCheckNull).
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

  protected TypeInfo[] typeInfos;

  protected boolean[] columnsToInclude;

  protected Category[] categories;
  protected PrimitiveCategory[] primitiveCategories;

  public DeserializeRead(TypeInfo[] typeInfos) {
    this.typeInfos = typeInfos;
    final int count = typeInfos.length;
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

        switch (primitiveCategory) {
        case DATE:
          if (currentDateWritable == null) {
            currentDateWritable = new DateWritable();
          }
          break;
        case TIMESTAMP:
          if (currentTimestampWritable == null) {
            currentTimestampWritable = new TimestampWritable();
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
      }
    }

    columnsToInclude = null;
  }

  // Don't allow for public.
  protected DeserializeRead() {
  }

  /*
   * The type information for all fields.
   */
  public TypeInfo[] typeInfos() {
    return typeInfos;
  }

  /*
   * If some fields are are not going to be used by the query, use this routine to specify
   * the columns to return.  The readCheckNull method will automatically return NULL for the
   * other columns.
   */
  public void setColumnsToInclude(boolean[] columnsToInclude) {
    this.columnsToInclude = columnsToInclude;
  }

  /*
   * Set the range of bytes to be deserialized.
   */
  public abstract void set(byte[] bytes, int offset, int length);

  /*
   * Reads the NULL information for a field.
   *
   * @return Return true when the field is NULL; reading is positioned to the next field.
   *         Otherwise, false when the field is NOT NULL; reading is positioned to the field data.
   */
  public abstract boolean readCheckNull() throws IOException;

  /*
   * Call this method after all fields have been read to check for extra fields.
   */
  public abstract void extraFieldsCheck();

  /*
   * Read integrity warning flags.
   */
  public abstract boolean readBeyondConfiguredFieldsWarned();
  public abstract boolean bufferRangeHasExtraDataWarned();

  /*
   * Get detailed read position information to help diagnose exceptions.
   */
  public abstract String getDetailedReadPositionString();

  /*
   * These members hold the current value that was read when readCheckNull return false.
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
   */
  public byte[] currentBytes;
  public int currentBytesStart;
  public int currentBytesLength;

  /*
   * DATE.
   */
  public DateWritable currentDateWritable;

  /*
   * TIMESTAMP.
   */
  public TimestampWritable currentTimestampWritable;

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
}