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

package org.apache.hadoop.hive.serde2.lazybinary.fast;

import java.io.EOFException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.VInt;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.VLong;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

/*
 * Directly deserialize with the caller reading field-by-field the LazyBinary serialization format.
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
public class LazyBinaryDeserializeRead implements DeserializeRead {
  public static final Log LOG = LogFactory.getLog(LazyBinaryDeserializeRead.class.getName());

  private PrimitiveTypeInfo[] primitiveTypeInfos;

  private byte[] bytes;
  private int start;
  private int offset;
  private int end;
  private int fieldCount;
  private int fieldIndex;
  private byte nullByte;

  private DecimalTypeInfo saveDecimalTypeInfo;
  private HiveDecimal saveDecimal;

  // Object to receive results of reading a decoded variable length int or long.
  private VInt tempVInt;
  private VLong tempVLong;
  private HiveDecimalWritable tempHiveDecimalWritable;

  private boolean readBeyondConfiguredFieldsWarned;
  private boolean readBeyondBufferRangeWarned;
  private boolean bufferRangeHasExtraDataWarned;

  public LazyBinaryDeserializeRead(PrimitiveTypeInfo[] primitiveTypeInfos) {
    this.primitiveTypeInfos = primitiveTypeInfos;
    fieldCount = primitiveTypeInfos.length;
    tempVInt = new VInt();
    tempVLong = new VLong();
    readBeyondConfiguredFieldsWarned = false;
    readBeyondBufferRangeWarned = false;
    bufferRangeHasExtraDataWarned = false;
  }

  // Not public since we must have the field count so every 8 fields NULL bytes can be navigated.
  private LazyBinaryDeserializeRead() {
  }

  /*
   * The primitive type information for all fields.
   */
  public PrimitiveTypeInfo[] primitiveTypeInfos() {
    return primitiveTypeInfos;
  }

  /*
   * Set the range of bytes to be deserialized.
   */
  @Override
  public void set(byte[] bytes, int offset, int length) {
    this.bytes = bytes;
    this.offset = offset;
    start = offset;
    end = offset + length;
    fieldIndex = 0;
  }

  /*
   * Reads the NULL information for a field.
   *
   * @return Returns true when the field is NULL; reading is positioned to the next field.
   *         Otherwise, false when the field is NOT NULL; reading is positioned to the field data.
   */
  @Override
  public boolean readCheckNull() throws IOException {
    if (fieldIndex >= fieldCount) {
      // Reading beyond the specified field count produces NULL.
      if (!readBeyondConfiguredFieldsWarned) {
        // Warn only once.
        LOG.info("Reading beyond configured fields! Configured " + fieldCount + " fields but "
            + " reading more (NULLs returned).  Ignoring similar problems.");
        readBeyondConfiguredFieldsWarned = true;
      }
      return true;
    }

    if (fieldIndex == 0) {
      // The rest of the range check for fields after the first is below after checking
      // the NULL byte.
      if (offset >= end) {
        warnBeyondEof();
      }
      nullByte = bytes[offset++];
    }

    // NOTE: The bit is set to 1 if a field is NOT NULL.
    if ((nullByte & (1 << (fieldIndex % 8))) != 0) {

      // Make sure there is at least one byte that can be read for a value.
      if (offset >= end) {
        // Careful: since we may be dealing with NULLs in the final NULL byte, we check after
        // the NULL byte check..
        warnBeyondEof();
      }

      // We have a field and are positioned to it.

      if (primitiveTypeInfos[fieldIndex].getPrimitiveCategory() != PrimitiveCategory.DECIMAL) {
        return false;
      }

      // Since enforcing precision and scale may turn a HiveDecimal into a NULL, we must read
      // it here.
      return earlyReadHiveDecimal();
    }

    // When NULL, we need to move past this field.
    fieldIndex++;

    // Every 8 fields we read a new NULL byte.
    if (fieldIndex < fieldCount) {
      if ((fieldIndex % 8) == 0) {
        // Get next null byte.
        if (offset >= end) {
          warnBeyondEof();
        }
        nullByte = bytes[offset++];
      }
    }

    return true;
  }

  /*
   * Call this method after all fields have been read to check for extra fields.
   */
  public void extraFieldsCheck() {
    if (offset < end) {
      // We did not consume all of the byte range.
      if (!bufferRangeHasExtraDataWarned) {
        // Warn only once.
        int length = end - start;
        int remaining = end - offset;
        LOG.info("Not all fields were read in the buffer range! Buffer range " +  start 
            + " for length " + length + " but " + remaining + " bytes remain. "
            + "(total buffer length " + bytes.length + ")"
            + "  Ignoring similar problems.");
        bufferRangeHasExtraDataWarned = true;
      }
    }
  }

  /*
   * Read integrity warning flags.
   */
  @Override
  public boolean readBeyondConfiguredFieldsWarned() {
    return readBeyondConfiguredFieldsWarned;
  }
  @Override
  public boolean readBeyondBufferRangeWarned() {
    return readBeyondBufferRangeWarned;
  }
  @Override
  public boolean bufferRangeHasExtraDataWarned() {
    return bufferRangeHasExtraDataWarned;
  }

  private void warnBeyondEof() throws EOFException {
    if (!readBeyondBufferRangeWarned) {
      // Warn only once.
      int length = end - start;
      LOG.info("Reading beyond buffer range! Buffer range " +  start 
          + " for length " + length + " but reading more... "
          + "(total buffer length " + bytes.length + ")"
          + "  Ignoring similar problems.");
      readBeyondBufferRangeWarned = true;
    }
  }

  /*
   * BOOLEAN.
   */
  @Override
  public boolean readBoolean() throws IOException {
    // No check needed for single byte read.
    byte result = bytes[offset++];

    // Move past this NOT NULL field.
    fieldIndex++;

    // Every 8 fields we read a new NULL byte.
    if (fieldIndex < fieldCount) {
      if ((fieldIndex % 8) == 0) {
        // Get next null byte.
        if (offset >= end) {
          warnBeyondEof();
        }
        nullByte = bytes[offset++];
      }
    }

    return (result != 0);
  }

  /*
   * BYTE.
   */
  @Override
  public byte readByte() throws IOException {
    // No check needed for single byte read.
    byte result = bytes[offset++];

    // Move past this NOT NULL field.
    fieldIndex++;

    // Every 8 fields we read a new NULL byte.
    if (fieldIndex < fieldCount) {
      // Get next null byte.
      if (offset >= end) {
        warnBeyondEof();
      }
      if ((fieldIndex % 8) == 0) {
        nullByte = bytes[offset++];
      }
    }

    return result;
  }

  /*
   * SHORT.
   */
  @Override
  public short readShort() throws IOException {
    // Last item -- ok to be at end.
    if (offset + 2 > end) {
      warnBeyondEof();
    }
    short result = LazyBinaryUtils.byteArrayToShort(bytes, offset);
    offset += 2;

    // Move past this NOT NULL field.
    fieldIndex++;

    // Every 8 fields we read a new NULL byte.
    if (fieldIndex < fieldCount) {
      if ((fieldIndex % 8) == 0) {
        // Get next null byte.
        if (offset >= end) {
          warnBeyondEof();
        }
        nullByte = bytes[offset++];
      }
    }

    return result;
  }

  /*
   * INT.
   */
  @Override
  public int readInt() throws IOException {
    LazyBinaryUtils.readVInt(bytes, offset, tempVInt);
    offset += tempVInt.length;
    // Last item -- ok to be at end.
    if (offset > end) {
      warnBeyondEof();
    }

    // Move past this NOT NULL field.
    fieldIndex++;

    // Every 8 fields we read a new NULL byte.
    if (fieldIndex < fieldCount) {
      if ((fieldIndex % 8) == 0) {
        // Get next null byte.
        if (offset >= end) {
          warnBeyondEof();
        }
        nullByte = bytes[offset++];
      }
    }

    return tempVInt.value;
  }

  /*
   * LONG.
   */
  @Override
  public long readLong() throws IOException {
    LazyBinaryUtils.readVLong(bytes, offset, tempVLong);
    offset += tempVLong.length;
    // Last item -- ok to be at end.
    if (offset > end) {
      warnBeyondEof();
    }

    // Move past this NOT NULL field.
    fieldIndex++;

    // Every 8 fields we read a new NULL byte.
    if (fieldIndex < fieldCount) {
      if ((fieldIndex % 8) == 0) {
        // Get next null byte.
        if (offset >= end) {
          warnBeyondEof();
        }
        nullByte = bytes[offset++];
      }
    }

    return tempVLong.value;
  }

  /*
   * FLOAT.
   */
  @Override
  public float readFloat() throws IOException {
    // Last item -- ok to be at end.
    if (offset + 4 > end) {
      warnBeyondEof();
    }
    float result = Float.intBitsToFloat(LazyBinaryUtils.byteArrayToInt(bytes, offset));
    offset += 4;

    // Move past this NOT NULL field.
    fieldIndex++;

    // Every 8 fields we read a new NULL byte.
    if (fieldIndex < fieldCount) {
      if ((fieldIndex % 8) == 0) {
        // Get next null byte.
        if (offset >= end) {
          warnBeyondEof();
        }
        nullByte = bytes[offset++];
      }
    }

    return result;
  }

  /*
   * DOUBLE.
   */
  @Override
  public double readDouble() throws IOException {
    // Last item -- ok to be at end.
    if (offset + 8 > end) {
      warnBeyondEof();
    }
    double result = Double.longBitsToDouble(LazyBinaryUtils.byteArrayToLong(bytes, offset));
    offset += 8;

    // Move past this NOT NULL field.
    fieldIndex++;

    // Every 8 fields we read a new NULL byte.
    if (fieldIndex < fieldCount) {
      if ((fieldIndex % 8) == 0) {
        // Get next null byte.
        if (offset >= end) {
          warnBeyondEof();
        }
        nullByte = bytes[offset++];
      }
    }

    return result;
  }

  /*
   * STRING.
   *
   * Can be used to read CHAR and VARCHAR when the caller takes responsibility for
   * truncation/padding issues.
   */

  // This class is for internal use.
  private class LazyBinaryReadStringResults extends ReadStringResults {
    public LazyBinaryReadStringResults() {
      super();
    }
  }

  // Reading a STRING field require a results object to receive value information.  A separate
  // results object is created by the caller at initialization per different bytes field. 
  @Override
  public ReadStringResults createReadStringResults() {
    return new LazyBinaryReadStringResults();
  }

  @Override
  public void readString(ReadStringResults readStringResults) throws IOException {
    // using vint instead of 4 bytes
    LazyBinaryUtils.readVInt(bytes, offset, tempVInt);
    offset += tempVInt.length;
    // Could be last item for empty string -- ok to be at end.
    if (offset > end) {
      warnBeyondEof();
    }
    int saveStart = offset;
    int length = tempVInt.value;
    offset += length;
    // Last item -- ok to be at end.
    if (offset > end) {
      warnBeyondEof();
    }

    // Move past this NOT NULL field.
    fieldIndex++;

    // Every 8 fields we read a new NULL byte.
    if (fieldIndex < fieldCount) {
      if ((fieldIndex % 8) == 0) {
        // Get next null byte.
        if (offset >= end) {
          warnBeyondEof();
        }
        nullByte = bytes[offset++];
      }
    }

    readStringResults.bytes = bytes;
    readStringResults.start = saveStart;
    readStringResults.length = length;
  }

  /*
   * CHAR.
   */

  // This class is for internal use.
  private static class LazyBinaryReadHiveCharResults extends ReadHiveCharResults {

    // Use our STRING reader.
    public LazyBinaryReadStringResults readStringResults;

    public LazyBinaryReadHiveCharResults() {
      super();
    }

    public HiveCharWritable getHiveCharWritable() {
      return hiveCharWritable;
    }
  }

  // Reading a CHAR field require a results object to receive value information.  A separate
  // results object is created by the caller at initialization per different CHAR field. 
  @Override
  public ReadHiveCharResults createReadHiveCharResults() {
    return new LazyBinaryReadHiveCharResults();
  }

  public void readHiveChar(ReadHiveCharResults readHiveCharResults) throws IOException {
    LazyBinaryReadHiveCharResults lazyBinaryReadHiveCharResults = (LazyBinaryReadHiveCharResults) readHiveCharResults;

    if (!lazyBinaryReadHiveCharResults.isInit()) {
      lazyBinaryReadHiveCharResults.init((CharTypeInfo) primitiveTypeInfos[fieldIndex]);
    }

    if (lazyBinaryReadHiveCharResults.readStringResults == null) {
      lazyBinaryReadHiveCharResults.readStringResults = new LazyBinaryReadStringResults();
    }
    LazyBinaryReadStringResults readStringResults = lazyBinaryReadHiveCharResults.readStringResults;

    // Read the bytes using our basic method.
    readString(readStringResults);

    // Copy the bytes into our Text object, then truncate.
    HiveCharWritable hiveCharWritable = lazyBinaryReadHiveCharResults.getHiveCharWritable();
    hiveCharWritable.getTextValue().set(readStringResults.bytes, readStringResults.start, readStringResults.length);
    hiveCharWritable.enforceMaxLength(lazyBinaryReadHiveCharResults.getMaxLength());

    readHiveCharResults.bytes = hiveCharWritable.getTextValue().getBytes();
    readHiveCharResults.start = 0;
    readHiveCharResults.length = hiveCharWritable.getTextValue().getLength();
  }

  /*
   * VARCHAR.
   */

  // This class is for internal use.
  private static class LazyBinaryReadHiveVarcharResults extends ReadHiveVarcharResults {

    // Use our STRING reader.
    public LazyBinaryReadStringResults readStringResults;

    public LazyBinaryReadHiveVarcharResults() {
      super();
    }

    public HiveVarcharWritable getHiveVarcharWritable() {
      return hiveVarcharWritable;
    }
  }

  // Reading a VARCHAR field require a results object to receive value information.  A separate
  // results object is created by the caller at initialization per different VARCHAR field. 
  @Override
  public ReadHiveVarcharResults createReadHiveVarcharResults() {
    return new LazyBinaryReadHiveVarcharResults();
  }

  public void readHiveVarchar(ReadHiveVarcharResults readHiveVarcharResults) throws IOException {
    LazyBinaryReadHiveVarcharResults lazyBinaryReadHiveVarcharResults = (LazyBinaryReadHiveVarcharResults) readHiveVarcharResults;

    if (!lazyBinaryReadHiveVarcharResults.isInit()) {
      lazyBinaryReadHiveVarcharResults.init((VarcharTypeInfo) primitiveTypeInfos[fieldIndex]);
    }

    if (lazyBinaryReadHiveVarcharResults.readStringResults == null) {
      lazyBinaryReadHiveVarcharResults.readStringResults = new LazyBinaryReadStringResults();
    }
    LazyBinaryReadStringResults readStringResults = lazyBinaryReadHiveVarcharResults.readStringResults;

    // Read the bytes using our basic method.
    readString(readStringResults);

    // Copy the bytes into our Text object, then truncate.
    HiveVarcharWritable hiveVarcharWritable = lazyBinaryReadHiveVarcharResults.getHiveVarcharWritable();
    hiveVarcharWritable.getTextValue().set(readStringResults.bytes, readStringResults.start, readStringResults.length);
    hiveVarcharWritable.enforceMaxLength(lazyBinaryReadHiveVarcharResults.getMaxLength());

    readHiveVarcharResults.bytes = hiveVarcharWritable.getTextValue().getBytes();
    readHiveVarcharResults.start = 0;
    readHiveVarcharResults.length = hiveVarcharWritable.getTextValue().getLength();
  }

  /*
   * BINARY.
   */

  // This class is for internal use.
  private class LazyBinaryReadBinaryResults extends ReadBinaryResults {

    // Use our STRING reader.
    public LazyBinaryReadStringResults readStringResults;

    public LazyBinaryReadBinaryResults() {
      super();
    }
  }

  // Reading a BINARY field require a results object to receive value information.  A separate
  // results object is created by the caller at initialization per different bytes field. 
  @Override
  public ReadBinaryResults createReadBinaryResults() {
    return new LazyBinaryReadBinaryResults();
  }

  public void readBinary(ReadBinaryResults readBinaryResults) throws IOException {
    LazyBinaryReadBinaryResults lazyBinaryReadBinaryResults = (LazyBinaryReadBinaryResults) readBinaryResults;

    if (lazyBinaryReadBinaryResults.readStringResults == null) {
      lazyBinaryReadBinaryResults.readStringResults = new LazyBinaryReadStringResults();
    }
    LazyBinaryReadStringResults readStringResults = lazyBinaryReadBinaryResults.readStringResults;

    // Read the bytes using our basic method.
    readString(readStringResults);

    readBinaryResults.bytes = readStringResults.bytes;
    readBinaryResults.start = readStringResults.start;
    readBinaryResults.length = readStringResults.length;
  }

  /*
   * DATE.
   */

  // This class is for internal use.
  private static class LazyBinaryReadDateResults extends ReadDateResults {

    public LazyBinaryReadDateResults() {
      super();
    }

    public DateWritable getDateWritable() {
      return dateWritable;
    }
  }

  // Reading a DATE field require a results object to receive value information.  A separate
  // results object is created by the caller at initialization per different DATE field. 
  @Override
  public ReadDateResults createReadDateResults() {
    return new LazyBinaryReadDateResults();
  }

  @Override
  public void readDate(ReadDateResults readDateResults) throws IOException {
    LazyBinaryReadDateResults lazyBinaryReadDateResults = (LazyBinaryReadDateResults) readDateResults;
    LazyBinaryUtils.readVInt(bytes, offset, tempVInt);
    offset += tempVInt.length;
    // Last item -- ok to be at end.
    if (offset > end) {
      warnBeyondEof();
    }

    // Move past this NOT NULL field.
    fieldIndex++;

    // Every 8 fields we read a new NULL byte.
    if (fieldIndex < fieldCount) {
      if ((fieldIndex % 8) == 0) {
        // Get next null byte.
        if (offset >= end) {
          warnBeyondEof();
        }
        nullByte = bytes[offset++];
      }
    }

    DateWritable dateWritable = lazyBinaryReadDateResults.getDateWritable();
    dateWritable.set(tempVInt.value);
  }

  /*
   * INTERVAL_YEAR_MONTH.
   */

  // This class is for internal use.
  private static class LazyBinaryReadIntervalYearMonthResults extends ReadIntervalYearMonthResults {

    public LazyBinaryReadIntervalYearMonthResults() {
      super();
    }

    public HiveIntervalYearMonthWritable getHiveIntervalYearMonthWritable() {
      return hiveIntervalYearMonthWritable;
    }
  }

  // Reading a INTERVAL_YEAR_MONTH field require a results object to receive value information.
  // A separate results object is created by the caller at initialization per different
  // INTERVAL_YEAR_MONTH field. 
  @Override
  public ReadIntervalYearMonthResults createReadIntervalYearMonthResults() {
    return new LazyBinaryReadIntervalYearMonthResults();
  }

  @Override
  public void readIntervalYearMonth(ReadIntervalYearMonthResults readIntervalYearMonthResults)
          throws IOException {
    LazyBinaryReadIntervalYearMonthResults lazyBinaryReadIntervalYearMonthResults =
                (LazyBinaryReadIntervalYearMonthResults) readIntervalYearMonthResults;

    LazyBinaryUtils.readVInt(bytes, offset, tempVInt);
    offset += tempVInt.length;
    // Last item -- ok to be at end.
    if (offset > end) {
      warnBeyondEof();
    }

    // Move past this NOT NULL field.
    fieldIndex++;

    // Every 8 fields we read a new NULL byte.
    if (fieldIndex < fieldCount) {
      if ((fieldIndex % 8) == 0) {
        // Get next null byte.
        if (offset >= end) {
          warnBeyondEof();
        }
        nullByte = bytes[offset++];
      }
    }

    HiveIntervalYearMonthWritable hiveIntervalYearMonthWritable =
                lazyBinaryReadIntervalYearMonthResults.getHiveIntervalYearMonthWritable();
    hiveIntervalYearMonthWritable.set(tempVInt.value);
  }

  /*
   * INTERVAL_DAY_TIME.
   */

  // This class is for internal use.
  private static class LazyBinaryReadIntervalDayTimeResults extends ReadIntervalDayTimeResults {

    public LazyBinaryReadIntervalDayTimeResults() {
      super();
    }

    public HiveIntervalDayTimeWritable getHiveIntervalDayTimeWritable() {
      return hiveIntervalDayTimeWritable;
    }
  }

  // Reading a INTERVAL_DAY_TIME field require a results object to receive value information.
  // A separate results object is created by the caller at initialization per different
  // INTERVAL_DAY_TIME field. 
  @Override
  public ReadIntervalDayTimeResults createReadIntervalDayTimeResults() {
    return new LazyBinaryReadIntervalDayTimeResults();
  }

  @Override
  public void readIntervalDayTime(ReadIntervalDayTimeResults readIntervalDayTimeResults)
          throws IOException {
    LazyBinaryReadIntervalDayTimeResults lazyBinaryReadIntervalDayTimeResults =
                (LazyBinaryReadIntervalDayTimeResults) readIntervalDayTimeResults;
    LazyBinaryUtils.readVLong(bytes, offset, tempVLong);
    offset += tempVLong.length;
    if (offset >= end) {
      // Overshoot or not enough for next item.
      warnBeyondEof();
    }
    LazyBinaryUtils.readVInt(bytes, offset, tempVInt);
    offset += tempVInt.length;
    // Last item -- ok to be at end.
    if (offset > end) {
      warnBeyondEof();
    }

    // Move past this NOT NULL field.
    fieldIndex++;

    // Every 8 fields we read a new NULL byte.
    if (fieldIndex < fieldCount) {
      if ((fieldIndex % 8) == 0) {
        // Get next null byte.
        if (offset >= end) {
          warnBeyondEof();
        }
        nullByte = bytes[offset++];
      }
    }

    HiveIntervalDayTimeWritable hiveIntervalDayTimeWritable =
                lazyBinaryReadIntervalDayTimeResults.getHiveIntervalDayTimeWritable();
    hiveIntervalDayTimeWritable.set(tempVLong.value, tempVInt.value);
  }

  /*
   * TIMESTAMP.
   */

  // This class is for internal use.
  private static class LazyBinaryReadTimestampResults extends ReadTimestampResults {

    public LazyBinaryReadTimestampResults() {
      super();
    }

    public TimestampWritable getTimestampWritable() {
      return timestampWritable;
    }
  }

  // Reading a TIMESTAMP field require a results object to receive value information.  A separate
  // results object is created by the caller at initialization per different TIMESTAMP field. 
  @Override
  public ReadTimestampResults createReadTimestampResults() {
    return new LazyBinaryReadTimestampResults();
  }

  @Override
  public void readTimestamp(ReadTimestampResults readTimestampResults) throws IOException {
    LazyBinaryReadTimestampResults lazyBinaryReadTimestampResults = (LazyBinaryReadTimestampResults) readTimestampResults;
    int length = TimestampWritable.getTotalLength(bytes, offset);
    int saveStart = offset;
    offset += length;
    // Last item -- ok to be at end.
    if (offset > end) {
      warnBeyondEof();
    }

    // Move past this NOT NULL field.
    fieldIndex++;

    // Every 8 fields we read a new NULL byte.
    if (fieldIndex < fieldCount) {
      if ((fieldIndex % 8) == 0) {
        // Get next null byte.
        if (offset >= end) {
          warnBeyondEof();
        }
        nullByte = bytes[offset++];
      }
    }

    TimestampWritable timestampWritable = lazyBinaryReadTimestampResults.getTimestampWritable();
    timestampWritable.set(bytes, saveStart);
  }

  /*
   * DECIMAL.
   */

  // This class is for internal use.
  private static class LazyBinaryReadDecimalResults extends ReadDecimalResults {

    public HiveDecimal hiveDecimal;

    public void init(DecimalTypeInfo decimalTypeInfo) {
      super.init(decimalTypeInfo);
    }

    @Override
    public HiveDecimal getHiveDecimal() {
      return hiveDecimal;
    }
  }

  // Reading a DECIMAL field require a results object to receive value information.  A separate
  // results object is created by the caller at initialization per different DECIMAL field. 
  @Override
  public ReadDecimalResults createReadDecimalResults() {
    return new LazyBinaryReadDecimalResults();
  }

  @Override
  public void readHiveDecimal(ReadDecimalResults readDecimalResults) throws IOException {
    LazyBinaryReadDecimalResults lazyBinaryReadDecimalResults = (LazyBinaryReadDecimalResults) readDecimalResults;

    if (!lazyBinaryReadDecimalResults.isInit()) {
      lazyBinaryReadDecimalResults.init(saveDecimalTypeInfo);
    }

    lazyBinaryReadDecimalResults.hiveDecimal = saveDecimal;

    saveDecimal = null;
    saveDecimalTypeInfo = null;
  }

  /**
   * We read the whole HiveDecimal value and then enforce precision and scale, which may
   * make it a NULL.
   * @return     Returns true if this HiveDecimal enforced to a NULL.
   */
  private boolean earlyReadHiveDecimal() throws EOFException {

    // Since enforcing precision and scale can cause a HiveDecimal to become NULL,
    // we must read it, enforce it here, and either return NULL or buffer the result.

    // These calls are to see how much data there is. The setFromBytes call below will do the same
    // readVInt reads but actually unpack the decimal.
    LazyBinaryUtils.readVInt(bytes, offset, tempVInt);
    int saveStart = offset;
    offset += tempVInt.length;
    if (offset >= end) {
      // Overshoot or not enough for next item.
      warnBeyondEof();
    }
    LazyBinaryUtils.readVInt(bytes, offset, tempVInt);
    offset += tempVInt.length;
    if (offset >= end) {
      // Overshoot or not enough for next item.
      warnBeyondEof();
    }
    offset += tempVInt.value;
    // Last item -- ok to be at end.
    if (offset > end) {
      warnBeyondEof();
    }
    int length = offset - saveStart;

    if (tempHiveDecimalWritable == null) {
      tempHiveDecimalWritable = new HiveDecimalWritable();
    }
    tempHiveDecimalWritable.setFromBytes(bytes, saveStart, length);

    saveDecimalTypeInfo = (DecimalTypeInfo) primitiveTypeInfos[fieldIndex];

    int precision = saveDecimalTypeInfo.getPrecision();
    int scale = saveDecimalTypeInfo.getScale();

    saveDecimal = tempHiveDecimalWritable.getHiveDecimal(precision, scale);

    // Move past this field whether it is NULL or NOT NULL.
    fieldIndex++;

    // Every 8 fields we read a new NULL byte.
    if (fieldIndex < fieldCount) {
      if ((fieldIndex % 8) == 0) {
        // Get next null byte.
        if (offset >= end) {
          warnBeyondEof();
        }
        nullByte = bytes[offset++];
      }
    }

    // Now return whether it is NULL or NOT NULL.
    return (saveDecimal == null);
  }
}