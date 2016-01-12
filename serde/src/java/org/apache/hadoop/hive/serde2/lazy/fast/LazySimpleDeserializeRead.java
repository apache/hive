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

package org.apache.hadoop.hive.serde2.lazy.fast;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.CharacterCodingException;
import java.sql.Date;
import java.sql.Timestamp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyBinary;
import org.apache.hadoop.hive.serde2.lazy.LazyByte;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazyShort;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hive.common.util.TimestampParser;

/*
 * Directly deserialize with the caller reading field-by-field the LazySimple (text)
 * serialization format.
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
public final class LazySimpleDeserializeRead implements DeserializeRead {
  public static final Log LOG = LogFactory.getLog(LazySimpleDeserializeRead.class.getName());

  private TypeInfo[] typeInfos;


  private byte separator;
  private boolean isEscaped;
  private byte escapeChar;
  private byte[] nullSequenceBytes;
  private boolean isExtendedBooleanLiteral;

  private byte[] bytes;
  private int start;
  private int offset;
  private int end;
  private int fieldCount;
  private int fieldIndex;
  private int fieldStart;
  private int fieldLength;

  private boolean saveBool;
  private byte saveByte;
  private short saveShort;
  private int saveInt;
  private long saveLong;
  private float saveFloat;
  private double saveDouble;
  private byte[] saveBytes;
  private int saveBytesStart;
  private int saveBytesLength;
  private Date saveDate;
  private Timestamp saveTimestamp;
  private HiveIntervalYearMonth saveIntervalYearMonth;
  private HiveIntervalDayTime saveIntervalDayTime;
  private HiveDecimal saveDecimal;
  private DecimalTypeInfo saveDecimalTypeInfo;

  private Text tempText;
  private TimestampParser timestampParser;

  private boolean readBeyondConfiguredFieldsWarned;
  private boolean readBeyondBufferRangeWarned;
  private boolean bufferRangeHasExtraDataWarned;

  public LazySimpleDeserializeRead(TypeInfo[] typeInfos,
      byte separator, LazySerDeParameters lazyParams) {

    this.typeInfos = typeInfos;

    this.separator = separator;

    isEscaped = lazyParams.isEscaped();
    escapeChar = lazyParams.getEscapeChar();
    nullSequenceBytes = lazyParams.getNullSequence().getBytes();
    isExtendedBooleanLiteral = lazyParams.isExtendedBooleanLiteral();

    fieldCount = typeInfos.length;
    tempText = new Text();
    readBeyondConfiguredFieldsWarned = false;
    readBeyondBufferRangeWarned = false;
    bufferRangeHasExtraDataWarned = false;
  }

  // Not public since we must have the field count so every 8 fields NULL bytes can be navigated.
  private LazySimpleDeserializeRead() {
  }

  /*
   * The type information for all fields.
   */
  @Override
  public TypeInfo[] typeInfos() {
    return typeInfos;
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
    fieldIndex = -1;
  }

  /*
   * Reads the NULL information for a field.
   *
   * @return Returns true when the field is NULL; reading is positioned to the next field.
   *         Otherwise, false when the field is NOT NULL; reading is positioned to the field data.
   */
  @Override
  public boolean readCheckNull() {
    if (++fieldIndex >= fieldCount) {
      // Reading beyond the specified field count produces NULL.
      if (!readBeyondConfiguredFieldsWarned) {
        // Warn only once.
        LOG.info("Reading beyond configured fields! Configured " + fieldCount + " fields but "
            + " reading more (NULLs returned).  Ignoring similar problems.");
        readBeyondConfiguredFieldsWarned = true;
      }
      return true;
    }
    if (offset > end) {
      // We must allow for an empty field at the end, so no strict >= checking.
      if (!readBeyondBufferRangeWarned) {
        // Warn only once.
        int length = end - start;
        LOG.info("Reading beyond buffer range! Buffer range " +  start
            + " for length " + length + " but reading more (NULLs returned)."
            + "  Ignoring similar problems.");
        readBeyondBufferRangeWarned = true;
      }

      // char[] charsBuffer = new char[end - start];
      // for (int c = 0; c < charsBuffer.length; c++) {
      //  charsBuffer[c] = (char) (bytes[start + c] & 0xFF);
      // }

      return true;
    }

    fieldStart = offset;
    while (true) {
      if (offset >= end) {
        fieldLength = offset - fieldStart;
        break;
      }
      if (bytes[offset] == separator) {
        fieldLength = (offset++ - fieldStart);
        break;
      }
      if (isEscaped && bytes[offset] == escapeChar
          && offset + 1 < end) {
        // Ignore the char after escape char.
        offset += 2;
      } else {
        offset++;
      }
    }

    char[] charField = new char[fieldLength];
    for (int c = 0; c < charField.length; c++) {
      charField[c] = (char) (bytes[fieldStart + c] & 0xFF);
    }

    // Is the field the configured string representing NULL?
    if (nullSequenceBytes != null) {
      if (fieldLength == nullSequenceBytes.length) {
        int i = 0;
        while (true) {
          if (bytes[fieldStart + i] != nullSequenceBytes[i]) {
            break;
          }
          i++;
          if (i >= fieldLength) {
            return true;
          }
        }
      }
    }

    switch (((PrimitiveTypeInfo) typeInfos[fieldIndex]).getPrimitiveCategory()) {
    case BOOLEAN:
      {
        int i = fieldStart;
        if (fieldLength == 4) {
          if ((bytes[i] == 'T' || bytes[i] == 't') &&
              (bytes[i + 1] == 'R' || bytes[i + 1] == 'r') &&
              (bytes[i + 2] == 'U' || bytes[i + 1] == 'u') &&
              (bytes[i + 3] == 'E' || bytes[i + 3] == 'e')) {
            saveBool = true;
          } else {
            // No boolean value match for 5 char field.
            return true;
          }
        } else if (fieldLength == 5) {
          if ((bytes[i] == 'F' || bytes[i] == 'f') &&
              (bytes[i + 1] == 'A' || bytes[i + 1] == 'a') &&
              (bytes[i + 2] == 'L' || bytes[i + 2] == 'l') &&
              (bytes[i + 3] == 'S' || bytes[i + 3] == 's') &&
              (bytes[i + 4] == 'E' || bytes[i + 4] == 'e')) {
            saveBool = false;
          } else {
            // No boolean value match for 4 char field.
            return true;
          }
        } else if (isExtendedBooleanLiteral && fieldLength == 1) {
          byte b = bytes[fieldStart];
          if (b == '1' || b == 't' || b == 'T') {
            saveBool = true;
          } else if (b == '0' || b == 'f' || b == 'F') {
            saveBool = false;
          } else {
            // No boolean value match for extended 1 char field.
            return true;
          }
        } else {
          // No boolean value match for other lengths.
          return true;
        }
      }
      break;
    case BYTE:
      try {
        saveByte = LazyByte.parseByte(bytes, fieldStart, fieldLength, 10);
      } catch (NumberFormatException e) {
        logExceptionMessage(bytes, fieldStart, fieldLength, "TINYINT");
        return true;
      }
//    if (!parseLongFast()) {
//      return true;
//    }
//    saveShort = (short) saveLong;
//    if (saveShort != saveLong) {
//      return true;
//    }
      break;
    case SHORT:
      try {
        saveShort = LazyShort.parseShort(bytes, fieldStart, fieldLength, 10);
      } catch (NumberFormatException e) {
        logExceptionMessage(bytes, fieldStart, fieldLength, "SMALLINT");
        return true;
      }
//    if (!parseLongFast()) {
//      return true;
//    }
//    saveShort = (short) saveLong;
//    if (saveShort != saveLong) {
//      return true;
//    }
      break;
    case INT:
      try {
        saveInt = LazyInteger.parseInt(bytes, fieldStart, fieldLength, 10);
      } catch (NumberFormatException e) {
        logExceptionMessage(bytes, fieldStart, fieldLength, "INT");
        return true;
      }
//    if (!parseLongFast()) {
//      return true;
//    }
//    saveInt = (int) saveLong;
//    if (saveInt != saveLong) {
//      return true;
//    }
      break;
    case LONG:
      try {
        saveLong = LazyLong.parseLong(bytes, fieldStart, fieldLength, 10);
      } catch (NumberFormatException e) {
        logExceptionMessage(bytes, fieldStart, fieldLength, "BIGINT");
        return true;
      }
//    if (!parseLongFast()) {
//      return true;
//    }
      break;
    case FLOAT:
      {
        String byteData = null;
        try {
          byteData = Text.decode(bytes, fieldStart, fieldLength);
          saveFloat = Float.parseFloat(byteData);
        } catch (NumberFormatException e) {
          LOG.debug("Data not in the Float data type range so converted to null. Given data is :"
              + byteData, e);
          return true;
        } catch (CharacterCodingException e) {
          LOG.debug("Data not in the Float data type range so converted to null.", e);
          return true;
        }
      }
//    if (!parseFloat()) {
//      return true;
//    }
      break;
    case DOUBLE:
      {
        String byteData = null;
        try {
          byteData = Text.decode(bytes, fieldStart, fieldLength);
          saveDouble = Double.parseDouble(byteData);
        } catch (NumberFormatException e) {
          LOG.debug("Data not in the Double data type range so converted to null. Given data is :"
              + byteData, e);
          return true;
        } catch (CharacterCodingException e) {
          LOG.debug("Data not in the Double data type range so converted to null.", e);
          return true;
        }
      }
//    if (!parseDouble()) {
//      return true;
//    }
      break;

    case STRING:
    case CHAR:
    case VARCHAR:
      if (isEscaped) {
        LazyUtils.copyAndEscapeStringDataToText(bytes, fieldStart, fieldLength, escapeChar, tempText);
        saveBytes = tempText.getBytes();
        saveBytesStart = 0;
        saveBytesLength = tempText.getLength();
      } else {
        // if the data is not escaped, simply copy the data.
        saveBytes = bytes;
        saveBytesStart = fieldStart;
        saveBytesLength = fieldLength;
      }
      break;
    case BINARY:
      {
        byte[] recv = new byte[fieldLength];
        System.arraycopy(bytes, fieldStart, recv, 0, fieldLength);
        byte[] decoded = LazyBinary.decodeIfNeeded(recv);
        // use the original bytes in case decoding should fail
        decoded = decoded.length > 0 ? decoded : recv;
        saveBytes = decoded;
        saveBytesStart = 0;
        saveBytesLength = decoded.length;
      }
      break;
    case DATE:
      {
        String s = null;
        try {
          s = Text.decode(bytes, fieldStart, fieldLength);
          saveDate = Date.valueOf(s);
        } catch (Exception e) {
          logExceptionMessage(bytes, fieldStart, fieldLength, "DATE");
          return true;
        }
      }
//    if (!parseDate()) {
//      return true;
//    }
      break;
    case TIMESTAMP:
      {
        String s = null;
        try {
          s = new String(bytes, fieldStart, fieldLength, "US-ASCII");
        } catch (UnsupportedEncodingException e) {
          LOG.error("Unsupported encoding found ", e);
          s = "";
        }

        if (s.compareTo("NULL") == 0) {
          logExceptionMessage(bytes, fieldStart, fieldLength, "TIMESTAMP");
          return true;
        } else {
          try {
            if (timestampParser == null) {
              timestampParser = new TimestampParser();
            }
            saveTimestamp = timestampParser.parseTimestamp(s);
          } catch (IllegalArgumentException e) {
            logExceptionMessage(bytes, fieldStart, fieldLength, "TIMESTAMP");
            return true;
          }
        }
      }
//    if (!parseTimestamp()) {
//      return true;
//    }
      break;
    case INTERVAL_YEAR_MONTH:
      {
        String s = null;
        try {
          s = Text.decode(bytes, fieldStart, fieldLength);
          saveIntervalYearMonth = HiveIntervalYearMonth.valueOf(s);
        } catch (Exception e) {
          logExceptionMessage(bytes, fieldStart, fieldLength, "INTERVAL_YEAR_MONTH");
          return true;
        }
      }
//    if (!parseIntervalYearMonth()) {
//      return true;
//    }
      break;
    case INTERVAL_DAY_TIME:
      {
        String s = null;
        try {
          s = Text.decode(bytes, fieldStart, fieldLength);
          saveIntervalDayTime = HiveIntervalDayTime.valueOf(s);
        } catch (Exception e) {
          logExceptionMessage(bytes, fieldStart, fieldLength, "INTERVAL_DAY_TIME");
          return true;
        }
      }
//    if (!parseIntervalDayTime()) {
//      return true;
//    }
      break;
    case DECIMAL:
      {
        String byteData = null;
        try {
          byteData = Text.decode(bytes, fieldStart, fieldLength);
        } catch (CharacterCodingException e) {
          LOG.debug("Data not in the HiveDecimal data type range so converted to null.", e);
          return true;
        }

        saveDecimal = HiveDecimal.create(byteData);
        saveDecimalTypeInfo = (DecimalTypeInfo) typeInfos[fieldIndex];
        int precision = saveDecimalTypeInfo.getPrecision();
        int scale = saveDecimalTypeInfo.getScale();
        saveDecimal = HiveDecimalUtils.enforcePrecisionScale(saveDecimal, precision, scale);
        if (saveDecimal == null) {
          LOG.debug("Data not in the HiveDecimal data type range so converted to null. Given data is :"
              + byteData);
          return true;
        }
      }
//    if (!parseDecimal()) {
//      return true;
//    }
      break;

    default:
      throw new Error("Unexpected primitive category " + ((PrimitiveTypeInfo) typeInfos[fieldIndex]).getPrimitiveCategory());
    }

    return false;
  }

  public void logExceptionMessage(byte[] bytes, int bytesStart, int bytesLength, String dataType) {
    try {
      if(LOG.isDebugEnabled()) {
        String byteData = Text.decode(bytes, bytesStart, bytesLength);
        LOG.debug("Data not in the " + dataType
            + " data type range so converted to null. Given data is :" +
                    byteData, new Exception("For debugging purposes"));
      }
    } catch (CharacterCodingException e1) {
      LOG.debug("Data not in the " + dataType + " data type range so converted to null.", e1);
    }
  }

  /*
   * Call this method after all fields have been read to check for extra fields.
   */
  @Override
  public void extraFieldsCheck() {
    if (offset < end) {
      // We did not consume all of the byte range.
      if (!bufferRangeHasExtraDataWarned) {
        // Warn only once.
        int length = end - start;
        LOG.info("Not all fields were read in the buffer range! Buffer range " +  start
            + " for length " + length + " but reading more (NULLs returned)."
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

  /*
   * BOOLEAN.
   */
  @Override
  public boolean readBoolean() {
    return saveBool;
  }

  /*
   * BYTE.
   */
  @Override
  public byte readByte() {
    return saveByte;
  }

  /*
   * SHORT.
   */
  @Override
  public short readShort() {
    return saveShort;
  }

  /*
   * INT.
   */
  @Override
  public int readInt() {
    return saveInt;
 }

  /*
   * LONG.
   */
  @Override
  public long readLong() {
    return saveLong;
  }

  /*
   * FLOAT.
   */
  @Override
  public float readFloat() {
    return saveFloat;
  }

  /*
   * DOUBLE.
   */
  @Override
  public double readDouble() {
    return saveDouble;
  }

  /*
   * STRING.
   *
   * Can be used to read CHAR and VARCHAR when the caller takes responsibility for
   * truncation/padding issues.
   */

  // This class is for internal use.
  private class LazySimpleReadStringResults extends ReadStringResults {
    public LazySimpleReadStringResults() {
      super();
    }
  }

  // Reading a STRING field require a results object to receive value information.  A separate
  // results object is created by the caller at initialization per different bytes field.
  @Override
  public ReadStringResults createReadStringResults() {
    return new LazySimpleReadStringResults();
  }

  @Override
  public void readString(ReadStringResults readStringResults) {
    readStringResults.bytes = saveBytes;
    readStringResults.start = saveBytesStart;
    readStringResults.length = saveBytesLength;
  }

  /*
   * CHAR.
   */

  // This class is for internal use.
  private static class LazySimpleReadHiveCharResults extends ReadHiveCharResults {

    // Use our STRING reader.
    public LazySimpleReadStringResults readStringResults;

    public LazySimpleReadHiveCharResults() {
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
    return new LazySimpleReadHiveCharResults();
  }

  @Override
  public void readHiveChar(ReadHiveCharResults readHiveCharResults) throws IOException {
    LazySimpleReadHiveCharResults LazySimpleReadHiveCharResults = (LazySimpleReadHiveCharResults) readHiveCharResults;

    if (!LazySimpleReadHiveCharResults.isInit()) {
      LazySimpleReadHiveCharResults.init((CharTypeInfo) typeInfos[fieldIndex]);
    }

    if (LazySimpleReadHiveCharResults.readStringResults == null) {
      LazySimpleReadHiveCharResults.readStringResults = new LazySimpleReadStringResults();
    }
    LazySimpleReadStringResults readStringResults = LazySimpleReadHiveCharResults.readStringResults;

    // Read the bytes using our basic method.
    readString(readStringResults);

    // Copy the bytes into our Text object, then truncate.
    HiveCharWritable hiveCharWritable = LazySimpleReadHiveCharResults.getHiveCharWritable();
    hiveCharWritable.getTextValue().set(readStringResults.bytes, readStringResults.start, readStringResults.length);
    hiveCharWritable.enforceMaxLength(LazySimpleReadHiveCharResults.getMaxLength());

    readHiveCharResults.bytes = hiveCharWritable.getTextValue().getBytes();
    readHiveCharResults.start = 0;
    readHiveCharResults.length = hiveCharWritable.getTextValue().getLength();
  }

  /*
   * VARCHAR.
   */

  // This class is for internal use.
  private static class LazySimpleReadHiveVarcharResults extends ReadHiveVarcharResults {

    // Use our bytes reader.
    public LazySimpleReadStringResults readStringResults;

    public LazySimpleReadHiveVarcharResults() {
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
    return new LazySimpleReadHiveVarcharResults();
  }

  @Override
  public void readHiveVarchar(ReadHiveVarcharResults readHiveVarcharResults) throws IOException {
    LazySimpleReadHiveVarcharResults lazySimpleReadHiveVarvarcharResults = (LazySimpleReadHiveVarcharResults) readHiveVarcharResults;

    if (!lazySimpleReadHiveVarvarcharResults.isInit()) {
      lazySimpleReadHiveVarvarcharResults.init((VarcharTypeInfo) typeInfos[fieldIndex]);
    }

    if (lazySimpleReadHiveVarvarcharResults.readStringResults == null) {
      lazySimpleReadHiveVarvarcharResults.readStringResults = new LazySimpleReadStringResults();
    }
    LazySimpleReadStringResults readStringResults = lazySimpleReadHiveVarvarcharResults.readStringResults;

    // Read the bytes using our basic method.
    readString(readStringResults);

    // Copy the bytes into our Text object, then truncate.
    HiveVarcharWritable hiveVarcharWritable = lazySimpleReadHiveVarvarcharResults.getHiveVarcharWritable();
    hiveVarcharWritable.getTextValue().set(readStringResults.bytes, readStringResults.start, readStringResults.length);
    hiveVarcharWritable.enforceMaxLength(lazySimpleReadHiveVarvarcharResults.getMaxLength());

    readHiveVarcharResults.bytes = hiveVarcharWritable.getTextValue().getBytes();
    readHiveVarcharResults.start = 0;
    readHiveVarcharResults.length = hiveVarcharWritable.getTextValue().getLength();
  }

  /*
   * BINARY.
   */

  // This class is for internal use.
  private class LazySimpleReadBinaryResults extends ReadBinaryResults {
    public LazySimpleReadBinaryResults() {
      super();
    }
  }

  // Reading a BINARY field require a results object to receive value information.  A separate
  // results object is created by the caller at initialization per different bytes field.
  @Override
  public ReadBinaryResults createReadBinaryResults() {
    return new LazySimpleReadBinaryResults();
  }

  @Override
  public void readBinary(ReadBinaryResults readBinaryResults) {
    readBinaryResults.bytes = saveBytes;
    readBinaryResults.start = saveBytesStart;
    readBinaryResults.length = saveBytesLength;
  }

  /*
   * DATE.
   */

  // This class is for internal use.
  private static class LazySimpleReadDateResults extends ReadDateResults {

    public LazySimpleReadDateResults() {
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
    return new LazySimpleReadDateResults();
  }

  @Override
  public void readDate(ReadDateResults readDateResults) {
    LazySimpleReadDateResults lazySimpleReadDateResults = (LazySimpleReadDateResults) readDateResults;

    DateWritable dateWritable = lazySimpleReadDateResults.getDateWritable();
    dateWritable.set(saveDate);
    saveDate = null;
   }


  /*
   * INTERVAL_YEAR_MONTH.
   */

  // This class is for internal use.
  private static class LazySimpleReadIntervalYearMonthResults extends ReadIntervalYearMonthResults {

    public LazySimpleReadIntervalYearMonthResults() {
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
    return new LazySimpleReadIntervalYearMonthResults();
  }

  @Override
  public void readIntervalYearMonth(ReadIntervalYearMonthResults readIntervalYearMonthResults)
          throws IOException {
    LazySimpleReadIntervalYearMonthResults lazySimpleReadIntervalYearMonthResults =
            (LazySimpleReadIntervalYearMonthResults) readIntervalYearMonthResults;

    HiveIntervalYearMonthWritable hiveIntervalYearMonthWritable =
            lazySimpleReadIntervalYearMonthResults.getHiveIntervalYearMonthWritable();
    hiveIntervalYearMonthWritable.set(saveIntervalYearMonth);
    saveIntervalYearMonth = null;
  }

  /*
   * INTERVAL_DAY_TIME.
   */

  // This class is for internal use.
  private static class LazySimpleReadIntervalDayTimeResults extends ReadIntervalDayTimeResults {

    public LazySimpleReadIntervalDayTimeResults() {
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
    return new LazySimpleReadIntervalDayTimeResults();
  }

  @Override
  public void readIntervalDayTime(ReadIntervalDayTimeResults readIntervalDayTimeResults)
          throws IOException {
    LazySimpleReadIntervalDayTimeResults lazySimpleReadIntervalDayTimeResults =
        (LazySimpleReadIntervalDayTimeResults) readIntervalDayTimeResults;

    HiveIntervalDayTimeWritable hiveIntervalDayTimeWritable =
            lazySimpleReadIntervalDayTimeResults.getHiveIntervalDayTimeWritable();
    hiveIntervalDayTimeWritable.set(saveIntervalDayTime);
    saveIntervalDayTime = null;
  }

  /*
   * TIMESTAMP.
   */

  // This class is for internal use.
  private static class LazySimpleReadTimestampResults extends ReadTimestampResults {

    public LazySimpleReadTimestampResults() {
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
    return new LazySimpleReadTimestampResults();
  }

  @Override
  public void readTimestamp(ReadTimestampResults readTimestampResults) {
    LazySimpleReadTimestampResults lazySimpleReadTimestampResults =
            (LazySimpleReadTimestampResults) readTimestampResults;

    TimestampWritable timestampWritable = lazySimpleReadTimestampResults.getTimestampWritable();
    timestampWritable.set(saveTimestamp);
    saveTimestamp = null;
  }

  /*
   * DECIMAL.
   */

  // This class is for internal use.
  private static class LazySimpleReadDecimalResults extends ReadDecimalResults {

    HiveDecimal hiveDecimal;

    public LazySimpleReadDecimalResults() {
      super();
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
    return new LazySimpleReadDecimalResults();
  }

  @Override
  public void readHiveDecimal(ReadDecimalResults readDecimalResults) {
    LazySimpleReadDecimalResults lazySimpleReadDecimalResults = (LazySimpleReadDecimalResults) readDecimalResults;

    if (!lazySimpleReadDecimalResults.isInit()) {
      lazySimpleReadDecimalResults.init(saveDecimalTypeInfo);
    }

    lazySimpleReadDecimalResults.hiveDecimal = saveDecimal;

    saveDecimal = null;
    saveDecimalTypeInfo = null;
  }

  private static byte[] maxLongBytes = ((Long) Long.MAX_VALUE).toString().getBytes();
  private static int maxLongDigitsCount = maxLongBytes.length;
  private static byte[] minLongNoSignBytes = ((Long) Long.MIN_VALUE).toString().substring(1).getBytes();

  public static int byteArrayCompareRanges(byte[] arg1, int start1, byte[] arg2, int start2, int len) {
    for (int i = 0; i < len; i++) {
      // Note the "& 0xff" is just a way to convert unsigned bytes to signed integer.
      int b1 = arg1[i + start1] & 0xff;
      int b2 = arg2[i + start2] & 0xff;
      if (b1 != b2) {
        return b1 - b2;
      }
    }
    return 0;
  }

}
