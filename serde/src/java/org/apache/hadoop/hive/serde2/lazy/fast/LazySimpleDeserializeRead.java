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

import java.io.UnsupportedEncodingException;
import java.nio.charset.CharacterCodingException;
import java.sql.Date;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;
import org.apache.hadoop.hive.serde2.lazy.LazyBinary;
import org.apache.hadoop.hive.serde2.lazy.LazyByte;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazyShort;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
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
public final class LazySimpleDeserializeRead extends DeserializeRead {
  public static final Logger LOG = LoggerFactory.getLogger(LazySimpleDeserializeRead.class.getName());

  private int[] startPosition;

  private byte separator;
  private boolean isEscaped;
  private byte escapeChar;
  private byte[] nullSequenceBytes;
  private boolean isExtendedBooleanLiteral;
  private boolean lastColumnTakesRest;

  private byte[] bytes;
  private int start;
  private int offset;
  private int end;
  private int fieldCount;
  private int fieldIndex;
  private int parseFieldIndex;
  private int fieldStart;
  private int fieldLength;

  private Text tempText;
  private TimestampParser timestampParser;

  private boolean extraFieldWarned;
  private boolean missingFieldWarned;

  public LazySimpleDeserializeRead(TypeInfo[] typeInfos,
      byte separator, LazySerDeParameters lazyParams) {
    super(typeInfos);

    // Field length is difference between positions hence one extra.
    startPosition = new int[typeInfos.length + 1];

    this.separator = separator;

    isEscaped = lazyParams.isEscaped();
    escapeChar = lazyParams.getEscapeChar();
    nullSequenceBytes = lazyParams.getNullSequence().getBytes();
    isExtendedBooleanLiteral = lazyParams.isExtendedBooleanLiteral();
    lastColumnTakesRest = lazyParams.isLastColumnTakesRest();

    fieldCount = typeInfos.length;
    tempText = new Text();
    extraFieldWarned = false;
    missingFieldWarned = false;
  }

  public LazySimpleDeserializeRead(TypeInfo[] typeInfos, LazySerDeParameters lazyParams) {
    this(typeInfos, lazyParams.getSeparators()[0], lazyParams);
  }

  // Not public since we must have the field count so every 8 fields NULL bytes can be navigated.
  private LazySimpleDeserializeRead() {
    super();
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
   * Get detailed read position information to help diagnose exceptions.
   */
  public String getDetailedReadPositionString() {
    StringBuffer sb = new StringBuffer();

    sb.append("Reading byte[] of length ");
    sb.append(bytes.length);
    sb.append(" at start offset ");
    sb.append(start);
    sb.append(" for length ");
    sb.append(end - start);
    sb.append(" to read ");
    sb.append(fieldCount);
    sb.append(" fields with types ");
    sb.append(Arrays.toString(typeInfos));
    sb.append(".  ");
    if (fieldIndex == -1) {
      sb.append("Error during field delimitor parsing of field #");
      sb.append(parseFieldIndex);
    } else {
      sb.append("Read field #");
      sb.append(fieldIndex);
      sb.append(" at field start position ");
      sb.append(startPosition[fieldIndex]);
      int currentFieldLength = startPosition[fieldIndex + 1] - startPosition[fieldIndex] - 1;
      sb.append(" for field length ");
      sb.append(currentFieldLength);
      sb.append(" current read offset ");
      sb.append(offset);
    }

    return sb.toString();
  }

  /**
   * Parse the byte[] and fill each field.
   *
   * This is an adapted version of the parse method in the LazyStruct class.
   * They should parse things the same way.
   */
  private void parse() {

    int structByteEnd = end;
    int fieldByteBegin = start;
    int fieldByteEnd = start;

    // Kept as a member variable to support getDetailedReadPositionString.
    parseFieldIndex = 0;

    // Go through all bytes in the byte[]
    while (fieldByteEnd <= structByteEnd) {
      if (fieldByteEnd == structByteEnd || bytes[fieldByteEnd] == separator) {
        // Reached the end of a field?
        if (lastColumnTakesRest && parseFieldIndex == fieldCount - 1) {
          fieldByteEnd = structByteEnd;
        }
        startPosition[parseFieldIndex] = fieldByteBegin;
        parseFieldIndex++;
        if (parseFieldIndex == fieldCount || fieldByteEnd == structByteEnd) {
          // All fields have been parsed, or bytes have been parsed.
          // We need to set the startPosition of fields.length to ensure we
          // can use the same formula to calculate the length of each field.
          // For missing fields, their starting positions will all be the same,
          // which will make their lengths to be -1 and uncheckedGetField will
          // return these fields as NULLs.
          for (int i = parseFieldIndex; i <= fieldCount; i++) {
            startPosition[i] = fieldByteEnd + 1;
          }
          break;
        }
        fieldByteBegin = fieldByteEnd + 1;
        fieldByteEnd++;
      } else {
        if (isEscaped && bytes[fieldByteEnd] == escapeChar
            && fieldByteEnd + 1 < structByteEnd) {
          // ignore the char after escape_char
          fieldByteEnd += 2;
        } else {
          fieldByteEnd++;
        }
      }
    }

    // Extra bytes at the end?
    if (!extraFieldWarned && fieldByteEnd < structByteEnd) {
      doExtraFieldWarned();
    }

    // Missing fields?
    if (!missingFieldWarned && parseFieldIndex < fieldCount) {
      doMissingFieldWarned(parseFieldIndex);
    }
  }

  /*
   * Reads the NULL information for a field.
   *
   * @return Returns true when the field is NULL; reading is positioned to the next field.
   *         Otherwise, false when the field is NOT NULL; reading is positioned to the field data.
   */
  @Override
  public boolean readCheckNull() {
    if (fieldIndex == -1) {
      parse();
      fieldIndex = 0;
    } else if (fieldIndex + 1 >= fieldCount) {
      return true;
    } else {
      fieldIndex++;
    }

    // Do we want this field?
    if (columnsToInclude != null && !columnsToInclude[fieldIndex]) {
      return true;
    }

    fieldStart = startPosition[fieldIndex];
    fieldLength = startPosition[fieldIndex + 1] - startPosition[fieldIndex] - 1;
    if (fieldLength < 0) {
      return true;
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

    /*
     * We have a field and are positioned to it.  Read it.
     */
    switch (primitiveCategories[fieldIndex]) {
    case BOOLEAN:
      {
        int i = fieldStart;
        if (fieldLength == 4) {
          if ((bytes[i] == 'T' || bytes[i] == 't') &&
              (bytes[i + 1] == 'R' || bytes[i + 1] == 'r') &&
              (bytes[i + 2] == 'U' || bytes[i + 1] == 'u') &&
              (bytes[i + 3] == 'E' || bytes[i + 3] == 'e')) {
            currentBoolean = true;
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
            currentBoolean = false;
          } else {
            // No boolean value match for 4 char field.
            return true;
          }
        } else if (isExtendedBooleanLiteral && fieldLength == 1) {
          byte b = bytes[fieldStart];
          if (b == '1' || b == 't' || b == 'T') {
            currentBoolean = true;
          } else if (b == '0' || b == 'f' || b == 'F') {
            currentBoolean = false;
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
      if (!LazyUtils.isNumberMaybe(bytes, fieldStart, fieldLength)) {
        return true;
      }
      try {
        currentByte = LazyByte.parseByte(bytes, fieldStart, fieldLength, 10);
      } catch (NumberFormatException e) {
        logExceptionMessage(bytes, fieldStart, fieldLength, "TINYINT");
        return true;
      }
      break;
    case SHORT:
      if (!LazyUtils.isNumberMaybe(bytes, fieldStart, fieldLength)) {
        return true;
      }
      try {
        currentShort = LazyShort.parseShort(bytes, fieldStart, fieldLength, 10);
      } catch (NumberFormatException e) {
        logExceptionMessage(bytes, fieldStart, fieldLength, "SMALLINT");
        return true;
      }
      break;
    case INT:
      if (!LazyUtils.isNumberMaybe(bytes, fieldStart, fieldLength)) {
        return true;
      }
      try {
        currentInt = LazyInteger.parseInt(bytes, fieldStart, fieldLength, 10);
      } catch (NumberFormatException e) {
        logExceptionMessage(bytes, fieldStart, fieldLength, "INT");
        return true;
      }
      break;
    case LONG:
      if (!LazyUtils.isNumberMaybe(bytes, fieldStart, fieldLength)) {
        return true;
      }
      try {
        currentLong = LazyLong.parseLong(bytes, fieldStart, fieldLength, 10);
      } catch (NumberFormatException e) {
        logExceptionMessage(bytes, fieldStart, fieldLength, "BIGINT");
        return true;
      }
      break;
    case FLOAT:
      {
        if (!LazyUtils.isNumberMaybe(bytes, fieldStart, fieldLength)) {
          return true;
        }
        String byteData = null;
        try {
          byteData = Text.decode(bytes, fieldStart, fieldLength);
          currentFloat = Float.parseFloat(byteData);
        } catch (NumberFormatException e) {
          LOG.debug("Data not in the Float data type range so converted to null. Given data is :"
              + byteData, e);
          return true;
        } catch (CharacterCodingException e) {
          LOG.debug("Data not in the Float data type range so converted to null.", e);
          return true;
        }
      }
      break;
    case DOUBLE:
      {
        if (!LazyUtils.isNumberMaybe(bytes, fieldStart, fieldLength)) {
          return true;
        }
        String byteData = null;
        try {
          byteData = Text.decode(bytes, fieldStart, fieldLength);
          currentDouble = Double.parseDouble(byteData);
        } catch (NumberFormatException e) {
          LOG.debug("Data not in the Double data type range so converted to null. Given data is :"
              + byteData, e);
          return true;
        } catch (CharacterCodingException e) {
          LOG.debug("Data not in the Double data type range so converted to null.", e);
          return true;
        }
      }
      break;

    case STRING:
    case CHAR:
    case VARCHAR:
      if (isEscaped) {
        LazyUtils.copyAndEscapeStringDataToText(bytes, fieldStart, fieldLength, escapeChar, tempText);
        currentBytes = tempText.getBytes();
        currentBytesStart = 0;
        currentBytesLength = tempText.getLength();
      } else {
        // if the data is not escaped, simply copy the data.
        currentBytes = bytes;
        currentBytesStart = fieldStart;
        currentBytesLength = fieldLength;
      }
      break;
    case BINARY:
      {
        byte[] recv = new byte[fieldLength];
        System.arraycopy(bytes, fieldStart, recv, 0, fieldLength);
        byte[] decoded = LazyBinary.decodeIfNeeded(recv);
        // use the original bytes in case decoding should fail
        decoded = decoded.length > 0 ? decoded : recv;
        currentBytes = decoded;
        currentBytesStart = 0;
        currentBytesLength = decoded.length;
      }
      break;
    case DATE:
      {
        if (fieldLength == 0) {
          return true;
        }
        String s = null;
        try {
          s = Text.decode(bytes, fieldStart, fieldLength);
          currentDateWritable.set(Date.valueOf(s));
        } catch (Exception e) {
          logExceptionMessage(bytes, fieldStart, fieldLength, "DATE");
          return true;
        }
      }
      break;
    case TIMESTAMP:
      {
        if (fieldLength == 0) {
          return true;
        }
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
            currentTimestampWritable.set(timestampParser.parseTimestamp(s));
          } catch (IllegalArgumentException e) {
            logExceptionMessage(bytes, fieldStart, fieldLength, "TIMESTAMP");
            return true;
          }
        }
      }
      break;
    case INTERVAL_YEAR_MONTH:
      {
        String s = null;
        try {
          s = Text.decode(bytes, fieldStart, fieldLength);
          currentHiveIntervalYearMonthWritable.set(HiveIntervalYearMonth.valueOf(s));
        } catch (Exception e) {
          logExceptionMessage(bytes, fieldStart, fieldLength, "INTERVAL_YEAR_MONTH");
          return true;
        }
      }
      break;
    case INTERVAL_DAY_TIME:
      {
        String s = null;
        try {
          s = Text.decode(bytes, fieldStart, fieldLength);
          currentHiveIntervalDayTimeWritable.set(HiveIntervalDayTime.valueOf(s));
        } catch (Exception e) {
          logExceptionMessage(bytes, fieldStart, fieldLength, "INTERVAL_DAY_TIME");
          return true;
        }
      }
      break;
    case DECIMAL:
      {
        if (!LazyUtils.isNumberMaybe(bytes, fieldStart, fieldLength)) {
          return true;
        }
        String byteData = null;
        try {
          byteData = Text.decode(bytes, fieldStart, fieldLength);
        } catch (CharacterCodingException e) {
          LOG.debug("Data not in the HiveDecimal data type range so converted to null.", e);
          return true;
        }

        HiveDecimal decimal = HiveDecimal.create(byteData);
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfos[fieldIndex];
        int precision = decimalTypeInfo.getPrecision();
        int scale = decimalTypeInfo.getScale();
        decimal = HiveDecimal.enforcePrecisionScale(
            decimal, precision, scale);
        if (decimal == null) {
          LOG.debug("Data not in the HiveDecimal data type range so converted to null. Given data is :"
              + byteData);
          return true;
        }
        currentHiveDecimalWritable.set(decimal);
      }
      break;

    default:
      throw new Error("Unexpected primitive category " + primitiveCategories[fieldIndex].name());
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
    // UNDONE: Get rid of...
  }

  /*
   * Read integrity warning flags.
   */
  @Override
  public boolean readBeyondConfiguredFieldsWarned() {
    return missingFieldWarned;
  }
  @Override
  public boolean bufferRangeHasExtraDataWarned() {
    return false;
  }

  private void doExtraFieldWarned() {
    extraFieldWarned = true;
    LOG.warn("Extra bytes detected at the end of the row! Ignoring similar "
        + "problems.");
  }

  private void doMissingFieldWarned(int fieldId) {
    missingFieldWarned = true;
    LOG.info("Missing fields! Expected " + fieldCount + " fields but "
        + "only got " + fieldId + "! Ignoring similar problems.");
  }

  //------------------------------------------------------------------------------------------------

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
