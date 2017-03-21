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
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hive.common.util.TimestampParser;

/*
 * Directly deserialize with the caller reading field-by-field the LazySimple (text)
 * serialization format.
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
public final class LazySimpleDeserializeRead extends DeserializeRead {
  public static final Logger LOG = LoggerFactory.getLogger(LazySimpleDeserializeRead.class.getName());

  private int[] startPosition;

  private final byte separator;
  private final boolean isEscaped;
  private final byte escapeChar;
  private final int[] escapeCounts;
  private final byte[] nullSequenceBytes;
  private final boolean isExtendedBooleanLiteral;

  private final int fieldCount;

  private byte[] bytes;
  private int start;
  private int end;
  private boolean parsed;

  // Used by readNextField/skipNextField and not by readField.
  private int nextFieldIndex;

  // For getDetailedReadPositionString.
  private int currentFieldIndex;
  private int currentFieldStart;
  private int currentFieldLength;

  // For string/char/varchar buffering when there are escapes.
  private int internalBufferLen;
  private byte[] internalBuffer;

  private final TimestampParser timestampParser;

  private boolean isEndOfInputReached;

  public LazySimpleDeserializeRead(TypeInfo[] typeInfos, boolean useExternalBuffer,
      byte separator, LazySerDeParameters lazyParams) {
    super(typeInfos, useExternalBuffer);

    fieldCount = typeInfos.length;

    // Field length is difference between positions hence one extra.
    startPosition = new int[fieldCount + 1];

    this.separator = separator;

    isEscaped = lazyParams.isEscaped();
    if (isEscaped) {
      escapeChar = lazyParams.getEscapeChar();
      escapeCounts = new int[fieldCount];
    } else {
      escapeChar = (byte) 0;
      escapeCounts = null;
    }
    nullSequenceBytes = lazyParams.getNullSequence().getBytes();
    isExtendedBooleanLiteral = lazyParams.isExtendedBooleanLiteral();
    if (lazyParams.isLastColumnTakesRest()) {
      throw new RuntimeException("serialization.last.column.takes.rest not supported");
    }

    timestampParser = new TimestampParser();

    internalBufferLen = -1;
  }

  public LazySimpleDeserializeRead(TypeInfo[] typeInfos, boolean useExternalBuffer,
      LazySerDeParameters lazyParams) {
    this(typeInfos, useExternalBuffer, lazyParams.getSeparators()[0], lazyParams);
  }

  /*
   * Set the range of bytes to be deserialized.
   */
  @Override
  public void set(byte[] bytes, int offset, int length) {
    this.bytes = bytes;
    start = offset;
    end = offset + length;
    parsed = false;
    nextFieldIndex = -1;
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
    if (!parsed) {
      sb.append("Error during field separator parsing");
    } else {
      sb.append("Read field #");
      sb.append(currentFieldIndex);
      sb.append(" at field start position ");
      sb.append(startPosition[currentFieldIndex]);
      int currentFieldLength = startPosition[currentFieldIndex + 1] - startPosition[currentFieldIndex] - 1;
      sb.append(" for field length ");
      sb.append(currentFieldLength);
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

    int fieldId = 0;
    int fieldByteBegin = start;
    int fieldByteEnd = start;

    final byte separator = this.separator;
    final int fieldCount = this.fieldCount;
    final int[] startPosition = this.startPosition;
    final byte[] bytes = this.bytes;
    final int end = this.end;

    /*
     * Optimize the loops by pulling special end cases and global decisions like isEscaped out!
     */
    if (!isEscaped) {
      while (fieldByteEnd < end) {
        if (bytes[fieldByteEnd] == separator) {
          startPosition[fieldId++] = fieldByteBegin;
          if (fieldId == fieldCount) {
            break;
          }
          fieldByteBegin = ++fieldByteEnd;
        } else {
          fieldByteEnd++;
        }
      }
      // End serves as final separator.
      if (fieldByteEnd == end && fieldId < fieldCount) {
        startPosition[fieldId++] = fieldByteBegin;
      }
    } else {
      final byte escapeChar = this.escapeChar;
      final int endLessOne = end - 1;
      final int[] escapeCounts = this.escapeCounts;
      int escapeCount = 0;
      // Process the bytes that can be escaped (the last one can't be).
      while (fieldByteEnd < endLessOne) {
        if (bytes[fieldByteEnd] == separator) {
          escapeCounts[fieldId] = escapeCount;
          escapeCount = 0;
          startPosition[fieldId++] = fieldByteBegin;
          if (fieldId == fieldCount) {
            break;
          }
          fieldByteBegin = ++fieldByteEnd;
        } else if (bytes[fieldByteEnd] == escapeChar) {
          // Ignore the char after escape_char
          fieldByteEnd += 2;
          escapeCount++;
        } else {
          fieldByteEnd++;
        }
      }
      // Process the last byte if necessary.
      if (fieldByteEnd == endLessOne && fieldId < fieldCount) {
        if (bytes[fieldByteEnd] == separator) {
          escapeCounts[fieldId] = escapeCount;
          escapeCount = 0;
          startPosition[fieldId++] = fieldByteBegin;
          if (fieldId <= fieldCount) {
            fieldByteBegin = ++fieldByteEnd;
          }
        } else {
          fieldByteEnd++;
        }
      }
      // End serves as final separator.
      if (fieldByteEnd == end && fieldId < fieldCount) {
        escapeCounts[fieldId] = escapeCount;
        startPosition[fieldId++] = fieldByteBegin;
      }
    }

    if (fieldId == fieldCount || fieldByteEnd == end) {
      // All fields have been parsed, or bytes have been parsed.
      // We need to set the startPosition of fields.length to ensure we
      // can use the same formula to calculate the length of each field.
      // For missing fields, their starting positions will all be the same,
      // which will make their lengths to be -1 and uncheckedGetField will
      // return these fields as NULLs.
      Arrays.fill(startPosition, fieldId, startPosition.length, fieldByteEnd + 1);
    }

    isEndOfInputReached = (fieldByteEnd == end);
  }

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
  @Override
  public boolean readNextField() throws IOException {
    if (nextFieldIndex + 1 >= fieldCount) {
      return false;
    }
    nextFieldIndex++;
    return readField(nextFieldIndex);
  }

  /*
   * Reads through an undesired field.
   *
   * No data values are valid after this call.
   * Designed for skipping columns that are not included.
   */
  public void skipNextField() throws IOException {
    if (!parsed) {
      parse();
      parsed = true;
    }
    if (nextFieldIndex + 1 >= fieldCount) {
      // No more.
    } else {
      nextFieldIndex++;
    }
  }

  @Override
  public boolean isReadFieldSupported() {
    return true;
  }

  private boolean checkNull(byte[] bytes, int start, int len) {
    if (len != nullSequenceBytes.length) {
      return false;
    }
    final byte[] nullSequenceBytes = this.nullSequenceBytes;
    switch(len) {
    case 0:
      return true;
    case 2:
      return bytes[start] == nullSequenceBytes[0] && bytes[start+1] == nullSequenceBytes[1];
    case 4:
      return bytes[start] == nullSequenceBytes[0] && bytes[start+1] == nullSequenceBytes[1]
          && bytes[start+2] == nullSequenceBytes[2] && bytes[start+3] == nullSequenceBytes[3];
    default:
      for (int i = 0; i < nullSequenceBytes.length; i++) {
        if (bytes[start + i] != nullSequenceBytes[i]) {
          return false;
        }
      }
      return true;
    }
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

    if (!parsed) {
      parse();
      parsed = true;
    }

    currentFieldIndex = fieldIndex;

    final int fieldStart = startPosition[fieldIndex];
    currentFieldStart = fieldStart;
    final int fieldLength = startPosition[fieldIndex + 1] - startPosition[fieldIndex] - 1;
    currentFieldLength = fieldLength;
    if (fieldLength < 0) {
      return false;
    }

    final byte[] bytes = this.bytes;

    // Is the field the configured string representing NULL?
    if (nullSequenceBytes != null) {
      if (checkNull(bytes, fieldStart, fieldLength)) {
        return false;
      }
    }

    try {
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
                (bytes[i + 2] == 'U' || bytes[i + 2] == 'u') &&
                (bytes[i + 3] == 'E' || bytes[i + 3] == 'e')) {
              currentBoolean = true;
            } else {
              // No boolean value match for 4 char field.
              return false;
            }
          } else if (fieldLength == 5) {
            if ((bytes[i] == 'F' || bytes[i] == 'f') &&
                (bytes[i + 1] == 'A' || bytes[i + 1] == 'a') &&
                (bytes[i + 2] == 'L' || bytes[i + 2] == 'l') &&
                (bytes[i + 3] == 'S' || bytes[i + 3] == 's') &&
                (bytes[i + 4] == 'E' || bytes[i + 4] == 'e')) {
              currentBoolean = false;
            } else {
              // No boolean value match for 5 char field.
              return false;
            }
          } else if (isExtendedBooleanLiteral && fieldLength == 1) {
            byte b = bytes[fieldStart];
            if (b == '1' || b == 't' || b == 'T') {
              currentBoolean = true;
            } else if (b == '0' || b == 'f' || b == 'F') {
              currentBoolean = false;
            } else {
              // No boolean value match for extended 1 char field.
              return false;
            }
          } else {
            // No boolean value match for other lengths.
            return false;
          }
        }
        return true;
      case BYTE:
        if (!LazyUtils.isNumberMaybe(bytes, fieldStart, fieldLength)) {
          return false;
        }
        currentByte = LazyByte.parseByte(bytes, fieldStart, fieldLength, 10);
        return true;
      case SHORT:
        if (!LazyUtils.isNumberMaybe(bytes, fieldStart, fieldLength)) {
          return false;
        }
        currentShort = LazyShort.parseShort(bytes, fieldStart, fieldLength, 10);
        return true;
      case INT:
        if (!LazyUtils.isNumberMaybe(bytes, fieldStart, fieldLength)) {
          return false;
        }
        currentInt = LazyInteger.parseInt(bytes, fieldStart, fieldLength, 10);
        return true;
      case LONG:
        if (!LazyUtils.isNumberMaybe(bytes, fieldStart, fieldLength)) {
          return false;
        }
        currentLong = LazyLong.parseLong(bytes, fieldStart, fieldLength, 10);
        return true;
      case FLOAT:
        if (!LazyUtils.isNumberMaybe(bytes, fieldStart, fieldLength)) {
          return false;
        }
        currentFloat =
            Float.parseFloat(
                new String(bytes, fieldStart, fieldLength, StandardCharsets.UTF_8));
        return true;
      case DOUBLE:
        if (!LazyUtils.isNumberMaybe(bytes, fieldStart, fieldLength)) {
          return false;
        }
        currentDouble = StringToDouble.strtod(bytes, fieldStart, fieldLength);
        return true;
      case STRING:
      case CHAR:
      case VARCHAR:
        {
          if (isEscaped) {
            if (escapeCounts[fieldIndex] == 0) {
              // No escaping.
              currentExternalBufferNeeded = false;
              currentBytes = bytes;
              currentBytesStart = fieldStart;
              currentBytesLength = fieldLength;
            } else {
              final int unescapedLength = fieldLength - escapeCounts[fieldIndex];
              if (useExternalBuffer) {
                currentExternalBufferNeeded = true;
                currentExternalBufferNeededLen = unescapedLength;
              } else {
                // The copyToBuffer will reposition and re-read the input buffer.
                currentExternalBufferNeeded = false;
                if (internalBufferLen < unescapedLength) {
                  internalBufferLen = unescapedLength;
                  internalBuffer = new byte[internalBufferLen];
                }
                copyToBuffer(internalBuffer, 0, unescapedLength);
                currentBytes = internalBuffer;
                currentBytesStart = 0;
                currentBytesLength = unescapedLength;
              }
            }
          } else {
            // If the data is not escaped, reference the data directly.
            currentExternalBufferNeeded = false;
            currentBytes = bytes;
            currentBytesStart = fieldStart;
            currentBytesLength = fieldLength;
          }
        }
        return true;
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
        return true;
      case DATE:
        if (!LazyUtils.isDateMaybe(bytes, fieldStart, fieldLength)) {
          return false;
        }
        currentDateWritable.set(
            Date.valueOf(
                new String(bytes, fieldStart, fieldLength, StandardCharsets.UTF_8)));
        return true;
      case TIMESTAMP:
        {
          if (!LazyUtils.isDateMaybe(bytes, fieldStart, fieldLength)) {
            return false;
          }
          String s = new String(bytes, fieldStart, fieldLength, StandardCharsets.US_ASCII);
          if (s.compareTo("NULL") == 0) {
            logExceptionMessage(bytes, fieldStart, fieldLength, "TIMESTAMP");
            return false;
          }
          try {
            currentTimestampWritable.set(timestampParser.parseTimestamp(s));
          } catch (IllegalArgumentException e) {
            logExceptionMessage(bytes, fieldStart, fieldLength, "TIMESTAMP");
            return false;
          }
        }
        return true;
      case INTERVAL_YEAR_MONTH:
        if (fieldLength == 0) {
          return false;
        }
        try {
          String s = new String(bytes, fieldStart, fieldLength, StandardCharsets.UTF_8);
          currentHiveIntervalYearMonthWritable.set(HiveIntervalYearMonth.valueOf(s));
        } catch (Exception e) {
          logExceptionMessage(bytes, fieldStart, fieldLength, "INTERVAL_YEAR_MONTH");
          return false;
        }
        return true;
      case INTERVAL_DAY_TIME:
        if (fieldLength == 0) {
          return false;
        }
        try {
          String s = new String(bytes, fieldStart, fieldLength, StandardCharsets.UTF_8);
          currentHiveIntervalDayTimeWritable.set(HiveIntervalDayTime.valueOf(s));
        } catch (Exception e) {
          logExceptionMessage(bytes, fieldStart, fieldLength, "INTERVAL_DAY_TIME");
          return false;
        }
        return true;
      case DECIMAL:
        {
          if (!LazyUtils.isNumberMaybe(bytes, fieldStart, fieldLength)) {
            return false;
          }
          // Trim blanks because OldHiveDecimal did...
          currentHiveDecimalWritable.setFromBytes(bytes, fieldStart, fieldLength, /* trimBlanks */ true);
          boolean decimalIsNull = !currentHiveDecimalWritable.isSet();
          if (!decimalIsNull) {
            DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfos[fieldIndex];

            int precision = decimalTypeInfo.getPrecision();
            int scale = decimalTypeInfo.getScale();

            decimalIsNull = !currentHiveDecimalWritable.mutateEnforcePrecisionScale(precision, scale);
          }
          if (decimalIsNull) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Data not in the HiveDecimal data type range so converted to null. Given data is :"
                + new String(bytes, fieldStart, fieldLength, StandardCharsets.UTF_8));
            }
            return false;
          }
        }
        return true;

      default:
        throw new Error("Unexpected primitive category " + primitiveCategories[fieldIndex].name());
      }
    } catch (NumberFormatException nfe) {
       // U+FFFD will throw this as well
       logExceptionMessage(bytes, fieldStart, fieldLength, primitiveCategories[fieldIndex]);
       return false;
    } catch (IllegalArgumentException iae) {
       // E.g. can be thrown by Date.valueOf
       logExceptionMessage(bytes, fieldStart, fieldLength, primitiveCategories[fieldIndex]);
       return false;
    }
  }

  @Override
  public void copyToExternalBuffer(byte[] externalBuffer, int externalBufferStart) {
    copyToBuffer(externalBuffer, externalBufferStart, currentExternalBufferNeededLen);
  }

  private void copyToBuffer(byte[] buffer, int bufferStart, int bufferLength) {

    final int fieldStart = currentFieldStart;
    int k = 0;
    for (int i = 0; i < bufferLength; i++) {
      byte b = bytes[fieldStart + i];
      if (b == escapeChar && i < bufferLength - 1) {
        ++i;
        // Check if it's '\r' or '\n'
        if (bytes[fieldStart + i] == 'r') {
          buffer[bufferStart + k++] = '\r';
        } else if (bytes[fieldStart + i] == 'n') {
          buffer[bufferStart + k++] = '\n';
        } else {
          // get the next byte
          buffer[bufferStart + k++] = bytes[fieldStart + i];
        }
      } else {
        buffer[bufferStart + k++] = b;
      }
    }
  }

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
  public boolean isEndOfInputReached() {
    return isEndOfInputReached;
  }

  public void logExceptionMessage(byte[] bytes, int bytesStart, int bytesLength,
      PrimitiveCategory dataCategory) {
    final String dataType;
    switch (dataCategory) {
    case BYTE:
      dataType = "TINYINT";
      break;
    case LONG:
      dataType = "BIGINT";
      break;
    case SHORT:
      dataType = "SMALLINT";
      break;
    default:
      dataType = dataCategory.toString();
      break;
    }
    logExceptionMessage(bytes, bytesStart, bytesLength, dataType);
  }

  public void logExceptionMessage(byte[] bytes, int bytesStart, int bytesLength, String dataType) {
    try {
      if (LOG.isDebugEnabled()) {
        String byteData = Text.decode(bytes, bytesStart, bytesLength);
        LOG.debug("Data not in the " + dataType
            + " data type range so converted to null. Given data is :" +
                    byteData, new Exception("For debugging purposes"));
      }
    } catch (CharacterCodingException e1) {
      LOG.debug("Data not in the " + dataType + " data type range so converted to null.", e1);
    }
  }

  //------------------------------------------------------------------------------------------------

  private static final byte[] maxLongBytes = ((Long) Long.MAX_VALUE).toString().getBytes();

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
