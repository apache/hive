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
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.VInt;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.VLong;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.WritableUtils;

/*
 * Directly deserialize with the caller reading field-by-field the LazyBinary serialization format.
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
public final class LazyBinaryDeserializeRead extends DeserializeRead {
  public static final Logger LOG = LoggerFactory.getLogger(LazyBinaryDeserializeRead.class.getName());

  private byte[] bytes;
  private int start;
  private int offset;
  private int end;
  private int fieldCount;
  private int fieldStart;
  private int fieldIndex;
  private byte nullByte;

  // Object to receive results of reading a decoded variable length int or long.
  private VInt tempVInt;
  private VLong tempVLong;

  public LazyBinaryDeserializeRead(TypeInfo[] typeInfos, boolean useExternalBuffer) {
    super(typeInfos, useExternalBuffer);
    fieldCount = typeInfos.length;
    tempVInt = new VInt();
    tempVLong = new VLong();
    currentExternalBufferNeeded = false;
  }

  // Not public since we must have the field count so every 8 fields NULL bytes can be navigated.
  private LazyBinaryDeserializeRead() {
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
    fieldIndex = 0;
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
    sb.append(".  Read field #");
    sb.append(fieldIndex);
    sb.append(" at field start position ");
    sb.append(fieldStart);
    sb.append(" current read offset ");
    sb.append(offset);

    return sb.toString();
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
    if (fieldIndex >= fieldCount) {
      return false;
    }

    fieldStart = offset;

    if (fieldIndex == 0) {
      // The rest of the range check for fields after the first is below after checking
      // the NULL byte.
      if (offset >= end) {
        throw new EOFException();
      }
      nullByte = bytes[offset++];
    }

    // NOTE: The bit is set to 1 if a field is NOT NULL.    boolean isNull;
    if ((nullByte & (1 << (fieldIndex % 8))) == 0) {

      // Logically move past this field.
      fieldIndex++;

      // Every 8 fields we read a new NULL byte.
      if (fieldIndex < fieldCount) {
        if ((fieldIndex % 8) == 0) {
          // Get next null byte.
          if (offset >= end) {
            throw new EOFException();
          }
          nullByte = bytes[offset++];
        }
      }
      return false;
    } else {

      // Make sure there is at least one byte that can be read for a value.
      if (offset >= end) {
        throw new EOFException();
      }

      /*
       * We have a field and are positioned to it.  Read it.
       */
      switch (primitiveCategories[fieldIndex]) {
      case BOOLEAN:
        // No check needed for single byte read.
        currentBoolean = (bytes[offset++] != 0);
        break;
      case BYTE:
        // No check needed for single byte read.
        currentByte = bytes[offset++];
        break;
      case SHORT:
        // Last item -- ok to be at end.
        if (offset + 2 > end) {
          throw new EOFException();
        }
        currentShort = LazyBinaryUtils.byteArrayToShort(bytes, offset);
        offset += 2;
        break;
      case INT:
        // Parse the first byte of a vint/vlong to determine the number of bytes.
        if (offset + WritableUtils.decodeVIntSize(bytes[offset]) > end) {
          throw new EOFException();
        }
        LazyBinaryUtils.readVInt(bytes, offset, tempVInt);
        offset += tempVInt.length;
        currentInt = tempVInt.value;
        break;
      case LONG:
        // Parse the first byte of a vint/vlong to determine the number of bytes.
        if (offset + WritableUtils.decodeVIntSize(bytes[offset]) > end) {
          throw new EOFException();
        }
        LazyBinaryUtils.readVLong(bytes, offset, tempVLong);
        offset += tempVLong.length;
        currentLong = tempVLong.value;
        break;
      case FLOAT:
        // Last item -- ok to be at end.
        if (offset + 4 > end) {
          throw new EOFException();
        }
        currentFloat = Float.intBitsToFloat(LazyBinaryUtils.byteArrayToInt(bytes, offset));
        offset += 4;
        break;
      case DOUBLE:
        // Last item -- ok to be at end.
        if (offset + 8 > end) {
          throw new EOFException();
        }
        currentDouble = Double.longBitsToDouble(LazyBinaryUtils.byteArrayToLong(bytes, offset));
        offset += 8;
        break;

      case BINARY:
      case STRING:
      case CHAR:
      case VARCHAR:
        {
          // using vint instead of 4 bytes
          // Parse the first byte of a vint/vlong to determine the number of bytes.
          if (offset + WritableUtils.decodeVIntSize(bytes[offset]) > end) {
            throw new EOFException();
          }
          LazyBinaryUtils.readVInt(bytes, offset, tempVInt);
          offset += tempVInt.length;

          int saveStart = offset;
          int length = tempVInt.value;
          offset += length;
          // Last item -- ok to be at end.
          if (offset > end) {
            throw new EOFException();
          }

          currentBytes = bytes;
          currentBytesStart = saveStart;
          currentBytesLength = length;
        }
        break;
      case DATE:
        // Parse the first byte of a vint/vlong to determine the number of bytes.
        if (offset + WritableUtils.decodeVIntSize(bytes[offset]) > end) {
          throw new EOFException();
        }
        LazyBinaryUtils.readVInt(bytes, offset, tempVInt);
        offset += tempVInt.length;

        currentDateWritable.set(tempVInt.value);
        break;
      case TIMESTAMP:
        {
          int length = TimestampWritable.getTotalLength(bytes, offset);
          int saveStart = offset;
          offset += length;
          // Last item -- ok to be at end.
          if (offset > end) {
            throw new EOFException();
          }

          currentTimestampWritable.set(bytes, saveStart);
        }
        break;
      case INTERVAL_YEAR_MONTH:
        // Parse the first byte of a vint/vlong to determine the number of bytes.
        if (offset + WritableUtils.decodeVIntSize(bytes[offset]) > end) {
          throw new EOFException();
        }
        LazyBinaryUtils.readVInt(bytes, offset, tempVInt);
        offset += tempVInt.length;

        currentHiveIntervalYearMonthWritable.set(tempVInt.value);
        break;
      case INTERVAL_DAY_TIME:
        // The first bounds check requires at least one more byte beyond for 2nd int (hence >=).
        // Parse the first byte of a vint/vlong to determine the number of bytes.
        if (offset + WritableUtils.decodeVIntSize(bytes[offset]) >= end) {
          throw new EOFException();
        }
        LazyBinaryUtils.readVLong(bytes, offset, tempVLong);
        offset += tempVLong.length;

        // Parse the first byte of a vint/vlong to determine the number of bytes.
        if (offset + WritableUtils.decodeVIntSize(bytes[offset]) > end) {
          throw new EOFException();
        }
        LazyBinaryUtils.readVInt(bytes, offset, tempVInt);
        offset += tempVInt.length;

        currentHiveIntervalDayTimeWritable.set(tempVLong.value, tempVInt.value);
        break;
      case DECIMAL:
        {
          // Since enforcing precision and scale can cause a HiveDecimal to become NULL,
          // we must read it, enforce it here, and either return NULL or buffer the result.

          // These calls are to see how much data there is. The setFromBytes call below will do the same
          // readVInt reads but actually unpack the decimal.

          // The first bounds check requires at least one more byte beyond for 2nd int (hence >=).
          // Parse the first byte of a vint/vlong to determine the number of bytes.
          if (offset + WritableUtils.decodeVIntSize(bytes[offset]) >= end) {
            throw new EOFException();
          }
          LazyBinaryUtils.readVInt(bytes, offset, tempVInt);
          offset += tempVInt.length;
          int readScale = tempVInt.value;

          // Parse the first byte of a vint/vlong to determine the number of bytes.
          if (offset + WritableUtils.decodeVIntSize(bytes[offset]) > end) {
            throw new EOFException();
          }
          LazyBinaryUtils.readVInt(bytes, offset, tempVInt);
          offset += tempVInt.length;
          int saveStart = offset;
          offset += tempVInt.value;
          // Last item -- ok to be at end.
          if (offset > end) {
            throw new EOFException();
          }
          int length = offset - saveStart;

          //   scale = 2, length = 6, value = -6065716379.11
          //   \002\006\255\114\197\131\083\105
          //           \255\114\197\131\083\105

          currentHiveDecimalWritable.setFromBigIntegerBytesAndScale(
              bytes, saveStart, length, readScale);
          boolean decimalIsNull = !currentHiveDecimalWritable.isSet();
          if (!decimalIsNull) {

            DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfos[fieldIndex];

            int precision = decimalTypeInfo.getPrecision();
            int scale = decimalTypeInfo.getScale();

            decimalIsNull = !currentHiveDecimalWritable.mutateEnforcePrecisionScale(precision, scale);
          }
          if (decimalIsNull) {

            // Logically move past this field.
            fieldIndex++;

            // Every 8 fields we read a new NULL byte.
            if (fieldIndex < fieldCount) {
              if ((fieldIndex % 8) == 0) {
                // Get next null byte.
                if (offset >= end) {
                  throw new EOFException();
                }
                nullByte = bytes[offset++];
              }
            }
            return false;
          }
        }
        break;

      default:
        throw new Error("Unexpected primitive category " + primitiveCategories[fieldIndex].name());
      }
    }

    // Logically move past this field.
    fieldIndex++;

    // Every 8 fields we read a new NULL byte.
    if (fieldIndex < fieldCount) {
      if ((fieldIndex % 8) == 0) {
        // Get next null byte.
        if (offset >= end) {
          throw new EOFException();
        }
        nullByte = bytes[offset++];
      }
    }

    return true;
  }

  /*
   * Reads through an undesired field.
   *
   * No data values are valid after this call.
   * Designed for skipping columns that are not included.
   */
  public void skipNextField() throws IOException {
    // Not a known use case for LazyBinary -- so don't optimize.
    readNextField();
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
    return (offset == end);
  }
}
