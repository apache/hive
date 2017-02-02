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

package org.apache.hadoop.hive.serde2.binarysortable.fast;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.type.FastHiveDecimal;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.binarysortable.InputByteBuffer;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

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
public final class BinarySortableDeserializeRead extends DeserializeRead {
  public static final Logger LOG = LoggerFactory.getLogger(BinarySortableDeserializeRead.class.getName());

  // The sort order (ascending/descending) for each field. Set to true when descending (invert).
  private boolean[] columnSortOrderIsDesc;

  // Which field we are on.  We start with -1 so readNextField can increment once and the read
  // field data methods don't increment.
  private int fieldIndex;

  private int fieldCount;

  private int start;
  private int end;
  private int fieldStart;

  private int bytesStart;

  private int internalBufferLen;
  private byte[] internalBuffer;

  private byte[] tempTimestampBytes;

  private byte[] tempDecimalBuffer;

  private InputByteBuffer inputByteBuffer = new InputByteBuffer();

  /*
   * Use this constructor when only ascending sort order is used.
   */
  public BinarySortableDeserializeRead(PrimitiveTypeInfo[] primitiveTypeInfos,
      boolean useExternalBuffer) {
    this(primitiveTypeInfos, useExternalBuffer, null);
  }

  public BinarySortableDeserializeRead(TypeInfo[] typeInfos, boolean useExternalBuffer,
          boolean[] columnSortOrderIsDesc) {
    super(typeInfos, useExternalBuffer);
    fieldCount = typeInfos.length;
    if (columnSortOrderIsDesc != null) {
      this.columnSortOrderIsDesc = columnSortOrderIsDesc;
    } else {
      this.columnSortOrderIsDesc = new boolean[typeInfos.length];
      Arrays.fill(this.columnSortOrderIsDesc, false);
    }
    inputByteBuffer = new InputByteBuffer();
    internalBufferLen = -1;
  }

  // Not public since we must have column information.
  private BinarySortableDeserializeRead() {
    super();
  }

  /*
   * Set the range of bytes to be deserialized.
   */
  @Override
  public void set(byte[] bytes, int offset, int length) {
    fieldIndex = -1;
    start = offset;
    end = offset + length;
    inputByteBuffer.reset(bytes, start, end);
  }

  /*
   * Get detailed read position information to help diagnose exceptions.
   */
  public String getDetailedReadPositionString() {
    StringBuffer sb = new StringBuffer();

    sb.append("Reading inputByteBuffer of length ");
    sb.append(inputByteBuffer.getEnd());
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
      sb.append("Before first field?");
    } else {
      sb.append("Read field #");
      sb.append(fieldIndex);
      sb.append(" at field start position ");
      sb.append(fieldStart);
      sb.append(" current read offset ");
      sb.append(inputByteBuffer.tell());
    }
    sb.append(" column sort order ");
    sb.append(Arrays.toString(columnSortOrderIsDesc));

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

    // We start with fieldIndex as -1 so we can increment once here and then the read
    // field data methods don't increment.
    fieldIndex++;

    if (fieldIndex >= fieldCount) {
      return false;
    }
    if (inputByteBuffer.isEof()) {
      // Also, reading beyond our byte range produces NULL.
      return false;
    }

    fieldStart = inputByteBuffer.tell();

    byte isNullByte = inputByteBuffer.read(columnSortOrderIsDesc[fieldIndex]);

    if (isNullByte == 0) {
      return false;
    }

    /*
     * We have a field and are positioned to it.  Read it.
     */
    switch (primitiveCategories[fieldIndex]) {
    case BOOLEAN:
      currentBoolean = (inputByteBuffer.read(columnSortOrderIsDesc[fieldIndex]) == 2);
      return true;
    case BYTE:
      currentByte = (byte) (inputByteBuffer.read(columnSortOrderIsDesc[fieldIndex]) ^ 0x80);
      return true;
    case SHORT:
      {
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        int v = inputByteBuffer.read(invert) ^ 0x80;
        v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
        currentShort = (short) v;
      }
      return true;
    case INT:
      {
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        int v = inputByteBuffer.read(invert) ^ 0x80;
        for (int i = 0; i < 3; i++) {
          v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
        }
        currentInt = v;
      }
      return true;
    case LONG:
      {
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        long v = inputByteBuffer.read(invert) ^ 0x80;
        for (int i = 0; i < 7; i++) {
          v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
        }
        currentLong = v;
      }
      return true;
    case DATE:
      {
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        int v = inputByteBuffer.read(invert) ^ 0x80;
        for (int i = 0; i < 3; i++) {
          v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
        }
        currentDateWritable.set(v);
      }
      return true;
    case TIMESTAMP:
      {
        if (tempTimestampBytes == null) {
          tempTimestampBytes = new byte[TimestampWritable.BINARY_SORTABLE_LENGTH];
        }
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        for (int i = 0; i < tempTimestampBytes.length; i++) {
          tempTimestampBytes[i] = inputByteBuffer.read(invert);
        }
        currentTimestampWritable.setBinarySortable(tempTimestampBytes, 0);
      }
      return true;
    case FLOAT:
      {
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        int v = 0;
        for (int i = 0; i < 4; i++) {
          v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
        }
        if ((v & (1 << 31)) == 0) {
          // negative number, flip all bits
          v = ~v;
        } else {
          // positive number, flip the first bit
          v = v ^ (1 << 31);
        }
        currentFloat = Float.intBitsToFloat(v);
      }
      return true;
    case DOUBLE:
      {
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        long v = 0;
        for (int i = 0; i < 8; i++) {
          v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
        }
        if ((v & (1L << 63)) == 0) {
          // negative number, flip all bits
          v = ~v;
        } else {
          // positive number, flip the first bit
          v = v ^ (1L << 63);
        }
        currentDouble = Double.longBitsToDouble(v);
      }
      return true;
    case BINARY:
    case STRING:
    case CHAR:
    case VARCHAR:
      {
        /*
         * This code is a modified version of BinarySortableSerDe.deserializeText that lets us
         * detect if we can return a reference to the bytes directly.
         */

        // Get the actual length first
        bytesStart = inputByteBuffer.tell();
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        int length = 0;
        do {
          byte b = inputByteBuffer.read(invert);
          if (b == 0) {
            // end of string
            break;
          }
          if (b == 1) {
            // the last char is an escape char. read the actual char
            inputByteBuffer.read(invert);
          }
          length++;
        } while (true);

        if (length == 0 ||
            (!invert && length == inputByteBuffer.tell() - bytesStart - 1)) {
          // No inversion or escaping happened, so we are can reference directly.
          currentExternalBufferNeeded = false;
          currentBytes = inputByteBuffer.getData();
          currentBytesStart = bytesStart;
          currentBytesLength = length;
        } else {
          // We are now positioned at the end of this field's bytes.
          if (useExternalBuffer) {
            // If we decided not to reposition and re-read the buffer to copy it with
            // copyToExternalBuffer, we we will still be correctly positioned for the next field.
            currentExternalBufferNeeded = true;
            currentExternalBufferNeededLen = length;
          } else {
            // The copyToBuffer will reposition and re-read the input buffer.
            currentExternalBufferNeeded = false;
            if (internalBufferLen < length) {
              internalBufferLen = length;
              internalBuffer = new byte[internalBufferLen];
            }
            copyToBuffer(internalBuffer, 0, length);
            currentBytes = internalBuffer;
            currentBytesStart = 0;
            currentBytesLength = length;
          }
        }
      }
      return true;
    case INTERVAL_YEAR_MONTH:
      {
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        int v = inputByteBuffer.read(invert) ^ 0x80;
        for (int i = 0; i < 3; i++) {
          v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
        }
        currentHiveIntervalYearMonthWritable.set(v);
      }
      return true;
    case INTERVAL_DAY_TIME:
      {
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        long totalSecs = inputByteBuffer.read(invert) ^ 0x80;
        for (int i = 0; i < 7; i++) {
          totalSecs = (totalSecs << 8) + (inputByteBuffer.read(invert) & 0xff);
        }
        int nanos = inputByteBuffer.read(invert) ^ 0x80;
        for (int i = 0; i < 3; i++) {
          nanos = (nanos << 8) + (inputByteBuffer.read(invert) & 0xff);
        }
        currentHiveIntervalDayTimeWritable.set(totalSecs, nanos);
      }
      return true;
    case DECIMAL:
      {
        // Since enforcing precision and scale can cause a HiveDecimal to become NULL,
        // we must read it, enforce it here, and either return NULL or buffer the result.

        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        int b = inputByteBuffer.read(invert) - 1;
        if (!(b == 1 || b == -1 || b == 0)) {
          throw new IOException("Unexpected byte value " + (int)b + " in binary sortable format data (invert " + invert + ")");
        }
        boolean positive = b != -1;

        int factor = inputByteBuffer.read(invert) ^ 0x80;
        for (int i = 0; i < 3; i++) {
          factor = (factor << 8) + (inputByteBuffer.read(invert) & 0xff);
        }

        if (!positive) {
          factor = -factor;
        }

        int decimalStart = inputByteBuffer.tell();
        int length = 0;

        do {
          b = inputByteBuffer.read(positive ? invert : !invert);
          if (b == 1) {
            throw new IOException("Expected -1 and found byte value " + (int)b + " in binary sortable format data (invert " + invert + ")");
          }


          if (b == 0) {
            // end of digits
            break;
          }

          length++;
        } while (true);

        // CONSIDER: Allocate a larger initial size.
        if(tempDecimalBuffer == null || tempDecimalBuffer.length < length) {
          tempDecimalBuffer = new byte[length];
        }

        inputByteBuffer.seek(decimalStart);
        for (int i = 0; i < length; ++i) {
          tempDecimalBuffer[i] = inputByteBuffer.read(positive ? invert : !invert);
        }

        // read the null byte again
        inputByteBuffer.read(positive ? invert : !invert);

        String digits = new String(tempDecimalBuffer, 0, length, StandardCharsets.UTF_8);

        // Set the value of the writable from the decimal digits that were written with no dot.
        int scale = length - factor;
        currentHiveDecimalWritable.setFromDigitsOnlyBytesWithScale(
            !positive, tempDecimalBuffer, 0, length, scale);
        boolean decimalIsNull = !currentHiveDecimalWritable.isSet();
        if (!decimalIsNull) {

          // We have a decimal.  After we enforce precision and scale, will it become a NULL?

          DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfos[fieldIndex];

          int enforcePrecision = decimalTypeInfo.getPrecision();
          int enforceScale = decimalTypeInfo.getScale();

          decimalIsNull =
              !currentHiveDecimalWritable.mutateEnforcePrecisionScale(
                  enforcePrecision, enforceScale);

        }
        if (decimalIsNull) {
          return false;
        }
      }
      return true;
    default:
      throw new RuntimeException("Unexpected primitive type category " + primitiveCategories[fieldIndex]);
    }
  }

  /*
   * Reads through an undesired field.
   *
   * No data values are valid after this call.
   * Designed for skipping columns that are not included.
   */
  public void skipNextField() throws IOException {
    // Not a known use case for BinarySortable -- so don't optimize.
    readNextField();
  }

  @Override
  public void copyToExternalBuffer(byte[] externalBuffer, int externalBufferStart) throws IOException {
    copyToBuffer(externalBuffer, externalBufferStart, currentExternalBufferNeededLen);
  }

  private void copyToBuffer(byte[] buffer, int bufferStart, int bufferLength) throws IOException {
    final boolean invert = columnSortOrderIsDesc[fieldIndex];
    inputByteBuffer.seek(bytesStart);
    // 3. Copy the data.
    for (int i = 0; i < bufferLength; i++) {
      byte b = inputByteBuffer.read(invert);
      if (b == 1) {
        // The last char is an escape char, read the actual char.
        // The serialization format escape \0 to \1, and \1 to \2,
        // to make sure the string is null-terminated.
        b = (byte) (inputByteBuffer.read(invert) - 1);
      }
      buffer[bufferStart + i] = b;
    }
    // 4. Read the null terminator.
    byte b = inputByteBuffer.read(invert);
    if (b != 0) {
      throw new RuntimeException("Expected 0 terminating byte");
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
    return inputByteBuffer.isEof();
  }
}
