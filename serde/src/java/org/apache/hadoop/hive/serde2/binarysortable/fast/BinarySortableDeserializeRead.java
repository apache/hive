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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.binarysortable.InputByteBuffer;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;

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
public final class BinarySortableDeserializeRead extends DeserializeRead {
  public static final Logger LOG = LoggerFactory.getLogger(BinarySortableDeserializeRead.class.getName());

  // The sort order (ascending/descending) for each field. Set to true when descending (invert).
  private boolean[] columnSortOrderIsDesc;

  // Which field we are on.  We start with -1 so readCheckNull can increment once and the read
  // field data methods don't increment.
  private int fieldIndex;

  private int fieldCount;

  private int start;
  private int end;
  private int fieldStart;

  private byte[] tempTimestampBytes;
  private Text tempText;

  private byte[] tempDecimalBuffer;

  private boolean readBeyondConfiguredFieldsWarned;
  private boolean bufferRangeHasExtraDataWarned;

  private InputByteBuffer inputByteBuffer = new InputByteBuffer();

  /*
   * Use this constructor when only ascending sort order is used.
   */
  public BinarySortableDeserializeRead(PrimitiveTypeInfo[] primitiveTypeInfos) {
    this(primitiveTypeInfos, null);
  }

  public BinarySortableDeserializeRead(TypeInfo[] typeInfos,
          boolean[] columnSortOrderIsDesc) {
    super(typeInfos);
    fieldCount = typeInfos.length;
    if (columnSortOrderIsDesc != null) {
      this.columnSortOrderIsDesc = columnSortOrderIsDesc;
    } else {
      this.columnSortOrderIsDesc = new boolean[typeInfos.length];
      Arrays.fill(this.columnSortOrderIsDesc, false);
    }
    inputByteBuffer = new InputByteBuffer();
    readBeyondConfiguredFieldsWarned = false;
    bufferRangeHasExtraDataWarned = false;
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

    return sb.toString();
  }

  /*
   * Reads the NULL information for a field.
   *
   * @return Returns true when the field is NULL; reading is positioned to the next field.
   *         Otherwise, false when the field is NOT NULL; reading is positioned to the field data.
   */
  @Override
  public boolean readCheckNull() throws IOException {

    // We start with fieldIndex as -1 so we can increment once here and then the read
    // field data methods don't increment.
    fieldIndex++;

    if (fieldIndex >= fieldCount) {
      // Reading beyond the specified field count produces NULL.
      if (!readBeyondConfiguredFieldsWarned) {
        doReadBeyondConfiguredFieldsWarned();
      }
      return true;
    }
    if (inputByteBuffer.isEof()) {
      // Also, reading beyond our byte range produces NULL.
      return true;
    }

    fieldStart = inputByteBuffer.tell();

    byte isNullByte = inputByteBuffer.read(columnSortOrderIsDesc[fieldIndex]);

    if (isNullByte == 0) {
      return true;
    }

    /*
     * We have a field and are positioned to it.  Read it.
     */
    boolean isNull = false;    // Assume.
    switch (primitiveCategories[fieldIndex]) {
    case BOOLEAN:
      currentBoolean = (inputByteBuffer.read(columnSortOrderIsDesc[fieldIndex]) == 2);
      break;
    case BYTE:
      currentByte = (byte) (inputByteBuffer.read(columnSortOrderIsDesc[fieldIndex]) ^ 0x80);
      break;
    case SHORT:
      {
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        int v = inputByteBuffer.read(invert) ^ 0x80;
        v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
        currentShort = (short) v;
      }
      break;
    case INT:
      {
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        int v = inputByteBuffer.read(invert) ^ 0x80;
        for (int i = 0; i < 3; i++) {
          v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
        }
        currentInt = v;
      }
      break;
    case LONG:
      {
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        long v = inputByteBuffer.read(invert) ^ 0x80;
        for (int i = 0; i < 7; i++) {
          v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
        }
        currentLong = v;
      }
      break;
    case DATE:
      {
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        int v = inputByteBuffer.read(invert) ^ 0x80;
        for (int i = 0; i < 3; i++) {
          v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
        }
        currentDateWritable.set(v);
      }
      break;
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
      break;
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
      break;
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
      break;
    case BINARY:
    case STRING:
    case CHAR:
    case VARCHAR:
      {
        if (tempText == null) {
          tempText = new Text();
        }
        BinarySortableSerDe.deserializeText(
            inputByteBuffer, columnSortOrderIsDesc[fieldIndex], tempText);
        currentBytes = tempText.getBytes();
        currentBytesStart = 0;
        currentBytesLength = tempText.getLength();
      }
      break;
    case INTERVAL_YEAR_MONTH:
      {
        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        int v = inputByteBuffer.read(invert) ^ 0x80;
        for (int i = 0; i < 3; i++) {
          v = (v << 8) + (inputByteBuffer.read(invert) & 0xff);
        }
        currentHiveIntervalYearMonthWritable.set(v);
      }
      break;
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
      break;
    case DECIMAL:
      {
        // Since enforcing precision and scale can cause a HiveDecimal to become NULL,
        // we must read it, enforce it here, and either return NULL or buffer the result.

        final boolean invert = columnSortOrderIsDesc[fieldIndex];
        int b = inputByteBuffer.read(invert) - 1;
        assert (b == 1 || b == -1 || b == 0);
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
          assert(b != 1);

          if (b == 0) {
            // end of digits
            break;
          }

          length++;
        } while (true);

        if(tempDecimalBuffer == null || tempDecimalBuffer.length < length) {
          tempDecimalBuffer = new byte[length];
        }

        inputByteBuffer.seek(decimalStart);
        for (int i = 0; i < length; ++i) {
          tempDecimalBuffer[i] = inputByteBuffer.read(positive ? invert : !invert);
        }

        // read the null byte again
        inputByteBuffer.read(positive ? invert : !invert);

        String digits = new String(tempDecimalBuffer, 0, length, BinarySortableSerDe.decimalCharSet);
        BigInteger bi = new BigInteger(digits);
        HiveDecimal bd = HiveDecimal.create(bi).scaleByPowerOfTen(factor-length);

        if (!positive) {
          bd = bd.negate();
        }

        // We have a decimal.  After we enforce precision and scale, will it become a NULL?

        currentHiveDecimalWritable.set(bd);

        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfos[fieldIndex];

        int precision = decimalTypeInfo.getPrecision();
        int scale = decimalTypeInfo.getScale();

        HiveDecimal decimal = currentHiveDecimalWritable.getHiveDecimal(precision, scale);
        if (decimal == null) {
          isNull = true;
        } else {
          // Put value back into writable.
          currentHiveDecimalWritable.set(decimal);
        }
      }
      break;
    default:
      throw new RuntimeException("Unexpected primitive type category " + primitiveCategories[fieldIndex]);
    }

    /*
     * Now that we have read through the field -- did we really want it?
     */
    if (columnsToInclude != null && !columnsToInclude[fieldIndex]) {
      isNull = true;
    }

    return isNull;
  }

  /*
   * Call this method after all fields have been read to check for extra fields.
   */
  public void extraFieldsCheck() {
    if (!inputByteBuffer.isEof()) {
      // We did not consume all of the byte range.
      if (!bufferRangeHasExtraDataWarned) {
        // Warn only once.
       int length = inputByteBuffer.getEnd() - start;
       int remaining = inputByteBuffer.getEnd() - inputByteBuffer.tell();
        LOG.info("Not all fields were read in the buffer range! Buffer range " +  start
            + " for length " + length + " but " + remaining + " bytes remain. "
            + "(total buffer length " + inputByteBuffer.getData().length + ")"
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
  public boolean bufferRangeHasExtraDataWarned() {
    return bufferRangeHasExtraDataWarned;
  }

  /*
   * Pull these out of the regular execution path.
   */

  private void doReadBeyondConfiguredFieldsWarned() {
    // Warn only once.
    LOG.info("Reading beyond configured fields! Configured " + fieldCount + " fields but "
        + " reading more (NULLs returned).  Ignoring similar problems.");
    readBeyondConfiguredFieldsWarned = true;
  }
}
