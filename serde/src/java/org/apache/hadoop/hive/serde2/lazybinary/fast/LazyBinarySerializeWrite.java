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

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import org.apache.hadoop.hive.serde2.fast.SerializeWrite;
import org.apache.hive.common.util.DateUtils;

/*
 * Directly serialize, field-by-field, the LazyBinary format.
*
 * This is an alternative way to serialize than what is provided by LazyBinarySerDe.
  */
public class LazyBinarySerializeWrite implements SerializeWrite {
  public static final Logger LOG = LoggerFactory.getLogger(LazyBinarySerializeWrite.class.getName());

  private Output output;

  private int fieldCount;
  private int fieldIndex;
  private byte nullByte;
  private long nullOffset;

  // For thread safety, we allocate private writable objects for our use only.
  private TimestampWritable timestampWritable;
  private HiveIntervalYearMonthWritable hiveIntervalYearMonthWritable;
  private HiveIntervalDayTimeWritable hiveIntervalDayTimeWritable;
  private HiveIntervalDayTime hiveIntervalDayTime;
  private byte[] vLongBytes;
  private long[] scratchLongs;
  private byte[] scratchBuffer;

  public LazyBinarySerializeWrite(int fieldCount) {
    this();
    vLongBytes = new byte[LazyBinaryUtils.VLONG_BYTES_LEN];
    this.fieldCount = fieldCount;
  }

  // Not public since we must have the field count and other information.
  private LazyBinarySerializeWrite() {
  }

  /*
   * Set the buffer that will receive the serialized data.  The output buffer will be reset.
   */
  @Override
  public void set(Output output) {
    this.output = output;
    output.reset();
    fieldIndex = 0;
    nullByte = 0;
    nullOffset = 0;
  }

  /*
   * Set the buffer that will receive the serialized data.  The output buffer will NOT be reset.
   */
  @Override
  public void setAppend(Output output) {
    this.output = output;
    fieldIndex = 0;
    nullByte = 0;
    nullOffset = output.getLength();
  }

  /*
   * Reset the previously supplied buffer that will receive the serialized data.
   */
  @Override
  public void reset() {
    output.reset();
    fieldIndex = 0;
    nullByte = 0;
    nullOffset = 0;
  }

  /*
   * General Pattern:
   *
   *  // Every 8 fields we write a NULL byte.
   *  IF ((fieldIndex % 8) == 0), then
   *    IF (fieldIndex > 0), then
   *       Write back previous NullByte
   *       NullByte = 0
   *       Remember write position
   *    Allocate room for next NULL byte.
   *
   *  WHEN NOT NULL: Set bit in NULL byte; Write value.
   *  OTHERWISE NULL: We do not set a bit in the nullByte when we are writing a null.
   *
   *  Increment fieldIndex
   *
   *  IF (fieldIndex == fieldCount), then
   *     Write back final NullByte
   *
   */

  /*
   * Write a NULL field.
   */
  @Override
  public void writeNull() throws IOException {

    // Every 8 fields we write a NULL byte.
    if ((fieldIndex % 8) == 0) {
      if (fieldIndex > 0) {
        // Write back previous 8 field's NULL byte.
        output.writeByte(nullOffset, nullByte);
        nullByte = 0;
        nullOffset = output.getLength();
      }
      // Allocate next NULL byte.
      output.reserve(1);
    }

    // We DO NOT set a bit in the NULL byte when we are writing a NULL.

    fieldIndex++;

    if (fieldIndex == fieldCount) {
      // Write back the final NULL byte before the last fields.
      output.writeByte(nullOffset, nullByte);
    }
  }

  /*
   * BOOLEAN.
   */
  @Override
  public void writeBoolean(boolean v) throws IOException {

    // Every 8 fields we write a NULL byte.
    if ((fieldIndex % 8) == 0) {
      if (fieldIndex > 0) {
        // Write back previous 8 field's NULL byte.
        output.writeByte(nullOffset, nullByte);
        nullByte = 0;
        nullOffset = output.getLength();
      }
      // Allocate next NULL byte.
      output.reserve(1);
    }

    // Set bit in NULL byte when a field is NOT NULL.
    nullByte |= 1 << (fieldIndex % 8);

    output.write((byte) (v ? 1 : 0));

    fieldIndex++;

    if (fieldIndex == fieldCount) {
      // Write back the final NULL byte before the last fields.
      output.writeByte(nullOffset, nullByte);
    }
  }

  /*
   * BYTE.
   */
  @Override
  public void writeByte(byte v) throws IOException {

    // Every 8 fields we write a NULL byte.
    if ((fieldIndex % 8) == 0) {
      if (fieldIndex > 0) {
        // Write back previous 8 field's NULL byte.
        output.writeByte(nullOffset, nullByte);
        nullByte = 0;
        nullOffset = output.getLength();
      }
      // Allocate next NULL byte.
      output.reserve(1);
    }

    // Set bit in NULL byte when a field is NOT NULL.
    nullByte |= 1 << (fieldIndex % 8);

    output.write(v);

    fieldIndex++;

    if (fieldIndex == fieldCount) {
      // Write back the final NULL byte before the last fields.
      output.writeByte(nullOffset, nullByte);
    }
  }

  /*
   * SHORT.
   */
  @Override
  public void writeShort(short v) throws IOException {

    // Every 8 fields we write a NULL byte.
    if ((fieldIndex % 8) == 0) {
      if (fieldIndex > 0) {
        // Write back previous 8 field's NULL byte.
        output.writeByte(nullOffset, nullByte);
        nullByte = 0;
        nullOffset = output.getLength();
      }
      // Allocate next NULL byte.
      output.reserve(1);
    }

    // Set bit in NULL byte when a field is NOT NULL.
    nullByte |= 1 << (fieldIndex % 8);

    output.write((byte) (v >> 8));
    output.write((byte) (v));

    fieldIndex++;

    if (fieldIndex == fieldCount) {
      // Write back the final NULL byte before the last fields.
      output.writeByte(nullOffset, nullByte);
    }
  }

  /*
   * INT.
   */
  @Override
  public void writeInt(int v) throws IOException {

    // Every 8 fields we write a NULL byte.
    if ((fieldIndex % 8) == 0) {
      if (fieldIndex > 0) {
        // Write back previous 8 field's NULL byte.
        output.writeByte(nullOffset, nullByte);
        nullByte = 0;
        nullOffset = output.getLength();
      }
      // Allocate next NULL byte.
      output.reserve(1);
    }

    // Set bit in NULL byte when a field is NOT NULL.
    nullByte |= 1 << (fieldIndex % 8);

    writeVInt(v);

    fieldIndex++;

    if (fieldIndex == fieldCount) {
      // Write back the final NULL byte before the last fields.
      output.writeByte(nullOffset, nullByte);
    }
  }

  /*
   * LONG.
   */
  @Override
  public void writeLong(long v) throws IOException {

    // Every 8 fields we write a NULL byte.
    if ((fieldIndex % 8) == 0) {
      if (fieldIndex > 0) {
        // Write back previous 8 field's NULL byte.
        output.writeByte(nullOffset, nullByte);
        nullByte = 0;
        nullOffset = output.getLength();
      }
      // Allocate next NULL byte.
      output.reserve(1);
    }

    // Set bit in NULL byte when a field is NOT NULL.
    nullByte |= 1 << (fieldIndex % 8);

    writeVLong(v);

    fieldIndex++;

    if (fieldIndex == fieldCount) {
      // Write back the final NULL byte before the last fields.
      output.writeByte(nullOffset, nullByte);
    }
  }

  /*
   * FLOAT.
   */
  @Override
  public void writeFloat(float vf) throws IOException {

    // Every 8 fields we write a NULL byte.
    if ((fieldIndex % 8) == 0) {
      if (fieldIndex > 0) {
        // Write back previous 8 field's NULL byte.
        output.writeByte(nullOffset, nullByte);
        nullByte = 0;
        nullOffset = output.getLength();
      }
      // Allocate next NULL byte.
      output.reserve(1);
    }

    // Set bit in NULL byte when a field is NOT NULL.
    nullByte |= 1 << (fieldIndex % 8);

    int v = Float.floatToIntBits(vf);
    output.write((byte) (v >> 24));
    output.write((byte) (v >> 16));
    output.write((byte) (v >> 8));
    output.write((byte) (v));

    fieldIndex++;

    if (fieldIndex == fieldCount) {
      // Write back the final NULL byte before the last fields.
      output.writeByte(nullOffset, nullByte);
    }
  }

  /*
   * DOUBLE.
   */
  @Override
  public void writeDouble(double v) throws IOException {

    // Every 8 fields we write a NULL byte.
    if ((fieldIndex % 8) == 0) {
      if (fieldIndex > 0) {
        // Write back previous 8 field's NULL byte.
        output.writeByte(nullOffset, nullByte);
        nullByte = 0;
        nullOffset = output.getLength();
      }
      // Allocate next NULL byte.
      output.reserve(1);
    }

    // Set bit in NULL byte when a field is NOT NULL.
    nullByte |= 1 << (fieldIndex % 8);

    LazyBinaryUtils.writeDouble(output, v);

    fieldIndex++;

    if (fieldIndex == fieldCount) {
      // Write back the final NULL byte before the last fields.
      output.writeByte(nullOffset, nullByte);
    }
  }

  /*
   * STRING.
   * 
   * Can be used to write CHAR and VARCHAR when the caller takes responsibility for
   * truncation/padding issues.
   */
  @Override
  public void writeString(byte[] v) throws IOException {

    // Every 8 fields we write a NULL byte.
    if ((fieldIndex % 8) == 0) {
      if (fieldIndex > 0) {
        // Write back previous 8 field's NULL byte.
        output.writeByte(nullOffset, nullByte);
        nullByte = 0;
        nullOffset = output.getLength();
      }
      // Allocate next NULL byte.
      output.reserve(1);
    }

    // Set bit in NULL byte when a field is NOT NULL.
    nullByte |= 1 << (fieldIndex % 8);

    int length = v.length;
    writeVInt(length);

    output.write(v, 0, length);

    fieldIndex++;

    if (fieldIndex == fieldCount) {
      // Write back the final NULL byte before the last fields.
      output.writeByte(nullOffset, nullByte);
    }
  }

  @Override
  public void writeString(byte[] v, int start, int length) throws IOException {

    // Every 8 fields we write a NULL byte.
    if ((fieldIndex % 8) == 0) {
      if (fieldIndex > 0) {
        // Write back previous 8 field's NULL byte.
        output.writeByte(nullOffset, nullByte);
        nullByte = 0;
        nullOffset = output.getLength();
      }
      // Allocate next NULL byte.
      output.reserve(1);
    }

    // Set bit in NULL byte when a field is NOT NULL.
    nullByte |= 1 << (fieldIndex % 8);

    writeVInt(length);

    output.write(v, start, length);

    fieldIndex++;

    if (fieldIndex == fieldCount) {
      // Write back the final NULL byte before the last fields.
      output.writeByte(nullOffset, nullByte);
    }
  }

  /*
   * CHAR.
   */
  @Override
  public void writeHiveChar(HiveChar hiveChar) throws IOException {
    String string = hiveChar.getStrippedValue();
    byte[] bytes = string.getBytes();
    writeString(bytes);
  }

  /*
   * VARCHAR.
   */
  @Override
  public void writeHiveVarchar(HiveVarchar hiveVarchar) throws IOException {
    String string = hiveVarchar.getValue();
    byte[] bytes = string.getBytes();
    writeString(bytes);
  }

  /*
   * BINARY.
   */
  @Override
  public void writeBinary(byte[] v) throws IOException {
    writeString(v);
  }

  @Override
  public void writeBinary(byte[] v, int start, int length) throws IOException {
    writeString(v, start, length);
  }

  /*
   * DATE.
   */
  @Override
  public void writeDate(Date date) throws IOException {

    // Every 8 fields we write a NULL byte.
    if ((fieldIndex % 8) == 0) {
      if (fieldIndex > 0) {
        // Write back previous 8 field's NULL byte.
        output.writeByte(nullOffset, nullByte);
        nullByte = 0;
        nullOffset = output.getLength();
      }
      // Allocate next NULL byte.
      output.reserve(1);
    }

    // Set bit in NULL byte when a field is NOT NULL.
    nullByte |= 1 << (fieldIndex % 8);

    writeVInt(DateWritable.dateToDays(date));

    fieldIndex++;

    if (fieldIndex == fieldCount) {
      // Write back the final NULL byte before the last fields.
      output.writeByte(nullOffset, nullByte);
    }
  }

  // We provide a faster way to write a date without a Date object.
  @Override
  public void writeDate(int dateAsDays) throws IOException {

    // Every 8 fields we write a NULL byte.
    if ((fieldIndex % 8) == 0) {
      if (fieldIndex > 0) {
        // Write back previous 8 field's NULL byte.
        output.writeByte(nullOffset, nullByte);
        nullByte = 0;
        nullOffset = output.getLength();
      }
      // Allocate next NULL byte.
      output.reserve(1);
    }

    // Set bit in NULL byte when a field is NOT NULL.
    nullByte |= 1 << (fieldIndex % 8);

    writeVInt(dateAsDays);

    fieldIndex++;

    if (fieldIndex == fieldCount) {
      // Write back the final NULL byte before the last fields.
      output.writeByte(nullOffset, nullByte);
    }
  }

  /*
   * TIMESTAMP.
   */
  @Override
  public void writeTimestamp(Timestamp v) throws IOException {

    // Every 8 fields we write a NULL byte.
    if ((fieldIndex % 8) == 0) {
      if (fieldIndex > 0) {
        // Write back previous 8 field's NULL byte.
        output.writeByte(nullOffset, nullByte);
        nullByte = 0;
        nullOffset = output.getLength();
      }
      // Allocate next NULL byte.
      output.reserve(1);
    }

    // Set bit in NULL byte when a field is NOT NULL.
    nullByte |= 1 << (fieldIndex % 8);

    if (timestampWritable == null) {
      timestampWritable = new TimestampWritable();
    }
    timestampWritable.set(v);
    timestampWritable.writeToByteStream(output);

    fieldIndex++;

    if (fieldIndex == fieldCount) {
      // Write back the final NULL byte before the last fields.
      output.writeByte(nullOffset, nullByte);
    }
  }

  /*
   * INTERVAL_YEAR_MONTH.
   */
  @Override
  public void writeHiveIntervalYearMonth(HiveIntervalYearMonth viyt) throws IOException {

    // Every 8 fields we write a NULL byte.
    if ((fieldIndex % 8) == 0) {
      if (fieldIndex > 0) {
        // Write back previous 8 field's NULL byte.
        output.writeByte(nullOffset, nullByte);
        nullByte = 0;
        nullOffset = output.getLength();
      }
      // Allocate next NULL byte.
      output.reserve(1);
    }

    // Set bit in NULL byte when a field is NOT NULL.
    nullByte |= 1 << (fieldIndex % 8);

    if (hiveIntervalYearMonthWritable == null) {
      hiveIntervalYearMonthWritable = new HiveIntervalYearMonthWritable();
    }
    hiveIntervalYearMonthWritable.set(viyt);
    hiveIntervalYearMonthWritable.writeToByteStream(output);

    fieldIndex++;

    if (fieldIndex == fieldCount) {
      // Write back the final NULL byte before the last fields.
      output.writeByte(nullOffset, nullByte);
    }
  }

  @Override
  public void writeHiveIntervalYearMonth(int totalMonths) throws IOException {

    // Every 8 fields we write a NULL byte.
    if ((fieldIndex % 8) == 0) {
      if (fieldIndex > 0) {
        // Write back previous 8 field's NULL byte.
        output.writeByte(nullOffset, nullByte);
        nullByte = 0;
        nullOffset = output.getLength();
      }
      // Allocate next NULL byte.
      output.reserve(1);
    }

    // Set bit in NULL byte when a field is NOT NULL.
    nullByte |= 1 << (fieldIndex % 8);

    if (hiveIntervalYearMonthWritable == null) {
      hiveIntervalYearMonthWritable = new HiveIntervalYearMonthWritable();
    }
    hiveIntervalYearMonthWritable.set(totalMonths);
    hiveIntervalYearMonthWritable.writeToByteStream(output);

    fieldIndex++;

    if (fieldIndex == fieldCount) {
      // Write back the final NULL byte before the last fields.
      output.writeByte(nullOffset, nullByte);
    }
  }

  /*
   * INTERVAL_DAY_TIME.
   */
  @Override
  public void writeHiveIntervalDayTime(HiveIntervalDayTime vidt) throws IOException {

    // Every 8 fields we write a NULL byte.
    if ((fieldIndex % 8) == 0) {
      if (fieldIndex > 0) {
        // Write back previous 8 field's NULL byte.
        output.writeByte(nullOffset, nullByte);
        nullByte = 0;
        nullOffset = output.getLength();
      }
      // Allocate next NULL byte.
      output.reserve(1);
    }

    // Set bit in NULL byte when a field is NOT NULL.
    nullByte |= 1 << (fieldIndex % 8);

    if (hiveIntervalDayTimeWritable == null) {
      hiveIntervalDayTimeWritable = new HiveIntervalDayTimeWritable();
    }
    hiveIntervalDayTimeWritable.set(vidt);
    hiveIntervalDayTimeWritable.writeToByteStream(output);

    fieldIndex++;

    if (fieldIndex == fieldCount) {
      // Write back the final NULL byte before the last fields.
      output.writeByte(nullOffset, nullByte);
    }
  }

  /*
   * DECIMAL.
   *
   * NOTE: The scale parameter is for text serialization (e.g. HiveDecimal.toFormatString) that
   * creates trailing zeroes output decimals.
   */
  @Override
  public void writeHiveDecimal(HiveDecimal dec, int scale) throws IOException {

    // Every 8 fields we write a NULL byte.
    if ((fieldIndex % 8) == 0) {
      if (fieldIndex > 0) {
        // Write back previous 8 field's NULL byte.
        output.writeByte(nullOffset, nullByte);
        nullByte = 0;
        nullOffset = output.getLength();
      }
      // Allocate next NULL byte.
      output.reserve(1);
    }

    // Set bit in NULL byte when a field is NOT NULL.
    nullByte |= 1 << (fieldIndex % 8);

    if (scratchLongs == null) {
      scratchLongs = new long[HiveDecimal.SCRATCH_LONGS_LEN];
      scratchBuffer = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_BIG_INTEGER_BYTES];
    }
    LazyBinarySerDe.writeToByteStream(
        output,
        dec,
        scratchLongs,
        scratchBuffer);

    fieldIndex++;

    if (fieldIndex == fieldCount) {
      // Write back the final NULL byte before the last fields.
      output.writeByte(nullOffset, nullByte);
    }
  }

  @Override
  public void writeHiveDecimal(HiveDecimalWritable decWritable, int scale) throws IOException {

    // Every 8 fields we write a NULL byte.
    if ((fieldIndex % 8) == 0) {
      if (fieldIndex > 0) {
        // Write back previous 8 field's NULL byte.
        output.writeByte(nullOffset, nullByte);
        nullByte = 0;
        nullOffset = output.getLength();
      }
      // Allocate next NULL byte.
      output.reserve(1);
    }

    // Set bit in NULL byte when a field is NOT NULL.
    nullByte |= 1 << (fieldIndex % 8);

    if (scratchLongs == null) {
      scratchLongs = new long[HiveDecimal.SCRATCH_LONGS_LEN];
      scratchBuffer = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_BIG_INTEGER_BYTES];
    }
    LazyBinarySerDe.writeToByteStream(
        output,
        decWritable,
        scratchLongs,
        scratchBuffer);

    fieldIndex++;

    if (fieldIndex == fieldCount) {
      // Write back the final NULL byte before the last fields.
      output.writeByte(nullOffset, nullByte);
    }
  }

  /*
   * Write a VInt using our temporary byte buffer instead of paying the thread local performance
   * cost of LazyBinaryUtils.writeVInt
   */
  private void writeVInt(int v) {
    final int len = LazyBinaryUtils.writeVLongToByteArray(vLongBytes, v);
    output.write(vLongBytes, 0, len);
  }

  private void writeVLong(long v) {
    final int len = LazyBinaryUtils.writeVLongToByteArray(vLongBytes, v);
    output.write(vLongBytes, 0, len);
  }
}
