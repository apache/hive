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

package org.apache.hadoop.hive.serde2.lazybinary.fast;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import org.apache.hadoop.hive.serde2.fast.SerializeWrite;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.LIST;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.MAP;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.STRUCT;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.UNION;

/*
 * Directly serialize, field-by-field, the LazyBinary format.
*
 * This is an alternative way to serialize than what is provided by LazyBinarySerDe.
  */
public class LazyBinarySerializeWrite implements SerializeWrite {
  public static final Logger LOG = LoggerFactory.getLogger(LazyBinarySerializeWrite.class.getName());

  private Output output;

  private int rootFieldCount;
  private boolean skipLengthPrefix = false;

  // For thread safety, we allocate private writable objects for our use only.
  private TimestampWritableV2 timestampWritable;
  private HiveIntervalYearMonthWritable hiveIntervalYearMonthWritable;
  private HiveIntervalDayTimeWritable hiveIntervalDayTimeWritable;
  private HiveIntervalDayTime hiveIntervalDayTime;
  private HiveDecimalWritable hiveDecimalWritable;
  private byte[] vLongBytes;
  private long[] scratchLongs;
  private byte[] scratchBuffer;

  private Field root;
  private Deque<Field> stack = new ArrayDeque<>();
  private LazyBinarySerDe.BooleanRef warnedOnceNullMapKey;

  private static class Field {
    Category type;

    int fieldCount;
    int fieldIndex;
    int byteSizeStart;
    int start;
    long nullOffset;
    byte nullByte;

    Field(Category type) {
      this.type = type;
    }
  }

  public LazyBinarySerializeWrite(int fieldCount) {
    this();
    vLongBytes = new byte[LazyBinaryUtils.VLONG_BYTES_LEN];
    this.rootFieldCount = fieldCount;
    resetWithoutOutput();
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
    resetWithoutOutput();
  }

  /*
   * Set the buffer that will receive the serialized data.  The output buffer will NOT be reset.
   */
  @Override
  public void setAppend(Output output) {
    this.output = output;
    resetWithoutOutput();
    root.nullOffset = output.getLength();
  }

  /*
   * Reset the previously supplied buffer that will receive the serialized data.
   */
  @Override
  public void reset() {
    output.reset();
    resetWithoutOutput();
  }

  private void resetWithoutOutput() {
    root = new Field(STRUCT);
    root.fieldCount = rootFieldCount;
    stack.clear();
    stack.push(root);
    warnedOnceNullMapKey = null;
  }

  /*
   * Write a NULL field.
   */
  @Override
  public void writeNull() throws IOException {
    final Field current = stack.peek();

    if (current.type == STRUCT) {
      // Every 8 fields we write a NULL byte.
      if ((current.fieldIndex % 8) == 0) {
        if (current.fieldIndex > 0) {
          // Write back previous 8 field's NULL byte.
          output.writeByte(current.nullOffset, current.nullByte);
          current.nullByte = 0;
          current.nullOffset = output.getLength();
        }
        // Allocate next NULL byte.
        output.reserve(1);
      }

      // We DO NOT set a bit in the NULL byte when we are writing a NULL.

      current.fieldIndex++;

      if (current.fieldIndex == current.fieldCount) {
        // Write back the final NULL byte before the last fields.
        output.writeByte(current.nullOffset, current.nullByte);
      }
    }
  }

  /*
   * BOOLEAN.
   */
  @Override
  public void writeBoolean(boolean v) throws IOException {
    beginElement();
    output.write((byte) (v ? 1 : 0));
    finishElement();
  }

  /*
   * BYTE.
   */
  @Override
  public void writeByte(byte v) throws IOException {
    beginElement();
    output.write(v);
    finishElement();
  }

  /*
   * SHORT.
   */
  @Override
  public void writeShort(short v) throws IOException {
    beginElement();
    output.write((byte) (v >> 8));
    output.write((byte) (v));
    finishElement();
  }

  /*
   * INT.
   */
  @Override
  public void writeInt(int v) throws IOException {
    beginElement();
    writeVInt(v);
    finishElement();
  }

  /*
   * LONG.
   */
  @Override
  public void writeLong(long v) throws IOException {
    beginElement();
    writeVLong(v);
    finishElement();
  }

  /*
   * FLOAT.
   */
  @Override
  public void writeFloat(float vf) throws IOException {
    beginElement();
    int v = Float.floatToIntBits(vf);
    output.write((byte) (v >> 24));
    output.write((byte) (v >> 16));
    output.write((byte) (v >> 8));
    output.write((byte) (v));
    finishElement();
  }

  /*
   * DOUBLE.
   */
  @Override
  public void writeDouble(double v) throws IOException {
    beginElement();
    LazyBinaryUtils.writeDouble(output, v);
    finishElement();
  }

  /*
   * STRING.
   *
   * Can be used to write CHAR and VARCHAR when the caller takes responsibility for
   * truncation/padding issues.
   */
  @Override
  public void writeString(byte[] v) throws IOException {
    beginElement();
    final int length = v.length;
    writeVInt(length);
    output.write(v, 0, length);
    finishElement();
  }

  @Override
  public void writeString(byte[] v, int start, int length) throws IOException {
    beginElement();
    writeVInt(length);
    output.write(v, start, length);
    finishElement();
  }

  /*
   * CHAR.
   */
  @Override
  public void writeHiveChar(HiveChar hiveChar) throws IOException {
    final String string = hiveChar.getStrippedValue();
    final byte[] bytes = string.getBytes();
    writeString(bytes);
  }

  /*
   * VARCHAR.
   */
  @Override
  public void writeHiveVarchar(HiveVarchar hiveVarchar) throws IOException {
    final String string = hiveVarchar.getValue();
    final byte[] bytes = string.getBytes();
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
    beginElement();
    writeVInt(DateWritableV2.dateToDays(date));
    finishElement();
  }

  // We provide a faster way to write a date without a Date object.
  @Override
  public void writeDate(int dateAsDays) throws IOException {
    beginElement();
    writeVInt(dateAsDays);
    finishElement();
  }

  /*
   * TIMESTAMP.
   */
  @Override
  public void writeTimestamp(Timestamp v) throws IOException {
    beginElement();
    if (timestampWritable == null) {
      timestampWritable = new TimestampWritableV2();
    }
    timestampWritable.set(v);
    timestampWritable.writeToByteStream(output);
    finishElement();
  }

  /*
   * INTERVAL_YEAR_MONTH.
   */
  @Override
  public void writeHiveIntervalYearMonth(HiveIntervalYearMonth viyt) throws IOException {
    beginElement();
    if (hiveIntervalYearMonthWritable == null) {
      hiveIntervalYearMonthWritable = new HiveIntervalYearMonthWritable();
    }
    hiveIntervalYearMonthWritable.set(viyt);
    hiveIntervalYearMonthWritable.writeToByteStream(output);
    finishElement();
  }

  @Override
  public void writeHiveIntervalYearMonth(int totalMonths) throws IOException {
    beginElement();
    if (hiveIntervalYearMonthWritable == null) {
      hiveIntervalYearMonthWritable = new HiveIntervalYearMonthWritable();
    }
    hiveIntervalYearMonthWritable.set(totalMonths);
    hiveIntervalYearMonthWritable.writeToByteStream(output);
    finishElement();
  }

  /*
   * INTERVAL_DAY_TIME.
   */
  @Override
  public void writeHiveIntervalDayTime(HiveIntervalDayTime vidt) throws IOException {
    beginElement();
    if (hiveIntervalDayTimeWritable == null) {
      hiveIntervalDayTimeWritable = new HiveIntervalDayTimeWritable();
    }
    hiveIntervalDayTimeWritable.set(vidt);
    hiveIntervalDayTimeWritable.writeToByteStream(output);
    finishElement();
  }

  /*
   * DECIMAL.
   *
   * NOTE: The scale parameter is for text serialization (e.g. HiveDecimal.toFormatString) that
   * creates trailing zeroes output decimals.
   */
  @Override
  public void writeDecimal64(long decimal64Long, int scale) throws IOException {
    if (hiveDecimalWritable == null) {
      hiveDecimalWritable = new HiveDecimalWritable();
    }
    hiveDecimalWritable.deserialize64(decimal64Long, scale);
    writeHiveDecimal(hiveDecimalWritable, scale);
  }

  @Override
  public void writeHiveDecimal(HiveDecimal dec, int scale) throws IOException {
    beginElement();
    if (scratchLongs == null) {
      scratchLongs = new long[HiveDecimal.SCRATCH_LONGS_LEN];
      scratchBuffer = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_BIG_INTEGER_BYTES];
    }
    LazyBinarySerDe.writeToByteStream(
        output,
        dec,
        scratchLongs,
        scratchBuffer);
    finishElement();
  }

  @Override
  public void writeHiveDecimal(HiveDecimalWritable decWritable, int scale) throws IOException {
    beginElement();
    if (scratchLongs == null) {
      scratchLongs = new long[HiveDecimal.SCRATCH_LONGS_LEN];
      scratchBuffer = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_BIG_INTEGER_BYTES];
    }
    LazyBinarySerDe.writeToByteStream(
        output,
        decWritable,
        scratchLongs,
        scratchBuffer);
    finishElement();
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

  @Override
  public void beginList(List list) {
    final Field current = new Field(LIST);
    beginComplex(current);

    final int size = list.size();
    current.fieldCount = size;

    if (!skipLengthPrefix) {
      // 1/ reserve spaces for the byte size of the list
      // which is a integer and takes four bytes
      current.byteSizeStart = output.getLength();
      output.reserve(4);
      current.start = output.getLength();
    }
    // 2/ write the size of the list as a VInt
    LazyBinaryUtils.writeVInt(output, size);

    // 3/ write the null bytes
    byte nullByte = 0;
    for (int eid = 0; eid < size; eid++) {
      // set the bit to 1 if an element is not null
      if (null != list.get(eid)) {
        nullByte |= 1 << (eid % 8);
      }
      // store the byte every eight elements or
      // if this is the last element
      if (7 == eid % 8 || eid == size - 1) {
        output.write(nullByte);
        nullByte = 0;
      }
    }
  }

  @Override
  public void separateList() {
  }

  @Override
  public void finishList() {
    final Field current = stack.peek();

    if (!skipLengthPrefix) {
      // 5/ update the list byte size
      int listEnd = output.getLength();
      int listSize = listEnd - current.start;
      writeSizeAtOffset(output, current.byteSizeStart, listSize);
    }

    finishComplex();
  }

  @Override
  public void beginMap(Map<?, ?> map) {
    final Field current = new Field(MAP);
    beginComplex(current);

    if (!skipLengthPrefix) {
      // 1/ reserve spaces for the byte size of the map
      // which is a integer and takes four bytes
      current.byteSizeStart = output.getLength();
      output.reserve(4);
      current.start = output.getLength();
    }

    // 2/ write the size of the map which is a VInt
    final int size = map.size();
    current.fieldIndex = size;
    LazyBinaryUtils.writeVInt(output, size);

    // 3/ write the null bytes
    int b = 0;
    byte nullByte = 0;
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      // set the bit to 1 if a key is not null
      if (null != entry.getKey()) {
        nullByte |= 1 << (b % 8);
      } else if (warnedOnceNullMapKey != null) {
        if (!warnedOnceNullMapKey.value) {
          LOG.warn("Null map key encountered! Ignoring similar problems.");
        }
        warnedOnceNullMapKey.value = true;
      }
      b++;
      // set the bit to 1 if a value is not null
      if (null != entry.getValue()) {
        nullByte |= 1 << (b % 8);
      }
      b++;
      // write the byte to stream every 4 key-value pairs
      // or if this is the last key-value pair
      if (0 == b % 8 || b == size * 2) {
        output.write(nullByte);
        nullByte = 0;
      }
    }
  }

  @Override
  public void separateKey() {
  }

  @Override
  public void separateKeyValuePair() {
  }

  @Override
  public void finishMap() {
    final Field current = stack.peek();

    if (!skipLengthPrefix) {
      // 5/ update the byte size of the map
      int mapEnd = output.getLength();
      int mapSize = mapEnd - current.start;
      writeSizeAtOffset(output, current.byteSizeStart, mapSize);
    }

    finishComplex();
  }

  @Override
  public void beginStruct(List fieldValues) {
    final Field current = new Field(STRUCT);
    beginComplex(current);

    current.fieldCount = fieldValues.size();

    if (!skipLengthPrefix) {
      // 1/ reserve spaces for the byte size of the struct
      // which is a integer and takes four bytes
      current.byteSizeStart = output.getLength();
      output.reserve(4);
      current.start = output.getLength();
    }
    current.nullOffset = output.getLength();
  }

  @Override
  public void separateStruct() {
  }

  @Override
  public void finishStruct() {
    final Field current = stack.peek();

    if (!skipLengthPrefix) {
      // 3/ update the byte size of the struct
      int typeEnd = output.getLength();
      int typeSize = typeEnd - current.start;
      writeSizeAtOffset(output, current.byteSizeStart, typeSize);
    }

    finishComplex();
  }

  @Override
  public void beginUnion(int tag) throws IOException {
    final Field current = new Field(UNION);
    beginComplex(current);

    current.fieldCount = 1;

    if (!skipLengthPrefix) {
      // 1/ reserve spaces for the byte size of the struct
      // which is a integer and takes four bytes
      current.byteSizeStart = output.getLength();
      output.reserve(4);
      current.start = output.getLength();
    }

    // 2/ serialize the union
    output.write(tag);
  }

  @Override
  public void finishUnion() {
    final Field current = stack.peek();

    if (!skipLengthPrefix) {
      // 3/ update the byte size of the struct
      int typeEnd = output.getLength();
      int typeSize = typeEnd - current.start;
      writeSizeAtOffset(output, current.byteSizeStart, typeSize);
    }

    finishComplex();
  }

  private void beginElement() {
    final Field current = stack.peek();

    if (current.type == STRUCT) {
      // Every 8 fields we write a NULL byte.
      if ((current.fieldIndex % 8) == 0) {
        if (current.fieldIndex > 0) {
          // Write back previous 8 field's NULL byte.
          output.writeByte(current.nullOffset, current.nullByte);
          current.nullByte = 0;
          current.nullOffset = output.getLength();
        }
        // Allocate next NULL byte.
        output.reserve(1);
      }

      // Set bit in NULL byte when a field is NOT NULL.
      current.nullByte |= 1 << (current.fieldIndex % 8);
    }
  }

  private void finishElement() {
    final Field current = stack.peek();

    if (current.type == STRUCT) {
      current.fieldIndex++;

      if (current.fieldIndex == current.fieldCount) {
        // Write back the final NULL byte before the last fields.
        output.writeByte(current.nullOffset, current.nullByte);
      }
    }
  }

  private void beginComplex(Field field) {
    beginElement();
    stack.push(field);
  }

  private void finishComplex() {
    stack.pop();
    finishElement();
  }

  private static void writeSizeAtOffset(
      ByteStream.RandomAccessOutput byteStream, int byteSizeStart, int size) {
    byteStream.writeInt(byteSizeStart, size);
  }
}
