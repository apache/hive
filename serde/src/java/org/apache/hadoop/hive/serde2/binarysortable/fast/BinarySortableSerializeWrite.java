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

package org.apache.hadoop.hive.serde2.binarysortable.fast;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.fast.SerializeWrite;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Directly serialize, field-by-field, the BinarySortable format.
 *
 * This is an alternative way to serialize than what is provided by BinarySortableSerDe.
 */
public final class BinarySortableSerializeWrite implements SerializeWrite {
  public static final Logger LOG = LoggerFactory.getLogger(BinarySortableSerializeWrite.class.getName());

  private Output output;

  // The sort order (ascending/descending) for each field. Set to true when descending (invert).
  private boolean[] columnSortOrderIsDesc;
  // Null first/last
  private byte[] columnNullMarker;
  private byte[] columnNotNullMarker;

  // Which field we are on.  We start with -1 to be consistent in style with
  // BinarySortableDeserializeRead.
  private int index;
  private int level;

  private TimestampWritableV2 tempTimestampWritable;
  private HiveDecimalWritable hiveDecimalWritable;
  private byte[] decimalBytesScratch;

  public BinarySortableSerializeWrite(boolean[] columnSortOrderIsDesc,
          byte[] columnNullMarker, byte[] columnNotNullMarker) {
    this();
    this.columnSortOrderIsDesc = columnSortOrderIsDesc;
    this.columnNullMarker = columnNullMarker;
    this.columnNotNullMarker = columnNotNullMarker;
  }

  /*
   * Use this constructor when only ascending sort order is used.
   * By default for ascending order, NULL first.
   */
  public BinarySortableSerializeWrite(int fieldCount) {
    this();
    columnSortOrderIsDesc = new boolean[fieldCount];
    Arrays.fill(columnSortOrderIsDesc, false);
    columnNullMarker = new byte[fieldCount];
    Arrays.fill(columnNullMarker, BinarySortableSerDe.ZERO);
    columnNotNullMarker = new byte[fieldCount];
    Arrays.fill(columnNotNullMarker, BinarySortableSerDe.ONE);
  }

  // Not public since we must have the field count or column sort order information.
  private BinarySortableSerializeWrite() {
    tempTimestampWritable = new TimestampWritableV2();
  }

  /*
   * Set the buffer that will receive the serialized data.  The output buffer will be reset.
   */
  @Override
  public void set(Output output) {
    this.output = output;
    this.output.reset();
    index = -1;
    level = 0;
  }

  /*
   * Set the buffer that will receive the serialized data.  The output buffer will NOT be reset.
   */
  @Override
  public void setAppend(Output output) {
    this.output = output;
    index = -1;
    level = 0;
  }

  /*
   * Reset the previously supplied buffer that will receive the serialized data.
   */
  @Override
  public void reset() {
    output.reset();
    index = -1;
    level = 0;
  }

  /*
   * Write a NULL field.
   */
  @Override
  public void writeNull() throws IOException {
    if (level == 0) {
      index++;
    }
    BinarySortableSerDe.writeByte(output, columnNullMarker[index], columnSortOrderIsDesc[index]);
  }

  private void beginElement() {
    if (level == 0) {
      index++;
    }
    BinarySortableSerDe.writeByte(output, columnNotNullMarker[index], columnSortOrderIsDesc[index]);
  }

  /*
   * BOOLEAN.
   */
  @Override
  public void writeBoolean(boolean v) throws IOException {
    beginElement();
    BinarySortableSerDe.writeByte(output, (byte) (v ? 2 : 1), columnSortOrderIsDesc[index]);
  }

  /*
   * BYTE.
   */
  @Override
  public void writeByte(byte v) throws IOException {
    beginElement();
    BinarySortableSerDe.writeByte(output, (byte) (v ^ 0x80), columnSortOrderIsDesc[index]);
  }

  /*
   * SHORT.
   */
  @Override
  public void writeShort(short v) throws IOException {
    beginElement();
    BinarySortableSerDe.serializeShort(output, v, columnSortOrderIsDesc[index]);
  }

  /*
   * INT.
   */
  @Override
  public void writeInt(int v) throws IOException {
    beginElement();
    BinarySortableSerDe.serializeInt(output, v, columnSortOrderIsDesc[index]);
  }

  /*
   * LONG.
   */
  @Override
  public void writeLong(long v) throws IOException {
    beginElement();
    BinarySortableSerDe.serializeLong(output, v, columnSortOrderIsDesc[index]);
  }

  /*
   * FLOAT.
   */
  @Override
  public void writeFloat(float vf) throws IOException {
    beginElement();
    BinarySortableSerDe.serializeFloat(output, vf, columnSortOrderIsDesc[index]);
  }

  /*
   * DOUBLE.
   */
  @Override
  public void writeDouble(double vd) throws IOException {
    beginElement();
    BinarySortableSerDe.serializeDouble(output, vd, columnSortOrderIsDesc[index]);
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
    BinarySortableSerDe.serializeBytes(output, v, 0, v.length, columnSortOrderIsDesc[index]);
  }

  @Override
  public void writeString(byte[] v, int start, int length) throws IOException {
    beginElement();
    BinarySortableSerDe.serializeBytes(output, v, start, length, columnSortOrderIsDesc[index]);
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
    beginElement();
    BinarySortableSerDe.serializeBytes(output, v, 0, v.length, columnSortOrderIsDesc[index]);
  }

  @Override
  public void writeBinary(byte[] v, int start, int length) {
    beginElement();
    BinarySortableSerDe.serializeBytes(output, v, start, length, columnSortOrderIsDesc[index]);
  }

  /*
   * DATE.
   */
  @Override
  public void writeDate(Date date) throws IOException {
    beginElement();
    BinarySortableSerDe.serializeInt(output, DateWritableV2.dateToDays(date), columnSortOrderIsDesc[index]);
  }

  // We provide a faster way to write a date without a Date object.
  @Override
  public void writeDate(int dateAsDays) throws IOException {
    beginElement();
    BinarySortableSerDe.serializeInt(output, dateAsDays, columnSortOrderIsDesc[index]);
  }

  /*
   * TIMESTAMP.
   */
  @Override
  public void writeTimestamp(Timestamp vt) throws IOException {
    beginElement();
    tempTimestampWritable.set(vt);
    BinarySortableSerDe.serializeTimestampWritable(output, tempTimestampWritable, columnSortOrderIsDesc[index]);
  }

  /*
   * INTERVAL_YEAR_MONTH.
   */
  @Override
  public void writeHiveIntervalYearMonth(HiveIntervalYearMonth viyt) throws IOException {
    beginElement();
    BinarySortableSerDe.serializeHiveIntervalYearMonth(output, viyt, columnSortOrderIsDesc[index]);
  }

  @Override
  public void writeHiveIntervalYearMonth(int totalMonths) throws IOException {
    beginElement();
    BinarySortableSerDe.serializeInt(output, totalMonths, columnSortOrderIsDesc[index]);
  }

  /*
   * INTERVAL_DAY_TIME.
   */
  @Override
  public void writeHiveIntervalDayTime(HiveIntervalDayTime vidt) throws IOException {
    beginElement();
    BinarySortableSerDe.serializeHiveIntervalDayTime(output, vidt, columnSortOrderIsDesc[index]);
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
    if (decimalBytesScratch == null) {
      decimalBytesScratch = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_TO_BYTES];
    }
    BinarySortableSerDe.serializeHiveDecimal(output, dec, columnSortOrderIsDesc[index], decimalBytesScratch);
  }

  @Override
  public void writeHiveDecimal(HiveDecimalWritable decWritable, int scale) throws IOException {
    beginElement();
    if (decimalBytesScratch == null) {
      decimalBytesScratch = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_TO_BYTES];
    }
    BinarySortableSerDe.serializeHiveDecimal(output, decWritable, columnSortOrderIsDesc[index], decimalBytesScratch);
  }

  /*
   * List
   */
  @Override
  public void beginList(List list) {
    beginElement();
    level++;
    if (!list.isEmpty()) {
      BinarySortableSerDe.writeByte(output, (byte) 1, columnSortOrderIsDesc[index]);
    }
  }

  @Override
  public void separateList() {
    BinarySortableSerDe.writeByte(output, (byte) 1, columnSortOrderIsDesc[index]);
  }

  @Override
  public void finishList() {
    level--;
    // and \0 to terminate
    BinarySortableSerDe.writeByte(output, (byte) 0, columnSortOrderIsDesc[index]);
  }

  /*
   * Map
   */
  @Override
  public void beginMap(Map<?, ?> map) {
    beginElement();
    level++;
    if (!map.isEmpty()) {
      BinarySortableSerDe.writeByte(output, (byte) 1, columnSortOrderIsDesc[index]);
    }
  }

  @Override
  public void separateKey() {
  }

  @Override
  public void separateKeyValuePair() {
    BinarySortableSerDe.writeByte(output, (byte) 1, columnSortOrderIsDesc[index]);
  }

  @Override
  public void finishMap() {
    level--;
    // and \0 to terminate
    BinarySortableSerDe.writeByte(output, (byte) 0, columnSortOrderIsDesc[index]);
  }

  /*
   * Struct
   */
  @Override
  public void beginStruct(List fieldValues) {
    beginElement();
    level++;
  }

  @Override
  public void separateStruct() {
  }

  @Override
  public void finishStruct() {
    level--;
  }

  /*
   * Union
   */
  @Override
  public void beginUnion(int tag) throws IOException {
    beginElement();
    BinarySortableSerDe.writeByte(output, (byte) tag, columnSortOrderIsDesc[index]);
    level++;
  }

  @Override
  public void finishUnion() {
    level--;
  }
}
