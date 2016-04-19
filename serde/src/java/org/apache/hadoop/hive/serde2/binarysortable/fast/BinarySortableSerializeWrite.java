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
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.fast.SerializeWrite;
import org.apache.hive.common.util.DateUtils;

/*
 * Directly serialize, field-by-field, the BinarySortable format.
 *
 * This is an alternative way to serialize than what is provided by BinarySortableSerDe.
 */
public class BinarySortableSerializeWrite implements SerializeWrite {
  public static final Log LOG = LogFactory.getLog(BinarySortableSerializeWrite.class.getName());

  private Output output;

  // The sort order (ascending/descending) for each field. Set to true when descending (invert).
  private boolean[] columnSortOrderIsDesc;

  // Which field we are on.  We start with -1 to be consistent in style with
  // BinarySortableDeserializeRead.
  private int index;

  private int fieldCount;

  private TimestampWritable tempTimestampWritable;

  public BinarySortableSerializeWrite(boolean[] columnSortOrderIsDesc) {
    this();
    fieldCount = columnSortOrderIsDesc.length;
    this.columnSortOrderIsDesc = columnSortOrderIsDesc;
  }

  /*
   * Use this constructor when only ascending sort order is used.
   */
  public BinarySortableSerializeWrite(int fieldCount) {
    this();
    this.fieldCount = fieldCount;
    columnSortOrderIsDesc = new boolean[fieldCount];
    Arrays.fill(columnSortOrderIsDesc, false);
  }

  // Not public since we must have the field count or column sort order information.
  private BinarySortableSerializeWrite() {
    tempTimestampWritable = new TimestampWritable();
  }

  /*
   * Set the buffer that will receive the serialized data.  The output buffer will be reset.
   */
  @Override
  public void set(Output output) {
    this.output = output;
    this.output.reset();
    index = -1;
  }

  /*
   * Set the buffer that will receive the serialized data.  The output buffer will NOT be reset.
   */
  @Override
  public void setAppend(Output output) {
    this.output = output;
    index = -1;
  }

  /*
   * Reset the previously supplied buffer that will receive the serialized data.
   */
  @Override
  public void reset() {
    output.reset();
    index = -1;
  }

  /*
   * Write a NULL field.
   */
  @Override
  public void writeNull() throws IOException {
    BinarySortableSerDe.writeByte(output, (byte) 0, columnSortOrderIsDesc[++index]);
  }

  /*
   * BOOLEAN.
   */
  @Override
  public void writeBoolean(boolean v) throws IOException {
    final boolean invert = columnSortOrderIsDesc[++index];

    // This field is not a null.
    BinarySortableSerDe.writeByte(output, (byte) 1, invert);

    BinarySortableSerDe.writeByte(output, (byte) (v ? 2 : 1), invert);
  }

  /*
   * BYTE.
   */
  @Override
  public void writeByte(byte v) throws IOException {
    final boolean invert = columnSortOrderIsDesc[++index];

    // This field is not a null.
    BinarySortableSerDe.writeByte(output, (byte) 1, invert);

    BinarySortableSerDe.writeByte(output, (byte) (v ^ 0x80), invert);
  }

  /*
   * SHORT.
   */
  @Override
  public void writeShort(short v) throws IOException {
    final boolean invert = columnSortOrderIsDesc[++index];

    // This field is not a null.
    BinarySortableSerDe.writeByte(output, (byte) 1, invert);

    BinarySortableSerDe.serializeShort(output, v, invert);
  }

  /*
   * INT.
   */
  @Override
  public void writeInt(int v) throws IOException {
    final boolean invert = columnSortOrderIsDesc[++index];

    // This field is not a null.
    BinarySortableSerDe.writeByte(output, (byte) 1, invert);

    BinarySortableSerDe.serializeInt(output, v, invert);
  }

  /*
   * LONG.
   */
  @Override
  public void writeLong(long v) throws IOException {
    final boolean invert = columnSortOrderIsDesc[++index];

    // This field is not a null.
    BinarySortableSerDe.writeByte(output, (byte) 1, invert);

    BinarySortableSerDe.serializeLong(output, v, invert);
  }

  /*
   * FLOAT.
   */
  @Override
  public void writeFloat(float vf) throws IOException {
    final boolean invert = columnSortOrderIsDesc[++index];

    // This field is not a null.
    BinarySortableSerDe.writeByte(output, (byte) 1, invert);

    BinarySortableSerDe.serializeFloat(output, vf, invert);
  }

  /*
   * DOUBLE.
   */
  @Override
  public void writeDouble(double vd) throws IOException {
    final boolean invert = columnSortOrderIsDesc[++index];

    // This field is not a null.
    BinarySortableSerDe.writeByte(output, (byte) 1, invert);

    BinarySortableSerDe.serializeDouble(output, vd, invert);
  }

  /*
   * STRING.
   * 
   * Can be used to write CHAR and VARCHAR when the caller takes responsibility for
   * truncation/padding issues.
   */
  @Override
  public void writeString(byte[] v) throws IOException {
    final boolean invert = columnSortOrderIsDesc[++index];

    // This field is not a null.
    BinarySortableSerDe.writeByte(output, (byte) 1, invert);

    BinarySortableSerDe.serializeBytes(output, v, 0, v.length, invert);
  }

  @Override
  public void writeString(byte[] v, int start, int length) throws IOException {
    final boolean invert = columnSortOrderIsDesc[++index];

    // This field is not a null.
    BinarySortableSerDe.writeByte(output, (byte) 1, invert);

    BinarySortableSerDe.serializeBytes(output, v, start, length, invert);
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
    final boolean invert = columnSortOrderIsDesc[++index];

    // This field is not a null.
    BinarySortableSerDe.writeByte(output, (byte) 1, invert);

    BinarySortableSerDe.serializeBytes(output, v, 0, v.length, invert);
  }

  @Override
  public void writeBinary(byte[] v, int start, int length) {
    final boolean invert = columnSortOrderIsDesc[++index];

    // This field is not a null.
    BinarySortableSerDe.writeByte(output, (byte) 1, invert);

    BinarySortableSerDe.serializeBytes(output, v, start, length, invert);
  }

  /*
   * DATE.
   */
  @Override
  public void writeDate(Date date) throws IOException {
    final boolean invert = columnSortOrderIsDesc[++index];

    // This field is not a null.
    BinarySortableSerDe.writeByte(output, (byte) 1, invert);

    BinarySortableSerDe.serializeInt(output, DateWritable.dateToDays(date), invert);
  }

  // We provide a faster way to write a date without a Date object.
  @Override
  public void writeDate(int dateAsDays) throws IOException {
    final boolean invert = columnSortOrderIsDesc[++index];

    // This field is not a null.
    BinarySortableSerDe.writeByte(output, (byte) 1, invert);

    BinarySortableSerDe.serializeInt(output, dateAsDays, invert);
  }

  /*
   * TIMESTAMP.
   */
  @Override
  public void writeTimestamp(Timestamp vt) throws IOException {
    final boolean invert = columnSortOrderIsDesc[++index];

    // This field is not a null.
    BinarySortableSerDe.writeByte(output, (byte) 1, invert);

    tempTimestampWritable.set(vt);
    BinarySortableSerDe.serializeTimestampWritable(output, tempTimestampWritable, invert);
  }

  /*
   * INTERVAL_YEAR_MONTH.
   */
  @Override
  public void writeHiveIntervalYearMonth(HiveIntervalYearMonth viyt) throws IOException {
    final boolean invert = columnSortOrderIsDesc[++index];

    // This field is not a null.
    BinarySortableSerDe.writeByte(output, (byte) 1, invert);

    BinarySortableSerDe.serializeHiveIntervalYearMonth(output, viyt, invert);
  }

  @Override
  public void writeHiveIntervalYearMonth(int totalMonths) throws IOException {
    final boolean invert = columnSortOrderIsDesc[++index];

    // This field is not a null.
    BinarySortableSerDe.writeByte(output, (byte) 1, invert);

    BinarySortableSerDe.serializeInt(output, totalMonths, invert);
  }

  /*
   * INTERVAL_DAY_TIME.
   */
  @Override
  public void writeHiveIntervalDayTime(HiveIntervalDayTime vidt) throws IOException {
    final boolean invert = columnSortOrderIsDesc[++index];

    // This field is not a null.
    BinarySortableSerDe.writeByte(output, (byte) 1, invert);

    BinarySortableSerDe.serializeHiveIntervalDayTime(output, vidt, invert);
  }

  /*
   * DECIMAL.
   */
  @Override
  public void writeHiveDecimal(HiveDecimal dec) throws IOException {
    final boolean invert = columnSortOrderIsDesc[++index];

    // This field is not a null.
    BinarySortableSerDe.writeByte(output, (byte) 1, invert);

    BinarySortableSerDe.serializeHiveDecimal(output, dec, invert);
  }
}