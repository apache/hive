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
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyDate;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveDecimal;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveIntervalDayTime;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveIntervalYearMonth;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazyTimestamp;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.fast.SerializeWrite;
import org.apache.hadoop.io.Text;
import org.apache.hive.common.util.DateUtils;

/*
 * Directly serialize, field-by-field, the LazyBinary format.
*
 * This is an alternative way to serialize than what is provided by LazyBinarySerDe.
  */
public class LazySimpleSerializeWrite implements SerializeWrite {
  public static final Log LOG = LogFactory.getLog(LazySimpleSerializeWrite.class.getName());

  private LazySerDeParameters lazyParams;

  private byte separator;
  private boolean[] needsEscape;
  private boolean isEscaped;
  private byte escapeChar;
  private byte[] nullSequenceBytes;

  private Output output;

  private int fieldCount;
  private int index;

  // For thread safety, we allocate private writable objects for our use only.
  private DateWritable dateWritable;
  private TimestampWritable timestampWritable;
  private HiveIntervalYearMonthWritable hiveIntervalYearMonthWritable;
  private HiveIntervalDayTimeWritable hiveIntervalDayTimeWritable;
  private HiveIntervalDayTime hiveIntervalDayTime;

  public LazySimpleSerializeWrite(int fieldCount,
    byte separator, LazySerDeParameters lazyParams) {

    this();
    this.fieldCount = fieldCount;
  
    this.separator = separator;
    this.lazyParams = lazyParams;

    isEscaped = lazyParams.isEscaped();
    escapeChar = lazyParams.getEscapeChar();
    needsEscape = lazyParams.getNeedsEscape();
    nullSequenceBytes = lazyParams.getNullSequence().getBytes();
  }

  // Not public since we must have the field count and other information.
  private LazySimpleSerializeWrite() {
  }

  /*
   * Set the buffer that will receive the serialized data.  The output buffer will be reset.
   */
  @Override
  public void set(Output output) {
    this.output = output;
    output.reset();
    index = 0;
  }

  /*
   * Set the buffer that will receive the serialized data.  The output buffer will NOT be reset.
   */
  @Override
  public void setAppend(Output output) {
    this.output = output;
    index = 0;
  }

  /*
   * Reset the previously supplied buffer that will receive the serialized data.
   */
  @Override
  public void reset() {
    output.reset();
    index = 0;
  }

  /*
   * General Pattern:
   *
   *  if (index > 0) {
   *    output.write(separator);
   *  }
   *
   *  WHEN NOT NULL: Write value.
   *  OTHERWISE NULL: Write nullSequenceBytes.
   *
   *  Increment index
   *
   */

  /*
   * Write a NULL field.
   */
  @Override
  public void writeNull() throws IOException {

    if (index > 0) {
      output.write(separator);
    }

    output.write(nullSequenceBytes);

    index++;
  }

  /*
   * BOOLEAN.
   */
  @Override
  public void writeBoolean(boolean v) throws IOException {

    if (index > 0) {
      output.write(separator);
    }

    if (v) {
      output.write(LazyUtils.trueBytes, 0, LazyUtils.trueBytes.length);
    } else {
      output.write(LazyUtils.falseBytes, 0, LazyUtils.falseBytes.length);
    }

    index++;
  }

  /*
   * BYTE.
   */
  @Override
  public void writeByte(byte v) throws IOException {

    if (index > 0) {
      output.write(separator);
    }

    LazyInteger.writeUTF8(output, v);

    index++;
  }

  /*
   * SHORT.
   */
  @Override
  public void writeShort(short v) throws IOException {

    if (index > 0) {
      output.write(separator);
    }

    LazyInteger.writeUTF8(output, v);

    index++;
  }

  /*
   * INT.
   */
  @Override
  public void writeInt(int v) throws IOException {

    if (index > 0) {
      output.write(separator);
    }

    LazyInteger.writeUTF8(output, v);

    index++;
  }

  /*
   * LONG.
   */
  @Override
  public void writeLong(long v) throws IOException {

    if (index > 0) {
      output.write(separator);
    }

    LazyLong.writeUTF8(output, v);

    index++;
  }

  /*
   * FLOAT.
   */
  @Override
  public void writeFloat(float vf) throws IOException {

    if (index > 0) {
      output.write(separator);
    }

    ByteBuffer b = Text.encode(String.valueOf(vf));
    output.write(b.array(), 0, b.limit());

    index++;
  }

  /*
   * DOUBLE.
   */
  @Override
  public void writeDouble(double v) throws IOException  {

    if (index > 0) {
      output.write(separator);
    }

    ByteBuffer b = Text.encode(String.valueOf(v));
    output.write(b.array(), 0, b.limit());

    index++;
  }

  /*
   * STRING.
   * 
   * Can be used to write CHAR and VARCHAR when the caller takes responsibility for
   * truncation/padding issues.
   */
  @Override
  public void writeString(byte[] v) throws IOException  {

    if (index > 0) {
      output.write(separator);
    }

    LazyUtils.writeEscaped(output, v, 0, v.length, isEscaped, escapeChar,
        needsEscape);

    index++;
  }

  @Override
  public void writeString(byte[] v, int start, int length) throws IOException {

    if (index > 0) {
      output.write(separator);
    }

    LazyUtils.writeEscaped(output, v, start, length, isEscaped, escapeChar,
        needsEscape);

    index++;
  }

  /*
   * CHAR.
   */
  @Override
  public void writeHiveChar(HiveChar hiveChar) throws IOException {

    if (index > 0) {
      output.write(separator);
    }

    ByteBuffer b = Text.encode(hiveChar.getPaddedValue());
    LazyUtils.writeEscaped(output, b.array(), 0, b.limit(), isEscaped, escapeChar,
        needsEscape);

    index++;
  }

  /*
   * VARCHAR.
   */
  @Override
  public void writeHiveVarchar(HiveVarchar hiveVarchar) throws IOException {

    if (index > 0) {
      output.write(separator);
    }

    ByteBuffer b = Text.encode(hiveVarchar.getValue());
    LazyUtils.writeEscaped(output, b.array(), 0, b.limit(), isEscaped, escapeChar,
        needsEscape);

    index++;
  }

  /*
   * BINARY.
   */
  @Override
  public void writeBinary(byte[] v) throws IOException {

    if (index > 0) {
      output.write(separator);
    }

    byte[] toEncode = new byte[v.length];
    System.arraycopy(v, 0, toEncode, 0, v.length);
    byte[] toWrite = Base64.encodeBase64(toEncode);
    output.write(toWrite, 0, toWrite.length);

    index++;
  }

  @Override
  public void writeBinary(byte[] v, int start, int length) throws IOException {

    if (index > 0) {
      output.write(separator);
    }

    byte[] toEncode = new byte[length];
    System.arraycopy(v, start, toEncode, 0, length);
    byte[] toWrite = Base64.encodeBase64(toEncode);
    output.write(toWrite, 0, toWrite.length);

    index++;
  }

  /*
   * DATE.
   */
  @Override
  public void writeDate(Date date) throws IOException {

    if (index > 0) {
      output.write(separator);
    }

    if (dateWritable == null) {
      dateWritable = new DateWritable();
    }
    dateWritable.set(date);
    LazyDate.writeUTF8(output, dateWritable);

    index++;
  }

  // We provide a faster way to write a date without a Date object.
  @Override
  public void writeDate(int dateAsDays) throws IOException {

    if (index > 0) {
      output.write(separator);
    }

    if (dateWritable == null) {
      dateWritable = new DateWritable();
    }
    dateWritable.set(dateAsDays);
    LazyDate.writeUTF8(output, dateWritable);

    index++;
  }

  /*
   * TIMESTAMP.
   */
  @Override
  public void writeTimestamp(Timestamp v) throws IOException {

    if (index > 0) {
      output.write(separator);
    }

    if (timestampWritable == null) {
      timestampWritable = new TimestampWritable();
    }
    timestampWritable.set(v);
    LazyTimestamp.writeUTF8(output, timestampWritable);

    index++;
  }

  /*
   * INTERVAL_YEAR_MONTH.
   */
  @Override
  public void writeHiveIntervalYearMonth(HiveIntervalYearMonth viyt) throws IOException {

    if (index > 0) {
      output.write(separator);
    }

    if (hiveIntervalYearMonthWritable == null) {
      hiveIntervalYearMonthWritable = new HiveIntervalYearMonthWritable();
    }
    hiveIntervalYearMonthWritable.set(viyt);
    LazyHiveIntervalYearMonth.writeUTF8(output, hiveIntervalYearMonthWritable);

    index++;
  }


  @Override
  public void writeHiveIntervalYearMonth(int totalMonths) throws IOException {

    if (index > 0) {
      output.write(separator);
    }

    if (hiveIntervalYearMonthWritable == null) {
      hiveIntervalYearMonthWritable = new HiveIntervalYearMonthWritable();
    }
    hiveIntervalYearMonthWritable.set(totalMonths);
    LazyHiveIntervalYearMonth.writeUTF8(output, hiveIntervalYearMonthWritable);

    index++;
  }

  /*
   * INTERVAL_DAY_TIME.
   */
  @Override
  public void writeHiveIntervalDayTime(HiveIntervalDayTime vidt) throws IOException {

    if (index > 0) {
      output.write(separator);
    }

    if (hiveIntervalDayTimeWritable == null) {
      hiveIntervalDayTimeWritable = new HiveIntervalDayTimeWritable();
    }
    hiveIntervalDayTimeWritable.set(vidt);
    LazyHiveIntervalDayTime.writeUTF8(output, hiveIntervalDayTimeWritable);

    index++;
  }

  /*
   * DECIMAL.
   */
  @Override
  public void writeHiveDecimal(HiveDecimal v) throws IOException {

    if (index > 0) {
      output.write(separator);
    }

    LazyHiveDecimal.writeUTF8(output, v);

    index++;
  }
}
