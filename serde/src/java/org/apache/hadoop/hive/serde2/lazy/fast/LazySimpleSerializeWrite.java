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

package org.apache.hadoop.hive.serde2.lazy.fast;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
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

/*
 * Directly serialize, field-by-field, the LazyBinary format.
*
 * This is an alternative way to serialize than what is provided by LazyBinarySerDe.
  */
public final class LazySimpleSerializeWrite implements SerializeWrite {
  public static final Logger LOG = LoggerFactory.getLogger(LazySimpleSerializeWrite.class.getName());

  private LazySerDeParameters lazyParams;

  private byte[] separators;
  private boolean[] needsEscape;
  private boolean isEscaped;
  private byte escapeChar;
  private byte[] nullSequenceBytes;

  private Output output;

  private int fieldCount;
  private int index;
  private int currentLevel;
  private Deque<Integer> indexStack = new ArrayDeque<Integer>();

  // For thread safety, we allocate private writable objects for our use only.
  private DateWritable dateWritable;
  private TimestampWritable timestampWritable;
  private HiveIntervalYearMonthWritable hiveIntervalYearMonthWritable;
  private HiveIntervalDayTimeWritable hiveIntervalDayTimeWritable;
  private HiveIntervalDayTime hiveIntervalDayTime;
  private HiveDecimalWritable hiveDecimalWritable;
  private byte[] decimalScratchBuffer;

  public LazySimpleSerializeWrite(int fieldCount,
    LazySerDeParameters lazyParams) {

    this();
    this.fieldCount = fieldCount;

    this.lazyParams = lazyParams;

    separators = lazyParams.getSeparators();
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
    currentLevel = 0;
  }

  /*
   * Set the buffer that will receive the serialized data.  The output buffer will NOT be reset.
   */
  @Override
  public void setAppend(Output output) {
    this.output = output;
    index = 0;
    currentLevel = 0;
  }

  /*
   * Reset the previously supplied buffer that will receive the serialized data.
   */
  @Override
  public void reset() {
    output.reset();
    index = 0;
    currentLevel = 0;
  }

  /*
   * Write a NULL field.
   */
  @Override
  public void writeNull() throws IOException {
    beginPrimitive();

    output.write(nullSequenceBytes);

    finishPrimitive();
  }

  /*
   * BOOLEAN.
   */
  @Override
  public void writeBoolean(boolean v) throws IOException {
    beginPrimitive();
    if (v) {
      output.write(LazyUtils.trueBytes, 0, LazyUtils.trueBytes.length);
    } else {
      output.write(LazyUtils.falseBytes, 0, LazyUtils.falseBytes.length);
    }
    finishPrimitive();
  }

  /*
   * BYTE.
   */
  @Override
  public void writeByte(byte v) throws IOException {
    beginPrimitive();
    LazyInteger.writeUTF8(output, v);
    finishPrimitive();
  }

  /*
   * SHORT.
   */
  @Override
  public void writeShort(short v) throws IOException {
    beginPrimitive();
    LazyInteger.writeUTF8(output, v);
    finishPrimitive();
  }

  /*
   * INT.
   */
  @Override
  public void writeInt(int v) throws IOException {
    beginPrimitive();
    LazyInteger.writeUTF8(output, v);
    finishPrimitive();
  }

  /*
   * LONG.
   */
  @Override
  public void writeLong(long v) throws IOException {
    beginPrimitive();
    LazyLong.writeUTF8(output, v);
    finishPrimitive();
  }

  /*
   * FLOAT.
   */
  @Override
  public void writeFloat(float vf) throws IOException {
    beginPrimitive();
    ByteBuffer b = Text.encode(String.valueOf(vf));
    output.write(b.array(), 0, b.limit());
    finishPrimitive();
  }

  /*
   * DOUBLE.
   */
  @Override
  public void writeDouble(double v) throws IOException  {
    beginPrimitive();
    ByteBuffer b = Text.encode(String.valueOf(v));
    output.write(b.array(), 0, b.limit());
    finishPrimitive();
  }

  /*
   * STRING.
   *
   * Can be used to write CHAR and VARCHAR when the caller takes responsibility for
   * truncation/padding issues.
   */
  @Override
  public void writeString(byte[] v) throws IOException  {
    beginPrimitive();
    LazyUtils.writeEscaped(output, v, 0, v.length, isEscaped, escapeChar,
        needsEscape);
    finishPrimitive();
  }

  @Override
  public void writeString(byte[] v, int start, int length) throws IOException {
    beginPrimitive();
    LazyUtils.writeEscaped(output, v, start, length, isEscaped, escapeChar,
        needsEscape);
    finishPrimitive();
  }

  /*
   * CHAR.
   */
  @Override
  public void writeHiveChar(HiveChar hiveChar) throws IOException {
    beginPrimitive();
    ByteBuffer b = Text.encode(hiveChar.getPaddedValue());
    LazyUtils.writeEscaped(output, b.array(), 0, b.limit(), isEscaped, escapeChar,
        needsEscape);
    finishPrimitive();
  }

  /*
   * VARCHAR.
   */
  @Override
  public void writeHiveVarchar(HiveVarchar hiveVarchar) throws IOException {
    beginPrimitive();
    ByteBuffer b = Text.encode(hiveVarchar.getValue());
    LazyUtils.writeEscaped(output, b.array(), 0, b.limit(), isEscaped, escapeChar,
        needsEscape);
    finishPrimitive();
  }

  /*
   * BINARY.
   */
  @Override
  public void writeBinary(byte[] v) throws IOException {
    beginPrimitive();
    byte[] toEncode = new byte[v.length];
    System.arraycopy(v, 0, toEncode, 0, v.length);
    byte[] toWrite = Base64.encodeBase64(toEncode);
    output.write(toWrite, 0, toWrite.length);
    finishPrimitive();
  }

  @Override
  public void writeBinary(byte[] v, int start, int length) throws IOException {
    beginPrimitive();
    byte[] toEncode = new byte[length];
    System.arraycopy(v, start, toEncode, 0, length);
    byte[] toWrite = Base64.encodeBase64(toEncode);
    output.write(toWrite, 0, toWrite.length);
    finishPrimitive();
  }

  /*
   * DATE.
   */
  @Override
  public void writeDate(Date date) throws IOException {
    beginPrimitive();
    if (dateWritable == null) {
      dateWritable = new DateWritable();
    }
    dateWritable.set(date);
    LazyDate.writeUTF8(output, dateWritable);
    finishPrimitive();
  }

  // We provide a faster way to write a date without a Date object.
  @Override
  public void writeDate(int dateAsDays) throws IOException {
    beginPrimitive();
    if (dateWritable == null) {
      dateWritable = new DateWritable();
    }
    dateWritable.set(dateAsDays);
    LazyDate.writeUTF8(output, dateWritable);
    finishPrimitive();
  }

  /*
   * TIMESTAMP.
   */
  @Override
  public void writeTimestamp(Timestamp v) throws IOException {
    beginPrimitive();
    if (timestampWritable == null) {
      timestampWritable = new TimestampWritable();
    }
    timestampWritable.set(v);
    LazyTimestamp.writeUTF8(output, timestampWritable);
    finishPrimitive();
  }

  /*
   * INTERVAL_YEAR_MONTH.
   */
  @Override
  public void writeHiveIntervalYearMonth(HiveIntervalYearMonth viyt) throws IOException {
    beginPrimitive();
    if (hiveIntervalYearMonthWritable == null) {
      hiveIntervalYearMonthWritable = new HiveIntervalYearMonthWritable();
    }
    hiveIntervalYearMonthWritable.set(viyt);
    LazyHiveIntervalYearMonth.writeUTF8(output, hiveIntervalYearMonthWritable);
    finishPrimitive();
  }


  @Override
  public void writeHiveIntervalYearMonth(int totalMonths) throws IOException {
    beginPrimitive();
    if (hiveIntervalYearMonthWritable == null) {
      hiveIntervalYearMonthWritable = new HiveIntervalYearMonthWritable();
    }
    hiveIntervalYearMonthWritable.set(totalMonths);
    LazyHiveIntervalYearMonth.writeUTF8(output, hiveIntervalYearMonthWritable);
    finishPrimitive();
  }

  /*
   * INTERVAL_DAY_TIME.
   */
  @Override
  public void writeHiveIntervalDayTime(HiveIntervalDayTime vidt) throws IOException {
    beginPrimitive();
    if (hiveIntervalDayTimeWritable == null) {
      hiveIntervalDayTimeWritable = new HiveIntervalDayTimeWritable();
    }
    hiveIntervalDayTimeWritable.set(vidt);
    LazyHiveIntervalDayTime.writeUTF8(output, hiveIntervalDayTimeWritable);
    finishPrimitive();
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
    beginPrimitive();
    if (decimalScratchBuffer == null) {
      decimalScratchBuffer = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_TO_BYTES];
    }
    LazyHiveDecimal.writeUTF8(output, dec, scale, decimalScratchBuffer);
    finishPrimitive();
  }

  @Override
  public void writeHiveDecimal(HiveDecimalWritable decWritable, int scale) throws IOException {
    beginPrimitive();
    if (decimalScratchBuffer == null) {
      decimalScratchBuffer = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_TO_BYTES];
    }
    LazyHiveDecimal.writeUTF8(output, decWritable, scale, decimalScratchBuffer);
    finishPrimitive();
  }

  private void beginComplex() {
    if (index > 0) {
      output.write(separators[currentLevel]);
    }
    indexStack.push(index);

    // Always use index 0 so the write methods don't write a separator.
    index = 0;

    // Set "global" separator member to next level.
    currentLevel++;
  }

  private void finishComplex() {
    currentLevel--;
    index = indexStack.pop();
    index++;
  }

  @Override
  public void beginList(List list) {
    beginComplex();
  }

  @Override
  public void separateList() {
  }

  @Override
  public void finishList() {
    finishComplex();
  }

  @Override
  public void beginMap(Map<?, ?> map) {
    beginComplex();

    // MAP requires 2 levels: key separator and key-pair separator.
    currentLevel++;
  }

  @Override
  public void separateKey() {
    index = 0;
    output.write(separators[currentLevel]);
  }

  @Override
  public void separateKeyValuePair() {
    index = 0;
    output.write(separators[currentLevel - 1]);
  }

  @Override
  public void finishMap() {
    // Remove MAP extra level.
    currentLevel--;

    finishComplex();
  }

  @Override
  public void beginStruct(List fieldValues) {
    beginComplex();
  }

  @Override
  public void separateStruct() {
  }

  @Override
  public void finishStruct() {
    finishComplex();
  }

  @Override
  public void beginUnion(int tag) throws IOException {
    beginComplex();
    writeInt(tag);
    output.write(separators[currentLevel]);
    index = 0;
  }

  @Override
  public void finishUnion() {
    finishComplex();
  }

  private void beginPrimitive() {
    if (index > 0) {
      output.write(separators[currentLevel]);
    }
  }

  private void finishPrimitive() {
    index++;
  }
}
