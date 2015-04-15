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
import org.apache.hadoop.hive.serde2.binarysortable.InputByteBuffer;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveIntervalDayTime;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveIntervalYearMonth;
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
   * Set the buffer that will receive the serialized data.
   */
  @Override
  public void set(Output output) {
    this.output = output;
    this.output.reset();
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

    BinarySortableSerDe.writeByte(output, (byte) ((v >> 8) ^ 0x80), invert);
    BinarySortableSerDe.writeByte(output, (byte) v, invert);
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

    BinarySortableSerDe.writeByte(output, (byte) ((v >> 56) ^ 0x80), invert);
    BinarySortableSerDe.writeByte(output, (byte) (v >> 48), invert);
    BinarySortableSerDe.writeByte(output, (byte) (v >> 40), invert);
    BinarySortableSerDe.writeByte(output, (byte) (v >> 32), invert);
    BinarySortableSerDe.writeByte(output, (byte) (v >> 24), invert);
    BinarySortableSerDe.writeByte(output, (byte) (v >> 16), invert);
    BinarySortableSerDe.writeByte(output, (byte) (v >> 8), invert);
    BinarySortableSerDe.writeByte(output, (byte) v, invert);

  }

  /*
   * FLOAT.
   */
  @Override
  public void writeFloat(float vf) throws IOException {
    final boolean invert = columnSortOrderIsDesc[++index];

    // This field is not a null.
    BinarySortableSerDe.writeByte(output, (byte) 1, invert);

    int v = Float.floatToIntBits(vf);
    if ((v & (1 << 31)) != 0) {
      // negative number, flip all bits
      v = ~v;
    } else {
      // positive number, flip the first bit
      v = v ^ (1 << 31);
    }
    BinarySortableSerDe.writeByte(output, (byte) (v >> 24), invert);
    BinarySortableSerDe.writeByte(output, (byte) (v >> 16), invert);
    BinarySortableSerDe.writeByte(output, (byte) (v >> 8), invert);
    BinarySortableSerDe.writeByte(output, (byte) v, invert);
  }

  /*
   * DOUBLE.
   */
  @Override
  public void writeDouble(double vd) throws IOException {
    final boolean invert = columnSortOrderIsDesc[++index];

    // This field is not a null.
    BinarySortableSerDe.writeByte(output, (byte) 1, invert);

    long v = Double.doubleToLongBits(vd);
    if ((v & (1L << 63)) != 0) {
      // negative number, flip all bits
      v = ~v;
    } else {
      // positive number, flip the first bit
      v = v ^ (1L << 63);
    }
    BinarySortableSerDe.writeByte(output, (byte) (v >> 56), invert);
    BinarySortableSerDe.writeByte(output, (byte) (v >> 48), invert);
    BinarySortableSerDe.writeByte(output, (byte) (v >> 40), invert);
    BinarySortableSerDe.writeByte(output, (byte) (v >> 32), invert);
    BinarySortableSerDe.writeByte(output, (byte) (v >> 24), invert);
    BinarySortableSerDe.writeByte(output, (byte) (v >> 16), invert);
    BinarySortableSerDe.writeByte(output, (byte) (v >> 8), invert);
    BinarySortableSerDe.writeByte(output, (byte) v, invert);
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
    byte[] data = tempTimestampWritable.getBinarySortable();
    for (int i = 0; i < data.length; i++) {
      BinarySortableSerDe.writeByte(output, data[i], invert);
    }
  }

  /*
   * INTERVAL_YEAR_MONTH.
   */
  @Override
  public void writeHiveIntervalYearMonth(HiveIntervalYearMonth viyt) throws IOException {
    final boolean invert = columnSortOrderIsDesc[++index];

    // This field is not a null.
    BinarySortableSerDe.writeByte(output, (byte) 1, invert);

    int totalMonths = viyt.getTotalMonths();
    BinarySortableSerDe.serializeInt(output, totalMonths, invert);
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

    long totalSecs = vidt.getTotalSeconds();
    int nanos = vidt.getNanos();
    BinarySortableSerDe.serializeLong(output, totalSecs, invert);
    BinarySortableSerDe.serializeInt(output, nanos, invert);
  }

  @Override
  public void writeHiveIntervalDayTime(long totalNanos) throws IOException {
    final boolean invert = columnSortOrderIsDesc[++index];

    // This field is not a null.
    BinarySortableSerDe.writeByte(output, (byte) 1, invert);

    long totalSecs = DateUtils.getIntervalDayTimeTotalSecondsFromTotalNanos(totalNanos);
    int nanos = DateUtils.getIntervalDayTimeNanosFromTotalNanos(totalNanos);
    BinarySortableSerDe.serializeLong(output, totalSecs, invert);
    BinarySortableSerDe.serializeInt(output, nanos, invert);
  }

  /*
   * DECIMAL.
   */
  @Override
  public void writeHiveDecimal(HiveDecimal dec) throws IOException {
    final boolean invert = columnSortOrderIsDesc[++index];

    // This field is not a null.
    BinarySortableSerDe.writeByte(output, (byte) 1, invert);

    // decimals are encoded in three pieces:
    // sign: 1, 2 or 3 for smaller, equal or larger than 0 respectively
    // factor: Number that indicates the amount of digits you have to move
    // the decimal point left or right until the resulting number is smaller
    // than zero but has something other than 0 as the first digit.
    // digits: which is a string of all the digits in the decimal. If the number
    // is negative the binary string will be inverted to get the correct ordering.
    // Example: 0.00123
    // Sign is 3 (bigger than 0)
    // Factor is -2 (move decimal point 2 positions right)
    // Digits are: 123

    // get the sign of the big decimal
    int sign = dec.compareTo(HiveDecimal.ZERO);

    // we'll encode the absolute value (sign is separate)
    dec = dec.abs();

    // get the scale factor to turn big decimal into a decimal < 1
    int factor = dec.precision() - dec.scale();
    factor = sign == 1 ? factor : -factor;

    // convert the absolute big decimal to string
    dec.scaleByPowerOfTen(Math.abs(dec.scale()));
    String digits = dec.unscaledValue().toString();

    // finally write out the pieces (sign, scale, digits)
    BinarySortableSerDe.writeByte(output, (byte) ( sign + 1), invert);
    BinarySortableSerDe.writeByte(output, (byte) ((factor >> 24) ^ 0x80), invert);
    BinarySortableSerDe.writeByte(output, (byte) ( factor >> 16), invert);
    BinarySortableSerDe.writeByte(output, (byte) ( factor >> 8), invert);
    BinarySortableSerDe.writeByte(output, (byte)   factor, invert);
    BinarySortableSerDe.serializeBytes(output, digits.getBytes(BinarySortableSerDe.decimalCharSet),
        digits.length(), sign == -1 ? !invert : invert);
  }
}