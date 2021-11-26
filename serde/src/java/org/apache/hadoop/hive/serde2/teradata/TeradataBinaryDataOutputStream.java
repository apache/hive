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

package org.apache.hadoop.hive.serde2.teradata;

import org.apache.commons.io.EndianUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;

import static java.lang.String.join;
import static java.lang.String.format;


/**
 * The TeradataBinaryDataOutputStream is used to produce the output in compliance with the Teradata binary format,
 * so the output can be directly used to load into Teradata DB using TPT fastload.
 * Since the TD binary format uses little-endian to handle the SHORT, INT, LONG, DOUBLE and etc.
 * while the Hadoop uses big-endian,
 * We extend SwappedDataInputStream to return qualified bytes for these types and extend to handle the Teradata
 * specific types like VARCHAR, CHAR, TIMESTAMP, DATE...
 */
public class TeradataBinaryDataOutputStream extends ByteArrayOutputStream {

  private static final int TIMESTAMP_NO_NANOS_BYTE_NUM = 19;

  public TeradataBinaryDataOutputStream() {
  }

  /**
   * Write VARCHAR(N).
   * The representation of Varchar in Teradata binary format is:
   * the first two bytes represent the length N of this varchar field,
   * the next N bytes represent the content of this varchar field.
   * To pad the null varchar, the length will be 0 and the content will be none.
   *
   * @param writable the writable
   * @throws IOException the io exception
   */
  public void writeVarChar(HiveVarcharWritable writable) throws IOException {
    if (writable == null) {
      EndianUtils.writeSwappedShort(this, (short) 0);
      return;
    }
    Text t = writable.getTextValue();
    int varcharLength = t.getLength();
    EndianUtils.writeSwappedShort(this, (short) varcharLength); // write the varchar length
    write(t.getBytes(), 0, varcharLength); // write the varchar content
  }

  /**
   * Write INT.
   * using little-endian to write integer.
   *
   * @param i the
   * @throws IOException the io exception
   */
  public void writeInt(int i) throws IOException {
    EndianUtils.writeSwappedInteger(this, i);
  }

  /**
   * Write TIMESTAMP(N).
   * The representation of timestamp in Teradata binary format is:
   * the byte number to read is based on the precision of timestamp,
   * each byte represents one char and the timestamp is using string representation,
   * eg: for 1911-11-11 19:20:21.433200 in TIMESTAMP(3), we will cut it to be 1911-11-11 19:20:21.433 and write
   * 31 39  31 31 2d 31 31 2d 31 31 20 31 39 3a 32 30 3a 32 31 2e 34 33 33.
   * the null timestamp will use space to pad.
   *
   * @param timestamp the timestamp
   * @param byteNum the byte number the timestamp will write
   * @throws IOException the io exception
   */
  public void writeTimestamp(TimestampWritableV2 timestamp, int byteNum) throws IOException {
    if (timestamp == null) {
      String pad = join("", Collections.nCopies(byteNum, " "));
      write(pad.getBytes("UTF8"));
      return;
    }
    String sTimeStamp = timestamp.getTimestamp().toString();
    if (sTimeStamp.length() >= byteNum) {
      write(sTimeStamp.substring(0, byteNum).getBytes("UTF8"));
      return;
    }
    write(sTimeStamp.getBytes("UTF8"));
    String pad;
    if (sTimeStamp.length() == TIMESTAMP_NO_NANOS_BYTE_NUM) {
      pad = "." + join("", Collections.nCopies(byteNum - sTimeStamp.length() - 1, "0"));
    } else {
      pad = join("", Collections.nCopies(byteNum - sTimeStamp.length(), "0"));
    }
    write(pad.getBytes("UTF8"));
  }

  /**
   * Write DOUBLE.
   * using little-endian to write double.
   *
   * @param d the d
   * @throws IOException the io exception
   */
  public void writeDouble(double d) throws IOException {
    EndianUtils.writeSwappedDouble(this, d);
  }

  /**
   * Write DATE.
   * The representation of date in Teradata binary format is:
   * The Date D is a int with 4 bytes using little endian.
   * The representation is (YYYYMMDD - 19000000).toInt -&gt; D
   * eg. 1911.11.11 -&gt; 19111111 -&gt; 111111 -&gt; 07 b2 01 00 in little endian.
   * the null date will use 0 to pad.
   *
   * @param date the date
   * @throws IOException the io exception
   */
  public void writeDate(DateWritableV2 date) throws IOException {
    if (date == null) {
      EndianUtils.writeSwappedInteger(this, 0);
      return;
    }
    int toWrite = date.get().getYear() * 10000 + date.get().getMonth() * 100 + date.get().getDay() - 19000000;
    EndianUtils.writeSwappedInteger(this, toWrite);
  }

  /**
   * Write LONG.
   * using little-endian to write double.
   *
   * @param l the l
   * @throws IOException the io exception
   */
  public void writeLong(long l) throws IOException {
    EndianUtils.writeSwappedLong(this, l);
  }

  /**
   * Write CHAR(N).
   * The representation of char in Teradata binary format is:
   * the byte number to read is based on the [charLength] * [bytePerChar] &lt;- totalLength,
   * bytePerChar is decided by the charset: LATIN charset is 2 bytes per char and UNICODE charset is 3 bytes per char.
   * the null char will use space to pad.
   *
   * @param writable the writable
   * @param length the byte n
   * @throws IOException the io exception
   */
  public void writeChar(HiveCharWritable writable, int length) throws IOException {
    if (writable == null) {
      String pad = join("", Collections.nCopies(length, " "));
      write(pad.getBytes("UTF8"));
      return;
    }
    Text t = writable.getStrippedValue();
    int contentLength = t.getLength();
    write(t.getBytes(), 0, contentLength);
    if (length - contentLength < 0) {
      throw new IOException(format("The byte num %s of HiveCharWritable is more than the byte num %s we can hold. "
          + "The content of HiveCharWritable is %s", contentLength, length, writable.getPaddedValue()));
    }
    if (length > contentLength) {
      String pad = join("", Collections.nCopies(length - contentLength, " "));
      write(pad.getBytes("UTF8"));
    }
  }

  /**
   * Write DECIMAL(P, S).
   * The representation of decimal in Teradata binary format is:
   * the byte number to read is decided solely by the precision(P),
   * HiveDecimal is constructed through the byte array and scale.
   * the rest of byte will use 0x00 to pad (positive) and use 0xFF to pad (negative).
   * the null DECIMAL will use 0x00 to pad.
   *
   * @param writable the writable
   * @param byteNum the byte num
   * @throws IOException the io exception
   */
  public void writeDecimal(HiveDecimalWritable writable, int byteNum, int scale) throws IOException {
    if (writable == null) {
      byte[] pad = new byte[byteNum];
      write(pad);
      return;
    }
    // since the HiveDecimal will auto adjust the scale to save resource
    // we need to adjust it back otherwise the output bytes will be wrong
    int hiveScale = writable.getHiveDecimal().scale();
    BigInteger bigInteger = writable.getHiveDecimal().unscaledValue();
    if (hiveScale < scale) {
      BigInteger multiplicand = new BigInteger("1" + join("", Collections.nCopies(scale - hiveScale, "0")));
      bigInteger = bigInteger.multiply(multiplicand);
    }
    byte[] content = bigInteger.toByteArray();
    int signBit = content[0] >> 7 & 1;
    ArrayUtils.reverse(content);
    write(content);
    if (byteNum > content.length) {
      byte[] pad;
      if (signBit == 0) {
        pad = new byte[byteNum - content.length];
      } else {
        pad = new byte[byteNum - content.length];
        Arrays.fill(pad, (byte) 255);
      }
      write(pad);
    }
  }

  /**
   * Write SHORT.
   * using little-endian to write short.
   *
   * @param s the s
   * @throws IOException the io exception
   */
  public void writeShort(short s) throws IOException {
    EndianUtils.writeSwappedShort(this, s);
  }

  /**
   * Write VARBYTE(N).
   * The representation of VARBYTE in Teradata binary format is:
   * the first two bytes represent the length N of this varchar field,
   * the next N bytes represent the content of this varchar field.
   * To pad the null varbyte, the length will be 0 and the content will be none.
   *
   * @param writable the writable
   * @throws IOException the io exception
   */
  public void writeVarByte(BytesWritable writable) throws IOException {
    if (writable == null) {
      EndianUtils.writeSwappedShort(this, (short) 0);
      return;
    }
    int varbyteLength = writable.getLength();
    EndianUtils.writeSwappedShort(this, (short) varbyteLength); // write the varbyte length
    write(writable.getBytes(), 0, varbyteLength); // write the varchar content
  }
}
