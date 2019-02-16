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

import org.apache.commons.io.input.SwappedDataInputStream;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.text.ParseException;

import static java.lang.String.format;

/**
 * The TeradataBinaryDataInputStream is used to handle the Teradata binary format input for record.
 * Since the TD binary format uses little-endian to handle the SHORT, INT, LONG, DOUBLE and etc.
 * while the Hadoop uses big-endian,
 * We extend SwappedDataInputStream to handle these types and extend to handle the Teradata
 * specific types like VARCHAR, CHAR, TIMESTAMP, DATE...
 */
public class TeradataBinaryDataInputStream extends SwappedDataInputStream {

  private static final int DATE_STRING_LENGTH = 8;

  /**
   * Instantiates a new Teradata binary data input stream.
   *
   * @param input the input
   */
  public TeradataBinaryDataInputStream(InputStream input) {
    super(input);
  }

  /**
   * Read VARCHAR(N).
   * The representation of Varchar in Teradata binary format is:
   * the first two bytes represent the length N of this varchar field,
   * the next N bytes represent the content of this varchar field.
   * To pad the null varchar, the length will be 0 and the content will be none.
   *
   * @return the string
   * @throws IOException the io exception
   */
  public String readVarchar() throws IOException {
    int varcharLength = readUnsignedShort();
    byte[] varcharContent = new byte[varcharLength];
    int numOfBytesRead = in.read(varcharContent);
    if (varcharContent.length != 0 && numOfBytesRead != varcharLength) {
      throw new EOFException(
          format("Fail to read the varchar. Expect %d bytes, get %d bytes", varcharLength, numOfBytesRead));
    }
    //force it to be UTF8 string
    return new String(varcharContent, "UTF8");
  }

  /**
   * Read TIMESTAMP(P).
   * The representation of timestamp in Teradata binary format is:
   * the byte number to read is based on the precision of timestamp,
   * each byte represents one char and the timestamp is using string representation,
   * eg: for TIMESTAMP(6), we need to read 26 bytes
   * 31 39  31 31 2d 31 31 2d 31 31 20 31 39 3a 32 30 3a 32 31 2e 34 33 33 32 30 30
   * will represent 1911-11-11 19:20:21.433200.
   * the null timestamp will use space to pad.
   *
   * @param byteNum the byte number that will be read from inputstream
   * @return the timestamp
   * @throws IOException the io exception
   */
  public Timestamp readTimestamp(Integer byteNum) throws IOException {
    // yyyy-mm-dd hh:mm:ss
    byte[] timestampContent = new byte[byteNum];
    int numOfBytesRead = in.read(timestampContent);
    if (timestampContent.length != 0 && numOfBytesRead != byteNum) {
      throw new EOFException(
          format("Fail to read the timestamp. Expect %d bytes, get %d bytes", byteNum, numOfBytesRead));
    }
    String timestampStr = new String(timestampContent, "UTF8");
    if (timestampStr.trim().length() == 0) {
      return null;
    }
    return Timestamp.valueOf(timestampStr);
  }

  /**
   * Read DATE.
   * The representation of date in Teradata binary format is:
   * The Date D is a int with 4 bytes using little endian,
   * The representation is (D+19000000).ToString -> YYYYMMDD,
   * eg: Date 07 b2 01 00 -> 111111 in little endian -> 19111111 - > 1911.11.11.
   * the null date will use 0 to pad.
   *
   * @return the date
   * @throws IOException the io exception
   * @throws ParseException the parse exception
   */
  public Date readDate() throws IOException, ParseException {
    int di = readInt();
    if (di == 0) {
      return null;
    }
    String dateString = String.valueOf(di + 19000000);
    if (dateString.length() < DATE_STRING_LENGTH) {
      dateString = StringUtils.leftPad(dateString, DATE_STRING_LENGTH, '0');
    }
    Date date = new Date();
    date.setYear(Integer.parseInt(dateString.substring(0, 4)));
    date.setMonth(Integer.parseInt(dateString.substring(4, 6)));
    date.setDayOfMonth(Integer.parseInt(dateString.substring(6, 8)));
    return date;
  }

  /**
   * Read CHAR(N).
   * The representation of char in Teradata binary format is
   * the byte number to read is based on the [charLength] * [bytePerChar] <- totalLength,
   * bytePerChar is decided by the charset: LATAIN charset is 2 bytes per char and UNICODE charset is 3 bytes per char.
   * the null char will use space to pad.
   *
   * @param totalLength the total length
   * @return the string
   * @throws IOException the io exception
   */
  public String readChar(int totalLength) throws IOException {
    byte[] charContent = new byte[totalLength];
    int numOfBytesRead = in.read(charContent);
    if (charContent.length != 0 && numOfBytesRead != totalLength) {
      throw new EOFException(
          format("Fail to read the varchar. Expect %d bytes, get %d bytes", totalLength, numOfBytesRead));
    }
    return new String(charContent, "UTF8");
  }

  /**
   * Read DECIMAL(P, S).
   * The representation of decimal in Teradata binary format is
   * the byte number to read is decided solely by the precision(P),
   * HiveDecimal is constructed through the byte array and scale.
   * the null DECIMAL will use 0x00 to pad.
   *
   * @param scale the scale
   * @param byteNum the byte num
   * @return the hive decimal
   * @throws IOException the io exception
   */
  public HiveDecimal readDecimal(int scale, int byteNum) throws IOException {
    byte[] decimalContent = new byte[byteNum];
    int numOfBytesRead = in.read(decimalContent);
    if (decimalContent.length != 0 && numOfBytesRead != byteNum) {
      throw new EOFException(
          format("Fail to read the decimal. Expect %d bytes, get %d bytes", byteNum, numOfBytesRead));
    }
    ArrayUtils.reverse(decimalContent);
    return HiveDecimal.create(new BigInteger(decimalContent), scale);
  }

  /**
   * Read VARBYTE(N).
   * The representation of VARBYTE in Teradata binary format is:
   * the first two bytes represent the length N of this varchar field
   * the next N bytes represent the content of this varchar field.
   * To pad the null varbyte, the length will be 0 and the content will be none.
   *
   * @return the byte [ ]
   * @throws IOException the io exception
   */
  public byte[] readVarbyte() throws IOException {
    int varbyteLength = readUnsignedShort();
    byte[] varbyteContent = new byte[varbyteLength];
    int numOfBytesRead = in.read(varbyteContent);
    if (varbyteContent.length != 0 && numOfBytesRead != varbyteLength) {
      throw new EOFException(
          format("Fail to read the varbyte. Expect %d bytes, get %d bytes", varbyteLength, numOfBytesRead));
    }
    return varbyteContent;
  }
}
