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

package org.apache.hadoop.hive.serde2.fast;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.ByteStream.Output;

/*
 * Directly serialize with the caller writing field-by-field a serialization format.
 *
 * The caller is responsible for calling the write method for the right type of each field
 * (or calling writeNull if the field is a NULL).
 *
 */
public interface SerializeWrite {

  /*
   * Set the buffer that will receive the serialized data.  The output buffer will be reset.
   */
  void set(Output output);

  /*
   * Set the buffer that will receive the serialized data.  The output buffer will NOT be reset.
   */
  void setAppend(Output output);


  /*
   * Reset the previously supplied buffer that will receive the serialized data.
   */
  void reset();

  /*
   * Write a NULL field.
   */
  void writeNull() throws IOException;

  /*
   * BOOLEAN.
   */
  void writeBoolean(boolean v) throws IOException;

  /*
   * BYTE.
   */
  void writeByte(byte v) throws IOException;

  /*
   * SHORT.
   */
  void writeShort(short v) throws IOException;

  /*
   * INT.
   */
  void writeInt(int v) throws IOException;

  /*
   * LONG.
   */
  void writeLong(long v) throws IOException;

  /*
   * FLOAT.
   */
  void writeFloat(float vf) throws IOException;

  /*
   * DOUBLE.
   */
  void writeDouble(double vd) throws IOException;

  /*
   * STRING.
   *
   * Can be used to write CHAR and VARCHAR when the caller takes responsibility for
   * truncation/padding issues.
   */
  void writeString(byte[] v) throws IOException;
  void writeString(byte[] v, int start, int length) throws IOException;

  /*
   * CHAR.
   */
  void writeHiveChar(HiveChar hiveChar) throws IOException;

  /*
   * VARCHAR.
   */
  void writeHiveVarchar(HiveVarchar hiveVarchar) throws IOException;

  /*
   * BINARY.
   */
  void writeBinary(byte[] v) throws IOException;
  void writeBinary(byte[] v, int start, int length) throws IOException;

  /*
   * DATE.
   */
  void writeDate(Date date) throws IOException;

  // We provide a faster way to write a date without a Date object.
  void writeDate(int dateAsDays) throws IOException;

  /*
   * TIMESTAMP.
   */
  void writeTimestamp(Timestamp vt) throws IOException;

  /*
   * INTERVAL_YEAR_MONTH.
   */
  void writeHiveIntervalYearMonth(HiveIntervalYearMonth viyt) throws IOException;

  // We provide a faster way to write a hive interval year month without a HiveIntervalYearMonth object.
  void writeHiveIntervalYearMonth(int totalMonths) throws IOException;

  /*
   * INTERVAL_DAY_TIME.
   */
  void writeHiveIntervalDayTime(HiveIntervalDayTime vidt) throws IOException;

  /*
   * DECIMAL.
   *
   * NOTE: The scale parameter is for text serialization (e.g. HiveDecimal.toFormatString) that
   * creates trailing zeroes output decimals.
   */
  void writeHiveDecimal(HiveDecimal dec, int scale) throws IOException;
  void writeHiveDecimal(HiveDecimalWritable decWritable, int scale) throws IOException;
}
