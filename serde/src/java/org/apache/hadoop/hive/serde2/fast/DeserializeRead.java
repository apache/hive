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
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

/*
 * Directly deserialize with the caller reading field-by-field a serialization format.
 * 
 * The caller is responsible for calling the read method for the right type of each field
 * (after calling readCheckNull).
 * 
 * Reading some fields require a results object to receive value information.  A separate
 * results object is created by the caller at initialization per different field even for the same
 * type.
 *
 * Some type values are by reference to either bytes in the deserialization buffer or to
 * other type specific buffers.  So, those references are only valid until the next time set is
 * called.
 */
public interface DeserializeRead {

  /*
   * The primitive type information for all fields.
   */
  PrimitiveTypeInfo[] primitiveTypeInfos();

  /*
   * Set the range of bytes to be deserialized.
   */
  void set(byte[] bytes, int offset, int length);

  /*
   * Reads the NULL information for a field.
   *
   * @return Return true when the field is NULL; reading is positioned to the next field.
   *         Otherwise, false when the field is NOT NULL; reading is positioned to the field data.
   */
  boolean readCheckNull() throws IOException;

  /*
   * Call this method after all fields have been read to check for extra fields.
   */
  void extraFieldsCheck();
 
  /*
   * Read integrity warning flags.
   */
  boolean readBeyondConfiguredFieldsWarned();
  boolean readBeyondBufferRangeWarned();
  boolean bufferRangeHasExtraDataWarned();

  /*
   * BOOLEAN.
   */
  boolean readBoolean() throws IOException;

  /*
   * BYTE.
   */
  byte readByte() throws IOException;

  /*
   * SHORT.
   */
  short readShort() throws IOException;

  /*
   * INT.
   */
  int readInt() throws IOException;

  /*
   * LONG.
   */
  long readLong() throws IOException;

  /*
   * FLOAT.
   */
  float readFloat() throws IOException;

  /*
   * DOUBLE.
   */
  double readDouble() throws IOException;

  /*
   * This class is the base abstract read bytes results for STRING, CHAR, VARCHAR, and BINARY.
   */
  public abstract class ReadBytesResults {

    public byte[] bytes;
    public int start;
    public int length;

    public ReadBytesResults() {
      bytes = null;
      start = 0;
      length = 0;
    }
  }

  /*
   * STRING.
   *
   * Can be used to read CHAR and VARCHAR when the caller takes responsibility for
   * truncation/padding issues.
   */

  // This class is for abstract since each format may need its own specialization.
  public abstract class ReadStringResults extends ReadBytesResults {

    public ReadStringResults() {
      super();
    }
  }

  // Reading a STRING field require a results object to receive value information.  A separate
  // results object is created at initialization per different bytes field. 
  ReadStringResults createReadStringResults();

  void readString(ReadStringResults readStringResults) throws IOException;

  /*
   * CHAR.
   */

  // This class is for abstract since each format may need its own specialization.
  public abstract class ReadHiveCharResults extends ReadBytesResults {

    private CharTypeInfo charTypeInfo;
    private int maxLength;

    protected HiveCharWritable hiveCharWritable;

    public ReadHiveCharResults() {
      super();
    }

    public void init(CharTypeInfo charTypeInfo) {
      this.charTypeInfo = charTypeInfo;
      this.maxLength = charTypeInfo.getLength();
      hiveCharWritable = new HiveCharWritable();
    }

    public boolean isInit() {
      return (charTypeInfo != null);
    }

    public int getMaxLength() {
      return maxLength;
    }

    public HiveChar getHiveChar() {
      return hiveCharWritable.getHiveChar();
    }
  }

  // Reading a CHAR field require a results object to receive value information.  A separate
  // results object is created at initialization per different CHAR field. 
  ReadHiveCharResults createReadHiveCharResults();

  void readHiveChar(ReadHiveCharResults readHiveCharResults) throws IOException;

  /*
   * VARCHAR.
   */

  // This class is for abstract since each format may need its own specialization.
  public abstract class ReadHiveVarcharResults extends ReadBytesResults {

    private VarcharTypeInfo varcharTypeInfo;
    private int maxLength;

    protected HiveVarcharWritable hiveVarcharWritable;

    public ReadHiveVarcharResults() {
      super();
    }

    public void init(VarcharTypeInfo varcharTypeInfo) {
      this.varcharTypeInfo = varcharTypeInfo;
      this.maxLength = varcharTypeInfo.getLength();
      hiveVarcharWritable = new HiveVarcharWritable();
    }

    public boolean isInit() {
      return (varcharTypeInfo != null);
    }

    public int getMaxLength() {
      return maxLength;
    }

    public HiveVarchar getHiveVarchar() {
      return hiveVarcharWritable.getHiveVarchar();
    }
  }

  // Reading a VARCHAR field require a results object to receive value information.  A separate
  // results object is created at initialization per different VARCHAR field. 
  ReadHiveVarcharResults createReadHiveVarcharResults();

  void readHiveVarchar(ReadHiveVarcharResults readHiveVarcharResults) throws IOException;

  /*
   * BINARY.
   */

  // This class is for abstract since each format may need its own specialization.
  public abstract class ReadBinaryResults extends ReadBytesResults {

    public ReadBinaryResults() {
      super();
    }
  }

  // Reading a BINARY field require a results object to receive value information.  A separate
  // results object is created at initialization per different bytes field. 
  ReadBinaryResults createReadBinaryResults();

  void readBinary(ReadBinaryResults readBinaryResults) throws IOException;

  /*
   * DATE.
   */

  // This class is for abstract since each format may need its own specialization.
  public abstract class ReadDateResults {

    protected DateWritable dateWritable;

    public ReadDateResults() {
      dateWritable = new DateWritable();
    }

    public Date getDate() {
      return dateWritable.get();
    }

    public int getDays() {
      return dateWritable.getDays();
    }
  }

  // Reading a DATE field require a results object to receive value information.  A separate
  // results object is created at initialization per different DATE field. 
  ReadDateResults createReadDateResults();

  void readDate(ReadDateResults readDateResults) throws IOException;

  /*
   * TIMESTAMP.
   */

  // This class is for abstract since each format may need its own specialization.
  public abstract class ReadTimestampResults {

    protected TimestampWritable timestampWritable;

    public ReadTimestampResults() {
      timestampWritable = new TimestampWritable();
    }

    public Timestamp getTimestamp() {
      return timestampWritable.getTimestamp();
    }
  }

  // Reading a TIMESTAMP field require a results object to receive value information.  A separate
  // results object is created at initialization per different TIMESTAMP field. 
  ReadTimestampResults createReadTimestampResults();

  void readTimestamp(ReadTimestampResults readTimestampResult) throws IOException;

  /*
   * INTERVAL_YEAR_MONTH.
   */

  // This class is for abstract since each format may need its own specialization.
  public abstract class ReadIntervalYearMonthResults {

    protected HiveIntervalYearMonthWritable hiveIntervalYearMonthWritable;

    public ReadIntervalYearMonthResults() {
      hiveIntervalYearMonthWritable = new HiveIntervalYearMonthWritable();
    }

    public HiveIntervalYearMonth getHiveIntervalYearMonth() {
      return hiveIntervalYearMonthWritable.getHiveIntervalYearMonth();
    }
  }

  // Reading a INTERVAL_YEAR_MONTH field require a results object to receive value information.
  // A separate results object is created at initialization per different INTERVAL_YEAR_MONTH field. 
  ReadIntervalYearMonthResults createReadIntervalYearMonthResults();

  void readIntervalYearMonth(ReadIntervalYearMonthResults readIntervalYearMonthResult) throws IOException;

  /*
   * INTERVAL_DAY_TIME.
   */

  // This class is for abstract since each format may need its own specialization.
  public abstract class ReadIntervalDayTimeResults {

    protected HiveIntervalDayTimeWritable hiveIntervalDayTimeWritable;

    public ReadIntervalDayTimeResults() {
      hiveIntervalDayTimeWritable = new HiveIntervalDayTimeWritable();
    }

    public HiveIntervalDayTime getHiveIntervalDayTime() {
      return hiveIntervalDayTimeWritable.getHiveIntervalDayTime();
    }
  }

  // Reading a INTERVAL_DAY_TIME field require a results object to receive value information.
  // A separate results object is created at initialization per different INTERVAL_DAY_TIME field. 
  ReadIntervalDayTimeResults createReadIntervalDayTimeResults();

  void readIntervalDayTime(ReadIntervalDayTimeResults readIntervalDayTimeResult) throws IOException;

  /*
   * DECIMAL.
   */

  // This class is for abstract since each format may need its own specialization.
  public abstract class ReadDecimalResults {

    protected DecimalTypeInfo decimalTypeInfo;

    public ReadDecimalResults() {
    }

    public void init(DecimalTypeInfo decimalTypeInfo) {
      this.decimalTypeInfo = decimalTypeInfo;
    }

    public boolean isInit() {
      return (decimalTypeInfo != null);
    }

    public abstract HiveDecimal getHiveDecimal();
  }

  // Reading a DECIMAL field require a results object to receive value information.  A separate
  // results object is created at initialization per different DECIMAL field. 
  ReadDecimalResults createReadDecimalResults();

  void readHiveDecimal(ReadDecimalResults readDecimalResults) throws IOException;
}