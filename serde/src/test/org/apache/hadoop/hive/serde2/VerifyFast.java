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
package org.apache.hadoop.hive.serde2;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.fast.SerializeWrite;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * TestBinarySortableSerDe.
 *
 */
public class VerifyFast {

  public static void verifyDeserializeRead(DeserializeRead deserializeRead,
      PrimitiveTypeInfo primitiveTypeInfo, Writable writable) throws IOException {

    boolean isNull;

    isNull = !deserializeRead.readNextField();
    if (isNull) {
      if (writable != null) {
        TestCase.fail("Field reports null but object is not null (class " + writable.getClass().getName() + ", " + writable.toString() + ")");
      }
      return;
    } else if (writable == null) {
      TestCase.fail("Field report not null but object is null");
    }
    switch (primitiveTypeInfo.getPrimitiveCategory()) {
      case BOOLEAN:
      {
        boolean value = deserializeRead.currentBoolean;
        if (!(writable instanceof BooleanWritable)) {
          TestCase.fail("Boolean expected writable not Boolean");
        }
        boolean expected = ((BooleanWritable) writable).get();
        if (value != expected) {
          TestCase.fail("Boolean field mismatch (expected " + expected + " found " + value + ")");
        }
      }
      break;
    case BYTE:
      {
        byte value = deserializeRead.currentByte;
        if (!(writable instanceof ByteWritable)) {
          TestCase.fail("Byte expected writable not Byte");
        }
        byte expected = ((ByteWritable) writable).get();
        if (value != expected) {
          TestCase.fail("Byte field mismatch (expected " + (int) expected + " found " + (int) value + ")");
        }
      }
      break;
    case SHORT:
      {
        short value = deserializeRead.currentShort;
        if (!(writable instanceof ShortWritable)) {
          TestCase.fail("Short expected writable not Short");
        }
        short expected = ((ShortWritable) writable).get();
        if (value != expected) {
          TestCase.fail("Short field mismatch (expected " + expected + " found " + value + ")");
        }
      }
      break;
    case INT:
      {
        int value = deserializeRead.currentInt;
        if (!(writable instanceof IntWritable)) {
          TestCase.fail("Integer expected writable not Integer");
        }
        int expected = ((IntWritable) writable).get();
        if (value != expected) {
          TestCase.fail("Int field mismatch (expected " + expected + " found " + value + ")");
        }
      }
      break;
    case LONG:
      {
        long value = deserializeRead.currentLong;
        if (!(writable instanceof LongWritable)) {
          TestCase.fail("Long expected writable not Long");
        }
        Long expected = ((LongWritable) writable).get();
        if (value != expected) {
          TestCase.fail("Long field mismatch (expected " + expected + " found " + value + ")");
        }
      }
      break;
    case FLOAT:
      {
        float value = deserializeRead.currentFloat;
        if (!(writable instanceof FloatWritable)) {
          TestCase.fail("Float expected writable not Float");
        }
        float expected = ((FloatWritable) writable).get();
        if (value != expected) {
          TestCase.fail("Float field mismatch (expected " + expected + " found " + value + ")");
        }
      }
      break;
    case DOUBLE:
      {
        double value = deserializeRead.currentDouble;
        if (!(writable instanceof DoubleWritable)) {
          TestCase.fail("Double expected writable not Double");
        }
        double expected = ((DoubleWritable) writable).get();
        if (value != expected) {
          TestCase.fail("Double field mismatch (expected " + expected + " found " + value + ")");
        }
      }
      break;
    case STRING:
      {
        byte[] stringBytes = Arrays.copyOfRange(
            deserializeRead.currentBytes,
            deserializeRead.currentBytesStart,
            deserializeRead.currentBytesStart + deserializeRead.currentBytesLength);
        Text text = new Text(stringBytes);
        String string = text.toString();
        String expected = ((Text) writable).toString();
        if (!string.equals(expected)) {
          TestCase.fail("String field mismatch (expected '" + expected + "' found '" + string + "')");
        }
      }
      break;
    case CHAR:
      {
        byte[] stringBytes = Arrays.copyOfRange(
            deserializeRead.currentBytes,
            deserializeRead.currentBytesStart,
            deserializeRead.currentBytesStart + deserializeRead.currentBytesLength);
        Text text = new Text(stringBytes);
        String string = text.toString();

        HiveChar hiveChar = new HiveChar(string, ((CharTypeInfo) primitiveTypeInfo).getLength());

        HiveChar expected = ((HiveCharWritable) writable).getHiveChar();
        if (!hiveChar.equals(expected)) {
          TestCase.fail("Char field mismatch (expected '" + expected + "' found '" + hiveChar + "')");
        }
      }
      break;
    case VARCHAR:
      {
        byte[] stringBytes = Arrays.copyOfRange(
            deserializeRead.currentBytes,
            deserializeRead.currentBytesStart,
            deserializeRead.currentBytesStart + deserializeRead.currentBytesLength);
        Text text = new Text(stringBytes);
        String string = text.toString();

        HiveVarchar hiveVarchar = new HiveVarchar(string, ((VarcharTypeInfo) primitiveTypeInfo).getLength());

        HiveVarchar expected = ((HiveVarcharWritable) writable).getHiveVarchar();
        if (!hiveVarchar.equals(expected)) {
          TestCase.fail("Varchar field mismatch (expected '" + expected + "' found '" + hiveVarchar + "')");
        }
      }
      break;
    case DECIMAL:
      {
        HiveDecimal value = deserializeRead.currentHiveDecimalWritable.getHiveDecimal();
        if (value == null) {
          TestCase.fail("Decimal field evaluated to NULL");
        }
        HiveDecimal expected = ((HiveDecimalWritable) writable).getHiveDecimal();
        if (!value.equals(expected)) {
          DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveTypeInfo;
          int precision = decimalTypeInfo.getPrecision();
          int scale = decimalTypeInfo.getScale();
          TestCase.fail("Decimal field mismatch (expected " + expected.toString() + " found " + value.toString() + ") precision " + precision + ", scale " + scale);
        }
      }
      break;
    case DATE:
      {
        Date value = deserializeRead.currentDateWritable.get();
        Date expected = ((DateWritable) writable).get();
        if (!value.equals(expected)) {
          TestCase.fail("Date field mismatch (expected " + expected.toString() + " found " + value.toString() + ")");
        }
      }
      break;
    case TIMESTAMP:
      {
        Timestamp value = deserializeRead.currentTimestampWritable.getTimestamp();
        Timestamp expected = ((TimestampWritable) writable).getTimestamp();
        if (!value.equals(expected)) {
          TestCase.fail("Timestamp field mismatch (expected " + expected.toString() + " found " + value.toString() + ")");
        }
      }
      break;
    case INTERVAL_YEAR_MONTH:
      {
        HiveIntervalYearMonth value = deserializeRead.currentHiveIntervalYearMonthWritable.getHiveIntervalYearMonth();
        HiveIntervalYearMonth expected = ((HiveIntervalYearMonthWritable) writable).getHiveIntervalYearMonth();
        if (!value.equals(expected)) {
          TestCase.fail("HiveIntervalYearMonth field mismatch (expected " + expected.toString() + " found " + value.toString() + ")");
        }
      }
      break;
    case INTERVAL_DAY_TIME:
      {
        HiveIntervalDayTime value = deserializeRead.currentHiveIntervalDayTimeWritable.getHiveIntervalDayTime();
        HiveIntervalDayTime expected = ((HiveIntervalDayTimeWritable) writable).getHiveIntervalDayTime();
        if (!value.equals(expected)) {
          TestCase.fail("HiveIntervalDayTime field mismatch (expected " + expected.toString() + " found " + value.toString() + ")");
        }
      }
      break;
    case BINARY:
      {
        byte[] byteArray = Arrays.copyOfRange(
            deserializeRead.currentBytes,
            deserializeRead.currentBytesStart,
            deserializeRead.currentBytesStart + deserializeRead.currentBytesLength);
        BytesWritable bytesWritable = (BytesWritable) writable;
        byte[] expected = Arrays.copyOfRange(bytesWritable.getBytes(), 0, bytesWritable.getLength());
        if (byteArray.length != expected.length){
          TestCase.fail("Byte Array field mismatch (expected " + Arrays.toString(expected)
              + " found " + Arrays.toString(byteArray) + ")");
        }
        for (int b = 0; b < byteArray.length; b++) {
          if (byteArray[b] != expected[b]) {
            TestCase.fail("Byte Array field mismatch (expected " + Arrays.toString(expected)
              + " found " + Arrays.toString(byteArray) + ")");
          }
        }
      }
      break;
    default:
      throw new Error("Unknown primitive category " + primitiveTypeInfo.getPrimitiveCategory());
    }
  }

  public static void serializeWrite(SerializeWrite serializeWrite,
      PrimitiveTypeInfo primitiveTypeInfo, Writable writable) throws IOException {
    if (writable == null) {
      serializeWrite.writeNull();
      return;
    }
    switch (primitiveTypeInfo.getPrimitiveCategory()) {
      case BOOLEAN:
      {
        boolean value = ((BooleanWritable) writable).get();
        serializeWrite.writeBoolean(value);
      }
      break;
    case BYTE:
      {
        byte value = ((ByteWritable) writable).get();
        serializeWrite.writeByte(value);
      }
      break;
    case SHORT:
      {
        short value = ((ShortWritable) writable).get();
        serializeWrite.writeShort(value);
      }
      break;
    case INT:
      {
        int value = ((IntWritable) writable).get();
        serializeWrite.writeInt(value);
      }
      break;
    case LONG:
      {
        long value = ((LongWritable) writable).get();
        serializeWrite.writeLong(value);
      }
      break;
    case FLOAT:
      {
        float value = ((FloatWritable) writable).get();
        serializeWrite.writeFloat(value);
      }
      break;
    case DOUBLE:
      {
        double value = ((DoubleWritable) writable).get();
        serializeWrite.writeDouble(value);
      }
      break;
    case STRING:
      {
        Text value = (Text) writable;
        byte[] stringBytes = value.getBytes();
        int stringLength = stringBytes.length;
        serializeWrite.writeString(stringBytes, 0, stringLength);
      }
      break;
    case CHAR:
      {
        HiveChar value = ((HiveCharWritable) writable).getHiveChar();
        serializeWrite.writeHiveChar(value);
      }
      break;
    case VARCHAR:
      {
        HiveVarchar value = ((HiveVarcharWritable) writable).getHiveVarchar();
        serializeWrite.writeHiveVarchar(value);
      }
      break;
    case DECIMAL:
      {
        HiveDecimal value = ((HiveDecimalWritable) writable).getHiveDecimal();
        DecimalTypeInfo decTypeInfo = (DecimalTypeInfo)primitiveTypeInfo;
        serializeWrite.writeHiveDecimal(value, decTypeInfo.scale());
      }
      break;
    case DATE:
      {
        Date value = ((DateWritable) writable).get();
        serializeWrite.writeDate(value);
      }
      break;
    case TIMESTAMP:
      {
        Timestamp value = ((TimestampWritable) writable).getTimestamp();
        serializeWrite.writeTimestamp(value);
      }
      break;
    case INTERVAL_YEAR_MONTH:
      {
        HiveIntervalYearMonth value = ((HiveIntervalYearMonthWritable) writable).getHiveIntervalYearMonth();
        serializeWrite.writeHiveIntervalYearMonth(value);
      }
      break;
    case INTERVAL_DAY_TIME:
      {
        HiveIntervalDayTime value = ((HiveIntervalDayTimeWritable) writable).getHiveIntervalDayTime();
        serializeWrite.writeHiveIntervalDayTime(value);
      }
      break;
    case BINARY:
      {
        BytesWritable byteWritable = (BytesWritable) writable;
        byte[] binaryBytes = byteWritable.getBytes();
        int length = byteWritable.getLength();
        serializeWrite.writeBinary(binaryBytes, 0, length);
      }
      break;
    default:
      throw new Error("Unknown primitive category " + primitiveTypeInfo.getPrimitiveCategory().name());
    }
  }
}