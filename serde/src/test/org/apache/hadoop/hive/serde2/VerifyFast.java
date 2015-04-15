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
import org.apache.hadoop.hive.serde2.binarysortable.MyTestPrimitiveClass.ExtraTypeInfo;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.fast.SerializeWrite;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.Text;

/**
 * TestBinarySortableSerDe.
 *
 */
public class VerifyFast {

  public static void verifyDeserializeRead(DeserializeRead deserializeRead, PrimitiveTypeInfo primitiveTypeInfo, Object object) throws IOException {

    boolean isNull;

    isNull = deserializeRead.readCheckNull();
    if (isNull) {
      if (object != null) {
        TestCase.fail("Field reports null but object is not null");
      }
      return;
    } else if (object == null) {
      TestCase.fail("Field report not null but object is null");
    }
    switch (primitiveTypeInfo.getPrimitiveCategory()) {
      case BOOLEAN:
      {
        boolean value = deserializeRead.readBoolean();
        if (!(object instanceof Boolean)) {
          TestCase.fail("Boolean expected object not Boolean");
        }
        Boolean expected = (Boolean) object;
        if (value != expected) {
          TestCase.fail("Boolean field mismatch (expected " + expected + " found " + value + ")");
        }
      }
      break;
    case BYTE:
      {
        byte value = deserializeRead.readByte();
        if (!(object instanceof Byte)) {
          TestCase.fail("Byte expected object not Byte");
        }
        Byte expected = (Byte) object;
        if (value != expected) {
          TestCase.fail("Byte field mismatch (expected " + (int) expected + " found " + (int) value + ")");
        }
      }
      break;
    case SHORT:
      {
        short value = deserializeRead.readShort();
        if (!(object instanceof Short)) {
          TestCase.fail("Short expected object not Short");
        }
        Short expected = (Short) object;
        if (value != expected) {
          TestCase.fail("Short field mismatch (expected " + expected + " found " + value + ")");
        }
      }
      break;
    case INT:
      {
        int value = deserializeRead.readInt();
        if (!(object instanceof Integer)) {
          TestCase.fail("Integer expected object not Integer");
        }
        Integer expected = (Integer) object;
        if (value != expected) {
          TestCase.fail("Int field mismatch (expected " + expected + " found " + value + ")");
        }
      }
      break;
    case LONG:
      {
        long value = deserializeRead.readLong();
        if (!(object instanceof Long)) {
          TestCase.fail("Long expected object not Long");
        }
        Long expected = (Long) object;
        if (value != expected) {
          TestCase.fail("Long field mismatch (expected " + expected + " found " + value + ")");
        }
      }
      break;
    case FLOAT:
      {
        float value = deserializeRead.readFloat();
        Float expected = (Float) object;
        if (!(object instanceof Float)) {
          TestCase.fail("Float expected object not Float");
        }
        if (value != expected) {
          TestCase.fail("Float field mismatch (expected " + expected + " found " + value + ")");
        }
      }
      break;
    case DOUBLE:
      {
        double value = deserializeRead.readDouble();
        Double expected = (Double) object;
        if (!(object instanceof Double)) {
          TestCase.fail("Double expected object not Double");
        }
        if (value != expected) {
          TestCase.fail("Double field mismatch (expected " + expected + " found " + value + ")");
        }
      }
      break;
    case STRING:
      {
        DeserializeRead.ReadStringResults readStringResults = deserializeRead.createReadStringResults();
        deserializeRead.readString(readStringResults);
        byte[] stringBytes = Arrays.copyOfRange(readStringResults.bytes, readStringResults.start, readStringResults.start + readStringResults.length);
        Text text = new Text(stringBytes);
        String string = text.toString();
        String expected = (String) object;
        if (!string.equals(expected)) {
          TestCase.fail("String field mismatch (expected '" + expected + "' found '" + string + "')");
        }
      }
      break;
    case CHAR:
      {
        DeserializeRead.ReadHiveCharResults readHiveCharResults = deserializeRead.createReadHiveCharResults();
        deserializeRead.readHiveChar(readHiveCharResults);
        HiveChar hiveChar = readHiveCharResults.getHiveChar();
        HiveChar expected = (HiveChar) object;
        if (!hiveChar.equals(expected)) {
          TestCase.fail("Char field mismatch (expected '" + expected + "' found '" + hiveChar + "')");
        }
      }
      break;
    case VARCHAR:
      {
        DeserializeRead.ReadHiveVarcharResults readHiveVarcharResults = deserializeRead.createReadHiveVarcharResults();
        deserializeRead.readHiveVarchar(readHiveVarcharResults);
        HiveVarchar hiveVarchar = readHiveVarcharResults.getHiveVarchar();
        HiveVarchar expected = (HiveVarchar) object;
        if (!hiveVarchar.equals(expected)) {
          TestCase.fail("Varchar field mismatch (expected '" + expected + "' found '" + hiveVarchar + "')");
        }
      }
      break;
    case DECIMAL:
      {
        DeserializeRead.ReadDecimalResults readDecimalResults = deserializeRead.createReadDecimalResults();
        deserializeRead.readHiveDecimal(readDecimalResults);
        HiveDecimal value = readDecimalResults.getHiveDecimal();
        if (value == null) {
          TestCase.fail("Decimal field evaluated to NULL");
        }
        HiveDecimal expected = (HiveDecimal) object;
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
        DeserializeRead.ReadDateResults readDateResults = deserializeRead.createReadDateResults();
        deserializeRead.readDate(readDateResults);
        Date value = readDateResults.getDate();
        Date expected = (Date) object;
        if (!value.equals(expected)) {
          TestCase.fail("Date field mismatch (expected " + expected.toString() + " found " + value.toString() + ")");
        }
      }
      break;
    case TIMESTAMP:
      {
        DeserializeRead.ReadTimestampResults readTimestampResults = deserializeRead.createReadTimestampResults();
        deserializeRead.readTimestamp(readTimestampResults);
        Timestamp value = readTimestampResults.getTimestamp();
        Timestamp expected = (Timestamp) object;
        if (!value.equals(expected)) {
          TestCase.fail("Timestamp field mismatch (expected " + expected.toString() + " found " + value.toString() + ")");
        }
      }
      break;
    case INTERVAL_YEAR_MONTH:
      {
        DeserializeRead.ReadIntervalYearMonthResults readIntervalYearMonthResults = deserializeRead.createReadIntervalYearMonthResults();
        deserializeRead.readIntervalYearMonth(readIntervalYearMonthResults);
        HiveIntervalYearMonth value = readIntervalYearMonthResults.getHiveIntervalYearMonth();
        HiveIntervalYearMonth expected = (HiveIntervalYearMonth) object;
        if (!value.equals(expected)) {
          TestCase.fail("HiveIntervalYearMonth field mismatch (expected " + expected.toString() + " found " + value.toString() + ")");
        }
      }
      break;
    case INTERVAL_DAY_TIME:
      {
        DeserializeRead.ReadIntervalDayTimeResults readIntervalDayTimeResults = deserializeRead.createReadIntervalDayTimeResults();
        deserializeRead.readIntervalDayTime(readIntervalDayTimeResults);
        HiveIntervalDayTime value = readIntervalDayTimeResults.getHiveIntervalDayTime();
        HiveIntervalDayTime expected = (HiveIntervalDayTime) object;
        if (!value.equals(expected)) {
          TestCase.fail("HiveIntervalDayTime field mismatch (expected " + expected.toString() + " found " + value.toString() + ")");
        }
      }
      break;
    case BINARY:
      {
        DeserializeRead.ReadBinaryResults readBinaryResults = deserializeRead.createReadBinaryResults();
        deserializeRead.readBinary(readBinaryResults);
        byte[] byteArray = Arrays.copyOfRange(readBinaryResults.bytes, readBinaryResults.start, readBinaryResults.start + readBinaryResults.length);
        byte[] expected = (byte[]) object;
        if (byteArray.length != expected.length){
          TestCase.fail("Byte Array field mismatch (expected " + expected + " found " + byteArray + ")");
        }
        for (int b = 0; b < byteArray.length; b++) {
          if (byteArray[b] != expected[b]) {
            TestCase.fail("Byte Array field mismatch (expected " + expected + " found " + byteArray + ")");
          }
        }
      }
      break;
    default:
      throw new Error("Unknown primitive category " + primitiveTypeInfo.getPrimitiveCategory());
    }
  }

  public static void serializeWrite(SerializeWrite serializeWrite, PrimitiveCategory primitiveCategory, Object object) throws IOException {
    if (object == null) {
      serializeWrite.writeNull();
      return;
    }
    switch (primitiveCategory) {
      case BOOLEAN:
      {
        boolean value = (Boolean) object;
        serializeWrite.writeBoolean(value);
      }
      break;
    case BYTE:
      {
        byte value = (Byte) object;
        serializeWrite.writeByte(value);
      }
      break;
    case SHORT:
      {
        short value = (Short) object;
        serializeWrite.writeShort(value);
      }
      break;
    case INT:
      {
        int value = (Integer) object;
        serializeWrite.writeInt(value);
      }
      break;
    case LONG:
      {
        long value = (Long) object;
        serializeWrite.writeLong(value);
      }
      break;
    case FLOAT:
      {
        float value = (Float) object;
        serializeWrite.writeFloat(value);
      }
      break;
    case DOUBLE:
      {
        double value = (Double) object;
        serializeWrite.writeDouble(value);
      }
      break;
    case STRING:
      {
        String value = (String) object;
        byte[] stringBytes = value.getBytes();
        int stringLength = stringBytes.length;
        serializeWrite.writeString(stringBytes, 0, stringLength);
      }
      break;
    case CHAR:
      {
        HiveChar value = (HiveChar) object;
        serializeWrite.writeHiveChar(value);
      }
      break;
    case VARCHAR:
      {
        HiveVarchar value = (HiveVarchar) object;
        serializeWrite.writeHiveVarchar(value);
      }
      break;
    case DECIMAL:
      {
        HiveDecimal value = (HiveDecimal) object;
        serializeWrite.writeHiveDecimal(value);
      }
      break;
    case DATE:
      {
        Date value = (Date) object;
        serializeWrite.writeDate(value);
      }
      break;
    case TIMESTAMP:
      {
        Timestamp value = (Timestamp) object;
        serializeWrite.writeTimestamp(value);
      }
      break;
    case INTERVAL_YEAR_MONTH:
      {
        HiveIntervalYearMonth value = (HiveIntervalYearMonth) object;
        serializeWrite.writeHiveIntervalYearMonth(value);
      }
      break;
    case INTERVAL_DAY_TIME:
      {
        HiveIntervalDayTime value = (HiveIntervalDayTime) object;
        serializeWrite.writeHiveIntervalDayTime(value);
      }
      break;
    case BINARY:
      {
        byte[] binaryBytes = (byte[]) object;
        int length = binaryBytes.length;
        serializeWrite.writeBinary(binaryBytes, 0, length);
      }
      break;
    default:
      throw new Error("Unknown primitive category " + primitiveCategory.name());
    }
  }
}