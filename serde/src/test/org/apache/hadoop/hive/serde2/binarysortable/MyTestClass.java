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
package org.apache.hadoop.hive.serde2.binarysortable;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.RandomTypeUtil;
import org.apache.hadoop.hive.serde2.binarysortable.MyTestPrimitiveClass.ExtraTypeInfo;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveIntervalDayTimeObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveIntervalYearMonthObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

public class MyTestClass {

    public Boolean myBool;
    public Byte myByte;
    public Short myShort;
    public Integer myInt;
    public Long myLong;
    public Float myFloat;
    public Double myDouble;
    public String myString;
    public HiveChar myHiveChar;
    public HiveVarchar myHiveVarchar;
    public byte[] myBinary;
    public HiveDecimal myDecimal;
    public Date myDate;
    public Timestamp myTimestamp;
    public HiveIntervalYearMonth myIntervalYearMonth;
    public HiveIntervalDayTime myIntervalDayTime;

    // Add more complex types.
    public MyTestInnerStruct myStruct;
    public  List<Integer> myList;

    public MyTestClass() {
    }

    public final static int fieldCount = 18;

    public int randomFill(Random r, ExtraTypeInfo extraTypeInfo) {
      int randField = r.nextInt(MyTestClass.fieldCount);
      int field = 0;

      myBool = (randField == field++) ? null : (r.nextInt(1) == 1);
      myByte = (randField == field++) ? null : Byte.valueOf((byte) r.nextInt());
      myShort = (randField == field++) ? null : Short.valueOf((short) r.nextInt());
      myInt = (randField == field++) ? null : Integer.valueOf(r.nextInt());
      myLong = (randField == field++) ? null : Long.valueOf(r.nextLong());
      myFloat = (randField == field++) ? null : Float
          .valueOf(r.nextFloat() * 10 - 5);
      myDouble = (randField == field++) ? null : Double
          .valueOf(r.nextDouble() * 10 - 5);
      myString = (randField == field++) ? null : MyTestPrimitiveClass.getRandString(r);
      myHiveChar = (randField == field++) ? null : MyTestPrimitiveClass.getRandHiveChar(r, extraTypeInfo);
      myHiveVarchar = (randField == field++) ? null : MyTestPrimitiveClass.getRandHiveVarchar(r, extraTypeInfo);
      myBinary = MyTestPrimitiveClass.getRandBinary(r, r.nextInt(1000));
      myDecimal = (randField == field++) ? null : MyTestPrimitiveClass.getRandHiveDecimal(r, extraTypeInfo);
      myDate = (randField == field++) ? null : MyTestPrimitiveClass.getRandDate(r);
      myTimestamp = (randField == field++) ? null : RandomTypeUtil.getRandTimestamp(r);
      myIntervalYearMonth = (randField == field++) ? null : MyTestPrimitiveClass.getRandIntervalYearMonth(r);
      myIntervalDayTime = (randField == field++) ? null : MyTestPrimitiveClass.getRandIntervalDayTime(r);

      myStruct = (randField == field++) ? null : new MyTestInnerStruct(
          r.nextInt(5) - 2, r.nextInt(5) - 2);
      myList = (randField == field++) ? null : getRandIntegerArray(r);
      return field;
    }

    public static List<Integer> getRandIntegerArray(Random r) {
      int length = r.nextInt(10);
      ArrayList<Integer> result = new ArrayList<Integer>(length);
      for (int i = 0; i < length; i++) {
        result.add(r.nextInt(128));
      }
      return result;
    }

    public void nonRandomFill(int idx) {
      myByte = (Byte) getNonRandValue(nrByte, idx);
      myShort = (Short) getNonRandValue(nrShort, idx);
      myInt = (Integer) getNonRandValue(nrInt, idx);
      myLong = (Long) getNonRandValue(nrLong, idx);
      myFloat = (Float) getNonRandValue(nrFloat, idx);
      myDouble = (Double) getNonRandValue(nrDouble, idx);
      myString = (String) getNonRandValue(nrString, idx);
      myHiveChar = new HiveChar(myString, myString.length());
      myHiveVarchar = new HiveVarchar(myString, myString.length());
      myDecimal = (HiveDecimal) getNonRandValue(nrDecimal, idx);
      myDate = (Date) getNonRandValue(nrDate, idx);
      myIntervalYearMonth = (HiveIntervalYearMonth) getNonRandValue(nrIntervalYearMonth, idx);
      myIntervalDayTime = (HiveIntervalDayTime) getNonRandValue(nrIntervalDayTime, idx);
      myStruct = null;
      myList = null;
    }

    public static Object getNonRandValue(Object[] nrArray, int index) {
      return nrArray[index % nrArray.length];
    }

    static Object[] nrByte = {
        Byte.valueOf((byte) 1)
    };

    static Object[] nrShort = {
        Short.valueOf((short) 1)
    };

    static Object[] nrInt = {
        Integer.valueOf(1)
    };

    static Object[] nrLong = {
        Long.valueOf(1)
    };

    static Object[] nrFloat = {
        Float.valueOf(1.0f)
    };

    static Object[] nrDouble = {
        Double.valueOf(1.0)
    };

    static Object[] nrDecimal = {
        HiveDecimal.create("100"),
        HiveDecimal.create("10"),
        HiveDecimal.create("1"),
        HiveDecimal.create("0"),
        HiveDecimal.create("0.1"),
        HiveDecimal.create("0.01"),
        HiveDecimal.create("0.001"),
        HiveDecimal.create("-100"),
        HiveDecimal.create("-10"),
        HiveDecimal.create("-1"),
        HiveDecimal.create("-0.1"),
        HiveDecimal.create("-0.01"),
        HiveDecimal.create("-0.001"),
        HiveDecimal.create("12345678900"),
        HiveDecimal.create("1234567890"),
        HiveDecimal.create("123456789"),
        HiveDecimal.create("12345678.9"),
        HiveDecimal.create("1234567.89"),
        HiveDecimal.create("123456.789"),
        HiveDecimal.create("12345.6789"),
        HiveDecimal.create("1234.56789"),
        HiveDecimal.create("123.456789"),
        HiveDecimal.create("1.23456789"),
        HiveDecimal.create("0.123456789"),
        HiveDecimal.create("0.0123456789"),
        HiveDecimal.create("0.00123456789"),
        HiveDecimal.create("0.000123456789"),
        HiveDecimal.create("-12345678900"),
        HiveDecimal.create("-1234567890"),
        HiveDecimal.create("-123456789"),
        HiveDecimal.create("-12345678.9"),
        HiveDecimal.create("-1234567.89"),
        HiveDecimal.create("-123456.789"),
        HiveDecimal.create("-12345.6789"),
        HiveDecimal.create("-1234.56789"),
        HiveDecimal.create("-123.456789"),
        HiveDecimal.create("-1.23456789"),
        HiveDecimal.create("-0.123456789"),
        HiveDecimal.create("-0.0123456789"),
        HiveDecimal.create("-0.00123456789"),
        HiveDecimal.create("-0.000123456789"),
    };

    static Object[] nrString = {
        "abcdefg"
    };

    static Object[] nrDate = {
        Date.valueOf("2001-01-01")
    };

    static Object[] nrIntervalYearMonth = {
      HiveIntervalYearMonth.valueOf("1-0")
    };

    static Object[] nrIntervalDayTime = {
      HiveIntervalDayTime.valueOf("1 0:0:0")
    };

    public static void nonRandomRowFill(Object[][] rows, PrimitiveCategory[] primitiveCategories) {
      int minCount = Math.min(rows.length, nrDecimal.length);
      for (int i = 0; i < minCount; i++) {
        Object[] row = rows[i];
        for (int c = 0; c < primitiveCategories.length; c++) {
          Object object = row[c];  // Current value.
          switch (primitiveCategories[c]) {
          case BOOLEAN:
            // Use current for now.
            break;
          case BYTE:
            object = nrByte;
            break;
          case SHORT:
            object = nrShort;
            break;
          case INT:
            object = nrInt;
            break;
          case LONG:
            object = nrLong;
            break;
          case DATE:
            object = nrDate;
            break;
          case FLOAT:
            object = nrFloat;
            break;
          case DOUBLE:
            object = nrDouble;
            break;
          case STRING:
            object = nrString;
            break;
          case CHAR:
            // Use current for now.
            break;
          case VARCHAR:
            // Use current for now.
            break;
          case BINARY:
            // Use current for now.
            break;
          case TIMESTAMP:
            // Use current for now.
            break;
          case INTERVAL_YEAR_MONTH:
            object = nrIntervalYearMonth;
            break;
          case INTERVAL_DAY_TIME:
            object = nrIntervalDayTime;
            break;
          case DECIMAL:
            object = nrDecimal[i];
            break;
          default:
            throw new Error("Unknown primitive category " + primitiveCategories[c]);
          }
        }
      }
    }
}
