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
package org.apache.hadoop.hive.serde2.binarysortable;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.RandomTypeUtil;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hive.common.util.DateUtils;

import junit.framework.TestCase;

// Just the primitive types.
public class MyTestPrimitiveClass {

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

    public MyTestPrimitiveClass() {
    }

    public final static int primitiveCount = 16;

    public int randomFill(Random r, ExtraTypeInfo extraTypeInfo) {
      int randField = r.nextInt(primitiveCount);
      int field = 0;
      return randomFill(r, randField, field, extraTypeInfo);
    }

    public boolean chooseNull(Random r, int randField, int field) {
      if (randField == field) {
        return true;
      }
      return (r.nextInt(5) == 0);
    }

    public int randomFill(Random r, int randField, int field, ExtraTypeInfo extraTypeInfo) {
      myBool = chooseNull(r, randField, field++) ? null : Boolean.valueOf(r.nextInt(1) == 1);
      myByte = chooseNull(r, randField, field++) ? null : Byte.valueOf((byte) r.nextInt());
      myShort = chooseNull(r, randField, field++) ? null : Short.valueOf((short) r.nextInt());
      myInt = chooseNull(r, randField, field++) ? null : Integer.valueOf(r.nextInt());
      myLong = chooseNull(r, randField, field++) ? null : Long.valueOf(r.nextLong());
      myFloat = chooseNull(r, randField, field++) ? null : Float
          .valueOf(r.nextFloat() * 10 - 5);
      myDouble = chooseNull(r, randField, field++) ? null : Double
          .valueOf(r.nextDouble() * 10 - 5);
      myString = chooseNull(r, randField, field++) ? null : getRandString(r);
      myHiveChar = chooseNull(r, randField, field++) ? null : getRandHiveChar(r, extraTypeInfo);
      myHiveVarchar = chooseNull(r, randField, field++) ? null : getRandHiveVarchar(r, extraTypeInfo);
      myBinary = getRandBinary(r, r.nextInt(1000));
      myDecimal = chooseNull(r, randField, field++) ? null : getRandHiveDecimal(r, extraTypeInfo);
      myDate = chooseNull(r, randField, field++) ? null : getRandDate(r);
      myTimestamp = chooseNull(r, randField, field++) ? null : RandomTypeUtil.getRandTimestamp(r);
      myIntervalYearMonth = chooseNull(r, randField, field++) ? null : getRandIntervalYearMonth(r);
      myIntervalDayTime = chooseNull(r, randField, field++) ? null : getRandIntervalDayTime(r);
      return field;
    }

    public static class ExtraTypeInfo {
      public int hiveCharMaxLength;
      public int hiveVarcharMaxLength;
      public int precision;
      public int scale;

      public ExtraTypeInfo() {
        // For NULL fields, make up a valid max length.
        hiveCharMaxLength = 1;
        hiveVarcharMaxLength = 1;
        precision = HiveDecimal.SYSTEM_DEFAULT_PRECISION;
        scale = HiveDecimal.SYSTEM_DEFAULT_SCALE;
      }
    }

    public static PrimitiveTypeInfo[] getPrimitiveTypeInfos(ExtraTypeInfo extraTypeInfo) {
      PrimitiveTypeInfo[] primitiveTypeInfos = new PrimitiveTypeInfo[primitiveCount];
      for (int i = 0; i < primitiveCount; i++) {
        primitiveTypeInfos[i] = getPrimitiveTypeInfo(i, extraTypeInfo);
      }
      return primitiveTypeInfos;
    }

    public static String getRandString(Random r) {
      return getRandString(r, null, r.nextInt(10));
    }

    public static String getRandString(Random r, String characters, int length) {
      if (characters == null) {
        characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        
      }
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < length; i++) {
        if (characters == null) {
          sb.append((char) (r.nextInt(128)));
        } else {
          sb.append(characters.charAt(r.nextInt(characters.length())));
        }
      }
      return sb.toString();
    }

    public static HiveChar getRandHiveChar(Random r, ExtraTypeInfo extraTypeInfo) {
      int maxLength = 10 + r.nextInt(60);
      extraTypeInfo.hiveCharMaxLength = maxLength;
      String randomString = getRandString(r, "abcdefghijklmnopqrstuvwxyz", 100);
      HiveChar hiveChar = new HiveChar(randomString, maxLength);
      return hiveChar;
    }

    public static HiveVarchar getRandHiveVarchar(Random r, ExtraTypeInfo extraTypeInfo) {
      int maxLength = 10 + r.nextInt(60);
      extraTypeInfo.hiveVarcharMaxLength = maxLength;
      String randomString = getRandString(r, "abcdefghijklmnopqrstuvwxyz", 100);
      HiveVarchar hiveVarchar = new HiveVarchar(randomString, maxLength);
      return hiveVarchar;
    }

    public static byte[] getRandBinary(Random r, int len){
      byte[] bytes = new byte[len];
      for (int j = 0; j < len; j++){
        bytes[j] = Byte.valueOf((byte) r.nextInt());
      }
      return bytes;
    }

    private static final String DECIMAL_CHARS = "0123456789";

    public static HiveDecimal getRandHiveDecimal(Random r, ExtraTypeInfo extraTypeInfo) {
      while (true) {
        StringBuilder sb = new StringBuilder();
        int precision = 1 + r.nextInt(18);
        int scale = 0 + r.nextInt(precision + 1);
  
        int integerDigits = precision - scale;

        if (r.nextBoolean()) {
          sb.append("-");
        }

        if (integerDigits == 0) {
          sb.append("0");
        } else {
          sb.append(getRandString(r, DECIMAL_CHARS, integerDigits));
        }
        if (scale != 0) {
          sb.append(".");
          sb.append(getRandString(r, DECIMAL_CHARS, scale));
        }

        HiveDecimal dec = HiveDecimal.create(sb.toString());
        extraTypeInfo.precision = dec.precision();
        extraTypeInfo.scale = dec.scale();
        return dec;
      }
    }

    public static Date getRandDate(Random r) {
      String dateStr = String.format("%d-%02d-%02d",
          Integer.valueOf(1800 + r.nextInt(500)),  // year
          Integer.valueOf(1 + r.nextInt(12)),      // month
          Integer.valueOf(1 + r.nextInt(28)));     // day
      Date dateVal = Date.valueOf(dateStr);
      return dateVal;
    }

    public static HiveIntervalYearMonth getRandIntervalYearMonth(Random r) {
      String yearMonthSignStr = r.nextInt(2) == 0 ? "" : "-";
      String intervalYearMonthStr = String.format("%s%d-%d",
          yearMonthSignStr,
          Integer.valueOf(1800 + r.nextInt(500)),  // year
          Integer.valueOf(0 + r.nextInt(12)));     // month
      HiveIntervalYearMonth intervalYearMonthVal = HiveIntervalYearMonth.valueOf(intervalYearMonthStr);
      TestCase.assertTrue(intervalYearMonthVal != null);
      return intervalYearMonthVal;
    }

    public static HiveIntervalDayTime getRandIntervalDayTime(Random r) {
      String optionalNanos = "";
      if (r.nextInt(2) == 1) {
        optionalNanos = String.format(".%09d",
            Integer.valueOf(0 + r.nextInt(DateUtils.NANOS_PER_SEC)));
      }
      String yearMonthSignStr = r.nextInt(2) == 0 ? "" : "-";
      String dayTimeStr = String.format("%s%d %02d:%02d:%02d%s",
          yearMonthSignStr,
          Integer.valueOf(1 + r.nextInt(28)),      // day
          Integer.valueOf(0 + r.nextInt(24)),      // hour
          Integer.valueOf(0 + r.nextInt(60)),      // minute
          Integer.valueOf(0 + r.nextInt(60)),      // second
          optionalNanos);
      HiveIntervalDayTime intervalDayTimeVal = HiveIntervalDayTime.valueOf(dayTimeStr);
      TestCase.assertTrue(intervalDayTimeVal != null);
      return intervalDayTimeVal;
    }

    public Object getPrimitiveObject(int index) {
      int field = 0;
      if (index == field++) {
        return myBool;
      } else if (index == field++) {
        return myByte;
      } else if (index == field++) {
        return myShort;
      } else if (index == field++) {
        return myInt;
      } else if (index == field++) {
        return myLong;
      } else if (index == field++) {
        return myFloat;
      } else if (index == field++) {
        return myDouble;
      } else if (index == field++) {
        return myString;
      } else if (index == field++) {
        return myHiveChar;
      } else if (index == field++) {
        return myHiveVarchar;
      } else if (index == field++) {
        return myBinary;
      } else if (index == field++) {
        return myDecimal;
      } else if (index == field++) {
        return myDate;
      } else if (index == field++) {
        return myTimestamp;
      } else if (index == field++) {
        return myIntervalYearMonth;
      } else if (index == field++) {
        return myIntervalDayTime;
      } else {
        throw new Error("Field " + " field not handled");
      }
    }

    public Object getPrimitiveWritableObject(int index, PrimitiveTypeInfo primitiveTypeInfo) {
      int field = 0;
      if (index == field++) {
        return (myBool == null ? null : PrimitiveObjectInspectorFactory.writableBooleanObjectInspector.create((boolean) myBool));
      } else if (index == field++) {
        return (myByte == null ? null : PrimitiveObjectInspectorFactory.writableByteObjectInspector.create((byte) myByte));
      } else if (index == field++) {
        return (myShort == null ? null : PrimitiveObjectInspectorFactory.writableShortObjectInspector.create((short) myShort));
      } else if (index == field++) {
        return (myInt == null ? null : PrimitiveObjectInspectorFactory.writableIntObjectInspector.create((int) myInt));
      } else if (index == field++) {
        return (myLong == null ? null : PrimitiveObjectInspectorFactory.writableLongObjectInspector.create((long) myLong));
      } else if (index == field++) {
        return (myFloat == null ? null : PrimitiveObjectInspectorFactory.writableFloatObjectInspector.create((float) myFloat));
      } else if (index == field++) {
        return (myDouble == null ? null : PrimitiveObjectInspectorFactory.writableDoubleObjectInspector.create((double) myDouble));
      } else if (index == field++) {
        return (myString == null ? null : PrimitiveObjectInspectorFactory.writableStringObjectInspector.create(myString));
      } else if (index == field++) {
        if (myHiveChar == null) {
          return null;
        }
        CharTypeInfo charTypeInfo = (CharTypeInfo) primitiveTypeInfo;
        WritableHiveCharObjectInspector writableCharObjectInspector = new WritableHiveCharObjectInspector(charTypeInfo);
        return writableCharObjectInspector.create(myHiveChar);
      } else if (index == field++) {
        if (myHiveVarchar == null) {
          return null;
        }
        VarcharTypeInfo varcharTypeInfo = (VarcharTypeInfo) primitiveTypeInfo;
        WritableHiveVarcharObjectInspector writableVarcharObjectInspector = new WritableHiveVarcharObjectInspector(varcharTypeInfo);
        return writableVarcharObjectInspector.create(myHiveVarchar);
      } else if (index == field++) {
        return (myBinary == null ? null : PrimitiveObjectInspectorFactory.writableBinaryObjectInspector.create(myBinary));
      } else if (index == field++) {
        if (myDecimal == null) {
          return null;
        }
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveTypeInfo;
        WritableHiveDecimalObjectInspector writableDecimalObjectInspector = new WritableHiveDecimalObjectInspector(decimalTypeInfo);
        return writableDecimalObjectInspector.create(myDecimal);
      } else if (index == field++) {
        return (myDate == null ? null : PrimitiveObjectInspectorFactory.writableDateObjectInspector.create(myDate));
      } else if (index == field++) {
        return (myTimestamp == null ? null : PrimitiveObjectInspectorFactory.writableTimestampObjectInspector.create(myTimestamp));
      } else if (index == field++) {
        return (myIntervalYearMonth == null ? null : PrimitiveObjectInspectorFactory.writableHiveIntervalYearMonthObjectInspector.create(myIntervalYearMonth));
      } else if (index == field++) {
        return (myIntervalDayTime == null ? null : PrimitiveObjectInspectorFactory.writableHiveIntervalDayTimeObjectInspector.create(myIntervalDayTime));
      } else {
        throw new Error("Field " + " field not handled");
      }
    }


    public static PrimitiveCategory getPrimitiveCategory(int index) {
      int field = 0;
      if (index == field++) {
        return PrimitiveCategory.BOOLEAN;
      } else if (index == field++) {
        return PrimitiveCategory.BYTE;
      } else if (index == field++) {
        return PrimitiveCategory.SHORT;
      } else if (index == field++) {
        return PrimitiveCategory.INT;
      } else if (index == field++) {
        return PrimitiveCategory.LONG;
      } else if (index == field++) {
        return PrimitiveCategory.FLOAT;
      } else if (index == field++) {
        return PrimitiveCategory.DOUBLE;
      } else if (index == field++) {
        return PrimitiveCategory.STRING;
      } else if (index == field++) {
        return PrimitiveCategory.CHAR;
      } else if (index == field++) {
        return PrimitiveCategory.VARCHAR;
      } else if (index == field++) {
        return PrimitiveCategory.BINARY;
      } else if (index == field++) {
        return PrimitiveCategory.DECIMAL;
      } else if (index == field++) {
        return PrimitiveCategory.DATE;
      } else if (index == field++) {
        return PrimitiveCategory.TIMESTAMP;
      } else if (index == field++) {
        return PrimitiveCategory.INTERVAL_YEAR_MONTH;
      } else if (index == field++) {
        return PrimitiveCategory.INTERVAL_DAY_TIME;
      } else {
        throw new Error("Field " + " field not handled");
      }
    }

    public static PrimitiveTypeInfo getPrimitiveTypeInfo(int index, ExtraTypeInfo extraTypeInfo) {
      PrimitiveCategory primitiveCategory = getPrimitiveCategory(index);
      String typeName;
      switch (primitiveCategory) {
      case BYTE:
        typeName = "tinyint";
        break;
      case SHORT:
        typeName = "smallint";
        break;
      case LONG:
        typeName = "bigint";
        break;
      case CHAR:
        typeName = String.format("char(%d)", extraTypeInfo.hiveCharMaxLength);
        break;
      case VARCHAR:
        typeName = String.format("varchar(%d)", extraTypeInfo.hiveVarcharMaxLength);
        break;
      case DECIMAL:
        typeName = String.format("decimal(%d,%d)", extraTypeInfo.precision, extraTypeInfo.scale);
        break;
      default:
        // No type name difference or adornment.
        typeName = primitiveCategory.name().toLowerCase();
        break;
      }
      PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(typeName);
      return primitiveTypeInfo;
    }

    public StructObjectInspector getRowInspector(PrimitiveTypeInfo[] primitiveTypeInfos) {
      List<String> columnNames = new ArrayList<String>(primitiveCount);
      List<ObjectInspector> primitiveObjectInspectorList = new ArrayList<ObjectInspector>(primitiveCount);
      for (int index = 0; index < MyTestPrimitiveClass.primitiveCount; index++) {
        columnNames.add(String.format("col%d", index));
        PrimitiveTypeInfo primitiveTypeInfo = primitiveTypeInfos[index];
        PrimitiveCategory primitiveCategory = primitiveTypeInfo.getPrimitiveCategory();
        primitiveObjectInspectorList.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(primitiveCategory));
      }
      StandardStructObjectInspector rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, primitiveObjectInspectorList);
      return rowOI;
    }

    public void nonRandomFill(int idx, ExtraTypeInfo extraTypeInfo) {
      myByte = (Byte) MyTestClass.getNonRandValue(MyTestClass.nrByte, idx);
      myShort = (Short) MyTestClass.getNonRandValue(MyTestClass.nrShort, idx);
      myInt = (Integer) MyTestClass.getNonRandValue(MyTestClass.nrInt, idx);
      myLong = (Long) MyTestClass.getNonRandValue(MyTestClass.nrLong, idx);
      myFloat = (Float) MyTestClass.getNonRandValue(MyTestClass.nrFloat, idx);
      myDouble = (Double) MyTestClass.getNonRandValue(MyTestClass.nrDouble, idx);
      myString = (String) MyTestClass.getNonRandValue(MyTestClass.nrString, idx);
      myHiveChar = new HiveChar(myString, myString.length());
      extraTypeInfo.hiveCharMaxLength = myString.length();
      myHiveVarchar = new HiveVarchar(myString, myString.length());
      extraTypeInfo.hiveVarcharMaxLength = myString.length();
      myDecimal = (HiveDecimal) MyTestClass.getNonRandValue(MyTestClass.nrDecimal, idx);
      extraTypeInfo.precision = myDecimal.precision();
      extraTypeInfo.scale = myDecimal.scale();
      myDate = (Date) MyTestClass.getNonRandValue(MyTestClass.nrDate, idx);
      myIntervalYearMonth = (HiveIntervalYearMonth) MyTestClass.getNonRandValue(MyTestClass.nrIntervalYearMonth, idx);
      myIntervalDayTime = (HiveIntervalDayTime) MyTestClass.getNonRandValue(MyTestClass.nrIntervalDayTime, idx);
    }
}
