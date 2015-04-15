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

package org.apache.hadoop.hive.ql.exec.vector;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
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
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hive.common.util.DateUtils;

/**
 * Generate object inspector and random row object[].
 */
public class RandomRowObjectSource {

  private Random r;

  private int columnCount;

  private List<String> typeNames;

  private PrimitiveCategory[] primitiveCategories;

  private PrimitiveTypeInfo[] primitiveTypeInfos;

  private List<ObjectInspector> primitiveObjectInspectorList;

  private StructObjectInspector rowStructObjectInspector;

  public List<String> typeNames() {
    return typeNames;
  }

  public PrimitiveCategory[] primitiveCategories() {
    return primitiveCategories;
  }

  public PrimitiveTypeInfo[] primitiveTypeInfos() {
    return primitiveTypeInfos;
  }

  public StructObjectInspector rowStructObjectInspector() {
    return rowStructObjectInspector;
  }

  public void init(Random r) {
    this.r = r;
    chooseSchema();
  }

  private static String[] possibleHiveTypeNames = {
      "boolean",
      "tinyint",
      "smallint",
      "int",
      "bigint",
      "date",
      "float",
      "double",
      "string",
      "char",
      "varchar",
      "binary",
      "date",
      "timestamp",
      serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME,
      serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME,
      "decimal"
  };

  private void chooseSchema() {
    columnCount = 1 + r.nextInt(20);
    typeNames = new ArrayList<String>(columnCount);
    primitiveCategories = new PrimitiveCategory[columnCount];
    primitiveTypeInfos = new PrimitiveTypeInfo[columnCount];
    primitiveObjectInspectorList = new ArrayList<ObjectInspector>(columnCount);
    List<String> columnNames = new ArrayList<String>(columnCount);
    for (int c = 0; c < columnCount; c++) {
      columnNames.add(String.format("col%d", c));
      int typeNum = r.nextInt(possibleHiveTypeNames.length);
      String typeName = possibleHiveTypeNames[typeNum];
      if (typeName.equals("char")) {
        int maxLength = 1 + r.nextInt(100);
        typeName = String.format("char(%d)", maxLength);
      } else if (typeName.equals("varchar")) {
        int maxLength = 1 + r.nextInt(100);
        typeName = String.format("varchar(%d)", maxLength);
      } else if (typeName.equals("decimal")) {
        typeName = String.format("decimal(%d,%d)", HiveDecimal.SYSTEM_DEFAULT_PRECISION, HiveDecimal.SYSTEM_DEFAULT_SCALE);
      }
      PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(typeName);
      primitiveTypeInfos[c] = primitiveTypeInfo;
      PrimitiveCategory primitiveCategory = primitiveTypeInfo.getPrimitiveCategory();
      primitiveCategories[c] = primitiveCategory;
      primitiveObjectInspectorList.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(primitiveTypeInfo));
      typeNames.add(typeName);
    }
    rowStructObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, primitiveObjectInspectorList);
  }

  public Object[][] randomRows(int n) {
    Object[][] result = new Object[n][];
    for (int i = 0; i < n; i++) {
      result[i] = randomRow();
    }
    return result;
  }

  public Object[] randomRow() {
    Object row[] = new Object[columnCount];
    for (int c = 0; c < columnCount; c++) {
      Object object = randomObject(c);
      if (object == null) {
        throw new Error("Unexpected null for column " + c);
      }
      row[c] = getWritableObject(c, object);
      if (row[c] == null) {
        throw new Error("Unexpected null for writable for column " + c);
      }
    }
    return row;
  }

  public Object getWritableObject(int column, Object object) {
    ObjectInspector objectInspector = primitiveObjectInspectorList.get(column);
    PrimitiveCategory primitiveCategory = primitiveCategories[column];
    PrimitiveTypeInfo primitiveTypeInfo = primitiveTypeInfos[column];
    switch (primitiveCategory) {
    case BOOLEAN:
      return ((WritableBooleanObjectInspector) objectInspector).create((boolean) object);
    case BYTE:
      return ((WritableByteObjectInspector) objectInspector).create((byte) object);
    case SHORT:
      return ((WritableShortObjectInspector) objectInspector).create((short) object);
    case INT:
      return ((WritableIntObjectInspector) objectInspector).create((int) object);
    case LONG:
      return ((WritableLongObjectInspector) objectInspector).create((long) object);
    case DATE:
      return ((WritableDateObjectInspector) objectInspector).create((Date) object);
    case FLOAT:
      return ((WritableFloatObjectInspector) objectInspector).create((float) object);
    case DOUBLE:
      return ((WritableDoubleObjectInspector) objectInspector).create((double) object);
    case STRING:
      return ((WritableStringObjectInspector) objectInspector).create((String) object);
    case CHAR:
      {
        WritableHiveCharObjectInspector writableCharObjectInspector = 
                new WritableHiveCharObjectInspector( (CharTypeInfo) primitiveTypeInfo);
        return writableCharObjectInspector.create(new HiveChar(StringUtils.EMPTY, -1));
      }
    case VARCHAR:
      {
        WritableHiveVarcharObjectInspector writableVarcharObjectInspector = 
                new WritableHiveVarcharObjectInspector( (VarcharTypeInfo) primitiveTypeInfo);
        return writableVarcharObjectInspector.create(new HiveVarchar(StringUtils.EMPTY, -1));
      }
    case BINARY:
      return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector.create(ArrayUtils.EMPTY_BYTE_ARRAY);
    case TIMESTAMP:
      return ((WritableTimestampObjectInspector) objectInspector).create(new Timestamp(0));
    case INTERVAL_YEAR_MONTH:
      return ((WritableHiveIntervalYearMonthObjectInspector) objectInspector).create(new HiveIntervalYearMonth(0));
    case INTERVAL_DAY_TIME:
      return ((WritableHiveIntervalDayTimeObjectInspector) objectInspector).create(new HiveIntervalDayTime(0, 0));
    case DECIMAL:
      {
        WritableHiveDecimalObjectInspector writableDecimalObjectInspector =
                new WritableHiveDecimalObjectInspector((DecimalTypeInfo) primitiveTypeInfo);
        return writableDecimalObjectInspector.create(HiveDecimal.ZERO);
      }
    default:
      throw new Error("Unknown primitive category " + primitiveCategory);
    }
  }

  public Object randomObject(int column) {
    PrimitiveCategory primitiveCategory = primitiveCategories[column];
    PrimitiveTypeInfo primitiveTypeInfo = primitiveTypeInfos[column];
    switch (primitiveCategory) {
    case BOOLEAN:
      return Boolean.valueOf(r.nextInt(1) == 1);
    case BYTE:
      return Byte.valueOf((byte) r.nextInt());
    case SHORT:
      return Short.valueOf((short) r.nextInt());
    case INT:
      return Integer.valueOf(r.nextInt());
    case LONG:
      return Long.valueOf(r.nextLong());
    case DATE:
      return getRandDate(r);
    case FLOAT:
      return Float.valueOf(r.nextFloat() * 10 - 5);
    case DOUBLE:
      return Double.valueOf(r.nextDouble() * 10 - 5);
    case STRING:
      return getRandString(r);
    case CHAR:
      return getRandHiveChar(r, (CharTypeInfo) primitiveTypeInfo);
    case VARCHAR:
      return getRandHiveVarchar(r, (VarcharTypeInfo) primitiveTypeInfo);
    case BINARY:
      return getRandBinary(r, 1 + r.nextInt(100));
    case TIMESTAMP:
      return getRandTimestamp(r);
    case INTERVAL_YEAR_MONTH:
      return getRandIntervalYearMonth(r);
    case INTERVAL_DAY_TIME:
      return getRandIntervalDayTime(r);
    case DECIMAL:
      return getRandHiveDecimal(r, (DecimalTypeInfo) primitiveTypeInfo);
    default:
      throw new Error("Unknown primitive category " + primitiveCategory);
    }
  }

  public static String getRandString(Random r) {
    return getRandString(r, null, r.nextInt(10));
  }

  public static String getRandString(Random r, String characters, int length) {
    if (characters == null) {
      characters = "ABCDEFGHIJKLMabcdefghijklm";
    }
    StringBuilder sb = new StringBuilder();
    sb.append("");
    for (int i = 0; i < length; i++) {
      if (characters == null) {
        sb.append((char) (r.nextInt(128)));
      } else {
        sb.append(characters.charAt(r.nextInt(characters.length())));
      }
    }
    return sb.toString();
  }

  public static HiveChar getRandHiveChar(Random r, CharTypeInfo charTypeInfo) {
    int maxLength = 1 + r.nextInt(charTypeInfo.getLength());
    String randomString = getRandString(r, "abcdefghijklmnopqrstuvwxyz", 100);
    HiveChar hiveChar = new HiveChar(randomString, maxLength);
    return hiveChar;
  }

  public static HiveVarchar getRandHiveVarchar(Random r, VarcharTypeInfo varcharTypeInfo) {
    int maxLength = 1 + r.nextInt(varcharTypeInfo.getLength());
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

  public static HiveDecimal getRandHiveDecimal(Random r, DecimalTypeInfo decimalTypeInfo) {
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

      HiveDecimal bd = HiveDecimal.create(sb.toString());
      if (bd.scale() > bd.precision()) {
        // Sometimes weird decimals are produced?
        continue;
      }

      return bd;
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

  public static Timestamp getRandTimestamp(Random r) {
    String optionalNanos = "";
    if (r.nextInt(2) == 1) {
      optionalNanos = String.format(".%09d",
          Integer.valueOf(0 + r.nextInt(DateUtils.NANOS_PER_SEC)));
    }
    String timestampStr = String.format("%d-%02d-%02d %02d:%02d:%02d%s",
        Integer.valueOf(1970 + r.nextInt(200)),  // year
        Integer.valueOf(1 + r.nextInt(12)),      // month
        Integer.valueOf(1 + r.nextInt(28)),      // day
        Integer.valueOf(0 + r.nextInt(24)),      // hour
        Integer.valueOf(0 + r.nextInt(60)),      // minute
        Integer.valueOf(0 + r.nextInt(60)),      // second
        optionalNanos);
    Timestamp timestampVal = Timestamp.valueOf(timestampStr);
    return timestampVal;
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
}
