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
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.RandomTypeUtil;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
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
import org.apache.hive.common.util.DateUtils;

import com.google.common.base.Charsets;

/**
 * Generate object inspector and random row object[].
 */
public class VectorRandomRowSource {

  private Random r;

  private int columnCount;

  private List<String> typeNames;

  private PrimitiveCategory[] primitiveCategories;

  private PrimitiveTypeInfo[] primitiveTypeInfos;

  private List<ObjectInspector> primitiveObjectInspectorList;

  private StructObjectInspector rowStructObjectInspector;

  private String[] alphabets;

  private boolean addEscapables;
  private String needsEscapeStr;

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

  public StructObjectInspector partialRowStructObjectInspector(int partialFieldCount) {
    ArrayList<ObjectInspector> partialPrimitiveObjectInspectorList =
        new ArrayList<ObjectInspector>(partialFieldCount);
    List<String> columnNames = new ArrayList<String>(partialFieldCount);
    for (int i = 0; i < partialFieldCount; i++) {
      columnNames.add(String.format("partial%d", i));
      partialPrimitiveObjectInspectorList.add(
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
              primitiveTypeInfos[i]));
    }

    return ObjectInspectorFactory.getStandardStructObjectInspector(
        columnNames, primitiveObjectInspectorList);
  }

  public void init(Random r) {
    this.r = r;
    chooseSchema();
  }

  /*
   * For now, exclude CHAR until we determine why there is a difference (blank padding)
   * serializing with LazyBinarySerializeWrite and the regular SerDe...
   */
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
//    "char",
      "varchar",
      "binary",
      "date",
      "timestamp",
      "interval_year_month",
      "interval_day_time",
      "decimal"
  };

  private void chooseSchema() {
    HashSet hashSet = null;
    boolean allTypes;
    boolean onlyOne = (r.nextInt(100) == 7);
    if (onlyOne) {
      columnCount = 1;
      allTypes = false;
    } else {
      allTypes = r.nextBoolean();
      if (allTypes) {
        // One of each type.
        columnCount = possibleHiveTypeNames.length;
        hashSet = new HashSet<Integer>();
      } else {
        columnCount = 1 + r.nextInt(20);
      }
    }
    typeNames = new ArrayList<String>(columnCount);
    primitiveCategories = new PrimitiveCategory[columnCount];
    primitiveTypeInfos = new PrimitiveTypeInfo[columnCount];
    primitiveObjectInspectorList = new ArrayList<ObjectInspector>(columnCount);
    List<String> columnNames = new ArrayList<String>(columnCount);
    for (int c = 0; c < columnCount; c++) {
      columnNames.add(String.format("col%d", c));
      String typeName;

      if (onlyOne) {
        typeName = possibleHiveTypeNames[r.nextInt(possibleHiveTypeNames.length)];
      } else {
        int typeNum;
        if (allTypes) {
          while (true) {
            typeNum = r.nextInt(possibleHiveTypeNames.length);
            Integer typeNumInteger = new Integer(typeNum);
            if (!hashSet.contains(typeNumInteger)) {
              hashSet.add(typeNumInteger);
              break;
            }
          }
        } else {
          typeNum = r.nextInt(possibleHiveTypeNames.length);
        }
        typeName = possibleHiveTypeNames[typeNum];
      }
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
    alphabets = new String[columnCount];
  }

  public void addBinarySortableAlphabets() {
    for (int c = 0; c < columnCount; c++) {
      switch (primitiveCategories[c]) {
      case STRING:
      case CHAR:
      case VARCHAR:
        byte[] bytes = new byte[10 + r.nextInt(10)];
        for (int i = 0; i < bytes.length; i++) {
          bytes[i] = (byte) (32 + r.nextInt(96));
        }
        int alwaysIndex = r.nextInt(bytes.length);
        bytes[alwaysIndex] = 0;  // Must be escaped by BinarySortable.
        int alwaysIndex2 = r.nextInt(bytes.length);
        bytes[alwaysIndex2] = 1;  // Must be escaped by BinarySortable.
        alphabets[c] = new String(bytes, Charsets.UTF_8);
        break;
      default:
        // No alphabet needed.
        break;
      }
    }
  }

  public void addEscapables(String needsEscapeStr) {
    addEscapables = true;
    this.needsEscapeStr = needsEscapeStr;
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

  public Object[] randomRow(int columnCount) {
    return randomRow(columnCount, r, primitiveObjectInspectorList, primitiveCategories,
        primitiveTypeInfos);
  }

  public static Object[] randomRow(int columnCount, Random r,
      List<ObjectInspector> primitiveObjectInspectorList, PrimitiveCategory[] primitiveCategories,
      PrimitiveTypeInfo[] primitiveTypeInfos) {
    Object row[] = new Object[columnCount];
    for (int c = 0; c < columnCount; c++) {
      Object object = randomObject(c, r, primitiveCategories, primitiveTypeInfos);
      if (object == null) {
        throw new Error("Unexpected null for column " + c);
      }
      row[c] = getWritableObject(c, object, primitiveObjectInspectorList,
          primitiveCategories, primitiveTypeInfos);
      if (row[c] == null) {
        throw new Error("Unexpected null for writable for column " + c);
      }
    }
    return row;
  }

  public static void sort(Object[][] rows, ObjectInspector oi) {
    for (int i = 0; i < rows.length; i++) {
      for (int j = i + 1; j < rows.length; j++) {
        if (ObjectInspectorUtils.compare(rows[i], oi, rows[j], oi) > 0) {
          Object[] t = rows[i];
          rows[i] = rows[j];
          rows[j] = t;
        }
      }
    }
  }

  public void sort(Object[][] rows) {
    VectorRandomRowSource.sort(rows, rowStructObjectInspector);
  }

  public Object getWritableObject(int column, Object object) {
    return getWritableObject(column, object, primitiveObjectInspectorList,
        primitiveCategories, primitiveTypeInfos);
  }

  public static Object getWritableObject(int column, Object object,
      List<ObjectInspector> primitiveObjectInspectorList, PrimitiveCategory[] primitiveCategories,
      PrimitiveTypeInfo[] primitiveTypeInfos) {
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
        return writableCharObjectInspector.create((HiveChar) object);
      }
    case VARCHAR:
      {
        WritableHiveVarcharObjectInspector writableVarcharObjectInspector =
                new WritableHiveVarcharObjectInspector( (VarcharTypeInfo) primitiveTypeInfo);
        return writableVarcharObjectInspector.create((HiveVarchar) object);
      }
    case BINARY:
      return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector.create((byte[]) object);
    case TIMESTAMP:
      return ((WritableTimestampObjectInspector) objectInspector).create((Timestamp) object);
    case INTERVAL_YEAR_MONTH:
      return ((WritableHiveIntervalYearMonthObjectInspector) objectInspector).create((HiveIntervalYearMonth) object);
    case INTERVAL_DAY_TIME:
      return ((WritableHiveIntervalDayTimeObjectInspector) objectInspector).create((HiveIntervalDayTime) object);
    case DECIMAL:
      {
        WritableHiveDecimalObjectInspector writableDecimalObjectInspector =
                new WritableHiveDecimalObjectInspector((DecimalTypeInfo) primitiveTypeInfo);
        HiveDecimalWritable result = (HiveDecimalWritable) writableDecimalObjectInspector.create((HiveDecimal) object);
        return result;
      }
    default:
      throw new Error("Unknown primitive category " + primitiveCategory);
    }
  }

  public Object randomObject(int column) {
    return randomObject(column, r, primitiveCategories, primitiveTypeInfos, alphabets, addEscapables, needsEscapeStr);
  }

  public static Object randomObject(int column, Random r, PrimitiveCategory[] primitiveCategories,
      PrimitiveTypeInfo[] primitiveTypeInfos) {
    return randomObject(column, r, primitiveCategories, primitiveTypeInfos, null, false, "");
  }

  public static Object randomObject(int column, Random r, PrimitiveCategory[] primitiveCategories,
      PrimitiveTypeInfo[] primitiveTypeInfos, String[] alphabets, boolean addEscapables, String needsEscapeStr) {
    PrimitiveCategory primitiveCategory = primitiveCategories[column];
    PrimitiveTypeInfo primitiveTypeInfo = primitiveTypeInfos[column];
    try {
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
        return RandomTypeUtil.getRandDate(r);
      case FLOAT:
        return Float.valueOf(r.nextFloat() * 10 - 5);
      case DOUBLE:
        return Double.valueOf(r.nextDouble() * 10 - 5);
      case STRING:
      case CHAR:
      case VARCHAR:
        {
          String result;
          if (alphabets != null && alphabets[column] != null) {
            result = RandomTypeUtil.getRandString(r, alphabets[column], r.nextInt(10));
          } else {
            result = RandomTypeUtil.getRandString(r);
          }
          if (addEscapables && result.length() > 0) {
            int escapeCount = 1 + r.nextInt(2);
            for (int i = 0; i < escapeCount; i++) {
              int index = r.nextInt(result.length());
              String begin = result.substring(0, index);
              String end = result.substring(index);
              Character needsEscapeChar = needsEscapeStr.charAt(r.nextInt(needsEscapeStr.length()));
              result = begin + needsEscapeChar + end;
            }
          }
          switch (primitiveCategory) {
          case STRING:
            return result;
          case CHAR:
            return new HiveChar(result, ((CharTypeInfo) primitiveTypeInfo).getLength());
          case VARCHAR:
            return new HiveVarchar(result, ((VarcharTypeInfo) primitiveTypeInfo).getLength());
          default:
            throw new Error("Unknown primitive category " + primitiveCategory);
          }
        }
      case BINARY:
        return getRandBinary(r, 1 + r.nextInt(100));
      case TIMESTAMP:
        return RandomTypeUtil.getRandTimestamp(r);
      case INTERVAL_YEAR_MONTH:
        return getRandIntervalYearMonth(r);
      case INTERVAL_DAY_TIME:
        return getRandIntervalDayTime(r);
      case DECIMAL:
        return getRandHiveDecimal(r, (DecimalTypeInfo) primitiveTypeInfo);
      default:
        throw new Error("Unknown primitive category " + primitiveCategory);
      }
    } catch (Exception e) {
      throw new RuntimeException("randomObject failed on column " + column + " type " + primitiveCategory, e);
    }
  }

  public static HiveChar getRandHiveChar(Random r, CharTypeInfo charTypeInfo, String alphabet) {
    int maxLength = 1 + r.nextInt(charTypeInfo.getLength());
    String randomString = RandomTypeUtil.getRandString(r, alphabet, 100);
    HiveChar hiveChar = new HiveChar(randomString, maxLength);
    return hiveChar;
  }

  public static HiveChar getRandHiveChar(Random r, CharTypeInfo charTypeInfo) {
    return getRandHiveChar(r, charTypeInfo, "abcdefghijklmnopqrstuvwxyz");
  }

  public static HiveVarchar getRandHiveVarchar(Random r, VarcharTypeInfo varcharTypeInfo, String alphabet) {
    int maxLength = 1 + r.nextInt(varcharTypeInfo.getLength());
    String randomString = RandomTypeUtil.getRandString(r, alphabet, 100);
    HiveVarchar hiveVarchar = new HiveVarchar(randomString, maxLength);
    return hiveVarchar;
  }

  public static HiveVarchar getRandHiveVarchar(Random r, VarcharTypeInfo varcharTypeInfo) {
    return getRandHiveVarchar(r, varcharTypeInfo, "abcdefghijklmnopqrstuvwxyz");
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
        sb.append(RandomTypeUtil.getRandString(r, DECIMAL_CHARS, integerDigits));
      }
      if (scale != 0) {
        sb.append(".");
        sb.append(RandomTypeUtil.getRandString(r, DECIMAL_CHARS, scale));
      }

      HiveDecimal dec = HiveDecimal.create(sb.toString());

      return dec;
    }
  }

  public static HiveIntervalYearMonth getRandIntervalYearMonth(Random r) {
    String yearMonthSignStr = r.nextInt(2) == 0 ? "" : "-";
    String intervalYearMonthStr = String.format("%s%d-%d",
        yearMonthSignStr,
        Integer.valueOf(1800 + r.nextInt(500)),  // year
        Integer.valueOf(0 + r.nextInt(12)));     // month
    HiveIntervalYearMonth intervalYearMonthVal = HiveIntervalYearMonth.valueOf(intervalYearMonthStr);
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
    return intervalDayTimeVal;
  }
}
