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

package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.hive.common.type.TimestampUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * ObjectInspectorFactory is the primary way to create new ObjectInspector
 * instances.
 *
 * SerDe classes should call the static functions in this library to create an
 * ObjectInspector to return to the caller of SerDe2.getObjectInspector().
 */
public final class PrimitiveObjectInspectorUtils {

  /**
   * TypeEntry stores information about a Hive Primitive TypeInfo.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public static class PrimitiveTypeEntry implements Writable, Cloneable {

    /**
     * The category of the PrimitiveType.
     */
    public PrimitiveObjectInspector.PrimitiveCategory primitiveCategory;

    /**
     * primitiveJavaType refers to java types like int, double, etc.
     */
    public Class<?> primitiveJavaType;
    /**
     * primitiveJavaClass refers to java classes like Integer, Double, String
     * etc.
     */
    public Class<?> primitiveJavaClass;
    /**
     * writableClass refers to hadoop Writable classes like IntWritable,
     * DoubleWritable, Text etc.
     */
    public Class<?> primitiveWritableClass;
    /**
     * typeName is the name of the type as in DDL.
     */
    public String typeName;

    protected PrimitiveTypeEntry() {
      super();
    }

    PrimitiveTypeEntry(
        PrimitiveObjectInspector.PrimitiveCategory primitiveCategory,
        String typeName, Class<?> primitiveType, Class<?> javaClass,
        Class<?> hiveClass) {
      this.primitiveCategory = primitiveCategory;
      primitiveJavaType = primitiveType;
      primitiveJavaClass = javaClass;
      primitiveWritableClass = hiveClass;
      this.typeName = typeName;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      primitiveCategory = WritableUtils.readEnum(in,
          PrimitiveObjectInspector.PrimitiveCategory.class);
      typeName = WritableUtils.readString(in);
      try {
        primitiveJavaType = Class.forName(WritableUtils.readString(in));
        primitiveJavaClass = Class.forName(WritableUtils.readString(in));
        primitiveWritableClass = Class.forName(WritableUtils.readString(in));
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {

      WritableUtils.writeEnum(out, primitiveCategory);
      WritableUtils.writeString(out, typeName);
      WritableUtils.writeString(out, primitiveJavaType.getName());
      WritableUtils.writeString(out, primitiveJavaClass.getName());
      WritableUtils.writeString(out, primitiveWritableClass.getName());
    }

    @Override
    public Object clone() {
      PrimitiveTypeEntry result = new PrimitiveTypeEntry(
          this.primitiveCategory,
          this.typeName,
          this.primitiveJavaType,
          this.primitiveJavaClass,
          this.primitiveWritableClass);
      return result;
    }

    @Override
    public String toString() {
      return typeName;
    }

  }

  static final Map<PrimitiveCategory, PrimitiveTypeEntry> primitiveCategoryToTypeEntry = new HashMap<PrimitiveCategory, PrimitiveTypeEntry>();
  static final Map<Class<?>, PrimitiveTypeEntry> primitiveJavaTypeToTypeEntry = new HashMap<Class<?>, PrimitiveTypeEntry>();
  static final Map<Class<?>, PrimitiveTypeEntry> primitiveJavaClassToTypeEntry = new HashMap<Class<?>, PrimitiveTypeEntry>();
  static final Map<Class<?>, PrimitiveTypeEntry> primitiveWritableClassToTypeEntry = new HashMap<Class<?>, PrimitiveTypeEntry>();

  // Base type name to PrimitiveTypeEntry map.
  private static final Map<String, PrimitiveTypeEntry> typeNameToTypeEntry = new HashMap<String, PrimitiveTypeEntry>();

  static void addParameterizedType(PrimitiveTypeEntry t) {
    typeNameToTypeEntry.put(t.toString(), t);
  }

  static void registerType(PrimitiveTypeEntry t) {
    if (t.primitiveCategory != PrimitiveCategory.UNKNOWN) {
      primitiveCategoryToTypeEntry.put(t.primitiveCategory, t);
    }
    if (t.primitiveJavaType != null) {
      primitiveJavaTypeToTypeEntry.put(t.primitiveJavaType, t);
    }
    if (t.primitiveJavaClass != null) {
      primitiveJavaClassToTypeEntry.put(t.primitiveJavaClass, t);
    }
    if (t.primitiveWritableClass != null) {
      primitiveWritableClassToTypeEntry.put(t.primitiveWritableClass, t);
    }
    if (t.typeName != null) {
      typeNameToTypeEntry.put(t.typeName, t);
    }
  }

  public static final PrimitiveTypeEntry binaryTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.BINARY, serdeConstants.BINARY_TYPE_NAME, byte[].class,
      byte[].class, BytesWritable.class);
  public static final PrimitiveTypeEntry stringTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.STRING, serdeConstants.STRING_TYPE_NAME, null, String.class,
      Text.class);
  public static final PrimitiveTypeEntry booleanTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.BOOLEAN, serdeConstants.BOOLEAN_TYPE_NAME, Boolean.TYPE,
      Boolean.class, BooleanWritable.class);
  public static final PrimitiveTypeEntry intTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.INT, serdeConstants.INT_TYPE_NAME, Integer.TYPE,
      Integer.class, IntWritable.class);
  public static final PrimitiveTypeEntry longTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.LONG, serdeConstants.BIGINT_TYPE_NAME, Long.TYPE,
      Long.class, LongWritable.class);
  public static final PrimitiveTypeEntry floatTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.FLOAT, serdeConstants.FLOAT_TYPE_NAME, Float.TYPE,
      Float.class, FloatWritable.class);
  public static final PrimitiveTypeEntry voidTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.VOID, serdeConstants.VOID_TYPE_NAME, Void.TYPE, Void.class,
      NullWritable.class);

  // No corresponding Writable classes for the following 3 in hadoop 0.17.0
  public static final PrimitiveTypeEntry doubleTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.DOUBLE, serdeConstants.DOUBLE_TYPE_NAME, Double.TYPE,
      Double.class, DoubleWritable.class);
  public static final PrimitiveTypeEntry byteTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.BYTE, serdeConstants.TINYINT_TYPE_NAME, Byte.TYPE,
      Byte.class, ByteWritable.class);
  public static final PrimitiveTypeEntry shortTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.SHORT, serdeConstants.SMALLINT_TYPE_NAME, Short.TYPE,
      Short.class, ShortWritable.class);
  public static final PrimitiveTypeEntry dateTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.DATE, serdeConstants.DATE_TYPE_NAME, null,
      Date.class, DateWritableV2.class);
  public static final PrimitiveTypeEntry timestampTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.TIMESTAMP, serdeConstants.TIMESTAMP_TYPE_NAME, null,
      Timestamp.class, TimestampWritableV2.class);
  public static final PrimitiveTypeEntry timestampTZTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.TIMESTAMPLOCALTZ, serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME, null,
      TimestampTZ.class, TimestampLocalTZWritable.class);
  public static final PrimitiveTypeEntry intervalYearMonthTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.INTERVAL_YEAR_MONTH, serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME, null,
      HiveIntervalYearMonth.class, HiveIntervalYearMonthWritable.class);
  public static final PrimitiveTypeEntry intervalDayTimeTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.INTERVAL_DAY_TIME, serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME, null,
      HiveIntervalDayTime.class, HiveIntervalDayTimeWritable.class);
  public static final PrimitiveTypeEntry decimalTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.DECIMAL, serdeConstants.DECIMAL_TYPE_NAME, null,
      HiveDecimal.class, HiveDecimalWritable.class);
  public static final PrimitiveTypeEntry varcharTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.VARCHAR, serdeConstants.VARCHAR_TYPE_NAME, null, HiveVarchar.class,
      HiveVarcharWritable.class);
  public static final PrimitiveTypeEntry charTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.CHAR, serdeConstants.CHAR_TYPE_NAME, null, HiveChar.class,
      HiveCharWritable.class);

  // The following is a complex type for special handling
  public static final PrimitiveTypeEntry unknownTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.UNKNOWN, "unknown", null, Object.class, null);

  static {
    registerType(binaryTypeEntry);
    registerType(stringTypeEntry);
    registerType(charTypeEntry);
    registerType(varcharTypeEntry);
    registerType(booleanTypeEntry);
    registerType(intTypeEntry);
    registerType(longTypeEntry);
    registerType(floatTypeEntry);
    registerType(voidTypeEntry);
    registerType(doubleTypeEntry);
    registerType(byteTypeEntry);
    registerType(shortTypeEntry);
    registerType(dateTypeEntry);
    registerType(timestampTypeEntry);
    registerType(timestampTZTypeEntry);
    registerType(intervalYearMonthTypeEntry);
    registerType(intervalDayTimeTypeEntry);
    registerType(decimalTypeEntry);
    registerType(unknownTypeEntry);
  }

  /**
   * Return Whether the class is a Java Primitive type or a Java Primitive
   * class.
   */
  public static Class<?> primitiveJavaTypeToClass(Class<?> clazz) {
    PrimitiveTypeEntry t = primitiveJavaTypeToTypeEntry.get(clazz);
    return t == null ? clazz : t.primitiveJavaClass;
  }

  /**
   * Whether the class is a Java Primitive type or a Java Primitive class.
   */
  public static boolean isPrimitiveJava(Class<?> clazz) {
    return primitiveJavaTypeToTypeEntry.get(clazz) != null
        || primitiveJavaClassToTypeEntry.get(clazz) != null;
  }

  /**
   * Whether the class is a Java Primitive type.
   */
  public static boolean isPrimitiveJavaType(Class<?> clazz) {
    return primitiveJavaTypeToTypeEntry.get(clazz) != null;
  }

  /**
   * Whether the class is a Java Primitive class.
   */
  public static boolean isPrimitiveJavaClass(Class<?> clazz) {
    return primitiveJavaClassToTypeEntry.get(clazz) != null;
  }

  /**
   * Whether the class is a Hive Primitive Writable class.
   */
  public static boolean isPrimitiveWritableClass(Class<?> clazz) {
    return primitiveWritableClassToTypeEntry.get(clazz) != null;
  }

  /**
   * Get the typeName from a Java Primitive Type or Java PrimitiveClass.
   */
  public static String getTypeNameFromPrimitiveJava(Class<?> clazz) {
    PrimitiveTypeEntry t = primitiveJavaTypeToTypeEntry.get(clazz);
    if (t == null) {
      t = primitiveJavaClassToTypeEntry.get(clazz);
    }
    return t == null ? null : t.typeName;
  }

  /**
   * Get the typeName from a Primitive Writable Class.
   */
  public static String getTypeNameFromPrimitiveWritable(Class<?> clazz) {
    PrimitiveTypeEntry t = primitiveWritableClassToTypeEntry.get(clazz);
    return t == null ? null : t.typeName;
  }

  /**
   * Get the typeName from a Java Primitive Type or Java PrimitiveClass.
   */
  public static PrimitiveTypeEntry getTypeEntryFromPrimitiveCategory(
      PrimitiveCategory category) {
    return primitiveCategoryToTypeEntry.get(category);
  }

  /**
   * Get the TypeEntry for a Java Primitive Type or Java PrimitiveClass.
   */
  public static PrimitiveTypeEntry getTypeEntryFromPrimitiveJava(Class<?> clazz) {
    PrimitiveTypeEntry t = primitiveJavaTypeToTypeEntry.get(clazz);
    if (t == null) {
      t = primitiveJavaClassToTypeEntry.get(clazz);
    }
    return t;
  }

  /**
   * Get the TypeEntry for a Java Primitive Type or Java PrimitiveClass.
   */
  public static PrimitiveTypeEntry getTypeEntryFromPrimitiveJavaType(
      Class<?> clazz) {
    return primitiveJavaTypeToTypeEntry.get(clazz);
  }

  /**
   * Get the TypeEntry for a Java Primitive Type or Java PrimitiveClass.
   */
  public static PrimitiveTypeEntry getTypeEntryFromPrimitiveJavaClass(
      Class<?> clazz) {
    return primitiveJavaClassToTypeEntry.get(clazz);
  }

  /**
   * Get the TypeEntry for a Primitive Writable Class.
   */
  public static PrimitiveTypeEntry getTypeEntryFromPrimitiveWritableClass(
      Class<?> clazz) {
    return primitiveWritableClassToTypeEntry.get(clazz);
  }

  /**
   * Get the TypeEntry for the given base type name (int, varchar, etc).
   */
  public static PrimitiveTypeEntry getTypeEntryFromTypeName(String typeName) {
    return typeNameToTypeEntry.get(typeName);
  }

  /**
   * Compare 2 primitive objects. Conversion not allowed. Note that NULL does
   * not equal to NULL according to SQL standard.
   */
  public static boolean comparePrimitiveObjects(Object o1,
      PrimitiveObjectInspector oi1, Object o2, PrimitiveObjectInspector oi2) {
    if (o1 == null || o2 == null) {
      return false;
    }

    if (oi1.getPrimitiveCategory() != oi2.getPrimitiveCategory()) {
      return false;
    }
    switch (oi1.getPrimitiveCategory()) {
    case BOOLEAN: {
      return ((BooleanObjectInspector) oi1).get(o1) == ((BooleanObjectInspector) oi2)
          .get(o2);
    }
    case BYTE: {
      return ((ByteObjectInspector) oi1).get(o1) == ((ByteObjectInspector) oi2)
          .get(o2);
    }
    case SHORT: {
      return ((ShortObjectInspector) oi1).get(o1) == ((ShortObjectInspector) oi2)
          .get(o2);
    }
    case INT: {
      return ((IntObjectInspector) oi1).get(o1) == ((IntObjectInspector) oi2)
          .get(o2);
    }
    case LONG: {
      return ((LongObjectInspector) oi1).get(o1) == ((LongObjectInspector) oi2)
          .get(o2);
    }
    case FLOAT: {
      return ((FloatObjectInspector) oi1).get(o1) == ((FloatObjectInspector) oi2)
          .get(o2);
    }
    case DOUBLE: {
      return ((DoubleObjectInspector) oi1).get(o1) == ((DoubleObjectInspector) oi2)
          .get(o2);
    }
    case STRING: {
      Writable t1 = ((StringObjectInspector) oi1)
          .getPrimitiveWritableObject(o1);
      Writable t2 = ((StringObjectInspector) oi2)
          .getPrimitiveWritableObject(o2);
      return t1.equals(t2);
    }
    case CHAR: {
      return ((HiveCharObjectInspector)oi1).getPrimitiveWritableObject(o1)
          .equals(((HiveCharObjectInspector)oi2).getPrimitiveWritableObject(o2));
    }
    case VARCHAR: {
      return ((HiveVarcharObjectInspector)oi1).getPrimitiveWritableObject(o1)
          .equals(((HiveVarcharObjectInspector)oi2).getPrimitiveWritableObject(o2));
    }
    case DATE: {
      return ((DateObjectInspector) oi1).getPrimitiveWritableObject(o1)
          .equals(((DateObjectInspector) oi2).getPrimitiveWritableObject(o2));
    }
    case TIMESTAMP: {
      return ((TimestampObjectInspector) oi1).getPrimitiveWritableObject(o1)
          .equals(((TimestampObjectInspector) oi2).getPrimitiveWritableObject(o2));
    }
    case TIMESTAMPLOCALTZ: {
      return ((TimestampLocalTZObjectInspector) oi1).getPrimitiveWritableObject(o1).equals(
          ((TimestampLocalTZObjectInspector) oi2).getPrimitiveWritableObject(o2));
    }
    case INTERVAL_YEAR_MONTH: {
      return ((HiveIntervalYearMonthObjectInspector) oi1).getPrimitiveWritableObject(o1)
          .equals(((HiveIntervalYearMonthObjectInspector) oi2).getPrimitiveWritableObject(o2));
    }
    case INTERVAL_DAY_TIME: {
      return ((HiveIntervalDayTimeObjectInspector) oi1).getPrimitiveWritableObject(o1)
          .equals(((HiveIntervalDayTimeObjectInspector) oi2).getPrimitiveWritableObject(o2));
    }
    case BINARY: {
      return ((BinaryObjectInspector) oi1).getPrimitiveWritableObject(o1).
          equals(((BinaryObjectInspector) oi2).getPrimitiveWritableObject(o2));
    }
    case DECIMAL: {
      return ((HiveDecimalObjectInspector) oi1).getPrimitiveJavaObject(o1)
          .compareTo(((HiveDecimalObjectInspector) oi2).getPrimitiveJavaObject(o2)) == 0;
    }
    default:
      return false;
    }
  }

  /**
   * Compare 2 Primitive Objects with their Object Inspector, conversions
   * allowed. Note that NULL does not equal to NULL according to SQL standard.
   */
  public static boolean comparePrimitiveObjectsWithConversion(Object o1,
      PrimitiveObjectInspector oi1, Object o2, PrimitiveObjectInspector oi2) {
    if (o1 == null || o2 == null) {
      return false;
    }

    if (oi1.getPrimitiveCategory() == oi2.getPrimitiveCategory()) {
      return comparePrimitiveObjects(o1, oi1, o2, oi2);
    }

    // If not equal, convert all to double and compare
    try {
      return getDouble(o1, oi1) == getDouble(o2, oi2);
    } catch (NumberFormatException e) {
      return false;
    }
  }

  /**
   * Get the boolean value out of a primitive object. Note that
   * NullPointerException will be thrown if o is null. Note that
   * NumberFormatException will be thrown if o is not a valid number.
   */
  public static boolean getBoolean(Object o, PrimitiveObjectInspector oi) {
    boolean result = false;
    switch (oi.getPrimitiveCategory()) {
    case VOID:
      result = false;
      break;
    case BOOLEAN:
      result = ((BooleanObjectInspector) oi).get(o);
      break;
    case BYTE:
      result = ((ByteObjectInspector) oi).get(o) != 0;
      break;
    case SHORT:
      result = ((ShortObjectInspector) oi).get(o) != 0;
      break;
    case INT:
      result = ((IntObjectInspector) oi).get(o) != 0;
      break;
    case LONG:
      result = (int) ((LongObjectInspector) oi).get(o) != 0;
      break;
    case FLOAT:
      result = (int) ((FloatObjectInspector) oi).get(o) != 0;
      break;
    case DOUBLE:
      result = (int) ((DoubleObjectInspector) oi).get(o) != 0;
      break;
    case STRING:
      StringObjectInspector soi = (StringObjectInspector) oi;
      if (soi.preferWritable()) {
        Text t = soi.getPrimitiveWritableObject(o);
        result = parseBoolean(t);
      } else {
        String s = soi.getPrimitiveJavaObject(o);
        result = parseBoolean(s);
      }
      break;
    case TIMESTAMP:
      result = (((TimestampObjectInspector) oi)
          .getPrimitiveWritableObject(o).getSeconds() != 0);
      break;
    case DECIMAL:
      result = HiveDecimal.ZERO.compareTo(
          ((HiveDecimalObjectInspector) oi).getPrimitiveJavaObject(o)) != 0;
      break;
    case DATE:  // unsupported conversion
    default:
      throw new RuntimeException("Hive 2 Internal error: unsupported conversion from type: "
          + oi.getTypeName());
    }
    return result;
  }


  enum FalseValues {
    FALSE("false"), OFF("off"), NO("no"), ZERO("0"), EMPTY("");

    private final byte[] bytes;
    private String str;

    FalseValues(String s) {
      str = s;
      bytes = s.getBytes(StandardCharsets.UTF_8);
    }

    public boolean accept(byte[] arr, int st) {
      for (int i = 0; i < bytes.length; i++) {
        byte b = arr[i + st];
        if (!(b == bytes[i] || b + 'a' - 'A' == bytes[i])) {
          return false;
        }
      }
      return true;
    }

    public boolean accept(String s) {
      return str.equalsIgnoreCase(s);
    }
  }
  /**
   * Parses a boolean from string
   *
   * Accepts "false","off","no","0" and "" as FALSE
   * All other values are interpreted as true.
   */
  public static boolean parseBoolean(byte[] arr, int st, int len) {
    switch (len) {
    case 5:
      return !FalseValues.FALSE.accept(arr, st);
    case 3:
      return !FalseValues.OFF.accept(arr, st);
    case 2:
      return !FalseValues.NO.accept(arr, st);
    case 1:
      return !FalseValues.ZERO.accept(arr, st);
    case 0:
      return false;
    default:
      return true;
    }
  }

  private static final FalseValues[] FALSE_BOOLEANS = FalseValues.values();

  private static boolean parseBoolean(String s) {
    for (int i = 0; i < FALSE_BOOLEANS.length; i++) {
      if (FALSE_BOOLEANS[i].accept(s)) {
        return false;
      }
    }
    return true;
  }

  private static boolean parseBoolean(Text t) {
    if(t.getLength()>5) {
      return true;
    }
    String strVal=t.toString();
    return parseBoolean(strVal);
  }

  /**
   * Get the byte value out of a primitive object. Note that
   * NullPointerException will be thrown if o is null. Note that
   * NumberFormatException will be thrown if o is not a valid number.
   */
  public static byte getByte(Object o, PrimitiveObjectInspector oi) {
    byte result;
    switch (oi.getPrimitiveCategory()) {
    case DECIMAL:
      {
        HiveDecimal dec = ((HiveDecimalObjectInspector) oi)
            .getPrimitiveJavaObject(o);
        if (!dec.isByte()) {
          throw new NumberFormatException();
        }
        result = dec.byteValue();
      }
      break;
    default:
      // For all other data types, use int conversion.  At some point, we should have all
      // conversions make sure the value fits.
      return (byte) getInt(o, oi);
    }
    return result;
  }

  /**
   * Get the short value out of a primitive object. Note that
   * NullPointerException will be thrown if o is null. Note that
   * NumberFormatException will be thrown if o is not a valid number.
   */
  public static short getShort(Object o, PrimitiveObjectInspector oi) {
    short result;
    switch (oi.getPrimitiveCategory()) {
    case DECIMAL:
      {
        HiveDecimal dec = ((HiveDecimalObjectInspector) oi)
            .getPrimitiveJavaObject(o);
        if (!dec.isShort()) {
          throw new NumberFormatException();
        }
        result = dec.shortValue();
      }
      break;
    default:
      // For all other data types, use int conversion.  At some point, we should have all
      // conversions make sure the value fits.
      return (short) getInt(o, oi);
    }
    return result;
  }

  /**
   * Get the integer value out of a primitive object. Note that
   * NullPointerException will be thrown if o is null. Note that
   * NumberFormatException will be thrown if o is not a valid number.
   */
  public static int getInt(Object o, PrimitiveObjectInspector oi) {
    int result = 0;
    switch (oi.getPrimitiveCategory()) {
    case VOID: {
      result = 0;
      break;
    }
    case BOOLEAN: {
      result = (((BooleanObjectInspector) oi).get(o) ? 1 : 0);
      break;
    }
    case BYTE: {
      result = ((ByteObjectInspector) oi).get(o);
      break;
    }
    case SHORT: {
      result = ((ShortObjectInspector) oi).get(o);
      break;
    }
    case INT: {
      result = ((IntObjectInspector) oi).get(o);
      break;
    }
    case LONG: {
      result = (int) ((LongObjectInspector) oi).get(o);
      break;
    }
    case FLOAT: {
      result = (int) ((FloatObjectInspector) oi).get(o);
      break;
    }
    case DOUBLE: {
      result = (int) ((DoubleObjectInspector) oi).get(o);
      break;
    }
    case STRING: {
      StringObjectInspector soi = (StringObjectInspector) oi;
      if (soi.preferWritable()) {
        Text t = soi.getPrimitiveWritableObject(o);
        result = LazyInteger.parseInt(t.getBytes(), 0, t.getLength());
      } else {
        String s = soi.getPrimitiveJavaObject(o);
        result = Integer.parseInt(s);
      }
      break;
    }
    case CHAR:
    case VARCHAR: {
      result = Integer.parseInt(getString(o, oi));
      break;
    }
    case TIMESTAMP:
      result = (int) (((TimestampObjectInspector) oi)
          .getPrimitiveWritableObject(o).getSeconds());
      break;
    case DECIMAL:
      {
        HiveDecimal dec = ((HiveDecimalObjectInspector) oi)
            .getPrimitiveJavaObject(o);
        if (!dec.isInt()) {
          throw new NumberFormatException();
        }
        result = dec.intValue();
      }
      break;
    case DATE:  // unsupported conversion
    default: {
      throw new RuntimeException("Hive 2 Internal error: unsupported conversion from type: "
          + oi.getTypeName());
    }
    }
    return result;
  }

  /**
   * Get the long value out of a primitive object. Note that
   * NullPointerException will be thrown if o is null. Note that
   * NumberFormatException will be thrown if o is not a valid number.
   */
  public static long getLong(Object o, PrimitiveObjectInspector oi) {
    long result = 0;
    switch (oi.getPrimitiveCategory()) {
    case VOID:
      result = 0;
      break;
    case BOOLEAN:
      result = (((BooleanObjectInspector) oi).get(o) ? 1 : 0);
      break;
    case BYTE:
      result = ((ByteObjectInspector) oi).get(o);
      break;
    case SHORT:
      result = ((ShortObjectInspector) oi).get(o);
      break;
    case INT:
      result = ((IntObjectInspector) oi).get(o);
      break;
    case LONG:
      result = ((LongObjectInspector) oi).get(o);
      break;
    case FLOAT:
      result = (long) ((FloatObjectInspector) oi).get(o);
      break;
    case DOUBLE:
      result = (long) ((DoubleObjectInspector) oi).get(o);
      break;
    case STRING:
      StringObjectInspector soi = (StringObjectInspector) oi;
      if (soi.preferWritable()) {
        Text t = soi.getPrimitiveWritableObject(o);
        result = LazyLong.parseLong(t.getBytes(), 0, t.getLength());
      } else {
        String s = soi.getPrimitiveJavaObject(o);
        result = Long.parseLong(s);
      }
      break;
    case CHAR:
    case VARCHAR: {
      result = Long.parseLong(getString(o, oi));
      break;
    }
    case TIMESTAMP:
      result = ((TimestampObjectInspector) oi).getPrimitiveWritableObject(o)
          .getSeconds();
      break;
    case DECIMAL:
      {
        HiveDecimal dec = ((HiveDecimalObjectInspector) oi)
            .getPrimitiveJavaObject(o);
        if (!dec.isLong()) {
          throw new NumberFormatException();
        }
        result = dec.longValue();
      }
      break;
    case DATE:  // unsupported conversion
    default:
      throw new RuntimeException("Hive 2 Internal error: unsupported conversion from type: "
          + oi.getTypeName());
    }
    return result;
  }

  /**
   * Get the double value out of a primitive object. Note that
   * NullPointerException will be thrown if o is null. Note that
   * NumberFormatException will be thrown if o is not a valid number.
   */
  public static double getDouble(Object o, PrimitiveObjectInspector oi) {
    double result = 0;
    switch (oi.getPrimitiveCategory()) {
    case VOID:
      result = 0;
      break;
    case BOOLEAN:
      result = (((BooleanObjectInspector) oi).get(o) ? 1 : 0);
      break;
    case BYTE:
      result = ((ByteObjectInspector) oi).get(o);
      break;
    case SHORT:
      result = ((ShortObjectInspector) oi).get(o);
      break;
    case INT:
      result = ((IntObjectInspector) oi).get(o);
      break;
    case LONG:
      result = ((LongObjectInspector) oi).get(o);
      break;
    case FLOAT:
      result = ((FloatObjectInspector) oi).get(o);
      break;
    case DOUBLE:
      result = ((DoubleObjectInspector) oi).get(o);
      break;
    case STRING:
      StringObjectInspector soi = (StringObjectInspector) oi;
      String s = soi.getPrimitiveJavaObject(o);
      result = Double.parseDouble(s);
      break;
    case CHAR:
    case VARCHAR:
      result = Double.parseDouble(getString(o, oi));
      break;
    case TIMESTAMP:
      result = ((TimestampObjectInspector) oi).getPrimitiveWritableObject(o).getDouble();
      break;
    case DECIMAL:
      result = ((HiveDecimalObjectInspector) oi)
          .getPrimitiveJavaObject(o).doubleValue();
      break;
    case DATE:  // unsupported conversion
    default:
      throw new RuntimeException("Hive 2 Internal error: unsupported conversion from type: "
          + oi.getTypeName());
    }
    return result;
  }

  /**
   * Get the float value out of a primitive object. Note that
   * NullPointerException will be thrown if o is null. Note that
   * NumberFormatException will be thrown if o is not a valid number.
   */
  public static float getFloat(Object o, PrimitiveObjectInspector oi) {
    return (float) getDouble(o, oi);
  }

  /**
   * Get the String value out of a primitive object. Note that
   * NullPointerException will be thrown if o is null. Note that
   * RuntimeException will be thrown if o is not a valid string.
   */
  public static String getString(Object o, PrimitiveObjectInspector oi) {

    if (o == null) {
      return null;
    }

    String result = null;
    switch (oi.getPrimitiveCategory()) {
    case VOID:
      result = null;
      break;
    case BINARY:
      try {
        byte[] bytes = ((BinaryObjectInspector) oi).getPrimitiveWritableObject(o).getBytes();
        int byteLen = ((BinaryObjectInspector) oi).getPrimitiveWritableObject(o).getLength();
        result = Text.decode(bytes, 0, byteLen);
      } catch (CharacterCodingException err) {
        // we tried ..
        result = null;
      }
      break;
    case BOOLEAN:
      result = (((BooleanObjectInspector) oi).get(o)) ? "TRUE" : "FALSE";
      break;
    case BYTE:
      result = String.valueOf((((ByteObjectInspector) oi).get(o)));
      break;
    case SHORT:
      result = String.valueOf((((ShortObjectInspector) oi).get(o)));
      break;
    case INT:
      result = String.valueOf((((IntObjectInspector) oi).get(o)));
      break;
    case LONG:
      result = String.valueOf((((LongObjectInspector) oi).get(o)));
      break;
    case FLOAT:
      result = String.valueOf((((FloatObjectInspector) oi).get(o)));
      break;
    case DOUBLE:
      result = String.valueOf((((DoubleObjectInspector) oi).get(o)));
      break;
    case STRING:
      StringObjectInspector soi = (StringObjectInspector) oi;
      result = soi.getPrimitiveJavaObject(o);
      break;
    case CHAR:
      // when converting from char to string/varchar, strip any trailing spaces
      result = ((HiveCharObjectInspector) oi).getPrimitiveJavaObject(o).getStrippedValue();
      break;
    case VARCHAR:
      HiveVarcharObjectInspector hcoi = (HiveVarcharObjectInspector) oi;
      result = hcoi.getPrimitiveJavaObject(o).toString();
      break;
    case DATE:
      result = ((DateObjectInspector) oi).getPrimitiveWritableObject(o).toString();
      break;
    case TIMESTAMP:
      result = ((TimestampObjectInspector) oi).getPrimitiveWritableObject(o).toString();
      break;
    case TIMESTAMPLOCALTZ:
      result = ((TimestampLocalTZObjectInspector) oi).getPrimitiveWritableObject(o).toString();
      break;
    case INTERVAL_YEAR_MONTH:
      result = ((HiveIntervalYearMonthObjectInspector) oi).getPrimitiveWritableObject(o).toString();
      break;
    case INTERVAL_DAY_TIME:
      result = ((HiveIntervalDayTimeObjectInspector) oi).getPrimitiveWritableObject(o).toString();
      break;
    case DECIMAL:
      int scale = ((HiveDecimalObjectInspector) oi).scale();
      result = ((HiveDecimalObjectInspector) oi)
          .getPrimitiveJavaObject(o).toFormatString(scale);
      break;
    default:
      throw new RuntimeException("Hive 2 Internal error: unknown type: "
          + oi.getTypeName());
    }
    return result;
  }

  public static HiveChar getHiveChar(Object o, PrimitiveObjectInspector oi) {
    if (o == null) {
      return null;
    }

    HiveChar result = null;
    switch (oi.getPrimitiveCategory()) {
      case CHAR:
        result = ((HiveCharObjectInspector) oi).getPrimitiveJavaObject(o);
        break;
      default:
        // No char length available, copy whole string value here.
        result = new HiveChar();
        result.setValue(getString(o, oi));
        break;
    }
    return result;
  }

  public static HiveVarchar getHiveVarchar(Object o, PrimitiveObjectInspector oi) {

    if (o == null) {
      return null;
    }

    HiveVarchar result = null;
    switch (oi.getPrimitiveCategory()) {
      case VARCHAR:
        result = ((HiveVarcharObjectInspector)oi).getPrimitiveJavaObject(o);
        break;
      default:
        // Is there a way to provide char length here?
        // It might actually be ok as long as there is an object inspector (with char length)
        // receiving this value.
        result = new HiveVarchar();
        result.setValue(getString(o, oi));
        break;
    }
    return result;
  }

  public static BytesWritable getBinaryFromText(Text text) {
    BytesWritable bw = new BytesWritable();
    bw.set(text.getBytes(), 0, text.getLength());
    return bw;
  }

  public static BytesWritable getBinary(Object o, PrimitiveObjectInspector oi) {

    if (null == o) {
      return null;
    }

    switch (oi.getPrimitiveCategory()) {

    case VOID:
      return null;

    case STRING:
      Text text = ((StringObjectInspector) oi).getPrimitiveWritableObject(o);
      return getBinaryFromText(text);
    case CHAR:
      // char to binary conversion: include trailing spaces?
      return getBinaryFromText(
          ((HiveCharObjectInspector) oi).getPrimitiveWritableObject(o).getPaddedValue());
    case VARCHAR:
      return getBinaryFromText(
          ((HiveVarcharObjectInspector) oi).getPrimitiveWritableObject(o).getTextValue());

    case BINARY:
      return ((BinaryObjectInspector) oi).getPrimitiveWritableObject(o);

    default:
      throw new RuntimeException("Cannot convert to Binary from: "
          + oi.getTypeName());
    }
  }

  public static HiveDecimal getHiveDecimal(Object o, PrimitiveObjectInspector oi) {
    if (o == null) {
      return null;
    }

    HiveDecimal result = null;
    switch (oi.getPrimitiveCategory()) {
    case VOID:
      result = null;
      break;
    case BOOLEAN:
      result = ((BooleanObjectInspector) oi).get(o) ?
        HiveDecimal.ONE : HiveDecimal.ZERO;
      break;
    case BYTE:
      result = HiveDecimal.create(((ByteObjectInspector) oi).get(o));
      break;
    case SHORT:
      result = HiveDecimal.create(((ShortObjectInspector) oi).get(o));
      break;
    case INT:
      result = HiveDecimal.create(((IntObjectInspector) oi).get(o));
      break;
    case LONG:
      result = HiveDecimal.create(((LongObjectInspector) oi).get(o));
      break;
    case FLOAT:
      Float f = ((FloatObjectInspector) oi).get(o);
      result = HiveDecimal.create(f.toString());
      break;
    case DOUBLE:
      Double d = ((DoubleObjectInspector) oi).get(o);
      result = HiveDecimal.create(d.toString());
      break;
    case STRING:
      result = HiveDecimal.create(((StringObjectInspector) oi).getPrimitiveJavaObject(o));
      break;
    case CHAR:
    case VARCHAR:
      result = HiveDecimal.create(getString(o, oi));
      break;
    case TIMESTAMP:
      Double ts = ((TimestampObjectInspector) oi).getPrimitiveWritableObject(o)
        .getDouble();
      result = HiveDecimal.create(ts.toString());
      break;
    case DECIMAL:
      result = ((HiveDecimalObjectInspector) oi).getPrimitiveJavaObject(o);
      break;
    case DATE:  // unsupported conversion
    default:
      throw new RuntimeException("Hive 2 Internal error: unsupported conversion from type: "
                                 + oi.getTypeName());
    }
    return result;
  }

  public static Date getDate(Object o, PrimitiveObjectInspector oi) {
    if (o == null) {
      return null;
    }

    Date result = null;
    switch (oi.getPrimitiveCategory()) {
    case VOID:
      break;
    case STRING:
      StringObjectInspector soi = (StringObjectInspector) oi;
      String s = soi.getPrimitiveJavaObject(o).trim();
      try {
        if (s.length() == DATE_LENGTH) {
          result = Date.valueOf(s);
        } else {
          Timestamp ts = getTimestampFromString(s);
          if (ts != null) {
            result = Date.ofEpochMilli(ts.toEpochMilli());
          }
        }
      } catch (IllegalArgumentException e) {
        // Do nothing
      }
      break;
    case CHAR:
    case VARCHAR: {
      String val = getString(o, oi).trim();
      try {
        if (val.length() == DATE_LENGTH) {
          result = Date.valueOf(val);
        } else {
          Timestamp ts = getTimestampFromString(val);
          if (ts != null) {
            result = Date.ofEpochMilli(ts.toEpochMilli());
          }
        }
      } catch (IllegalArgumentException e) {
        // Do nothing
      }
      break;
    }
    case DATE:
      result = ((DateObjectInspector) oi).getPrimitiveWritableObject(o).get();
      break;
    case TIMESTAMP:
      result = DateWritableV2.timeToDate(
          ((TimestampObjectInspector) oi).getPrimitiveWritableObject(o).getSeconds());
      break;
    case TIMESTAMPLOCALTZ:
      String tstz = oi.getPrimitiveWritableObject(o).toString();
      int divSpace = tstz.indexOf(' ');
      if (divSpace == -1) {
        return null;
      }
      result = Date.valueOf(tstz.substring(0, divSpace));
      break;
    default:
      throw new RuntimeException("Cannot convert to Date from: "
        + oi.getTypeName());
    }

    return result;
  }

  public static Timestamp getTimestamp(Object o, PrimitiveObjectInspector oi) {
    return getTimestamp(o, oi, false);
  }

  public static Timestamp getTimestamp(Object o, PrimitiveObjectInspector inputOI, boolean intToTimestampInSeconds) {
    if (o == null) {
      return null;
    }

    Timestamp result = null;
    long longValue = 0;
    switch (inputOI.getPrimitiveCategory()) {
    case VOID:
      result = null;
      break;
    case BOOLEAN:
      longValue = ((BooleanObjectInspector) inputOI).get(o) ? 1 : 0;
      result = TimestampWritableV2.longToTimestamp(longValue, intToTimestampInSeconds);
      break;
    case BYTE:
      longValue = ((ByteObjectInspector) inputOI).get(o);
      result = TimestampWritableV2.longToTimestamp(longValue, intToTimestampInSeconds);
      break;
    case SHORT:
      longValue = ((ShortObjectInspector) inputOI).get(o);
      result = TimestampWritableV2.longToTimestamp(longValue, intToTimestampInSeconds);
      break;
    case INT:
      longValue = ((IntObjectInspector) inputOI).get(o);
      result = TimestampWritableV2.longToTimestamp(longValue, intToTimestampInSeconds);
      break;
    case LONG:
      longValue = ((LongObjectInspector) inputOI).get(o);
      result = TimestampWritableV2.longToTimestamp(longValue, intToTimestampInSeconds);
      break;
    case FLOAT:
      result = TimestampUtils.doubleToTimestamp(((FloatObjectInspector) inputOI).get(o));
      break;
    case DOUBLE:
      result = TimestampUtils.doubleToTimestamp(((DoubleObjectInspector) inputOI).get(o));
      break;
    case DECIMAL:
      result = TimestampUtils.decimalToTimestamp(
          ((HiveDecimalObjectInspector) inputOI).getPrimitiveJavaObject(o));
      break;
    case STRING:
      StringObjectInspector soi = (StringObjectInspector) inputOI;
      String s = soi.getPrimitiveJavaObject(o);
      result = getTimestampFromString(s);
      break;
    case CHAR:
    case VARCHAR:
      result = getTimestampFromString(getString(o, inputOI));
      break;
    case DATE:
      result = Timestamp.ofEpochMilli(
          ((DateObjectInspector) inputOI).getPrimitiveWritableObject(o).get().toEpochMilli());
      break;
    case TIMESTAMP:
      result = ((TimestampObjectInspector) inputOI).getPrimitiveWritableObject(o).getTimestamp();
      break;
    case TIMESTAMPLOCALTZ:
      String tstz = inputOI.getPrimitiveWritableObject(o).toString();
      int index = tstz.indexOf(" ");
      index = tstz.indexOf(" ", index + 1);
      if (index == -1) {
        return null;
      }
      result = Timestamp.valueOf(tstz.substring(0, index));
      break;
    default:
      throw new RuntimeException("Hive 2 Internal error: unknown type: "
          + inputOI.getTypeName());
    }
    return result;
  }

  private final static int DATE_LENGTH = "YYYY-MM-DD".length();
  private final static int TS_LENGTH = "yyyy-mm-dd hh:mm:ss".length();

  public static Timestamp getTimestampFromString(String s) {
    Timestamp result = null;
    s = s.trim();
    s = trimNanoTimestamp(s);

    // Handle simpler cases directly avoiding exceptions
    try {
      if (s.length() == DATE_LENGTH) {
        // Its a date!
        return Timestamp.ofEpochMilli(Date.valueOf(s).toEpochMilli());
      } else if (isValidTimeStamp(s)) {
        return Timestamp.valueOf(s);
      }
      // If a timestamp does not have a space, then it is likely zoned time.
      if (s.contains("+") || (s.length() > DATE_LENGTH && s.charAt(DATE_LENGTH) == '-')) {
        // Timestamp with timezone
        // Let's try to parse it as timestamp with time zone and transform
        try {
          result = Timestamp.valueOf(TimestampTZUtil.parse(s).getZonedDateTime()
                  .toLocalDateTime().toString());
        } catch (DateTimeException e2) {
          // Do nothing
        }
      } else {
        // Last attempt
        result = Timestamp.ofEpochMilli(Date.valueOf(s).toEpochMilli());
      }
    } catch (IllegalArgumentException e) {
      // Do nothing
    }
    return result;
  }

  private static String trimNanoTimestamp(String s) {
    int firstSpace = s.indexOf(' ');
    // Throw away extra if more than 9 decimal places
    int periodIdx = s.indexOf(".");
    if (periodIdx != -1) {
      int secondSpace = firstSpace < 0 ? -1 : s.indexOf(' ', firstSpace + 1);
      int maxLength = secondSpace == -1 ? s.length() : secondSpace;
      if (maxLength - periodIdx > 9) {
        s = s.substring(0, periodIdx + 10).concat(s.substring(maxLength, s.length()));
      }
    }
    return s;
  }

  private static boolean isValidTimeStamp(final String s) {
    if (s.length() == TS_LENGTH ||
            (s.contains(".") &&
                    s.substring(0, s.indexOf('.')).length() == TS_LENGTH)) {
      // Possible timestamp
      if (s.charAt(DATE_LENGTH) == '-') {
        return false;
      }
      return true;
    }
    return false;
  }

  public static TimestampTZ getTimestampLocalTZ(Object o, PrimitiveObjectInspector oi,
          ZoneId timeZone) {
    if (o == null) {
      return null;
    }
    switch (oi.getPrimitiveCategory()) {
    case VOID: {
      return null;
    }
    case STRING: {
      StringObjectInspector soi = (StringObjectInspector) oi;
      String s = soi.getPrimitiveJavaObject(o).trim();
      return TimestampTZUtil.parseOrNull(trimNanoTimestamp(s), timeZone);
    }
    case CHAR:
    case VARCHAR: {
      String s = getString(o, oi).trim();
      return TimestampTZUtil.parseOrNull(trimNanoTimestamp(s), timeZone);
    }
    case DATE: {
      Date date = ((DateObjectInspector) oi).getPrimitiveWritableObject(o).get();
      return TimestampTZUtil.convert(date, timeZone);
    }
    case TIMESTAMP: {
      Timestamp ts = ((TimestampObjectInspector) oi).getPrimitiveWritableObject(o).getTimestamp();
      return TimestampTZUtil.convert(ts, timeZone);
    }
    case TIMESTAMPLOCALTZ: {
      return ((TimestampLocalTZObjectInspector) oi).getPrimitiveWritableObject(o).getTimestampTZ();
    }
    default:
      throw new RuntimeException("Cannot convert to " + serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME +
          " from: " + oi.getTypeName());
    }
  }

  public static HiveIntervalYearMonth getHiveIntervalYearMonth(Object o, PrimitiveObjectInspector oi) {
    if (o == null) {
      return null;
    }

    HiveIntervalYearMonth result = null;
    switch (oi.getPrimitiveCategory()) {
      case VOID:
        result = null;
        break;
      case STRING:
      case CHAR:
      case VARCHAR: {
        try {
          String val = getString(o, oi).trim();
          result = HiveIntervalYearMonth.valueOf(val);
        } catch (IllegalArgumentException e) {
          result = null;
        }
        break;
      }
      case INTERVAL_YEAR_MONTH:
        result = ((HiveIntervalYearMonthObjectInspector) oi).getPrimitiveJavaObject(o);
        break;

      default:
        throw new RuntimeException("Cannot convert to IntervalYearMonth from: " + oi.getTypeName());
    }

    return result;
  }


  public static HiveIntervalDayTime getHiveIntervalDayTime(Object o, PrimitiveObjectInspector oi) {
    if (o == null) {
      return null;
    }

    HiveIntervalDayTime result = null;
    switch (oi.getPrimitiveCategory()) {
      case VOID:
        result = null;
        break;
      case STRING:
      case CHAR:
      case VARCHAR: {
        try {
          String val = getString(o, oi).trim();
          result = HiveIntervalDayTime.valueOf(val);
        } catch (IllegalArgumentException e) {
          result = null;
        }
        break;
      }
      case INTERVAL_DAY_TIME:
        result = ((HiveIntervalDayTimeObjectInspector) oi).getPrimitiveJavaObject(o);
        break;

      default:
        throw new RuntimeException("Cannot convert to IntervalDayTime from: " + oi.getTypeName());
    }

    return result;
  }

  public static Class<?> getJavaPrimitiveClassFromObjectInspector(ObjectInspector oi) {
    if (oi.getCategory() != Category.PRIMITIVE) {
      return null;
    }
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
    PrimitiveTypeEntry t =
        getTypeEntryFromPrimitiveCategory(poi.getPrimitiveCategory());
    return t == null ? null : t.primitiveJavaClass;
  }

  /**
   * Provide a general grouping for each primitive data type.
   */
  public static enum PrimitiveGrouping {
    NUMERIC_GROUP, STRING_GROUP, BOOLEAN_GROUP, DATE_GROUP, INTERVAL_GROUP, BINARY_GROUP,
    VOID_GROUP, UNKNOWN_GROUP
  };

  /**
   * Based on the PrimitiveCategory of a type, return the PrimitiveGrouping
   * that the PrimitiveCategory belongs to (numeric, string, date, etc).
   * @param primitiveCategory Primitive category of the type
   * @return PrimitveGrouping corresponding to the PrimitiveCategory,
   *         or UNKNOWN_GROUP if the type does not match to a grouping.
   */
  public static PrimitiveGrouping getPrimitiveGrouping(PrimitiveCategory primitiveCategory) {
    switch (primitiveCategory) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
        return PrimitiveGrouping.NUMERIC_GROUP;
      case STRING:
      case CHAR:
      case VARCHAR:
        return PrimitiveGrouping.STRING_GROUP;
      case BOOLEAN:
        return PrimitiveGrouping.BOOLEAN_GROUP;
      case DATE:
      case TIMESTAMP:
      case TIMESTAMPLOCALTZ:
        return PrimitiveGrouping.DATE_GROUP;
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_DAY_TIME:
        return PrimitiveGrouping.INTERVAL_GROUP;
      case BINARY:
        return PrimitiveGrouping.BINARY_GROUP;
      case VOID:
        return PrimitiveGrouping.VOID_GROUP;
      default:
        return PrimitiveGrouping.UNKNOWN_GROUP;
    }
  }

  private PrimitiveObjectInspectorUtils() {
    // prevent instantiation
  }

}
