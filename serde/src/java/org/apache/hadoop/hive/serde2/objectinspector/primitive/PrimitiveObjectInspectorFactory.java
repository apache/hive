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

package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * PrimitiveObjectInspectorFactory is the primary way to create new
 * PrimitiveObjectInspector instances.
 *
 * The reason of having caches here is that ObjectInspector is because
 * ObjectInspectors do not have an internal state - so ObjectInspectors with the
 * same construction parameters should result in exactly the same
 * ObjectInspector.
 */
public final class PrimitiveObjectInspectorFactory {

  public static final WritableBooleanObjectInspector writableBooleanObjectInspector =
      new WritableBooleanObjectInspector();
  public static final WritableByteObjectInspector writableByteObjectInspector =
      new WritableByteObjectInspector();
  public static final WritableShortObjectInspector writableShortObjectInspector =
      new WritableShortObjectInspector();
  public static final WritableIntObjectInspector writableIntObjectInspector =
      new WritableIntObjectInspector();
  public static final WritableLongObjectInspector writableLongObjectInspector =
      new WritableLongObjectInspector();
  public static final WritableFloatObjectInspector writableFloatObjectInspector =
      new WritableFloatObjectInspector();
  public static final WritableDoubleObjectInspector writableDoubleObjectInspector =
      new WritableDoubleObjectInspector();
  public static final WritableStringObjectInspector writableStringObjectInspector =
      new WritableStringObjectInspector();
  public static final WritableHiveCharObjectInspector writableHiveCharObjectInspector =
      new WritableHiveCharObjectInspector((CharTypeInfo) TypeInfoFactory.charTypeInfo);
  public static final WritableHiveVarcharObjectInspector writableHiveVarcharObjectInspector =
      new WritableHiveVarcharObjectInspector((VarcharTypeInfo) TypeInfoFactory.varcharTypeInfo);
  public static final WritableVoidObjectInspector writableVoidObjectInspector =
      new WritableVoidObjectInspector();
  public static final WritableDateObjectInspector writableDateObjectInspector =
      new WritableDateObjectInspector();
  public static final WritableTimestampObjectInspector writableTimestampObjectInspector =
      new WritableTimestampObjectInspector();
  public static final WritableBinaryObjectInspector writableBinaryObjectInspector =
      new WritableBinaryObjectInspector();
  public static final WritableHiveDecimalObjectInspector writableHiveDecimalObjectInspector =
      new WritableHiveDecimalObjectInspector(TypeInfoFactory.decimalTypeInfo);

  // Map from PrimitiveTypeInfo to AbstractPrimitiveWritableObjectInspector.
  private static HashMap<PrimitiveTypeInfo, AbstractPrimitiveWritableObjectInspector> cachedPrimitiveWritableInspectorCache =
      new HashMap<PrimitiveTypeInfo, AbstractPrimitiveWritableObjectInspector>();
  static {
    cachedPrimitiveWritableInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.BOOLEAN_TYPE_NAME),
        writableBooleanObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.TINYINT_TYPE_NAME),
        writableByteObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.SMALLINT_TYPE_NAME),
        writableShortObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.INT_TYPE_NAME),
        writableIntObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.BIGINT_TYPE_NAME),
        writableLongObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.FLOAT_TYPE_NAME),
        writableFloatObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.DOUBLE_TYPE_NAME),
        writableDoubleObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME),
        writableStringObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(TypeInfoFactory.charTypeInfo, writableHiveCharObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(TypeInfoFactory.varcharTypeInfo, writableHiveVarcharObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.VOID_TYPE_NAME),
        writableVoidObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.DATE_TYPE_NAME),
        writableDateObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.TIMESTAMP_TYPE_NAME),
        writableTimestampObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.BINARY_TYPE_NAME),
        writableBinaryObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(TypeInfoFactory.decimalTypeInfo, writableHiveDecimalObjectInspector);
  }

  private static Map<PrimitiveCategory, AbstractPrimitiveWritableObjectInspector> primitiveCategoryToWritableOI =
      new EnumMap<PrimitiveCategory, AbstractPrimitiveWritableObjectInspector>(PrimitiveCategory.class);
  static {
    primitiveCategoryToWritableOI.put(PrimitiveCategory.BOOLEAN, writableBooleanObjectInspector);
    primitiveCategoryToWritableOI.put(PrimitiveCategory.BYTE, writableByteObjectInspector);
    primitiveCategoryToWritableOI.put(PrimitiveCategory.SHORT, writableShortObjectInspector);
    primitiveCategoryToWritableOI.put(PrimitiveCategory.INT, writableIntObjectInspector);
    primitiveCategoryToWritableOI.put(PrimitiveCategory.LONG, writableLongObjectInspector);
    primitiveCategoryToWritableOI.put(PrimitiveCategory.FLOAT, writableFloatObjectInspector);
    primitiveCategoryToWritableOI.put(PrimitiveCategory.DOUBLE, writableDoubleObjectInspector);
    primitiveCategoryToWritableOI.put(PrimitiveCategory.STRING, writableStringObjectInspector);
    primitiveCategoryToWritableOI.put(PrimitiveCategory.CHAR, writableHiveCharObjectInspector);
    primitiveCategoryToWritableOI.put(PrimitiveCategory.VARCHAR, writableHiveVarcharObjectInspector);
    primitiveCategoryToWritableOI.put(PrimitiveCategory.VOID, writableVoidObjectInspector);
    primitiveCategoryToWritableOI.put(PrimitiveCategory.DATE, writableDateObjectInspector);
    primitiveCategoryToWritableOI.put(PrimitiveCategory.TIMESTAMP, writableTimestampObjectInspector);
    primitiveCategoryToWritableOI.put(PrimitiveCategory.BINARY, writableBinaryObjectInspector);
    primitiveCategoryToWritableOI.put(PrimitiveCategory.DECIMAL, writableHiveDecimalObjectInspector);
  }

  public static final JavaBooleanObjectInspector javaBooleanObjectInspector =
      new JavaBooleanObjectInspector();
  public static final JavaByteObjectInspector javaByteObjectInspector =
      new JavaByteObjectInspector();
  public static final JavaShortObjectInspector javaShortObjectInspector =
      new JavaShortObjectInspector();
  public static final JavaIntObjectInspector javaIntObjectInspector =
      new JavaIntObjectInspector();
  public static final JavaLongObjectInspector javaLongObjectInspector =
      new JavaLongObjectInspector();
  public static final JavaFloatObjectInspector javaFloatObjectInspector =
      new JavaFloatObjectInspector();
  public static final JavaDoubleObjectInspector javaDoubleObjectInspector =
      new JavaDoubleObjectInspector();
  public static final JavaStringObjectInspector javaStringObjectInspector =
      new JavaStringObjectInspector();
  public static final JavaHiveCharObjectInspector javaHiveCharObjectInspector =
      new JavaHiveCharObjectInspector((CharTypeInfo) TypeInfoFactory.charTypeInfo);
  public static final JavaHiveVarcharObjectInspector javaHiveVarcharObjectInspector =
      new JavaHiveVarcharObjectInspector((VarcharTypeInfo) TypeInfoFactory.varcharTypeInfo);
  public static final JavaVoidObjectInspector javaVoidObjectInspector =
      new JavaVoidObjectInspector();
  public static final JavaDateObjectInspector javaDateObjectInspector =
      new JavaDateObjectInspector();
  public static final JavaTimestampObjectInspector javaTimestampObjectInspector =
      new JavaTimestampObjectInspector();
  public static final JavaBinaryObjectInspector javaByteArrayObjectInspector =
      new JavaBinaryObjectInspector();
  public static final JavaHiveDecimalObjectInspector javaHiveDecimalObjectInspector =
      new JavaHiveDecimalObjectInspector(TypeInfoFactory.decimalTypeInfo);

  // Map from PrimitiveTypeInfo to AbstractPrimitiveJavaObjectInspector.
  private static HashMap<PrimitiveTypeInfo, AbstractPrimitiveJavaObjectInspector> cachedPrimitiveJavaInspectorCache =
      new HashMap<PrimitiveTypeInfo, AbstractPrimitiveJavaObjectInspector>();
  static {
    cachedPrimitiveJavaInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.BOOLEAN_TYPE_NAME),
        javaBooleanObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.TINYINT_TYPE_NAME),
        javaByteObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.SMALLINT_TYPE_NAME),
        javaShortObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.INT_TYPE_NAME),
        javaIntObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.BIGINT_TYPE_NAME),
        javaLongObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.FLOAT_TYPE_NAME),
        javaFloatObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.DOUBLE_TYPE_NAME),
        javaDoubleObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME),
        javaStringObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(TypeInfoFactory.charTypeInfo, javaHiveCharObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(TypeInfoFactory.varcharTypeInfo, javaHiveVarcharObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.VOID_TYPE_NAME),
        javaVoidObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.DATE_TYPE_NAME),
        javaDateObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.TIMESTAMP_TYPE_NAME),
        javaTimestampObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.BINARY_TYPE_NAME),
        javaByteArrayObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(TypeInfoFactory.decimalTypeInfo, javaHiveDecimalObjectInspector);
  }

  private static Map<PrimitiveCategory, AbstractPrimitiveJavaObjectInspector> primitiveCategoryToJavaOI =
      new EnumMap<PrimitiveCategory, AbstractPrimitiveJavaObjectInspector>(PrimitiveCategory.class);
  static {
    primitiveCategoryToJavaOI.put(PrimitiveCategory.BOOLEAN, javaBooleanObjectInspector);
    primitiveCategoryToJavaOI.put(PrimitiveCategory.BYTE, javaByteObjectInspector);
    primitiveCategoryToJavaOI.put(PrimitiveCategory.SHORT, javaShortObjectInspector);
    primitiveCategoryToJavaOI.put(PrimitiveCategory.INT, javaIntObjectInspector);
    primitiveCategoryToJavaOI.put(PrimitiveCategory.LONG, javaLongObjectInspector);
    primitiveCategoryToJavaOI.put(PrimitiveCategory.FLOAT, javaFloatObjectInspector);
    primitiveCategoryToJavaOI.put(PrimitiveCategory.DOUBLE, javaDoubleObjectInspector);
    primitiveCategoryToJavaOI.put(PrimitiveCategory.STRING, javaStringObjectInspector);
    primitiveCategoryToJavaOI.put(PrimitiveCategory.CHAR, javaHiveCharObjectInspector);
    primitiveCategoryToJavaOI.put(PrimitiveCategory.VARCHAR, javaHiveVarcharObjectInspector);
    primitiveCategoryToJavaOI.put(PrimitiveCategory.VOID, javaVoidObjectInspector);
    primitiveCategoryToJavaOI.put(PrimitiveCategory.DATE, javaDateObjectInspector);
    primitiveCategoryToJavaOI.put(PrimitiveCategory.TIMESTAMP, javaTimestampObjectInspector);
    primitiveCategoryToJavaOI.put(PrimitiveCategory.BINARY, javaByteArrayObjectInspector);
    primitiveCategoryToJavaOI.put(PrimitiveCategory.DECIMAL, javaHiveDecimalObjectInspector);
  }

  /**
   * Returns the PrimitiveWritableObjectInspector for the PrimitiveCategory.
   *
   * @param primitiveCategory
   */
  public static AbstractPrimitiveWritableObjectInspector getPrimitiveWritableObjectInspector(
      PrimitiveCategory primitiveCategory) {
    AbstractPrimitiveWritableObjectInspector result = primitiveCategoryToWritableOI.get(primitiveCategory);

    if (result == null) {
      throw new RuntimeException("Internal error: Cannot find ObjectInspector "
          + " for " + primitiveCategory);
    }

    return result;
  }

  /**
   * Returns the PrimitiveWritableObjectInspector for the given type info
   * @param PrimitiveTypeInfo    PrimitiveTypeInfo instance
   * @return AbstractPrimitiveWritableObjectInspector instance
   */
  public static AbstractPrimitiveWritableObjectInspector getPrimitiveWritableObjectInspector(
      PrimitiveTypeInfo typeInfo) {
    AbstractPrimitiveWritableObjectInspector result = cachedPrimitiveWritableInspectorCache.get(typeInfo);
    if (result != null) {
      return result;
    }

    switch (typeInfo.getPrimitiveCategory()) {
    case CHAR:
      result = new WritableHiveCharObjectInspector((CharTypeInfo) typeInfo);
      break;
    case VARCHAR:
      result = new WritableHiveVarcharObjectInspector((VarcharTypeInfo)typeInfo);
      break;
    case DECIMAL:
      result = new WritableHiveDecimalObjectInspector((DecimalTypeInfo)typeInfo);
      break;
    default:
      throw new RuntimeException("Failed to create object inspector for " + typeInfo );
    }

    cachedPrimitiveWritableInspectorCache.put(typeInfo, result);
    return result;
  }

  /**
   * Returns a PrimitiveWritableObjectInspector which implements ConstantObjectInspector
   * for the PrimitiveCategory.
   *
   * @param primitiveCategory
   * @param value
   */
  public static ConstantObjectInspector getPrimitiveWritableConstantObjectInspector(
      PrimitiveTypeInfo typeInfo, Object value) {
    switch (typeInfo.getPrimitiveCategory()) {
    case BOOLEAN:
      return new WritableConstantBooleanObjectInspector((BooleanWritable)value);
    case BYTE:
      return new WritableConstantByteObjectInspector((ByteWritable)value);
    case SHORT:
      return new WritableConstantShortObjectInspector((ShortWritable)value);
    case INT:
      return new WritableConstantIntObjectInspector((IntWritable)value);
    case LONG:
      return new WritableConstantLongObjectInspector((LongWritable)value);
    case FLOAT:
      return new WritableConstantFloatObjectInspector((FloatWritable)value);
    case DOUBLE:
      return new WritableConstantDoubleObjectInspector((DoubleWritable)value);
    case STRING:
      return new WritableConstantStringObjectInspector((Text)value);
    case CHAR:
      return new WritableConstantHiveCharObjectInspector((CharTypeInfo) typeInfo,
          (HiveCharWritable) value);
    case VARCHAR:
      return new WritableConstantHiveVarcharObjectInspector((VarcharTypeInfo)typeInfo,
          (HiveVarcharWritable)value);
    case DATE:
      return new WritableConstantDateObjectInspector((DateWritable)value);
    case TIMESTAMP:
      return new WritableConstantTimestampObjectInspector((TimestampWritable)value);
    case DECIMAL:
      return new WritableConstantHiveDecimalObjectInspector((DecimalTypeInfo)typeInfo, (HiveDecimalWritable)value);
    case BINARY:
      return new WritableConstantBinaryObjectInspector((BytesWritable)value);
    case VOID:
      return new WritableVoidObjectInspector();
    default:
      throw new RuntimeException("Internal error: Cannot find "
        + "ConstantObjectInspector for " + typeInfo);
    }
  }

  /**
   * Returns the PrimitiveJavaObjectInspector for the PrimitiveCategory.
   *
   * @param primitiveCategory
   */
  public static AbstractPrimitiveJavaObjectInspector getPrimitiveJavaObjectInspector(
      PrimitiveCategory primitiveCategory) {
    AbstractPrimitiveJavaObjectInspector result = primitiveCategoryToJavaOI.get(primitiveCategory);

    if (result == null) {
      throw new RuntimeException("Internal error: Cannot find ObjectInspector "
          + " for " + primitiveCategory);
    }

    return result;
  }

  /**
   * Returns the PrimitiveJavaObjectInspector for the given PrimitiveTypeInfo instance,
   * @param PrimitiveTypeInfo    PrimitiveTypeInfo instance
   * @return AbstractPrimitiveJavaObjectInspector instance
   */
  public static AbstractPrimitiveJavaObjectInspector getPrimitiveJavaObjectInspector(
        PrimitiveTypeInfo typeInfo) {
    AbstractPrimitiveJavaObjectInspector result = cachedPrimitiveJavaInspectorCache.get(typeInfo);
    if (result != null) {
      return result;
    }

    switch (typeInfo.getPrimitiveCategory()) {
    case CHAR:
      result = new JavaHiveCharObjectInspector((CharTypeInfo) typeInfo);
      break;
    case VARCHAR:
      result = new JavaHiveVarcharObjectInspector((VarcharTypeInfo)typeInfo);
      break;
    case DECIMAL:
      result = new JavaHiveDecimalObjectInspector((DecimalTypeInfo)typeInfo);
      break;
      default:
        throw new RuntimeException("Failed to create JavaHiveVarcharObjectInspector for " + typeInfo );
    }

    cachedPrimitiveJavaInspectorCache.put(typeInfo, result);
    return result;
  }

  /**
   * Returns an ObjectInspector for a primitive Class. The Class can be a Hive
   * Writable class, or a Java Primitive Class.
   *
   * A runtimeException will be thrown if the class is not recognized as a
   * primitive type by Hive.
   */
  public static PrimitiveObjectInspector getPrimitiveObjectInspectorFromClass(
      Class<?> c) {
    if (Writable.class.isAssignableFrom(c)) {
      // It is a writable class
      PrimitiveTypeEntry te = PrimitiveObjectInspectorUtils
          .getTypeEntryFromPrimitiveWritableClass(c);
      if (te == null) {
        throw new RuntimeException("Internal error: Cannot recognize " + c);
      }
      return PrimitiveObjectInspectorFactory
          .getPrimitiveWritableObjectInspector(te.primitiveCategory);
    } else {
      // It is a Java class
      PrimitiveTypeEntry te = PrimitiveObjectInspectorUtils
          .getTypeEntryFromPrimitiveJavaClass(c);
      if (te == null) {
        throw new RuntimeException("Internal error: Cannot recognize " + c);
      }
      return PrimitiveObjectInspectorFactory
          .getPrimitiveJavaObjectInspector(te.primitiveCategory);
    }
  }

  private PrimitiveObjectInspectorFactory() {
    // prevent instantiation
  }
}
