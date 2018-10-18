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

package org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

/**
 * LazyPrimitiveObjectInspectorFactory is the primary way to create new
 * ObjectInspector instances.
 *
 * SerDe classes should call the static functions in this library to create an
 * ObjectInspector to return to the caller of SerDe2.getObjectInspector().
 *
 * The reason of having caches here is that ObjectInspector is because
 * ObjectInspectors do not have an internal state - so ObjectInspectors with the
 * same construction parameters should result in exactly the same
 * ObjectInspector.
 */
public final class LazyPrimitiveObjectInspectorFactory {

  public static final LazyBooleanObjectInspector LAZY_BOOLEAN_OBJECT_INSPECTOR =
      new LazyBooleanObjectInspector();

  public static final LazyBooleanObjectInspector LAZY_EXT_BOOLEAN_OBJECT_INSPECTOR =
      new LazyBooleanObjectInspector();
  static {
    LAZY_EXT_BOOLEAN_OBJECT_INSPECTOR.setExtendedLiteral(true);
  }

  public static final LazyByteObjectInspector LAZY_BYTE_OBJECT_INSPECTOR =
      new LazyByteObjectInspector();
  public static final LazyShortObjectInspector LAZY_SHORT_OBJECT_INSPECTOR =
      new LazyShortObjectInspector();
  public static final LazyIntObjectInspector LAZY_INT_OBJECT_INSPECTOR =
      new LazyIntObjectInspector();
  public static final LazyLongObjectInspector LAZY_LONG_OBJECT_INSPECTOR =
      new LazyLongObjectInspector();
  public static final LazyFloatObjectInspector LAZY_FLOAT_OBJECT_INSPECTOR =
      new LazyFloatObjectInspector();
  public static final LazyDoubleObjectInspector LAZY_DOUBLE_OBJECT_INSPECTOR =
      new LazyDoubleObjectInspector();
  public static final LazyVoidObjectInspector LAZY_VOID_OBJECT_INSPECTOR =
      new LazyVoidObjectInspector();
  public static final LazyDateObjectInspector LAZY_DATE_OBJECT_INSPECTOR =
      new LazyDateObjectInspector();
  public static final LazyTimestampObjectInspector LAZY_TIMESTAMP_OBJECT_INSPECTOR =
      new LazyTimestampObjectInspector();
  public static final LazyHiveIntervalYearMonthObjectInspector LAZY_INTERVAL_YEAR_MONTH_OBJECT_INSPECTOR =
      new LazyHiveIntervalYearMonthObjectInspector();
  public static final LazyHiveIntervalDayTimeObjectInspector LAZY_INTERVAL_DAY_TIME_OBJECT_INSPECTOR =
      new LazyHiveIntervalDayTimeObjectInspector();
  public static final LazyBinaryObjectInspector LAZY_BINARY_OBJECT_INSPECTOR =
      new LazyBinaryObjectInspector();

  private LazyPrimitiveObjectInspectorFactory() {
    // prevent instantiation
  }

  // Lazy object inspectors for string/char/varchar will all be cached in the same map.
  // Map key will be list of [typeInfo, isEscaped, escapeChar]
  private static ConcurrentHashMap<ArrayList<Object>, AbstractPrimitiveLazyObjectInspector<?>> cachedLazyStringTypeOIs =
      new ConcurrentHashMap<ArrayList<Object>, AbstractPrimitiveLazyObjectInspector<?>>();

  private static ConcurrentHashMap<PrimitiveTypeInfo, AbstractPrimitiveLazyObjectInspector<?>>
     cachedPrimitiveLazyObjectInspectors =
    new ConcurrentHashMap<PrimitiveTypeInfo, AbstractPrimitiveLazyObjectInspector<?>>();
  static {
    cachedPrimitiveLazyObjectInspectors.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.BOOLEAN_TYPE_NAME),
        LAZY_BOOLEAN_OBJECT_INSPECTOR);
    cachedPrimitiveLazyObjectInspectors.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.TINYINT_TYPE_NAME),
        LAZY_BYTE_OBJECT_INSPECTOR);
    cachedPrimitiveLazyObjectInspectors.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.SMALLINT_TYPE_NAME),
        LAZY_SHORT_OBJECT_INSPECTOR);
    cachedPrimitiveLazyObjectInspectors.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.INT_TYPE_NAME),
        LAZY_INT_OBJECT_INSPECTOR);
    cachedPrimitiveLazyObjectInspectors.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.FLOAT_TYPE_NAME),
        LAZY_FLOAT_OBJECT_INSPECTOR);
    cachedPrimitiveLazyObjectInspectors.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.DOUBLE_TYPE_NAME),
        LAZY_DOUBLE_OBJECT_INSPECTOR);
    cachedPrimitiveLazyObjectInspectors.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.BIGINT_TYPE_NAME),
        LAZY_LONG_OBJECT_INSPECTOR);
    cachedPrimitiveLazyObjectInspectors.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.VOID_TYPE_NAME),
        LAZY_VOID_OBJECT_INSPECTOR);
    cachedPrimitiveLazyObjectInspectors.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.DATE_TYPE_NAME),
        LAZY_DATE_OBJECT_INSPECTOR);
    cachedPrimitiveLazyObjectInspectors.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.TIMESTAMP_TYPE_NAME),
        LAZY_TIMESTAMP_OBJECT_INSPECTOR);
    cachedPrimitiveLazyObjectInspectors.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME),
        LAZY_INTERVAL_YEAR_MONTH_OBJECT_INSPECTOR);
    cachedPrimitiveLazyObjectInspectors.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME),
        LAZY_INTERVAL_DAY_TIME_OBJECT_INSPECTOR);
    cachedPrimitiveLazyObjectInspectors.put(TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.BINARY_TYPE_NAME),
        LAZY_BINARY_OBJECT_INSPECTOR);
  }

  public static AbstractPrimitiveLazyObjectInspector<?> getLazyObjectInspector(
      PrimitiveTypeInfo typeInfo, boolean escaped, byte escapeChar) {
    return getLazyObjectInspector(typeInfo, escaped, escapeChar, false);
  }

  public static AbstractPrimitiveLazyObjectInspector<?> getLazyObjectInspector(
      PrimitiveTypeInfo typeInfo, boolean escaped, byte escapeChar, boolean extBoolean) {
    LazyObjectInspectorParameters lazyParams = new LazyObjectInspectorParametersImpl(
        escaped, escapeChar, extBoolean, null, null, null);
    return getLazyObjectInspector(typeInfo, lazyParams);
  }

  public static AbstractPrimitiveLazyObjectInspector<?> getLazyObjectInspector(
      PrimitiveTypeInfo typeInfo, LazyObjectInspectorParameters lazyParams) {
    PrimitiveCategory primitiveCategory = typeInfo.getPrimitiveCategory();

    switch(primitiveCategory) {
    case STRING:
      return getLazyStringObjectInspector(lazyParams.isEscaped(), lazyParams.getEscapeChar());
    case CHAR:
      return getLazyHiveCharObjectInspector((CharTypeInfo)typeInfo,
          lazyParams.isEscaped(), lazyParams.getEscapeChar());
    case VARCHAR:
      return getLazyHiveVarcharObjectInspector((VarcharTypeInfo)typeInfo,
          lazyParams.isEscaped(), lazyParams.getEscapeChar());
    case BOOLEAN:
      return getLazyBooleanObjectInspector(lazyParams.isExtendedBooleanLiteral());
    case TIMESTAMP:
      return getLazyTimestampObjectInspector(lazyParams.getTimestampFormats());
    default:
     return getLazyObjectInspector(typeInfo);
    }
  }

  public static AbstractPrimitiveLazyObjectInspector<?> getLazyObjectInspector(
      PrimitiveTypeInfo typeInfo) {
    AbstractPrimitiveLazyObjectInspector<?> poi = cachedPrimitiveLazyObjectInspectors.get(typeInfo);
    if (poi != null) {
      return poi;
    }

    // Object inspector hasn't been cached for this type/params yet, create now
    switch (typeInfo.getPrimitiveCategory()) {
    case CHAR:
      poi = new LazyHiveCharObjectInspector((CharTypeInfo) typeInfo);
      break;
    case VARCHAR:
      poi = new LazyHiveVarcharObjectInspector((VarcharTypeInfo)typeInfo);
      break;
    case DECIMAL:
      poi = new LazyHiveDecimalObjectInspector((DecimalTypeInfo)typeInfo);
      break;
    case TIMESTAMPLOCALTZ:
      poi = new LazyTimestampLocalTZObjectInspector((TimestampLocalTZTypeInfo)typeInfo);
      break;
    default:
      throw new RuntimeException(
          "Primitive type " + typeInfo.getPrimitiveCategory() + " should not take parameters");
    }

    AbstractPrimitiveLazyObjectInspector<?> prev =
      cachedPrimitiveLazyObjectInspectors.putIfAbsent(typeInfo, poi);
    if (prev != null) {
      poi = prev;
    }
    return poi;
  }

  public static LazyStringObjectInspector getLazyStringObjectInspector(boolean escaped, byte escapeChar) {
    ArrayList<Object> signature = new ArrayList<Object>();
    signature.add(TypeInfoFactory.stringTypeInfo);
    signature.add(Boolean.valueOf(escaped));
    signature.add(Byte.valueOf(escapeChar));
    LazyStringObjectInspector result = (LazyStringObjectInspector) cachedLazyStringTypeOIs
        .get(signature);
    if (result == null) {
      result = new LazyStringObjectInspector(escaped, escapeChar);
      AbstractPrimitiveLazyObjectInspector<?> prev =
        cachedLazyStringTypeOIs.putIfAbsent(signature, result);
      if (prev != null) {
        result = (LazyStringObjectInspector) prev;
      }
    }
    return result;
  }

  public static LazyHiveCharObjectInspector getLazyHiveCharObjectInspector(
      CharTypeInfo typeInfo, boolean escaped, byte escapeChar) {
    ArrayList<Object> signature = new ArrayList<Object>();
    signature.add(typeInfo);
    signature.add(Boolean.valueOf(escaped));
    signature.add(Byte.valueOf(escapeChar));
    LazyHiveCharObjectInspector result = (LazyHiveCharObjectInspector) cachedLazyStringTypeOIs
        .get(signature);
    if (result == null) {
      result = new LazyHiveCharObjectInspector(typeInfo, escaped, escapeChar);
      AbstractPrimitiveLazyObjectInspector<?> prev =
        cachedLazyStringTypeOIs.putIfAbsent(signature, result);
      if (prev != null) {
        result = (LazyHiveCharObjectInspector) prev;
      }
    }
    return result;
  }

  public static LazyHiveVarcharObjectInspector getLazyHiveVarcharObjectInspector(
      VarcharTypeInfo typeInfo, boolean escaped, byte escapeChar) {
    ArrayList<Object> signature = new ArrayList<Object>();
    signature.add(typeInfo);
    signature.add(Boolean.valueOf(escaped));
    signature.add(Byte.valueOf(escapeChar));
    LazyHiveVarcharObjectInspector result = (LazyHiveVarcharObjectInspector) cachedLazyStringTypeOIs
        .get(signature);
    if (result == null) {
      result = new LazyHiveVarcharObjectInspector(typeInfo, escaped, escapeChar);
      AbstractPrimitiveLazyObjectInspector<?> prev =
        cachedLazyStringTypeOIs.putIfAbsent(signature, result);
      if (prev != null) {
        result = (LazyHiveVarcharObjectInspector) prev;
      }
    }
    return result;
  }

  public static LazyTimestampObjectInspector getLazyTimestampObjectInspector(
      List<String> tsFormats) {
    if (tsFormats == null) {
      // No timestamp format specified, just use default lazy inspector
      return (LazyTimestampObjectInspector) getLazyObjectInspector(TypeInfoFactory.timestampTypeInfo);
    }

    ArrayList<Object> signature = new ArrayList<Object>();
    signature.add(TypeInfoFactory.timestampTypeInfo);
    signature.add(tsFormats);
    LazyTimestampObjectInspector result = (LazyTimestampObjectInspector) cachedLazyStringTypeOIs
        .get(signature);
    if (result == null) {
      result = new LazyTimestampObjectInspector(tsFormats);
      AbstractPrimitiveLazyObjectInspector<?> prev =
        cachedLazyStringTypeOIs.putIfAbsent(signature, result);
      if (prev != null) {
        result = (LazyTimestampObjectInspector) prev;
      }
    }
    return result;
  }

  private static LazyBooleanObjectInspector getLazyBooleanObjectInspector(boolean extLiteral) {
    return extLiteral ? LAZY_EXT_BOOLEAN_OBJECT_INSPECTOR : LAZY_BOOLEAN_OBJECT_INSPECTOR;
  }

}
