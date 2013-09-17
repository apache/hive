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

package org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;
import org.apache.hadoop.hive.serde2.typeinfo.BaseTypeParams;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeSpec;

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
  public static final LazyBinaryObjectInspector LAZY_BINARY_OBJECT_INSPECTOR =
      new LazyBinaryObjectInspector();
  public static final LazyHiveDecimalObjectInspector LAZY_BIG_DECIMAL_OBJECT_INSPECTOR =
      new LazyHiveDecimalObjectInspector();
  public static final LazyHiveVarcharObjectInspector LAZY_VARCHAR_OBJECT_INSPECTOR =
      new LazyHiveVarcharObjectInspector(PrimitiveObjectInspectorUtils.varcharTypeEntry);

  static HashMap<ArrayList<Object>, LazyStringObjectInspector> cachedLazyStringObjectInspector =
      new HashMap<ArrayList<Object>, LazyStringObjectInspector>();

  public static LazyStringObjectInspector getLazyStringObjectInspector(
      boolean escaped, byte escapeChar) {
    ArrayList<Object> signature = new ArrayList<Object>();
    signature.add(Boolean.valueOf(escaped));
    signature.add(Byte.valueOf(escapeChar));
    LazyStringObjectInspector result = cachedLazyStringObjectInspector
        .get(signature);
    if (result == null) {
      result = new LazyStringObjectInspector(escaped, escapeChar);
      cachedLazyStringObjectInspector.put(signature, result);
    }
    return result;
  }

  static PrimitiveObjectInspectorUtils.ParameterizedObjectInspectorMap
    cachedParameterizedLazyObjectInspectors =
      new PrimitiveObjectInspectorUtils.ParameterizedObjectInspectorMap();

  public static PrimitiveObjectInspector getParameterizedObjectInspector(
      PrimitiveTypeSpec typeSpec) {
    PrimitiveCategory primitiveCategory = typeSpec.getPrimitiveCategory();
    BaseTypeParams typeParams = typeSpec.getTypeParams();
    PrimitiveObjectInspector poi =
        cachedParameterizedLazyObjectInspectors.getObjectInspector(typeSpec);
    if (poi == null) {
      // Object inspector hasn't been cached for this type/params yet, create now
      switch (primitiveCategory) {
        case VARCHAR:
          PrimitiveTypeEntry typeEntry = PrimitiveObjectInspectorUtils.getTypeEntryFromTypeSpecs(
              primitiveCategory,
              typeParams);
          poi = new LazyHiveVarcharObjectInspector(typeEntry);
          poi.setTypeParams(typeParams);
          cachedParameterizedLazyObjectInspectors.setObjectInspector(poi);
          break;

        default:
          throw new RuntimeException(
              "Primitve type " + primitiveCategory + " should not take parameters");
      }
    }

    return poi;
  }
  public static AbstractPrimitiveLazyObjectInspector<?> getLazyObjectInspector(
      PrimitiveCategory primitiveCategory, boolean escaped, byte escapeChar) {
    switch (primitiveCategory) {
    case BOOLEAN:
      return LAZY_BOOLEAN_OBJECT_INSPECTOR;
    case BYTE:
      return LAZY_BYTE_OBJECT_INSPECTOR;
    case SHORT:
      return LAZY_SHORT_OBJECT_INSPECTOR;
    case INT:
      return LAZY_INT_OBJECT_INSPECTOR;
    case LONG:
      return LAZY_LONG_OBJECT_INSPECTOR;
    case FLOAT:
      return LAZY_FLOAT_OBJECT_INSPECTOR;
    case DOUBLE:
      return LAZY_DOUBLE_OBJECT_INSPECTOR;
    case STRING:
      return getLazyStringObjectInspector(escaped, escapeChar);
    case VARCHAR:
      return LAZY_VARCHAR_OBJECT_INSPECTOR;
    case BINARY:
      return LAZY_BINARY_OBJECT_INSPECTOR;
    case VOID:
      return LAZY_VOID_OBJECT_INSPECTOR;
    case DATE:
      return LAZY_DATE_OBJECT_INSPECTOR;
    case TIMESTAMP:
      return LAZY_TIMESTAMP_OBJECT_INSPECTOR;
    case DECIMAL:
      return LAZY_BIG_DECIMAL_OBJECT_INSPECTOR;
    default:
      throw new RuntimeException("Internal error: Cannot find ObjectInspector "
          + " for " + primitiveCategory);
    }
  }

  public static AbstractPrimitiveLazyObjectInspector<?> getLazyObjectInspector(
      PrimitiveTypeSpec typeSpec, boolean escaped, byte escapeChar) {
    PrimitiveCategory primitiveCategory = typeSpec.getPrimitiveCategory();
    BaseTypeParams typeParams = typeSpec.getTypeParams();

    if (typeParams == null) {
      return getLazyObjectInspector(primitiveCategory, escaped, escapeChar);
    } else {
      switch(primitiveCategory) {
        case VARCHAR:
          LazyHiveVarcharObjectInspector oi = (LazyHiveVarcharObjectInspector)
            getParameterizedObjectInspector(typeSpec);
          return oi;

        default:
          throw new RuntimeException("Type " + primitiveCategory + " does not take parameters");
      }
    }
  }

  private LazyPrimitiveObjectInspectorFactory() {
    // prevent instantiation
  }

}
