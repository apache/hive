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

package org.apache.hadoop.hive.serde2.typeinfo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;

/**
 * TypeInfoFactory can be used to create the TypeInfo object for any types.
 *
 * TypeInfo objects are all read-only so we can reuse them easily.
 * TypeInfoFactory has internal cache to make sure we don't create 2 TypeInfo
 * objects that represents the same type.
 */
public final class TypeInfoFactory {

  private TypeInfoFactory() {
    // prevent instantiation
  }

  public static final PrimitiveTypeInfo voidTypeInfo = new PrimitiveTypeInfo(serdeConstants.VOID_TYPE_NAME);
  public static final PrimitiveTypeInfo booleanTypeInfo = new PrimitiveTypeInfo(serdeConstants.BOOLEAN_TYPE_NAME);
  public static final PrimitiveTypeInfo intTypeInfo = new PrimitiveTypeInfo(serdeConstants.INT_TYPE_NAME);
  public static final PrimitiveTypeInfo longTypeInfo = new PrimitiveTypeInfo(serdeConstants.BIGINT_TYPE_NAME);
  public static final PrimitiveTypeInfo stringTypeInfo = new PrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME);
  public static final PrimitiveTypeInfo charTypeInfo = new CharTypeInfo(HiveChar.MAX_CHAR_LENGTH);
  public static final PrimitiveTypeInfo varcharTypeInfo = new VarcharTypeInfo(HiveVarchar.MAX_VARCHAR_LENGTH);
  public static final PrimitiveTypeInfo floatTypeInfo = new PrimitiveTypeInfo(serdeConstants.FLOAT_TYPE_NAME);
  public static final PrimitiveTypeInfo doubleTypeInfo = new PrimitiveTypeInfo(serdeConstants.DOUBLE_TYPE_NAME);
  public static final PrimitiveTypeInfo byteTypeInfo = new PrimitiveTypeInfo(serdeConstants.TINYINT_TYPE_NAME);
  public static final PrimitiveTypeInfo shortTypeInfo = new PrimitiveTypeInfo(serdeConstants.SMALLINT_TYPE_NAME);
  public static final PrimitiveTypeInfo dateTypeInfo = new PrimitiveTypeInfo(serdeConstants.DATE_TYPE_NAME);
  public static final PrimitiveTypeInfo timestampTypeInfo = new PrimitiveTypeInfo(serdeConstants.TIMESTAMP_TYPE_NAME);
  public static final PrimitiveTypeInfo intervalYearMonthTypeInfo = new PrimitiveTypeInfo(serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME);
  public static final PrimitiveTypeInfo intervalDayTimeTypeInfo = new PrimitiveTypeInfo(serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME);
  public static final PrimitiveTypeInfo binaryTypeInfo = new PrimitiveTypeInfo(serdeConstants.BINARY_TYPE_NAME);

  /**
   * A DecimalTypeInfo instance that has max precision and max scale.
   */
  public static final DecimalTypeInfo decimalTypeInfo = new DecimalTypeInfo(HiveDecimal.SYSTEM_DEFAULT_PRECISION,
      HiveDecimal.SYSTEM_DEFAULT_SCALE);

  public static final PrimitiveTypeInfo unknownTypeInfo = new PrimitiveTypeInfo("unknown");

  // Map from type name (such as int or varchar(40) to the corresponding PrimitiveTypeInfo
  // instance.
  private static ConcurrentHashMap<String, PrimitiveTypeInfo> cachedPrimitiveTypeInfo =
      new ConcurrentHashMap<String, PrimitiveTypeInfo>();
  static {
    cachedPrimitiveTypeInfo.put(serdeConstants.VOID_TYPE_NAME, voidTypeInfo);
    cachedPrimitiveTypeInfo.put(serdeConstants.BOOLEAN_TYPE_NAME, booleanTypeInfo);
    cachedPrimitiveTypeInfo.put(serdeConstants.INT_TYPE_NAME, intTypeInfo);
    cachedPrimitiveTypeInfo.put(serdeConstants.BIGINT_TYPE_NAME, longTypeInfo);
    cachedPrimitiveTypeInfo.put(serdeConstants.STRING_TYPE_NAME, stringTypeInfo);
    cachedPrimitiveTypeInfo.put(charTypeInfo.getQualifiedName(), charTypeInfo);
    cachedPrimitiveTypeInfo.put(varcharTypeInfo.getQualifiedName(), varcharTypeInfo);
    cachedPrimitiveTypeInfo.put(serdeConstants.FLOAT_TYPE_NAME, floatTypeInfo);
    cachedPrimitiveTypeInfo.put(serdeConstants.DOUBLE_TYPE_NAME, doubleTypeInfo);
    cachedPrimitiveTypeInfo.put(serdeConstants.TINYINT_TYPE_NAME, byteTypeInfo);
    cachedPrimitiveTypeInfo.put(serdeConstants.SMALLINT_TYPE_NAME, shortTypeInfo);
    cachedPrimitiveTypeInfo.put(serdeConstants.DATE_TYPE_NAME, dateTypeInfo);
    cachedPrimitiveTypeInfo.put(serdeConstants.TIMESTAMP_TYPE_NAME, timestampTypeInfo);
    cachedPrimitiveTypeInfo.put(serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME, intervalYearMonthTypeInfo);
    cachedPrimitiveTypeInfo.put(serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME, intervalDayTimeTypeInfo);
    cachedPrimitiveTypeInfo.put(serdeConstants.BINARY_TYPE_NAME, binaryTypeInfo);
    cachedPrimitiveTypeInfo.put(decimalTypeInfo.getQualifiedName(), decimalTypeInfo);
    cachedPrimitiveTypeInfo.put("unknown", unknownTypeInfo);
  }

  /**
   * Get PrimitiveTypeInfo instance for the given type name of a type
   * including types with parameters, such as varchar(20).
   *
   * @param typeName type name possibly with parameters.
   * @return aPrimitiveTypeInfo instance
   */
  public static PrimitiveTypeInfo getPrimitiveTypeInfo(String typeName) {
    PrimitiveTypeInfo result = cachedPrimitiveTypeInfo.get(typeName);
    if (result != null) {
      return result;
    }

    // Not found in the cache. Must be parameterized types. Create it.
    result = createPrimitiveTypeInfo(typeName);
    if (result == null) {
      throw new RuntimeException("Error creating PrimitiveTypeInfo instance for " + typeName);
    }

    PrimitiveTypeInfo prev = cachedPrimitiveTypeInfo.putIfAbsent(typeName, result);
    if (prev != null) {
      result = prev;
    }
    return result;
  }

  /**
   * Create PrimitiveTypeInfo instance for the given full name of the type. The returned
   * type is one of the parameterized type info such as VarcharTypeInfo.
   *
   * @param fullName Fully qualified name of the type
   * @return PrimitiveTypeInfo instance
   */
  private static PrimitiveTypeInfo createPrimitiveTypeInfo(String fullName) {
    String baseName = TypeInfoUtils.getBaseName(fullName);
    PrimitiveTypeEntry typeEntry =
        PrimitiveObjectInspectorUtils.getTypeEntryFromTypeName(baseName);
    if (null == typeEntry) {
      throw new RuntimeException("Unknown type " + fullName);
    }

    TypeInfoUtils.PrimitiveParts parts = TypeInfoUtils.parsePrimitiveParts(fullName);
    if (parts.typeParams == null || parts.typeParams.length < 1) {
      return null;
    }

    switch (typeEntry.primitiveCategory) {
      case CHAR:
        if (parts.typeParams.length != 1) {
          return null;
        }
        return new CharTypeInfo(Integer.valueOf(parts.typeParams[0]));
      case VARCHAR:
        if (parts.typeParams.length != 1) {
          return null;
        }
        return new VarcharTypeInfo(Integer.valueOf(parts.typeParams[0]));
      case DECIMAL:
        if (parts.typeParams.length != 2) {
          return null;
        }
        return new DecimalTypeInfo(Integer.valueOf(parts.typeParams[0]),
            Integer.valueOf(parts.typeParams[1]));
      default:
        return null;
    }
  }

  public static CharTypeInfo getCharTypeInfo(int length) {
    String fullName = BaseCharTypeInfo.getQualifiedName(serdeConstants.CHAR_TYPE_NAME, length);
    return (CharTypeInfo) getPrimitiveTypeInfo(fullName);
  }

  public static VarcharTypeInfo getVarcharTypeInfo(int length) {
    String fullName = BaseCharTypeInfo.getQualifiedName(serdeConstants.VARCHAR_TYPE_NAME, length);
    return (VarcharTypeInfo) getPrimitiveTypeInfo(fullName);
  }

  public static DecimalTypeInfo getDecimalTypeInfo(int precision, int scale) {
    String fullName = DecimalTypeInfo.getQualifiedName(precision, scale);
    return (DecimalTypeInfo) getPrimitiveTypeInfo(fullName);
  };

  public static TypeInfo getPrimitiveTypeInfoFromPrimitiveWritable(
      Class<?> clazz) {
    String typeName = PrimitiveObjectInspectorUtils
        .getTypeNameFromPrimitiveWritable(clazz);
    if (typeName == null) {
      throw new RuntimeException("Internal error: Cannot get typeName for "
          + clazz);
    }
    return getPrimitiveTypeInfo(typeName);
  }

  public static TypeInfo getPrimitiveTypeInfoFromJavaPrimitive(Class<?> clazz) {
    return getPrimitiveTypeInfo(PrimitiveObjectInspectorUtils
        .getTypeNameFromPrimitiveJava(clazz));
  }

  static ConcurrentHashMap<ArrayList<List<?>>, TypeInfo> cachedStructTypeInfo =
    new ConcurrentHashMap<ArrayList<List<?>>, TypeInfo>();

  public static TypeInfo getStructTypeInfo(List<String> names,
      List<TypeInfo> typeInfos) {
    ArrayList<List<?>> signature = new ArrayList<List<?>>(2);
    signature.add(names);
    signature.add(typeInfos);
    TypeInfo result = cachedStructTypeInfo.get(signature);
    if (result == null) {
      result = new StructTypeInfo(names, typeInfos);
      TypeInfo prev = cachedStructTypeInfo.putIfAbsent(signature, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  static ConcurrentHashMap<List<?>, TypeInfo> cachedUnionTypeInfo =
    new ConcurrentHashMap<List<?>, TypeInfo>();

  public static TypeInfo getUnionTypeInfo(List<TypeInfo> typeInfos) {
    TypeInfo result = cachedUnionTypeInfo.get(typeInfos);
    if (result == null) {
      result = new UnionTypeInfo(typeInfos);
      TypeInfo prev = cachedUnionTypeInfo.putIfAbsent(typeInfos, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  static ConcurrentHashMap<TypeInfo, TypeInfo> cachedListTypeInfo = new ConcurrentHashMap<TypeInfo, TypeInfo>();

  public static TypeInfo getListTypeInfo(TypeInfo elementTypeInfo) {
    TypeInfo result = cachedListTypeInfo.get(elementTypeInfo);
    if (result == null) {
      result = new ListTypeInfo(elementTypeInfo);
      TypeInfo prev = cachedListTypeInfo.putIfAbsent(elementTypeInfo, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  static ConcurrentHashMap<ArrayList<TypeInfo>, TypeInfo> cachedMapTypeInfo =
    new ConcurrentHashMap<ArrayList<TypeInfo>, TypeInfo>();

  public static TypeInfo getMapTypeInfo(TypeInfo keyTypeInfo,
      TypeInfo valueTypeInfo) {
    ArrayList<TypeInfo> signature = new ArrayList<TypeInfo>(2);
    signature.add(keyTypeInfo);
    signature.add(valueTypeInfo);
    TypeInfo result = cachedMapTypeInfo.get(signature);
    if (result == null) {
      result = new MapTypeInfo(keyTypeInfo, valueTypeInfo);
      TypeInfo prev = cachedMapTypeInfo.putIfAbsent(signature, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

}
