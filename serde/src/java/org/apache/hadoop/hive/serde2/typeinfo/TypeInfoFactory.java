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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
  private static Log LOG = LogFactory.getLog(TypeInfoFactory.class);
  static ConcurrentHashMap<String, TypeInfo> cachedPrimitiveTypeInfo = new ConcurrentHashMap<String, TypeInfo>();

  private TypeInfoFactory() {
    // prevent instantiation
  }

  public static TypeInfo getPrimitiveTypeInfo(String typeName) {
    PrimitiveTypeEntry typeEntry = PrimitiveObjectInspectorUtils
        .getTypeEntryFromTypeName(TypeInfoUtils.getBaseName(typeName));
    if (null == typeEntry) {
      throw new RuntimeException("Cannot getPrimitiveTypeInfo for " + typeName);
    }
    TypeInfo result = cachedPrimitiveTypeInfo.get(typeName);
    if (result == null) {
      TypeInfoUtils.PrimitiveParts parts = TypeInfoUtils.parsePrimitiveParts(typeName);
      // Create params if there are any
      if (parts.typeParams != null && parts.typeParams.length > 0) {
        // The type string came with parameters.  Parse and add to TypeInfo
        try {
          BaseTypeParams typeParams = PrimitiveTypeEntry.createTypeParams(
              parts.typeName, parts.typeParams);
          result = new PrimitiveTypeInfo(typeName);
          ((PrimitiveTypeInfo) result).setTypeParams(typeParams);
        } catch (Exception err) {
          LOG.error(err);
          throw new RuntimeException("Error creating type parameters for " + typeName
              + ": " + err, err);
        }
      } else {
        // No type params
        result = new PrimitiveTypeInfo(parts.typeName);
      }

      cachedPrimitiveTypeInfo.put(typeName, result);
    }
    return result;
  }

  public static final TypeInfo voidTypeInfo = getPrimitiveTypeInfo(serdeConstants.VOID_TYPE_NAME);
  public static final TypeInfo booleanTypeInfo = getPrimitiveTypeInfo(serdeConstants.BOOLEAN_TYPE_NAME);
  public static final TypeInfo intTypeInfo = getPrimitiveTypeInfo(serdeConstants.INT_TYPE_NAME);
  public static final TypeInfo longTypeInfo = getPrimitiveTypeInfo(serdeConstants.BIGINT_TYPE_NAME);
  public static final TypeInfo stringTypeInfo = getPrimitiveTypeInfo(serdeConstants.STRING_TYPE_NAME);
  public static final TypeInfo floatTypeInfo = getPrimitiveTypeInfo(serdeConstants.FLOAT_TYPE_NAME);
  public static final TypeInfo doubleTypeInfo = getPrimitiveTypeInfo(serdeConstants.DOUBLE_TYPE_NAME);
  public static final TypeInfo byteTypeInfo = getPrimitiveTypeInfo(serdeConstants.TINYINT_TYPE_NAME);
  public static final TypeInfo shortTypeInfo = getPrimitiveTypeInfo(serdeConstants.SMALLINT_TYPE_NAME);
  public static final TypeInfo dateTypeInfo = getPrimitiveTypeInfo(serdeConstants.DATE_TYPE_NAME);
  public static final TypeInfo timestampTypeInfo = getPrimitiveTypeInfo(serdeConstants.TIMESTAMP_TYPE_NAME);
  public static final TypeInfo binaryTypeInfo = getPrimitiveTypeInfo(serdeConstants.BINARY_TYPE_NAME);
  public static final TypeInfo decimalTypeInfo = getPrimitiveTypeInfo(serdeConstants.DECIMAL_TYPE_NAME);
  // Disallow usage of varchar without length specifier.
  //public static final TypeInfo varcharTypeInfo = getPrimitiveTypeInfo(serdeConstants.VARCHAR_TYPE_NAME);

  public static final TypeInfo unknownTypeInfo = getPrimitiveTypeInfo("unknown");

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
      cachedStructTypeInfo.put(signature, result);
    }
    return result;
  }

  static ConcurrentHashMap<List<?>, TypeInfo> cachedUnionTypeInfo =
    new ConcurrentHashMap<List<?>, TypeInfo>();

  public static TypeInfo getUnionTypeInfo(List<TypeInfo> typeInfos) {
    TypeInfo result = cachedUnionTypeInfo.get(typeInfos);
    if (result == null) {
      result = new UnionTypeInfo(typeInfos);
      cachedUnionTypeInfo.put(typeInfos, result);
    }
    return result;
  }

  static ConcurrentHashMap<TypeInfo, TypeInfo> cachedListTypeInfo = new ConcurrentHashMap<TypeInfo, TypeInfo>();

  public static TypeInfo getListTypeInfo(TypeInfo elementTypeInfo) {
    TypeInfo result = cachedListTypeInfo.get(elementTypeInfo);
    if (result == null) {
      result = new ListTypeInfo(elementTypeInfo);
      cachedListTypeInfo.put(elementTypeInfo, result);
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
      cachedMapTypeInfo.put(signature, result);
    }
    return result;
  };

}
