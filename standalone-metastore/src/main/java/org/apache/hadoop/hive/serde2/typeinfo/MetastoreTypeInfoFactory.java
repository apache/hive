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
package org.apache.hadoop.hive.serde2.typeinfo;

import org.apache.hadoop.hive.metastore.ColumnType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class MetastoreTypeInfoFactory implements ITypeInfoFactory {

  private static final MetastoreTypeInfoFactory instance = new MetastoreTypeInfoFactory();

  public static final MetastoreTypeInfoFactory getInstance() {
    return instance;
  }
  private static ConcurrentHashMap<String, MetastorePrimitiveTypeInfo> cachedPrimitiveTypeInfo =
      new ConcurrentHashMap<>();

  @Override
  public MetastorePrimitiveTypeInfo getPrimitiveTypeInfo(String typeName, Object... parameters) {
    String qualifiedTypeName = MetastoreTypeInfoUtils
        .getQualifiedPrimitiveTypeName(typeName, parameters);
    MetastorePrimitiveTypeInfo result = cachedPrimitiveTypeInfo.get(qualifiedTypeName);
    if (result != null) {
      return result;
    }

    if (ColumnType.CHAR_TYPE_NAME.equals(typeName) || ColumnType.VARCHAR_TYPE_NAME
        .equals(typeName)) {
      MetastoreTypeInfoUtils.validateCharVarCharParameters((int) parameters[0]);
    } else if (ColumnType.DECIMAL_TYPE_NAME.equals(typeName)) {
      MetastoreTypeInfoUtils.validateDecimalParameters((int) parameters[0], (int) parameters[1]);
    }
    // Not found in the cache. Must be parameterized types. Create it.
    result = new MetastorePrimitiveTypeInfo(qualifiedTypeName);

    MetastorePrimitiveTypeInfo prev = cachedPrimitiveTypeInfo.putIfAbsent(qualifiedTypeName, result);
    if (prev != null) {
      result = prev;
    }
    return result;
  }

  private static ConcurrentHashMap<ArrayList<TypeInfo>, MapTypeInfo> cachedMapTypeInfo =
      new ConcurrentHashMap<>();
  @Override
  public MapTypeInfo getMapTypeInfo(TypeInfo keyTypeInfo,
      TypeInfo valueTypeInfo) {
    ArrayList<TypeInfo> signature = new ArrayList<TypeInfo>(2);
    signature.add(keyTypeInfo);
    signature.add(valueTypeInfo);
    MapTypeInfo result = cachedMapTypeInfo.get(signature);
    if (result == null) {
      result = new MapTypeInfo(keyTypeInfo, valueTypeInfo);
      MapTypeInfo prev = cachedMapTypeInfo.putIfAbsent(signature, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  private static ConcurrentHashMap<TypeInfo, ListTypeInfo> cachedListTypeInfo = new ConcurrentHashMap<>();

  @Override
  public ListTypeInfo getListTypeInfo(TypeInfo listElementTypeInfo) {
    ListTypeInfo result = cachedListTypeInfo.get(listElementTypeInfo);
    if (result == null) {
      result = new ListTypeInfo(listElementTypeInfo);
      ListTypeInfo prev = cachedListTypeInfo.putIfAbsent(listElementTypeInfo, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  private static ConcurrentHashMap<List<?>, UnionTypeInfo> cachedUnionTypeInfo =
      new ConcurrentHashMap<>();

  @Override
  public UnionTypeInfo getUnionTypeInfo(List<TypeInfo> typeInfos) {
    UnionTypeInfo result = cachedUnionTypeInfo.get(typeInfos);
    if (result == null) {
      result = new UnionTypeInfo(typeInfos);
      UnionTypeInfo prev = cachedUnionTypeInfo.putIfAbsent(typeInfos, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }
  static ConcurrentHashMap<ArrayList<List<?>>, StructTypeInfo> cachedStructTypeInfo =
      new ConcurrentHashMap<>();
  @Override
  public StructTypeInfo getStructTypeInfo(List<String> names,
      List<TypeInfo> typeInfos) {
      ArrayList<List<?>> signature = new ArrayList<List<?>>(2);
      signature.add(names);
      signature.add(typeInfos);
    StructTypeInfo result = cachedStructTypeInfo.get(signature);
      if (result == null) {
        result = new StructTypeInfo(names, typeInfos);
        StructTypeInfo prev = cachedStructTypeInfo.putIfAbsent(signature, result);
        if (prev != null) {
          result = prev;
        }
      }
      return result;
  }
}
