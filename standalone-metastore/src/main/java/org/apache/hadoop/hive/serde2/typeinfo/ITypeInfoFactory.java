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

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;

import java.util.List;

@LimitedPrivate("Hive")
public interface ITypeInfoFactory {
  /**
   * Get or create a Primitive TypeInfo object of name typeName and parameters provided by
   * paramaters. Eg. a primitive typeInfo of char(10) can be represented a typename --> char
   * and 10 as the parameter. Similarly, decimal(10,2) has typename decimal and 10,2 as
   * parameters
   *
   * @param typeName   name of the type
   * @param parameters optional parameters in case of parameterized primitive types
   * @return TypeInfo representing the primitive typeInfo
   */
  MetastorePrimitiveTypeInfo getPrimitiveTypeInfo(String typeName, Object... parameters);

  /**
   * Get or create a Map type TypeInfo
   *
   * @param keyTypeInfo   TypeInfo for the key
   * @param valueTypeInfo TypeInfo for the value
   * @return MapTypeInfo
   */
  MapTypeInfo getMapTypeInfo(TypeInfo keyTypeInfo, TypeInfo valueTypeInfo);

  /**
   * Get or create a List type TypeInfo
   *
   * @param listElementTypeInfo TypeInfo of the list elements
   * @return ListTypeInfo
   */
  ListTypeInfo getListTypeInfo(TypeInfo listElementTypeInfo);

  /**
   * Get or create a UnionTypeInfo
   *
   * @param typeInfos child TypeInfos for the UnionTypeInfo
   * @return UnionTypeInfo
   */
  UnionTypeInfo getUnionTypeInfo(List<TypeInfo> typeInfos);

  /**
   * Get or create a StructTypeInfo
   *
   * @param names     names of the fields in the struct typeInfo
   * @param typeInfos TypeInfos for each fields
   * @return StructTypeInfo
   */
  StructTypeInfo getStructTypeInfo(List<String> names, List<TypeInfo> typeInfos);
}
