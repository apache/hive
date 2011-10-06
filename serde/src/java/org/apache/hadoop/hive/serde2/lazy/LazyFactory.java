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
package org.apache.hadoop.hive.serde2.lazy;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyListObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyUnionObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyByteObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyFloatObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyIntObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyLongObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyShortObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyStringObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.Text;

/**
 * LazyFactory.
 *
 */
public final class LazyFactory {

  /**
   * Create a lazy primitive class given the type name.
   */
  public static LazyPrimitive<?, ?> createLazyPrimitiveClass(
      PrimitiveObjectInspector oi) {
    PrimitiveCategory p = oi.getPrimitiveCategory();
    switch (p) {
    case BOOLEAN:
      return new LazyBoolean((LazyBooleanObjectInspector) oi);
    case BYTE:
      return new LazyByte((LazyByteObjectInspector) oi);
    case SHORT:
      return new LazyShort((LazyShortObjectInspector) oi);
    case INT:
      return new LazyInteger((LazyIntObjectInspector) oi);
    case LONG:
      return new LazyLong((LazyLongObjectInspector) oi);
    case FLOAT:
      return new LazyFloat((LazyFloatObjectInspector) oi);
    case DOUBLE:
      return new LazyDouble((LazyDoubleObjectInspector) oi);
    case STRING:
      return new LazyString((LazyStringObjectInspector) oi);
    case TIMESTAMP:
      return new LazyTimestamp((LazyTimestampObjectInspector) oi);
    case BINARY:
      return new LazyBinary((LazyBinaryObjectInspector) oi);
    default:
      throw new RuntimeException("Internal error: no LazyObject for " + p);
    }
  }

  /**
   * Create a hierarchical LazyObject based on the given typeInfo.
   */
  public static LazyObject createLazyObject(ObjectInspector oi) {
    ObjectInspector.Category c = oi.getCategory();
    switch (c) {
    case PRIMITIVE:
      return createLazyPrimitiveClass((PrimitiveObjectInspector) oi);
    case MAP:
      return new LazyMap((LazyMapObjectInspector) oi);
    case LIST:
      return new LazyArray((LazyListObjectInspector) oi);
    case STRUCT:
      return new LazyStruct((LazySimpleStructObjectInspector) oi);
    case UNION:
      return new LazyUnion((LazyUnionObjectInspector) oi);
    }

    throw new RuntimeException("Hive LazySerDe Internal error.");
  }

  /**
   * Create a hierarchical ObjectInspector for LazyObject with the given
   * typeInfo.
   * 
   * @param typeInfo
   *          The type information for the LazyObject
   * @param separator
   *          The array of separators for delimiting each level
   * @param separatorIndex
   *          The current level (for separators). List(array), struct uses 1
   *          level of separator, and map uses 2 levels: the first one for
   *          delimiting entries, the second one for delimiting key and values.
   * @param nullSequence
   *          The sequence of bytes representing NULL.
   * @return The ObjectInspector
   */
  public static ObjectInspector createLazyObjectInspector(TypeInfo typeInfo,
      byte[] separator, int separatorIndex, Text nullSequence, boolean escaped,
      byte escapeChar) {
    ObjectInspector.Category c = typeInfo.getCategory();
    switch (c) {
    case PRIMITIVE:
      return LazyPrimitiveObjectInspectorFactory.getLazyObjectInspector(
          ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory(), escaped,
          escapeChar);
    case MAP:
      return LazyObjectInspectorFactory.getLazySimpleMapObjectInspector(
          createLazyObjectInspector(((MapTypeInfo) typeInfo)
          .getMapKeyTypeInfo(), separator, separatorIndex + 2,
          nullSequence, escaped, escapeChar), createLazyObjectInspector(
          ((MapTypeInfo) typeInfo).getMapValueTypeInfo(), separator,
          separatorIndex + 2, nullSequence, escaped, escapeChar),
          separator[separatorIndex], separator[separatorIndex + 1],
          nullSequence, escaped, escapeChar);
    case LIST:
      return LazyObjectInspectorFactory.getLazySimpleListObjectInspector(
          createLazyObjectInspector(((ListTypeInfo) typeInfo)
          .getListElementTypeInfo(), separator, separatorIndex + 1,
          nullSequence, escaped, escapeChar), separator[separatorIndex],
          nullSequence, escaped, escapeChar);
    case STRUCT:
      StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
      List<String> fieldNames = structTypeInfo.getAllStructFieldNames();
      List<TypeInfo> fieldTypeInfos = structTypeInfo
          .getAllStructFieldTypeInfos();
      List<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>(
          fieldTypeInfos.size());
      for (int i = 0; i < fieldTypeInfos.size(); i++) {
        fieldObjectInspectors.add(createLazyObjectInspector(fieldTypeInfos
            .get(i), separator, separatorIndex + 1, nullSequence, escaped,
            escapeChar));
      }
      return LazyObjectInspectorFactory.getLazySimpleStructObjectInspector(
          fieldNames, fieldObjectInspectors, separator[separatorIndex],
          nullSequence, false, escaped, escapeChar);
    case UNION:
      UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
      List<ObjectInspector> lazyOIs = new ArrayList<ObjectInspector>();
      for (TypeInfo uti : unionTypeInfo.getAllUnionObjectTypeInfos()) {
        lazyOIs.add(createLazyObjectInspector(uti, separator,
            separatorIndex + 1, nullSequence, escaped,
            escapeChar));
      }
      return LazyObjectInspectorFactory.getLazyUnionObjectInspector(lazyOIs,
          separator[separatorIndex], nullSequence, escaped, escapeChar);
    }

    throw new RuntimeException("Hive LazySerDe Internal error.");
  }

  /**
   * Create a hierarchical ObjectInspector for LazyStruct with the given
   * columnNames and columnTypeInfos.
   * 
   * @param lastColumnTakesRest
   *          whether the last column of the struct should take the rest of the
   *          row if there are extra fields.
   * @see LazyFactory#createLazyObjectInspector(TypeInfo, byte[], int, Text,
   *      boolean, byte)
   */
  public static ObjectInspector createLazyStructInspector(
      List<String> columnNames, List<TypeInfo> typeInfos, byte[] separators,
      Text nullSequence, boolean lastColumnTakesRest, boolean escaped,
      byte escapeChar) {
    ArrayList<ObjectInspector> columnObjectInspectors = new ArrayList<ObjectInspector>(
        typeInfos.size());
    for (int i = 0; i < typeInfos.size(); i++) {
      columnObjectInspectors.add(LazyFactory.createLazyObjectInspector(
          typeInfos.get(i), separators, 1, nullSequence, escaped, escapeChar));
    }
    return LazyObjectInspectorFactory.getLazySimpleStructObjectInspector(
        columnNames, columnObjectInspectors, separators[0], nullSequence,
        lastColumnTakesRest, escaped, escapeChar);
  }

  /**
   * Create a hierarchical ObjectInspector for ColumnarStruct with the given
   * columnNames and columnTypeInfos.
   * 
   * @see LazyFactory#createLazyObjectInspector(TypeInfo, byte[], int, Text,
   *      boolean, byte)
   */
  public static ObjectInspector createColumnarStructInspector(
      List<String> columnNames, List<TypeInfo> columnTypes, byte[] separators,
      Text nullSequence, boolean escaped, byte escapeChar) {
    ArrayList<ObjectInspector> columnObjectInspectors = new ArrayList<ObjectInspector>(
        columnTypes.size());
    for (int i = 0; i < columnTypes.size(); i++) {
      columnObjectInspectors
          .add(LazyFactory.createLazyObjectInspector(columnTypes.get(i),
          separators, 1, nullSequence, escaped, escapeChar));
    }
    return ObjectInspectorFactory.getColumnarStructObjectInspector(columnNames,
        columnObjectInspectors);
  }

  private LazyFactory() {
    // prevent instantiation
  }
}
