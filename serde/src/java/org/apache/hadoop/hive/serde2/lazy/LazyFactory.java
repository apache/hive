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

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyListObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyUnionObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyByteObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyDateObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyFloatObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyIntObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyLongObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyShortObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyStringObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyVoidObjectInspector;
import org.apache.hadoop.hive.serde2.lazydio.LazyDioBoolean;
import org.apache.hadoop.hive.serde2.lazydio.LazyDioByte;
import org.apache.hadoop.hive.serde2.lazydio.LazyDioDouble;
import org.apache.hadoop.hive.serde2.lazydio.LazyDioFloat;
import org.apache.hadoop.hive.serde2.lazydio.LazyDioInteger;
import org.apache.hadoop.hive.serde2.lazydio.LazyDioLong;
import org.apache.hadoop.hive.serde2.lazydio.LazyDioShort;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * LazyFactory.
 *
 */
public final class LazyFactory {

  /**
   * Create a lazy primitive object instance given a primitive object inspector based on it's
   * type. It takes a boolean switch to decide whether to return a binary or standard variant
   * of the lazy object.
   *
   * @param poi PrimitiveObjectInspector
   * @param typeBinary a switch to return either a LazyPrimtive class or it's binary
   *        companion
   * @return LazyPrimitive<? extends ObjectInspector, ? extends Writable>
   */
  public static LazyPrimitive<? extends ObjectInspector, ? extends Writable>
  createLazyPrimitiveClass(PrimitiveObjectInspector poi, boolean typeBinary) {
    if (typeBinary) {
      return createLazyPrimitiveBinaryClass(poi);
    } else {
      return createLazyPrimitiveClass(poi);
    }
  }

  /**
   * Create a lazy primitive class given the type name.
   */
  public static LazyPrimitive<? extends ObjectInspector, ? extends Writable>
  createLazyPrimitiveClass(PrimitiveObjectInspector oi) {

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
    case CHAR:
      return new LazyHiveChar((LazyHiveCharObjectInspector) oi);
    case VARCHAR:
      return new LazyHiveVarchar((LazyHiveVarcharObjectInspector) oi);
    case DATE:
      return new LazyDate((LazyDateObjectInspector) oi);
    case TIMESTAMP:
      return new LazyTimestamp((LazyTimestampObjectInspector) oi);
    case BINARY:
      return new LazyBinary((LazyBinaryObjectInspector) oi);
    case DECIMAL:
      return new LazyHiveDecimal((LazyHiveDecimalObjectInspector) oi);
    case VOID:
      return new LazyVoid((LazyVoidObjectInspector) oi);
    default:
      throw new RuntimeException("Internal error: no LazyObject for " + p);
    }
  }

  public static LazyPrimitive<? extends ObjectInspector, ? extends Writable>
  createLazyPrimitiveBinaryClass(PrimitiveObjectInspector poi) {

    PrimitiveCategory pc = poi.getPrimitiveCategory();

    switch (pc) {
    case BOOLEAN:
      return new LazyDioBoolean((LazyBooleanObjectInspector) poi);
    case BYTE:
      return new LazyDioByte((LazyByteObjectInspector) poi);
    case SHORT:
      return new LazyDioShort((LazyShortObjectInspector) poi);
    case INT:
      return new LazyDioInteger((LazyIntObjectInspector) poi);
    case LONG:
      return new LazyDioLong((LazyLongObjectInspector) poi);
    case FLOAT:
      return new LazyDioFloat((LazyFloatObjectInspector) poi);
    case DOUBLE:
      return new LazyDioDouble((LazyDoubleObjectInspector) poi);
    default:
      throw new RuntimeException("Hive Internal Error: no LazyObject for " + poi);
    }
  }

  /**
   * Create a hierarchical LazyObject based on the given typeInfo.
   */
  public static LazyObject<? extends ObjectInspector> createLazyObject(ObjectInspector oi) {
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
   * Creates a LazyObject based on the LazyObjectInspector. Will create binary variants for
   * primitive objects when the switch <code>typeBinary</code> is specified as true.
   *
   * @param oi ObjectInspector
   * @param typeBinary Boolean value used as switch to return variants of LazyPrimitive
   *                   objects which are initialized from a binary format for the data.
   * @return LazyObject<? extends ObjectInspector>
   */
  public static LazyObject<? extends ObjectInspector>
  createLazyObject(ObjectInspector oi, boolean typeBinary) {

    if (oi.getCategory() == Category.PRIMITIVE) {
      return createLazyPrimitiveClass((PrimitiveObjectInspector) oi, typeBinary);
    } else {
      return createLazyObject(oi);
    }
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
   * @throws SerDeException
   */
  public static ObjectInspector createLazyObjectInspector(TypeInfo typeInfo,
      byte[] separator, int separatorIndex, Text nullSequence, boolean escaped,
      byte escapeChar, ObjectInspectorOptions option) throws SerDeException {
    return createLazyObjectInspector(typeInfo, separator, separatorIndex, nullSequence,
        escaped, escapeChar, false, option);
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
   * @throws SerDeException
   */
  public static ObjectInspector createLazyObjectInspector(TypeInfo typeInfo,
      byte[] separator, int separatorIndex, Text nullSequence, boolean escaped,
      byte escapeChar) throws SerDeException {
    return createLazyObjectInspector(typeInfo, separator, separatorIndex, nullSequence,
        escaped, escapeChar, false, ObjectInspectorOptions.JAVA);
  }

  /**
   * Create a hierarchical ObjectInspector for LazyObject with the given typeInfo.
   *
   * @param typeInfo The type information for the LazyObject
   * @param separator The array of separators for delimiting each level
   * @param separatorIndex The current level (for separators). List(array), struct uses 1 level of
   *          separator, and map uses 2 levels: the first one for delimiting entries, the second one
   *          for delimiting key and values.
   * @param nullSequence The sequence of bytes representing NULL.
   * @param extendedBooleanLiteral whether extended boolean literal set is legal
   * @param option the {@link ObjectInspectorOption}
   * @return The ObjectInspector
   * @throws SerDeException
   */
  public static ObjectInspector createLazyObjectInspector(TypeInfo typeInfo,
      byte[] separator, int separatorIndex, Text nullSequence, boolean escaped,
      byte escapeChar, boolean extendedBooleanLiteral) throws SerDeException {
    return createLazyObjectInspector(typeInfo, separator, separatorIndex, nullSequence, escaped,
        escapeChar, extendedBooleanLiteral, ObjectInspectorOptions.JAVA);
  }
  
  /**
   * Create a hierarchical ObjectInspector for LazyObject with the given typeInfo.
   *
   * @param typeInfo The type information for the LazyObject
   * @param separator The array of separators for delimiting each level
   * @param separatorIndex The current level (for separators). List(array), struct uses 1 level of
   *          separator, and map uses 2 levels: the first one for delimiting entries, the second one
   *          for delimiting key and values.
   * @param nullSequence The sequence of bytes representing NULL.
   * @param extendedBooleanLiteral whether extended boolean literal set is legal
   * @param option the {@link ObjectInspectorOption}
   * @return The ObjectInspector
   * @throws SerDeException
   */
  public static ObjectInspector createLazyObjectInspector(TypeInfo typeInfo,
      byte[] separator, int separatorIndex, Text nullSequence, boolean escaped,
      byte escapeChar, boolean extendedBooleanLiteral, ObjectInspectorOptions option) throws SerDeException {
    ObjectInspector.Category c = typeInfo.getCategory();
    switch (c) {
    case PRIMITIVE:
      return LazyPrimitiveObjectInspectorFactory.getLazyObjectInspector(
          (PrimitiveTypeInfo) typeInfo, escaped, escapeChar, extendedBooleanLiteral);
    case MAP:
      return LazyObjectInspectorFactory.getLazySimpleMapObjectInspector(
          createLazyObjectInspector(((MapTypeInfo) typeInfo)
          .getMapKeyTypeInfo(), separator, separatorIndex + 2,
          nullSequence, escaped, escapeChar, extendedBooleanLiteral, option), createLazyObjectInspector(
          ((MapTypeInfo) typeInfo).getMapValueTypeInfo(), separator,
          separatorIndex + 2, nullSequence, escaped, escapeChar, extendedBooleanLiteral, option),
          LazyUtils.getSeparator(separator, separatorIndex),
          LazyUtils.getSeparator(separator, separatorIndex+1),
          nullSequence, escaped, escapeChar);
    case LIST:
      return LazyObjectInspectorFactory.getLazySimpleListObjectInspector(
          createLazyObjectInspector(((ListTypeInfo) typeInfo)
          .getListElementTypeInfo(), separator, separatorIndex + 1,
          nullSequence, escaped, escapeChar, extendedBooleanLiteral, option), LazyUtils.getSeparator(separator, separatorIndex),
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
            escapeChar, extendedBooleanLiteral, option));
      }
      return LazyObjectInspectorFactory.getLazySimpleStructObjectInspector(
          fieldNames, fieldObjectInspectors,
          LazyUtils.getSeparator(separator, separatorIndex),
 nullSequence,
          false, escaped, escapeChar, option);
    case UNION:
      UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
      List<ObjectInspector> lazyOIs = new ArrayList<ObjectInspector>();
      for (TypeInfo uti : unionTypeInfo.getAllUnionObjectTypeInfos()) {
        lazyOIs.add(createLazyObjectInspector(uti, separator,
            separatorIndex + 1, nullSequence, escaped,
            escapeChar, extendedBooleanLiteral, option));
      }
      return LazyObjectInspectorFactory.getLazyUnionObjectInspector(lazyOIs,
          LazyUtils.getSeparator(separator, separatorIndex),
          nullSequence, escaped, escapeChar);
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
   * @throws SerDeException
   * @see LazyFactory#createLazyObjectInspector(TypeInfo, byte[], int, Text,
   *      boolean, byte)
   */
  public static ObjectInspector createLazyStructInspector(
      List<String> columnNames, List<TypeInfo> typeInfos, byte[] separators,
      Text nullSequence, boolean lastColumnTakesRest, boolean escaped,
      byte escapeChar) throws SerDeException {
    return createLazyStructInspector(columnNames, typeInfos, separators,
        nullSequence, lastColumnTakesRest, escaped, escapeChar, false);
  }

  /**
   * Create a hierarchical ObjectInspector for LazyStruct with the given
   * columnNames and columnTypeInfos.
   *
   * @param lastColumnTakesRest
   *          whether the last column of the struct should take the rest of the
   *          row if there are extra fields.
   * @param extendedBooleanLiteral whether extended boolean literal set is legal
   * @throws SerDeException
   * @see LazyFactory#createLazyObjectInspector(TypeInfo, byte[], int, Text,
   *      boolean, byte)
   */
  public static ObjectInspector createLazyStructInspector(
      List<String> columnNames, List<TypeInfo> typeInfos, byte[] separators,
      Text nullSequence, boolean lastColumnTakesRest, boolean escaped,
      byte escapeChar, boolean extendedBooleanLiteral) throws SerDeException {
    ArrayList<ObjectInspector> columnObjectInspectors = new ArrayList<ObjectInspector>(
        typeInfos.size());
    for (int i = 0; i < typeInfos.size(); i++) {
      columnObjectInspectors.add(LazyFactory.createLazyObjectInspector(
          typeInfos.get(i), separators, 1, nullSequence, escaped, escapeChar,
          extendedBooleanLiteral));
    }
    return LazyObjectInspectorFactory.getLazySimpleStructObjectInspector(
        columnNames, columnObjectInspectors, separators[0], nullSequence,
        lastColumnTakesRest, escaped, escapeChar);
  }

  /**
   * Create a hierarchical ObjectInspector for ColumnarStruct with the given
   * columnNames and columnTypeInfos.
   * @throws SerDeException
   *
   * @see LazyFactory#createLazyObjectInspector(TypeInfo, byte[], int, Text,
   *      boolean, byte)
   */
  public static ObjectInspector createColumnarStructInspector(
      List<String> columnNames, List<TypeInfo> columnTypes, byte[] separators,
      Text nullSequence, boolean escaped, byte escapeChar) throws SerDeException {
    ArrayList<ObjectInspector> columnObjectInspectors = new ArrayList<ObjectInspector>(
        columnTypes.size());
    for (int i = 0; i < columnTypes.size(); i++) {
      columnObjectInspectors
          .add(LazyFactory.createLazyObjectInspector(columnTypes.get(i),
          separators, 1, nullSequence, escaped, escapeChar, false));
    }
    return ObjectInspectorFactory.getColumnarStructObjectInspector(columnNames,
        columnObjectInspectors);
  }

  private LazyFactory() {
    // prevent instantiation
  }
}
