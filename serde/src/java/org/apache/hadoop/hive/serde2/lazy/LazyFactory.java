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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;

public class LazyFactory {

  /**
   * Create a lazy primitive class given the java class. 
   */
  public static LazyPrimitive<?> createLazyPrimitiveClass(Class<?> c) {
    if (String.class.equals(c)) {
      return new LazyString();
    } else if (Integer.class.equals(c)) {
      return new LazyInteger();
    } else if (Double.class.equals(c)) {
      return new LazyDouble();
    } else if (Byte.class.equals(c)) {
      return new LazyByte();
    } else if (Short.class.equals(c)) {
      return new LazyShort();
    } else if (Long.class.equals(c)) {
      return new LazyLong();
    } else {
      return null;
    }
  }

  /**
   * Create a hierarchical LazyObject based on the given typeInfo.
   */
  public static LazyObject createLazyObject(TypeInfo typeInfo) {
    ObjectInspector.Category c = typeInfo.getCategory();
    switch(c) {
    case PRIMITIVE:
      return createLazyPrimitiveClass(typeInfo.getPrimitiveClass());
    case MAP:
      return new LazyMap(typeInfo);      
    case LIST: 
      return new LazyArray(typeInfo);      
    case STRUCT:
      return new LazyStruct(typeInfo);      
    }

    throw new RuntimeException("Hive LazySerDe Internal error.");
  }
  
  /**
   * Create a hierarchical ObjectInspector for LazyObject with the given
   * typeInfo.
   * @param typeInfo  The type information for the LazyObject
   * @param separator The array of separators for delimiting each level
   * @param separatorIndex  The current level (for separators). List(array), 
   *                        struct uses 1 level of separator, and map uses 2
   *                        levels: the first one for delimiting entries, the
   *                        second one for delimiting key and values. 
   * @param nullSequence    The sequence of bytes representing NULL.
   * @return  The ObjectInspector
   */
  public static ObjectInspector createLazyObjectInspector(TypeInfo typeInfo, byte[] separator, 
      int separatorIndex, Text nullSequence) {
    ObjectInspector.Category c = typeInfo.getCategory();
    switch(c) {
    case PRIMITIVE:
      return ObjectInspectorFactory.getStandardPrimitiveObjectInspector(typeInfo.getPrimitiveClass());
    case MAP:
      return ObjectInspectorFactory.getLazySimpleMapObjectInspector(
          createLazyObjectInspector(typeInfo.getMapKeyTypeInfo(), separator, separatorIndex+2, nullSequence), 
          createLazyObjectInspector(typeInfo.getMapValueTypeInfo(), separator, separatorIndex+2, nullSequence), 
          separator[separatorIndex], 
          separator[separatorIndex+1], 
          nullSequence);
    case LIST: 
      return ObjectInspectorFactory.getLazySimpleListObjectInspector(
          createLazyObjectInspector(typeInfo.getListElementTypeInfo(), separator, separatorIndex+1, nullSequence),
          separator[separatorIndex], 
          nullSequence);
    case STRUCT:
      List<String> fieldNames = typeInfo.getAllStructFieldNames();
      List<TypeInfo> fieldTypeInfos = typeInfo.getAllStructFieldTypeInfos();
      List<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>(fieldTypeInfos.size());
      for(int i=0; i<fieldTypeInfos.size(); i++) {
        fieldObjectInspectors.add(
            createLazyObjectInspector(fieldTypeInfos.get(i), separator, separatorIndex+1, nullSequence));
      }
      return ObjectInspectorFactory.getLazySimpleStructObjectInspector(
          fieldNames, 
          fieldObjectInspectors, 
          separator[separatorIndex], 
          nullSequence,
          false);  
    }

    throw new RuntimeException("Hive LazySerDe Internal error.");
  }

  /**
   * Create a hierarchical ObjectInspector for LazyStruct with the given
   * columnNames and columnTypeInfos.
   * 
   * @param lastColumnTakesRest whether the last column of the struct should take
   *                            the rest of the row if there are extra fields. 
   * @see LazyFactory#createLazyObjectInspector(TypeInfo, byte[], int, Text)
   */  
  public static ObjectInspector createLazyStructInspector(List<String> columnNames, 
      List<TypeInfo> typeInfos, byte[] separators, 
      Text nullSequence, boolean lastColumnTakesRest) {
    ArrayList<ObjectInspector> columnObjectInspectors =
        new ArrayList<ObjectInspector>(typeInfos.size());  
    for (int i=0; i<typeInfos.size(); i++) {
      columnObjectInspectors.add(
          LazyFactory.createLazyObjectInspector(typeInfos.get(i), separators, 1, nullSequence));
    }
    return 
        ObjectInspectorFactory.getLazySimpleStructObjectInspector(columnNames,
            columnObjectInspectors, separators[0], nullSequence, lastColumnTakesRest);
  }
  
}
