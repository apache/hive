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

package org.apache.hadoop.hive.serde2.objectinspector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.serde2.ColumnSet;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * StructObjectInspector works on struct data that is stored as a Java List or
 * Java Array object. Basically, the fields are stored sequentially in the List
 * object.
 *
 * The names of the struct fields and the internal structure of the struct
 * fields are specified in the ctor of the StructObjectInspector.
 *
 */
public class MetadataListStructObjectInspector extends
    StandardStructObjectInspector {

  static ConcurrentHashMap<List<List<String>>, MetadataListStructObjectInspector>
      cached = new ConcurrentHashMap<>();

  // public static MetadataListStructObjectInspector getInstance(int fields) {
  // return getInstance(ObjectInspectorUtils.getIntegerArray(fields));
  // }
  public static MetadataListStructObjectInspector getInstance(
      List<String> columnNames) {
    List<List<String>> key = Collections.singletonList(columnNames);
    MetadataListStructObjectInspector result = cached.get(key);
    if (result == null) {
      result = new MetadataListStructObjectInspector(columnNames);
      MetadataListStructObjectInspector prev = cached.putIfAbsent(key, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  public static MetadataListStructObjectInspector getInstance(
      List<String> columnNames, List<String> columnComments) {
    List<List<String>> key = Arrays.asList(columnNames, columnComments);

    MetadataListStructObjectInspector result = cached.get(key);
    if (result == null) {
      result = new MetadataListStructObjectInspector(columnNames, columnComments);
      MetadataListStructObjectInspector prev = cached.putIfAbsent(key, result);
      if (prev != null) {
        result = prev;
      }
    }
    return result;
  }

  static List<ObjectInspector> getFieldObjectInspectors(int fields) {
    return Collections.nCopies(fields,
        PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING));
  }

  protected MetadataListStructObjectInspector() {
    super();
  }
  MetadataListStructObjectInspector(List<String> columnNames) {
    super(columnNames, getFieldObjectInspectors(columnNames.size()));
  }

  public MetadataListStructObjectInspector(List<String> columnNames,
                                           List<String> columnComments) {
    super(columnNames, getFieldObjectInspectors(columnNames.size()),
          columnComments);
  }

  // Get col object out
  @Override
  public Object getStructFieldData(Object data, StructField fieldRef) {
    if (data instanceof ColumnSet) {
      data = ((ColumnSet) data).col;
    }
    return super.getStructFieldData(data, fieldRef);
  }

  // Get col object out
  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data instanceof ColumnSet) {
      data = ((ColumnSet) data).col;
    }
    return super.getStructFieldsDataAsList(data);
  }

}
