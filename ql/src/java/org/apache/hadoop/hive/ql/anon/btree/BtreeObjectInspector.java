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

package org.apache.hadoop.hive.ql.anon.btree;

import org.apache.hadoop.hive.ql.anon.ex.BtreeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.ArrayWritable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BtreeObjectInspector extends SettableStructObjectInspector {

  private List<StructField> fields;

  BtreeObjectInspector(StructTypeInfo completeTypeInfo) {
    List<String> fieldNames = completeTypeInfo.getAllStructFieldNames();
    List<TypeInfo> fieldInfos = completeTypeInfo.getAllStructFieldTypeInfos();
    fields = new ArrayList<>(fieldNames.size());

    for (int i = 0; i < fieldNames.size(); ++i) {
      final String name = fieldNames.get(i).replace("v", "col");
      final TypeInfo fieldInfo = fieldInfos.get(i);

      BtreeStructField field = new BtreeStructField(name, getObjectInspector(fieldInfo), i);
      fields.add(field);
    }
  }

  ObjectInspector getObjectInspector(TypeInfo typeInfo) {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE: {
        switch (typeInfo.getTypeName()) {
          case "int": {
            return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
          }
          case "bigint": {
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
          }
          case "string": {
            return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
          }
          default: {
            throw new BtreeException("missing oi conversion");
          }
        }
      }
      case LIST: {
        return new BtreeListObjectInspector((ListTypeInfo) typeInfo);
      }
      case STRUCT: {
        return new BtreeObjectInspector((StructTypeInfo) typeInfo);
      }
      default: {
        throw new BtreeException("missing ti conversion");
      }
    }
  }

  @Override
  public String getTypeName() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("struct<");
    for (int i = 0; i < fields.size(); ++i) {
      StructField field = fields.get(i);
      if (i != 0) {
        buffer.append(",");
      }
      buffer.append(field.getFieldName());
      buffer.append(":");
      buffer.append(field.getFieldObjectInspector().getTypeName());
    }
    buffer.append(">");
    return buffer.toString();
  }

  @Override
  public Category getCategory() {
    return Category.STRUCT;
  }

  @Override
  public Object create() {
    final ArrayList<Object> list = new ArrayList<>(fields.size());
    for (int i = 0; i < fields.size(); ++i) {
      list.add(null);
    }
    return list;
  }

  @Override
  public Object setStructFieldData(Object struct, StructField field, Object fieldValue) {
    final List<Object> list = (ArrayList<Object>) struct;
    int index = field.getFieldID();
    list.set(index, fieldValue);
    return list;
  }

  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return fields;
  }

  @Override
  public StructField getStructFieldRef(String fieldName) {
    for (StructField field : fields) {
      if (field.getFieldName().equalsIgnoreCase(fieldName)) {
        return field;
      }
    }
    return null;
  }

  @Override
  public Object getStructFieldData(Object data, StructField fieldRef) {
    if (data == null) {
      return null;
    }
    if (!(data instanceof KeyValueStruct)) {
      throw new UnsupportedOperationException("Cannot inspect " + data.getClass().getCanonicalName());
    }
    final KeyValueStruct struct = (KeyValueStruct) data;
    final BtreeStructField structField = (BtreeStructField) fieldRef;
    int index = structField.getFieldID();
    return index == 0 ? struct.getKey() : struct.getValue();
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data == null) {
      return null;
    }

    if (data instanceof ArrayWritable) {
      final ArrayWritable arr = (ArrayWritable) data;
      final Object[] arrWritable = arr.get();
      return new ArrayList<>(Arrays.asList(arrWritable));
    }

    if (data instanceof List) {
      return ((List) data);
    }

    throw new UnsupportedOperationException("Cannot inspect " + data.getClass().getCanonicalName());
  }
}
