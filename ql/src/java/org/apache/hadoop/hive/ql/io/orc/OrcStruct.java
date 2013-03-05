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
package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

final class OrcStruct implements Writable {

  private final Object[] fields;

  OrcStruct(int children) {
    fields = new Object[children];
  }

  Object getFieldValue(int fieldIndex) {
    return fields[fieldIndex];
  }

  void setFieldValue(int fieldIndex, Object value) {
    fields[fieldIndex] = value;
  }

   @Override
  public void write(DataOutput dataOutput) throws IOException {
    throw new UnsupportedOperationException("write unsupported");
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    throw new UnsupportedOperationException("readFields unsupported");
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || other.getClass() != OrcStruct.class) {
      return false;
    } else {
      OrcStruct oth = (OrcStruct) other;
      if (fields.length != oth.fields.length) {
        return false;
      }
      for(int i=0; i < fields.length; ++i) {
        if (fields[i] == null) {
          if (oth.fields[i] != null) {
            return false;
          }
        } else {
          if (!fields[i].equals(oth.fields[i])) {
            return false;
          }
        }
      }
      return true;
    }
  }

  @Override
  public int hashCode() {
    int result = fields.length;
    for(Object field: fields) {
      if (field != null) {
        result ^= field.hashCode();
      }
    }
    return result;
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("{");
    for(int i=0; i < fields.length; ++i) {
      if (i != 0) {
        buffer.append(", ");
      }
      buffer.append(fields[i]);
    }
    buffer.append("}");
    return buffer.toString();
  }

  static class Field implements StructField {
    private final String name;
    private final ObjectInspector inspector;
    private final int offset;

    Field(String name, ObjectInspector inspector, int offset) {
      this.name = name;
      this.inspector = inspector;
      this.offset = offset;
    }

    @Override
    public String getFieldName() {
      return name;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return inspector;
    }

    @Override
    public String getFieldComment() {
      return null;
    }
  }

  static class OrcStructInspector extends StructObjectInspector {
    private final List<StructField> fields;

    OrcStructInspector(StructTypeInfo info) {
      ArrayList<String> fieldNames = info.getAllStructFieldNames();
      ArrayList<TypeInfo> fieldTypes = info.getAllStructFieldTypeInfos();
      fields = new ArrayList<StructField>(fieldNames.size());
      for(int i=0; i < fieldNames.size(); ++i) {
        fields.add(new Field(fieldNames.get(i),
          createObjectInspector(fieldTypes.get(i)), i));
      }
    }

    OrcStructInspector(int columnId, List<OrcProto.Type> types) {
      OrcProto.Type type = types.get(columnId);
      int fieldCount = type.getSubtypesCount();
      fields = new ArrayList<StructField>(fieldCount);
      for(int i=0; i < fieldCount; ++i) {
        int fieldType = type.getSubtypes(i);
        fields.add(new Field(type.getFieldNames(i),
          createObjectInspector(fieldType, types), i));
      }
    }

    @Override
    public List<StructField> getAllStructFieldRefs() {
      return fields;
    }

    @Override
    public StructField getStructFieldRef(String s) {
      for(StructField field: fields) {
        if (field.getFieldName().equals(s)) {
          return field;
        }
      }
      return null;
    }

    @Override
    public Object getStructFieldData(Object object, StructField field) {
      return ((OrcStruct) object).fields[((Field) field).offset];
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object object) {
      OrcStruct struct = (OrcStruct) object;
      List<Object> result = new ArrayList<Object>(struct.fields.length);
      for (Object child: struct.fields) {
        result.add(child);
      }
      return result;
    }

    @Override
    public String getTypeName() {
      StringBuilder buffer = new StringBuilder();
      buffer.append("struct<");
      for(int i=0; i < fields.size(); ++i) {
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
  }

  static class OrcMapObjectInspector implements MapObjectInspector {
    private final ObjectInspector key;
    private final ObjectInspector value;

    OrcMapObjectInspector(MapTypeInfo info) {
      key = createObjectInspector(info.getMapKeyTypeInfo());
      value = createObjectInspector(info.getMapValueTypeInfo());
    }

    OrcMapObjectInspector(int columnId, List<OrcProto.Type> types) {
      OrcProto.Type type = types.get(columnId);
      key = createObjectInspector(type.getSubtypes(0), types);
      value = createObjectInspector(type.getSubtypes(1), types);
    }

    @Override
    public ObjectInspector getMapKeyObjectInspector() {
      return key;
    }

    @Override
    public ObjectInspector getMapValueObjectInspector() {
      return value;
    }

    @Override
    public Object getMapValueElement(Object map, Object key) {
      return ((Map) map).get(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<Object, Object> getMap(Object map) {
      return (Map) map;
    }

    @Override
    public int getMapSize(Object map) {
      return ((Map) map).size();
    }

    @Override
    public String getTypeName() {
      return "map<" + key.getTypeName() + "," + value.getTypeName() + ">";
    }

    @Override
    public Category getCategory() {
      return Category.MAP;
    }
  }

  static class OrcListObjectInspector implements ListObjectInspector {
    private final ObjectInspector child;

    OrcListObjectInspector(ListTypeInfo info) {
      child = createObjectInspector(info.getListElementTypeInfo());
    }

    OrcListObjectInspector(int columnId, List<OrcProto.Type> types) {
      OrcProto.Type type = types.get(columnId);
      child = createObjectInspector(type.getSubtypes(0), types);
    }

    @Override
    public ObjectInspector getListElementObjectInspector() {
      return child;
    }

    @Override
    public Object getListElement(Object list, int i) {
      return ((List) list).get(i);
    }

    @Override
    public int getListLength(Object list) {
      return ((List) list).size();
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<?> getList(Object list) {
      return (List) list;
    }

    @Override
    public String getTypeName() {
      return "array<" + child.getTypeName() + ">";
    }

    @Override
    public Category getCategory() {
      return Category.LIST;
    }
  }

  static ObjectInspector createObjectInspector(TypeInfo info) {
    switch (info.getCategory()) {
      case PRIMITIVE:
        switch (((PrimitiveTypeInfo) info).getPrimitiveCategory()) {
          case FLOAT:
            return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
          case DOUBLE:
            return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
          case BOOLEAN:
            return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
          case BYTE:
            return PrimitiveObjectInspectorFactory.writableByteObjectInspector;
          case SHORT:
            return PrimitiveObjectInspectorFactory.writableShortObjectInspector;
          case INT:
            return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
          case LONG:
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
          case BINARY:
            return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
          case STRING:
            return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
          case TIMESTAMP:
            return PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
          default:
            throw new IllegalArgumentException("Unknown primitive type " +
              ((PrimitiveTypeInfo) info).getPrimitiveCategory());
        }
      case STRUCT:
        return new OrcStructInspector((StructTypeInfo) info);
      case UNION:
        return new OrcUnion.OrcUnionObjectInspector((UnionTypeInfo) info);
      case MAP:
        return new OrcMapObjectInspector((MapTypeInfo) info);
      case LIST:
        return new OrcListObjectInspector((ListTypeInfo) info);
      default:
        throw new IllegalArgumentException("Unknown type " +
          info.getCategory());
    }
  }

  static ObjectInspector createObjectInspector(int columnId,
                                               List<OrcProto.Type> types){
    OrcProto.Type type = types.get(columnId);
    switch (type.getKind()) {
      case FLOAT:
        return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
      case DOUBLE:
        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
      case BOOLEAN:
        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
      case BYTE:
        return PrimitiveObjectInspectorFactory.writableByteObjectInspector;
      case SHORT:
        return PrimitiveObjectInspectorFactory.writableShortObjectInspector;
      case INT:
        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
      case LONG:
        return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
      case BINARY:
        return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
      case STRING:
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
      case TIMESTAMP:
        return PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
      case STRUCT:
        return new OrcStructInspector(columnId, types);
      case UNION:
        return new OrcUnion.OrcUnionObjectInspector(columnId, types);
      case MAP:
        return new OrcMapObjectInspector(columnId, types);
      case LIST:
        return new OrcListObjectInspector(columnId, types);
      default:
        throw new UnsupportedOperationException("Unknown type " +
          type.getKind());
    }
  }
}
