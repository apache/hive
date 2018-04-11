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
package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.orc.OrcProto;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final public class OrcStruct implements Writable {

  private Object[] fields;

  OrcStruct(int children) {
    fields = new Object[children];
  }

  Object getFieldValue(int fieldIndex) {
    return fields[fieldIndex];
  }

  void setFieldValue(int fieldIndex, Object value) {
    fields[fieldIndex] = value;
  }

  public int getNumFields() {
    return fields.length;
  }

  /**
   * Change the number of fields in the struct. No effect if the number of
   * fields is the same. The old field values are copied to the new array.
   * @param numFields the new number of fields
   */
  public void setNumFields(int numFields) {
    if (fields.length != numFields) {
      Object[] oldFields = fields;
      fields = new Object[numFields];
      System.arraycopy(oldFields, 0, fields, 0,
          Math.min(oldFields.length, numFields));
    }
  }

  /**
   * Destructively make this object link to other's values.
   * @param other the value to point to
   */
  void linkFields(OrcStruct other) {
    fields = other.fields;
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
    public int getFieldID() {
      return offset;
    }

    @Override
    public String getFieldComment() {
      return null;
    }
  }

  static class OrcStructInspector extends SettableStructObjectInspector {
    private List<StructField> fields;

    protected OrcStructInspector() {
      super();
    }

    OrcStructInspector(List<StructField> fields) {
      this.fields = fields;
    }

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
        if (field.getFieldName().equalsIgnoreCase(s)) {
          return field;
        }
      }
      return null;
    }

    @Override
    public Object getStructFieldData(Object object, StructField field) {
      if (object == null) {
        return null;
      }
      int offset = ((Field) field).offset;
      OrcStruct struct = (OrcStruct) object;
      if (offset >= struct.fields.length) {
        return null;
      }

      return struct.fields[offset];
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object object) {
      if (object == null) {
        return null;
      }
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

    @Override
    public Object create() {
      return new OrcStruct(0);
    }

    @Override
    public Object setStructFieldData(Object struct, StructField field,
                                     Object fieldValue) {
      OrcStruct orcStruct = (OrcStruct) struct;
      int offset = ((Field) field).offset;
      // if the offset is bigger than our current number of fields, grow it
      if (orcStruct.getNumFields() <= offset) {
        orcStruct.setNumFields(offset+1);
      }
      orcStruct.setFieldValue(offset, fieldValue);
      return struct;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || o.getClass() != getClass()) {
        return false;
      } else if (o == this) {
        return true;
      } else {
        List<StructField> other = ((OrcStructInspector) o).fields;
        if (other.size() != fields.size()) {
          return false;
        }
        for(int i = 0; i < fields.size(); ++i) {
          StructField left = other.get(i);
          StructField right = fields.get(i);
          if (!(left.getFieldName().equalsIgnoreCase(right.getFieldName()) &&
                left.getFieldObjectInspector().equals
                    (right.getFieldObjectInspector()))) {
            return false;
          }
        }
        return true;
      }
    }
  }

  static class OrcMapObjectInspector
      implements MapObjectInspector, SettableMapObjectInspector {
    private ObjectInspector key;
    private ObjectInspector value;

    private OrcMapObjectInspector() {
      super();
    }
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
      return ((map == null || key == null)? null : ((Map) map).get(key));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<Object, Object> getMap(Object map) {
      if (map == null) {
        return null;
      }
      return (Map) map;
    }

    @Override
    public int getMapSize(Object map) {
      if (map == null) {
        return -1;
      }
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

    @Override
    public Object create() {
      return new HashMap<Object,Object>();
    }

    @Override
    public Object put(Object map, Object key, Object value) {
      ((Map) map).put(key, value);
      return map;
    }

    @Override
    public Object remove(Object map, Object key) {
      ((Map) map).remove(key);
      return map;
    }

    @Override
    public Object clear(Object map) {
      ((Map) map).clear();
      return map;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || o.getClass() != getClass()) {
        return false;
      } else if (o == this) {
        return true;
      } else {
        OrcMapObjectInspector other = (OrcMapObjectInspector) o;
        return other.key.equals(key) && other.value.equals(value);
      }
    }
  }

  static class OrcListObjectInspector
      implements ListObjectInspector, SettableListObjectInspector {
    private ObjectInspector child;

    private OrcListObjectInspector() {
      super();
    }
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
      if (list == null || i < 0 || i >= getListLength(list)) {
        return null;
      }
      return ((List) list).get(i);
    }

    @Override
    public int getListLength(Object list) {
      if (list == null) {
        return -1;
      }
      return ((List) list).size();
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<?> getList(Object list) {
      if (list == null) {
        return null;
      }
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

    @Override
    public Object create(int size) {
      ArrayList<Object> result = new ArrayList<Object>(size);
      for(int i = 0; i < size; ++i) {
        result.add(null);
      }
      return result;
    }

    @Override
    public Object set(Object list, int index, Object element) {
      List l = (List) list;
      for(int i=l.size(); i < index+1; ++i) {
        l.add(null);
      }
      l.set(index, element);
      return list;
    }

    @Override
    public Object resize(Object list, int newSize) {
      ((ArrayList) list).ensureCapacity(newSize);
      return list;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || o.getClass() != getClass()) {
        return false;
      } else if (o == this) {
        return true;
      } else {
        ObjectInspector other = ((OrcListObjectInspector) o).child;
        return other.equals(child);
      }
    }
  }

  static public ObjectInspector createObjectInspector(TypeInfo info) {
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
          case CHAR:
            return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                (PrimitiveTypeInfo) info);
          case VARCHAR:
            return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                (PrimitiveTypeInfo) info);
          case TIMESTAMP:
            return PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
          case DATE:
            return PrimitiveObjectInspectorFactory.writableDateObjectInspector;
          case DECIMAL:
            return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                (PrimitiveTypeInfo)info);
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
      case CHAR:
        if (!type.hasMaximumLength()) {
          throw new UnsupportedOperationException(
              "Illegal use of char type without length in ORC type definition.");
        }
        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            TypeInfoFactory.getCharTypeInfo(type.getMaximumLength()));
      case VARCHAR:
        if (!type.hasMaximumLength()) {
          throw new UnsupportedOperationException(
              "Illegal use of varchar type without length in ORC type definition.");
        }
        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            TypeInfoFactory.getVarcharTypeInfo(type.getMaximumLength()));
      case TIMESTAMP:
        return PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
      case DATE:
        return PrimitiveObjectInspectorFactory.writableDateObjectInspector;
      case DECIMAL:
        int precision = type.hasPrecision() ? type.getPrecision() : HiveDecimal.SYSTEM_DEFAULT_PRECISION;
        int scale =  type.hasScale()? type.getScale() : HiveDecimal.SYSTEM_DEFAULT_SCALE;
        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            TypeInfoFactory.getDecimalTypeInfo(precision, scale));
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
