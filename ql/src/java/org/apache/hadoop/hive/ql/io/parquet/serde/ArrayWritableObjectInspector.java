/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.serde;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.ql.io.parquet.serde.primitive.ParquetPrimitiveInspectorFactory;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.ArrayWritable;

/**
 *
 * The ArrayWritableObjectInspector will inspect an ArrayWritable, considering it as a Hive struct.<br />
 * It can also inspect a List if Hive decides to inspect the result of an inspection.
 *
 */
public class ArrayWritableObjectInspector extends SettableStructObjectInspector {

  private final TypeInfo typeInfo;
  private final List<TypeInfo> fieldInfos;
  private final List<String> fieldNames;
  private final List<StructField> fields;
  private final HashMap<String, StructFieldImpl> fieldsByName;

  // Whether this OI is for the column-level schema (as opposed to nested column fields).
  private final boolean isRoot;

  public ArrayWritableObjectInspector(final StructTypeInfo rowTypeInfo) {
    this(true, rowTypeInfo, null);
  }

  public ArrayWritableObjectInspector(StructTypeInfo originalTypeInfo, StructTypeInfo prunedTypeInfo) {
    this(true, originalTypeInfo, prunedTypeInfo);
  }

  public ArrayWritableObjectInspector(boolean isRoot,
      StructTypeInfo originalTypeInfo, StructTypeInfo prunedTypeInfo) {
    this.isRoot = isRoot;
    typeInfo = originalTypeInfo;
    fieldNames = originalTypeInfo.getAllStructFieldNames();
    fieldInfos = originalTypeInfo.getAllStructFieldTypeInfos();
    fields = new ArrayList<>(fieldNames.size());
    fieldsByName = new HashMap<>();

    for (int i = 0; i < fieldNames.size(); ++i) {
      final String name = fieldNames.get(i);
      final TypeInfo fieldInfo = fieldInfos.get(i);

      StructFieldImpl field = null;
      if (prunedTypeInfo != null) {
        for (int idx = 0; idx < prunedTypeInfo.getAllStructFieldNames().size(); ++idx) {
          if (prunedTypeInfo.getAllStructFieldNames().get(idx).equalsIgnoreCase(name)) {
            TypeInfo prunedFieldInfo = prunedTypeInfo.getAllStructFieldTypeInfos().get(idx);
            field = new StructFieldImpl(name, getObjectInspector(fieldInfo, prunedFieldInfo), i, idx);
            break;
          }
        }
      }
      if (field == null) {
        field = new StructFieldImpl(name, getObjectInspector(fieldInfo, null), i, i);
      }

      fields.add(field);
      fieldsByName.put(name.toLowerCase(), field);
    }
  }

  private ObjectInspector getObjectInspector(
      TypeInfo typeInfo, TypeInfo prunedTypeInfo) {
    if (typeInfo.equals(TypeInfoFactory.doubleTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    } else if (typeInfo.equals(TypeInfoFactory.booleanTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    } else if (typeInfo.equals(TypeInfoFactory.floatTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
    } else if (typeInfo.equals(TypeInfoFactory.intTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    } else if (typeInfo.equals(TypeInfoFactory.longTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    } else if (typeInfo.equals(TypeInfoFactory.stringTypeInfo)) {
      return ParquetPrimitiveInspectorFactory.parquetStringInspector;
    }  else if (typeInfo instanceof DecimalTypeInfo) {
      return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector((DecimalTypeInfo) typeInfo);
    } else if (typeInfo.getCategory().equals(Category.STRUCT)) {
      return new ArrayWritableObjectInspector(false, (StructTypeInfo) typeInfo, (StructTypeInfo) prunedTypeInfo);
    } else if (typeInfo.getCategory().equals(Category.LIST)) {
      final TypeInfo subTypeInfo = ((ListTypeInfo) typeInfo).getListElementTypeInfo();
      return new ParquetHiveArrayInspector(getObjectInspector(subTypeInfo, null));
    } else if (typeInfo.getCategory().equals(Category.MAP)) {
      final TypeInfo keyTypeInfo = ((MapTypeInfo) typeInfo).getMapKeyTypeInfo();
      final TypeInfo valueTypeInfo = ((MapTypeInfo) typeInfo).getMapValueTypeInfo();
      if (keyTypeInfo.equals(TypeInfoFactory.stringTypeInfo) || keyTypeInfo.equals(TypeInfoFactory.byteTypeInfo)
              || keyTypeInfo.equals(TypeInfoFactory.shortTypeInfo)) {
        return new DeepParquetHiveMapInspector(getObjectInspector(keyTypeInfo, null),
            getObjectInspector(valueTypeInfo, null));
      } else {
        return new StandardParquetHiveMapInspector(getObjectInspector(keyTypeInfo, null),
            getObjectInspector(valueTypeInfo, null));
      }
    } else if (typeInfo.equals(TypeInfoFactory.byteTypeInfo)) {
      return ParquetPrimitiveInspectorFactory.parquetByteInspector;
    } else if (typeInfo.equals(TypeInfoFactory.shortTypeInfo)) {
      return ParquetPrimitiveInspectorFactory.parquetShortInspector;
    } else if (typeInfo.equals(TypeInfoFactory.timestampTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    } else if (typeInfo.equals(TypeInfoFactory.binaryTypeInfo)){
      return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    }else if (typeInfo.equals(TypeInfoFactory.dateTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    } else if (typeInfo.getTypeName().toLowerCase().startsWith(serdeConstants.CHAR_TYPE_NAME)) {
      return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector((CharTypeInfo) typeInfo);
    } else if (typeInfo.getTypeName().toLowerCase().startsWith(serdeConstants.VARCHAR_TYPE_NAME)) {
      return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector((VarcharTypeInfo) typeInfo);
    } else {
      throw new UnsupportedOperationException("Unknown field type: " + typeInfo);
    }

  }

  @Override
  public Category getCategory() {
    return Category.STRUCT;
  }

  @Override
  public String getTypeName() {
    return typeInfo.getTypeName();
  }

  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return fields;
  }

  @Override
  public Object getStructFieldData(final Object data, final StructField fieldRef) {
    if (data == null) {
      return null;
    }
    if (data instanceof ArrayWritable) {
      final ArrayWritable arr = (ArrayWritable) data;
      final StructFieldImpl structField = (StructFieldImpl) fieldRef;
      int index = isRoot ? structField.getIndex() : structField.adjustedIndex;
      if (index < arr.get().length) {
        return arr.get()[index];
      } else {
        return null;
      }
    }

    //since setStructFieldData and create return a list, getStructFieldData should be able to
    //handle list data. This is required when table serde is ParquetHiveSerDe and partition serde
    //is something else.
    if (data instanceof List) {
      return ((List) data).get(((StructFieldImpl) fieldRef).getIndex());
    }

    throw new UnsupportedOperationException("Cannot inspect " + data.getClass().getCanonicalName());
  }

  @Override
  public StructField getStructFieldRef(final String name) {
    return fieldsByName.get(name.toLowerCase());
  }

  @Override
  public List<Object> getStructFieldsDataAsList(final Object data) {
    if (data == null) {
      return null;
    }

    if (data instanceof ArrayWritable) {
      final ArrayWritable arr = (ArrayWritable) data;
      final Object[] arrWritable = arr.get();
      return new ArrayList<>(Arrays.asList(arrWritable));
    }

    //since setStructFieldData and create return a list, getStructFieldData should be able to
    //handle list data. This is required when table serde is ParquetHiveSerDe and partition serde
    //is something else.
    if (data instanceof List) {
      return ((List) data);
    }

    throw new UnsupportedOperationException("Cannot inspect " + data.getClass().getCanonicalName());
  }

  @Override
  public Object create() {
    final ArrayList<Object> list = new ArrayList<Object>(fields.size());
    for (int i = 0; i < fields.size(); ++i) {
      list.add(null);
    }
    return list;
  }

  @Override
  public Object setStructFieldData(Object struct, StructField field, Object fieldValue) {
    final ArrayList<Object> list = (ArrayList<Object>) struct;
    list.set(((StructFieldImpl) field).getIndex(), fieldValue);
    return list;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArrayWritableObjectInspector that = (ArrayWritableObjectInspector) o;

    if (isRoot != that.isRoot ||
      (typeInfo != null ? !typeInfo.equals(that.typeInfo) : that.typeInfo != null) ||
      (fieldInfos != null ? !fieldInfos.equals(that.fieldInfos) : that.fieldInfos != null) ||
      (fieldNames != null ? !fieldNames.equals(that.fieldNames) : that.fieldNames != null) ||
      (fields != null ? !fields.equals(that.fields) : that.fields != null)) {
      return false;
    }

    return fieldsByName != null ? fieldsByName.equals(that.fieldsByName) : that.fieldsByName == null;
  }

  @Override
  public int hashCode() {
    int result = typeInfo != null ? typeInfo.hashCode() : 0;
    result = 31 * result + (fieldInfos != null ? fieldInfos.hashCode() : 0);
    result = 31 * result + (fieldNames != null ? fieldNames.hashCode() : 0);
    result = 31 * result + (fields != null ? fields.hashCode() : 0);
    result = 31 * result + (fieldsByName != null ? fieldsByName.hashCode() : 0);
    result = 31 * result + (isRoot ? 1 : 0);
    return result;
  }

  private class StructFieldImpl implements StructField {
    private final String name;
    private final ObjectInspector inspector;
    private final int index;

    // This is the adjusted index after nested column pruning.
    // For instance, given the struct type: s:<struct<a:int, b:boolean>>
    // If only 's.b' is used, the pruned type is: s:<struct<b:boolean>>.
    // Here, the index of field 'b' is changed from 1 to 0.
    // When we look up the data from Parquet, index needs to be adjusted accordingly.
    // Note: currently this is only used in the read path.
    final int adjustedIndex;

    public StructFieldImpl(final String name, final ObjectInspector inspector,
        final int index, int adjustedIndex) {
      this.name = name;
      this.inspector = inspector;
      this.index = index;
      this.adjustedIndex = adjustedIndex;
    }

    @Override
    public String getFieldComment() {
      return "";
    }

    @Override
    public String getFieldName() {
      return name;
    }

    public int getIndex() {
      return index;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return inspector;
    }

    @Override
    public int getFieldID() {
      return index;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      StructFieldImpl that = (StructFieldImpl) o;

      if (index != that.index) {
        return false;
      }
      if (adjustedIndex != that.adjustedIndex) {
        return false;
      }
      if (name != null ? !name.equals(that.name) : that.name != null) {
        return false;
      }
      return inspector != null ? inspector.equals(that.inspector) : that.inspector == null;
    }

    @Override
    public int hashCode() {
      int result = name != null ? name.hashCode() : 0;
      result = 31 * result + (inspector != null ? inspector.hashCode() : 0);
      result = 31 * result + index;
      result = 31 * result + adjustedIndex;
      return result;
    }
  }
}
