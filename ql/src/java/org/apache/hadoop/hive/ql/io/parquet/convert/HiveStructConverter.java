/*
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
package org.apache.hadoop.hive.ql.io.parquet.convert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

/**
 *
 * A MapWritableGroupConverter, real converter between hive and parquet types recursively for complex types.
 *
 */
public class HiveStructConverter extends HiveGroupConverter {

  private final int totalFieldCount;
  private Converter[] converters;
  private final ConverterParent parent;
  private final int index;
  private Writable[] writables;
  private List<Repeated> repeatedConverters;
  private boolean reuseWritableArray = false;
  private List<String> hiveFieldNames;
  private List<TypeInfo> hiveFieldTypeInfos;

  public HiveStructConverter(final GroupType requestedSchema, final GroupType tableSchema,
                             Map<String, String> metadata, TypeInfo hiveTypeInfo) {
    setMetadata(metadata);
    this.reuseWritableArray = true;
    this.writables = new Writable[tableSchema.getFieldCount()];
    this.parent = null;
    this.index = 0;
    this.totalFieldCount = tableSchema.getFieldCount();
    init(requestedSchema, null, 0, tableSchema, hiveTypeInfo);
  }

  public HiveStructConverter(final GroupType groupType, final ConverterParent parent,
                             final int index, TypeInfo hiveTypeInfo) {
    this(groupType, parent, index, groupType, hiveTypeInfo);
  }

  public HiveStructConverter(final GroupType selectedGroupType,
                             final ConverterParent parent, final int index, final GroupType containingGroupType, TypeInfo hiveTypeInfo) {
    this.parent = parent;
    this.index = index;
    this.totalFieldCount = containingGroupType.getFieldCount();
    init(selectedGroupType, parent, index, containingGroupType, hiveTypeInfo);
  }

  private void init(final GroupType selectedGroupType,
                    final ConverterParent parent, final int index, final GroupType containingGroupType, TypeInfo hiveTypeInfo) {
    if (parent != null) {
      setMetadata(parent.getMetadata());
    }
    final int selectedFieldCount = selectedGroupType.getFieldCount();

    converters = new Converter[selectedFieldCount];
    this.repeatedConverters = new ArrayList<Repeated>();

    if (hiveTypeInfo != null && hiveTypeInfo.getCategory().equals(ObjectInspector.Category.STRUCT)) {
      this.hiveFieldNames = ((StructTypeInfo) hiveTypeInfo).getAllStructFieldNames();
      this.hiveFieldTypeInfos = ((StructTypeInfo) hiveTypeInfo).getAllStructFieldTypeInfos();
    }

    List<Type> selectedFields = selectedGroupType.getFields();
    for (int i = 0; i < selectedFieldCount; i++) {
      Type subtype = selectedFields.get(i);
      if (isSubType(containingGroupType, subtype)) {
        int fieldIndex = containingGroupType.getFieldIndex(subtype.getName());
        TypeInfo _hiveTypeInfo = getFieldTypeIgnoreCase(hiveTypeInfo, subtype.getName(), fieldIndex);
        converters[i] = getFieldConverter(subtype, fieldIndex, _hiveTypeInfo);
      } else {
        throw new IllegalStateException("Group type [" + containingGroupType +
            "] does not contain requested field: " + subtype);
      }
    }
  }

  // This method is used to check whether the subType is a sub type of the groupType.
  // For nested attribute, we need to check its existence by the root path in a recursive way.
  private boolean isSubType(
    final GroupType groupType,
    final Type subtype) {
    if (subtype.isPrimitive() || subtype.isRepetition(Type.Repetition.REPEATED)) {
      return groupType.getFields().contains(subtype);
    } else {
      for (Type g : groupType.getFields()) {
        if (!g.isPrimitive() && g.getName().equals(subtype.getName())) {
          // check all elements are contained in g
          boolean containsAll = false;
          for (Type subSubType : subtype.asGroupType().getFields()) {
            containsAll = isSubType(g.asGroupType(), subSubType);
            if (!containsAll) {
              break;
            }
          }
          if (containsAll) {
            return containsAll;
          }
        }
      }
      return false;
    }
  }

  private TypeInfo getFieldTypeIgnoreCase(TypeInfo hiveTypeInfo, String fieldName, int fieldIndex) {
    if (hiveTypeInfo == null) {
      return null;
    } else if (hiveTypeInfo.getCategory().equals(ObjectInspector.Category.STRUCT)) {
      return getStructFieldTypeInfo(fieldName, fieldIndex);
    } else if (hiveTypeInfo.getCategory().equals(ObjectInspector.Category.MAP)) {
      //This cover the case where hive table may have map<key, value> but the data file is
      // of type array<struct<value1, value2>>
      //Using index in place of type name.
      if (fieldIndex == 0) {
        return ((MapTypeInfo) hiveTypeInfo).getMapKeyTypeInfo();
      } else if (fieldIndex == 1) {
        return ((MapTypeInfo) hiveTypeInfo).getMapValueTypeInfo();
      } else {//Other fields are skipped for this case
        return null;
      }
    }
    throw new RuntimeException("Unknown hive type info " + hiveTypeInfo + " when searching for field " + fieldName);
  }

  private TypeInfo getStructFieldTypeInfo(String field, int fieldIndex) {
    String fieldLowerCase = field.toLowerCase();
    if (Boolean.parseBoolean(getMetadata().get(DataWritableReadSupport.PARQUET_COLUMN_INDEX_ACCESS))
        && fieldIndex < hiveFieldNames.size()) {
      return hiveFieldTypeInfos.get(fieldIndex);
    }
    for (int i = 0; i < hiveFieldNames.size(); i++) {
      if (fieldLowerCase.equalsIgnoreCase(hiveFieldNames.get(i))) {
        return hiveFieldTypeInfos.get(i);
      }
    }
    //This means hive type doesn't refer this field that comes from file schema.
    //i.e. the field is not required for hive table. It can occur due to schema
    //evolution where some field is deleted.
    return null;
  }

  private Converter getFieldConverter(Type type, int fieldIndex, TypeInfo hiveTypeInfo) {
    Converter converter;
    if (type.isRepetition(Type.Repetition.REPEATED)) {
      if (type.isPrimitive()) {
        converter = new Repeated.RepeatedPrimitiveConverter(
            type.asPrimitiveType(), this, fieldIndex, hiveTypeInfo);
      } else {
        converter = new Repeated.RepeatedGroupConverter(
            type.asGroupType(), this, fieldIndex, hiveTypeInfo == null ? null : ((ListTypeInfo) hiveTypeInfo)
            .getListElementTypeInfo());
      }

      repeatedConverters.add((Repeated) converter);
    } else {
      converter = getConverterFromDescription(type, fieldIndex, this, hiveTypeInfo);
    }

    return converter;
  }

  public final ArrayWritable getCurrentArray() {
    return new ArrayWritable(Writable.class, writables);
  }

  @Override
  public void set(int fieldIndex, Writable value) {
    writables[fieldIndex] = value;
  }

  @Override
  public Converter getConverter(final int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {
    if (reuseWritableArray) {
      // reset the array to null values
      for (int i = 0; i < writables.length; i += 1) {
        writables[i] = null;
      }
    } else {
      this.writables = new Writable[totalFieldCount];
    }
    for (Repeated repeated : repeatedConverters) {
      repeated.parentStart();
    }
  }

  @Override
  public void end() {
    for (Repeated repeated : repeatedConverters) {
      repeated.parentEnd();
    }
    if (parent != null) {
      parent.set(index, getCurrentArray());
    }
  }

}
