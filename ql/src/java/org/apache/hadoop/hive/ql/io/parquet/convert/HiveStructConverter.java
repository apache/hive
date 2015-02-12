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
package org.apache.hadoop.hive.ql.io.parquet.convert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import parquet.io.api.Converter;
import parquet.schema.GroupType;
import parquet.schema.Type;

/**
 *
 * A MapWritableGroupConverter, real converter between hive and parquet types recursively for complex types.
 *
 */
public class HiveStructConverter extends HiveGroupConverter {

  private final int totalFieldCount;
  private final Converter[] converters;
  private final ConverterParent parent;
  private final int index;
  private Writable[] writables;
  private final List<Repeated> repeatedConverters;
  private boolean reuseWritableArray = false;

  public HiveStructConverter(final GroupType requestedSchema, final GroupType tableSchema, Map<String, String> metadata) {
    this(requestedSchema, null, 0, tableSchema);
    setMetadata(metadata);
    this.reuseWritableArray = true;
    this.writables = new Writable[tableSchema.getFieldCount()];
  }

  public HiveStructConverter(final GroupType groupType, final ConverterParent parent,
                             final int index) {
    this(groupType, parent, index, groupType);
  }

  public HiveStructConverter(final GroupType selectedGroupType,
                             final ConverterParent parent, final int index, final GroupType containingGroupType) {
    if (parent != null) {
      setMetadata(parent.getMetadata());
    }
    this.parent = parent;
    this.index = index;
    this.totalFieldCount = containingGroupType.getFieldCount();
    final int selectedFieldCount = selectedGroupType.getFieldCount();

    converters = new Converter[selectedFieldCount];
    this.repeatedConverters = new ArrayList<Repeated>();

    List<Type> selectedFields = selectedGroupType.getFields();
    for (int i = 0; i < selectedFieldCount; i++) {
      Type subtype = selectedFields.get(i);
      if (containingGroupType.getFields().contains(subtype)) {
        int fieldIndex = containingGroupType.getFieldIndex(subtype.getName());
        converters[i] = getFieldConverter(subtype, fieldIndex);
      } else {
        throw new IllegalStateException("Group type [" + containingGroupType +
            "] does not contain requested field: " + subtype);
      }
    }
  }

  private Converter getFieldConverter(Type type, int fieldIndex) {
    Converter converter;
    if (type.isRepetition(Type.Repetition.REPEATED)) {
      if (type.isPrimitive()) {
        converter = new Repeated.RepeatedPrimitiveConverter(
            type.asPrimitiveType(), this, fieldIndex);
      } else {
        converter = new Repeated.RepeatedGroupConverter(
            type.asGroupType(), this, fieldIndex);
      }

      repeatedConverters.add((Repeated) converter);
    } else {
      converter = getConverterFromDescription(type, fieldIndex, this);
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
