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
public class DataWritableGroupConverter extends HiveGroupConverter {

  private final Converter[] converters;
  private final HiveGroupConverter parent;
  private final int index;
  private final Object[] currentArr;
  private Writable[] rootMap;

  public DataWritableGroupConverter(final GroupType requestedSchema, final GroupType tableSchema) {
    this(requestedSchema, null, 0, tableSchema);
    final int fieldCount = tableSchema.getFieldCount();
    this.rootMap = new Writable[fieldCount];
  }

  public DataWritableGroupConverter(final GroupType groupType, final HiveGroupConverter parent,
      final int index) {
    this(groupType, parent, index, groupType);
  }

  public DataWritableGroupConverter(final GroupType selectedGroupType,
      final HiveGroupConverter parent, final int index, final GroupType containingGroupType) {
    this.parent = parent;
    this.index = index;
    final int totalFieldCount = containingGroupType.getFieldCount();
    final int selectedFieldCount = selectedGroupType.getFieldCount();

    currentArr = new Object[totalFieldCount];
    converters = new Converter[selectedFieldCount];

    List<Type> selectedFields = selectedGroupType.getFields();
    for (int i = 0; i < selectedFieldCount; i++) {
      Type subtype = selectedFields.get(i);
      if (containingGroupType.getFields().contains(subtype)) {
        converters[i] = getConverterFromDescription(subtype,
            containingGroupType.getFieldIndex(subtype.getName()), this);
      } else {
        throw new IllegalStateException("Group type [" + containingGroupType +
            "] does not contain requested field: " + subtype);
      }
    }
  }

  public final ArrayWritable getCurrentArray() {
    final Writable[] writableArr;
    if (this.rootMap != null) { // We're at the root : we can safely re-use the same map to save perf
      writableArr = this.rootMap;
    } else {
      writableArr = new Writable[currentArr.length];
    }

    for (int i = 0; i < currentArr.length; i++) {
      final Object obj = currentArr[i];
      if (obj instanceof List) {
        final List<?> objList = (List<?>)obj;
        final ArrayWritable arr = new ArrayWritable(Writable.class,
            objList.toArray(new Writable[objList.size()]));
        writableArr[i] = arr;
      } else {
        writableArr[i] = (Writable) obj;
      }
    }
    return new ArrayWritable(Writable.class, writableArr);
  }

  @Override
  final protected void set(final int index, final Writable value) {
    currentArr[index] = value;
  }

  @Override
  public Converter getConverter(final int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {
    for (int i = 0; i < currentArr.length; i++) {
      currentArr[i] = null;
    }
  }

  @Override
  public void end() {
    if (parent != null) {
      parent.set(index, getCurrentArray());
    }
  }

  @Override
  protected void add(final int index, final Writable value) {
    if (currentArr[index] != null) {
      final Object obj = currentArr[index];
      if (obj instanceof List) {
        final List<Writable> list = (List<Writable>) obj;
        list.add(value);
      } else {
        throw new IllegalStateException("This should be a List: " + obj);
      }
    } else {
      // create a list here because we don't know the final length of the object
      // and it is more flexible than ArrayWritable.
      //
      // converted to ArrayWritable by getCurrentArray().
      final List<Writable> buffer = new ArrayList<Writable>();
      buffer.add(value);
      currentArr[index] = (Object) buffer;
    }

  }
}
