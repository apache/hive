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

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

import parquet.io.ParquetDecodingException;
import parquet.io.api.Converter;
import parquet.schema.GroupType;

public class ArrayWritableGroupConverter extends HiveGroupConverter {

  private final Converter[] converters;
  private final HiveGroupConverter parent;
  private final int index;
  private final boolean isMap;
  private Writable currentValue;
  private Writable[] mapPairContainer;

  public ArrayWritableGroupConverter(final GroupType groupType, final HiveGroupConverter parent,
      final int index) {
    this.parent = parent;
    this.index = index;
    int count = groupType.getFieldCount();
    if (count < 1 || count > 2) {
      throw new IllegalStateException("Field count must be either 1 or 2: " + count);
    }
    isMap = count == 2;
    converters = new Converter[count];
    for (int i = 0; i < count; i++) {
      converters[i] = getConverterFromDescription(groupType.getType(i), i, this);
    }
  }

  @Override
  public Converter getConverter(final int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {
    if (isMap) {
      mapPairContainer = new Writable[2];
    }
  }

  @Override
  public void end() {
    if (isMap) {
      currentValue = new ArrayWritable(Writable.class, mapPairContainer);
    }
    parent.add(index, currentValue);
  }

  @Override
  protected void set(final int index, final Writable value) {
    if (index != 0 && mapPairContainer == null || index > 1) {
      throw new ParquetDecodingException("Repeated group can only have one or two fields for maps." +
        " Not allowed to set for the index : " + index);
    }

    if (isMap) {
      mapPairContainer[index] = value;
    } else {
      currentValue = value;
    }
  }

  @Override
  protected void add(final int index, final Writable value) {
    set(index, value);
  }
}
