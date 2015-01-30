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
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.schema.GroupType;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;

import java.util.Map;

public abstract class HiveGroupConverter extends GroupConverter implements ConverterParent {

  private Map<String, String> metadata;

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  protected static PrimitiveConverter getConverterFromDescription(PrimitiveType type, int index, ConverterParent parent) {
    if (type == null) {
      return null;
    }

    return ETypeConverter.getNewConverter(type, index, parent);
  }

  protected static HiveGroupConverter getConverterFromDescription(GroupType type, int index, ConverterParent parent) {
    if (type == null) {
      return null;
    }

    OriginalType annotation = type.getOriginalType();
    if (annotation == OriginalType.LIST) {
      return HiveCollectionConverter.forList(type, parent, index);
    } else if (annotation == OriginalType.MAP || annotation == OriginalType.MAP_KEY_VALUE) {
      return HiveCollectionConverter.forMap(type, parent, index);
    }

    return new HiveStructConverter(type, parent, index);
  }

  protected static Converter getConverterFromDescription(Type type, int index, ConverterParent parent) {
    if (type == null) {
      return null;
    }

    if (type.isPrimitive()) {
      return getConverterFromDescription(type.asPrimitiveType(), index, parent);
    }

    return getConverterFromDescription(type.asGroupType(), index, parent);
  }

  /**
   * The original list and map conversion didn't remove the synthetic layer and
   * the ObjectInspector had to remove it. This is a temporary fix that adds an
   * extra layer for the ObjectInspector to remove.
   */
  static ArrayWritable wrapList(ArrayWritable list) {
    return new ArrayWritable(Writable.class, new Writable[] {list});
  }

  public abstract void set(int index, Writable value);

}
