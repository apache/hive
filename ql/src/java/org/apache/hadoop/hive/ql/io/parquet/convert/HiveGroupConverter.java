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

import org.apache.hadoop.io.Writable;

import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.schema.Type;
import parquet.schema.Type.Repetition;

public abstract class HiveGroupConverter extends GroupConverter {

  protected static Converter getConverterFromDescription(final Type type, final int index,
      final HiveGroupConverter parent) {
    if (type == null) {
      return null;
    }
    if (type.isPrimitive()) {
      return ETypeConverter.getNewConverter(type.asPrimitiveType().getPrimitiveTypeName().javaType,
          index, parent);
    } else {
      if (type.asGroupType().getRepetition() == Repetition.REPEATED) {
        return new ArrayWritableGroupConverter(type.asGroupType(), parent, index);
      } else {
        return new DataWritableGroupConverter(type.asGroupType(), parent, index);
      }
    }
  }

  protected abstract void set(int index, Writable value);

  protected abstract void add(int index, Writable value);

}
