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

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapKeyValueTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.Map;
import java.util.Optional;

public abstract class HiveGroupConverter extends GroupConverter implements ConverterParent {

  private Map<String, String> metadata;

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  protected static PrimitiveConverter getConverterFromDescription(PrimitiveType type, int index, ConverterParent
      parent, TypeInfo hiveTypeInfo) {
    if (type == null) {
      return null;
    }

    return ETypeConverter.getNewConverter(type, index, parent, hiveTypeInfo);
  }

  protected static HiveGroupConverter getConverterFromDescription(final GroupType type,
      final int index, final ConverterParent parent, final TypeInfo hiveTypeInfo) {
    if (type == null) {
      return null;
    }

    if (type.getLogicalTypeAnnotation() != null) {
      Optional<HiveGroupConverter> converter =
          type.getLogicalTypeAnnotation().accept(new LogicalTypeAnnotationVisitor<HiveGroupConverter>(){
            @Override
            public Optional<HiveGroupConverter> visit(ListLogicalTypeAnnotation logicalTypeAnnotation) {
              return Optional.of(HiveCollectionConverter.forList(type, parent, index, hiveTypeInfo));
            }

            @Override
            public Optional<HiveGroupConverter> visit(MapLogicalTypeAnnotation logicalTypeAnnotation) {
              return Optional.of(HiveCollectionConverter.forMap(type, parent, index, hiveTypeInfo));
            }

            @Override
            public Optional<HiveGroupConverter> visit(MapKeyValueTypeAnnotation logicalTypeAnnotation) {
              return Optional.of(HiveCollectionConverter.forMap(type, parent, index, hiveTypeInfo));
            }
          });

      if (converter.isPresent()) {
        return converter.get();
      }
    }

    return new HiveStructConverter(type, parent, index, hiveTypeInfo);
  }

  protected static Converter getConverterFromDescription(Type type, int index, ConverterParent parent, TypeInfo hiveTypeInfo) {
    if (type == null) {
      return null;
    }

    if (type.isPrimitive()) {
      return getConverterFromDescription(type.asPrimitiveType(), index, parent, hiveTypeInfo);
    }

    return getConverterFromDescription(type.asGroupType(), index, parent, hiveTypeInfo);
  }

  public abstract void set(int index, Writable value);

}
