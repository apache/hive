/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.mapreduce;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.DateTimeUtil;

public class IcebergInternalRecordWrapper implements Record, StructLike {

  private Function<Object, Object>[] transforms;
  private StructType readSchema;
  private StructType tableSchema;
  private Object[] values;
  private int size;
  private Map<String, Integer> fieldToPositionInReadSchema;
  private Map<String, Integer> fieldToPositionInTableSchema;

  public IcebergInternalRecordWrapper(StructType tableSchema, StructType readSchema) {
    this.readSchema = readSchema;
    this.tableSchema = tableSchema;
    this.size = readSchema.fields().size();
    this.values = new Object[size];
    this.fieldToPositionInReadSchema = buildFieldPositionMap(readSchema);
    this.fieldToPositionInTableSchema = buildFieldPositionMap(tableSchema);
    this.transforms = readSchema.fields().stream().map(field -> converter(field.type()))
        .toArray(length -> (Function<Object, Object>[]) Array.newInstance(Function.class, length));
  }

  public IcebergInternalRecordWrapper wrap(StructLike record) {
    int idx = 0;
    for (Types.NestedField field : readSchema.fields()) {
      int position = fieldToPositionInReadSchema.get(field.name());
      values[idx] = record.get(position, Object.class);
      idx++;
    }
    return this;
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    if (transforms[pos] != null && values[pos] != null) {
      return javaClass.cast(transforms[pos].apply(values[pos]));
    }
    return javaClass.cast(values[pos]);
  }

  @Override
  public Object getField(String name) {
    Integer pos = fieldToPositionInReadSchema.get(name);
    if (pos != null) {
      return get(pos, Object.class);
    }

    return null;
  }

  @Override
  public StructType struct() {
    return readSchema;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public Object get(int pos) {
    return get(pos, Object.class);
  }

  @Override
  public IcebergInternalRecordWrapper copy() {
    return new IcebergInternalRecordWrapper(this);
  }

  @Override
  public IcebergInternalRecordWrapper copy(Map<String, Object> overwriteValues) {
    throw new UnsupportedOperationException(
        "Copying an IcebergInternalRecordWrapper with overwrite values is not supported.");
  }

  @Override
  public <T> void set(int pos, T value) {
    throw new UnsupportedOperationException("Cannot update value in IcebergInternalRecordWrapper.");

  }

  @Override
  public void setField(String name, Object value) {
    throw new UnsupportedOperationException("Cannot update fields in IcebergInternalRecordWrapper.");

  }

  private IcebergInternalRecordWrapper(IcebergInternalRecordWrapper toCopy) {
    this.readSchema = toCopy.readSchema;
    this.size = toCopy.size;
    this.values = Arrays.copyOf(toCopy.values, toCopy.values.length);
    this.fieldToPositionInReadSchema = buildFieldPositionMap(readSchema);
    this.fieldToPositionInTableSchema = buildFieldPositionMap(toCopy.tableSchema);
    this.transforms = readSchema.fields().stream().map(field -> converter(field.type()))
        .toArray(length -> (Function<Object, Object>[]) Array.newInstance(Function.class, length));
  }

  private Map<String, Integer> buildFieldPositionMap(StructType schema) {
    Map<String, Integer> nameToPosition = Maps.newHashMap();
    List<Types.NestedField> fields = schema.fields();
    for (int i = 0; i < fields.size(); i += 1) {
      nameToPosition.put(fields.get(i).name(), i);
    }
    return nameToPosition;
  }

  private static Function<Object, Object> converter(Type type) {
    switch (type.typeId()) {
      case TIMESTAMP:
        return timestamp -> DateTimeUtil.timestampFromMicros((Long) timestamp);
      case DATE:
        return date -> DateTimeUtil.dateFromDays((Integer) date);
      case STRUCT:
        IcebergInternalRecordWrapper wrapper =
            new IcebergInternalRecordWrapper(type.asStructType(), type.asStructType());
        return struct -> wrapper.wrap((StructLike) struct);
      case LIST:
        if (Type.TypeID.STRUCT.equals(type.asListType().elementType().typeId())) {
          StructType listElementSchema = type.asListType().elementType().asStructType();
          Function<Type, IcebergInternalRecordWrapper> createWrapper =
              t -> new IcebergInternalRecordWrapper(listElementSchema, listElementSchema);
          return list -> {
            return ((List<?>) list).stream().map(item -> createWrapper.apply(type).wrap((StructLike) item))
                .collect(Collectors.toList());
          };
        }
        break;
      default:
    }
    return null;
  }
}
