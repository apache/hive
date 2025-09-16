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

package org.apache.iceberg.mr.hive.writer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.io.Writable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.DataWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mr.hive.FilesForCommit;
import org.apache.iceberg.mr.hive.writer.WriterBuilder.Context;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;

class HiveIcebergRecordWriter extends HiveIcebergWriterBase {

  private final int currentSpecId;
  private final Set<String> missingColumns;

  HiveIcebergRecordWriter(Table table, HiveFileWriterFactory fileWriterFactory,
      OutputFileFactory dataFileFactory, Context context, String missingColumns) {
    super(table, newDataWriter(table, fileWriterFactory, dataFileFactory, context));

    this.currentSpecId = table.spec().specId();
    this.missingColumns = Optional.ofNullable(missingColumns)
        .map(columns -> Arrays.stream(columns.split(",")).collect(Collectors.toCollection(HashSet::new)))
        .orElse(Sets.newHashSet());
  }

  @Override
  public void write(Writable row) throws IOException {
    Record record = ((Container<Record>) row).get();
    setDefault(specs.get(currentSpecId).schema().asStruct().fields(), record, missingColumns);

    writer.write(record, specs.get(currentSpecId), partition(record, currentSpecId));
  }

  private static void setDefault(List<Types.NestedField> fields, Record record, Set<String> missingColumns) {
    for (Types.NestedField field : fields) {
      Object fieldValue = record.getField(field.name());

      if (fieldValue == null) {
        boolean isMissing = missingColumns.contains(field.name());

        if (isMissing) {
          if (field.type().isStructType()) {
            // Create struct and apply defaults to all nested fields
            Record nestedRecord = GenericRecord.create(field.type().asStructType());
            record.setField(field.name(), nestedRecord);
            // For nested fields, we consider ALL fields as "missing" to apply defaults
            setDefaultForNestedStruct(field.type().asStructType().fields(), nestedRecord);
          } else if (field.writeDefault() != null) {
            Object defaultValue = convertToWriteType(field.writeDefault(), field.type());
            record.setField(field.name(), defaultValue);
          }
        }
        // Explicit NULLs remain NULL
      } else if (field.type().isStructType() && fieldValue instanceof Record) {
        // For existing structs, apply defaults to any null nested fields
        setDefaultForNestedStruct(field.type().asStructType().fields(), (Record) fieldValue);
      }
    }
  }

  // Special method for nested structs that always applies defaults to null fields
  private static void setDefaultForNestedStruct(List<Types.NestedField> fields, Record record) {
    for (Types.NestedField field : fields) {
      Object fieldValue = record.getField(field.name());

      if (fieldValue == null && field.writeDefault() != null) {
        // Always apply default to null fields in nested structs
        Object defaultValue = convertToWriteType(field.writeDefault(), field.type());
        record.setField(field.name(), defaultValue);
      } else if (field.type().isStructType() && fieldValue instanceof Record) {
        // Recursively process nested structs
        setDefaultForNestedStruct(field.type().asStructType().fields(), (Record) fieldValue);
      }
    }
  }

  private static Object convertToWriteType(Object value, Type type) {
    if (value == null) {
      return null;
    }

    switch (type.typeId()) {
      case DATE:
        // Convert days since epoch (Integer) to LocalDate
        if (value instanceof Integer) {
          return DateTimeUtil.dateFromDays((Integer) value);
        }
        break;
      case TIMESTAMP:
        // Convert microseconds since epoch (Long) to LocalDateTime
        if (value instanceof Long) {
          return DateTimeUtil.timestampFromMicros((Long) value);
        }
        break;
      case TIMESTAMP_NANO:
        // Convert nanoseconds since epoch (Long) to LocalDateTime
        if (value instanceof Long) {
          return DateTimeUtil.timestampFromNanos((Long) value);
        }
        break;
      case TIME:
        // Convert microseconds since midnight (Long) to LocalTime
        if (value instanceof Long) {
          return DateTimeUtil.timeFromMicros((Long) value);
        }
        break;
      default:
        // For other types, no conversion needed
        return value;
    }

    return value; // fallback
  }

  @Override
  public FilesForCommit files() {
    List<DataFile> dataFiles = ((DataWriteResult) writer.result()).dataFiles();
    return FilesForCommit.onlyData(dataFiles);
  }
}
