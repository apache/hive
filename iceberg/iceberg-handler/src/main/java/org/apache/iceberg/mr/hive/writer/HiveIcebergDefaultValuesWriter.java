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
import java.util.List;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.io.DataWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitioningWriter;
import org.apache.iceberg.mr.hive.writer.WriterBuilder.Context;
import org.apache.iceberg.types.Types.NestedField;

/**
 * Base writer that applies Iceberg/Hive schema defaulting for missing columns and struct fields.
 *
 * <p>For Parquet writes with variant shredding enabled, the underlying data writer may be wrapped to buffer a small
 * sample of records to initialize VARIANT {@code typed_value} schema inference before the first Parquet file is
 * created.
 */
abstract class HiveIcebergDefaultValuesWriter extends HiveIcebergWriterBase {

  private final int currentSpecId;
  private final Set<String> missingColumns;
  private final List<NestedField> missingOrStructFields;

  HiveIcebergDefaultValuesWriter(
      Table table,
      HiveFileWriterFactory fileWriterFactory,
      OutputFileFactory dataFileFactory,
      Context context) {
    super(table, newDataWriterWithVariantSchemaInference(table, fileWriterFactory, dataFileFactory, context));

    Schema schema = table.schema();
    this.currentSpecId = table.spec().specId();
    this.missingColumns =
        context != null && context.missingColumns() != null ? context.missingColumns() : Set.of();

    this.missingOrStructFields =
        schema.columns().stream()
            .filter(field -> missingColumns.contains(field.name()) || field.type().isStructType())
            .toList();
  }

  private static PartitioningWriter<Record, DataWriteResult> newDataWriterWithVariantSchemaInference(
      Table table,
      HiveFileWriterFactory fileWriterFactory,
      OutputFileFactory dataFileFactory,
      Context context) {
    PartitioningWriter<Record, DataWriteResult> delegate =
        newDataWriter(table, fileWriterFactory, dataFileFactory, context);
    return ParquetVariantRecordWriter
        .tryWrap(table, fileWriterFactory, context, delegate)
        .orElse(delegate);
  }

  protected final void applyDefaultValues(Record record) {
    HiveSchemaUtil.setDefaultValues(record, missingOrStructFields, missingColumns);
  }

  protected final void write(Record record) {
    applyDefaultValues(record);
    writer.write(record, specs.get(currentSpecId), partition(record, currentSpecId));
  }

  @Override
  public void close(boolean abort) throws IOException {
    if (abort && writer instanceof ParquetVariantRecordWriter variantWriter) {
      variantWriter.abort();
    }
    super.close(abort);
  }
}
