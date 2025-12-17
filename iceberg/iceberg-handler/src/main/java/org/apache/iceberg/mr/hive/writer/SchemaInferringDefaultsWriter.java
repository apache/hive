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
import java.util.BitSet;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mr.hive.writer.WriterBuilder.Context;
import org.apache.iceberg.parquet.VariantUtil;
import org.apache.iceberg.parquet.VariantUtil.VariantField;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.NestedField;

abstract class SchemaInferringDefaultsWriter extends HiveIcebergWriterBase {

  private static final int VARIANT_SAMPLE_BUFFER_SIZE = 100;

  private final HiveFileWriterFactory fileWriterFactory;

  private final int currentSpecId;
  private final Set<String> missingColumns;
  private final List<NestedField> missingOrStructFields;

  private final List<VariantField> variantFields;
  private final BitSet sampledVariantFields;

  private final List<Record> buffer;
  private final Record accumulatedSample;
  private boolean sampleInitialized = false;

  SchemaInferringDefaultsWriter(
      Table table,
      HiveFileWriterFactory fileWriterFactory,
      OutputFileFactory dataFileFactory,
      Context context) {

    super(table, newDataWriter(table, fileWriterFactory, dataFileFactory, context));
    Schema schema = table.schema();
    this.fileWriterFactory = fileWriterFactory;

    this.currentSpecId = table.spec().specId();
    this.missingColumns = context.missingColumns();
    this.missingOrStructFields = schema.columns().stream()
        .filter(field -> missingColumns.contains(field.name()) || field.type().isStructType())
        .toList();

    this.variantFields = VariantUtil.variantFieldsForShredding(table.properties(), schema);
    this.sampledVariantFields = new BitSet(variantFields.size());

    boolean shouldBuffer = !variantFields.isEmpty();
    this.buffer = shouldBuffer ? Lists.newArrayListWithCapacity(VARIANT_SAMPLE_BUFFER_SIZE) : null;
    this.accumulatedSample = shouldBuffer ? GenericRecord.create(schema) : null;
  }

  protected void writeOrBuffer(Record record) {
    HiveSchemaUtil.setDefaultValues(record, missingOrStructFields, missingColumns);

    if (buffer != null && !sampleInitialized) {
      accumulateSample(record);

      if (allVariantFieldsSampled() || buffer.size() >= VARIANT_SAMPLE_BUFFER_SIZE) {
        // Use accumulated sample for schema inference
        fileWriterFactory.initialize(accumulatedSample);
        sampleInitialized = true;

        flushBufferedRecords();
      } else {
        buffer.add(record.copy());
        return;
      }
    }
    writeRecord(record);
  }

  private void writeRecord(Record record) {
    writer.write(record, specs.get(currentSpecId), partition(record, currentSpecId));
  }

  private void flushBufferedRecords() {
    for (Record bufferedRecord : buffer) {
      writeRecord(bufferedRecord);
    }
    buffer.clear();
  }

  private boolean allVariantFieldsSampled() {
    return sampledVariantFields.nextClearBit(0) >= variantFields.size();
  }

  private void accumulateSample(Record record) {
    if (accumulatedSample == null || allVariantFieldsSampled()) {
      return;
    }
    for (int fieldIndex = sampledVariantFields.nextClearBit(0);
         fieldIndex < variantFields.size();
         fieldIndex = sampledVariantFields.nextClearBit(fieldIndex + 1)) {
      trySampleVariantField(fieldIndex, record);
    }
  }

  private void trySampleVariantField(int fieldIndex, Record record) {
    VariantField variantField = variantFields.get(fieldIndex);
    Object val = safeGet(variantField, record);
    if (!VariantUtil.isShreddable(val)) {
      return;
    }
    HiveSchemaUtil.setStructField(accumulatedSample, variantField.path(), val);
    sampledVariantFields.set(fieldIndex);
  }

  private static Object safeGet(VariantField variantField, Record record) {
    try {
      return variantField.accessor().get(record);
    } catch (RuntimeException e) {
      // Treat unexpected access failures as "no sample" and keep scanning.
      return null;
    }
  }

  @Override
  public void close(boolean abort) throws IOException {
    if (buffer != null) {
      if (abort) {
        // Don't write anything on abort. Just drop any buffered records.
        buffer.clear();
      } else if (!buffer.isEmpty()) {
        if (!sampleInitialized) {
          // Use whatever we have accumulated so far
          fileWriterFactory.initialize(accumulatedSample);
        }
        flushBufferedRecords();
      }
    }
    super.close(abort);
  }

}
