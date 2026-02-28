/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Optional;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.io.DataWriteResult;
import org.apache.iceberg.io.PartitioningWriter;
import org.apache.iceberg.mr.hive.writer.WriterBuilder.Context;
import org.apache.iceberg.parquet.VariantUtil;
import org.apache.iceberg.parquet.VariantUtil.VariantField;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.StructLikeUtil;

/**
 * Writer-side helper that buffers a small sample of records to initialize Parquet VARIANT shredding schema inference.
 *
 * <p>This is only applicable for Parquet writes when {@code variant.shredding.enabled=true}. When enabled, Iceberg
 * may use a data-driven schema for {@code typed_value}. To avoid picking the first record arbitrarily (or
 * initializing before any variant value is seen), this buffers up to a small number of records and initializes the
 * {@link HiveFileWriterFactory} with an accumulated sample record once enough variant fields have been observed.
 */
final class ParquetVariantRecordWriter implements PartitioningWriter<Record, DataWriteResult> {

  private static final int VARIANT_SAMPLE_BUFFER_SIZE = 100;

  private final PartitioningWriter<Record, DataWriteResult> delegate;
  private final HiveFileWriterFactory fileWriterFactory;

  private final List<VariantField> variantFields;
  private final BitSet sampledVariantFields;
  private boolean sampleInitialized = false;

  private final List<BufferedWrite> buffer;
  private final Record accumulatedSample;

  private record BufferedWrite(Record record, PartitionSpec spec, StructLike partition) {
  }

  static Optional<PartitioningWriter<Record, DataWriteResult>> tryWrap(
      Table table,
      HiveFileWriterFactory fileWriterFactory,
      Context context,
      PartitioningWriter<Record, DataWriteResult> delegate) {
    if (table == null || fileWriterFactory == null || context == null || delegate == null) {
      return Optional.empty();
    }

    if (context.dataFileFormat() != FileFormat.PARQUET) {
      return Optional.empty();
    }

    Schema schema = table.schema();
    List<VariantField> variantFields = VariantUtil.variantFieldsForShredding(table.properties(), schema);
    if (variantFields.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(new ParquetVariantRecordWriter(delegate, fileWriterFactory, schema, variantFields));
  }

  private ParquetVariantRecordWriter(
      PartitioningWriter<Record, DataWriteResult> delegate,
      HiveFileWriterFactory fileWriterFactory,
      Schema schema,
      List<VariantField> variantFields) {
    this.delegate = delegate;
    this.fileWriterFactory = fileWriterFactory;
    this.variantFields = variantFields;
    this.sampledVariantFields = new BitSet(variantFields.size());
    this.buffer = Lists.newArrayListWithCapacity(VARIANT_SAMPLE_BUFFER_SIZE);
    this.accumulatedSample = GenericRecord.create(schema);
  }

  void abort() {
    buffer.clear();
  }

  @Override
  public void write(Record record, PartitionSpec spec, StructLike partition) {
    if (record == null) {
      delegate.write(null, spec, partition);
      return;
    }

    if (!sampleInitialized) {
      accumulateSample(record);

      if (allVariantFieldsSampled() || buffer.size() >= VARIANT_SAMPLE_BUFFER_SIZE) {
        fileWriterFactory.initialize(accumulatedSample);
        sampleInitialized = true;
        flush();
        delegate.write(record, spec, partition);
      } else {
        buffer.add(new BufferedWrite(record.copy(), spec, StructLikeUtil.copy(partition)));
      }
      return;
    }

    delegate.write(record, spec, partition);
  }

  @Override
  public void close() throws IOException {
    try {
      if (!buffer.isEmpty()) {
        if (!sampleInitialized) {
          // Initialize using whatever we have accumulated so far.
          fileWriterFactory.initialize(accumulatedSample);
          sampleInitialized = true;
        }
        flush();
      }
    } finally {
      delegate.close();
    }
  }

  @Override
  public DataWriteResult result() {
    return delegate.result();
  }

  private void flush() {
    for (BufferedWrite buffered : buffer) {
      delegate.write(buffered.record(), buffered.spec(), buffered.partition());
    }
    buffer.clear();
  }

  private boolean allVariantFieldsSampled() {
    return sampledVariantFields.nextClearBit(0) >= variantFields.size();
  }

  private void accumulateSample(Record record) {
    if (allVariantFieldsSampled()) {
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

}
