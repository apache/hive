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

package org.apache.iceberg.mr.hive.vector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.parquet.ParquetRecordReaderBase;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.mr.hive.variant.VariantProjectionUtil.VariantColumnDescriptor;
import org.apache.iceberg.mr.hive.variant.VariantProjectionUtil.VariantProjection;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.ParquetVariantVisitor;
import org.apache.iceberg.parquet.VariantReaderBuilder;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

final class ParquetVariantRecordReader implements RecordReader<NullWritable, VectorizedRowBatch> {

  private final RecordReader<NullWritable, VectorizedRowBatch> delegate;
  private final ParquetFileReader parquetReader;

  private final List<VariantColumnDescriptor> variantColumns;
  private final ParquetValueReader<Variant>[] readers;
  private final StructColumnVector[] structCvb;

  private long remainingInRowGroup;

  @SuppressWarnings("unchecked")
  private ParquetVariantRecordReader(
      RecordReader<NullWritable, VectorizedRowBatch> delegate,
      JobConf job,
      Path path,
      ParquetMetadata parquetMetadata,
      List<BlockMetaData> blocks,
      VariantProjection projection) throws IOException {
    this.delegate = delegate;
    this.variantColumns = projection.variantColumns();
    this.structCvb = new StructColumnVector[variantColumns.size()];
    this.readers = new ParquetValueReader[variantColumns.size()];

    MessageType fileSchema = parquetMetadata.getFileMetaData().getSchema();
    for (int i = 0; i < variantColumns.size(); i++) {
      readers[i] = createReader(fileSchema, variantColumns.get(i));
    }

    this.parquetReader = new ParquetFileReader(
        job, parquetMetadata.getFileMetaData(), path, blocks, projection.requestedColumns());

    advanceRowGroup();
  }

  /**
   * Wrapper to read Iceberg shredded VARIANT data and populate Hive's VARIANT struct (metadata/value).
   * <p>
   * Ensures correct reconstruction from shredded fields which standard Hive readers might miss.
   */
  static Optional<RecordReader<NullWritable, VectorizedRowBatch>> tryWrap(
      RecordReader<NullWritable, VectorizedRowBatch> delegate,
      JobConf job,
      FileScanTask task,
      Path path,
      long start,
      long length,
      ParquetMetadata parquetMetadata) throws IOException {

    if (delegate == null || parquetMetadata == null || parquetMetadata.getFileMetaData() == null) {
      return Optional.empty();
    }

    MessageType fileSchema = parquetMetadata.getFileMetaData().getSchema();
    if (fileSchema == null) {
      return Optional.empty();
    }

    VariantProjection projection = VariantProjection.create(fileSchema, job, task.spec().schema());
    if (projection == null) {
      return Optional.empty();
    }

    List<BlockMetaData> blocks = blocksForSplit(delegate, start, length, parquetMetadata);
    if (blocks.isEmpty()) {
      // Delegate reader will handle empty split; don't wrap.
      return Optional.empty();
    }

    return Optional.of(
        new ParquetVariantRecordReader(
            delegate, job, path, parquetMetadata, blocks, projection));
  }

  private static List<BlockMetaData> blocksForSplit(
      RecordReader<NullWritable, VectorizedRowBatch> delegate,
      long start,
      long length,
      ParquetMetadata parquetMetadata) {

    // If the underlying Hive Parquet reader already computed row-group filtering (e.g. from SARG),
    // we must use the exact same blocks to keep this reader aligned with the delegate.
    if (delegate instanceof ParquetRecordReaderBase parquetDelegate) {
      List<BlockMetaData> filteredBlocks = parquetDelegate.getFilteredBlocks();
      // Treat an empty list as authoritative (delegate filtered out all row groups).
      if (filteredBlocks != null) {
        return filteredBlocks;
      }
    }
    // Fallback: compute blocks from split boundaries
    List<BlockMetaData> splitBlocks = Lists.newArrayList();
    for (BlockMetaData block : parquetMetadata.getBlocks()) {
      long firstDataPage = block.getColumns().getFirst().getFirstDataPageOffset();
      if (firstDataPage >= start && firstDataPage < start + length) {
        splitBlocks.add(block);
      }
    }
    return splitBlocks;
  }

  @Override
  public boolean next(NullWritable key, VectorizedRowBatch value) throws IOException {
    boolean hasNext = delegate.next(key, value);
    if (!hasNext || value == null || value.size <= 0) {
      return hasNext;
    }
    populateVariantStructColumns(value);
    return true;
  }

  private void populateVariantStructColumns(VectorizedRowBatch batch) throws IOException {
    for (int i = 0; i < variantColumns.size(); i++) {
      structCvb[i] = variantStructVector(batch, variantColumns.get(i));
    }

    int offset = 0;
    while (offset < batch.size) {
      ensureRowAvailable();
      if (remainingInRowGroup == 0) {
        break;
      }
      int length = (int) Math.min(batch.size - offset, remainingInRowGroup);
      processChunk(batch, offset, length);

      remainingInRowGroup -= length;
      offset += length;
    }
  }

  private void processChunk(VectorizedRowBatch batch, int offset, int length) {
    boolean selectedInUse = batch.selectedInUse;
    int[] selected = batch.selected;

    for (int i = 0; i < variantColumns.size(); i++) {
      StructColumnVector structVector = structCvb[i];
      ParquetValueReader<Variant> reader = readers[i];

      // variantStructVector guarantees fields != null and fields.length >= 2 when non-null
      // and that metadata/value vectors are present.
      BytesColumnVector metadataVector = (BytesColumnVector) structVector.fields[0];
      BytesColumnVector valueVector = (BytesColumnVector) structVector.fields[1];

      ByteBuffer metadataBuffer = null;
      byte[] lastMetadataArray = null;

      ByteBuffer valueBuffer = null;
      byte[] lastValueArray = null;

      for (int pos = 0; pos < length; pos++) {
        int batchIndex = offset + pos;
        int row = selectedInUse ? selected[batchIndex] : batchIndex;

        Variant variant = reader.read(null);

        if (variant != null) {
          structVector.isNull[row] = false;
          metadataVector.isNull[row] = false;
          valueVector.isNull[row] = false;

          metadataBuffer = writeMetadata(
              metadataVector, row, variant.metadata(), metadataBuffer, lastMetadataArray);
          lastMetadataArray = metadataVector.getValPreallocatedBytes();

          valueBuffer = writeValue(
              valueVector, row, variant.value(), valueBuffer, lastValueArray);
          lastValueArray = valueVector.getValPreallocatedBytes();
        } else {
          structVector.noNulls = false;
          structVector.isNull[row] = true;
          metadataVector.noNulls = false;
          metadataVector.isNull[row] = true;
          valueVector.noNulls = false;
          valueVector.isNull[row] = true;
        }
      }
    }
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private static StructColumnVector variantStructVector(
      VectorizedRowBatch batch, VariantColumnDescriptor vc) throws IOException {
    if (batch == null || vc == null || batch.cols == null) {
      throw new IOException("Invalid batch or descriptor: batch=" + batch + ", vc=" + vc);
    }

    int rootIndex = vc.rootColumnIndex();
    if (rootIndex < 0 || rootIndex >= batch.cols.length) {
      throw new IOException("Root column index " + rootIndex + " out of bounds (cols=" + batch.cols.length + ")");
    }

    ColumnVector current = batch.cols[rootIndex];

    for (int fieldIndex : vc.fieldPath()) {
      if (!(current instanceof StructColumnVector struct)) {
        throw new IOException("Expected nested StructColumnVector at field index " + fieldIndex +
            " for column " + Arrays.toString(vc.physicalPath()) + ", found " +
            (current == null ? "null" : current.getClass().getSimpleName()));
      }
      struct.isRepeating = false;

      if (struct.fields == null || fieldIndex < 0 || fieldIndex >= struct.fields.length) {
        throw new IOException("Invalid field path index " + fieldIndex + " for struct with " +
            (struct.fields == null ? "null" : struct.fields.length) + " fields");
      }
      current = struct.fields[fieldIndex];
    }

    if (!(current instanceof StructColumnVector variantStruct)) {
      throw new IOException("Expected Variant StructColumnVector at " + Arrays.toString(vc.physicalPath()) +
          ", found " + (current == null ? "null" : current.getClass().getSimpleName()));
    }

    if (variantStruct.fields == null || variantStruct.fields.length != 2) {
      throw new IOException("Invalid Variant struct fields at " + Arrays.toString(vc.physicalPath()) +
          ": expected exactly 2 fields (metadata, value), found " +
          (variantStruct.fields == null ? "null" : variantStruct.fields.length));
    }
    variantStruct.isRepeating = false;

    if (variantStruct.fields[0] == null) {
      throw new IOException("Invalid Variant vector structure for column " +
          Arrays.toString(vc.physicalPath()) + ": metadata vector is null.");
    }
    variantStruct.fields[0].isRepeating = false;

    if (variantStruct.fields[1] == null) {
      throw new IOException("Invalid Variant vector structure for column " +
          Arrays.toString(vc.physicalPath()) + ": value vector is null.");
    }
    variantStruct.fields[1].isRepeating = false;

    return variantStruct;
  }

  private void ensureRowAvailable() throws IOException {
    if (remainingInRowGroup <= 0) {
      advanceRowGroup();
    }
  }

  private void advanceRowGroup() throws IOException {
    PageReadStore nextRowGroup = parquetReader.readNextRowGroup();
    if (nextRowGroup == null) {
      remainingInRowGroup = 0;
      return;
    }
    remainingInRowGroup = nextRowGroup.getRowCount();
    for (ParquetValueReader<Variant> reader : readers) {
      reader.setPageSource(nextRowGroup);
    }
  }

  @FunctionalInterface
  private interface VariantWriteFunction<T> {
    void write(T data, ByteBuffer buffer, int offset);
  }

  private static ByteBuffer writeMetadata(
      BytesColumnVector vector, int row, VariantMetadata metadata,
      ByteBuffer reusableBuffer, byte[] lastBackingArray) {
    return writeToVector(vector, row, metadata, reusableBuffer, lastBackingArray,
        VariantMetadata::sizeInBytes, VariantMetadata::writeTo);
  }

  private static ByteBuffer writeValue(
      BytesColumnVector vector, int row, VariantValue value,
      ByteBuffer reusableBuffer, byte[] lastBackingArray) {
    return writeToVector(vector, row, value, reusableBuffer, lastBackingArray,
        VariantValue::sizeInBytes, VariantValue::writeTo);
  }

  private static <T> ByteBuffer writeToVector(
      BytesColumnVector vector, int row, T data,
      ByteBuffer reusableBuffer, byte[] lastBackingArray,
      ToIntFunction<T> sizeFunction, VariantWriteFunction<T> writeFunction) {
    if (vector == null || data == null) {
      return reusableBuffer;
    }

    int length = sizeFunction.applyAsInt(data);
    vector.ensureValPreallocated(length);

    // Only create new ByteBuffer if backing array changed
    byte[] currentBackingArray = vector.getValPreallocatedBytes();
    ByteBuffer resultBuffer = reusableBuffer;
    if (resultBuffer == null || currentBackingArray != lastBackingArray) {
      resultBuffer = ByteBuffer.wrap(currentBackingArray).order(ByteOrder.LITTLE_ENDIAN);
    }

    writeFunction.write(data, resultBuffer, vector.getValPreallocatedStart());
    vector.setValPreallocated(row, length);

    return resultBuffer;
  }

  @Override
  public NullWritable createKey() {
    return delegate.createKey();
  }

  @Override
  public VectorizedRowBatch createValue() {
    return delegate.createValue();
  }

  @Override
  public long getPos() throws IOException {
    return delegate.getPos();
  }

  @Override
  public void close() throws IOException {
    try {
      delegate.close();
    } finally {
      parquetReader.close();
    }
  }

  @Override
  public float getProgress() throws IOException {
    return delegate.getProgress();
  }

  private static ParquetValueReader<Variant> createReader(
      MessageType fileSchema, VariantColumnDescriptor variantColumn) {
    Type readSchema = variantColumn.prunedSchema();

    // Prune the reader schema to match the projection so we don't expect columns skipped by I/O filtering
    return (ParquetValueReader<Variant>) ParquetVariantVisitor.visit(
        readSchema.asGroupType(),
        new VariantReaderBuilder(fileSchema, Arrays.asList(variantColumn.physicalPath())));
  }
}
