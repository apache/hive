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
import java.util.Objects;
import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.ParquetRecordReaderBase;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.ParquetVariantVisitor;
import org.apache.iceberg.parquet.VariantReaderBuilder;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;

final class ParquetVariantRecordReader implements RecordReader<NullWritable, VectorizedRowBatch> {

  /**
   * Iceberg's shredded VARIANT Parquet layout may encode values in {@code typed_value} and either omit the serialized
   * {@code value} bytes entirely (fully shredded) or store only a serialized remainder. Hive's Parquet vectorized
   * reader does not reconstruct the full serialized VARIANT, so vectorized evaluation of functions like
   * {@code variant_get(...)} may fail or produce incorrect results.
   *
   * <p>This wrapper reads VARIANT values using Iceberg's Parquet VARIANT readers and populates Hive's VARIANT struct
   * representation (metadata/value byte arrays) in the output {@link VectorizedRowBatch}.
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

    String columns = job.get(IOConstants.COLUMNS);
    if (columns == null || columns.isEmpty()) {
      return Optional.empty();
    }

    List<String> columnNames = DataWritableReadSupport.getColumnNames(columns);
    if (columnNames == null || columnNames.isEmpty()) {
      return Optional.empty();
    }
    boolean readAll = ColumnProjectionUtils.isReadAllColumns(job);
    List<Integer> readColumnIds = ColumnProjectionUtils.getReadColumnIDs(job);

    List<VariantColumn> variantColumns =
        variantColumns(fileSchema, columnNames, task.spec().schema(), readAll, readColumnIds);
    if (variantColumns.isEmpty()) {
      return Optional.empty();
    }

    List<ColumnDescriptor> requestedColumns = requestedColumns(fileSchema, variantColumns);
    if (requestedColumns.isEmpty()) {
      return Optional.empty();
    }

    List<BlockMetaData> blocks = blocksForSplit(delegate, start, length, parquetMetadata);
    if (blocks.isEmpty()) {
      // Delegate reader will handle empty split; don't wrap.
      return Optional.empty();
    }

    return Optional.of(new ParquetVariantRecordReader(
        delegate, job, path, parquetMetadata, blocks, requestedColumns, variantColumns));
  }

  private record VariantColumn(
      int rootColumnIndex,
      int[] fieldPath,
      ParquetValueReader<Variant> reader,
      String[] parquetPath) {
  }

  private static List<VariantColumn> variantColumns(
      MessageType fileSchema,
      List<String> columnNames,
      org.apache.iceberg.Schema icebergSchema,
      boolean readAll,
      List<Integer> readColumnIds) {

    boolean[] projected = projectedTopLevelColumns(readAll, readColumnIds, columnNames.size());

    List<VariantColumn> result = Lists.newArrayList();
    for (int colIdx = 0; colIdx < columnNames.size(); colIdx++) {
      if (!projected[colIdx]) {
        continue;
      }

      String columnName = columnNames.get(colIdx);
      if (!fileSchema.containsField(columnName)) {
        continue;
      }

      Types.NestedField field = icebergSchema.findField(columnName);
      if (field == null || field.type() == null) {
        continue;
      }

      org.apache.parquet.schema.Type parquetType = fileSchema.getType(columnName);
      if (parquetType == null) {
        continue;
      }

      collectVariantColumns(
          fileSchema,
          colIdx,
          field.type(),
          parquetType,
          new int[0],
          new String[] {columnName},
          result);
    }

    return result;
  }

  private static boolean[] projectedTopLevelColumns(
      boolean readAll, List<Integer> readColumnIds, int fieldCount) {
    boolean[] projected = new boolean[fieldCount];
    if (readAll) {
      Arrays.fill(projected, true);
      return projected;
    }

    if (readColumnIds == null || readColumnIds.isEmpty()) {
      return projected;
    }

    for (Integer id : readColumnIds) {
      if (id != null && id >= 0 && id < fieldCount) {
        projected[id] = true;
      }
    }

    return projected;
  }

  private static void collectVariantColumns(
      MessageType fileSchema,
      int rootColumnIndex,
      org.apache.iceberg.types.Type icebergType,
      org.apache.parquet.schema.Type parquetType,
      int[] fieldPath,
      String[] parquetPath,
      List<VariantColumn> results) {

    if (icebergType == null || parquetType == null) {
      return;
    }

    switch (icebergType.typeId()) {
      case VARIANT:
        addVariantColumn(
            fileSchema, rootColumnIndex, parquetType, fieldPath, parquetPath, results);
        return;

      case STRUCT:
        collectFromStruct(
            fileSchema,
            rootColumnIndex,
            icebergType.asStructType(),
            parquetType,
            fieldPath,
            parquetPath,
            results);
        return;

      case LIST:
      case MAP:
        // VARIANT shredding is not applied within arrays or maps
        return;

      default:
    }
  }

  private static void addVariantColumn(
      MessageType fileSchema,
      int rootColumnIndex,
      org.apache.parquet.schema.Type parquetType,
      int[] fieldPath,
      String[] parquetPath,
      List<VariantColumn> results) {

    VariantColumn variantColumn =
        getOrCreateVariantColumn(fileSchema, rootColumnIndex, parquetType, fieldPath, parquetPath);

    if (variantColumn != null) {
      results.add(variantColumn);
    }
  }

  private static void collectFromStruct(
      MessageType fileSchema,
      int rootColumnIndex,
      Types.StructType structType,
      org.apache.parquet.schema.Type parquetType,
      int[] fieldPath,
      String[] parquetPath,
      List<VariantColumn> results) {

    if (parquetType.isPrimitive()) {
      return;
    }

    List<Types.NestedField> fields = structType.fields();
    if (fields.isEmpty()) {
      return;
    }

    GroupType parquetGroup = parquetType.asGroupType();

    for (int i = 0; i < fields.size(); i++) {
      Types.NestedField field = fields.get(i);
      if (field == null || field.type() == null) {
        continue;
      }

      org.apache.parquet.schema.Type nestedParquetType = childType(parquetGroup, field);

      if (nestedParquetType == null) {
        continue;
      }

      collectVariantColumns(
          fileSchema,
          rootColumnIndex,
          field.type(),
          nestedParquetType,
          append(fieldPath, i),
          append(parquetPath, nestedParquetType.getName()),
          results);
    }
  }

  private static VariantColumn getOrCreateVariantColumn(
      MessageType fileSchema,
      int rootColumnIndex,
      org.apache.parquet.schema.Type parquetType,
      int[] fieldPath,
      String[] parquetPath) {
    if (parquetType.isPrimitive()) {
      return null;
    }

    GroupType variantGroup = parquetType.asGroupType();
    if (!variantGroup.containsField("metadata") || !variantGroup.containsField("typed_value")) {
      // If there is no typed_value, Hive's reader is sufficient.
      return null;
    }

    ParquetValueReader<Variant> variantReader =
        (ParquetValueReader<Variant>) ParquetVariantVisitor.visit(
            variantGroup, new VariantReaderBuilder(fileSchema, Arrays.asList(parquetPath)));

    return new VariantColumn(rootColumnIndex, fieldPath, variantReader, parquetPath);
  }

  private static org.apache.parquet.schema.Type childType(GroupType parent, Types.NestedField child) {
    if (parent == null || child == null) {
      return null;
    }

    int id = child.fieldId();
    for (org.apache.parquet.schema.Type candidate : parent.getFields()) {
      if (candidate != null && candidate.getId() != null && candidate.getId().intValue() == id) {
        return candidate;
      }
    }

    if (child.name() == null || child.name().isEmpty()) {
      return null;
    }

    if (!parent.containsField(child.name())) {
      return null;
    }

    return parent.getType(child.name());
  }

  private static int[] append(int[] path, int index) {
    int[] copy = Arrays.copyOf(path, path.length + 1);
    copy[path.length] = index;
    return copy;
  }

  private static String[] append(String[] path, String segment) {
    String[] copy = Arrays.copyOf(path, path.length + 1);
    copy[path.length] = segment;
    return copy;
  }

  private static List<ColumnDescriptor> requestedColumns(
      MessageType fileSchema, List<VariantColumn> variantColumns) {
    // Build the list of Parquet leaf columns needed to read all requested VARIANT columns.
    List<ColumnDescriptor> requested = Lists.newArrayList();
    for (ColumnDescriptor desc : fileSchema.getColumns()) {
      String[] pathParts = desc.getPath();
      if (pathParts == null || pathParts.length == 0) {
        continue;
      }

      for (VariantColumn vc : variantColumns) {
        if (startsWith(pathParts, vc.parquetPath())) {
          requested.add(desc);
          break;
        }
      }
    }

    return requested;
  }

  private static boolean startsWith(String[] path, String[] prefix) {
    if (path == null || prefix == null || path.length < prefix.length) {
      return false;
    }
    for (int i = 0; i < prefix.length; i++) {
      if (!Objects.equals(path[i], prefix[i])) {
        return false;
      }
    }
    return true;
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

    List<BlockMetaData> splitBlocks = Lists.newArrayList();
    for (BlockMetaData block : parquetMetadata.getBlocks()) {
      long firstDataPage = block.getColumns().getFirst().getFirstDataPageOffset();
      if (firstDataPage >= start && firstDataPage < start + length) {
        splitBlocks.add(block);
      }
    }
    return splitBlocks;
  }

  private final RecordReader<NullWritable, VectorizedRowBatch> delegate;
  private final ParquetFileReader parquetReader;
  private final List<VariantColumn> variantColumns;

  private long remainingInRowGroup;

  private ParquetVariantRecordReader(
      RecordReader<NullWritable, VectorizedRowBatch> delegate,
      JobConf job,
      Path path,
      ParquetMetadata parquetMetadata,
      List<BlockMetaData> blocks,
      List<ColumnDescriptor> requestedColumns,
      List<VariantColumn> variantColumns) throws IOException {
    this.delegate = delegate;
    this.variantColumns = variantColumns;

    this.parquetReader =
        new ParquetFileReader(job, parquetMetadata.getFileMetaData(), path, blocks, requestedColumns);

    advanceRowGroup();
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
    int size = batch.size;

    StructColumnVector[] structVectors = new StructColumnVector[variantColumns.size()];
    for (int i = 0; i < variantColumns.size(); i++) {
      structVectors[i] = variantStructVector(batch, variantColumns.get(i));
    }

    boolean selectedInUse = batch.selectedInUse;
    int[] selected = batch.selected;

    for (int position = 0; position < size; position++) {
      ensureRowAvailable();

      int row = selectedInUse ? selected[position] : position;

      for (int i = 0; i < variantColumns.size(); i++) {
        VariantColumn vc = variantColumns.get(i);
        StructColumnVector structVector = structVectors[i];
        if (structVector == null || structVector.fields == null || structVector.fields.length < 2) {
          continue;
        }

        Variant variant = vc.reader().read(null);

        BytesColumnVector metadataVector = (BytesColumnVector) structVector.fields[0];
        BytesColumnVector valueVector = (BytesColumnVector) structVector.fields[1];

        if (variant == null) {
          markNull(structVector, metadataVector, valueVector, row);
        } else {
          markNonNull(structVector, metadataVector, valueVector, row);
          writeToVector(metadataVector, row, variant.metadata());
          writeToVector(valueVector, row, variant.value());
        }
      }

      remainingInRowGroup--;
    }
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private static StructColumnVector variantStructVector(VectorizedRowBatch batch, VariantColumn vc) {
    if (batch == null || vc == null || batch.cols == null) {
      return null;
    }

    int rootIndex = vc.rootColumnIndex();
    if (rootIndex < 0 || rootIndex >= batch.cols.length) {
      return null;
    }

    ColumnVector current = batch.cols[rootIndex];
    if (!(current instanceof StructColumnVector)) {
      return null;
    }

    // If the delegate marked any of the struct vectors as repeating, clear it because we are about to materialize
    // per-row bytes.
    current.isRepeating = false;

    int[] fieldPath = vc.fieldPath();
    for (int i = 0; i < fieldPath.length; i++) {
      if (!(current instanceof StructColumnVector struct)) {
        return null;
      }

      struct.isRepeating = false;
      if (struct.fields == null) {
        return null;
      }

      int fieldIndex = fieldPath[i];
      if (fieldIndex < 0 || fieldIndex >= struct.fields.length) {
        return null;
      }

      current = struct.fields[fieldIndex];
    }

    if (!(current instanceof StructColumnVector variantStruct) ||
        variantStruct.fields == null ||
        variantStruct.fields.length < 2) {
      return null;
    }

    variantStruct.isRepeating = false;
    if (variantStruct.fields[0] != null) {
      variantStruct.fields[0].isRepeating = false;
    }
    if (variantStruct.fields[1] != null) {
      variantStruct.fields[1].isRepeating = false;
    }
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
    for (VariantColumn vc : variantColumns) {
      vc.reader().setPageSource(nextRowGroup);
    }
  }

  private static void writeToVector(BytesColumnVector vector, int row, VariantMetadata metadata) {
    if (vector == null || metadata == null) {
      return;
    }
    int length = metadata.sizeInBytes();
    vector.ensureValPreallocated(length);

    ByteBuffer buffer = ByteBuffer.wrap(vector.getValPreallocatedBytes()).order(ByteOrder.LITTLE_ENDIAN);
    metadata.writeTo(buffer, vector.getValPreallocatedStart());
    vector.setValPreallocated(row, length);
  }

  private static void writeToVector(BytesColumnVector vector, int row, VariantValue value) {
    if (vector == null || value == null) {
      return;
    }
    int length = value.sizeInBytes();
    vector.ensureValPreallocated(length);

    ByteBuffer buffer = ByteBuffer.wrap(vector.getValPreallocatedBytes()).order(ByteOrder.LITTLE_ENDIAN);
    value.writeTo(buffer, vector.getValPreallocatedStart());
    vector.setValPreallocated(row, length);
  }

  private static void markNull(
      StructColumnVector structVector, BytesColumnVector metadataVector, BytesColumnVector valueVector, int row) {
    structVector.noNulls = false;
    structVector.isNull[row] = true;
    if (metadataVector != null) {
      metadataVector.noNulls = false;
      metadataVector.isNull[row] = true;
    }
    if (valueVector != null) {
      valueVector.noNulls = false;
      valueVector.isNull[row] = true;
    }
  }

  private static void markNonNull(
      StructColumnVector structVector, BytesColumnVector metadataVector, BytesColumnVector valueVector, int row) {
    structVector.isNull[row] = false;
    if (metadataVector != null) {
      metadataVector.isNull[row] = false;
    }
    if (valueVector != null) {
      valueVector.isNull[row] = false;
    }
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
}
