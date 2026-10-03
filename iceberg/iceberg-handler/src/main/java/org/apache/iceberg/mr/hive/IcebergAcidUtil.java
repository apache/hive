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

package org.apache.iceberg.mr.hive;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.IOContext;
import org.apache.hadoop.hive.ql.io.PositionDeleteInfo;
import org.apache.hadoop.hive.ql.io.RowLineageInfo;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.hadoop.io.WritableUtils;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.hive.HiveTxnCoordinator;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.mr.mapreduce.RowLineageReader;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;

public class IcebergAcidUtil {

  private IcebergAcidUtil() {
  }

  private static final Map<Types.NestedField, Integer> FILE_READ_META_COLS = Maps.newLinkedHashMap();
  public static final String META_TABLE_PROPERTY = "metaTable";
  private static final Map<Types.NestedField, Integer> DELETE_FILE_META_COLS = Maps.newLinkedHashMap();

  static {
    DELETE_FILE_META_COLS.put(MetadataColumns.FILE_PATH, 0);
    DELETE_FILE_META_COLS.put(MetadataColumns.ROW_POSITION, 1);

    FILE_READ_META_COLS.put(MetadataColumns.SPEC_ID, 0);
    FILE_READ_META_COLS.put(MetadataColumns.FILE_PATH, 1);
    FILE_READ_META_COLS.put(MetadataColumns.ROW_POSITION, 2);
  }

  // the partition of the data file of a record, serialized by serializePartition
  private static final Types.NestedField PARTITION_META_COL = Types.NestedField.required(
      MetadataColumns.PARTITION_COLUMN_ID, MetadataColumns.PARTITION_COLUMN_NAME, Types.BinaryType.get());

  // the virtual columns of a deleted record, the row data of a copy-on-write record follows them
  private static final Map<Types.NestedField, Integer> SERDE_META_COLS = Maps.newLinkedHashMap();

  static {
    SERDE_META_COLS.put(MetadataColumns.SPEC_ID, 0);
    SERDE_META_COLS.put(PARTITION_META_COL, 1);
    SERDE_META_COLS.put(MetadataColumns.FILE_PATH, 2);
    SERDE_META_COLS.put(MetadataColumns.ROW_POSITION, 3);
  }

  /**
   * @param dataCols The columns of the original file read schema
   * @return The schema for reading files, extended with metadata columns
   */
  public static Schema createFileReadSchemaWithVirtualColums(List<Types.NestedField> dataCols) {
    List<Types.NestedField> cols = Lists.newArrayListWithCapacity(dataCols.size() + FILE_READ_META_COLS.size());
    FILE_READ_META_COLS.forEach((metaCol, index) -> cols.add(metaCol));
    cols.addAll(dataCols);
    return new Schema(cols);
  }

  /**
   * The schema of the records a merge-on-read DML or a merge task deletes: their virtual columns.
   */
  public static Schema createSerdeSchemaForDelete() {
    return new Schema(Lists.newArrayList(SERDE_META_COLS.keySet()));
  }

  /**
   * @param dataCols The columns of the serde projection schema
   * @return The schema for SerDe operations, extended with metadata columns needed for deletes
   */
  public static Schema createSerdeSchemaForDelete(List<Types.NestedField> dataCols) {
    List<Types.NestedField> cols = Lists.newArrayListWithCapacity(dataCols.size() + SERDE_META_COLS.size());
    cols.addAll(SERDE_META_COLS.keySet());
    cols.addAll(dataCols);
    return new Schema(cols);
  }

  /**
   * @param rec The record a merge-on-read DML or a merge task deletes
   * @return The position delete of the record, without row data
   */
  public static PositionDelete<Record> getPositionDelete(Record rec) {
    PositionDelete<Record> positionDelete = PositionDelete.create();
    return positionDelete.set(rec.get(SERDE_META_COLS.get(MetadataColumns.FILE_PATH), String.class),
        rec.get(SERDE_META_COLS.get(MetadataColumns.ROW_POSITION), Long.class));
  }

  /**
   * Based on `rec` the method creates a position delete object, and also populates the data filed of `rowData` with
   * the field values from `rec`.
   * @param rec The record read by the file scan task, which contains both the metadata fields and the row data fields
   * @param rowData The record object to populate with the rowData fields only
   * @return The position delete object
   */
  public static PositionDelete<Record> getPositionDelete(Record rec, Record rowData) {
    PositionDelete<Record> positionDelete = PositionDelete.create();
    String filePath = rec.get(SERDE_META_COLS.get(MetadataColumns.FILE_PATH), String.class);
    Long filePosition = rec.get(SERDE_META_COLS.get(MetadataColumns.ROW_POSITION), Long.class);

    int dataOffset = SERDE_META_COLS.size(); // position in the rec where the actual row data begins
    for (int i = dataOffset; i < rec.size(); ++i) {
      rowData.set(i - dataOffset, rec.get(i));
    }

    positionDelete.set(filePath, ObjectUtils.defaultIfNull(filePosition, 0L));
    return positionDelete;
  }

  /**
   * @param dataCols The columns of the serde projection schema
   * @return The schema for SerDe operations, extended with metadata columns needed for deletes
   */
  public static Schema createSerdeSchemaForUpdate(List<Types.NestedField> dataCols) {
    List<Types.NestedField> cols = Lists.newArrayListWithCapacity(dataCols.size() + SERDE_META_COLS.size());
    SERDE_META_COLS.forEach((metaCol, index) -> cols.add(metaCol));
    // Old column values
    cols.addAll(dataCols.stream()
        .map(f -> Types.NestedField.optional(1147483545 + f.fieldId(), "__old_value_for_" + f.name(), f.type()))
        .toList());
    // New column values
    cols.addAll(dataCols);
    return new Schema(cols);
  }

  public static int parseSpecId(Record rec) {
    return rec.get(FILE_READ_META_COLS.get(MetadataColumns.SPEC_ID), Integer.class);
  }

  public static ByteBuffer parseSerializedPartition(Record rec) {
    return rec.get(SERDE_META_COLS.get(PARTITION_META_COL), ByteBuffer.class);
  }

  /**
   * Serializes the partition of a data file for the delete writer: per field of the partition type, a VInt of 0 for
   * null, otherwise of 1 + the length of the single-value serialization of the value, followed by it.
   * @param partition The partition of the data file
   * @param partitionSpec The spec of the data file
   * @return The serialized partition, empty for an unpartitioned spec
   */
  public static byte[] serializePartition(StructLike partition, PartitionSpec partitionSpec) {
    List<Types.NestedField> fields = partitionSpec.partitionType().fields();
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      for (int pos = 0; pos < fields.size(); pos++) {
        Type type = fields.get(pos).type();
        Object value = partition.get(pos, Object.class);
        // the source column of a field of the unknown type was dropped, the field reads as null
        if (value == null || type.typeId() == Type.TypeID.UNKNOWN) {
          WritableUtils.writeVInt(out, 0);
        } else {
          byte[] serialized = ByteBuffers.toByteArray(Conversions.toByteBuffer(type, value));
          WritableUtils.writeVInt(out, serialized.length + 1);
          out.write(serialized);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return bytes.toByteArray();
  }

  /**
   * @param serialized The partition serialized by {@link #serializePartition}
   * @param partitionSpec The spec of the data file
   * @return The partition of the data file
   */
  public static StructLike deserializePartition(ByteBuffer serialized, PartitionSpec partitionSpec) {
    Types.StructType partitionType = partitionSpec.partitionType();
    GenericRecord partition = GenericRecord.create(partitionType);
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(ByteBuffers.toByteArray(serialized)))) {
      for (int pos = 0; pos < partitionType.fields().size(); pos++) {
        int length = WritableUtils.readVInt(in);
        if (length > 0) {
          byte[] value = new byte[length - 1];
          in.readFully(value);
          Object field = Conversions.fromByteBuffer(partitionType.fields().get(pos).type(), ByteBuffer.wrap(value));
          // a partition holds a string as a String, the single-value serialization reads it as a CharBuffer
          partition.set(pos, field instanceof CharSequence chars ? chars.toString() : field);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return partition;
  }

  public static String getFilePath(Record rec) {
    return rec.get(DELETE_FILE_META_COLS.get(MetadataColumns.FILE_PATH), String.class);
  }

  public static long getDeleteFilePosition(Record rec) {
    return rec.get(DELETE_FILE_META_COLS.get(MetadataColumns.ROW_POSITION), Long.class);
  }

  public static void copyFields(GenericRecord source, int start, int len, GenericRecord target) {
    for (int sourceIdx = start, targetIdx = 0; targetIdx < len; ++sourceIdx, ++targetIdx) {
      target.set(targetIdx, source.get(sourceIdx));
    }
  }

  public static Transaction getOrCreateTransaction(Table table, Configuration conf) {
    HiveTxnManager txnManager = Optional.ofNullable(SessionState.get())
        .map(SessionState::getTxnMgr).orElse(null);
    if (txnManager == null) {
      return table.newTransaction();
    }
    boolean isExplicitTxnOpen = txnManager.isTxnOpen() && !txnManager.isImplicitTransactionOpen(null);
    int outputCount = SessionStateUtil.getOutputTableCount(conf)
        .orElse(1);

    if (!isExplicitTxnOpen && outputCount < 2) {
      return table.newTransaction();
    }
    HiveTxnCoordinator txnCoordinator = txnManager.getOrSetTxnCoordinator(
        HiveTxnCoordinator.class, msClient -> new HiveTxnCoordinator(conf, msClient, isExplicitTxnOpen));
    return txnCoordinator != null ?
        txnCoordinator.getOrCreateTransaction(table) : table.newTransaction();
  }

  public static Transaction getTransaction(Table table) {
    HiveTxnManager txnManager = Optional.ofNullable(SessionState.get())
        .map(SessionState::getTxnMgr).orElse(null);
    if (txnManager == null) {
      return null;
    }
    HiveTxnCoordinator txnCoordinator = txnManager.getOrSetTxnCoordinator(
        HiveTxnCoordinator.class, null);
    return txnCoordinator != null ?
        txnCoordinator.getTransaction(table) : null;
  }

  public static class VirtualColumnAwareIterator<T> implements CloseableIterator<T> {

    private static final int ROW_POSITION_INDEX = FILE_READ_META_COLS.get(MetadataColumns.ROW_POSITION);

    private final CloseableIterator<T> currentIterator;
    private final GenericRecord current;
    // the reader's IOContext, read per row: the reader opens its first file before the wrapper hands it over
    private final Supplier<IOContext> ioContext;

    private final int specId;
    private final String filePath;
    private final String partitionName;
    private final byte[] partitionKey;

    public VirtualColumnAwareIterator(CloseableIterator<T> currentIterator, List<Types.NestedField> columns,
        FileScanTask task, Supplier<IOContext> ioContext) {
      this.currentIterator = currentIterator;
      this.current = GenericRecord.create(
          new Schema(columns.subList(FILE_READ_META_COLS.size(), columns.size())));
      this.ioContext = ioContext;

      this.specId = task.file().specId();
      this.filePath = task.file().location();
      this.partitionName = IcebergTableUtil.toPartitionName(task.spec(), task.file().partition());
      this.partitionKey = serializePartition(task.file().partition(), task.spec());
    }

    @Override
    public void close() throws IOException {
      currentIterator.close();
    }

    @Override
    public boolean hasNext() {
      return currentIterator.hasNext();
    }

    @Override
    public T next() {
      T next = currentIterator.next();
      GenericRecord rec = (GenericRecord) next;
      IcebergAcidUtil.copyFields(rec, FILE_READ_META_COLS.size(), current.size(), current);
      IOContext ioCxt = ioContext.get();
      ioCxt.setPartitionName(partitionName);
      ioCxt.setPartitionKey(partitionKey);
      ioCxt.setPositionDeleteInfo(
          new PositionDeleteInfo(specId, filePath, rec.get(ROW_POSITION_INDEX, Long.class)));
      ioCxt.setRowLineageInfo(new RowLineageInfo(
          RowLineageReader.readRowId(rec), RowLineageReader.readLastUpdatedSequenceNumber(rec)));
      return (T) current;
    }
  }

  public static class MergeTaskVirtualColumnAwareIterator<T> implements CloseableIterator<T> {

    private final CloseableIterator<T> currentIterator;
    private final MergeTaskRecordBuilder<T> recordBuilder;

    private final int specId;
    private final byte[] partitionKey;

    public MergeTaskVirtualColumnAwareIterator(CloseableIterator<T> currentIterator, Schema expectedSchema,
        PartitionSpec spec, ContentFile<?> file) {
      this.currentIterator = currentIterator;
      this.recordBuilder = new MergeTaskRecordBuilder<>(expectedSchema);

      this.specId = spec.specId();
      this.partitionKey = serializePartition(file.partition(), spec);
    }

    @Override
    public void close() throws IOException {
      currentIterator.close();
    }

    @Override
    public boolean hasNext() {
      return currentIterator.hasNext();
    }

    @Override
    public T next() {
      T next = currentIterator.next();
      GenericRecord rec = (GenericRecord) next;

      return recordBuilder.withSpecId(specId)
          .withFilePath(IcebergAcidUtil.getFilePath(rec))
          .withFilePosition(IcebergAcidUtil.getDeleteFilePosition(rec))
          .withPartitionKey(partitionKey)
          .build();
    }
  }

  private static final class MergeTaskRecordBuilder<T> {
    private final GenericRecord current;

    MergeTaskRecordBuilder(Schema schema) {
      current = GenericRecord.create(schema);
    }

    public MergeTaskRecordBuilder<T> withSpecId(int specId) {
      current.set(SERDE_META_COLS.get(MetadataColumns.SPEC_ID), specId);
      return this;
    }

    public MergeTaskRecordBuilder<T> withFilePath(String filePath) {
      current.set(SERDE_META_COLS.get(MetadataColumns.FILE_PATH), filePath);
      return this;
    }

    public MergeTaskRecordBuilder<T> withFilePosition(long filePosition) {
      current.set(SERDE_META_COLS.get(MetadataColumns.ROW_POSITION), filePosition);
      return this;
    }

    public MergeTaskRecordBuilder<T> withPartitionKey(byte[] partitionKey) {
      current.set(SERDE_META_COLS.get(PARTITION_META_COL), ByteBuffer.wrap(partitionKey));
      return this;
    }

    public T build() {
      return (T) current;
    }
  }

}
