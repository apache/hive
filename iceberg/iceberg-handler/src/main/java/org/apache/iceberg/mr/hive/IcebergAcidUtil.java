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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.IOContextMap;
import org.apache.hadoop.hive.ql.io.PositionDeleteInfo;
import org.apache.hadoop.hive.ql.io.RowLineageInfo;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionKey;
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
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;

public class IcebergAcidUtil {

  private IcebergAcidUtil() {
  }

  private static final Map<Types.NestedField, Integer> FILE_READ_META_COLS = Maps.newLinkedHashMap();
  public static final String META_TABLE_PROPERTY = "metaTable";
  private static final Map<Types.NestedField, Integer> DELETE_FILE_META_COLS = Maps.newLinkedHashMap();
  public static final Integer PARTITION_PROJECTION_COLUMN_ID = Integer.MAX_VALUE - 6;
  private static final String PARTITION_PROJECTION_COLUMN_NAME = "_partition_projection";

  static {
    DELETE_FILE_META_COLS.put(MetadataColumns.FILE_PATH, 0);
    DELETE_FILE_META_COLS.put(MetadataColumns.ROW_POSITION, 1);

    FILE_READ_META_COLS.put(MetadataColumns.SPEC_ID, 0);
    FILE_READ_META_COLS.put(MetadataColumns.FILE_PATH, 1);
    FILE_READ_META_COLS.put(MetadataColumns.ROW_POSITION, 2);
  }

  private static final Types.NestedField PARTITION_HASH_META_COL = Types.NestedField.required(
      MetadataColumns.PARTITION_COLUMN_ID, MetadataColumns.PARTITION_COLUMN_NAME, Types.LongType.get());

  private static final Types.NestedField PARTITION_PROJECTION = Types.NestedField.required(
      PARTITION_PROJECTION_COLUMN_ID, PARTITION_PROJECTION_COLUMN_NAME, Types.StringType.get());

  private static final Map<Types.NestedField, Integer> SERDE_META_COLS = Maps.newLinkedHashMap();

  // a merge task reads delete files, so its writer has no row data to derive the partition key from
  private static final Map<Types.NestedField, Integer> MERGE_SERDE_META_COLS = Maps.newLinkedHashMap();

  static {
    SERDE_META_COLS.put(MetadataColumns.SPEC_ID, 0);
    SERDE_META_COLS.put(PARTITION_HASH_META_COL, 1);
    SERDE_META_COLS.put(MetadataColumns.FILE_PATH, 2);
    SERDE_META_COLS.put(MetadataColumns.ROW_POSITION, 3);

    MERGE_SERDE_META_COLS.putAll(SERDE_META_COLS);
    MERGE_SERDE_META_COLS.put(PARTITION_PROJECTION, 4);
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
   * @param dataCols The columns of the serde projection schema
   * @param isMergeTask Whether the schema is for a merge task, which also carries the partition key
   * @return The schema for SerDe operations, extended with metadata columns needed for deletes
   */
  public static Schema createSerdeSchemaForDelete(List<Types.NestedField> dataCols, boolean isMergeTask) {
    Map<Types.NestedField, Integer> metaCols = isMergeTask ?
        MERGE_SERDE_META_COLS : SERDE_META_COLS;
    List<Types.NestedField> cols = Lists.newArrayListWithCapacity(dataCols.size() + metaCols.size());
    cols.addAll(metaCols.keySet());
    cols.addAll(dataCols);
    return new Schema(cols);
  }

  /**
   * Based on `rec` the method creates a position delete object, and also populates the data filed of `rowData` with
   * the field values from `rec`.
   * @param rec The record read by the file scan task, which contains both the metadata fields and the row data fields
   * @param rowData The record object to populate with the rowData fields only
   * @param isMergeTask Whether the record was built by a merge task
   * @return The position delete object
   */
  public static PositionDelete<Record> getPositionDelete(Record rec, Record rowData, boolean isMergeTask) {
    Map<Types.NestedField, Integer> metaCols = isMergeTask ?
        MERGE_SERDE_META_COLS : SERDE_META_COLS;
    PositionDelete<Record> positionDelete = PositionDelete.create();
    String filePath = rec.get(metaCols.get(MetadataColumns.FILE_PATH), String.class);
    Long filePosition = rec.get(metaCols.get(MetadataColumns.ROW_POSITION), Long.class);

    int dataOffset = metaCols.size(); // position in the rec where the actual row data begins
    for (int i = dataOffset; i < rec.size(); ++i) {
      rowData.set(i - dataOffset, rec.get(i));
    }

    positionDelete.set(filePath, ObjectUtils.defaultIfNull(filePosition, 0L), rowData);
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

  public static PartitionKey parsePartitionKey(Record rec) {
    String serializedStr = rec.get(MERGE_SERDE_META_COLS.get(PARTITION_PROJECTION), String.class);
    return SerializationUtil.deserializeFromBase64(serializedStr);
  }

  public static String getSerializedPartitionKey(StructLike structLike, PartitionSpec partitionSpec) {
    PartitionKey partitionKey = new PartitionKey(partitionSpec, partitionSpec.schema());
    if (structLike != null) {
      for (int idx = 0; idx < structLike.size(); idx++) {
        partitionKey.set(idx, structLike.get(idx, Object.class));
      }
    }
    return SerializationUtil.serializeToBase64(partitionKey);
  }

  public static String getFilePath(Record rec) {
    return rec.get(DELETE_FILE_META_COLS.get(MetadataColumns.FILE_PATH), String.class);
  }

  public static long getFilePosition(Record rec) {
    return rec.get(FILE_READ_META_COLS.get(MetadataColumns.ROW_POSITION), Long.class);
  }

  public static long getDeleteFilePosition(Record rec) {
    return rec.get(DELETE_FILE_META_COLS.get(MetadataColumns.ROW_POSITION), Long.class);
  }

  public static long computeHash(StructLike struct) {
    long partHash = -1;
    if (struct != null) {
      Object[] partFields = new Object[struct.size()];
      for (int i = 0; i < struct.size(); ++i) {
        partFields[i] = struct.get(i, Object.class);
      }
      partHash = Objects.hash(partFields);
    }
    return partHash;
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

    private final CloseableIterator<T> currentIterator;
    private final GenericRecord current;
    private final Configuration conf;

    private final int specId;
    private final long partitionHash;
    private final String filePath;

    public VirtualColumnAwareIterator(CloseableIterator<T> currentIterator, List<Types.NestedField> columns,
        Configuration conf, FileScanTask task) {
      this.currentIterator = currentIterator;
      this.current = GenericRecord.create(
          new Schema(columns.subList(FILE_READ_META_COLS.size(), columns.size())));
      this.conf = conf;

      this.specId = task.file().specId();
      this.partitionHash = computeHash(task.file().partition());
      this.filePath = task.file().location();

      IOContextMap.get(conf).setPartitionName(
          IcebergTableUtil.toPartitionName(task.spec(), task.file().partition()));
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
      PositionDeleteInfo.setIntoConf(conf,
          specId,
          partitionHash,
          filePath,
          IcebergAcidUtil.getFilePosition(rec));
      RowLineageInfo.setRowLineageInfoIntoConf(RowLineageReader.readRowId(rec),
          RowLineageReader.readLastUpdatedSequenceNumber(rec), conf);
      return (T) current;
    }
  }

  public static class MergeTaskVirtualColumnAwareIterator<T> implements CloseableIterator<T> {

    private final CloseableIterator<T> currentIterator;
    private final MergeTaskRecordBuilder<T> recordBuilder;

    private final int specId;
    private final long partitionHash;
    private final String serializedPartitionKey;

    public MergeTaskVirtualColumnAwareIterator(CloseableIterator<T> currentIterator, Schema expectedSchema,
        PartitionSpec spec, ContentFile<?> file) {
      this.currentIterator = currentIterator;
      this.recordBuilder = new MergeTaskRecordBuilder<>(expectedSchema);

      this.specId = spec.specId();
      this.partitionHash = computeHash(file.partition());
      this.serializedPartitionKey = getSerializedPartitionKey(file.partition(), spec);
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
          .withPartitionHash(partitionHash)
          .withFilePath(IcebergAcidUtil.getFilePath(rec))
          .withFilePosition(IcebergAcidUtil.getDeleteFilePosition(rec))
          .withPartitionKey(serializedPartitionKey)
          .build();
    }
  }

  private static final class MergeTaskRecordBuilder<T> {
    private final GenericRecord current;

    MergeTaskRecordBuilder(Schema schema) {
      current = GenericRecord.create(schema);
    }

    public MergeTaskRecordBuilder<T> withSpecId(int specId) {
      current.set(MERGE_SERDE_META_COLS.get(MetadataColumns.SPEC_ID), specId);
      return this;
    }

    public MergeTaskRecordBuilder<T> withPartitionHash(long partitionHash) {
      current.set(MERGE_SERDE_META_COLS.get(PARTITION_HASH_META_COL), partitionHash);
      return this;
    }

    public MergeTaskRecordBuilder<T> withFilePath(String filePath) {
      current.set(MERGE_SERDE_META_COLS.get(MetadataColumns.FILE_PATH), filePath);
      return this;
    }

    public MergeTaskRecordBuilder<T> withFilePosition(long filePosition) {
      current.set(MERGE_SERDE_META_COLS.get(MetadataColumns.ROW_POSITION), filePosition);
      return this;
    }

    public MergeTaskRecordBuilder<T> withPartitionKey(String serializedPartitionKey) {
      current.set(MERGE_SERDE_META_COLS.get(PARTITION_PROJECTION), serializedPartitionKey);
      return this;
    }

    public T build() {
      return (T) current;
    }
  }

}
