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
import java.util.stream.Collectors;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.PositionDeleteInfo;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;

public class IcebergAcidUtil {

  private IcebergAcidUtil() {
  }

  private static final Types.NestedField PARTITION_STRUCT_META_COL = null; // placeholder value in the map
  private static final Map<Types.NestedField, Integer> FILE_READ_META_COLS = Maps.newLinkedHashMap();
  private static final Map<String, Types.NestedField> VIRTUAL_COLS_TO_META_COLS = Maps.newLinkedHashMap();

  static {
    FILE_READ_META_COLS.put(MetadataColumns.SPEC_ID, 0);
    FILE_READ_META_COLS.put(PARTITION_STRUCT_META_COL, 1);
    FILE_READ_META_COLS.put(MetadataColumns.FILE_PATH, 2);
    FILE_READ_META_COLS.put(MetadataColumns.ROW_POSITION, 3);

    VIRTUAL_COLS_TO_META_COLS.put(VirtualColumn.PARTITION_SPEC_ID.getName(), MetadataColumns.SPEC_ID);
    VIRTUAL_COLS_TO_META_COLS.put(VirtualColumn.PARTITION_HASH.getName(), PARTITION_STRUCT_META_COL);
    VIRTUAL_COLS_TO_META_COLS.put(VirtualColumn.FILE_PATH.getName(), MetadataColumns.FILE_PATH);
    VIRTUAL_COLS_TO_META_COLS.put(VirtualColumn.ROW_POSITION.getName(), MetadataColumns.ROW_POSITION);
  }

  private static final Types.NestedField PARTITION_HASH_META_COL = Types.NestedField.required(
      MetadataColumns.PARTITION_COLUMN_ID, MetadataColumns.PARTITION_COLUMN_NAME, Types.LongType.get());
  private static final Map<Types.NestedField, Integer> SERDE_META_COLS = Maps.newLinkedHashMap();

  static {
    SERDE_META_COLS.put(MetadataColumns.SPEC_ID, 0);
    SERDE_META_COLS.put(PARTITION_HASH_META_COL, 1);
    SERDE_META_COLS.put(MetadataColumns.FILE_PATH, 2);
    SERDE_META_COLS.put(MetadataColumns.ROW_POSITION, 3);
  }

  /**
   * @param dataCols The columns of the original file read schema
   * @param table The table object - it is used for populating the partition struct meta column
   * @return The schema for reading files, extended with metadata columns
   */
  public static Schema createFileReadSchemaWithVirtualColums(List<Types.NestedField> dataCols, Table table) {
    List<Types.NestedField> cols = Lists.newArrayListWithCapacity(dataCols.size() + FILE_READ_META_COLS.size());
    FILE_READ_META_COLS.forEach((metaCol, index) -> {
      if (metaCol == PARTITION_STRUCT_META_COL) {
        cols.add(MetadataColumns.metadataColumn(table, MetadataColumns.PARTITION_COLUMN_NAME));
      } else {
        cols.add(metaCol);
      }
    });
    cols.addAll(dataCols);
    return new Schema(cols);
  }

  /**
   * @param dataCols The columns of the serde projection schema
   * @return The schema for SerDe operations, extended with metadata columns needed for deletes
   */
  public static Schema createSerdeSchemaForDelete(List<Types.NestedField> dataCols) {
    List<Types.NestedField> cols = Lists.newArrayListWithCapacity(dataCols.size() + SERDE_META_COLS.size());
    SERDE_META_COLS.forEach((metaCol, index) -> cols.add(metaCol));
    cols.addAll(dataCols);
    return new Schema(cols);
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

    positionDelete.set(filePath, ObjectUtils.defaultIfNull(filePosition, 0L), rowData);
    return positionDelete;
  }

  /**
   * @param dataCols The columns of the original file read schema
   * @param table The table object - it is used for populating the partition struct meta column
   * @return The schema for reading files, extended with metadata columns needed for deletes
   */
  public static Schema createFileReadSchemaForUpdate(List<Types.NestedField> dataCols, Table table) {
    List<Types.NestedField> cols = Lists.newArrayListWithCapacity(dataCols.size() + FILE_READ_META_COLS.size());
    FILE_READ_META_COLS.forEach((metaCol, index) -> {
      if (metaCol == PARTITION_STRUCT_META_COL) {
        cols.add(MetadataColumns.metadataColumn(table, MetadataColumns.PARTITION_COLUMN_NAME));
      } else {
        cols.add(metaCol);
      }
    });
    // Old column values
    cols.addAll(dataCols.stream()
        .map(f -> Types.NestedField.optional(1147483545 + f.fieldId(), "__old_value_for" + f.name(), f.type()))
        .collect(Collectors.toList()));
    // New column values
    cols.addAll(dataCols);
    return new Schema(cols);
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
        .collect(Collectors.toList()));
    // New column values
    cols.addAll(dataCols);
    return new Schema(cols);
  }

  /**
   * Get the original record from the updated record. Populate the `original` with the filed values from `rec`.
   * @param rec The record read by the file scan task, which contains both the metadata fields and the row data fields
   * @param original The record object to populate. The end result is the original record before the update.
   */
  public static void populateWithOriginalValues(Record rec, Record original) {
    int dataOffset = SERDE_META_COLS.size();
    for (int i = dataOffset; i < dataOffset + original.size(); ++i) {
      original.set(i - dataOffset, rec.get(i));
    }
  }

  /**
   * Get the new record from the updated record. Populate the `newRecord` with the filed values from `rec`.
   * @param rec The record read by the file scan task, which contains both the metadata fields and the row data fields
   * @param newRecord The record object to populate. The end result is the new record after the update.
   */
  public static void populateWithNewValues(Record rec, Record newRecord) {
    int dataOffset = SERDE_META_COLS.size() + newRecord.size();
    for (int i = dataOffset; i < dataOffset + newRecord.size(); ++i) {
      newRecord.set(i - dataOffset, rec.get(i));
    }
  }

  public static int parseSpecId(Record rec) {
    return rec.get(FILE_READ_META_COLS.get(MetadataColumns.SPEC_ID), Integer.class);
  }

  public static long computePartitionHash(Record rec) {
    StructProjection part = rec.get(FILE_READ_META_COLS.get(PARTITION_STRUCT_META_COL), StructProjection.class);
    // we need to compute a hash value for the partition struct so that it can be used as a sorting key
    return computeHash(part);
  }

  public static String parseFilePath(Record rec) {
    return rec.get(FILE_READ_META_COLS.get(MetadataColumns.FILE_PATH), String.class);
  }

  public static long parseFilePosition(Record rec) {
    return rec.get(FILE_READ_META_COLS.get(MetadataColumns.ROW_POSITION), Long.class);
  }

  public static long computeHash(StructProjection struct) {
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

  public static class VirtualColumnAwareIterator<T> implements CloseableIterator<T> {

    private final CloseableIterator<T> currentIterator;

    private GenericRecord current;
    private final Schema expectedSchema;
    private final Configuration conf;

    public VirtualColumnAwareIterator(CloseableIterator<T> currentIterator, Schema expectedSchema, Configuration conf) {
      this.currentIterator = currentIterator;
      this.expectedSchema = expectedSchema;
      this.conf = conf;
      current = GenericRecord.create(
          new Schema(expectedSchema.columns().subList(4, expectedSchema.columns().size())));
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
      PositionDeleteInfo.setIntoConf(conf,
          IcebergAcidUtil.parseSpecId(rec),
          IcebergAcidUtil.computePartitionHash(rec),
          IcebergAcidUtil.parseFilePath(rec),
          IcebergAcidUtil.parseFilePosition(rec));
      IcebergAcidUtil.copyFields(rec, FILE_READ_META_COLS.size(), current.size(), current);
      return (T) current;
    }
  }
}
