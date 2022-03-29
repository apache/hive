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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hive.ql.io.PositionDeleteInfo;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;

public class IcebergAcidUtil {

  private IcebergAcidUtil() {
  }

  private static final Types.NestedField PARTITION_STRUCT_META_COL = null; // placeholder value in the map
  private static final Map<Types.NestedField, Integer> DELETE_FILEREAD_META_COLS = Maps.newLinkedHashMap();

  static {
    DELETE_FILEREAD_META_COLS.put(MetadataColumns.SPEC_ID, 0);
    DELETE_FILEREAD_META_COLS.put(PARTITION_STRUCT_META_COL, 1);
    DELETE_FILEREAD_META_COLS.put(MetadataColumns.FILE_PATH, 2);
    DELETE_FILEREAD_META_COLS.put(MetadataColumns.ROW_POSITION, 3);
  }

  private static final Types.NestedField PARTITION_HASH_META_COL = Types.NestedField.required(
      MetadataColumns.PARTITION_COLUMN_ID, MetadataColumns.PARTITION_COLUMN_NAME, Types.LongType.get());
  private static final Map<Types.NestedField, Integer> DELETE_SERDE_META_COLS = Maps.newLinkedHashMap();

  static {
    DELETE_SERDE_META_COLS.put(MetadataColumns.SPEC_ID, 0);
    DELETE_SERDE_META_COLS.put(PARTITION_HASH_META_COL, 1);
    DELETE_SERDE_META_COLS.put(MetadataColumns.FILE_PATH, 2);
    DELETE_SERDE_META_COLS.put(MetadataColumns.ROW_POSITION, 3);
  }

  /**
   * @param columns The columns of the file read schema
   * @return The schema for reading files, extended with metadata columns needed for deletes
   */
  public static Schema createFileReadSchemaForDelete(List<Types.NestedField> columns, Table table) {
    List<Types.NestedField> deleteCols = new ArrayList<>();
    DELETE_FILEREAD_META_COLS.forEach((col, index) -> {
      if (col == PARTITION_STRUCT_META_COL) {
        deleteCols.add(MetadataColumns.metadataColumn(table, MetadataColumns.PARTITION_COLUMN_NAME));
      } else {
        deleteCols.add(col);
      }
    });
    deleteCols.addAll(columns);
    return new Schema(deleteCols);
  }

  /**
   * @param columns The columns of the serde projection schema
   * @return The schema for SerDe operations, extended with metadata columns needed for deletes
   */
  public static Schema createSerdeSchemaForDelete(List<Types.NestedField> columns) {
    List<Types.NestedField> deleteCols = new ArrayList<>();
    DELETE_SERDE_META_COLS.forEach((col, index) -> deleteCols.add(col));
    deleteCols.addAll(columns);
    return new Schema(deleteCols);
  }

  public static PositionDelete<Record> getPositionDelete(Schema schema, Record rec) {
    PositionDelete<Record> positionDelete = PositionDelete.create();
    String filePath = rec.get(DELETE_SERDE_META_COLS.get(MetadataColumns.FILE_PATH), String.class);
    long filePosition = rec.get(DELETE_SERDE_META_COLS.get(MetadataColumns.ROW_POSITION), Long.class);

    int dataOffset = DELETE_SERDE_META_COLS.size(); // position in the rec where the actual row data begins
    Record rowData = GenericRecord.create(schema);
    for (int i = dataOffset; i < rec.size(); ++i) {
      rowData.set(i - dataOffset, rec.get(i));
    }

    positionDelete.set(filePath, filePosition, rowData);
    return positionDelete;
  }

  public static PositionDeleteInfo parsePositionDeleteInfoFromRecord(GenericRecord rec) {
    int specId = rec.get(DELETE_FILEREAD_META_COLS.get(MetadataColumns.SPEC_ID), Integer.class);
    StructProjection struct = rec.get(DELETE_FILEREAD_META_COLS.get(PARTITION_STRUCT_META_COL), StructProjection.class);
    String filePath = rec.get(DELETE_FILEREAD_META_COLS.get(MetadataColumns.FILE_PATH), String.class);
    long filePos = rec.get(DELETE_FILEREAD_META_COLS.get(MetadataColumns.ROW_POSITION), Long.class);

    // we need to compute a hash value for the partition struct so that it can be used as a sorting key
    return new PositionDeleteInfo(specId, computeHash(struct), filePath, filePos);
  }

  private static long computeHash(StructProjection struct) {
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
}
