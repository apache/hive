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
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.types.Types;

public class IcebergAcidUtil {

  private IcebergAcidUtil() {
  }

  enum DeleteMetaColumns {
    SPEC_ID(MetadataColumns.SPEC_ID),
    PARTITION_HASH(Types.NestedField.required(MetadataColumns.PARTITION_COLUMN_ID,
        MetadataColumns.PARTITION_COLUMN_NAME, Types.LongType.get())),
    FILE_PATH(MetadataColumns.DELETE_FILE_PATH),
    FILE_POS(MetadataColumns.DELETE_FILE_POS);

    private final Types.NestedField field;

    DeleteMetaColumns(Types.NestedField field) {
      this.field = field;
    }

    public Types.NestedField getMetadataCol() {
      return field;
    }
  }

  /**
   * @param columns The Iceberg table's columns that the delete statement is targeting
   * @return The deleteSchema for the table, extended with metadata columns needed for deletes
   */
  public static Schema createDeleteSchema(List<Types.NestedField> columns) {
    List<Types.NestedField> deleteCols = new ArrayList<>(columns);
    for (DeleteMetaColumns value : DeleteMetaColumns.values()) {
      deleteCols.add(value.getMetadataCol());
    }
    return new Schema(deleteCols);
  }

  public static PositionDelete<Record> buildPositionDelete(Schema schema, Record rec) {
    String filePath = rec.get(DeleteMetaColumns.FILE_PATH.ordinal(), String.class);
    long filePosition = rec.get(DeleteMetaColumns.FILE_POS.ordinal(), Long.class);
    int dataOffset = DeleteMetaColumns.values().length;
    Record rowData = GenericRecord.create(schema);
    for (int i = dataOffset; i < rec.size(); ++i) {
      rowData.set(i - dataOffset, rec.get(i));
    }
    PositionDelete<Record> positionDelete = PositionDelete.create();
    positionDelete.set(filePath, filePosition, rowData);
    return positionDelete;
  }
}
