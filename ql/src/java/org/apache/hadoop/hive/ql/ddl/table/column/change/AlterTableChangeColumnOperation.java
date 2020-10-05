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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.ddl.table.column.change;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableUtils;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableOperation;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;

/**
 * Operation process changing a column.
 */
public class AlterTableChangeColumnOperation extends AbstractAlterTableOperation<AlterTableChangeColumnDesc> {
  public AlterTableChangeColumnOperation(DDLOperationContext context, AlterTableChangeColumnDesc desc) {
    super(context, desc);
  }

  @Override
  protected void doAlteration(Table table, Partition partition) throws HiveException {
    StorageDescriptor sd = getStorageDescriptor(table, partition);
    String serializationLib = sd.getSerdeInfo().getSerializationLib();
    AvroSerdeUtils.handleAlterTableForAvro(context.getConf(), serializationLib, table.getTTable().getParameters());

    // if orc table, restrict reordering columns as it will break schema evolution
    boolean isOrcSchemaEvolution = sd.getInputFormat().equals(OrcInputFormat.class.getName()) &&
        AlterTableUtils.isSchemaEvolutionEnabled(table, context.getConf());
    if (isOrcSchemaEvolution && (desc.isFirst() || StringUtils.isNotBlank(desc.getAfterColumn()))) {
      throw new HiveException(ErrorMsg.CANNOT_REORDER_COLUMNS, desc.getDbTableName());
    }

    FieldSchema column = null;
    boolean found = false;
    int position = desc.isFirst() ? 0 : -1;
    int i = 1;

    List<FieldSchema> oldColumns = (partition == null ? table.getColsForMetastore() : partition.getColsForMetastore());
    List<FieldSchema> newColumns = new ArrayList<>();
    for (FieldSchema oldColumn : oldColumns) {
      String oldColumnName = oldColumn.getName();
      if (oldColumnName.equalsIgnoreCase(desc.getOldColumnName())) {
        oldColumn.setName(desc.getNewColumnName().toLowerCase());
        if (StringUtils.isNotBlank(desc.getNewColumnType())) {
          oldColumn.setType(desc.getNewColumnType());
        }
        if (desc.getNewColumnComment() != null) {
          oldColumn.setComment(desc.getNewColumnComment());
        }
        if (CollectionUtils.isNotEmpty(sd.getBucketCols()) && sd.getBucketCols().contains(oldColumnName)) {
          sd.getBucketCols().remove(oldColumnName);
          sd.getBucketCols().add(desc.getNewColumnName().toLowerCase());
        }
        found = true;
        if (desc.isFirst() || StringUtils.isNotBlank(desc.getAfterColumn())) {
          column = oldColumn;
          continue;
        }
      } else if (oldColumnName.equalsIgnoreCase(desc.getNewColumnName())) {
        throw new HiveException(ErrorMsg.DUPLICATE_COLUMN_NAMES, desc.getNewColumnName());
      }

      if (oldColumnName.equalsIgnoreCase(desc.getAfterColumn())) {
        position = i;
      }

      i++;
      newColumns.add(oldColumn);
    }

    if (!found) {
      throw new HiveException(ErrorMsg.INVALID_COLUMN, desc.getOldColumnName());
    }
    if (StringUtils.isNotBlank(desc.getAfterColumn()) && position < 0) {
      throw new HiveException(ErrorMsg.INVALID_COLUMN, desc.getAfterColumn());
    }

    if (position >= 0) {
      newColumns.add(position, column);
    }

    sd.setCols(newColumns);
  }
}
