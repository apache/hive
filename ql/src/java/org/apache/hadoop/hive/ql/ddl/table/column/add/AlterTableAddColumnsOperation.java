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

package org.apache.hadoop.hive.ql.ddl.table.column.add;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;

/**
 * Operation process of adding some new columns.
 */
public class AlterTableAddColumnsOperation extends AbstractAlterTableOperation<AlterTableAddColumnsDesc> {
  public AlterTableAddColumnsOperation(DDLOperationContext context, AlterTableAddColumnsDesc desc) {
    super(context, desc);
  }

  @Override
  protected void doAlteration(Table table, Partition partition) throws HiveException {
    StorageDescriptor sd = getStorageDescriptor(table, partition);
    String serializationLib = sd.getSerdeInfo().getSerializationLib();
    AvroSerdeUtils.handleAlterTableForAvro(context.getConf(), serializationLib, table.getTTable().getParameters());

    List<FieldSchema> oldColumns = (partition == null ? table.getColsForMetastore() : partition.getColsForMetastore());
    List<FieldSchema> newColumns = desc.getNewColumns();

    if ("org.apache.hadoop.hive.serde.thrift.columnsetSerDe".equals(serializationLib)) {
      context.getConsole().printInfo("Replacing columns for columnsetSerDe and changing to LazySimpleSerDe");
      sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
      sd.setCols(newColumns);
    } else {
      // make sure the columns does not already exist
      for (FieldSchema newColumn : newColumns) {
        for (FieldSchema oldColumn : oldColumns) {
          if (oldColumn.getName().equalsIgnoreCase(newColumn.getName())) {
            throw new HiveException(ErrorMsg.DUPLICATE_COLUMN_NAMES, newColumn.getName());
          }
        }

        oldColumns.add(newColumn);
      }
      sd.setCols(oldColumns);
    }
  }
}
