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

package org.apache.hadoop.hive.ql.ddl.table.column.update;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hive.metastore.HiveMetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.Deserializer;

/**
 * Operation process of adding some new columns.
 */
public class AlterTableUpdateColumnsOperation extends AbstractAlterTableOperation<AlterTableUpdateColumnsDesc> {
  public AlterTableUpdateColumnsOperation(DDLOperationContext context, AlterTableUpdateColumnsDesc desc) {
    super(context, desc);
  }

  @Override
  protected void doAlteration(Table table, Partition partition) throws HiveException {
    //StorageDescriptor sd = getStorageDescriptor(table, partition);
    String serializationLib = table.getSd().getSerdeInfo().getSerializationLib();

    Collection<String> serdes = MetastoreConf.getStringCollection(context.getConf(),
        MetastoreConf.ConfVars.SERDES_USING_METASTORE_FOR_SCHEMA);
    if (serdes.contains(serializationLib)) {
      throw new HiveException(table.getTableName() + " has serde " + serializationLib + " for which schema " +
          "is already handled by HMS.");
    }

    Deserializer deserializer = table.getDeserializer(true);
    try {
      LOG.info("Updating metastore columns for table: {}", table.getTableName());
      List<FieldSchema> fields = HiveMetaStoreUtils.getFieldsFromDeserializer(table.getTableName(), deserializer,
          context.getConf());
      StorageDescriptor sd = getStorageDescriptor(table, partition);
      sd.setCols(fields);
    } catch (org.apache.hadoop.hive.serde2.SerDeException | MetaException e) {
      LOG.error("alter table update columns: {}", e);
      throw new HiveException(e, ErrorMsg.GENERIC_ERROR);
    }
  }
}
