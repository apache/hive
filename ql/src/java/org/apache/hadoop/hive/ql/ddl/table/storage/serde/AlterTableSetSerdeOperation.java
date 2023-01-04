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

package org.apache.hadoop.hive.ql.ddl.table.storage.serde;

import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableUtils;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableOperation;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.Deserializer;

/**
 * Operation process of setting the serde.
 */
public class AlterTableSetSerdeOperation extends AbstractAlterTableOperation<AlterTableSetSerdeDesc> {
  public AlterTableSetSerdeOperation(DDLOperationContext context, AlterTableSetSerdeDesc desc) {
    super(context, desc);
  }

  @Override
  protected void doAlteration(Table table, Partition partition) throws HiveException {
    StorageDescriptor sd = getStorageDescriptor(table, partition);
    String serdeName = desc.getSerdeName();
    String oldSerdeName = sd.getSerdeInfo().getSerializationLib();

    // if orc table, restrict changing the serde as it can break schema evolution
    if (AlterTableUtils.isSchemaEvolutionEnabled(table, context.getConf()) &&
        oldSerdeName.equalsIgnoreCase(OrcSerde.class.getName()) &&
        !serdeName.equalsIgnoreCase(OrcSerde.class.getName())) {
      throw new HiveException(ErrorMsg.CANNOT_CHANGE_SERDE, OrcSerde.class.getSimpleName(),
          desc.getDbTableName());
    }

    sd.getSerdeInfo().setSerializationLib(serdeName);
    if (MapUtils.isNotEmpty(desc.getProps())) {
      sd.getSerdeInfo().getParameters().putAll(desc.getProps());
    }

    if (partition == null) {
      if (Table.shouldStoreFieldsInMetastore(context.getConf(), serdeName, table.getParameters())
          && !Table.hasMetastoreBasedSchema(context.getConf(), oldSerdeName)) {
        // If new SerDe needs to store fields in metastore, but the old serde doesn't, save
        // the fields so that new SerDe could operate. Note that this may fail if some fields
        // from old SerDe are too long to be stored in metastore, but there's nothing we can do.
        try {
          Deserializer oldSerde = HiveMetaStoreUtils.getDeserializer(context.getConf(), table.getTTable(), null,
              false, oldSerdeName);
          table.setFields(Hive.getFieldsFromDeserializer(table.getTableName(), oldSerde, context.getConf()));
        } catch (MetaException ex) {
          throw new HiveException(ex);
        }
      }
    }
  }
}
