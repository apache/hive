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

package org.apache.hadoop.hive.ql.ddl.table.partition.exchange;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * Operation process of exchanging some partitions between tables.
 */
public class AlterTableExchangePartitionsOperation extends DDLOperation<AlterTableExchangePartitionsDesc> {
  public AlterTableExchangePartitionsOperation(DDLOperationContext context, AlterTableExchangePartitionsDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    Map<String, String> partitionSpecs = desc.getPartitionSpecs();
    Table destTable = desc.getDestinationTable();
    Table sourceTable = desc.getSourceTable();

    List<Partition> partitions = context.getDb().exchangeTablePartitions(partitionSpecs, sourceTable.getDbName(),
        sourceTable.getTableName(), destTable.getDbName(), destTable.getTableName());
    for (Partition partition : partitions) {
      // Reuse the partition specs from dest partition since they should be the same
      context.getWork().getInputs().add(new ReadEntity(new Partition(sourceTable, partition.getSpec(), null)));

      DDLUtils.addIfAbsentByName(new WriteEntity(new Partition(sourceTable, partition.getSpec(), null),
          WriteEntity.WriteType.DELETE), context);

      DDLUtils.addIfAbsentByName(new WriteEntity(new Partition(destTable, partition.getSpec(), null),
          WriteEntity.WriteType.INSERT), context);
    }

    return 0;
  }
}
