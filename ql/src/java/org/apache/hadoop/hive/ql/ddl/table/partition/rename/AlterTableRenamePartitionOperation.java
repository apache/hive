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

package org.apache.hadoop.hive.ql.ddl.table.partition.rename;

import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableUtils;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.HiveTableName;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;

/**
 * Operation process of renaming a partition of a table.
 */
public class AlterTableRenamePartitionOperation extends DDLOperation<AlterTableRenamePartitionDesc> {
  public AlterTableRenamePartitionOperation(DDLOperationContext context, AlterTableRenamePartitionDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    String tableName = desc.getTableName();
    Map<String, String> oldPartSpec = desc.getOldPartSpec();
    ReplicationSpec replicationSpec = desc.getReplicationSpec();

    if (!AlterTableUtils.allowOperationInReplicationScope(context.getDb(), tableName, oldPartSpec, replicationSpec)) {
      // no rename, the table is missing either due to drop/rename which follows the current rename.
      // or the existing table is newer than our update.
      LOG.debug("DDLTask: Rename Partition is skipped as table {} / partition {} is newer than update", tableName,
              FileUtils.makePartName(new ArrayList<>(oldPartSpec.keySet()), new ArrayList<>(oldPartSpec.values())));
      return 0;
    }

    if (Utils.isBootstrapDumpInProgress(context.getDb(), HiveTableName.of(tableName).getDb())) {
      LOG.error("DDLTask: Rename Partition not allowed as bootstrap dump in progress");
      throw new HiveException("Rename Partition: Not allowed as bootstrap dump in progress");
    }

    Table tbl = context.getDb().getTable(tableName);
    Partition oldPart = context.getDb().getPartition(tbl, oldPartSpec, false);
    if (oldPart == null) {
      String partName = FileUtils.makePartName(new ArrayList<String>(oldPartSpec.keySet()),
          new ArrayList<String>(oldPartSpec.values()));
      throw new HiveException("Rename partition: source partition [" + partName + "] does not exist.");
    }

    Partition part = context.getDb().getPartition(tbl, oldPartSpec, false);
    part.setValues(desc.getNewPartSpec());

    long writeId = desc.getWriteId();

    context.getDb().renamePartition(tbl, oldPartSpec, part, writeId);
    Partition newPart = context.getDb().getPartition(tbl, desc.getNewPartSpec(), false);
    context.getWork().getInputs().add(new ReadEntity(oldPart));

    // We've already obtained a lock on the table, don't lock the partition too
    DDLUtils.addIfAbsentByName(new WriteEntity(newPart, WriteEntity.WriteType.DDL_NO_LOCK), context);

    return 0;
  }
}
