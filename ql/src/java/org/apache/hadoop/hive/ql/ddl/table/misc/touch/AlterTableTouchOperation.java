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

package org.apache.hadoop.hive.ql.ddl.table.misc.touch;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * Operation process of touching a table.
 */
public class AlterTableTouchOperation extends DDLOperation<AlterTableTouchDesc> {
  public AlterTableTouchOperation(DDLOperationContext context, AlterTableTouchDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException  {
    // TODO: catalog
    Table table = context.getDb().getTable(desc.getTableName());
    EnvironmentContext environmentContext = new EnvironmentContext();
    environmentContext.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);

    if (desc.getPartitionSpec() == null) {
      context.getDb().alterTable(table, false, environmentContext, true);
      context.getWork().getInputs().add(new ReadEntity(table));
      DDLUtils.addIfAbsentByName(new WriteEntity(table, WriteEntity.WriteType.DDL_NO_LOCK), context);
    } else {
      Partition part = context.getDb().getPartition(table, desc.getPartitionSpec(), false);
      if (part == null) {
        throw new HiveException("Specified partition does not exist");
      }
      try {
        context.getDb().alterPartition(table.getCatalogName(), table.getDbName(), table.getTableName(), part,
            environmentContext, true);
      } catch (InvalidOperationException e) {
        throw new HiveException(e);
      }
      context.getWork().getInputs().add(new ReadEntity(part));
      DDLUtils.addIfAbsentByName(new WriteEntity(part, WriteEntity.WriteType.DDL_NO_LOCK), context);
    }
    return 0;
  }
}
