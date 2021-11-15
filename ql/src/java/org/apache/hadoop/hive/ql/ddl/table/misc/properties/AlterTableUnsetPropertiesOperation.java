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

package org.apache.hadoop.hive.ql.ddl.table.misc.properties;

import java.util.Set;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableOperation;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * Operation process of unsetting properties of a table.
 */
public class AlterTableUnsetPropertiesOperation extends AbstractAlterTableOperation<AlterTableUnsetPropertiesDesc> {
  public AlterTableUnsetPropertiesOperation(DDLOperationContext context, AlterTableUnsetPropertiesDesc desc) {
    super(context, desc);
  }

  @Override
  protected void doAlteration(Table table, Partition partition) throws HiveException {
    if (StatsSetupConst.USER.equals(environmentContext.getProperties().get(StatsSetupConst.STATS_GENERATED))) {
      // drop a stats parameter, which triggers recompute stats update automatically
      environmentContext.getProperties().remove(StatsSetupConst.DO_NOT_UPDATE_STATS);
    }

    if (partition == null) {
      Set<String> removedSet = desc.getProps().keySet();
      boolean isFromMmTable = AcidUtils.isInsertOnlyTable(table.getParameters());
      boolean isRemoved = AcidUtils.isRemovedInsertOnlyTable(removedSet);
      if (isFromMmTable && isRemoved) {
        throw new HiveException("Cannot convert an ACID table to non-ACID");
      }

      // Check if external table property being removed
      if (removedSet.contains("EXTERNAL") && table.getTableType() == TableType.EXTERNAL_TABLE) {
        table.setTableType(TableType.MANAGED_TABLE);
      }
    }
    for (String key : desc.getProps().keySet()) {
      if (partition != null) {
        partition.getTPartition().getParameters().remove(key);
      } else {
        table.getTTable().getParameters().remove(key);
      }
    }
  }
}
