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

package org.apache.hadoop.hive.ql.ddl.table.partition.add;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.table.partition.PartitionUtils;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MoveWork;

/**
 * Analyzer for add partition commands for tables.
 */
@DDLType(types = HiveParser.TOK_ALTERTABLE_ADDPARTS)
public class AlterTableAddPartitionAnalyzer extends AbstractAddPartitionAnalyzer {
  public AlterTableAddPartitionAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected boolean expectView() {
    return false;
  }

  @Override
  protected boolean allowLocation() {
    return true;
  }

  /**
   * Add partition for Transactional tables needs to add (copy/rename) the data so that it lands
   * in a delta_x_x/ folder in the partition dir.
   */
  @Override
  protected void postProcess(TableName tableName, Table table, AlterTableAddPartitionDesc desc, Task<DDLWork> ddlTask)
      throws SemanticException {
    if (!AcidUtils.isTransactionalTable(table)) {
      return;
    }

    setAcidDdlDesc(desc);

    Long writeId = null;
    int stmtId = 0;

    for (AlterTableAddPartitionDesc.PartitionDesc partitionDesc : desc.getPartitions()) {
      if (partitionDesc.getLocation() != null) {
        AcidUtils.validateAcidPartitionLocation(partitionDesc.getLocation(), conf);
        if (desc.isIfNotExists()) {
          //Don't add partition data if it already exists
          Partition oldPart = PartitionUtils.getPartition(db, table, partitionDesc.getPartSpec(), false);
          if (oldPart != null) {
            continue;
          }
        }

        if (writeId == null) {
          // so that we only allocate a writeId if actually adding data (vs. adding a partition w/o data)
          try {
            writeId = getTxnMgr().getTableWriteId(table.getDbName(), table.getTableName());
          } catch (LockException ex) {
            throw new SemanticException("Failed to allocate the write id", ex);
          }
          stmtId = getTxnMgr().getStmtIdAndIncrement();
        }
        LoadTableDesc loadTableWork = new LoadTableDesc(new Path(partitionDesc.getLocation()),
            Utilities.getTableDesc(table), partitionDesc.getPartSpec(),
            LoadTableDesc.LoadFileType.KEEP_EXISTING, //not relevant - creating new partition
            writeId);
        loadTableWork.setStmtId(stmtId);
        loadTableWork.setInheritTableSpecs(true);
        try {
          partitionDesc.setLocation(new Path(table.getDataLocation(),
              Warehouse.makePartPath(partitionDesc.getPartSpec())).toString());
        } catch (MetaException ex) {
          throw new SemanticException("Could not determine partition path due to: " + ex.getMessage(), ex);
        }
        Task<MoveWork> moveTask = TaskFactory.get(
            new MoveWork(getInputs(), getOutputs(), loadTableWork, null,
                true, //make sure to check format
                false)); //is this right?
        ddlTask.addDependentTask(moveTask);
      }
    }
  }
}
