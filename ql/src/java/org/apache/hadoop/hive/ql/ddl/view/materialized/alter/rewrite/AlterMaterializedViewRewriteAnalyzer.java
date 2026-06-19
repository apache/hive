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

package org.apache.hadoop.hive.ql.ddl.view.materialized.alter.rewrite;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.SourceTable;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.view.materialized.update.MaterializedViewUpdateDesc;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for alter materialized view rewrite commands.
 */
@DDLType(types = HiveParser.TOK_ALTER_MATERIALIZED_VIEW_REWRITE)
public class AlterMaterializedViewRewriteAnalyzer extends BaseSemanticAnalyzer {
  public AlterMaterializedViewRewriteAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  private DDLDescWithWriteId ddlDescWithWriteId;

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    TableName tableName = getQualifiedTableName((ASTNode)root.getChild(0));

    // Value for the flag
    boolean rewriteEnable;
    switch (root.getChild(1).getType()) {
    case HiveParser.TOK_REWRITE_ENABLED:
      rewriteEnable = true;
      break;
    case HiveParser.TOK_REWRITE_DISABLED:
      rewriteEnable = false;
      break;
    default:
      throw new SemanticException("Invalid alter materialized view expression");
    }

    // It can be fully qualified name or use default database
    Table materializedViewTable = getTable(tableName, true);

    // One last test: if we are enabling the rewrite, we need to check that query
    // only uses transactional (MM and ACID and Iceberg) tables
    if (rewriteEnable) {
      for (SourceTable sourceTable : materializedViewTable.getMVMetadata().getSourceTables()) {
        Table table = new Table(sourceTable.getTable());
        if (!AcidUtils.isTransactionalTable(sourceTable.getTable()) &&
                !(table.isNonNative() && table.getStorageHandler().areSnapshotsSupported())) {
          throw new SemanticException("Automatic rewriting for materialized view cannot be enabled if the " +
              "materialized view uses non-transactional tables");
        }
      }
    }

    AlterMaterializedViewRewriteDesc desc =
        new AlterMaterializedViewRewriteDesc(tableName.getNotEmptyDbTable(), rewriteEnable);
    if (AcidUtils.isTransactionalTable(materializedViewTable)) {
      ddlDescWithWriteId = desc;
    }

    inputs.add(new ReadEntity(materializedViewTable));
    WriteEntity.WriteType type;
    if (MetaStoreUtils.isNonNativeTable(materializedViewTable.getTTable())
        && materializedViewTable.getStorageHandler().areSnapshotsSupported()) {
      type = WriteEntity.WriteType.DDL_SHARED;
    } else {
      type = AcidUtils.isLocklessReadsEnabled(materializedViewTable, conf) ?
          WriteEntity.WriteType.DDL_EXCL_WRITE : WriteEntity.WriteType.DDL_EXCLUSIVE;
    }
    outputs.add(new WriteEntity(materializedViewTable, type));

    // Create task for alterMVRewriteDesc
    DDLWork work = new DDLWork(getInputs(), getOutputs(), desc);
    Task<?> targetTask = TaskFactory.get(work);

    // Create task to update rewrite flag as dependant of previous one
    MaterializedViewUpdateDesc materializedViewUpdateDesc =
        new MaterializedViewUpdateDesc(tableName.getNotEmptyDbTable(), rewriteEnable, !rewriteEnable, false);
    DDLWork updateDdlWork = new DDLWork(getInputs(), getOutputs(), materializedViewUpdateDesc);
    targetTask.addDependentTask(TaskFactory.get(updateDdlWork, conf));

    // Add root task
    rootTasks.add(targetTask);
  }

  @Override
  public DDLDescWithWriteId getAcidDdlDesc() {
    return ddlDescWithWriteId;
  }
}
