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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.ddl.table.partition.PartitionUtils;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for rename partition commands.
 */
@DDLType(types = HiveParser.TOK_ALTERTABLE_RENAMEPART)
public  class AlterTableRenamePartitionAnalyzer extends AbstractAlterTableAnalyzer {
  public AlterTableRenamePartitionAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {
    Table table = getTable(tableName, true);
    validateAlterTableType(table, AlterTableType.RENAMEPARTITION, false);

    Map<String, String> newPartitionSpec = getValidatedPartSpec(table, (ASTNode)command.getChild(0), conf, false);
    if (newPartitionSpec == null) {
      throw new SemanticException("RENAME PARTITION Missing Destination" + command);
    }
    ReadEntity re = new ReadEntity(table);
    re.noLockNeeded();
    inputs.add(re);

    List<Map<String, String>> allPartitionSpecs = new ArrayList<>();
    allPartitionSpecs.add(partitionSpec);
    allPartitionSpecs.add(newPartitionSpec);

    boolean clonePart = HiveConf.getBoolVar(conf, ConfVars.HIVE_ACID_RENAME_PARTITION_MAKE_COPY)
      || HiveConf.getBoolVar(conf, ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED)
      && AcidUtils.isTransactionalTable(table);
    
    PartitionUtils.addTablePartsOutputs(db, outputs, table, allPartitionSpecs, false,
      clonePart ? WriteType.DDL_EXCL_WRITE : WriteType.DDL_EXCLUSIVE);

    AlterTableRenamePartitionDesc desc = new AlterTableRenamePartitionDesc(tableName, partitionSpec, newPartitionSpec,
        null, table);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));

    if (AcidUtils.isTransactionalTable(table)) {
      setAcidDdlDesc(desc);
    }
  }
}
