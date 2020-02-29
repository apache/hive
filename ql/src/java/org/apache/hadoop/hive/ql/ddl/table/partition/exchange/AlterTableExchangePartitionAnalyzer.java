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

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.partition.PartitionUtils;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for exchange partition commands.
 */
@DDLType(types = HiveParser.TOK_ALTERTABLE_EXCHANGEPARTITION)
public  class AlterTableExchangePartitionAnalyzer extends AbstractAlterTableAnalyzer {
  public AlterTableExchangePartitionAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {
    Table destTable = getTable(tableName);
    Table sourceTable = getTable(getUnescapedName((ASTNode)command.getChild(1)));

    // Get the partition specs
    Map<String, String> partitionSpecs = getValidatedPartSpec(sourceTable, (ASTNode)command.getChild(0), conf, false);
    PartitionUtils.validatePartitions(conf, partitionSpecs);

    boolean sameColumns = MetaStoreUtils.compareFieldColumns(destTable.getAllCols(), sourceTable.getAllCols());
    boolean samePartitions = MetaStoreUtils.compareFieldColumns(destTable.getPartitionKeys(),
        sourceTable.getPartitionKeys());
    if (!sameColumns || !samePartitions) {
      throw new SemanticException(ErrorMsg.TABLES_INCOMPATIBLE_SCHEMAS.getMsg());
    }

    // Exchange partition is not allowed with transactional tables.
    // If only source is transactional table, then target will see deleted rows too as no snapshot
    // isolation applicable for non-acid tables.
    // If only target is transactional table, then data would be visible to all ongoing transactions
    // affecting the snapshot isolation.
    // If both source and targets are transactional tables, then target partition may have delta/base
    // files with write IDs may not be valid. It may affect snapshot isolation for on-going txns as well.
    if (AcidUtils.isTransactionalTable(sourceTable) || AcidUtils.isTransactionalTable(destTable)) {
      throw new SemanticException(ErrorMsg.EXCHANGE_PARTITION_NOT_ALLOWED_WITH_TRANSACTIONAL_TABLES.getMsg());
    }

    // check if source partition exists
    PartitionUtils.getPartitions(db, sourceTable, partitionSpecs, true);

    // Verify that the partitions specified are continuous
    // If a subpartition value is specified without specifying a partition's value then we throw an exception
    int counter = isPartitionValueContinuous(sourceTable.getPartitionKeys(), partitionSpecs);
    if (counter < 0) {
      throw new SemanticException(ErrorMsg.PARTITION_VALUE_NOT_CONTINUOUS.getMsg(partitionSpecs.toString()));
    }

    List<Partition> destPartitions = null;
    try {
      destPartitions = PartitionUtils.getPartitions(db, destTable, partitionSpecs, true);
    } catch (SemanticException ex) {
      // We should expect a semantic exception being throw as this partition should not be present.
    }
    if (destPartitions != null) {
      // If any destination partition is present then throw a Semantic Exception.
      throw new SemanticException(ErrorMsg.PARTITION_EXISTS.getMsg(destPartitions.toString()));
    }

    AlterTableExchangePartitionsDesc desc =
        new AlterTableExchangePartitionsDesc(sourceTable, destTable, partitionSpecs);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));

    inputs.add(new ReadEntity(sourceTable));
    outputs.add(new WriteEntity(destTable, WriteType.DDL_SHARED));
  }


  /**
   * @return >=0 if no subpartition value is specified without a partition's value being specified else it returns -1
   */
  private int isPartitionValueContinuous(List<FieldSchema> partitionKeys, Map<String, String> partitionSpecs) {
    int counter = 0;
    for (FieldSchema partitionKey : partitionKeys) {
      if (partitionSpecs.containsKey(partitionKey.getName())) {
        counter++;
        continue;
      }
      return partitionSpecs.size() == counter ? counter : -1;
    }
    return counter;
  }
}
