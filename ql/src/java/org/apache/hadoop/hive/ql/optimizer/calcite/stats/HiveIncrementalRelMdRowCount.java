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
package org.apache.hadoop.hive.ql.optimizer.calcite.stats;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.type.SnapshotContext;
import org.apache.hadoop.hive.metastore.api.SourceTable;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.MaterializedViewMetadata;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTezModelRelMetadataProvider;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.StreamSupport;

public class HiveIncrementalRelMdRowCount extends HiveRelMdRowCount {

  public static JaninoRelMetadataProvider createMetadataProvider(RelOptMaterialization materialization) {
    return JaninoRelMetadataProvider.of(
            ChainedRelMetadataProvider.of(
                    ImmutableList.of(
                            HiveIncrementalRelMdRowCount.source(materialization),
                            HiveTezModelRelMetadataProvider.DEFAULT
                    )));
  }

  public static RelMetadataProvider source(RelOptMaterialization materialization) {
    MaterializedViewMetadata mvMetadata = ((RelOptHiveTable) materialization.tableRel.getTable())
            .getHiveTableMD().getMVMetadata();
    Map<String, SourceTable> sourceTableMap = new HashMap<>(mvMetadata.getSourceTables().size());
    for (SourceTable sourceTable : mvMetadata.getSourceTables()) {
      Table table = sourceTable.getTable();
      sourceTableMap.put(
              TableName.getQualified(table.getCatName(), table.getDbName(), table.getTableName()), sourceTable);
    }

    return ReflectiveRelMetadataProvider
            .reflectiveSource(BuiltInMethod.ROW_COUNT.method, new HiveIncrementalRelMdRowCount(sourceTableMap));
  }

  private final Map<String, SourceTable> sourceTableMap;

  public HiveIncrementalRelMdRowCount(Map<String, SourceTable> sourceTableMap) {
    this.sourceTableMap = sourceTableMap;
  }


  @Override
  public Double getRowCount(TableScan rel, RelMetadataQuery mq) {
    if (!(rel instanceof HiveTableScan)) {
      return super.getRowCount(rel, mq);
    }

    HiveTableScan tableScan = (HiveTableScan) rel;
    RelOptHiveTable relOptHiveTable = (RelOptHiveTable) tableScan.getTable();
    org.apache.hadoop.hive.ql.metadata.Table table = relOptHiveTable.getHiveTableMD();
    String fullyQualifiedName = TableName.getQualified(table.getCatName(), table.getDbName(), table.getTableName());
    SourceTable sourceTable = sourceTableMap.get(fullyQualifiedName);
    if (sourceTable == null) {
      return super.getRowCount(rel, mq);
    }

    HiveStorageHandler storageHandler = table.getStorageHandler();
    if (storageHandler != null && storageHandler.areSnapshotsSupported()) {
      SnapshotContext since = new SnapshotContext(Long.parseLong(table.getVersionIntervalFrom()));
      return StreamSupport.stream(storageHandler.getSnapshotContexts(table, since).spliterator(), false)
          .mapToDouble(SnapshotContext::getAddedRowCount).sum();
    }

    return (double) sourceTable.getInsertedCount();
  }
}
