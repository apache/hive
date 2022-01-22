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

package org.apache.hadoop.hive.ql.metadata;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.SourceTable;

import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;

public class MaterializedViewMetadata {
  final CreationMetadata creationMetadata;

  MaterializedViewMetadata(CreationMetadata creationMetadata) {
    this.creationMetadata = creationMetadata;
  }

  public MaterializedViewMetadata(
          String catalogName, String dbName, String mvName, Set<SourceTable> sourceTables, String validTxnList) {
    this.creationMetadata = new CreationMetadata(catalogName, dbName, mvName, toFullTableNames(sourceTables));
    this.creationMetadata.setValidTxnList(validTxnList);
    this.creationMetadata.setSourceTables(unmodifiableSet(sourceTables));
  }

  public Set<String> getSourceTableFullNames() {
    if (!creationMetadata.isSetSourceTables()) {
      return emptySet();
    }

    return toFullTableNames(creationMetadata.getSourceTables());
  }

  private Set<String> toFullTableNames(Set<SourceTable> sourceTables) {
    return unmodifiableSet(sourceTables.stream()
            .map(sourceTable -> TableName.getDbTable(
                    sourceTable.getTable().getDbName(), sourceTable.getTable().getTableName()))
            .collect(Collectors.toSet()));
  }

  public Set<TableName> getSourceTableNames() {
    if (!creationMetadata.isSetSourceTables()) {
      return emptySet();
    }

    return unmodifiableSet(creationMetadata.getSourceTables().stream()
            .map(sourceTable -> new TableName(
                    sourceTable.getTable().getCatName(),
                    sourceTable.getTable().getDbName(),
                    sourceTable.getTable().getTableName()))
            .collect(Collectors.toSet()));
  }

  public Set<SourceTable> getSourceTables() {
    if (!creationMetadata.isSetSourceTables()) {
      return emptySet();
    }

    return unmodifiableSet(creationMetadata.getSourceTables());
  }

  public String getValidTxnList() {
    return creationMetadata.getValidTxnList();
  }

  public long getMaterializationTime() {
    return creationMetadata.getMaterializationTime();
  }

  public MaterializedViewMetadata reset(String validTxnList) {
    Set<SourceTable> newSourceTables =
            creationMetadata.getSourceTables().stream().map(this::from).collect(Collectors.toSet());

    return new MaterializedViewMetadata(
            creationMetadata.getCatName(),
            creationMetadata.getDbName(),
            creationMetadata.getTblName(),
            unmodifiableSet(newSourceTables),
            validTxnList);
  }

  private SourceTable from(SourceTable sourceTable) {
    SourceTable newSourceTable = new SourceTable();

    newSourceTable.setTable(sourceTable.getTable());
    newSourceTable.setInsertedCount(0L);
    newSourceTable.setUpdatedCount(0L);
    newSourceTable.setDeletedCount(0L);

    return newSourceTable;
  }
}
