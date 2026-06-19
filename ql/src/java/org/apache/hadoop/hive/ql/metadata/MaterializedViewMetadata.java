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

import org.apache.hadoop.hive.common.MaterializationSnapshot;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.SourceTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableSet;

public class MaterializedViewMetadata {
  private static final Logger LOG = LoggerFactory.getLogger(MaterializedViewMetadata.class);

  final CreationMetadata creationMetadata;

  MaterializedViewMetadata(CreationMetadata creationMetadata) {
    this.creationMetadata = creationMetadata;
  }

  public MaterializedViewMetadata(
          String catalogName, String dbName, String mvName, Set<SourceTable> sourceTables,
          MaterializationSnapshot snapshot) {
    this.creationMetadata = new CreationMetadata(catalogName, dbName, mvName, toFullTableNames(sourceTables));
    this.creationMetadata.setValidTxnList(snapshot.asJsonString());
    this.creationMetadata.setSourceTables(unmodifiableList(new ArrayList<>(sourceTables)));
  }

  public Set<String> getSourceTableFullNames() {
    if (!creationMetadata.isSetSourceTables()) {
      return emptySet();
    }

    return toFullTableNames(creationMetadata.getSourceTables());
  }

  private Set<String> toFullTableNames(Collection<SourceTable> sourceTables) {
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

  public Collection<SourceTable> getSourceTables() {
    if (!creationMetadata.isSetSourceTables()) {
      return emptySet();
    }

    return unmodifiableList(creationMetadata.getSourceTables());
  }

  /**
   * Get the snapshot data of the materialized view source tables stored in HMS.
   * The returned object contains the valid txn list in case of native acid tables
   * or the snapshotIds of tables which storage handle supports snapshots.
   * @return {@link MaterializationSnapshot} object or null if the wrapped {@link CreationMetadata} object
   * does not contain snapshot data
   */
  public MaterializationSnapshot getSnapshot() {
    if (creationMetadata.getValidTxnList() == null || creationMetadata.getValidTxnList().isEmpty()) {
      LOG.debug("Could not obtain materialization snapshot of materialized view {}.{}",
          creationMetadata.getDbName(), creationMetadata.getTblName());
      return null;
    }
    return MaterializationSnapshot.fromJson(creationMetadata.getValidTxnList());
  }

  public long getMaterializationTime() {
    return creationMetadata.getMaterializationTime();
  }

  public MaterializedViewMetadata reset(MaterializationSnapshot snapshot) {
    Set<SourceTable> newSourceTables =
            creationMetadata.getSourceTables().stream().map(this::from).collect(Collectors.toSet());

    return new MaterializedViewMetadata(
            creationMetadata.getCatName(),
            creationMetadata.getDbName(),
            creationMetadata.getTblName(),
            unmodifiableSet(newSourceTables),
            snapshot);
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
