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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.tools;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.Batchable;
import org.apache.hadoop.hive.metastore.Deadline;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.directsql.MetastoreDirectSqlUtils;
import org.apache.hadoop.hive.metastore.metastore.RawStoreBundle;
import org.apache.hadoop.hive.metastore.model.MColumnDescriptor;
import org.apache.hadoop.hive.metastore.model.MConstraint;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.metastore.model.MStorageDescriptor;
import org.apache.hadoop.hive.metastore.model.MTable;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.metastore.ObjectStore.appendPatternCondition;
import static org.apache.hadoop.hive.metastore.metastore.impl.TableStoreImpl.convertToFieldSchemas;
import static org.apache.hadoop.hive.metastore.metastore.impl.TableStoreImpl.hasRemainingCDReference;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.isEmpty;

/**
 * De-duplicates column descriptors (CDs) for partitioned tables in the metastore.
 * Identical column schemas within a table are merged so that partitions share
 * the same CD, reducing metadata bloat that can accumulate during replication.
 */
public final class ColumnDeduplicator {
  public static final String COLUMN_DEDUPLICATOR_MARKER = "__metatool_column_deduplication__";
  private final RawStore store;
  private final PersistenceManager pm;
  private final AtomicReference<String> progress;
  private final boolean isDryRun;
  private final boolean isVerbose;
  private final boolean clearMarker;

  public ColumnDeduplicator(RawStoreBundle bundle, AtomicReference<String> progress,
      boolean isDryRun, boolean isVerbose, boolean clearMarker) {
    this.store = bundle.getBaseStore();
    this.pm = bundle.getPersistentManager();
    this.progress = progress;
    this.isDryRun = isDryRun;
    this.isVerbose = isVerbose;
    this.clearMarker = clearMarker;
  }

  MetaToolObjectStore.DedupColumnsResult run(String catalogFilter, String dbFilter, String tableFilter) {
    List<Long> tables = findPartitionedTables(catalogFilter, dbFilter, tableFilter);
    MetaToolObjectStore.DedupColumnsResult result = new MetaToolObjectStore.DedupColumnsResult(tables.size());

    long start = System.currentTimeMillis();
    for (int i = 0; i < tables.size() && result.getException() == null; i++) {
      boolean committed = false;
      long tableId = tables.get(i);
      PostCommitValidation validation = null;
      TableInfo table = null;
      store.openTransaction();
      try {
        Deadline.checkTimeout();
        table = getTable(tableId);
        // This target table might be dropped or deduplicated, ignore
        // If this is the stage to clean up the marker, ignore
        if (table != null) {
          validation = deduplicateTable(table, result);
        }
        committed = store.commitTransaction();
      } catch (Throwable ex) {
        System.err.println("Failed to deduplicate the column in this table: " + (table == null ? tableId : table));
        ex.printStackTrace(System.err);
        result.catchException(ex);
      } finally {
        if (!committed) {
          store.rollbackTransaction();
          if (result.getException() == null) {
            result.catchException(
                new MetaException("Failed to apply column de-duplication updates for table: "
                    + (table == null ? tableId : table)));
          }
        }
      }

      try {
        if (committed && validation != null) {
          validateTableAfterDedup(table, validation.schemaSnapshots());
          result.incrementTablesValidated();
        }
      } catch (Exception e) {
        result.catchException(new IllegalStateException("Failed to validate the column deduplication for this table: " + table, e));
      }

      if (progress != null) {
        progress.set(String.format(
            "Finished %d tables in %d total tables, time taken: %d ms, columns updated: %d, removed: %d",
            (i + 1),
            result.getTablesScanned(),
            (System.currentTimeMillis() - start),
            result.getStorageDescriptorsUpdated(),
            result.getColumnDescriptorsRemoved()));
      }
    }
    return result;
  }

  private TableInfo getTable(long tableId) {
    MTable table = pm.getObjectById(MTable.class, new MTable.PK(tableId));
    if (table == null) {
      return null;
    }
    Map<String, String> params = table.getParameters() == null ?
        new HashMap<>() : table.getParameters();
    // This table has been processed early
    if (params.containsKey(COLUMN_DEDUPLICATOR_MARKER)) {
      if (clearMarker) {
        params.remove(COLUMN_DEDUPLICATOR_MARKER);
        table.setParameters(params);
      }
      return null;
    }

    if (!isDryRun) {
      params.put(COLUMN_DEDUPLICATOR_MARKER, "" + System.currentTimeMillis() / 1000);
      table.setParameters(params);
    }
    pm.retrieve(table.getSd().getCD());
    pm.retrieve(table.getDatabase());
    return new TableInfo(tableId, table.getSd().getCD().getId(),
        new TableName(table.getDatabase().getCatalogName(), table.getDatabase().getName(), table.getTableName()));
  }

  private PostCommitValidation deduplicateTable(TableInfo table, MetaToolObjectStore.DedupColumnsResult result)
      throws MetaException {
    List<PartitionSdInfo> partitionSds = loadPartitionStorageDescriptors(table.tableId);
    if (partitionSds.isEmpty()) {
      return null;
    }

    Set<Long> cdIds = partitionSds.stream().map(p -> p.cdId).collect(Collectors.toSet());
    cdIds.add(table.tableCdId);
    Map<List<FieldSchema>, Set<Long>> groups = groupByColumnSchema(cdIds);

    Map<Long, Long> cdRemap = new HashMap<>();
    for (Set<Long> group : groups.values()) {
      if (group.size() <= 1) {
        continue;
      }
      long canonicalCdId = pickCanonicalCdId(group, table.tableCdId, partitionSds);
      for (long cdId : group) {
        if (cdId != canonicalCdId) {
          cdRemap.put(cdId, canonicalCdId);
        }
      }
    }

    List<Map.Entry<PartitionSdInfo, Long>> partSdUpdates = buildPartitionUpdates(partitionSds, cdRemap);
    if (partSdUpdates.isEmpty()) {
      return null;
    }
    result.incrementTablesWithDuplicates();
    for (Map.Entry<PartitionSdInfo, Long> update : partSdUpdates) {
      result.incrementStorageDescriptorsUpdated();
      if (isVerbose) {
        PartitionSdInfo partSd = update.getKey();
        long newCdId = update.getValue();
        result.addDetail(String.format("table %s: partition %d CD %d -> %d",
           table.tableName, partSd.partId, partSd.cdId, newCdId));
      }
    }
    if (isDryRun) {
      return null;
    }
    List<PartitionSchemaSnapshot> schemaSnapshots = capturePartitionSchemas(partSdUpdates);
    applySchemaChanges(partSdUpdates, result);
    return new PostCommitValidation(schemaSnapshots);
  }

  private void applySchemaChanges(List<Map.Entry<PartitionSdInfo, Long>> partSdUpdates,
      MetaToolObjectStore.DedupColumnsResult result) {
    Set<Long> replacedCdIds = new HashSet<>();
    Map<Long, MColumnDescriptor> newCDs = new HashMap<>();
    for (Map.Entry<PartitionSdInfo, Long> update : partSdUpdates) {
      PartitionSdInfo partSd = update.getKey();
      long newCdId = update.getValue();
      MColumnDescriptor canonicalCd =
          newCDs.computeIfAbsent(newCdId, id -> pm.getObjectById(MColumnDescriptor.class, id));
      partSd.sd.setCD(canonicalCd);
      replacedCdIds.add(partSd.cdId);
    }
    result.addColumnDescriptorsRemoved(deleteUnusedColumnDescriptors(pm, replacedCdIds));
  }

  private List<Map.Entry<PartitionSdInfo, Long>> buildPartitionUpdates(
      List<PartitionSdInfo> partitionSds, Map<Long, Long> cdRemap) {
    List<Map.Entry<PartitionSdInfo, Long>> updates = new ArrayList<>();
    if (cdRemap.isEmpty()) {
      return updates;
    }
    for (PartitionSdInfo partSd : partitionSds) {
      Long newCdId = cdRemap.get(partSd.cdId);
      if (newCdId != null && !newCdId.equals(partSd.cdId)) {
        updates.add(Map.entry(partSd, newCdId));
      }
    }
    return updates;
  }

  private long pickCanonicalCdId(Set<Long> group, long tableCdId, List<PartitionSdInfo> partitionSds) {
    if (group.contains(tableCdId)) {
      return tableCdId;
    }
    Map<Long, Long> usageCount = new HashMap<>();
    for (PartitionSdInfo partSd : partitionSds) {
      if (group.contains(partSd.cdId)) {
        usageCount.merge(partSd.cdId, 1L, Long::sum);
      }
    }
    return group.stream()
        .max((a, b) -> {
          int usageCompare = Long.compare(usageCount.getOrDefault(a, 0L), usageCount.getOrDefault(b, 0L));
          return usageCompare != 0 ? usageCompare : Long.compare(b, a);
        })
        .orElse(group.iterator().next());
  }

  private List<Long> findPartitionedTables(String catalogFilter, String dbFilter, String tableFilter) {
    StringBuilder filter = new StringBuilder();
    List<String> parameterVals = new ArrayList<>();
    if (!isEmpty(catalogFilter)) {
      appendPatternCondition(filter, "table.database.catalogName", catalogFilter, parameterVals);
    }
    if (!isEmpty(dbFilter)) {
      appendPatternCondition(filter, "table.database.name", dbFilter, parameterVals);
    }
    if (!isEmpty(tableFilter)) {
      appendPatternCondition(filter, "table.tableName", tableFilter, parameterVals);
    }

    Query query = !filter.isEmpty() ?
        pm.newQuery(MPartition.class, filter.toString()) :
        pm.newQuery(MPartition.class);
    query.setResult("DISTINCT this.table.id");
    boolean success = false;
    List<Long> tables = new ArrayList<>();
    store.openTransaction();
    try {
      tables.addAll((List<Long>) query.executeWithArray(parameterVals.toArray(new String[0])));
      success = store.commitTransaction();
    } catch (Throwable e) {
      e.printStackTrace(System.err);
      throw new RuntimeException(e);
    } finally {
      query.closeAll();
      if (!success) {
        store.rollbackTransaction();
      }
    }
    return tables;
  }

  private List<PartitionSdInfo> loadPartitionStorageDescriptors(long tableId) throws MetaException {
    Query query = pm.newQuery(MPartition.class, "table.id == tblId");
    query.declareParameters("long tblId");
    query.setResult("this.id, this.sd");
    List<PartitionSdInfo> partitionSds = new ArrayList<>();
    try {
      List<Object[]> results = (List<Object[]>) query.execute(tableId);
      if (results == null) {
        return partitionSds;
      }
      for (Object[] row : results) {
        long partId = MetastoreDirectSqlUtils.extractSqlLong(row[0]);
        MStorageDescriptor sd = (MStorageDescriptor) row[1];
        if (sd == null) {
          continue;
        }
        pm.retrieve(sd.getCD());
        partitionSds.add(new PartitionSdInfo(partId, sd, sd.getCD().getId()));
      }
    } finally {
      query.closeAll();
    }
    return partitionSds;
  }

  private List<PartitionSchemaSnapshot> capturePartitionSchemas(List<Map.Entry<PartitionSdInfo, Long>> updates) {
    List<PartitionSchemaSnapshot> snapshots = new ArrayList<>(updates.size());
    for (Map.Entry<PartitionSdInfo, Long> update: updates) {
      PartitionSdInfo psi = update.getKey();
      snapshots.add(new PartitionSchemaSnapshot(
          psi.partId,
          update.getValue(),
          convertToFieldSchemas(psi.sd.getCD().getCols())));
    }
    return snapshots;
  }

  public void validateTableAfterDedup(TableInfo table, List<PartitionSchemaSnapshot> before)
      throws MetaException {
    List<PartitionSchemaSnapshot> partitions =
        loadPartitionCdRefs(before.stream().map(b -> b.partitionId).toList());
    if (partitions.size() != before.size()) {
      throw new MetaException(String.format(
          "Post-commit validation failed for %s: expected %d partitions, found %d",
          table, before.size(), partitions.size()));
    }
    Map<Long, PartitionSchemaSnapshot> beforeByPartitionId = new HashMap<>();
    for (PartitionSchemaSnapshot snapshot : before) {
      beforeByPartitionId.put(snapshot.partitionId(), snapshot);
    }
    for (PartitionSchemaSnapshot partition : partitions) {
      PartitionSchemaSnapshot snapshot = beforeByPartitionId.get(partition.partitionId());
      if (snapshot == null) {
        throw new MetaException(String.format(
            "Post-commit validation failed for %s: unexpected partition id %d",
            table, partition.partitionId()));
      }
      if (partition.cdId() != snapshot.cdId()) {
        throw new MetaException(String.format(
            "Post-commit validation failed for %s: partition %d still references CD %d, expected canonical CD %d",
            table, partition.partitionId(), partition.cdId(), snapshot.cdId()));
      }
      List<FieldSchema> expected = snapshot.columns();
      List<FieldSchema> actual = partition.columns();
      if (!Objects.equals(expected, actual)) {
        throw new MetaException(String.format(
            "Post-commit validation failed for %s: column schema changed for partition %d",
            table, partition.partitionId()));
      }
    }
  }

  private List<PartitionSchemaSnapshot> loadPartitionCdRefs(List<Long> partIds) throws MetaException {
    if (partIds.isEmpty()) {
      return new ArrayList<>();
    }
    int batchSize = MetastoreConf.getIntVar(store.getConf(),
        MetastoreConf.ConfVars.RAWSTORE_PARTITION_BATCH_SIZE);
    Batchable<Long, PartitionSchemaSnapshot> batch = new Batchable<>() {
      @Override
      public List<PartitionSchemaSnapshot> run(List<Long> batchPartIds) throws Exception {
        Query query = pm.newQuery(MPartition.class, "partIds.contains(id)");
        query.declareParameters("java.util.Collection partIds");
        query.setResult("this.id, this.sd");
        addQueryAfterUse(query);
        List<PartitionSchemaSnapshot> partitions = new ArrayList<>();
        boolean success = false;
        store.openTransaction();
        try {
          List<Object[]> results = (List<Object[]>) query.execute(batchPartIds);
          if (results != null) {
            for (Object[] row : results) {
              long partId = MetastoreDirectSqlUtils.extractSqlLong(row[0]);
              MStorageDescriptor sd = (MStorageDescriptor) row[1];
              if (sd == null) {
                continue;
              }
              pm.retrieve(sd.getCD());
              partitions.add(new PartitionSchemaSnapshot(partId, sd.getCD().getId(),
                  convertToFieldSchemas(sd.getCD().getCols())));
            }
          }
          success = store.commitTransaction();
        } finally {
          if (!success) {
            store.rollbackTransaction();
          }
        }
        return partitions;
      }
    };
    try {
      return Batchable.runBatched(batchSize, partIds, batch);
    } finally {
      batch.closeAllQueries();
    }
  }

  private Map<List<FieldSchema>, Set<Long>> groupByColumnSchema(Set<Long> cdIds) {
    Map<List<FieldSchema>, Set<Long>> groups = new HashMap<>();
    for (Long cdId : cdIds) {
      MColumnDescriptor cd = pm.getObjectById(MColumnDescriptor.class, cdId);
      if (cd != null) {
        pm.retrieve(cd);
        List<FieldSchema> schemas = convertToFieldSchemas(cd.getCols());
        groups.computeIfAbsent(schemas, k -> new HashSet<>()).add(cdId);
        schemas = null;
      }
    }
    return groups;
  }

  private int deleteUnusedColumnDescriptors(PersistenceManager pm, Set<Long> candidateCdIds) {
    int removed = 0;
    for (long cdId : candidateCdIds) {
      MColumnDescriptor cd = pm.getObjectById(MColumnDescriptor.class, cdId);
      if (cd == null || hasRemainingCDReference(pm, cd)) {
        continue;
      }
      removeConstraintsForCd(cd);
      pm.retrieve(cd);
      pm.deletePersistent(cd);
      removed++;
    }
    return removed;
  }

  /** Same constraint cleanup as {@code TableStoreImpl.removeUnusedColumnDescriptor}. */
  private void removeConstraintsForCd(MColumnDescriptor cd) {
    Query query = pm.newQuery(MConstraint.class, "parentColumn == inCD || childColumn == inCD");
    query.declareParameters("MColumnDescriptor inCD");
    try {
      List<MConstraint> constraints = (List<MConstraint>) query.execute(cd);
      if (CollectionUtils.isNotEmpty(constraints)) {
        pm.deletePersistentAll(constraints);
      }
    } finally {
      query.closeAll();
    }
  }

  public record TableInfo(long tableId, long tableCdId, TableName tableName) {
    @Override
    public String toString() {
      return tableName.toString();
    }
  }

  private record PartitionSdInfo(long partId, MStorageDescriptor sd, long cdId) {
  }

  public record PartitionSchemaSnapshot(long partitionId, long cdId, List<FieldSchema> columns) {
  }

  private record PostCommitValidation(List<PartitionSchemaSnapshot> schemaSnapshots) {
  }
}
