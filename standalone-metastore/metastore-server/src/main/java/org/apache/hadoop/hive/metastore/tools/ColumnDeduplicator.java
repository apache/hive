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
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.metastore.RawStoreBundle;
import org.apache.hadoop.hive.metastore.model.MColumnDescriptor;
import org.apache.hadoop.hive.metastore.model.MConstraint;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.metastore.model.MStorageDescriptor;
import org.apache.hadoop.hive.metastore.model.MTable;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
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
final class ColumnDeduplicator {
  private final RawStore store;
  private final PersistenceManager pm;
  private final AtomicReference<String> progress;
  private final boolean isDryRun;
  private final boolean isVerbose;

  ColumnDeduplicator(RawStoreBundle bundle, AtomicReference<String> progress,
      boolean isDryRun, boolean isVerbose) {
    this.store = bundle.getBaseStore();
    this.pm = bundle.getPersistentManager();
    this.progress = progress;
    this.isDryRun = isDryRun;
    this.isVerbose = isVerbose;
  }

  MetaToolObjectStore.DedupColumnsResult run(String catalogFilter, String dbFilter, String tableFilter) {
    List<TableInfo> tables = findPartitionedTables(catalogFilter, dbFilter, tableFilter);
    MetaToolObjectStore.DedupColumnsResult result = new MetaToolObjectStore.DedupColumnsResult(tables.size());

    long start = System.currentTimeMillis();
    for (int i = 0; i < tables.size() && result.getException() == null; i++) {
      boolean committed = false;
      TableInfo table = tables.get(i);
      store.openTransaction();
      try {
        deduplicateTable(table, result);
        committed = store.commitTransaction();
      } catch (Exception ex) {
        result.catchException(ex);
      } finally {
        if (!committed) {
          store.rollbackTransaction();
          if (result.getException() == null) {
            result.catchException(
                new MetaException("Failed to apply column descriptor de-duplication updates for table " + table));
          }
        }
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

  private void deduplicateTable(TableInfo table, MetaToolObjectStore.DedupColumnsResult result) throws MetaException {
    List<PartitionSdInfo> partitionSds = loadPartitionStorageDescriptors(table.tableId);
    if (partitionSds.isEmpty()) {
      return;
    }

    Set<Long> cdIds = partitionSds.stream().map(p -> p.cdId).collect(Collectors.toSet());
    cdIds.add(table.tableCdId);

    Map<Long, List<FieldSchema>> cdColumns = loadColumnSchemas(cdIds);
    Map<List<FieldSchema>, List<Long>> groups = groupByColumnSchema(cdColumns);

    Map<Long, Long> cdRemap = new HashMap<>();
    for (List<Long> group : groups.values()) {
      if (group.size() <= 1) {
        continue;
      }
      long canonicalCdId = pickCanonicalCdId(new HashSet<>(group), table.tableCdId, partitionSds);
      for (long cdId : group) {
        if (cdId != canonicalCdId) {
          cdRemap.put(cdId, canonicalCdId);
        }
      }
    }

    if (cdRemap.isEmpty()) {
      return;
    }

    List<Map.Entry<PartitionSdInfo, Long>> partSdUpdates = buildPartitionUpdates(partitionSds, cdRemap);
    if (partSdUpdates.isEmpty()) {
      return;
    }

    result.incrementTablesWithDuplicates();
    for (Map.Entry<PartitionSdInfo, Long> update : partSdUpdates) {
      result.incrementStorageDescriptorsUpdated();
      if (isVerbose) {
        PartitionSdInfo partSd = update.getKey();
        long newCdId = update.getValue();
        result.addDetail(String.format("table %s.%s.%s: SD %s CD %d -> %d",
            table.catalogName, table.dbName, table.tableName,
            JDOHelper.getObjectId(partSd.sd), partSd.cdId, newCdId));
      }
    }
    if (!isDryRun) {
      applyTableChanges(partSdUpdates, result);
    } else {
      Set<Long> candidateCdIds = new HashSet<>();
      for (Map.Entry<PartitionSdInfo, Long> update : partSdUpdates) {
        candidateCdIds.add(update.getKey().cdId);
      }
      result.addColumnDescriptorsRemoved(countRemovableColumnDescriptors(candidateCdIds));
    }
  }

  private void applyTableChanges(List<Map.Entry<PartitionSdInfo, Long>> partSdUpdates,
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

  private List<TableInfo> findPartitionedTables(String catalogFilter, String dbFilter, String tableFilter) {
    StringBuilder filter = new StringBuilder();
    List<String> parameterVals = new ArrayList<>();
    if (!isEmpty(catalogFilter)) {
      appendPatternCondition(filter, "database.catalogName", catalogFilter, parameterVals);
    }
    if (!isEmpty(dbFilter)) {
      appendPatternCondition(filter, "database.name", dbFilter, parameterVals);
    }
    if (!isEmpty(tableFilter)) {
      appendPatternCondition(filter, "tableName", tableFilter, parameterVals);
    }

    Query query = filter.length() > 0 ?
        pm.newQuery(MTable.class, filter.toString()) :
        pm.newQuery(MTable.class);
    boolean success = false;
    List<TableInfo> tables = new ArrayList<>();
    store.openTransaction();
    try {
      List<MTable> mTables = (List<MTable>) query.executeWithArray(parameterVals.toArray(new String[0]));
      pm.retrieveAll(mTables);
      for (MTable mTable : mTables) {
        if (!isPartitionedTable(mTable.getId())) {
          continue;
        }
        pm.retrieve(mTable.getDatabase());
        pm.retrieve(mTable.getSd());
        pm.retrieve(mTable.getSd().getCD());
        tables.add(new TableInfo(
            mTable.getId(),
            mTable.getSd().getCD().getId(),
            mTable.getDatabase().getCatalogName(),
            mTable.getDatabase().getName(),
            mTable.getTableName()));
      }
      success = store.commitTransaction();
    } finally {
      query.closeAll();
      if (!success) {
        store.rollbackTransaction();
      }
    }
    return tables;
  }

  private boolean isPartitionedTable(long tableId) {
    Query query = pm.newQuery(MPartition.class, "table.id == tblId");
    query.declareParameters("long tblId");
    query.setRange(0L, 1L);
    try {
      List<MPartition> partitions = (List<MPartition>) query.execute(tableId);
      return partitions != null && !partitions.isEmpty();
    } finally {
      query.closeAll();
    }
  }

  private List<PartitionSdInfo> loadPartitionStorageDescriptors(long tableId) {
    Query query = pm.newQuery(MPartition.class, "table.id == tblId");
    query.declareParameters("long tblId");
    query.setResult("sd");
    List<PartitionSdInfo> partitionSds = new ArrayList<>();
    try {
      List<MStorageDescriptor> sds = (List<MStorageDescriptor>) query.execute(tableId);
      if (sds == null) {
        return partitionSds;
      }
      pm.retrieveAll(sds);
      for (MStorageDescriptor sd : sds) {
        pm.retrieve(sd.getCD());
        partitionSds.add(new PartitionSdInfo(sd, sd.getCD().getId()));
      }
    } finally {
      query.closeAll();
    }
    return partitionSds;
  }

  private Map<Long, List<FieldSchema>> loadColumnSchemas(Set<Long> cdIds) {
    Map<Long, List<FieldSchema>> result = new HashMap<>();
    for (Long cdId : cdIds) {
      MColumnDescriptor cd = pm.getObjectById(MColumnDescriptor.class, cdId);
      if (cd != null) {
        pm.retrieve(cd);
        result.put(cdId, convertToFieldSchemas(cd.getCols()));
      }
    }
    return result;
  }

  private static Map<List<FieldSchema>, List<Long>> groupByColumnSchema(Map<Long, List<FieldSchema>> cdColumns) {
    Map<List<FieldSchema>, List<Long>> groups = new HashMap<>();
    for (Map.Entry<Long, List<FieldSchema>> entry : cdColumns.entrySet()) {
      groups.computeIfAbsent(entry.getValue(), ignored -> new ArrayList<>())
          .add(entry.getKey());
    }
    return groups;
  }

  private int countRemovableColumnDescriptors(Set<Long> candidateCdIds) {
    int removable = 0;
    for (long cdId : candidateCdIds) {
      MColumnDescriptor cd = pm.getObjectById(MColumnDescriptor.class, cdId);
      if (cd != null && !hasRemainingCDReference(pm, cd)) {
        removable++;
      }
    }
    return removable;
  }

  private int deleteUnusedColumnDescriptors(PersistenceManager pm, Set<Long> candidateCdIds) {
    int removed = 0;
    for (long cdId : candidateCdIds) {
      MColumnDescriptor cd = pm.getObjectById(MColumnDescriptor.class, cdId);
      if (cd == null || hasRemainingCDReference(pm, cd)) {
        continue;
      }
      removeConstraintsForCd(pm, cd);
      pm.retrieve(cd);
      pm.deletePersistent(cd);
      removed++;
    }
    return removed;
  }

  /** Same constraint cleanup as {@code TableStoreImpl.removeUnusedColumnDescriptor}. */
  private static void removeConstraintsForCd(PersistenceManager pm, MColumnDescriptor cd) {
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

  private static final class TableInfo {
    private final long tableId;
    private final long tableCdId;
    private final String catalogName;
    private final String dbName;
    private final String tableName;

    private TableInfo(long tableId, long tableCdId, String catalogName, String dbName, String tableName) {
      this.tableId = tableId;
      this.tableCdId = tableCdId;
      this.catalogName = catalogName;
      this.dbName = dbName;
      this.tableName = tableName;
    }

    @Override
    public String toString() {
      return catalogName + "." + dbName + "." + tableName;
    }
  }

  private record PartitionSdInfo(MStorageDescriptor sd, long cdId) {

  }
}
