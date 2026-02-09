/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hive;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.TxnCoordinator;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableParamsUpdate;
import org.apache.iceberg.BaseMetastoreOperations;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HiveTransaction;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.thrift.TException;

/**
 * Transaction coordinator that aggregates Iceberg table commits and performs atomic HMS updates
 * across multiple tables using {@code updateTableParams}.
 */
public final class HiveTxnCoordinator implements TxnCoordinator {

  private final Configuration conf;
  private final IMetaStoreClient msClient;

  private final Map<String, HiveTransaction> stagedCommits = Maps.newConcurrentMap();

  public HiveTxnCoordinator(Configuration conf, IMetaStoreClient msClient) {
    this.conf = conf;
    this.msClient = msClient;
  }

  @Override
  public boolean hasPendingWork() {
    return !stagedCommits.isEmpty();
  }

  public Transaction getOrCreateTransaction(org.apache.iceberg.Table table) {
    Transaction txn = getTransaction(table);
    if (txn != null) {
      return txn;
    }
    if (!(table instanceof BaseTable baseTable &&
        baseTable.operations() instanceof HiveTableOperations ops)) {
      return table.newTransaction();
    }
    return stagedCommits.computeIfAbsent(
        table.name(), ignored -> new HiveTransaction(table, ops));
  }

  public Transaction getTransaction(org.apache.iceberg.Table table) {
    return stagedCommits.get(table.name());
  }

  @Override
  public synchronized void commit() throws TException {
    if (stagedCommits.isEmpty()) {
      return;
    }

    // Sort commits by table name for deterministic ordering
    List<Map.Entry<String, HiveTransaction>> updates = Lists.newArrayList(stagedCommits.entrySet());
    updates.sort(Map.Entry.comparingByKey());

    attemptCommit(updates);
  }

  @Override
  public synchronized void rollback() {
    clearState();
  }

  private void clearState() {
    stagedCommits.clear();
  }

  private TableParamsUpdate buildTableParamsUpdate(
      TableMetadata base, TableMetadata newMetadata, HiveTableOperations ops, String newMetadataLocation) {
    Set<String> removedProps =
        base.properties().keySet().stream()
            .filter(k -> !newMetadata.properties().containsKey(k))
            .collect(Collectors.toSet());

    Table tbl = new Table();
    tbl.setParameters(Maps.newHashMap());

    long maxPropSize = conf.getLong(
        HiveOperationsBase.HIVE_TABLE_PROPERTY_MAX_SIZE,
        HiveOperationsBase.HIVE_TABLE_PROPERTY_MAX_SIZE_DEFAULT);

    HMSTablePropertyHelper.updateHmsTableForIcebergTable(
        newMetadataLocation,
        tbl,
        newMetadata,
        removedProps,
        true,
        maxPropSize,
        base.metadataFileLocation());

    TableParamsUpdate newParams = new TableParamsUpdate();
    newParams.setDb_name(ops.database());
    newParams.setTable_name(ops.table());
    newParams.setParams(tbl.getParameters());

    newParams.setExpected_param_key(HiveTableOperations.METADATA_LOCATION_PROP);
    if (base.metadataFileLocation() != null) {
      newParams.setExpected_param_value(base.metadataFileLocation());
    }
    return newParams;
  }

  private void attemptCommit(List<Map.Entry<String, HiveTransaction>> updates) throws TException {
    List<TableParamsUpdate> payload = Lists.newArrayList();
    List<HiveTransaction> txns = Lists.newArrayList();
    List<HiveLock> locks = Lists.newArrayList();

    try {
      for (Map.Entry<String, HiveTransaction> entry : updates) {
        HiveTransaction hiveTxn = entry.getValue();

        // Stage the transaction and track it for potential reset on retry
        hiveTxn.commitTransaction();
        txns.add(hiveTxn);

        TableMetadata base = hiveTxn.startMetadata();
        TableMetadata newMetadata = hiveTxn.currentMetadata();
        HiveTableOperations ops = hiveTxn.ops();

        String newMetadataFileLocation = hiveTxn.stagingOps().metadataLocation();

        // Acquire lock - if this fails, txn is already in list for cleanup
        HiveLock lock = ops.lockObject(base);
        lock.lock();
        locks.add(lock);

        // Build and add the HMS update payload
        TableParamsUpdate paramsUpdate = buildTableParamsUpdate(base, newMetadata, ops, newMetadataFileLocation);
        payload.add(paramsUpdate);
      }

      // Ensure all locks are active
      locks.forEach(HiveLock::ensureActive);

      msClient.updateTableParams(payload);

      // Success - release locks and clear state
      releaseLocks(locks);
      clearState();

    } catch (Exception e) {
      cleanupFailedCommit(txns, locks);
      throw e;
    }
  }

  private void cleanupFailedCommit(List<HiveTransaction> txns, List<HiveLock> locks) {
    cleanupMetadata(txns);
    releaseLocks(locks);
    clearState();
  }

  private void cleanupMetadata(List<HiveTransaction> txns) {
    txns.forEach(txn -> {
      String metadataLocation = txn.stagingOps().metadataLocation();
      if (metadataLocation != null) {
        HiveOperationsBase.cleanupMetadata(
            txn.ops().io(),
            BaseMetastoreOperations.CommitStatus.FAILURE.name(),
            metadataLocation);
      }
    });
  }

  private void releaseLocks(List<HiveLock> locks) {
    locks.forEach(HiveLock::unlock);
  }

}
