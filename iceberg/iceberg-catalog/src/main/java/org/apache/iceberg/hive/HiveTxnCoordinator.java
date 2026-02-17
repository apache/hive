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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.TxnCoordinator;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableParamsUpdate;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HiveTransaction;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;
import org.apache.thrift.TException;

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

/**
 * Transaction coordinator that aggregates Iceberg table commits and performs atomic HMS updates
 * across multiple tables using {@code updateTableParams}.
 */
public class HiveTxnCoordinator implements TxnCoordinator {

  private final Configuration conf;
  private final IMetaStoreClient msClient;

  private final Map<String, HiveTransaction> stagedUpdates = Maps.newConcurrentMap();

  public HiveTxnCoordinator(Configuration conf, IMetaStoreClient msClient) {
    this.conf = conf;
    this.msClient = msClient;
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
    return stagedUpdates.computeIfAbsent(
        table.name(), ignored -> new HiveTransaction(table, ops));
  }

  public Transaction getTransaction(org.apache.iceberg.Table table) {
    return stagedUpdates.get(table.name());
  }

  @Override
  public synchronized void commit() throws TException {
    if (stagedUpdates.isEmpty()) {
      return;
    }

    List<Map.Entry<String, HiveTransaction>> updates = Lists.newArrayList(stagedUpdates.entrySet());
    updates.sort(Map.Entry.comparingByKey());

    TableMetadata base = updates.getFirst().getValue()
        .startMetadata();

    try {
      Tasks.foreach(1)
          .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
          .exponentialBackoff(
              base.propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
              base.propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
              base.propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
              2.0 /* exponential */)
          .onlyRetryOn(CommitFailedException.class)
          .run(i -> doCommit(updates));

    } catch (CommitStateUnknownException e) {
      throw MetaStoreUtils.newMetaException(e);

    } catch (ValidationException | CommitFailedException e) {
      // All retries exhausted — clean up manifests, metadata files, and uncommitted files
      cleanUpOnCommitFailure(e);
      throw MetaStoreUtils.newMetaException(e);

    } catch (RuntimeException e) {
      cleanUpOnCommitFailure(e);
      throw e;

    } finally {
      clearState();
    }
  }

  @Override
  public synchronized void rollback() {
    clearState();
  }

  @Override
  public boolean hasPendingWork() {
    return !stagedUpdates.isEmpty();
  }

  private void doCommit(List<Map.Entry<String, HiveTransaction>> updates) {
    List<TableParamsUpdate> payload = Lists.newArrayList();
    List<HiveLock> locks = Lists.newArrayList();

    try {
      for (Map.Entry<String, HiveTransaction> entry : updates) {
        HiveTransaction hiveTxn = entry.getValue();

        // 1. WRITE: stage transaction (writes metadata file only)
        hiveTxn.commitTransaction();

        TableMetadata base = hiveTxn.startMetadata();
        HiveTableOperations ops = hiveTxn.ops();

        // 2. LOCK
        HiveLock lock = ops.lockObject(base);
        lock.lock();
        locks.add(lock);

        // 3. VERIFY under lock: refresh from HMS via original ops
        TableMetadata current = ops.refresh();

        if (!Objects.equals(base.metadataFileLocation(), current.metadataFileLocation())) {
          throw new CommitFailedException(
              "Base metadata location '%s' is not same as the current table metadata location '%s' for %s",
              base.metadataFileLocation(), current.metadataFileLocation(), entry.getKey());
        }

        // 4. Build payload using staged metadata
        TableParamsUpdate paramsUpdate = buildTableParamsUpdate(
            base, hiveTxn.currentMetadata(), ops, hiveTxn.stagingOps().metadataLocation());
        payload.add(paramsUpdate);
      }

      // 5. PERSIST: batch commit (CAS guaranteed to pass — verified under lock)
      locks.forEach(HiveLock::ensureActive);
      msClient.updateTableParams(payload);

      // 6. Verify locks still active after persist — if lost, commit state is unknown
      try {
        locks.forEach(HiveLock::ensureActive);
      } catch (LockException le) {
        throw new CommitStateUnknownException(
            "Failed to heartbeat for hive lock while committing changes. " +
            "This can lead to a concurrent commit attempt be able to overwrite this commit. " +
            "Please check the commit history. If you are running into this issue, try reducing " +
            "iceberg.hive.lock-heartbeat-interval-ms.",
            le);
      }

      releaseLocks(locks);

    } catch (CommitStateUnknownException e) {
      // Commit may have succeeded — do NOT clean up metadata/manifests, only release locks
      releaseLocks(locks);
      throw e;

    } catch (LockException e) {
      // Lock acquisition or pre-persist ensureActive failed — safe to retry
      releaseLocks(locks);
      throw new CommitFailedException(e);

    } catch (TException e) {
      if (isCasFailure(e)) {
        releaseLocks(locks);
        throw new CommitFailedException(e,
            "The table %s.%s has been modified concurrently",
            payload.getLast().getDb_name(), payload.getLast().getTable_name());
      }
      // Non-CAS TException from updateTableParams — we can't tell if the batch update was applied
      releaseLocks(locks);
      throw new CommitStateUnknownException(e);

    } catch (RuntimeException e) {
      releaseLocks(locks);
      throw e;
    }
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

    populateStatsState(newMetadata, tbl);

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

  private static void populateStatsState(TableMetadata metadata, Table tbl) {
    if (metadata.spec().isUnpartitioned() || !metadata.partitionStatisticsFiles().isEmpty()) {
      StatsSetupConst.setBasicStatsState(tbl.getParameters(), StatsSetupConst.TRUE);
    }
    List<String> colNames = getStatsColumnNames(metadata);
    if (!colNames.isEmpty()) {
      StatsSetupConst.setColumnStatsState(tbl.getParameters(), colNames);
    }
  }

  private static List<String> getStatsColumnNames(TableMetadata metadata) {
    // Find the first statistics file that contains ColumnStatisticsObj blobs.
    return metadata.statisticsFiles().stream()
        .filter(sf -> sf.blobMetadata().stream()
            .anyMatch(blob -> ColumnStatisticsObj.class.getSimpleName().equals(blob.type())))
        .findFirst()
        .map(sf -> {
          // Unpartitioned: each blob = one column, need all blobs' fields.
          // Partitioned: first blob has ALL column field IDs.
          if (metadata.spec().isUnpartitioned()) {
            return sf.blobMetadata().stream()
                .flatMap(blob -> blob.fields().stream())
                .map(fieldId -> metadata.schema().findColumnName(fieldId))
                .toList();
          }
          return sf.blobMetadata().getFirst().fields().stream()
              .map(fieldId -> metadata.schema().findColumnName(fieldId))
              .toList();
        })
        .orElse(List.of());
  }

  private static boolean isCasFailure(TException ex) {
    return ex.getMessage() != null &&
        ex.getMessage().contains("The table has been modified. The parameter value for key '" +
            HiveTableOperations.METADATA_LOCATION_PROP +
            "' is");
  }

  private void releaseLocks(List<HiveLock> locks) {
    locks.forEach(HiveLock::unlock);
  }

  private void cleanUpOnCommitFailure(Exception ex) {
    stagedUpdates.values().forEach(txn -> {
      if (!txn.ops().requireStrictCleanup() || ex instanceof CleanableFailure) {
        txn.cleanUpOnCommitFailure();
      }
    });
  }

  private void clearState() {
    stagedUpdates.clear();
  }

}
