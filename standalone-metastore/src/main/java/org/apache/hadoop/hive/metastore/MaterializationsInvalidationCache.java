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
package org.apache.hadoop.hive.metastore;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.api.BasicTxnInfo;
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * This cache keeps information in memory about the table modifications so materialized views
 * can verify their invalidation time, i.e., the moment after materialization on which the
 * first transaction to the tables they used happened. This information is kept in memory
 * to check the invalidation quickly. However, we store enough information in the metastore
 * to bring this cache up if the metastore is restarted or would crashed. This cache lives
 * in the metastore server.
 */
public final class MaterializationsInvalidationCache {

  private static final Logger LOG = LoggerFactory.getLogger(MaterializationsInvalidationCache.class);

  /* Singleton */
  private static final MaterializationsInvalidationCache SINGLETON = new MaterializationsInvalidationCache();

  /* Key is the database name. Each value is a map from the unique view qualified name to
   * the materialization invalidation info. This invalidation object contains information
   * such as the tables used by the materialized view or the invalidation time, i.e., first
   * modification of the tables used by materialized view after the view was created. */
  private final ConcurrentMap<String, ConcurrentMap<String, MaterializationInvalidationInfo>> materializations =
      new ConcurrentHashMap<String, ConcurrentMap<String, MaterializationInvalidationInfo>>();

  /*
   * Key is a qualified table name. The value is a (sorted) tree set (supporting concurrent
   * modifications) that will keep the modifications for a given table in the order that they
   * happen. This is useful to quickly check the invalidation time for a given materialized
   * view. 
   */
  private final ConcurrentMap<String, ConcurrentSkipListMap<Long, Long>> tableModifications =
      new ConcurrentHashMap<String, ConcurrentSkipListMap<Long, Long>>();

  /* Whether the cache has been initialized or not. */
  private boolean initialized;
  /* Store to answer calls not related to transactions. */
  private RawStore store;
  /* Store to answer calls related to transactions. */
  private TxnStore txnStore;

  private MaterializationsInvalidationCache() {
  }

  /**
   * Get instance of MaterializationsInvalidationCache.
   *
   * @return the singleton
   */
  public static MaterializationsInvalidationCache get() {
    return SINGLETON;
  }

  /**
   * Initialize the invalidation cache.
   *
   * The method is synchronized because we want to avoid initializing the invalidation cache
   * multiple times in embedded mode. This will not happen when we run the metastore remotely
   * as the method is called only once.
   */
  public synchronized void init(final RawStore store, final TxnStore txnStore) {
    this.store = store;
    this.txnStore = txnStore;

    if (!initialized) {
      this.initialized = true;
      ExecutorService pool = Executors.newCachedThreadPool();
      pool.submit(new Loader());
      pool.shutdown();
    }
  }

  private class Loader implements Runnable {
    @Override
    public void run() {
      try {
        for (String dbName : store.getAllDatabases()) {
          for (Table mv : store.getTableObjectsByName(dbName, store.getTables(dbName, null, TableType.MATERIALIZED_VIEW))) {
            addMaterializedView(mv, ImmutableSet.copyOf(mv.getCreationMetadata().getTablesUsed()), OpType.LOAD);
          }
        }
        LOG.info("Initialized materializations invalidation cache");
      } catch (Exception e) {
        LOG.error("Problem connecting to the metastore when initializing the view registry");
      }
    }
  }

  /**
   * Adds a newly created materialized view to the cache.
   *
   * @param materializedViewTable the materialized view
   * @param tablesUsed tables used by the materialized view
   */
  public void createMaterializedView(Table materializedViewTable, Set<String> tablesUsed) {
    addMaterializedView(materializedViewTable, tablesUsed, OpType.CREATE);
  }

  /**
   * Method to call when materialized view is modified.
   *
   * @param materializedViewTable the materialized view
   * @param tablesUsed tables used by the materialized view
   */
  public void alterMaterializedView(Table materializedViewTable, Set<String> tablesUsed) {
    addMaterializedView(materializedViewTable, tablesUsed, OpType.ALTER);
  }

  /**
   * Adds the materialized view to the cache.
   *
   * @param materializedViewTable the materialized view
   * @param tablesUsed tables used by the materialized view
   */
  private void addMaterializedView(Table materializedViewTable, Set<String> tablesUsed, OpType opType) {
    // We are going to create the map for each view in the given database
    ConcurrentMap<String, MaterializationInvalidationInfo> cq =
        new ConcurrentHashMap<String, MaterializationInvalidationInfo>();
    final ConcurrentMap<String, MaterializationInvalidationInfo> prevCq = materializations.putIfAbsent(
        materializedViewTable.getDbName(), cq);
    if (prevCq != null) {
      cq = prevCq;
    }
    // Start the process to add materialization to the cache
    // Before loading the materialization in the cache, we need to update some
    // important information in the registry to account for rewriting invalidation
    String txnListString = materializedViewTable.getCreationMetadata().getValidTxnList();
    if (txnListString == null) {
      // This can happen when the materialized view was created on non-transactional tables
      return;
    }
    if (opType == OpType.CREATE || opType == OpType.ALTER) {
      // You store the materialized view
      cq.put(materializedViewTable.getTableName(),
          new MaterializationInvalidationInfo(materializedViewTable, tablesUsed));
    } else {
      ValidTxnList txnList = new ValidReadTxnList(txnListString);
      for (String qNameTableUsed : tablesUsed) {
        // First we insert a new tree set to keep table modifications, unless it already exists
        ConcurrentSkipListMap<Long, Long> modificationsTree =
                new ConcurrentSkipListMap<Long, Long>();
        final ConcurrentSkipListMap<Long, Long> prevModificationsTree = tableModifications.putIfAbsent(
                qNameTableUsed, modificationsTree);
        if (prevModificationsTree != null) {
          modificationsTree = prevModificationsTree;
        }
        // If we are not creating the MV at this instant, but instead it was created previously
        // and we are loading it into the cache, we need to go through the transaction entries and
        // check if the MV is still valid.
        try {
          String[] names =  qNameTableUsed.split("\\.");
          BasicTxnInfo e = txnStore.getFirstCompletedTransactionForTableAfterCommit(
                  names[0], names[1], txnList);
          if (!e.isIsnull()) {
            modificationsTree.put(e.getTxnid(), e.getTime());
            // We do not need to do anything more for current table, as we detected
            // a modification event that was in the metastore.
            continue;
          }
        } catch (MetaException ex) {
          LOG.debug("Materialized view " +
                  Warehouse.getQualifiedName(materializedViewTable.getDbName(), materializedViewTable.getTableName()) +
                  " ignored; error loading view into invalidation cache", ex);
          return;
        }
      }
      // For LOAD, you only add it if it does exist as you might be loading an outdated MV
      cq.putIfAbsent(materializedViewTable.getTableName(),
          new MaterializationInvalidationInfo(materializedViewTable, tablesUsed));
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cached materialized view for rewriting in invalidation cache: " +
          Warehouse.getQualifiedName(materializedViewTable.getDbName(), materializedViewTable.getTableName()));
    }
  }

  /**
   * This method is called when a table is modified. That way we can keep track of the
   * invalidation for the MVs that use that table.
   */
  public void notifyTableModification(String dbName, String tableName,
      long txnId, long newModificationTime) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Notification for table {} in database {} received -> id: {}, time: {}",
          tableName, dbName, txnId, newModificationTime);
    }
    ConcurrentSkipListMap<Long, Long> modificationsTree =
        new ConcurrentSkipListMap<Long, Long>();
    final ConcurrentSkipListMap<Long, Long> prevModificationsTree =
        tableModifications.putIfAbsent(Warehouse.getQualifiedName(dbName, tableName), modificationsTree);
    if (prevModificationsTree != null) {
      modificationsTree = prevModificationsTree;
    }
    modificationsTree.put(txnId, newModificationTime);
  }

  /**
   * Removes the materialized view from the cache.
   *
   * @param materializedViewTable the materialized view to remove
   */
  public void dropMaterializedView(Table materializedViewTable) {
    dropMaterializedView(materializedViewTable.getDbName(), materializedViewTable.getTableName());
  }

  public void dropMaterializedView(String dbName, String tableName) {
    materializations.get(dbName).remove(tableName);
  }

  /**
   * Returns the materialized views in the cache for the given database.
   *
   * @param dbName the database
   * @return the collection of materialized views, or the empty collection if none
   */
  public Map<String, Materialization> getMaterializationInvalidationInfo(
      String dbName, List<String> materializationNames) {
    if (materializations.get(dbName) != null) {
      ImmutableMap.Builder<String, Materialization> m = ImmutableMap.builder();
      for (String materializationName : materializationNames) {
        MaterializationInvalidationInfo materialization =
            materializations.get(dbName).get(materializationName);
        if (materialization == null) {
          LOG.debug("Materialization {} skipped as there is no information "
              + "in the invalidation cache about it", materializationName);
          continue;
        }
        long invalidationTime = getInvalidationTime(materialization);
        // We need to check whether previous value is zero, as data modification
        // in another table used by the materialized view might have modified
        // the value too
        boolean modified = materialization.compareAndSetInvalidationTime(0L, invalidationTime);
        while (!modified) {
          long currentInvalidationTime = materialization.getInvalidationTime();
          if (invalidationTime < currentInvalidationTime) {
            // It was set by other table modification, but it was after this table modification
            // hence we need to set it
            modified = materialization.compareAndSetInvalidationTime(currentInvalidationTime, invalidationTime);
          } else {
            // Nothing to do
            modified = true;
          }
        }
        m.put(materializationName, materialization);
      }
      Map<String, Materialization> result = m.build();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Retrieved the following materializations from the invalidation cache: {}", result);
      }
      return result;
    }
    return ImmutableMap.of();
  }

  private long getInvalidationTime(MaterializationInvalidationInfo materialization) {
    String txnListString = materialization.getMaterializationTable().getCreationMetadata().getValidTxnList();
    if (txnListString == null) {
      // This can happen when the materialization was created on non-transactional tables
      return Long.MIN_VALUE;
    }

    // We will obtain the modification time as follows.
    // First, we obtain the first element after high watermark (if any)
    // Then, we iterate through the elements from min open txn till high
    // watermark, updating the modification time after creation if needed
    ValidTxnList txnList = new ValidReadTxnList(txnListString);
    long firstModificationTimeAfterCreation = 0L;
    for (String qNameTableUsed : materialization.getTablesUsed()) {
      final Long tn = tableModifications.get(qNameTableUsed)
          .higherKey(txnList.getHighWatermark());
      if (tn != null) {
        if (firstModificationTimeAfterCreation == 0L ||
            tn < firstModificationTimeAfterCreation) {
          firstModificationTimeAfterCreation = tn;
        }
      }
      // Min open txn might be null if there were no open transactions
      // when this transaction was being executed
      if (txnList.getMinOpenTxn() != null) {
        // Invalid transaction list is sorted
        int pos = 0;
        for (Map.Entry<Long, Long> t : tableModifications.get(qNameTableUsed)
                .subMap(txnList.getMinOpenTxn(), txnList.getHighWatermark()).entrySet()) {
          while (pos < txnList.getInvalidTransactions().length &&
              txnList.getInvalidTransactions()[pos] != t.getKey()) {
            pos++;
          }
          if (pos >= txnList.getInvalidTransactions().length) {
            break;
          }
          if (firstModificationTimeAfterCreation == 0L ||
              t.getValue() < firstModificationTimeAfterCreation) {
            firstModificationTimeAfterCreation = t.getValue();
          }
        }
      }
    }
    return firstModificationTimeAfterCreation;
  }

  private enum OpType {
    CREATE,
    LOAD,
    ALTER
  }

}
