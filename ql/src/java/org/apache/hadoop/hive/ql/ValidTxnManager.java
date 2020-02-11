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

package org.apache.hadoop.hive.ql;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockMode;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hive.common.util.TxnIdUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helps the Driver finding the valid transactions/write ids, and record them for the plan.
 */
class ValidTxnManager {
  private static final String CLASS_NAME = Driver.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  private final Driver driver;
  private final DriverContext driverContext;

  ValidTxnManager(Driver driver, DriverContext driverContext) {
    this.driver = driver;
    this.driverContext = driverContext;
  }

  /**
   * Checks whether txn list has been invalidated while planning the query.
   * This would happen if query requires exclusive/semi-shared lock, and there has been a committed transaction
   * on the table over which the lock is required.
   */
  boolean isValidTxnListState() throws LockException {
    // 1) Get valid txn list.
    String txnString = driverContext.getConf().get(ValidTxnList.VALID_TXNS_KEY);
    if (txnString == null) {
      return true; // Not a transactional op, nothing more to do
    }

    String currentTxnString = driverContext.getTxnManager().getValidTxns().toString();
    if (currentTxnString.equals(txnString)) {
      return true; // Still valid, nothing more to do
    }

    // 2) Get locks that are relevant:
    // - Exclusive for INSERT OVERWRITE.
    // - Semi-shared for UPDATE/DELETE.
    Set<String> nonSharedLockedTables = getNonSharedLockedTables();
    if (nonSharedLockedTables == null) {
      return true; // Nothing to check
    }

    // 3) Get txn tables that are being written
    String txnWriteIdListString = driverContext.getConf().get(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY);
    if (StringUtils.isEmpty(txnWriteIdListString)) {
      return true; // Nothing to check
    }

    return checkWriteIds(currentTxnString, nonSharedLockedTables, txnWriteIdListString);
  }

  private Set<String> getNonSharedLockedTables() {
    if (CollectionUtils.isEmpty(driver.getContext().getHiveLocks())) {
      return null; // Nothing to check
    }

    Set<String> nonSharedLockedTables = new HashSet<>();
    for (HiveLock lock : driver.getContext().getHiveLocks()) {
      if (lock.mayContainComponents()) {
        // The lock may have multiple components, e.g., DbHiveLock, hence we need to check for each of them
        for (LockComponent lockComponent : lock.getHiveLockComponents()) {
          // We only consider tables for which we hold either an exclusive or a shared write lock
          if ((lockComponent.getType() == LockType.EXCLUSIVE || lockComponent.getType() == LockType.SHARED_WRITE) &&
              lockComponent.getTablename() != null && lockComponent.getDbname() != DbTxnManager.GLOBAL_LOCKS) {
            nonSharedLockedTables.add(TableName.getDbTable(lockComponent.getDbname(), lockComponent.getTablename()));
          }
        }
      } else {
        // The lock has a single components, e.g., SimpleHiveLock or ZooKeeperHiveLock.
        // Pos 0 of lock paths array contains dbname, pos 1 contains tblname
        if ((lock.getHiveLockMode() == HiveLockMode.EXCLUSIVE || lock.getHiveLockMode() == HiveLockMode.SEMI_SHARED) &&
            lock.getHiveLockObject().getPaths().length == 2) {
          nonSharedLockedTables.add(
              TableName.getDbTable(lock.getHiveLockObject().getPaths()[0], lock.getHiveLockObject().getPaths()[1]));
        }
      }
    }
    return nonSharedLockedTables;
  }

  private boolean checkWriteIds(String currentTxnString, Set<String> nonSharedLockedTables, String txnWriteIdListString)
      throws LockException {
    ValidTxnWriteIdList txnWriteIdList = new ValidTxnWriteIdList(txnWriteIdListString);
    Map<String, Table> writtenTables = getTables(false, true);

    ValidTxnWriteIdList currentTxnWriteIds = driverContext.getTxnManager().getValidWriteIds(
        getTransactionalTables(writtenTables), currentTxnString);

    for (Map.Entry<String, Table> tableInfo : writtenTables.entrySet()) {
      String fullQNameForLock = TableName.getDbTable(tableInfo.getValue().getDbName(),
          MetaStoreUtils.encodeTableName(tableInfo.getValue().getTableName()));
      if (nonSharedLockedTables.contains(fullQNameForLock)) {
        // Check if table is transactional
        if (AcidUtils.isTransactionalTable(tableInfo.getValue())) {
          // Check that write id is still valid
          if (!TxnIdUtils.checkEquivalentWriteIds(txnWriteIdList.getTableValidWriteIdList(tableInfo.getKey()),
              currentTxnWriteIds.getTableValidWriteIdList(tableInfo.getKey()))) {
            // Write id has changed, it is not valid anymore, we need to recompile
            return false;
          }
        }
        nonSharedLockedTables.remove(fullQNameForLock);
      }
    }

    if (!nonSharedLockedTables.isEmpty()) {
      throw new LockException("Wrong state: non-shared locks contain information for tables that have not" +
          " been visited when trying to validate the locks from query tables.\n" +
          "Tables: " + writtenTables.keySet() + "\n" +
          "Remaining locks after check: " + nonSharedLockedTables);
    }

    return true; // It passes the test, it is valid
  }

  /**
   *  Write the current set of valid write ids for the operated acid tables into the configuration so
   *  that it can be read by the input format.
   */
  ValidTxnWriteIdList recordValidWriteIds() throws LockException {
    String txnString = driverContext.getConf().get(ValidTxnList.VALID_TXNS_KEY);
    if (StringUtils.isEmpty(txnString)) {
      throw new IllegalStateException("calling recordValidWritsIdss() without initializing ValidTxnList " +
          JavaUtils.txnIdToString(driverContext.getTxnManager().getCurrentTxnId()));
    }

    ValidTxnWriteIdList txnWriteIds = getTxnWriteIds(txnString);
    setValidWriteIds(txnWriteIds);

    LOG.debug("Encoding valid txn write ids info {} txnid: {}", txnWriteIds.toString(),
        driverContext.getTxnManager().getCurrentTxnId());
    return txnWriteIds;
  }

  private ValidTxnWriteIdList getTxnWriteIds(String txnString) throws LockException {
    List<String> txnTables = getTransactionalTables(getTables(true, true));
    ValidTxnWriteIdList txnWriteIds = null;
    if (driverContext.getCompactionWriteIds() != null) {
      // This is kludgy: here we need to read with Compactor's snapshot/txn rather than the snapshot of the current
      // {@code txnMgr}, in effect simulating a "flashback query" but can't actually share compactor's txn since it
      // would run multiple statements.  See more comments in {@link org.apache.hadoop.hive.ql.txn.compactor.Worker}
      // where it start the compactor txn*/
      if (txnTables.size() != 1) {
        throw new LockException("Unexpected tables in compaction: " + txnTables);
      }
      txnWriteIds = new ValidTxnWriteIdList(driverContext.getCompactorTxnId());
      txnWriteIds.addTableValidWriteIdList(driverContext.getCompactionWriteIds());
    } else {
      txnWriteIds = driverContext.getTxnManager().getValidWriteIds(txnTables, txnString);
    }
    if (driverContext.getTxnType() == TxnType.READ_ONLY && !getTables(false, true).isEmpty()) {
      throw new IllegalStateException(String.format(
          "Inferred transaction type '%s' doesn't conform to the actual query string '%s'",
          driverContext.getTxnType(), driverContext.getQueryState().getQueryString()));
    }
    return txnWriteIds;
  }

  private void setValidWriteIds(ValidTxnWriteIdList txnWriteIds) {
    driverContext.getConf().set(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY, txnWriteIds.toString());
    if (driverContext.getPlan().getFetchTask() != null) {
      // This is needed for {@link HiveConf.ConfVars.HIVEFETCHTASKCONVERSION} optimization which initializes JobConf
      // in FetchOperator before recordValidTxns() but this has to be done after locks are acquired to avoid race
      // conditions in ACID. This case is supported only for single source query.
      Operator<?> source = driverContext.getPlan().getFetchTask().getWork().getSource();
      if (source instanceof TableScanOperator) {
        TableScanOperator tsOp = (TableScanOperator)source;
        String fullTableName = AcidUtils.getFullTableName(tsOp.getConf().getDatabaseName(),
            tsOp.getConf().getTableName());
        ValidWriteIdList writeIdList = txnWriteIds.getTableValidWriteIdList(fullTableName);
        if (tsOp.getConf().isTranscationalTable() && (writeIdList == null)) {
          throw new IllegalStateException(String.format(
              "ACID table: %s is missing from the ValidWriteIdList config: %s", fullTableName, txnWriteIds.toString()));
        }
        if (writeIdList != null) {
          driverContext.getPlan().getFetchTask().setValidWriteIdList(writeIdList.toString());
        }
      }
    }
  }

  private Map<String, Table> getTables(boolean inputNeeded, boolean outputNeeded) {
    Map<String, Table> tables = new HashMap<>();
    if (inputNeeded) {
      driverContext.getPlan().getInputs().forEach(input -> addTableFromEntity(input, tables));
    }
    if (outputNeeded) {
      driverContext.getPlan().getOutputs().forEach(output -> addTableFromEntity(output, tables));
    }
    return tables;
  }

  private void addTableFromEntity(Entity entity, Map<String, Table> tables) {
    Table table;
    switch (entity.getType()) {
    case TABLE:
      table = entity.getTable();
      break;
    case PARTITION:
    case DUMMYPARTITION:
      table = entity.getPartition().getTable();
      break;
    default:
      return;
    }
    String fullTableName = AcidUtils.getFullTableName(table.getDbName(), table.getTableName());
    tables.put(fullTableName, table);
  }

  private List<String> getTransactionalTables(Map<String, Table> tables) {
    return tables.entrySet().stream()
      .filter(entry -> AcidUtils.isTransactionalTable(entry.getValue()))
      .map(Map.Entry::getKey)
      .collect(Collectors.toList());
  }
}
