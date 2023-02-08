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
package org.apache.hadoop.hive.ql.stats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreThread;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.RawStoreProxy;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.StatsUpdateMode;
import org.apache.hadoop.hive.metastore.txn.TxnCommonUtils;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.DriverUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

public class StatsUpdaterThread extends Thread implements MetaStoreThread {
  public static final String SKIP_STATS_AUTOUPDATE_PROPERTY = "skip.stats.autoupdate";
  public static final String WORKER_NAME_PREFIX = "Stats updater worker ";
  private static final Logger LOG = LoggerFactory.getLogger(StatsUpdaterThread.class);

  protected Configuration conf;
  protected int threadId;
  protected AtomicBoolean stop;
  protected AtomicBoolean looped;

  private RawStore rs;
  private TxnStore txnHandler;
  /** Full tables, and partitions that currently have analyze commands queued or in progress. */
  private ConcurrentHashMap<TableName, Boolean> tablesInProgress = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, Boolean> partsInProgress = new ConcurrentHashMap<>();
  private AtomicInteger itemsInProgress = new AtomicInteger(0);

  // Configuration
  /** Whether to only update stats that already exist and are out of date. */
  private boolean isExistingOnly, areTxnStatsEnabled;
  private long noUpdatesWaitMs;
  private int batchSize;

  // Worker threads stuff
  private BlockingQueue<AnalyzeWork> workQueue;
  private Thread[] workers;

  @Override
  public void setConf(Configuration conf) {
    StatsUpdateMode mode = StatsUpdateMode.valueOf(
        MetastoreConf.getVar(conf, ConfVars.STATS_AUTO_UPDATE).toUpperCase());
    switch (mode) {
    case ALL: this.isExistingOnly = false; break;
    case EXISTING: this.isExistingOnly = true; break;
    default: throw new AssertionError("Unexpected mode " + mode);
    }
    noUpdatesWaitMs = MetastoreConf.getTimeVar(
        conf, ConfVars.STATS_AUTO_UPDATE_NOOP_WAIT, TimeUnit.MILLISECONDS);
    areTxnStatsEnabled = MetastoreConf.getBoolVar(conf, ConfVars.HIVE_TXN_STATS_ENABLED);
    batchSize = MetastoreConf.getIntVar(conf, ConfVars.BATCH_RETRIEVE_MAX);
    int workerCount = MetastoreConf.getIntVar(conf, ConfVars.STATS_AUTO_UPDATE_WORKER_COUNT);
    if (workerCount <= 0) {
      workerCount = 1;
    }
    workers = new Thread[workerCount];
    // Don't store too many items; if the queue is full we'll block the checker thread.
    // Since the worker count determines how many queries can be running in parallel, it makes
    // no sense to produce more work if the backlog is getting too long.
    workQueue = new ArrayBlockingQueue<AnalyzeWork>(workerCount * 3);
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void init(AtomicBoolean stop) throws MetaException {
    this.stop = stop;
    setPriority(MIN_PRIORITY);
    setDaemon(true);
    String user = "anonymous";
    try {
      user = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      LOG.warn("Cannot determine the current user; executing as anonymous", e);
    }
    txnHandler = TxnUtils.getTxnStore(conf);
    rs = RawStoreProxy.getProxy(conf, conf,
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.RAW_STORE_IMPL));
    for (int i = 0; i < workers.length; ++i) {
      workers[i] = new Thread(new WorkerRunnable(conf, user));
      workers[i].setDaemon(true);
      workers[i].setName(WORKER_NAME_PREFIX + i);
    }
  }

  @Override
  public void run() {
    LOG.info("Stats updater thread started");
    startWorkers();
    while (!stop.get()) {
      boolean hadUpdates = runOneIteration();
      try {
        Thread.sleep(hadUpdates ? 0 : noUpdatesWaitMs);
      } catch (InterruptedException e) {
        LOG.info("Stats updater thread was interrupted and will now exit");
        stopWorkers();
        return;
      }
    }
    stopWorkers();
    LOG.info("Stats updater thread was stopped and will now exit");
  }

  @VisibleForTesting
  void startWorkers() {
    for (int i = 0; i < workers.length; ++i) {
      LOG.info("Stats updater worker thread " + workers[i].getName() + " started");
      workers[i].start();
    }
  }

  @VisibleForTesting
  public boolean runOneIteration() {
    List<TableName> fullTableNames;
    try {
      fullTableNames = getTablesToCheck();
    } catch (Throwable t) {
      LOG.error("Stats updater thread cannot retrieve tables and will now exit", t);
      stopWorkers();
      throw new RuntimeException(t);
    }
    LOG.debug("Processing {}", fullTableNames);
    boolean hadUpdates = false;
    Map<String, Boolean> dbsToSkip = new HashMap<>();
    for (TableName fullTableName : fullTableNames) {
      try {
        List<AnalyzeWork> commands = processOneTable(fullTableName, dbsToSkip);
        hadUpdates = hadUpdates || commands != null;
        if (commands != null) {
          for (AnalyzeWork req : commands) {
            markAnalyzeInProgress(req);
            workQueue.put(req);
          }
        }
      } catch (Exception e) {
        LOG.error("Failed to process " + fullTableName + "; skipping for now", e);
      }
    }
    return hadUpdates;
  }

  private void stopWorkers() {
    for (int i = 0; i < workers.length; ++i) {
      workers[i].interrupt();
    }
  }

  private List<AnalyzeWork> processOneTable(TableName fullTableName, Map<String, Boolean> dbsToSkip)
      throws MetaException, NoSuchTxnException, NoSuchObjectException {
    if (isAnalyzeTableInProgress(fullTableName)) return null;
    String cat = fullTableName.getCat(), db = fullTableName.getDb(), tbl = fullTableName.getTable();
    String dbName = MetaStoreUtils.prependCatalogToDbName(cat,db, conf);
    if (!dbsToSkip.containsKey(dbName)) {
      dbsToSkip.put(dbName, MetaStoreUtils.checkIfDbNeedsToBeSkipped(rs.getDatabase(cat, db)));
    }
    if (dbsToSkip.get(dbName)) {
      LOG.debug("Skipping table {}", tbl);
      return null;
    }
    Table table = rs.getTable(cat, db, tbl);
    LOG.debug("Processing table {}", table);

    // Check if the table should be skipped.
    String skipParam = table.getParameters().get(SKIP_STATS_AUTOUPDATE_PROPERTY);
    if ("true".equalsIgnoreCase(skipParam)) return null;

    // Note: ideally we should take a lock here to pretend to be a real reader.
    //       For now, this check is going to have race potential; it may run a spurious analyze.
    String writeIdString = null;
    boolean isTxn = AcidUtils.isTransactionalTable(table);
    if (isTxn) {
      if (!areTxnStatsEnabled) return null; // Skip transactional tables.
      ValidReaderWriteIdList writeIds = getWriteIds(fullTableName);
      if (writeIds == null) {
        LOG.error("Cannot get writeIds for transactional table " + fullTableName + "; skipping");
        return null;
      }
      writeIdString = writeIds.writeToString();
    }
    List<String> allCols = new ArrayList<>(table.getSd().getColsSize());
    for (FieldSchema fs : table.getSd().getCols()) {
      allCols.add(fs.getName());
    }
    Collections.sort(allCols);
    if (table.getPartitionKeysSize() == 0) {
      Map<String, String> params = table.getParameters();
      List<String> colsToUpdate = null;
      long writeId = isTxn ? table.getWriteId() : -1;
      if (isExistingOnly) {
        // Get the existing stats, including the txn state if any, to see if we need to update.
        colsToUpdate = getExistingNonPartTableStatsToUpdate(
            fullTableName, cat, db, tbl, params, writeId, allCols, writeIdString);
      } else {
        colsToUpdate = getAnyStatsToUpdate(db, tbl, allCols, params, writeId, writeIdString);
      }

      LOG.debug("Columns to update are {}; existing only: {}, out of: {} based on {}",
          colsToUpdate, isExistingOnly, allCols, params);

      if (colsToUpdate == null || colsToUpdate.isEmpty()) {
        return null; // No update necessary.
      }
      return Lists.newArrayList(new AnalyzeWork(fullTableName,
          null, null, allCols.size() == colsToUpdate.size() ? null : colsToUpdate));
    } else {
      Map<String, List<String>> partsToAnalyze = new HashMap<>();
      List<String> colsForAllParts = findPartitionsToAnalyze(
          fullTableName, cat, db, tbl, allCols, partsToAnalyze, writeIdString);
      LOG.debug("Columns to update are {} for all partitions; {} individual partitions."
          + " Existing only: {}, out of: {}", colsForAllParts, partsToAnalyze.size(),
          isExistingOnly, allCols);
      if (colsForAllParts == null && partsToAnalyze.isEmpty()) {
        return null; // No partitions need update.
      }
      if (colsForAllParts != null) {
        // We can update all partitions with a single analyze query.
        return Lists.newArrayList(new AnalyzeWork(
            fullTableName, null, buildPartColStr(table), colsForAllParts));
      }
      List<AnalyzeWork> result = new ArrayList<>(partsToAnalyze.size());
      for (Map.Entry<String, List<String>> e : partsToAnalyze.entrySet()) {
        LOG.debug("Adding analyze work for {}", e.getKey());
        result.add(new AnalyzeWork(fullTableName, e.getKey(), null, e.getValue()));
      }
      return result;
    }
  }

  private List<String> findPartitionsToAnalyze(TableName fullTableName, String cat, String db,
      String tbl, List<String> allCols, Map<String, List<String>> partsToAnalyze,
      String writeIdString) throws MetaException, NoSuchObjectException {
    // TODO: ideally when col-stats-accurate stuff is stored in some sane structure, this should
    //       retrieve partsToUpdate in a single query; no checking partition params in java.
    List<String> partNames = null;
    Map<String, List<String>> colsPerPartition = null;
    boolean isAllParts = true;
    if (isExistingOnly) {
      // Make sure the number of partitions we get, and the number of stats objects, is consistent.
      rs.openTransaction();
      boolean isOk = false;
      try {
        colsPerPartition = rs.getPartitionColsWithStats(cat, db, tbl);
        partNames = Lists.newArrayList(colsPerPartition.keySet());
        int partitionCount = rs.getNumPartitionsByFilter(cat, db, tbl, "");
        isAllParts = partitionCount == partNames.size();
        isOk = true;
      } finally {
        if (isOk) {
          rs.commitTransaction();
        } else {
          rs.rollbackTransaction();
        }
      }
    } else {
      partNames = rs.listPartitionNames(cat, db, tbl, (short) -1);
      isAllParts = true;
    }
    Table t = rs.getTable(cat, db, tbl);
    List<Partition> currentBatch = null;
    int nextBatchStart = 0, nextIxInBatch = -1, currentBatchStart = 0;
    List<String> colsToUpdateForAll = null;
    while (true) {
      if (currentBatch == null || nextIxInBatch == currentBatch.size()) {
        if (nextBatchStart >= partNames.size()) {
          break;
        }
        int nextBatchEnd = Math.min(partNames.size(), nextBatchStart + this.batchSize);
        List<String> currentNames = partNames.subList(nextBatchStart, nextBatchEnd);
        currentBatchStart = nextBatchStart;
        nextBatchStart = nextBatchEnd;
        try {
          currentBatch = rs.getPartitionsByNames(cat, db, tbl, currentNames);
        } catch (NoSuchObjectException e) {
          LOG.error("Failed to get partitions for " + fullTableName + ", skipping some partitions", e);
          currentBatch = null;
          continue;
        }
        nextIxInBatch = 0;
      }
      int currentIxInBatch = nextIxInBatch++;
      Partition part = currentBatch.get(currentIxInBatch);
      String partName = Warehouse.makePartName(t.getPartitionKeys(), part.getValues());
      LOG.debug("Processing partition ({} in batch), {}", currentIxInBatch, partName);

      // Skip the partitions in progress, and the ones for which stats update is disabled.
      // We could filter the skipped partititons out as part of the initial names query,
      // but we assume it's extremely rare for individual partitions.
      Map<String, String> params = part.getParameters();
      String skipParam = params.get(SKIP_STATS_AUTOUPDATE_PROPERTY);
      if (isAnalyzePartInProgress(fullTableName, partName) || "true".equalsIgnoreCase(skipParam)) {
        if (isAllParts) {
          addPreviousPartitions(t, partNames, currentBatchStart, currentBatch, currentIxInBatch,
              colsToUpdateForAll, partsToAnalyze);
        }
        isAllParts = false;
        continue;
      }

      // Find which columns we need to update for this partition, if any.
      List<String> colsToMaybeUpdate = allCols;
      if (isExistingOnly) {
        colsToMaybeUpdate = colsPerPartition.get(partName);
        Collections.sort(colsToMaybeUpdate);
      }
      List<String> colsToUpdate = getAnyStatsToUpdate(db, tbl, colsToMaybeUpdate, params,
          writeIdString == null ? -1 : part.getWriteId(), writeIdString);

      LOG.debug("Updating {} based on {} and {}", colsToUpdate, colsToMaybeUpdate, params);

      if (colsToUpdate == null || colsToUpdate.isEmpty()) {
        if (isAllParts) {
          addPreviousPartitions(t, partNames, currentBatchStart, currentBatch, currentIxInBatch,
              colsToUpdateForAll, partsToAnalyze);
        }
        isAllParts = false;
        continue;
      }

      // If issuing a query for all partitions, verify that we need update the same columns.
      // TODO: for non columnar we don't need to do this... might as well update all stats.
      if (isAllParts) {
        List<String> newCols = verifySameColumnsForAllParts(colsToUpdateForAll, colsToUpdate);
        if (newCols == null) {
          isAllParts = false;
          addPreviousPartitions(t, partNames, currentBatchStart, currentBatch, currentIxInBatch,
              colsToUpdateForAll, partsToAnalyze);
        } else if (colsToUpdateForAll == null) {
          colsToUpdateForAll = newCols;
        }
      }

      if (!isAllParts) {
        LOG.trace("Adding {}, {}", partName, colsToUpdate);
        partsToAnalyze.put(partName, colsToUpdate);
      }
    }
    return isAllParts ? colsToUpdateForAll : null;
  }

  private List<String> verifySameColumnsForAllParts(
      List<String> colsToUpdateForAll, List<String> colsToUpdate) {
    if (colsToUpdateForAll == null) {
      return colsToUpdate;
    }
    if (colsToUpdate.size() != colsToUpdateForAll.size()) {
      return null;
    }
    // Assumes the lists are sorted.
    for (int i = 0; i < colsToUpdateForAll.size(); ++i) {
      if (!colsToUpdate.get(i).equals(colsToUpdateForAll.get(i))) {
        return null;
      }
    }
    return colsToUpdateForAll;
  }

  private void addPreviousPartitions(Table t, List<String> allPartNames,
      int currentBatchStart, List<Partition> currentBatch, int currentIxInBatch,
      List<String> cols, Map<String, List<String>> partsToAnalyze) throws MetaException {
    // Add all the names for previous batches.
    for (int i = 0; i < currentBatchStart; ++i) {
      LOG.trace("Adding previous {}, {}", allPartNames.get(i), cols);
      partsToAnalyze.put(allPartNames.get(i), cols);
    }
    // Current match may be out of order w.r.t. the global name list, so add specific parts.
    for (int i = 0; i < currentIxInBatch; ++i) {
      String name = Warehouse.makePartName(t.getPartitionKeys(), currentBatch.get(i).getValues());
      LOG.trace("Adding previous {}, {}", name, cols);
      partsToAnalyze.put(name, cols);
    }
  }

  private String buildPartColStr(Table table) {
    String partColStr = "";
    for (int i = 0; i < table.getPartitionKeysSize(); ++i) {
      if (i != 0) {
        partColStr += ",";
      }
      partColStr += table.getPartitionKeys().get(i).getName();
    }
    return partColStr;
  }

  private List<String> getExistingNonPartTableStatsToUpdate(TableName fullTableName,
      String cat, String db, String tbl, Map<String, String> params, long statsWriteId,
      List<String> allCols, String writeIdString) throws MetaException {
    ColumnStatistics existingStats = null;
    try {
      // Note: this should NOT do txn verification - we want to get outdated stats, to
      //       see if we need to update anything.
      existingStats = rs.getTableColumnStatistics(cat, db, tbl, allCols, Constants.HIVE_ENGINE);
    } catch (NoSuchObjectException e) {
      LOG.error("Cannot retrieve existing stats, skipping " + fullTableName, e);
      return null;
    }
    // TODO: we should probably skip updating if writeId is from an active txn
    boolean isTxnValid = (writeIdString == null) || ObjectStore.isCurrentStatsValidForTheQuery(
        params, statsWriteId, writeIdString, false);
    return getExistingStatsToUpdate(existingStats, params, isTxnValid);
  }

  private List<String> getExistingStatsToUpdate(
      ColumnStatistics existingStats, Map<String, String> params, boolean isTxnValid) {
    boolean hasAnyAccurate = isTxnValid && StatsSetupConst.areBasicStatsUptoDate(params);
    List<String> colsToUpdate = new ArrayList<>();
    for (ColumnStatisticsObj obj : existingStats.getStatsObj()) {
      String col = obj.getColName();
      if (!hasAnyAccurate || !StatsSetupConst.areColumnStatsUptoDate(params, col)) {
        colsToUpdate.add(col);
      }
    }
    return colsToUpdate;
  }

  private List<String> getAnyStatsToUpdate(String db, String tbl, List<String> allCols,
      Map<String, String> params, long statsWriteId, String writeIdString) throws MetaException {
    // Note: we only run "for columns" command and assume no basic stats means no col stats.
    if (!StatsSetupConst.areBasicStatsUptoDate(params)) {
      return allCols;
    }
    // TODO: we should probably skip updating if writeId is from an active txn
    if (writeIdString != null && !ObjectStore.isCurrentStatsValidForTheQuery(
        params, statsWriteId, writeIdString, false)) {
      return allCols;
    }
    List<String> colsToUpdate = new ArrayList<>();
    for (String col : allCols) {
      if (!StatsSetupConst.areColumnStatsUptoDate(params, col)) {
        colsToUpdate.add(col);
      }
    }
    return colsToUpdate;
  }

  private List<TableName> getTablesToCheck() throws MetaException, NoSuchObjectException {
    if (isExistingOnly) {
      try {
        return rs.getTableNamesWithStats();
      } catch (Exception ex) {
        LOG.error("Error from getTablesWithStats, getting all the tables", ex);
      }
    }
    return rs.getAllTableNamesForStats();
  }

  private ValidReaderWriteIdList getWriteIds(
      TableName fullTableName) throws NoSuchTxnException, MetaException {
    // TODO: acid utils don't support catalogs
    GetValidWriteIdsRequest req = new GetValidWriteIdsRequest(
        Lists.newArrayList(fullTableName.getDbTable()));
    return TxnCommonUtils.createValidReaderWriteIdList(
        txnHandler.getValidWriteIds(req).getTblValidWriteIds().get(0));
  }


  private void markAnalyzeInProgress(AnalyzeWork req) {
    if (req.partName == null) {
      Boolean old = tablesInProgress.putIfAbsent(req.tableName, true);
      if (old != null) {
        throw new AssertionError("The table was added to progress twice: " + req.tableName);
      }
    } else {
      String partName = req.makeFullPartName();
      Boolean old = partsInProgress.putIfAbsent(partName, true);
      if (old != null) {
        throw new AssertionError("The partition was added to progress twice: " + partName);
      }
    }
    itemsInProgress.incrementAndGet();
  }

  private void markAnalyzeDone(AnalyzeWork req) {
    if (req.partName == null) {
      Boolean old = tablesInProgress.remove(req.tableName);
      if (old == null) {
        throw new AssertionError("The table was not in progress: " + req.tableName);
      }
    } else {
      String partName = req.makeFullPartName();
      Boolean old = partsInProgress.remove(partName);
      if (old == null) {
        throw new AssertionError("Partition was not in progress: " + partName);
      }
    }
    // This is used for tests where there's always just one batch of work and we do the
    // checks after the batch, so the check will only come at the end of queueing.
    int remaining = itemsInProgress.decrementAndGet();
    if (remaining == 0) {
      synchronized (itemsInProgress) {
        itemsInProgress.notifyAll();
      }
    }
  }

  private boolean isAnalyzeTableInProgress(TableName fullTableName) {
    return tablesInProgress.containsKey(fullTableName);
  }

  private boolean isAnalyzePartInProgress(TableName tableName, String partName) {
    return partsInProgress.containsKey(makeFullPartName(tableName, partName));
  }

  private static String makeFullPartName(TableName tableName, String partName) {
    return tableName + "/" + partName;
  }

  private final static class AnalyzeWork {
    TableName tableName;
    String partName, allParts;
    List<String> cols;

    public AnalyzeWork(TableName tableName, String partName, String allParts, List<String> cols) {
      this.tableName = tableName;
      this.partName = partName;
      this.allParts = allParts;
      this.cols = cols;
    }

    public String makeFullPartName() {
      return StatsUpdaterThread.makeFullPartName(tableName, partName);
    }

    public String buildCommand() {
      // Catalogs cannot be parsed as part of the query. Seems to be a bug.
      String cmd = "analyze table " + tableName.getDb() + "." + tableName.getTable();
      assert partName == null || allParts == null;
      if (partName != null) {
        cmd += " partition(" + partName + ")";
      }
      if (allParts != null) {
        cmd += " partition(" + allParts + ")";
      }
      cmd += " compute statistics for columns";
      if (cols != null) {
        cmd += " " + String.join(",", cols);
      }
      return cmd;
    }

    @Override
    public String toString() {
      return "AnalyzeWork [tableName=" + tableName + ", partName=" + partName
          + ", allParts=" + allParts + ", cols=" + cols + "]";
    }
  }

  @VisibleForTesting
  public boolean runOneWorkerIteration(
      SessionState ss, String user, HiveConf conf, boolean doWait) throws InterruptedException {
    AnalyzeWork req;
    if (doWait) {
      req = workQueue.take();
    } else {
      req = workQueue.poll();
      if (req == null) {
        return false;
      }
    }
    String cmd = null;
    try {
      if (doWait) {
        SessionState.start(ss); // This is the first call, open the session
      }
      TableName tb = req.tableName;
      if (MetaStoreUtils.isDbBeingPlannedFailedOver(rs.getDatabase(tb.getCat(), tb.getDb()))) {
        LOG.info("Skipping table: {} as it belongs to database which is being failed over." + tb.getTable());
        return true;
      }
      cmd = req.buildCommand();
      LOG.debug("Running {} based on {}", cmd, req);
      DriverUtils.runOnDriver(conf, ss, cmd);
    } catch (Exception e) {
      LOG.error("Analyze command failed: " + cmd, e);
      try {
        ss.close();
      } catch (IOException e1) {
        LOG.warn("Failed to close a bad session", e1);
      } finally {
        SessionState.detachSession();
      }
    } finally {
      markAnalyzeDone(req);
    }
    return true;
  }

  public class WorkerRunnable implements Runnable {
    private final HiveConf conf;
    private final String user;

    public WorkerRunnable(Configuration conf, String user) {
      this.conf = new HiveConf(conf, HiveConf.class);
      this.user = user;
    }

    @Override
    public void run() {
      while (true) {
        // This should not start the actual Tez AM.
        SessionState ss = DriverUtils.setUpSessionState(conf, user, false);
        // Wait for the first item to arrive at the queue and process it.
        try {
          runOneWorkerIteration(ss, user, conf, true);
        } catch (InterruptedException e) {
          closeSession(ss);
          LOG.info("Worker thread was interrupted and will now exit");
          return;
        }
        // Keep draining the queue in the same session.
        try {
          while (runOneWorkerIteration(ss, user, conf, false)) {}
        } catch (InterruptedException e) {
          closeSession(ss);
          LOG.info("Worker thread was interrupted unexpectedly and will now exit");
          return;
        };
        // Close the session before we have to wait again.
        closeSession(ss);
        SessionState.detachSession();
      }
    }
  }

  private static void closeSession(SessionState ss) {
    try {
      ss.close();
    } catch (IOException e1) {
      LOG.error("Failed to close the session", e1);
    }
  }

  @VisibleForTesting
  public void waitForQueuedCommands() throws InterruptedException {
    while (itemsInProgress.get() > 0) {
      synchronized (itemsInProgress) {
        itemsInProgress.wait(100L);
      }
    }
  }

  @VisibleForTesting
  public int getQueueLength() {
    return workQueue.size();
  }
}
