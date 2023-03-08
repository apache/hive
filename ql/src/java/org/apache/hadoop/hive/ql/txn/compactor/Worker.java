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
package org.apache.hadoop.hive.ql.txn.compactor;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidCompactorWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.HiveMetaStoreUtils;
import org.apache.hadoop.hive.metastore.MetaStoreThread;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.FindNextCompactRequest;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.AcidMetricService;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.txn.TxnErrorMsg;
import org.apache.hadoop.hive.metastore.txn.TxnStatus;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hive.common.util.Ref;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * A class to do compactions.  This will run in a separate thread.  It will spin on the
 * compaction queue and look for new work to do.
 */
public class Worker extends RemoteCompactorThread implements MetaStoreThread {
  static final private String CLASS_NAME = Worker.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  private static long SLEEP_TIME_MAX;
  static private long SLEEP_TIME;

  private String workerName;
  private final CompactorFactory compactorFactory;

  public Worker() {
    compactorFactory = CompactorFactory.getInstance();
  }

  public Worker(CompactorFactory compactorFactory) {
    this.compactorFactory = compactorFactory;
  }

  static StatsUpdater statsUpdater = new StatsUpdater();

  // TODO: this doesn't check if compaction is already running (even though Initiator does but we
  //  don't go through Initiator for user initiated compactions)
  @Override
  public void run() {
    LOG.info("Starting Worker thread");
    boolean genericStats = conf.getBoolVar(HiveConf.ConfVars.HIVE_COMPACTOR_GATHER_STATS);
    boolean mrStats = conf.getBoolVar(HiveConf.ConfVars.HIVE_MR_COMPACTOR_GATHER_STATS);
    long timeout = conf.getTimeVar(HiveConf.ConfVars.HIVE_COMPACTOR_WORKER_TIMEOUT, TimeUnit.MILLISECONDS);
    long nextSleep = SLEEP_TIME;
    boolean launchedJob;
    ExecutorService executor = getTimeoutHandlingExecutor();
    try {
      do {
        long startedAt = System.currentTimeMillis();
        boolean err = false;
        launchedJob = false;
        Future<Boolean> singleRun = executor.submit(() -> findNextCompactionAndExecute(genericStats, mrStats));
        try {
          launchedJob = singleRun.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException te) {
          LOG.info("Timeout during executing compaction", te);
          // Cancel the job, and recreate the Executor as well, so we can be sure that we have an available thread
          // even if we can not interrupt the task somehow. (Trade possible resource hogging for compactor stability)
          singleRun.cancel(true);
          executor.shutdownNow();
          executor = getTimeoutHandlingExecutor();
          err = true;
        } catch (ExecutionException e) {
          LOG.info("Exception during executing compaction", e);
          err = true;
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        } catch (Throwable t) {
          err = true;
        }

        doPostLoopActions(System.currentTimeMillis() - startedAt);

        // If we didn't try to launch a job it either means there was no work to do or we got
        // here as the result of an error like communication failure with the DB, schema failures etc.  Either way we want to wait
        // a bit before, otherwise we can start over the loop immediately.
        if ((!launchedJob || err) && !stop.get()) {
          Thread.sleep(nextSleep);
        }
        //Backoff mechanism
        //Increase sleep time if error persist
        //Reset sleep time to default once error is resolved
        nextSleep = (err) ? nextSleep * 2 : SLEEP_TIME;
        if (nextSleep > SLEEP_TIME_MAX) nextSleep = SLEEP_TIME_MAX;

      } while (!stop.get());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Throwable t) {
      LOG.error("Caught an exception in the main loop of compactor worker, exiting.", t);
    } finally {
      if (Thread.currentThread().isInterrupted()) {
        LOG.info("Interrupt received, Worker is shutting down.");
      }
      executor.shutdownNow();
      if (msc != null) {
        msc.close();
      }
    }
  }

  @Override
  public void init(AtomicBoolean stop) throws Exception {
    super.init(stop);
    SLEEP_TIME = conf.getTimeVar(HiveConf.ConfVars.HIVE_COMPACTOR_WORKER_SLEEP_TIME, TimeUnit.MILLISECONDS);
    SLEEP_TIME_MAX = conf.getTimeVar(HiveConf.ConfVars.HIVE_COMPACTOR_WORKER_MAX_SLEEP_TIME, TimeUnit.MILLISECONDS);
    this.workerName = getWorkerId();
    setName(workerName);
  }

  /**
   * Determine if compaction can run in a specified directory.
   * @param ci  {@link CompactionInfo}
   * @param dir the delta directory
   * @param sd resolved storage descriptor
   * @return true, if compaction can run.
   */
  static boolean isEnoughToCompact(CompactionInfo ci, AcidDirectory dir, StorageDescriptor sd) {
    int deltaCount = dir.getCurrentDirectories().size();
    int origCount = dir.getOriginalFiles().size();

    StringBuilder deltaInfo = new StringBuilder().append(deltaCount);
    boolean isEnoughToCompact;

    if (ci.isRebalanceCompaction()) {
      //TODO: For now, we are allowing rebalance compaction regardless of the table state. Thresholds will be added later.
      return true;
    } else if (ci.isMajorCompaction()) {
      isEnoughToCompact =
          (origCount > 0 || deltaCount + (dir.getBaseDirectory() == null ? 0 : 1) > 1);

    } else {
      isEnoughToCompact = (deltaCount > 1);

      if (deltaCount == 2) {
        Map<String, Long> deltaByType = dir.getCurrentDirectories().stream().collect(Collectors
            .groupingBy(delta -> (delta
                    .isDeleteDelta() ? AcidUtils.DELETE_DELTA_PREFIX : AcidUtils.DELTA_PREFIX),
                Collectors.counting()));

        isEnoughToCompact = (deltaByType.size() != deltaCount);
        deltaInfo.append(" ").append(deltaByType);
      }
    }

    if (!isEnoughToCompact) {
      LOG.info("Not enough files in {} to compact; current base: {}, delta files: {}, originals: {}",
          sd.getLocation(), dir.getBaseDirectory(), deltaInfo, origCount);
    }
    return isEnoughToCompact;
  }

  /**
   * Check for obsolete directories, and return true if any exist and Cleaner should be
   * run. For example if we insert overwrite into a table with only deltas, a new base file with
   * the highest writeId is created so there will be no live delta directories, only obsolete
   * ones. Compaction is not needed, but the cleaner should still be run.
   *
   * @return true if cleaning is needed
   */
  public static boolean needsCleaning(AcidDirectory dir, StorageDescriptor sd) {
    int numObsoleteDirs = dir.getObsolete().size() + dir.getAbortedDirectories().size();
    boolean needsJustCleaning = numObsoleteDirs > 0;
    if (needsJustCleaning) {
      LOG.info("{} obsolete directories in {} found; marked for cleaning.", numObsoleteDirs,
          sd.getLocation());
    }
    return needsJustCleaning;
  }

  /**
   * Creates a single threaded executor used for handling timeouts.
   * The thread settings are inherited from the current thread.
   * @return Single threaded executor service to be used for timeout handling
   */
  private ExecutorService getTimeoutHandlingExecutor() {
    return Executors.newSingleThreadExecutor((r) -> {
      Thread masterThread = Thread.currentThread();
      Thread t = new Thread(masterThread.getThreadGroup(), r, masterThread.getName() + "_timeout_executor");
      t.setDaemon(masterThread.isDaemon());
      t.setPriority(masterThread.getPriority());
      return t;
    });
  }

  /**
   * Finds the next compaction and executes it. The main thread might interrupt the execution of this method
   * in case of timeout.
   * @param collectGenericStats If true then for both MR and Query based compaction the stats are regenerated
   * @param collectMrStats If true then for MR compaction the stats are regenerated
   * @return Returns true, if there was compaction in the queue, and we started working on it.
   */
  @VisibleForTesting
  protected Boolean findNextCompactionAndExecute(boolean collectGenericStats, boolean collectMrStats) {
    // Make sure nothing escapes this run method and kills the metastore at large,
    // so wrap it in a big catch Throwable statement.
    PerfLogger perfLogger = SessionState.getPerfLogger(false);
    String workerMetric = null;

    CompactionInfo ci = null;
    boolean computeStats = false;
    Table table = null;

    // If an exception is thrown in the try-with-resources block below, msc is closed and nulled, so a new instance
    // is need to be obtained here.
    if (msc == null) {
      try {
        msc = HiveMetaStoreUtils.getHiveMetastoreClient(conf);
      } catch (Exception e) {
        LOG.error("Failed to connect to HMS", e);
        return false;
      }
    }

    try (CompactionTxn compactionTxn = new CompactionTxn()) {

      FindNextCompactRequest findNextCompactRequest = new FindNextCompactRequest();
      findNextCompactRequest.setWorkerId(workerName);
      findNextCompactRequest.setWorkerVersion(runtimeVersion);
      findNextCompactRequest.setPoolName(this.getPoolName());
      ci = CompactionInfo.optionalCompactionInfoStructToInfo(msc.findNextCompact(findNextCompactRequest));
      LOG.info("Processing compaction request {}", ci);

      if (ci == null) {
        return false;
      }
      if ((runtimeVersion == null && ci.initiatorVersion != null) || (runtimeVersion != null && !runtimeVersion.equals(ci.initiatorVersion))) {
        LOG.warn("Worker and Initiator versions do not match. Worker: v{}, Initiator: v{}", runtimeVersion, ci.initiatorVersion);
      }

      if (StringUtils.isBlank(getPoolName()) && StringUtils.isNotBlank(ci.poolName)) {
        LOG.warn("A timed out copmaction pool entry ({}) is picked up by one of the default compaction pool workers.", ci);
      }
      if (StringUtils.isNotBlank(getPoolName()) && StringUtils.isNotBlank(ci.poolName) && !getPoolName().equals(ci.poolName)) {
        LOG.warn("The returned compaction request ({}) belong to a different pool. Although the worker is assigned to the {} pool," +
            " it will process the request.", ci, getPoolName());
      }
      checkInterrupt();

      if (MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_EXT_ON)) {
        workerMetric = MetricsConstants.COMPACTION_WORKER_CYCLE + "_" +
            (ci.type != null ? ci.type.toString().toLowerCase() : null);
        perfLogger.perfLogBegin(CLASS_NAME, workerMetric);
      }

      // Find the table we will be working with.
      try {
        table = resolveTable(ci);
        if (table == null) {
          ci.errorMessage = "Unable to find table " + ci.getFullTableName() + ", assuming it was dropped and moving on.";
          LOG.warn(ci.errorMessage + " Compaction info: {}", ci);
          msc.markRefused(CompactionInfo.compactionInfoToStruct(ci));
          return false;
        }
      } catch (MetaException e) {
        LOG.error("Unexpected error during resolving table. Compaction info: " + ci, e);
        ci.errorMessage = e.getMessage();
        msc.markFailed(CompactionInfo.compactionInfoToStruct(ci));
        return false;
      }

      if (ci.isRebalanceCompaction() && table.getSd().getNumBuckets() > 0) {
        LOG.error("Cannot execute rebalancing compaction on bucketed tables.");
        ci.errorMessage = "Cannot execute rebalancing compaction on bucketed tables.";
        msc.markRefused(CompactionInfo.compactionInfoToStruct(ci));
        return false;
      }

      if (!ci.type.equals(CompactionType.REBALANCE) && ci.numberOfBuckets > 0) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Only the REBALANCE compaction accepts the number of buckets clause (CLUSTERED INTO {N} BUCKETS). " +
              "Since the compaction request is {}, it will be ignored.", ci.type);
        }
      }

      checkInterrupt();

      String fullTableName = TxnUtils.getFullTableName(table.getDbName(), table.getTableName());


      // Find the partition we will be working with, if there is one.
      Partition p;
      try {
        p = resolvePartition(ci);
        if (p == null && ci.partName != null) {
          ci.errorMessage = "Unable to find partition " + ci.getFullPartitionName() + ", assuming it was dropped and moving on.";
          LOG.warn(ci.errorMessage + " Compaction info: {}", ci);
          msc.markRefused(CompactionInfo.compactionInfoToStruct(ci));
          return false;
        }
      } catch (Exception e) {
        LOG.error("Unexpected error during resolving partition.", e);
        ci.errorMessage = e.getMessage();
        msc.markFailed(CompactionInfo.compactionInfoToStruct(ci));
        return false;
      }

      checkInterrupt();

      // Find the appropriate storage descriptor
      final StorageDescriptor sd =  resolveStorageDescriptor(table, p);

      // Check that the table or partition isn't sorted, as we don't yet support that.
      if (sd.getSortCols() != null && !sd.getSortCols().isEmpty()) {
        ci.errorMessage = "Attempt to compact sorted table " + ci.getFullTableName() + ", which is not yet supported!";
        LOG.warn(ci.errorMessage + " Compaction info: {}", ci);
        msc.markRefused(CompactionInfo.compactionInfoToStruct(ci));
        return false;
      }

      if (ci.runAs == null) {
        ci.runAs = TxnUtils.findUserToRunAs(sd.getLocation(), table, conf);
      }

      checkInterrupt();

      /**
       * we cannot have Worker use HiveTxnManager (which is on ThreadLocal) since
       * then the Driver would already have the an open txn but then this txn would have
       * multiple statements in it (for query based compactor) which is not supported (and since
       * this case some of the statements are DDL, even in the future will not be allowed in a
       * multi-stmt txn. {@link Driver#setCompactionWriteIds(ValidWriteIdList, long)} */
      compactionTxn.open(ci);

      ValidTxnList validTxnList = msc.getValidTxns(compactionTxn.getTxnId());
      //with this ValidWriteIdList is capped at whatever HWM validTxnList has
      final ValidCompactorWriteIdList tblValidWriteIds =
          TxnUtils.createValidCompactWriteIdList(msc.getValidWriteIds(
              Collections.singletonList(fullTableName), validTxnList.writeToString()).get(0));
      LOG.debug("ValidCompactWriteIdList: " + tblValidWriteIds.writeToString());
      conf.set(ValidTxnList.VALID_TXNS_KEY, validTxnList.writeToString());

      ci.highestWriteId = tblValidWriteIds.getHighWatermark();
      //this writes TXN_COMPONENTS to ensure that if compactorTxnId fails, we keep metadata about
      //it until after any data written by it are physically removed
      msc.updateCompactorState(CompactionInfo.compactionInfoToStruct(ci), compactionTxn.getTxnId());

      checkInterrupt();

      // Don't start compaction or cleaning if not necessary
      if (isDynPartAbort(table, ci)) {
        msc.markCompacted(CompactionInfo.compactionInfoToStruct(ci));
        compactionTxn.wasSuccessful();
        return false;
      }
      AcidDirectory dir = getAcidStateForWorker(ci, sd, tblValidWriteIds);
      if (!isEnoughToCompact(ci, dir, sd)) {
        if (needsCleaning(dir, sd)) {
          msc.markCompacted(CompactionInfo.compactionInfoToStruct(ci));
        } else {
          // do nothing
          ci.errorMessage = "None of the compaction thresholds met, compaction request is refused!";
          LOG.debug(ci.errorMessage + " Compaction info: {}", ci);
          msc.markRefused(CompactionInfo.compactionInfoToStruct(ci));
        }
        compactionTxn.wasSuccessful();
        return false;
      }
      if (!ci.isMajorCompaction() && !isMinorCompactionSupported(table.getParameters(), dir)) {
        ci.errorMessage = "Query based Minor compaction is not possible for full acid tables having raw format " +
            "(non-acid) data in them.";
        LOG.error(ci.errorMessage + " Compaction info: {}", ci);
        try {
          msc.markRefused(CompactionInfo.compactionInfoToStruct(ci));
        } catch (Throwable tr) {
          LOG.error("Caught an exception while trying to mark compaction {} as failed: {}", ci, tr);
        }
        return false;
      }
      checkInterrupt();

      try {
        failCompactionIfSetForTest();

        /*
        First try to run compaction via HiveQL queries.
        Compaction for MM tables happens here, or run compaction for Crud tables if query-based compaction is enabled.
        todo Find a more generic approach to collecting files in the same logical bucket to compact within the same
        task (currently we're using Tez split grouping).
        */
        CompactorPipeline compactorPipeline = compactorFactory.getCompactorPipeline(table, conf, ci, msc);
        computeStats = (compactorPipeline.isMRCompaction() && collectMrStats) || collectGenericStats;

        LOG.info("Starting " + ci.type.toString() + " compaction for " + ci.getFullPartitionName() + ", id:" +
                ci.id + " in " + compactionTxn + " with compute stats set to " + computeStats);

        CompactorContext compactorContext = new CompactorContext(conf, table, p, sd, tblValidWriteIds, ci, dir);
        compactorPipeline.execute(compactorContext);

        LOG.info("Completed " + ci.type.toString() + " compaction for " + ci.getFullPartitionName() + " in "
            + compactionTxn + ", marking as compacted.");
        msc.markCompacted(CompactionInfo.compactionInfoToStruct(ci));
        compactionTxn.wasSuccessful();

        AcidMetricService.updateMetricsFromWorker(ci.dbname, ci.tableName, ci.partName, ci.type,
            dir.getCurrentDirectories().size(), dir.getDeleteDeltas().size(), conf, msc);
      } catch (Throwable e) {
        LOG.error("Caught exception while trying to compact " + ci +
            ". Marking failed to avoid repeated failures", e);
        final CompactionType ctype = ci.type;
        markFailed(ci, e.getMessage());

        computeStats = false;

        if (runJobAsSelf(ci.runAs)) {
          cleanupResultDirs(sd, tblValidWriteIds, ctype, dir);
        } else {
          LOG.info("Cleaning as user " + ci.runAs);
          UserGroupInformation ugi = UserGroupInformation.createProxyUser(ci.runAs,
              UserGroupInformation.getLoginUser());

          ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            cleanupResultDirs(sd, tblValidWriteIds, ctype, dir);
            return null;
          });
          try {
            FileSystem.closeAllForUGI(ugi);
          } catch (IOException ex) {
            LOG.error("Could not clean up file-system handles for UGI: " + ugi, e);
          }
        }
      }
    } catch (TException | IOException t) {
      LOG.error("Caught an exception in the main loop of compactor worker " + workerName, t);

      markFailed(ci, t.getMessage());

      if (msc != null) {
        msc.close();
        msc = null;
      }
    } catch (Throwable t) {
      LOG.error("Caught an exception in the main loop of compactor worker " + workerName, t);
    } finally {
      if (workerMetric != null && MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_EXT_ON)) {
        perfLogger.perfLogEnd(CLASS_NAME, workerMetric);
      }
    }

    if (computeStats) {
       statsUpdater.gatherStats(ci, conf, runJobAsSelf(ci.runAs) ? ci.runAs : table.getOwner(),
              CompactorUtil.getCompactorJobQueueName(conf, ci, table), msc);
    }
    return true;
  }

  /**
   * Just AcidUtils.getAcidState, but with impersonation if needed.
   */
  private AcidDirectory getAcidStateForWorker(CompactionInfo ci, StorageDescriptor sd,
          ValidCompactorWriteIdList tblValidWriteIds) throws IOException, InterruptedException {
    if (runJobAsSelf(ci.runAs)) {
      return AcidUtils.getAcidState(null, new Path(sd.getLocation()), conf,
              tblValidWriteIds, Ref.from(false), true);
    }

    UserGroupInformation ugi = UserGroupInformation.createProxyUser(ci.runAs, UserGroupInformation.getLoginUser());
    try {
      return ugi.doAs((PrivilegedExceptionAction<AcidDirectory>) () ->
              AcidUtils.getAcidState(null, new Path(sd.getLocation()), conf, tblValidWriteIds,
                      Ref.from(false), true));
    } finally {
      try {
        FileSystem.closeAllForUGI(ugi);
      } catch (IOException exception) {
        LOG.error("Could not clean up file-system handles for UGI: " + ugi + " for " + ci.getFullPartitionName(),
                exception);
      }
    }
  }

  private void cleanupResultDirs(StorageDescriptor sd, ValidWriteIdList writeIds, CompactionType ctype, AcidDirectory dir) {
    // result directory for compactor to write new files
    Path resultDir = QueryCompactor.Util.getCompactionResultDir(sd, writeIds, conf,
        ctype == CompactionType.MAJOR, false, false, dir);
    LOG.info("Deleting result directories created by the compactor:\n");
    try {
      FileSystem fs = resultDir.getFileSystem(conf);
      LOG.info(resultDir.toString());
      fs.delete(resultDir, true);

      if (ctype == CompactionType.MINOR) {
        Path deleteDeltaDir = QueryCompactor.Util.getCompactionResultDir(sd, writeIds, conf,
            false, true, false, dir);

        LOG.info(deleteDeltaDir.toString());
        fs.delete(deleteDeltaDir, true);
      }
    } catch (IOException ex) {
      LOG.error("Caught exception while cleaning result directories:", ex);
    }
  }

  private void failCompactionIfSetForTest() {
    if(conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST) && conf.getBoolVar(HiveConf.ConfVars.HIVETESTMODEFAILCOMPACTION)) {
      throw new RuntimeException(HiveConf.ConfVars.HIVETESTMODEFAILCOMPACTION.name() + "=true");
    }
  }

  private void markFailed(CompactionInfo ci, String errorMessage) {
    if (ci == null) {
      LOG.warn("CompactionInfo client was null. Could not mark failed");
      return;
    }
    if (ci != null && StringUtils.isNotBlank(errorMessage)) {
      ci.errorMessage = errorMessage;
    }
    if (msc == null) {
      LOG.warn("Metastore client was null. Could not mark failed: {}", ci);
      return;
    }
    try {
      msc.markFailed(CompactionInfo.compactionInfoToStruct(ci));
    } catch (Throwable t) {
      LOG.error("Caught an exception while trying to mark compaction {} as failed: {}", ci, t);
    }
  }

  private static boolean isDynPartAbort(Table t, CompactionInfo ci) {
    return t.getPartitionKeys() != null && t.getPartitionKeys().size() > 0
        && ci.partName == null;
  }

  private String getWorkerId() {
    StringBuilder name = new StringBuilder(this.hostName);
    name.append("-");
    name.append(getId());
    return name.toString();
  }

  /**
   * Keep track of the compaction's transaction and its operations.
   */
  class CompactionTxn implements AutoCloseable {
    private long txnId = 0;
    private long lockId = 0;

    private TxnStatus status = TxnStatus.UNKNOWN;
    private boolean successfulCompaction = false;

    /**
     * Try to open a new txn.
     * @throws TException
     */
    void open(CompactionInfo ci) throws TException {
      this.txnId = msc.openTxn(ci.runAs, ci.type == CompactionType.REBALANCE ? TxnType.REBALANCE_COMPACTION : TxnType.COMPACTION);
      status = TxnStatus.OPEN;

      LockRequest lockRequest;
      if (CompactionType.REBALANCE.equals(ci.type)) {
        lockRequest = createLockRequest(ci, txnId, LockType.EXCL_WRITE, DataOperationType.UPDATE);
      } else {
        lockRequest = createLockRequest(ci, txnId, LockType.SHARED_READ, DataOperationType.SELECT);
      }
      LockResponse res = msc.lock(lockRequest);
      if (res.getState() != LockState.ACQUIRED) {
        throw new TException("Unable to acquire lock(s) on {" + ci.getFullPartitionName()
            + "}, status {" + res.getState() + "}, reason {" + res.getErrorMessage() + "}");
      }
      lockId = res.getLockid();
      CompactionHeartbeatService.getInstance(conf).startHeartbeat(txnId, lockId, TxnUtils.getFullTableName(ci.dbname, ci.tableName));
    }

    /**
     * Mark compaction as successful. This means the txn will be committed; otherwise it will be aborted.
     */
    void wasSuccessful() {
      this.successfulCompaction = true;
    }

    /**
     * Commit or abort txn.
     * @throws Exception
     */
    @Override public void close() throws Exception {
      if (status == TxnStatus.UNKNOWN) {
        return;
      }
      try {
        //the transaction is about to close, we can stop heartbeating regardless of it's state
        CompactionHeartbeatService.getInstance(conf).stopHeartbeat(txnId);
      } finally {
        if (successfulCompaction) {
          commit();
        } else {
          abort();
        }
      }
    }

    long getTxnId() {
      return txnId;
    }

    @Override public String toString() {
      return "txnId=" + txnId + ", lockId=" + lockId + " (TxnStatus: " + status + ")";
    }

    /**
     * Commit the txn if open.
     */
    private void commit() throws TException {
      if (status == TxnStatus.OPEN) {
        msc.commitTxn(txnId);
        status = TxnStatus.COMMITTED;
      }
    }

    /**
     * Abort the txn if open.
     */
    private void abort() throws TException {
      if (status == TxnStatus.OPEN) {
        AbortTxnRequest abortTxnRequest = new AbortTxnRequest(txnId);
        abortTxnRequest.setErrorCode(TxnErrorMsg.ABORT_COMPACTION_TXN.getErrorCode());
        msc.rollbackTxn(abortTxnRequest);
        status = TxnStatus.ABORTED;
      }
    }
  }

}
