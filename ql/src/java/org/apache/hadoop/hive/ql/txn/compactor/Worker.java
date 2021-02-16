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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidCompactorWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.HiveMetaStoreUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreThread;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.TxnStatus;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.common.util.Ref;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
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
  static final private long SLEEP_TIME = 10000;

  private String workerName;

  /**
   * Get the hostname that this worker is run on.  Made static and public so that other classes
   * can use the same method to know what host their worker threads are running on.
   * @return hostname
   */
  public static String hostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Unable to resolve my host name " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  // TODO: this doesn't check if compaction is already running (even though Initiator does but we
  // don't go through Initiator for user initiated compactions)
  @Override
  public void run() {
    LOG.info("Starting Worker thread");
    boolean computeStats = conf.getBoolVar(HiveConf.ConfVars.HIVE_MR_COMPACTOR_GATHER_STATS);
    long timeout = conf.getTimeVar(HiveConf.ConfVars.HIVE_COMPACTOR_WORKER_TIMEOUT, TimeUnit.MILLISECONDS);
    boolean launchedJob;
    ExecutorService executor = getTimeoutHandlingExecutor();
    try {
      do {
        Future<Boolean> singleRun = executor.submit(() -> findNextCompactionAndExecute(computeStats));
        try {
          launchedJob = singleRun.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException te) {
          LOG.info("Timeout during executing compaction", te);
          // Cancel the job, and recreate the Executor as well, so we can be sure that we have an available thread
          // even if we can not interrupt the task somehow. (Trade possible resource hogging for compactor stability)
          singleRun.cancel(true);
          executor.shutdownNow();
          executor = getTimeoutHandlingExecutor();
          launchedJob = true;
        } catch (ExecutionException e) {
          LOG.info("Exception during executing compaction", e);
          launchedJob = true;
        } catch (InterruptedException ie) {
          // Do not do anything - stop should be set anyway
          launchedJob = true;
        }

        // If we didn't try to launch a job it either means there was no work to do or we got
        // here as the result of a communication failure with the DB.  Either way we want to wait
        // a bit before we restart the loop.
        if (!launchedJob && !stop.get()) {
          try {
            Thread.sleep(SLEEP_TIME);
          } catch (InterruptedException e) {
          }
        }
        LOG.info("Worker thread finished one loop.");
      } while (!stop.get());
    } finally {
      if (executor != null) {
        executor.shutdownNow();
      }
      if (msc != null) {
        msc.close();
      }
    }
  }

  private void verifyTableIdHasNotChanged(CompactionInfo ci, Table originalTable) throws HiveException, MetaException {
    Table currentTable = resolveTable(ci);
    if (originalTable.getId() != currentTable.getId()) {
      throw new HiveException("Table " + originalTable.getDbName() + "." + originalTable.getTableName()
          + " id (" + currentTable.getId() + ") is not equal to its id when compaction started ("
          + originalTable.getId() + "). The table might have been dropped and recreated while compaction was running."
          + " Marking compaction as failed.");
    }
  }

  @Override
  public void init(AtomicBoolean stop) throws Exception {
    super.init(stop);

    StringBuilder name = new StringBuilder(hostname());
    name.append("-");
    name.append(getId());
    this.workerName = name.toString();
    setName(name.toString());
  }

  static final class StatsUpdater {
    static final private Logger LOG = LoggerFactory.getLogger(StatsUpdater.class);

    public static StatsUpdater init(CompactionInfo ci, List<String> columnListForStats,
        HiveConf conf, String userName) {
      return new StatsUpdater(ci, columnListForStats, conf, userName);
    }

    /**
     * list columns for which to compute stats.  This maybe empty which means no stats gathering
     * is needed.
     */
    private final List<String> columnList;
    private final HiveConf conf;
    private final String userName;
    private final CompactionInfo ci;

    private StatsUpdater(CompactionInfo ci, List<String> columnListForStats,
        HiveConf conf, String userName) {
      this.conf = new HiveConf(conf);
      //so that Driver doesn't think it's arleady in a transaction
      this.conf.unset(ValidTxnList.VALID_TXNS_KEY);
      this.userName = userName;
      this.ci = ci;
      if (!ci.isMajorCompaction() || columnListForStats == null || columnListForStats.isEmpty()) {
        columnList = Collections.emptyList();
        return;
      }
      columnList = columnListForStats;
    }

    /**
     * This doesn't throw any exceptions because we don't want the Compaction to appear as failed
     * if stats gathering fails since this prevents Cleaner from doing it's job and if there are
     * multiple failures, auto initiated compactions will stop which leads to problems that are
     * much worse than stale stats.
     *
     * todo: longer term we should write something COMPACTION_QUEUE.CQ_META_INFO.  This is a binary
     * field so need to figure out the msg format and how to surface it in SHOW COMPACTIONS, etc
     */
    void gatherStats() {
      try {
        if (!ci.isMajorCompaction()) {
          return;
        }
        if (columnList.isEmpty()) {
          LOG.debug(ci + ": No existing stats found.  Will not run analyze.");
          return;//nothing to do
        }
        //e.g. analyze table page_view partition(dt='10/15/2014',country=’US’)
        // compute statistics for columns viewtime
        StringBuilder sb = new StringBuilder("analyze table ")
            .append(StatsUtils.getFullyQualifiedTableName(ci.dbname, ci.tableName));
        if (ci.partName != null) {
          sb.append(" partition(");
          Map<String, String> partitionColumnValues = Warehouse.makeEscSpecFromName(ci.partName);
          for (Map.Entry<String, String> ent : partitionColumnValues.entrySet()) {
            sb.append(ent.getKey()).append("='").append(ent.getValue()).append("',");
          }
          sb.setLength(sb.length() - 1); //remove trailing ,
          sb.append(")");
        }
        sb.append(" compute statistics for columns ");
        for (String colName : columnList) {
          sb.append(colName).append(",");
        }
        sb.setLength(sb.length() - 1); //remove trailing ,
        LOG.info(ci + ": running '" + sb.toString() + "'");
        conf.setVar(HiveConf.ConfVars.METASTOREURIS,"");

        //todo: use DriverUtils.runOnDriver() here
        QueryState queryState = new QueryState.Builder().withGenerateNewQueryId(true).withHiveConf(conf).build();
        SessionState localSession = null;
        try (Driver d = new Driver(queryState)) {
          if (SessionState.get() == null) {
            localSession = new SessionState(conf);
            SessionState.start(localSession);
          }
          try {
            d.run(sb.toString());
          } catch (CommandProcessorException e) {
            LOG.warn(ci + ": " + sb.toString() + " failed due to: " + e);
          }
        } finally {
          if (localSession != null) {
            try {
              localSession.close();
            } catch (IOException ex) {
              LOG.warn(ci + ": localSession.close() failed due to: " + ex.getMessage(), ex);
            }
          }
        }
      } catch (Throwable t) {
        LOG.error(ci + ": gatherStats(" + ci.dbname + "," + ci.tableName + "," + ci.partName +
                      ") failed due to: " + t.getMessage(), t);
      }
    }
  }

  static final class CompactionHeartbeater extends Thread {
    static final private Logger LOG = LoggerFactory.getLogger(CompactionHeartbeater.class);
    private final AtomicBoolean stop = new AtomicBoolean();
    private final CompactionTxn compactionTxn;
    private final String tableName;
    private final HiveConf conf;
    private final long interval;
    public CompactionHeartbeater(CompactionTxn compactionTxn, String tableName, HiveConf conf) {
      this.tableName = tableName;
      this.compactionTxn = compactionTxn;
      this.conf = conf;

      this.interval =
          MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.TXN_TIMEOUT, TimeUnit.MILLISECONDS) / 2;
      setDaemon(true);
      setPriority(MIN_PRIORITY);
      setName("CompactionHeartbeater-" + compactionTxn.getTxnId());
    }
    @Override
    public void run() {
      IMetaStoreClient msc = null;
      try {
        // We need to create our own metastore client since the thrifts clients
        // are not thread safe.
        msc = HiveMetaStoreUtils.getHiveMetastoreClient(conf);
        LOG.debug("Heartbeating compaction transaction id {} for table: {}", compactionTxn, tableName);
        while(!stop.get()) {
          msc.heartbeat(compactionTxn.getTxnId(), 0);
          Thread.sleep(interval);
        }
      } catch (Exception e) {
        LOG.error("Error while heartbeating txn {} in {}, error: ", compactionTxn, Thread.currentThread().getName(), e.getMessage());
      } finally {
        if (msc != null) {
          msc.close();
        }
      }
    }

    public void cancel() {
      if(!this.stop.get()) {
        LOG.debug("Successfully stop the heartbeating the transaction {}", this.compactionTxn);
        this.stop.set(true);
      }
    }
  }

  /**
   * Determine if compaction can run in a specified directory.
   * @param isMajorCompaction type of compaction.
   * @param dir the delta directory
   * @param sd resolved storage descriptor
   * @return true, if compaction can run.
   */
  static boolean isEnoughToCompact(boolean isMajorCompaction, AcidDirectory dir,
      StorageDescriptor sd) {
    int deltaCount = dir.getCurrentDirectories().size();
    int origCount = dir.getOriginalFiles().size();

    StringBuilder deltaInfo = new StringBuilder().append(deltaCount);
    boolean isEnoughToCompact;

    if (isMajorCompaction) {
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
   * @param computeStats If true then for MR compaction the stats are regenerated
   * @return Returns true, if there was compaction in the queue, and we started working on it.
   * @throws InterruptedException is thrown when the process is interrupted because of timeout for example
   */
  @VisibleForTesting
  protected Boolean findNextCompactionAndExecute(boolean computeStats) throws InterruptedException {
    // Make sure nothing escapes this run method and kills the metastore at large,
    // so wrap it in a big catch Throwable statement.
    CompactionHeartbeater heartbeater = null;
    CompactionInfo ci = null;
    try (CompactionTxn compactionTxn = new CompactionTxn()) {
      if (msc == null) {
        try {
          msc = HiveMetaStoreUtils.getHiveMetastoreClient(conf);
        } catch (Exception e) {
          LOG.error("Failed to connect to HMS", e);
          return false;
        }
      }
      ci = CompactionInfo.optionalCompactionInfoStructToInfo(msc.findNextCompact(workerName));
      LOG.debug("Processing compaction request " + ci);

      if (ci == null) {
        return false;
      }

      checkInterrupt();

      // Find the table we will be working with.
      Table t1;
      try {
        t1 = resolveTable(ci);
        if (t1 == null) {
          LOG.info("Unable to find table " + ci.getFullTableName() +
                       ", assuming it was dropped and moving on.");
          msc.markCleaned(CompactionInfo.compactionInfoToStruct(ci));
          return false;
        }
      } catch (MetaException e) {
        msc.markCleaned(CompactionInfo.compactionInfoToStruct(ci));
        return false;
      }

      checkInterrupt();

      // This chicanery is to get around the fact that the table needs to be final in order to
      // go into the doAs below.
      final Table t = t1;
      String fullTableName = TxnUtils.getFullTableName(t.getDbName(), t.getTableName());

      // Find the partition we will be working with, if there is one.
      Partition p;
      try {
        p = resolvePartition(ci);
        if (p == null && ci.partName != null) {
          LOG.info("Unable to find partition " + ci.getFullPartitionName() +
                       ", assuming it was dropped and moving on.");
          msc.markCleaned(CompactionInfo.compactionInfoToStruct(ci));
          return false;
        }
      } catch (Exception e) {
        msc.markCleaned(CompactionInfo.compactionInfoToStruct(ci));
        return false;
      }

      checkInterrupt();

      // Find the appropriate storage descriptor
      final StorageDescriptor sd =  resolveStorageDescriptor(t, p);

      // Check that the table or partition isn't sorted, as we don't yet support that.
      if (sd.getSortCols() != null && !sd.getSortCols().isEmpty()) {
        LOG.error("Attempt to compact sorted table " + ci.getFullTableName() + ", which is not yet supported!");
        msc.markCleaned(CompactionInfo.compactionInfoToStruct(ci));
        return false;
      }

      if (ci.runAs == null) {
        ci.runAs = findUserToRunAs(sd.getLocation(), t);
      }

      checkInterrupt();

      /**
       * we cannot have Worker use HiveTxnManager (which is on ThreadLocal) since
       * then the Driver would already have the an open txn but then this txn would have
       * multiple statements in it (for query based compactor) which is not supported (and since
       * this case some of the statements are DDL, even in the future will not be allowed in a
       * multi-stmt txn. {@link Driver#setCompactionWriteIds(ValidWriteIdList, long)} */
      compactionTxn.open(ci);

      heartbeater = new CompactionHeartbeater(compactionTxn, fullTableName, conf);
      heartbeater.start();

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

      final StringBuilder jobName = new StringBuilder(workerName);
      jobName.append("-compactor-");
      jobName.append(ci.getFullPartitionName());

      // Don't start compaction or cleaning if not necessary
      if (isDynPartAbort(t, ci)) {
        msc.markCompacted(CompactionInfo.compactionInfoToStruct(ci));
        compactionTxn.wasSuccessful();
        return false;
      }
      AcidDirectory dir = getAcidStateForWorker(ci, sd, tblValidWriteIds);
      if (!isEnoughToCompact(ci.isMajorCompaction(), dir, sd)) {
        if (needsCleaning(dir, sd)) {
          msc.markCompacted(CompactionInfo.compactionInfoToStruct(ci));
        } else {
          // do nothing
          msc.markCleaned(CompactionInfo.compactionInfoToStruct(ci));
        }
        compactionTxn.wasSuccessful();
        return false;
      }

      checkInterrupt();

      LOG.info("Starting " + ci.type.toString() + " compaction for " + ci.getFullPartitionName() + " in " +
                   compactionTxn + " with compute stats set to " + computeStats);
      final StatsUpdater su = computeStats ? StatsUpdater.init(ci, msc.findColumnsWithStats(
          CompactionInfo.compactionInfoToStruct(ci)), conf,
          runJobAsSelf(ci.runAs) ? ci.runAs : t.getOwner()) : null;

      try {
        failCompactionIfSetForTest();

        /*
        First try to run compaction via HiveQL queries.
        Compaction for MM tables happens here, or run compaction for Crud tables if query-based compaction is enabled.
        todo Find a more generic approach to collecting files in the same logical bucket to compact within the same
        task (currently we're using Tez split grouping).
        */
        QueryCompactor queryCompactor = QueryCompactorFactory.getQueryCompactor(t, conf, ci);
        if (queryCompactor != null) {
          LOG.info("Will compact id: " + ci.id + " with query-based compactor class: "
              + queryCompactor.getClass().getName());
          queryCompactor.runCompaction(conf, t, p, sd, tblValidWriteIds, ci, dir);
        } else {
          LOG.info("Will compact id: " + ci.id + " via MR job");
          runCompactionViaMrJob(ci, t, p, sd, tblValidWriteIds, jobName, dir, su);
        }

        heartbeater.cancel();

        verifyTableIdHasNotChanged(ci, t1);

        LOG.info("Completed " + ci.type.toString() + " compaction for " + ci.getFullPartitionName() + " in "
            + compactionTxn + ", marking as compacted.");
        msc.markCompacted(CompactionInfo.compactionInfoToStruct(ci));
        compactionTxn.wasSuccessful();
      } catch (Throwable e) {
        LOG.error("Caught exception while trying to compact " + ci +
            ".  Marking failed to avoid repeated failures", e);
        markFailed(ci, e);
      }
    } catch (TException | IOException t) {
      LOG.error("Caught an exception in the main loop of compactor worker " + workerName, t);
      markFailed(ci, t);
      if (msc != null) {
        msc.close();
        msc = null;
      }
    } catch (Throwable t) {
      LOG.error("Caught an exception in the main loop of compactor worker " + workerName, t);
    } finally {
      if (heartbeater != null) {
        heartbeater.cancel();
      }
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

  private void failCompactionIfSetForTest() {
    if(conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST) && conf.getBoolVar(HiveConf.ConfVars.HIVETESTMODEFAILCOMPACTION)) {
      throw new RuntimeException(HiveConf.ConfVars.HIVETESTMODEFAILCOMPACTION.name() + "=true");
    }
  }

  private void runCompactionViaMrJob(CompactionInfo ci, Table t, Partition p, StorageDescriptor sd,
      ValidCompactorWriteIdList tblValidWriteIds, StringBuilder jobName, AcidDirectory dir, StatsUpdater su)
      throws IOException, HiveException, InterruptedException {
    final CompactorMR mr = new CompactorMR();
    if (runJobAsSelf(ci.runAs)) {
      mr.run(conf, jobName.toString(), t, p, sd, tblValidWriteIds, ci, su, msc, dir);
    } else {
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(ci.runAs, UserGroupInformation.getLoginUser());
      ugi.doAs((PrivilegedExceptionAction<Object>) () -> {
        mr.run(conf, jobName.toString(), t, p, sd, tblValidWriteIds, ci, su, msc, dir);
        return null;
      });
      try {
        FileSystem.closeAllForUGI(ugi);
      } catch (IOException exception) {
        LOG.error("Could not clean up file-system handles for UGI: " + ugi + " for " + ci.getFullPartitionName(),
            exception);
      }
    }
  }

  private void markFailed(CompactionInfo ci, Throwable e) {
    if (ci != null) {
      ci.errorMessage = e.getMessage();
    }
    if (msc == null) {
      LOG.warn("Metastore client was null. Could not mark failed: {}", ci);
      return;
    }
    try {
      msc.markFailed(CompactionInfo.compactionInfoToStruct(ci));
    } catch (TException e1) {
      LOG.error("Caught an exception while trying to mark compaction {} as failed: {}", ci, e);
    }
  }

  private void checkInterrupt() throws InterruptedException {
    if (Thread.interrupted()) {
      throw new InterruptedException("Compaction execution is interrupted");
    }
  }

  private static boolean isDynPartAbort(Table t, CompactionInfo ci) {
    return t.getPartitionKeys() != null && t.getPartitionKeys().size() > 0
        && ci.partName == null;
  }

  /**
   * Keep track of the compaction's transaction and its operations.
   */
  private class CompactionTxn implements AutoCloseable {
    private long txnId = 0;
    private TxnStatus status = TxnStatus.UNKNOWN;
    private boolean succeessfulCompaction = false;

    /**
     * Try to open a new txn.
     * @throws TException
     */
    void open(CompactionInfo ci) throws TException {
      if (msc == null) {
        LOG.error("Metastore client was null. Could not open a new transaction.");
        return;
      }
      this.txnId = msc.openTxn(ci.runAs, TxnType.COMPACTION);
      status = TxnStatus.OPEN;
    }

    /**
     * Mark compaction as successful. This means the txn will be committed; otherwise it will be aborted.
     */
    void wasSuccessful() {
      this.succeessfulCompaction = true;
    }

    /**
     * Commit or abort txn.
     * @throws Exception
     */
    @Override public void close() throws Exception {
      if (status == TxnStatus.UNKNOWN) {
        return;
      }
      if (succeessfulCompaction) {
        commit();
      } else {
        abort();
      }
    }

    long getTxnId() {
      return txnId;
    }

    @Override public String toString() {
      return "txnId=" + txnId + " (TxnStatus: " + status + ")";
    }

    /**
     * Commit the txn if open.
     */
    private void commit() {
      if (msc == null) {
        LOG.error("Metastore client was null. Could not commit txn " + this);
        return;
      }
      if (status == TxnStatus.OPEN) {
        try {
          msc.commitTxn(txnId);
          status = TxnStatus.COMMITTED;
        } catch (TException e) {
          LOG.error("Caught an exception while committing compaction txn in worker " + workerName, e);
        }
      }
    }

    /**
     * Abort the txn if open.
     */
    private void abort() {
      if (msc == null) {
        LOG.error("Metastore client was null. Could not abort txn " + this);
        return;
      }
      if (status == TxnStatus.OPEN) {
        try {
          msc.abortTxns(Collections.singletonList(txnId));
          status = TxnStatus.ABORTED;
        } catch (TException e) {
          LOG.error("Caught an exception while aborting compaction txn in worker " + workerName, e);
        }
      }
    }
  }
}