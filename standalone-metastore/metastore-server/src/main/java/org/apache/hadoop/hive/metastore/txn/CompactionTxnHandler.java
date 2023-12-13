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
package org.apache.hadoop.hive.metastore.txn;

import org.apache.hadoop.hive.common.classification.RetrySemantics;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.FindNextCompactRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionMetricsData;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionState;
import org.apache.hadoop.hive.metastore.txn.entities.OperationType;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.jdbc.commands.InsertCompactionInfoCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.commands.RemoveCompactionMetricsDataCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.commands.RemoveDuplicateCompleteTxnComponentsCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.functions.CleanTxnToWriteIdTableFunction;
import org.apache.hadoop.hive.metastore.txn.jdbc.functions.FindPotentialCompactionsFunction;
import org.apache.hadoop.hive.metastore.txn.jdbc.functions.GenerateCompactionQueueIdFunction;
import org.apache.hadoop.hive.metastore.txn.jdbc.functions.MarkCleanedFunction;
import org.apache.hadoop.hive.metastore.txn.jdbc.functions.MinOpenTxnIdWaterMarkFunction;
import org.apache.hadoop.hive.metastore.txn.jdbc.functions.NextCompactionFunction;
import org.apache.hadoop.hive.metastore.txn.jdbc.functions.PurgeCompactionHistoryFunction;
import org.apache.hadoop.hive.metastore.txn.jdbc.functions.TopCompactionMetricsDataPerTypeFunction;
import org.apache.hadoop.hive.metastore.txn.jdbc.functions.UpdateCompactionMetricsDataFunction;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.CheckFailedCompactionsHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.CompactionMetricsDataHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.FindColumnsWithStatsHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.GetCompactionInfoHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.OpenTxnTimeoutLowBoundaryTxnIdHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.ReadyToCleanAbortHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.ReadyToCleanHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.ParameterizedCommand;
import org.apache.hadoop.hive.metastore.txn.retry.SqlRetryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.sql.Types;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;
import static org.apache.hadoop.hive.metastore.txn.TxnUtils.thriftCompactionType2DbType;

/**
 * Extends the transaction handler with methods needed only by the compactor threads.  These
 * methods are not available through the thrift interface.
 */
class CompactionTxnHandler extends TxnHandler {
  
  private static final Logger LOG = LoggerFactory.getLogger(CompactionTxnHandler.class.getName());

  public CompactionTxnHandler() {
  }

  /**
   * This will look through the completed_txn_components table and look for partitions or tables
   * that may be ready for compaction.  Also, look through txns and txn_components tables for
   * aborted transactions that we should add to the list.
   * @param abortedThreshold  number of aborted queries forming a potential compaction request.
   * @return set of CompactionInfo structs.  These will not have id, type,
   * or runAs set since these are only potential compactions not actual ones.
   */
  @Override
  @RetrySemantics.ReadOnly
  public Set<CompactionInfo> findPotentialCompactions(int abortedThreshold, long abortedTimeThreshold)
      throws MetaException {
    return findPotentialCompactions(abortedThreshold, abortedTimeThreshold, -1);
  }

  @Override
  @RetrySemantics.ReadOnly
  public Set<CompactionInfo> findPotentialCompactions(int abortedThreshold,
      long abortedTimeThreshold, long lastChecked) throws MetaException {
    return new FindPotentialCompactionsFunction(conf, abortedThreshold, abortedTimeThreshold, lastChecked).execute(jdbcResource);
  }

  /**
   * This will grab the next compaction request off of
   * the queue, and assign it to the worker.
   * @param workerId id of the worker calling this, will be recorded in the db
   * @deprecated  Replaced by
   *     {@link CompactionTxnHandler#findNextToCompact(org.apache.hadoop.hive.metastore.api.FindNextCompactRequest)}
   * @return an info element for next compaction in the queue, or null if there is no work to do now.
   */
  @Deprecated
  @Override
  @RetrySemantics.SafeToRetry
  public CompactionInfo findNextToCompact(String workerId) throws MetaException {
    FindNextCompactRequest findNextCompactRequest = new FindNextCompactRequest();
    findNextCompactRequest.setWorkerId(workerId);
    return findNextToCompact(findNextCompactRequest);
  }

  /**
   * This will grab the next compaction request off of the queue, and assign it to the worker.
   * @param rqst request to find next compaction to run
   * @return an info element for next compaction in the queue, or null if there is no work to do now.
   */
  @Override
  @RetrySemantics.SafeToRetry
  public CompactionInfo findNextToCompact(FindNextCompactRequest rqst) throws MetaException {
    if (rqst == null) {
      throw new MetaException("FindNextCompactRequest is null");
    }
    long poolTimeout = MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.COMPACTOR_WORKER_POOL_TIMEOUT, TimeUnit.MILLISECONDS);
    return new NextCompactionFunction(rqst, getDbTime(), poolTimeout).execute(jdbcResource);
  }

  /**
   * This will mark an entry in the queue as compacted
   * and put it in the ready to clean state.
   * @param info info on the compaction entry to mark as compacted.
   */
  @Override
  @RetrySemantics.SafeToRetry
  public void markCompacted(CompactionInfo info) throws MetaException {
    jdbcResource.execute(
        "UPDATE \"COMPACTION_QUEUE\" SET \"CQ_STATE\" = :state, \"CQ_WORKER_ID\" = NULL WHERE \"CQ_ID\" = :id",
        new MapSqlParameterSource()
            .addValue("state", Character.toString(READY_FOR_CLEANING), Types.CHAR)
            .addValue("id", info.id),
        ParameterizedCommand.EXACTLY_ONE_ROW);
  }

  /**
   * Find entries in the queue that are ready to
   * be cleaned.
   * @param minOpenTxnWaterMark Minimum open txnId
   * @return information on the entry in the queue.
   */
  @Override
  @RetrySemantics.ReadOnly
  public List<CompactionInfo> findReadyToClean(long minOpenTxnWaterMark, long retentionTime) throws MetaException {
    return jdbcResource.execute(new ReadyToCleanHandler(conf, minOpenTxnWaterMark, retentionTime));
  }

  @Override
  @RetrySemantics.ReadOnly
  public List<CompactionInfo> findReadyToCleanAborts(long abortedTimeThreshold, int abortedThreshold) throws MetaException {
    return jdbcResource.execute(new ReadyToCleanAbortHandler(conf, abortedTimeThreshold, abortedThreshold));
  }

  /**
   * Mark the cleaning start time for a particular compaction
   *
   * @param info info on the compaction entry
   */
  @Override
  @RetrySemantics.ReadOnly
  public void markCleanerStart(CompactionInfo info) throws MetaException {
    LOG.debug("Running markCleanerStart with CompactionInfo: {}", info);
    setCleanerStart(info, getDbTime().getTime());
  }

  /**
   * Removes the cleaning start time for a particular compaction
   *
   * @param info info on the compaction entry
   */
  @Override
  @RetrySemantics.ReadOnly
  public void clearCleanerStart(CompactionInfo info) throws MetaException {
    LOG.debug("Running clearCleanerStart with CompactionInfo: {}", info);
    setCleanerStart(info, -1L);
  }

  private void setCleanerStart(CompactionInfo info, Long timestamp) throws MetaException {
    jdbcResource.execute(
        " UPDATE \"COMPACTION_QUEUE\" " +
            " SET \"CQ_CLEANER_START\" = :timeStamp" +
            " WHERE \"CQ_ID\" = :id AND \"CQ_STATE\"= :state",
        new MapSqlParameterSource()
            .addValue("timeStamp", timestamp)
            .addValue("state", Character.toString(READY_FOR_CLEANING), Types.CHAR)
            .addValue("id", info.id),
        ParameterizedCommand.EXACTLY_ONE_ROW);
  }

  /**
   * This will remove an entry from the queue after
   * it has been compacted.
   *
   * @param info info on the compaction entry to remove
   */
  @Override
  @RetrySemantics.CannotRetry
  public void markCleaned(CompactionInfo info) throws MetaException {
    LOG.debug("Running markCleaned with CompactionInfo: {}", info);
    new MarkCleanedFunction(info).execute(jdbcResource);
  }
  
  /**
   * Clean up entries from TXN_TO_WRITE_ID table less than min_uncommited_txnid as found by
   * min(max(TXNS.txn_id), min(WRITE_SET.WS_COMMIT_ID), min(Aborted TXNS.txn_id)).
   */
  @Override
  @RetrySemantics.SafeToRetry
  public void cleanTxnToWriteIdTable() throws MetaException {
    new CleanTxnToWriteIdTableFunction(findMinTxnIdSeenOpen()).execute(jdbcResource);
  }

  @Override
  @RetrySemantics.SafeToRetry
  public void removeDuplicateCompletedTxnComponents() throws MetaException {
    jdbcResource.execute(RemoveDuplicateCompleteTxnComponentsCommand.INSTANCE);
  }

  /**
   * Clean up aborted / committed transactions from txns that have no components in txn_components.
   * The committed txns are left there for TXN_OPENTXN_TIMEOUT window period intentionally.
   * The reason such aborted txns exist can be that now work was done in this txn
   * (e.g. Streaming opened TransactionBatch and abandoned it w/o doing any work)
   * or due to {@link #markCleaned(CompactionInfo)} being called.
   */
  @Override
  @RetrySemantics.SafeToRetry
  public void cleanEmptyAbortedAndCommittedTxns() throws MetaException {
    LOG.info("Start to clean empty aborted or committed TXNS");
    //after that, so READ COMMITTED is sufficient.
    /*
     * Only delete aborted / committed transaction in a way that guarantees two things:
     * 1. never deletes anything that is inside the TXN_OPENTXN_TIMEOUT window
     * 2. never deletes the maximum txnId even if it is before the TXN_OPENTXN_TIMEOUT window
     */
    try {
      long lowWaterMark = jdbcResource.execute(new OpenTxnTimeoutLowBoundaryTxnIdHandler(openTxnTimeOutMillis));
      jdbcResource.execute(
          "DELETE FROM \"TXNS\" WHERE \"TXN_ID\" NOT IN (SELECT \"TC_TXNID\" FROM \"TXN_COMPONENTS\") " +
              "AND (\"TXN_STATE\" = :abortedState OR \"TXN_STATE\" = :committedState) AND \"TXN_ID\" < :txnId",
          new MapSqlParameterSource()
              .addValue("txnId", lowWaterMark)
              .addValue("abortedState", TxnStatus.ABORTED.getSqlConst(), Types.CHAR)
              .addValue("committedState", TxnStatus.COMMITTED.getSqlConst(), Types.CHAR),
          null);
    } catch (DataAccessException e) {
      throw new MetaException("Unable to get the txn id: " + SqlRetryHandler.getMessage(e));
    }
  }

  /**
   * This will take all entries assigned to workers
   * on a host return them to INITIATED state.  The initiator should use this at start up to
   * clean entries from any workers that were in the middle of compacting when the metastore
   * shutdown.  It does not reset entries from worker threads on other hosts as those may still
   * be working.
   * @param hostname Name of this host.  It is assumed this prefixes the thread's worker id,
   *                 so that like hostname% will match the worker id.
   */
  @Override
  @RetrySemantics.Idempotent
  public void revokeFromLocalWorkers(String hostname) throws MetaException {
    jdbcResource.execute(
        "UPDATE \"COMPACTION_QUEUE\" SET \"CQ_WORKER_ID\" = NULL, \"CQ_START\" = NULL," +
            " \"CQ_STATE\" = :initiatedState WHERE \"CQ_STATE\" = :workingState AND \"CQ_WORKER_ID\" LIKE :hostname",
        new MapSqlParameterSource()
            .addValue("initiatedState", Character.toString(INITIATED_STATE), Types.CHAR)
            .addValue("workingState", Character.toString(WORKING_STATE), Types.CHAR)
            .addValue("hostname", hostname + "%"),
        null);
  }

  /**
   * This call will return all compaction queue
   * entries assigned to a worker but over the timeout back to the initiated state.
   * This should be called by the initiator on start up and occasionally when running to clean up
   * after dead threads.  At start up {@link #revokeFromLocalWorkers(String)} should be called
   * first.
   * @param timeout number of milliseconds since start time that should elapse before a worker is
   *                declared dead.
   */
  @Override
  @RetrySemantics.Idempotent
  public void revokeTimedoutWorkers(long timeout) throws MetaException {
    long latestValidStart = getDbTime().getTime() - timeout;
    jdbcResource.execute(
        "UPDATE \"COMPACTION_QUEUE\" SET \"CQ_WORKER_ID\" = NULL, \"CQ_START\" = NULL, " +
            "\"CQ_STATE\" = :initiatedState WHERE \"CQ_STATE\" = :workingState AND \"CQ_START\" < :timeout",
        new MapSqlParameterSource()
            .addValue("initiatedState", Character.toString(INITIATED_STATE), Types.CHAR)
            .addValue("workingState", Character.toString(WORKING_STATE), Types.CHAR)
            .addValue("timeout", latestValidStart),
        null);
  }

  /**
   * Queries metastore DB directly to find columns in the table which have statistics information.
   * If {@code ci} includes partition info then per partition stats info is examined, otherwise
   * table level stats are examined.
   * @throws MetaException
   */
  @Override
  @RetrySemantics.ReadOnly
  public List<String> findColumnsWithStats(CompactionInfo ci) throws MetaException {
    return jdbcResource.execute(new FindColumnsWithStatsHandler(ci));
  }

  @Override
  public void updateCompactorState(CompactionInfo ci, long compactionTxnId) throws MetaException {
    jdbcResource.execute(
        "UPDATE \"COMPACTION_QUEUE\" SET \"CQ_HIGHEST_WRITE_ID\" = :highestWriteId, " +
            "\"CQ_RUN_AS\" = :runAs, \"CQ_TXN_ID\" = :txnId WHERE \"CQ_ID\" = :id",
        new MapSqlParameterSource()
            .addValue("highestWriteId", ci.highestWriteId)
            .addValue("runAs", ci.runAs)
            .addValue("txnId", compactionTxnId)
            .addValue("id", ci.id),
        ParameterizedCommand.EXACTLY_ONE_ROW);

    MapSqlParameterSource parameterSource = new MapSqlParameterSource()
        .addValue("txnId", compactionTxnId)
        .addValue("dbName", ci.dbname)
        .addValue("tableName", ci.tableName)
        .addValue("partName", ci.partName, Types.VARCHAR)
        .addValue("highestWriteId", ci.highestWriteId)
        .addValue("operationType", OperationType.COMPACT.getSqlConst());
    if (ci.partName != null) {
      parameterSource.addValue("partName", ci.partName);
    }
    jdbcResource.execute(
        /*We make an entry in TXN_COMPONENTS for the partition/table that the compactor is
         * working on in case this txn aborts and so we need to ensure that its TXNS entry is
         * not removed until Cleaner has removed all files that this txn may have written, i.e.
         * make it work the same way as any other write.  TC_WRITEID is set to the highest
         * WriteId that this compactor run considered since there compactor doesn't allocate
         * a new write id (so as not to invalidate result set caches/materialized views) but
         * we need to set it to something to that markCleaned() only cleans TXN_COMPONENTS up to
         * the level to which aborted files/data has been cleaned.*/
        "INSERT INTO \"TXN_COMPONENTS\"(\"TC_TXNID\", \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\", " +
            "\"TC_WRITEID\", \"TC_OPERATION_TYPE\") " +
            "VALUES(:txnId, :dbName, :tableName, :partName, :highestWriteId, :operationType)",
        parameterSource,
        ParameterizedCommand.EXACTLY_ONE_ROW);
  }

  /**
   * For any given compactable entity (partition; table if not partitioned) the history of compactions
   * may look like "sssfffaaasffss", for example.  The idea is to retain the tail (most recent) of the
   * history such that a configurable number of each type of state is present.  Any other entries
   * can be purged.  This scheme has advantage of always retaining the last failure/success even if
   * it's not recent.
   * Also, "not initiated" and "failed" compactions are purged if they are older than
   * metastore.compactor.history.retention.timeout and there is a newer "succeeded"
   * compaction on the table and either (1) the "succeeded" compaction is major or (2) it is minor
   * and the "not initiated" or "failed" compaction is also minor –– so a minor succeeded compaction
   * will not cause the deletion of a major "not initiated" or "failed" compaction.
   *
   * @throws MetaException
   */
  @Override
  @RetrySemantics.SafeToRetry
  public void purgeCompactionHistory() throws MetaException {
    new PurgeCompactionHistoryFunction().execute(jdbcResource);
  }

  /**
   * Returns {@code true} if there already exists sufficient number of consecutive failures for
   * this table/partition so that no new automatic compactions will be scheduled.
   * User initiated compactions don't do this check.
   * Do we allow compacting whole table (when it's partitioned)?  No, though perhaps we should.
   * That would be a meta operations, i.e. first find all partitions for this table (which have
   * txn info) and schedule each compaction separately.  This avoids complications in this logic.
   */
  @Override
  @RetrySemantics.ReadOnly
  public boolean checkFailedCompactions(CompactionInfo ci) throws MetaException {
    return jdbcResource.execute(new CheckFailedCompactionsHandler(conf, ci));
  }

  private void updateStatus(CompactionInfo ci) throws MetaException {
    String strState = CompactionState.fromSqlConst(ci.state).toString();

    LOG.debug("Marking as {}: CompactionInfo: {}", strState, ci);
    CompactionInfo ciActual = jdbcResource.execute(new GetCompactionInfoHandler(ci.id, false)); 

    long endTime = getDbTime().getTime();
    if (ciActual != null) {
      //preserve errorMessage and state
      ciActual.errorMessage = ci.errorMessage;
      ciActual.state = ci.state;

      jdbcResource.execute("DELETE FROM \"COMPACTION_QUEUE\" WHERE \"CQ_ID\" = :id",
          new MapSqlParameterSource("id", ci.id), null);
    } else {
      if (ci.id > 0) {
        //the record with valid CQ_ID has disappeared - this is a sign of something wrong
        throw new IllegalStateException("No record with CQ_ID=" + ci.id + " found in COMPACTION_QUEUE");
      }
      ciActual = ci;
    }
    if (ciActual.id == 0) {
      //The failure occurred before we even made an entry in COMPACTION_QUEUE
      //generate ID so that we can make an entry in COMPLETED_COMPACTIONS
      ciActual.id = new GenerateCompactionQueueIdFunction().execute(jdbcResource);
      //this is not strictly accurate, but 'type' cannot be null.
      if (ciActual.type == null) {
        ciActual.type = CompactionType.MINOR;
      }
      //in case of creating a new entry start and end time will be the same
      ciActual.start = endTime;
      LOG.debug("The failure occurred before we even made an entry in COMPACTION_QUEUE. Generated ID so that we "
          + "can make an entry in COMPLETED_COMPACTIONS. New Id: {}", ciActual.id);
    }

    jdbcResource.execute(new InsertCompactionInfoCommand(ciActual, endTime));
  }

  /**
   * If there is an entry in compaction_queue with ci.id, remove it
   * Make entry in completed_compactions with status 'f'.
   * If there is no entry in compaction_queue, it means Initiator failed to even schedule a compaction,
   * which we record as DID_NOT_INITIATE entry in history.
   */
  @Override
  @RetrySemantics.CannotRetry
  public void markFailed(CompactionInfo ci) throws MetaException {
    ci.state = ci.id == 0 ? DID_NOT_INITIATE : FAILED_STATE;
    updateStatus(ci);
  }

  /**
   * Mark a compaction as refused (to run).
   * @param info compaction job.
   * @throws MetaException
   */
  @Override
  @RetrySemantics.CannotRetry
  public void markRefused(CompactionInfo info) throws MetaException {
    info.state = REFUSED_STATE;
    updateStatus(info);
  }

  @Override
  @RetrySemantics.CannotRetry
  public void setCleanerRetryRetentionTimeOnError(CompactionInfo info) throws MetaException {
    if (info.isAbortedTxnCleanup() && info.id == 0) {
      /*
       * MUTEX_KEY.CompactionScheduler lock ensures that there is only 1 entry in
       * Initiated/Working state for any resource.  This ensures that we don't run concurrent
       * compactions for any resource.
       */
      try (TxnStore.MutexAPI.LockHandle ignored = getMutexAPI().acquireLock(MUTEX_KEY.CompactionScheduler.name())) {
        long id = new GenerateCompactionQueueIdFunction().execute(jdbcResource);
        int updCnt = jdbcResource.execute(
            "INSERT INTO \"COMPACTION_QUEUE\" (\"CQ_ID\", \"CQ_DATABASE\", \"CQ_TABLE\", \"CQ_PARTITION\", " +
                " \"CQ_TYPE\", \"CQ_STATE\", \"CQ_RETRY_RETENTION\", \"CQ_ERROR_MESSAGE\", \"CQ_COMMIT_TIME\") " +
                " VALUES (:id, :db, :table, :partition, :type, :state, :retention, :msg, " + getEpochFn(dbProduct) + ")",
            new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("db", info.dbname)
                .addValue("table", info.tableName)
                .addValue("partition", info.partName, Types.VARCHAR)
                .addValue("type", Character.toString(thriftCompactionType2DbType(info.type)))
                .addValue("state", Character.toString(info.state))
                .addValue("retention", info.retryRetention)
                .addValue("msg", info.errorMessage), null);
        if (updCnt == 0) {
          LOG.error("Unable to update/insert compaction queue record: {}. updCnt={}", info, updCnt);
          throw new MetaException("Unable to insert abort retry entry into COMPACTION QUEUE: " +
              " CQ_DATABASE=" + info.dbname + ", CQ_TABLE=" + info.tableName + ", CQ_PARTITION" + info.partName);
        }
      } catch (Exception e) {
        throw new MetaException("Failed to set retry retention time for compaction item: " + info + " Error: " + e);
      }
    } else {
      jdbcResource.execute(
          "UPDATE \"COMPACTION_QUEUE\" SET \"CQ_RETRY_RETENTION\" = :retention, \"CQ_ERROR_MESSAGE\"= :msg WHERE \"CQ_ID\" = :id",
          new MapSqlParameterSource()
              .addValue("retention", info.retryRetention)
              .addValue("msg", info.errorMessage)
              .addValue("id", info.id),
          ParameterizedCommand.EXACTLY_ONE_ROW);
    }
  }

  @Override
  @RetrySemantics.Idempotent
  public void setHadoopJobId(String hadoopJobId, long id) throws MetaException {
    jdbcResource.execute(
        "UPDATE \"COMPACTION_QUEUE\" SET \"CQ_HADOOP_JOB_ID\" = :hadoopJobId WHERE \"CQ_ID\" = :id",
        new MapSqlParameterSource()
            .addValue("id", id)
            .addValue("hadoopJobId", hadoopJobId),
        ParameterizedCommand.EXACTLY_ONE_ROW);
  }

  @Override
  @RetrySemantics.Idempotent
  public long findMinOpenTxnIdForCleaner() throws MetaException {
    if (ConfVars.useMinHistoryWriteId()) {
      return Long.MAX_VALUE;
    }
    return new MinOpenTxnIdWaterMarkFunction(openTxnTimeOutMillis).execute(jdbcResource);      
  }

  /**
   * Returns the min txnid seen open by any active transaction
   * @deprecated remove when min_history_level is dropped
   * @return txnId
   */
  @Override
  @RetrySemantics.Idempotent
  @Deprecated
  public long findMinTxnIdSeenOpen() {
    if (!ConfVars.useMinHistoryLevel() || ConfVars.useMinHistoryWriteId()) {
      return Long.MAX_VALUE;
    }
    try {
      Long minId = jdbcResource.getJdbcTemplate().queryForObject("SELECT MIN(\"MHL_MIN_OPEN_TXNID\") FROM \"MIN_HISTORY_LEVEL\"",
          new MapSqlParameterSource(), Long.class);
      return minId == null ? Long.MAX_VALUE : minId;
    } catch (DataAccessException e) {
      if (dbProduct.isTableNotExistsError(e)) {
        ConfVars.setUseMinHistoryLevel(false);
        return Long.MAX_VALUE;
      }
      LOG.error("Unable to execute findMinTxnIdSeenOpen", e);
      throw e;
    }
  }

  @Override
  public Optional<CompactionInfo> getCompactionByTxnId(long txnId) throws MetaException {
    return Optional.ofNullable(jdbcResource.execute(new GetCompactionInfoHandler(txnId, true)));
  }

  @Override
  public boolean updateCompactionMetricsData(CompactionMetricsData data) throws MetaException {
    return new UpdateCompactionMetricsDataFunction(data).execute(jdbcResource);
  }

  @Override
  public List<CompactionMetricsData> getTopCompactionMetricsDataPerType(int limit)      
      throws MetaException {
    return new TopCompactionMetricsDataPerTypeFunction(limit).execute(jdbcResource);
  }

  @Override
  public CompactionMetricsData getCompactionMetricsData(String dbName, String tblName, String partitionName,
      CompactionMetricsData.MetricType type) throws MetaException {
    return jdbcResource.execute(new CompactionMetricsDataHandler(dbName, tblName, partitionName, type));
  }

  @Override
  public void removeCompactionMetricsData(String dbName, String tblName, String partitionName,
      CompactionMetricsData.MetricType type) throws MetaException {
    jdbcResource.execute(new RemoveCompactionMetricsDataCommand(dbName, tblName, partitionName, type));
  }

}