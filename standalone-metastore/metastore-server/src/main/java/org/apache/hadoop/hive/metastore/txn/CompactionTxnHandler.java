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
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.events.CommitCompactionEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;


/**
 * Extends the transaction handler with methods needed only by the compactor threads.  These
 * methods are not available through the thrift interface.
 */
class CompactionTxnHandler extends TxnHandler {
  static final private String CLASS_NAME = CompactionTxnHandler.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  private static final String SELECT_COMPACTION_QUEUE_BY_TXN_ID =
      "SELECT \"CQ_ID\", \"CQ_DATABASE\", \"CQ_TABLE\", \"CQ_PARTITION\", "
          + "\"CQ_STATE\", \"CQ_TYPE\", \"CQ_TBLPROPERTIES\", \"CQ_WORKER_ID\", \"CQ_START\", \"CQ_RUN_AS\", "
          + "\"CQ_HIGHEST_WRITE_ID\", \"CQ_META_INFO\", \"CQ_HADOOP_JOB_ID\", \"CQ_ERROR_MESSAGE\", "
          + "\"CQ_ENQUEUE_TIME\" FROM \"COMPACTION_QUEUE\" WHERE \"CQ_TXN_ID\" = ?";

  public CompactionTxnHandler() {
  }

  /**
   * This will look through the completed_txn_components table and look for partitions or tables
   * that may be ready for compaction.  Also, look through txns and txn_components tables for
   * aborted transactions that we should add to the list.
   * @param abortedThreshold  number of aborted queries forming a potential compaction request.
   * @return list of CompactionInfo structs.  These will not have id, type,
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
      long abortedTimeThreshold, long checkInterval) throws MetaException {
    Connection dbConn = null;
    Set<CompactionInfo> response = new HashSet<>();
    Statement stmt = null;
    ResultSet rs = null;
    try {
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        // Check for completed transactions
        final String s = "SELECT DISTINCT \"TC\".\"CTC_DATABASE\", \"TC\".\"CTC_TABLE\", \"TC\".\"CTC_PARTITION\" " +
          "FROM \"COMPLETED_TXN_COMPONENTS\" \"TC\" " + (checkInterval > 0 ?
          "LEFT JOIN ( " +
          "  SELECT \"C1\".* FROM \"COMPLETED_COMPACTIONS\" \"C1\" " +
          "  INNER JOIN ( " +
          "    SELECT MAX(\"CC_ID\") \"CC_ID\" FROM \"COMPLETED_COMPACTIONS\" " +
          "    GROUP BY \"CC_DATABASE\", \"CC_TABLE\", \"CC_PARTITION\"" +
          "  ) \"C2\" " +
          "  ON \"C1\".\"CC_ID\" = \"C2\".\"CC_ID\" " +
          "  WHERE \"C1\".\"CC_STATE\" IN (" + quoteChar(ATTEMPTED_STATE) + "," + quoteChar(FAILED_STATE) + ")" +
          ") \"C\" " +
          "ON \"TC\".\"CTC_DATABASE\" = \"C\".\"CC_DATABASE\" AND \"TC\".\"CTC_TABLE\" = \"C\".\"CC_TABLE\" " +
          "  AND (\"TC\".\"CTC_PARTITION\" = \"C\".\"CC_PARTITION\" OR (\"TC\".\"CTC_PARTITION\" IS NULL AND \"C\".\"CC_PARTITION\" IS NULL)) " +
          "WHERE \"C\".\"CC_ID\" IS NOT NULL OR " + isWithinCheckInterval("\"TC\".\"CTC_TIMESTAMP\"", checkInterval) : "");

        LOG.debug("Going to execute query <" + s + ">");
        rs = stmt.executeQuery(s);
        while (rs.next()) {
          CompactionInfo info = new CompactionInfo();
          info.dbname = rs.getString(1);
          info.tableName = rs.getString(2);
          info.partName = rs.getString(3);
          response.add(info);
        }
        rs.close();

        // Check for aborted txns: number of aborted txns past threshold and age of aborted txns
        // past time threshold
        boolean checkAbortedTimeThreshold = abortedTimeThreshold >= 0;
        String sCheckAborted = "SELECT \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\", " +
          "MIN(\"TXN_STARTED\"), COUNT(*) FROM \"TXNS\", \"TXN_COMPONENTS\" " +
          "   WHERE \"TXN_ID\" = \"TC_TXNID\" AND \"TXN_STATE\" = " + TxnStatus.ABORTED + " " +
          "GROUP BY \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\" " +
              (checkAbortedTimeThreshold ? "" : " HAVING COUNT(*) > " + abortedThreshold);

        LOG.debug("Going to execute query <" + sCheckAborted + ">");
        rs = stmt.executeQuery(sCheckAborted);
        long systemTime = System.currentTimeMillis();
        while (rs.next()) {
          boolean pastTimeThreshold =
              checkAbortedTimeThreshold && rs.getLong(4) + abortedTimeThreshold < systemTime;
          int numAbortedTxns = rs.getInt(5);
          if (numAbortedTxns > abortedThreshold || pastTimeThreshold) {
            CompactionInfo info = new CompactionInfo();
            info.dbname = rs.getString(1);
            info.tableName = rs.getString(2);
            info.partName = rs.getString(3);
            info.tooManyAborts = numAbortedTxns > abortedThreshold;
            info.hasOldAbort = pastTimeThreshold;
            if (LOG.isDebugEnabled()) {
              LOG.debug("Found potential compaction: " + info.toString());
            }
            response.add(info);
          }
        }
      } catch (SQLException e) {
        LOG.error("Unable to connect to transaction database " + e.getMessage());
        checkRetryable(dbConn, e,
            "findPotentialCompactions(maxAborted:" + abortedThreshold
                + ", abortedTimeThreshold:" + abortedTimeThreshold + ")");
      } finally {
        close(rs, stmt, dbConn);
      }
      return response;
    }
    catch (RetryException e) {
      return findPotentialCompactions(abortedThreshold, abortedTimeThreshold, checkInterval);
    }
  }

  /**
   * This will grab the next compaction request off of
   * the queue, and assign it to the worker.
   * @param workerId id of the worker calling this, will be recorded in the db
   * @return an info element for this compaction request, or null if there is no work to do now.
   */
  @Override
  @RetrySemantics.SafeToRetry
  public CompactionInfo findNextToCompact(String workerId) throws MetaException {
    try {
      Connection dbConn = null;
      Statement stmt = null;
      //need a separate stmt for executeUpdate() otherwise it will close the ResultSet(HIVE-12725)
      Statement updStmt = null;
      ResultSet rs = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        String s = "SELECT \"CQ_ID\", \"CQ_DATABASE\", \"CQ_TABLE\", \"CQ_PARTITION\", " +
          "\"CQ_TYPE\", \"CQ_TBLPROPERTIES\" FROM \"COMPACTION_QUEUE\" WHERE \"CQ_STATE\" = '" + INITIATED_STATE + "'";
        LOG.debug("Going to execute query <" + s + ">");
        rs = stmt.executeQuery(s);
        if (!rs.next()) {
          LOG.debug("No compactions found ready to compact");
          dbConn.rollback();
          return null;
        }
        updStmt = dbConn.createStatement();
        do {
          CompactionInfo info = new CompactionInfo();
          info.id = rs.getLong(1);
          info.dbname = rs.getString(2);
          info.tableName = rs.getString(3);
          info.partName = rs.getString(4);
          info.type = dbCompactionType2ThriftType(rs.getString(5).charAt(0));
          info.properties = rs.getString(6);
          // Now, update this record as being worked on by this worker.
          long now = getDbTime(dbConn);
          s = "UPDATE \"COMPACTION_QUEUE\" SET \"CQ_WORKER_ID\" = '" + workerId + "', " +
            "\"CQ_START\" = " + now + ", \"CQ_STATE\" = '" + WORKING_STATE + "' WHERE \"CQ_ID\" = " + info.id +
            " AND \"CQ_STATE\"='" + INITIATED_STATE + "'";
          LOG.debug("Going to execute update <" + s + ">");
          int updCount = updStmt.executeUpdate(s);
          if(updCount == 1) {
            dbConn.commit();
            return info;
          }
          if(updCount == 0) {
            LOG.debug("Another Worker picked up " + info);
            continue;
          }
          LOG.error("Unable to set to cq_state=" + WORKING_STATE + " for compaction record: " +
            info + ". updCnt=" + updCount + ".");
          dbConn.rollback();
          return null;
        } while( rs.next());
        dbConn.rollback();
        return null;
      } catch (SQLException e) {
        LOG.error("Unable to select next element for compaction, " + e.getMessage());
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "findNextToCompact(workerId:" + workerId + ")");
        throw new MetaException("Unable to connect to transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        closeStmt(updStmt);
        close(rs, stmt, dbConn);
      }
    } catch (RetryException e) {
      return findNextToCompact(workerId);
    }
  }

  /**
   * This will mark an entry in the queue as compacted
   * and put it in the ready to clean state.
   * @param info info on the compaction entry to mark as compacted.
   */
  @Override
  @RetrySemantics.SafeToRetry
  public void markCompacted(CompactionInfo info) throws MetaException {
    try {
      Connection dbConn = null;
      Statement stmt = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        String s = "UPDATE \"COMPACTION_QUEUE\" SET \"CQ_STATE\" = '" + READY_FOR_CLEANING + "', "
            + "\"CQ_WORKER_ID\" = NULL"
            + " WHERE \"CQ_ID\" = " + info.id;
        LOG.debug("Going to execute update <" + s + ">");
        int updCnt = stmt.executeUpdate(s);
        if (updCnt != 1) {
          LOG.error("Unable to set cq_state=" + READY_FOR_CLEANING + " for compaction record: " + info + ". updCnt=" + updCnt);
          LOG.debug("Going to rollback");
          dbConn.rollback();
        }
        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.error("Unable to update compaction queue " + e.getMessage());
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "markCompacted(" + info + ")");
        throw new MetaException("Unable to connect to transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        closeStmt(stmt);
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      markCompacted(info);
    }
  }

  /**
   * Find entries in the queue that are ready to
   * be cleaned.
   * @param minOpenTxnWaterMark Minimum open txnId
   * @param retentionTime time in milliseconds to delay cleanup after compaction
   * @return information on the entry in the queue.
   */
  @Override
  @RetrySemantics.ReadOnly
  public List<CompactionInfo> findReadyToClean(long minOpenTxnWaterMark, long retentionTime) throws MetaException {
    Connection dbConn = null;
    List<CompactionInfo> rc = new ArrayList<>();

    Statement stmt = null;
    ResultSet rs = null;
    try {
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        /*
         * By filtering on minOpenTxnWaterMark, we will only cleanup after every transaction is committed, that could see
         * the uncompacted deltas. This way the cleaner can clean up everything that was made obsolete by this compaction.
         */
        String s = "SELECT \"CQ_ID\", \"CQ_DATABASE\", \"CQ_TABLE\", \"CQ_PARTITION\", "
                + "\"CQ_TYPE\", \"CQ_RUN_AS\", \"CQ_HIGHEST_WRITE_ID\" FROM \"COMPACTION_QUEUE\" WHERE \"CQ_STATE\" = '"
                + READY_FOR_CLEANING + "'";
        if (minOpenTxnWaterMark > 0) {
          s = s + " AND (\"CQ_NEXT_TXN_ID\" <= " + minOpenTxnWaterMark + " OR \"CQ_NEXT_TXN_ID\" IS NULL)";
        }
        if (retentionTime > 0) {
          s = s + " AND \"CQ_COMMIT_TIME\" < (" + TxnDbUtil.getEpochFn(dbProduct) + " - " + retentionTime + ")";
        }
        LOG.debug("Going to execute query <" + s + ">");
        rs = stmt.executeQuery(s);

        while (rs.next()) {
          CompactionInfo info = new CompactionInfo();
          info.id = rs.getLong(1);
          info.dbname = rs.getString(2);
          info.tableName = rs.getString(3);
          info.partName = rs.getString(4);
          info.type = dbCompactionType2ThriftType(rs.getString(5).charAt(0));
          info.runAs = rs.getString(6);
          info.highestWriteId = rs.getLong(7);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found ready to clean: " + info.toString());
          }
          rc.add(info);
        }
        return rc;
      } catch (SQLException e) {
        LOG.error("Unable to select next element for cleaning, " + e.getMessage());
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "findReadyToClean");
        throw new MetaException("Unable to connect to transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        close(rs, stmt, dbConn);
      }
    } catch (RetryException e) {
      return findReadyToClean(minOpenTxnWaterMark, retentionTime);
    }
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("Running markCleaned with CompactionInfo: " + info.toString());
    }
    try {
      Connection dbConn = null;
      PreparedStatement pStmt = null;
      ResultSet rs = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        String s = "INSERT INTO \"COMPLETED_COMPACTIONS\"(\"CC_ID\", \"CC_DATABASE\", "
            + "\"CC_TABLE\", \"CC_PARTITION\", \"CC_STATE\", \"CC_TYPE\", \"CC_TBLPROPERTIES\", \"CC_WORKER_ID\", "
            + "\"CC_START\", \"CC_END\", \"CC_RUN_AS\", \"CC_HIGHEST_WRITE_ID\", \"CC_META_INFO\", "
            + "\"CC_HADOOP_JOB_ID\", \"CC_ERROR_MESSAGE\", \"CC_ENQUEUE_TIME\") "
          + "SELECT \"CQ_ID\", \"CQ_DATABASE\", \"CQ_TABLE\", \"CQ_PARTITION\", "
            + quoteChar(SUCCEEDED_STATE) + ", \"CQ_TYPE\", \"CQ_TBLPROPERTIES\", \"CQ_WORKER_ID\", \"CQ_START\", "
            + TxnDbUtil.getEpochFn(dbProduct) + ", \"CQ_RUN_AS\", \"CQ_HIGHEST_WRITE_ID\", \"CQ_META_INFO\", "
            + "\"CQ_HADOOP_JOB_ID\", \"CQ_ERROR_MESSAGE\", \"CQ_ENQUEUE_TIME\" "
            + "FROM \"COMPACTION_QUEUE\" WHERE \"CQ_ID\" = ?";
        pStmt = dbConn.prepareStatement(s);
        pStmt.setLong(1, info.id);
        LOG.debug("Going to execute update <" + s + "> for CQ_ID=" + info.id);
        pStmt.executeUpdate();

        s = "DELETE FROM \"COMPACTION_QUEUE\" WHERE \"CQ_ID\" = ?";
        pStmt = dbConn.prepareStatement(s);
        pStmt.setLong(1, info.id);
        LOG.debug("Going to execute update <" + s + ">");
        int updCount = pStmt.executeUpdate();
        if (updCount != 1) {
          LOG.error("Unable to delete compaction record: " + info +  ".  Update count=" + updCount);
          LOG.debug("Going to rollback");
          dbConn.rollback();
        }
        // Remove entries from completed_txn_components as well, so we don't start looking there
        // again but only up to the highest write ID include in this compaction job.
        //highestWriteId will be NULL in upgrade scenarios
        s = "DELETE FROM \"COMPLETED_TXN_COMPONENTS\" WHERE \"CTC_DATABASE\" = ? AND \"CTC_TABLE\" = ?";
        if (info.partName != null) {
          s += " AND \"CTC_PARTITION\" = ?";
        }
        if(info.highestWriteId != 0) {
          s += " AND \"CTC_WRITEID\" <= ?";
        }
        pStmt = dbConn.prepareStatement(s);
        int paramCount = 1;
        pStmt.setString(paramCount++, info.dbname);
        pStmt.setString(paramCount++, info.tableName);
        if (info.partName != null) {
          pStmt.setString(paramCount++, info.partName);
        }
        if(info.highestWriteId != 0) {
          pStmt.setLong(paramCount, info.highestWriteId);
        }
        LOG.debug("Going to execute update <" + s + ">");
        if ((updCount = pStmt.executeUpdate()) < 1) {
          LOG.warn("Expected to remove at least one row from completed_txn_components when " +
            "marking compaction entry as clean!");
        }
        LOG.debug("Removed " + updCount + " records from completed_txn_components");
        /*
         * compaction may remove data from aborted txns above tc_writeid bit it only guarantees to
         * remove it up to (inclusive) tc_writeid, so it's critical to not remove metadata about
         * aborted TXN_COMPONENTS above tc_writeid (and consequently about aborted txns).
         * See {@link ql.txn.compactor.Cleaner.removeFiles()}
         */
        s = "DELETE FROM \"TXN_COMPONENTS\" WHERE \"TC_TXNID\" IN ( "
              + "SELECT \"TXN_ID\" FROM \"TXNS\" WHERE \"TXN_STATE\" = " + TxnStatus.ABORTED + ") "
            + "AND \"TC_DATABASE\" = ? AND \"TC_TABLE\" = ? "
            + "AND \"TC_PARTITION\" "+ (info.partName != null ? "= ?" : "IS NULL");

        List<String> queries = new ArrayList<>();
        Iterator<Long> writeIdsIter = null;
        List<Integer> counts = null;

        if (info.isSetWriteIds && !info.writeIds.isEmpty()) {
          StringBuilder prefix = new StringBuilder(s).append(" AND ");
          List<String> questions = Collections.nCopies(info.writeIds.size(), "?");

          counts = TxnUtils.buildQueryWithINClauseStrings(conf, queries, prefix,
            new StringBuilder(), questions, "\"TC_WRITEID\"", false, false);
          writeIdsIter = info.writeIds.iterator();
        } else if (!info.isSetWriteIds){
          if (info.highestWriteId != 0) {
            s += " AND \"TC_WRITEID\" <= ?";
          }
          queries.add(s);
        }

        for (int i = 0; i < queries.size(); i++) {
          String query = queries.get(i);
          int writeIdCount = (counts != null) ? counts.get(i) : 0;

          LOG.debug("Going to execute update <" + query + ">");
          pStmt = dbConn.prepareStatement(query);
          paramCount = 1;

          pStmt.setString(paramCount++, info.dbname);
          pStmt.setString(paramCount++, info.tableName);
          if (info.partName != null) {
            pStmt.setString(paramCount++, info.partName);
          }
          if (info.highestWriteId != 0 && writeIdCount == 0) {
            pStmt.setLong(paramCount, info.highestWriteId);
          }
          for (int j = 0; j < writeIdCount; j++) {
            if (writeIdsIter.hasNext()) {
              pStmt.setLong(paramCount + j, writeIdsIter.next());
            }
          }
          int rc = pStmt.executeUpdate();
          LOG.debug("Removed " + rc + " records from txn_components");
          // Don't bother cleaning from the txns table.  A separate call will do that.  We don't
          // know here which txns still have components from other tables or partitions in the
          // table, so we don't know which ones we can and cannot clean.
        }
        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.error("Unable to delete from compaction queue " + e.getMessage());
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "markCleaned(" + info + ")");
        throw new MetaException("Unable to connect to transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        close(rs, pStmt, dbConn);
      }
    } catch (RetryException e) {
      markCleaned(info);
    }
  }

  /**
   * Clean up entries from TXN_TO_WRITE_ID table less than min_uncommited_txnid as found by
   * min(max(TXNS.txn_id), min(WRITE_SET.WS_COMMIT_ID), min(Aborted TXNS.txn_id)).
   */
  @Override
  @RetrySemantics.SafeToRetry
  public void cleanTxnToWriteIdTable() throws MetaException {
    try {
      Connection dbConn = null;
      Statement stmt = null;
      ResultSet rs = null;

      try {
        // We query for minimum values in all the queries and they can only increase by any concurrent
        // operations. So, READ COMMITTED is sufficient.
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();

        // First need to find the min_uncommitted_txnid which is currently seen by any open transactions.
        // If there are no txns which are currently open or aborted in the system, then current value of
        // max(TXNS.txn_id) could be min_uncommitted_txnid.
        String s = "SELECT MIN(\"RES\".\"ID\") AS \"ID\" FROM (" +
            "SELECT MAX(\"TXN_ID\") + 1 AS \"ID\" FROM \"TXNS\" " +
            "UNION " +
            "SELECT MIN(\"WS_COMMIT_ID\") AS \"ID\" FROM \"WRITE_SET\" " +
            "UNION " +
            "SELECT MIN(\"TXN_ID\") AS \"ID\" FROM \"TXNS\" WHERE \"TXN_STATE\" = " + TxnStatus.ABORTED +
            " OR \"TXN_STATE\" = " + TxnStatus.OPEN +
            ") \"RES\"";
        LOG.debug("Going to execute query <" + s + ">");
        rs = stmt.executeQuery(s);
        if (!rs.next()) {
          throw new MetaException("Transaction tables not properly initialized, no record found in TXNS");
        }
        long minUncommitedTxnid = rs.getLong(1);

        // As all txns below min_uncommitted_txnid are either committed or empty_aborted, we are allowed
        // to cleanup the entries less than min_uncommitted_txnid from the TXN_TO_WRITE_ID table.
        s = "DELETE FROM \"TXN_TO_WRITE_ID\" WHERE \"T2W_TXNID\" < " + minUncommitedTxnid;
        LOG.debug("Going to execute delete <" + s + ">");
        int rc = stmt.executeUpdate(s);
        LOG.info("Removed " + rc + " rows from TXN_TO_WRITE_ID with Txn Low-Water-Mark: " + minUncommitedTxnid);

        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.error("Unable to delete from txns table " + e.getMessage());
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "cleanTxnToWriteIdTable");
        throw new MetaException("Unable to connect to transaction database " +
                StringUtils.stringifyException(e));
      } finally {
        close(rs, stmt, dbConn);
      }
    } catch (RetryException e) {
      cleanTxnToWriteIdTable();
    }
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
    try {
      Connection dbConn = null;
      Statement stmt = null;
      ResultSet rs = null;
      try {
        //Aborted and committed are terminal states, so nothing about the txn can change
        //after that, so READ COMMITTED is sufficient.
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        /*
         * Only delete aborted / committed transaction in a way that guarantees two things:
         * 1. never deletes anything that is inside the TXN_OPENTXN_TIMEOUT window
         * 2. never deletes the maximum txnId even if it is before the TXN_OPENTXN_TIMEOUT window
          */
        long lowWaterMark = getOpenTxnTimeoutLowBoundaryTxnId(dbConn);

        String s = "SELECT \"TXN_ID\" FROM \"TXNS\" WHERE " +
            "\"TXN_ID\" NOT IN (SELECT \"TC_TXNID\" FROM \"TXN_COMPONENTS\") AND " +
            " (\"TXN_STATE\" = " + TxnStatus.ABORTED + " OR \"TXN_STATE\" = " + TxnStatus.COMMITTED + ")  AND "
            + " \"TXN_ID\" < " + lowWaterMark;
        LOG.debug("Going to execute query <" + s + ">");
        rs = stmt.executeQuery(s);
        List<Long> txnids = new ArrayList<>();
        while (rs.next()) txnids.add(rs.getLong(1));
        close(rs);
        if(txnids.size() <= 0) {
          return;
        }
        Collections.sort(txnids);//easier to read logs

        List<String> queries = new ArrayList<>();
        StringBuilder prefix = new StringBuilder();
        StringBuilder suffix = new StringBuilder();

        // Delete from TXNS.
        prefix.append("DELETE FROM \"TXNS\" WHERE ");

        TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix, txnids, "\"TXN_ID\"", false, false);

        for (String query : queries) {
          LOG.debug("Going to execute update <" + query + ">");
          int rc = stmt.executeUpdate(query);
          LOG.debug("Removed " + rc + "  empty Aborted and Committed transactions from TXNS");
        }
        LOG.info("Aborted and committed transactions removed from TXNS: " + txnids);
        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.error("Unable to delete from txns table " + e.getMessage());
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "cleanEmptyAbortedTxns");
        throw new MetaException("Unable to connect to transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        close(rs, stmt, dbConn);
      }
    } catch (RetryException e) {
      cleanEmptyAbortedAndCommittedTxns();
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
    try {
      Connection dbConn = null;
      Statement stmt = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        String s = "UPDATE \"COMPACTION_QUEUE\" SET \"CQ_WORKER_ID\" = NULL, \"CQ_START\" = NULL, \"CQ_STATE\" = '"
          + INITIATED_STATE+ "' WHERE \"CQ_STATE\" = '" + WORKING_STATE + "' AND \"CQ_WORKER_ID\" LIKE '"
          +  hostname + "%'";
        LOG.debug("Going to execute update <" + s + ">");
        // It isn't an error if the following returns no rows, as the local workers could have died
        // with  nothing assigned to them.
        int updated = stmt.executeUpdate(s);
        LOG.debug("Set " + updated + " compaction queue entries to " + INITIATED_RESPONSE + " state for host "
            + hostname);
        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.error("Unable to change dead worker's records back to initiated state " +
          e.getMessage());
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "revokeFromLocalWorkers(hostname:" + hostname +")");
        throw new MetaException("Unable to connect to transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        closeStmt(stmt);
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      revokeFromLocalWorkers(hostname);
    }
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
    try {
      Connection dbConn = null;
      Statement stmt = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        long latestValidStart = getDbTime(dbConn) - timeout;
        stmt = dbConn.createStatement();
        String s = "UPDATE \"COMPACTION_QUEUE\" SET \"CQ_WORKER_ID\" = NULL, \"CQ_START\" = NULL, \"CQ_STATE\" = '"
          + INITIATED_STATE+ "' WHERE \"CQ_STATE\" = '" + WORKING_STATE + "' AND \"CQ_START\" < "
          +  latestValidStart;
        LOG.debug("Going to execute update <" + s + ">");
        // It isn't an error if the following returns no rows, as the local workers could have died
        // with  nothing assigned to them.
        int updated = stmt.executeUpdate(s);
        LOG.info(updated + " compaction queue entries timed out, set back to " + INITIATED_RESPONSE + " state. Latest "
            + "valid start: " + latestValidStart);
        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.error("Unable to change dead worker's records back to initiated state " +
          e.getMessage());
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "revokeTimedoutWorkers(timeout:" + timeout + ")");
        throw new MetaException("Unable to connect to transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        closeStmt(stmt);
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      revokeTimedoutWorkers(timeout);
    }
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("Finding columns with statistics info for CompactionInfo: " + ci.toString());
    }
    Connection dbConn = null;
    PreparedStatement pStmt = null;
    ResultSet rs = null;
    try {
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        String quote = getIdentifierQuoteString(dbConn);
        StringBuilder bldr = new StringBuilder();
        bldr.append("SELECT ").append(quote).append("COLUMN_NAME").append(quote)
          .append(" FROM ")
          .append(quote).append((ci.partName == null ? "TAB_COL_STATS" : "PART_COL_STATS"))
          .append(quote)
          .append(" WHERE ")
          .append(quote).append("DB_NAME").append(quote).append(" = ?")
          .append(" AND ").append(quote).append("TABLE_NAME").append(quote)
          .append(" = ?");
        if (ci.partName != null) {
          bldr.append(" AND ").append(quote).append("PARTITION_NAME").append(quote).append(" = ?");
        }
        String s = bldr.toString();
        pStmt = dbConn.prepareStatement(s);
        pStmt.setString(1, ci.dbname);
        pStmt.setString(2, ci.tableName);
        if (ci.partName != null) {
          pStmt.setString(3, ci.partName);
        }

      /*String s = "SELECT COLUMN_NAME FROM " + (ci.partName == null ? "TAB_COL_STATS" :
          "PART_COL_STATS")
         + " WHERE DB_NAME='" + ci.dbname + "' AND TABLE_NAME='" + ci.tableName + "'"
        + (ci.partName == null ? "" : " AND PARTITION_NAME='" + ci.partName + "'");*/
        LOG.debug("Going to execute <" + s + ">");
        rs = pStmt.executeQuery();
        List<String> columns = new ArrayList<>();
        while (rs.next()) {
          columns.add(rs.getString(1));
        }
        LOG.debug("Found columns to update stats: " + columns + " on " + ci.tableName +
          (ci.partName == null ? "" : "/" + ci.partName));
        dbConn.commit();
        return columns;
      } catch (SQLException e) {
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "findColumnsWithStats(" + ci.tableName +
          (ci.partName == null ? "" : "/" + ci.partName) + ")");
        throw new MetaException("Unable to connect to transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        close(rs, pStmt, dbConn);
      }
    } catch (RetryException ex) {
      return findColumnsWithStats(ci);
    }
  }

  @Override
  public void updateCompactorState(CompactionInfo ci, long compactionTxnId) throws MetaException {
    Connection dbConn = null;
    Statement stmt = null;
    try {
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        String sqlText = "UPDATE \"COMPACTION_QUEUE\" SET \"CQ_HIGHEST_WRITE_ID\" = " +
            ci.highestWriteId + ", \"CQ_RUN_AS\" = " + quoteString(ci.runAs) + ", \"CQ_TXN_ID\" = " + compactionTxnId +
            " WHERE \"CQ_ID\" = " + ci.id;
        if(LOG.isDebugEnabled()) {
          LOG.debug("About to execute: " + sqlText);
        }
        int updCount = stmt.executeUpdate(sqlText);
        if(updCount != 1) {
          throw new IllegalStateException("Could not find record in COMPACTION_QUEUE for " + ci);
        }
        /*We make an entry in TXN_COMPONENTS for the partition/table that the compactor is
        * working on in case this txn aborts and so we need to ensure that its TXNS entry is
        * not removed until Cleaner has removed all files that this txn may have written, i.e.
        * make it work the same way as any other write.  TC_WRITEID is set to the highest
        * WriteId that this compactor run considered since there compactor doesn't allocate
        * a new write id (so as not to invalidate result set caches/materialized views) but
        * we need to set it to something to that markCleaned() only cleans TXN_COMPONENTS up to
        * the level to which aborted files/data has been cleaned.*/
        sqlText = "INSERT INTO \"TXN_COMPONENTS\"(" +
            "\"TC_TXNID\", " +
            "\"TC_DATABASE\", " +
            "\"TC_TABLE\", " +
            (ci.partName == null ? "" : "\"TC_PARTITION\", ") +
            "\"TC_WRITEID\", " +
            "\"TC_OPERATION_TYPE\")" +
            " VALUES(" +
            compactionTxnId + "," +
            quoteString(ci.dbname) + "," +
            quoteString(ci.tableName) + "," +
            (ci.partName == null ? "" : quoteString(ci.partName) + ",") +
            ci.highestWriteId + ", " +
            OperationType.COMPACT + ")";
        if(LOG.isDebugEnabled()) {
          LOG.debug("About to execute: " + sqlText);
        }
        updCount = stmt.executeUpdate(sqlText);
        if(updCount != 1) {
          throw new IllegalStateException("Could not find record in COMPACTION_QUEUE for " + ci);
        }
        dbConn.commit();
      } catch (SQLException e) {
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "updateCompactorState(" + ci + "," + compactionTxnId +")");
        throw new MetaException("Unable to connect to transaction database " +
            StringUtils.stringifyException(e));
      } finally {
        close(null, stmt, dbConn);
      }
    } catch (RetryException ex) {
      updateCompactorState(ci, compactionTxnId);
    }
  }

  private static class RetentionCounters {
    int attemptedRetention = 0;
    int failedRetention = 0;
    int succeededRetention = 0;

    RetentionCounters(int attemptedRetention, int failedRetention, int succeededRetention) {
      this.attemptedRetention = attemptedRetention;
      this.failedRetention = failedRetention;
      this.succeededRetention = succeededRetention;
    }
  }

  private void checkForDeletion(List<Long> deleteSet, CompactionInfo ci, RetentionCounters rc) {
    switch (ci.state) {
      case ATTEMPTED_STATE:
        if(--rc.attemptedRetention < 0) {
          deleteSet.add(ci.id);
        }
        break;
      case FAILED_STATE:
        if(--rc.failedRetention < 0) {
          deleteSet.add(ci.id);
        }
        break;
      case SUCCEEDED_STATE:
        if(--rc.succeededRetention < 0) {
          deleteSet.add(ci.id);
        }
        break;
      default:
        //do nothing to hanlde future RU/D where we may want to add new state types
    }
  }

  /**
   * For any given compactable entity (partition; table if not partitioned) the history of compactions
   * may look like "sssfffaaasffss", for example.  The idea is to retain the tail (most recent) of the
   * history such that a configurable number of each type of state is present.  Any other entries
   * can be purged.  This scheme has advantage of always retaining the last failure/success even if
   * it's not recent.
   * @throws MetaException
   */
  @Override
  @RetrySemantics.SafeToRetry
  public void purgeCompactionHistory() throws MetaException {
    Connection dbConn = null;
    Statement stmt = null;
    PreparedStatement pStmt = null;
    ResultSet rs = null;
    List<Long> deleteSet = new ArrayList<>();
    RetentionCounters rc = null;
    try {
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        /* cc_id is monotonically increasing so for any entity sorts in order of compaction history,
        thus this query groups by entity and withing group sorts most recent first */
        rs = stmt.executeQuery("SELECT \"CC_ID\", \"CC_DATABASE\", \"CC_TABLE\", \"CC_PARTITION\", \"CC_STATE\" "
            + "FROM \"COMPLETED_COMPACTIONS\" ORDER BY \"CC_DATABASE\", \"CC_TABLE\", \"CC_PARTITION\", \"CC_ID\" DESC");
        String lastCompactedEntity = null;
        /* In each group, walk from most recent and count occurrences of each state type.  Once you
        * have counted enough (for each state) to satisfy retention policy, delete all other
        * instances of this status. */
        while(rs.next()) {
          CompactionInfo ci = new CompactionInfo(
              rs.getLong(1), rs.getString(2), rs.getString(3),
              rs.getString(4), rs.getString(5).charAt(0));
          if(!ci.getFullPartitionName().equals(lastCompactedEntity)) {
            lastCompactedEntity = ci.getFullPartitionName();
            rc = new RetentionCounters(MetastoreConf.getIntVar(conf, ConfVars.COMPACTOR_HISTORY_RETENTION_ATTEMPTED),
              getFailedCompactionRetention(),
              MetastoreConf.getIntVar(conf, ConfVars.COMPACTOR_HISTORY_RETENTION_SUCCEEDED));
          }
          checkForDeletion(deleteSet, ci, rc);
        }
        close(rs);

        if (deleteSet.size() <= 0) {
          return;
        }

        List<String> queries = new ArrayList<>();

        StringBuilder prefix = new StringBuilder();
        StringBuilder suffix = new StringBuilder();

        prefix.append("DELETE FROM \"COMPLETED_COMPACTIONS\" WHERE ");

        List<String> questions = new ArrayList<>(deleteSet.size());
        for (int  i = 0; i < deleteSet.size(); i++) {
          questions.add("?");
        }
        List<Integer> counts = TxnUtils.buildQueryWithINClauseStrings(conf, queries, prefix, suffix, questions,
            "\"CC_ID\"", false, false);
        int totalCount = 0;
        for (int i = 0; i < queries.size(); i++) {
          String query = queries.get(i);
          long insertCount = counts.get(i);
          LOG.debug("Going to execute update <" + query + ">");
          pStmt = dbConn.prepareStatement(query);
          for (int j = 0; j < insertCount; j++) {
            pStmt.setLong(j + 1, deleteSet.get(totalCount + j));
          }
          totalCount += insertCount;
          int count = pStmt.executeUpdate();
          LOG.debug("Removed " + count + " records from COMPLETED_COMPACTIONS");
        }
        dbConn.commit();
      } catch (SQLException e) {
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "purgeCompactionHistory()");
        throw new MetaException("Unable to connect to transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        close(rs, stmt, dbConn);
        closeStmt(pStmt);
      }
    } catch (RetryException ex) {
      purgeCompactionHistory();
    }
  }

  /**
   * this ensures that the number of failed compaction entries retained is > than number of failed
   * compaction threshold which prevents new compactions from being scheduled.
   */
  private int getFailedCompactionRetention() {
    int failedThreshold = MetastoreConf.getIntVar(conf, ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD);
    int failedRetention = MetastoreConf.getIntVar(conf, ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED);
    if(failedRetention < failedThreshold) {
      LOG.warn("Invalid configuration " + ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD.getVarname() +
        "=" + failedRetention + " < " + ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED + "=" +
        failedRetention + ".  Will use " + ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD.getVarname() +
        "=" + failedRetention);
      failedRetention = failedThreshold;
    }
    return failedRetention;
  }

  /**
   * Returns {@code true} if there already exists sufficient number of consecutive failures for
   * this table/partition so that no new automatic compactions will be scheduled.
   * User initiated compactions don't do this check.
   *
   * Do we allow compacting whole table (when it's partitioned)?  No, though perhaps we should.
   * That would be a meta operations, i.e. first find all partitions for this table (which have
   * txn info) and schedule each compaction separately.  This avoids complications in this logic.
   */
  @Override
  @RetrySemantics.ReadOnly
  public boolean checkFailedCompactions(CompactionInfo ci) throws MetaException {
    Connection dbConn = null;
    PreparedStatement pStmt = null;
    ResultSet rs = null;
    try {
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        pStmt = dbConn.prepareStatement("SELECT \"CC_STATE\" FROM \"COMPLETED_COMPACTIONS\" WHERE " +
          "\"CC_DATABASE\" = ? AND " +
          "\"CC_TABLE\" = ? " +
          (ci.partName != null ? "AND \"CC_PARTITION\" = ?" : "") +
          " AND \"CC_STATE\" != " + quoteChar(ATTEMPTED_STATE) + " ORDER BY \"CC_ID\" DESC");
        pStmt.setString(1, ci.dbname);
        pStmt.setString(2, ci.tableName);
        if (ci.partName != null) {
          pStmt.setString(3, ci.partName);
        }
        rs = pStmt.executeQuery();
        int numFailed = 0;
        int numTotal = 0;
        int failedThreshold = MetastoreConf.getIntVar(conf, ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD);
        while(rs.next() && ++numTotal <= failedThreshold) {
          if(rs.getString(1).charAt(0) == FAILED_STATE) {
            numFailed++;
          }
          else {
            numFailed--;
          }
        }
        return numFailed == failedThreshold;
      }
      catch (SQLException e) {
        LOG.error("Unable to check for failed compactions " + e.getMessage());
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "checkFailedCompactions(" + ci + ")");
        LOG.error("Unable to connect to transaction database " + StringUtils.stringifyException(e));
        return false;//weren't able to check
      } finally {
        close(rs, pStmt, dbConn);
      }
    } catch (RetryException e) {
      return checkFailedCompactions(ci);
    }
  }

  /**
   * If there is an entry in compaction_queue with ci.id, remove it
   * Make entry in completed_compactions with status 'f'.
   * If there is no entry in compaction_queue, it means Initiator failed to even schedule a compaction,
   * which we record as ATTEMPTED_STATE entry in history.
   */
  @Override
  @RetrySemantics.CannotRetry
  public void markFailed(CompactionInfo ci) throws MetaException {//todo: this should not throw
    if (LOG.isDebugEnabled()) {
      LOG.debug("Marking as failed: CompactionInfo: " + ci.toString());
    }
    try {
      Connection dbConn = null;
      Statement stmt = null;
      PreparedStatement pStmt = null;
      ResultSet rs = null;
      // the error message related to the failure is wrapped inside CompactionInfo
      // fetch this info, since ci will be reused in subsequent queries
      String errorMessage = ci.errorMessage;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        pStmt = dbConn.prepareStatement("SELECT \"CQ_ID\", \"CQ_DATABASE\", \"CQ_TABLE\", \"CQ_PARTITION\", "
            + "\"CQ_STATE\", \"CQ_TYPE\", \"CQ_TBLPROPERTIES\", \"CQ_WORKER_ID\", \"CQ_START\", \"CQ_RUN_AS\", "
            + "\"CQ_HIGHEST_WRITE_ID\", \"CQ_META_INFO\", \"CQ_HADOOP_JOB_ID\", \"CQ_ERROR_MESSAGE\", "
            + "\"CQ_ENQUEUE_TIME\" "
            + "FROM \"COMPACTION_QUEUE\" WHERE \"CQ_ID\" = ?");
        pStmt.setLong(1, ci.id);
        rs = pStmt.executeQuery();
        if (rs.next()) {
          ci = CompactionInfo.loadFullFromCompactionQueue(rs);
          String s = "DELETE FROM \"COMPACTION_QUEUE\" WHERE \"CQ_ID\" = ?";
          pStmt = dbConn.prepareStatement(s);
          pStmt.setLong(1, ci.id);
          LOG.debug("Going to execute update <" + s + ">");
          int updCnt = pStmt.executeUpdate();
        }
        else {
          if(ci.id > 0) {
            //the record with valid CQ_ID has disappeared - this is a sign of something wrong
            throw new IllegalStateException("No record with CQ_ID=" + ci.id + " found in COMPACTION_QUEUE");
          }
        }
        if(ci.id == 0) {
          //The failure occurred before we even made an entry in COMPACTION_QUEUE
          //generate ID so that we can make an entry in COMPLETED_COMPACTIONS
          ci.id = generateCompactionQueueId(stmt);
          //mostly this indicates that the Initiator is paying attention to some table even though
          //compactions are not happening.
          ci.state = ATTEMPTED_STATE;
          //this is not strictly accurate, but 'type' cannot be null.
          if(ci.type == null) {
            ci.type = CompactionType.MINOR;
          }
          ci.start = getDbTime(dbConn);
          LOG.debug("The failure occurred before we even made an entry in COMPACTION_QUEUE. Generated ID so that we "
              + "can make an entry in COMPLETED_COMPACTIONS. New Id: " + ci.id);
        } else {
          ci.state = FAILED_STATE;
        }
        close(rs, stmt, null);
        closeStmt(pStmt);

        pStmt = dbConn.prepareStatement("INSERT INTO \"COMPLETED_COMPACTIONS\" "
            + "(\"CC_ID\", \"CC_DATABASE\", \"CC_TABLE\", \"CC_PARTITION\", \"CC_STATE\", \"CC_TYPE\", "
            + "\"CC_TBLPROPERTIES\", \"CC_WORKER_ID\", \"CC_START\", \"CC_END\", \"CC_RUN_AS\", "
            + "\"CC_HIGHEST_WRITE_ID\", \"CC_META_INFO\", \"CC_HADOOP_JOB_ID\", \"CC_ERROR_MESSAGE\", "
            + "\"CC_ENQUEUE_TIME\") "
            + "VALUES(?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?,?)");
        if (errorMessage != null) {
          ci.errorMessage = errorMessage;
        }
        CompactionInfo.insertIntoCompletedCompactions(pStmt, ci, getDbTime(dbConn));
        int updCount = pStmt.executeUpdate();
        LOG.debug("Inserted " + updCount + " entries into COMPLETED_COMPACTIONS");
        LOG.debug("Going to commit");
        closeStmt(pStmt);
        dbConn.commit();
      } catch (SQLException e) {
        LOG.warn("markFailed(" + ci.id + "):" + e.getMessage());
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "markFailed(" + ci + ")");
        LOG.error("markFailed(" + ci + ") failed: " + e.getMessage(), e);
      } finally {
        close(rs, stmt, null);
        close(null, pStmt, dbConn);
      }
    } catch (RetryException e) {
      markFailed(ci);
    }
  }
  @Override
  @RetrySemantics.Idempotent
  public void setHadoopJobId(String hadoopJobId, long id) {
    try {
      Connection dbConn = null;
      Statement stmt = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        String s = "UPDATE \"COMPACTION_QUEUE\" SET \"CQ_HADOOP_JOB_ID\" = " + quoteString(hadoopJobId)
            + " WHERE \"CQ_ID\" = " + id;
        LOG.debug("Going to execute <" + s + ">  with jobId: " + hadoopJobId + " and CQ id: " + id);
        int updateCount = stmt.executeUpdate(s);
        LOG.debug("Going to commit");
        closeStmt(stmt);
        dbConn.commit();
      } catch (SQLException e) {
        LOG.warn("setHadoopJobId(" + hadoopJobId + "," + id + "):" + e.getMessage());
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "setHadoopJobId(" + hadoopJobId + "," + id + ")");
        LOG.error("setHadoopJobId(" + hadoopJobId + "," + id + ") failed: " + e.getMessage(), e);
      } finally {
        close(null, stmt, dbConn);
      }
    } catch (RetryException e) {
      setHadoopJobId(hadoopJobId, id);
    }
  }

  @Override
  @RetrySemantics.Idempotent
  public long findMinOpenTxnIdForCleaner() throws MetaException {
    Connection dbConn = null;
    try {
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        return getMinOpenTxnIdWaterMark(dbConn);
      } catch (SQLException e) {
        LOG.error("Unable to getMinOpenTxnIdForCleaner", e);
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "getMinOpenTxnForCleaner");
        throw new MetaException("Unable to execute getMinOpenTxnIfForCleaner() " +
            StringUtils.stringifyException(e));
      } finally {
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      return findMinOpenTxnIdForCleaner();
    }
  }

  /**
   * Returns the min txnid seen open by any active transaction
   * @deprecated remove when min_history_level is dropped
   * @return txnId
   * @throws MetaException ex
   */
  @Override
  @RetrySemantics.Idempotent
  @Deprecated
  public long findMinTxnIdSeenOpen() throws MetaException {
    if (!useMinHistoryLevel) {
      return -1L;
    }
    Connection dbConn = null;
    try {
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        long minOpenTxn;
        try (Statement stmt = dbConn.createStatement()) {
          try (ResultSet rs = stmt.executeQuery("SELECT MIN(\"MHL_MIN_OPEN_TXNID\") FROM \"MIN_HISTORY_LEVEL\"")) {
            if (!rs.next()) {
              throw new IllegalStateException("Scalar query returned no rows?!");
            }
            minOpenTxn = rs.getLong(1);
            if (rs.wasNull()) {
              minOpenTxn = -1L;
            }
          }
        }
        dbConn.rollback();
        return minOpenTxn;
      } catch (SQLException e) {
        if (dbProduct.isTableNotExistsError(e)) {
          useMinHistoryLevel = false;
          return -1L;
        } else {
          LOG.error("Unable to execute findMinTxnIdSeenOpen", e);
          rollbackDBConn(dbConn);
          checkRetryable(dbConn, e, "findMinTxnIdSeenOpen");
          throw new MetaException("Unable to execute findMinTxnIdSeenOpen() " + StringUtils.stringifyException(e));
        }
      } finally {
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      return findMinTxnIdSeenOpen();
    }
  }

  @Override
  protected void updateWSCommitIdAndCleanUpMetadata(Statement stmt, long txnid, TxnType txnType, Long commitId,
      long tempId) throws SQLException, MetaException {
    super.updateWSCommitIdAndCleanUpMetadata(stmt, txnid, txnType, commitId, tempId);
    if (txnType == TxnType.COMPACTION) {
      stmt.executeUpdate(
          "UPDATE \"COMPACTION_QUEUE\" SET \"CQ_NEXT_TXN_ID\" = " + commitId + ", \"CQ_COMMIT_TIME\" = " +
              TxnDbUtil.getEpochFn(dbProduct) + " WHERE \"CQ_TXN_ID\" = " + txnid);
    }
  }

  private Optional<CompactionInfo> getCompactionByTxnId(Connection dbConn, long txnid) throws SQLException, MetaException {
    CompactionInfo info = null;
    try (PreparedStatement pStmt = dbConn.prepareStatement(SELECT_COMPACTION_QUEUE_BY_TXN_ID)) {
      pStmt.setLong(1, txnid);
      try (ResultSet rs = pStmt.executeQuery()) {
        if (rs.next()) {
          info = CompactionInfo.loadFullFromCompactionQueue(rs);
        }
      }
    }
    return Optional.ofNullable(info);
  }

  @Override
  public Optional<CompactionInfo> getCompactionByTxnId(long txnId) throws MetaException {
    Connection dbConn = null;
    try {
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        return getCompactionByTxnId(dbConn, txnId);
      } catch (SQLException e) {
        LOG.error("Unable to getCompactionByTxnId", e);
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "getCompactionByTxnId");
        throw new MetaException("Unable to execute getCompactionByTxnId() " + StringUtils.stringifyException(e));
      } finally {
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      return getCompactionByTxnId(txnId);
    }
  }

  @Override
  protected void createCommitNotificationEvent(Connection dbConn, long txnid, Optional<TxnType> txnType)
      throws MetaException, SQLException {
    super.createCommitNotificationEvent(dbConn, txnid, txnType);
    if (transactionalListeners != null) {
      Optional<CompactionInfo> compactionInfo = getCompactionByTxnId(dbConn, txnid);
      if (compactionInfo.isPresent()) {
        MetaStoreListenerNotifier
            .notifyEventWithDirectSql(transactionalListeners, EventMessage.EventType.COMMIT_COMPACTION,
                new CommitCompactionEvent(txnid, compactionInfo.get()), dbConn, sqlGenerator);
      } else {
        LOG.warn("No compaction queue record found for Compaction type transaction commit. txnId:" + txnid);
      }
    }
  }
}


