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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.classification.RetrySemantics;
import org.apache.hadoop.hive.metastore.DatabaseProduct.DbType;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.FindNextCompactRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.datasource.DataSourceProvider;
import org.apache.hadoop.hive.metastore.events.CommitCompactionEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
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
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;

/**
 * Extends the transaction handler with methods needed only by the compactor threads.  These
 * methods are not available through the thrift interface.
 */
class CompactionTxnHandler extends TxnHandler {
  private static final String CLASS_NAME = CompactionTxnHandler.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  private static final String DB_FAILED_TO_CONNECT = "Unable to connect to transaction database: ";

  private static DataSource connPoolCompaction;

  private static final String SELECT_COMPACTION_QUEUE_BY_TXN_ID =
      "SELECT \"CQ_ID\", \"CQ_DATABASE\", \"CQ_TABLE\", \"CQ_PARTITION\", "
          + "\"CQ_STATE\", \"CQ_TYPE\", \"CQ_TBLPROPERTIES\", \"CQ_WORKER_ID\", \"CQ_START\", \"CQ_RUN_AS\", "
          + "\"CQ_HIGHEST_WRITE_ID\", \"CQ_META_INFO\", \"CQ_HADOOP_JOB_ID\", \"CQ_ERROR_MESSAGE\", "
          + "\"CQ_ENQUEUE_TIME\", \"CQ_WORKER_VERSION\", \"CQ_INITIATOR_ID\", \"CQ_INITIATOR_VERSION\", "
          + "\"CQ_RETRY_RETENTION\", \"CQ_NEXT_TXN_ID\", \"CQ_TXN_ID\", \"CQ_COMMIT_TIME\", \"CQ_POOL_NAME\", "
          + "\"CQ_NUMBER_OF_BUCKETS\", \"CQ_ORDER_BY\" "
          + "FROM \"COMPACTION_QUEUE\" WHERE \"CQ_TXN_ID\" = ?";
  private static final String SELECT_COMPACTION_METRICS_CACHE_QUERY =
      "SELECT \"CMC_METRIC_VALUE\", \"CMC_VERSION\" FROM \"COMPACTION_METRICS_CACHE\" " +
          "WHERE \"CMC_DATABASE\" = ? AND \"CMC_TABLE\" = ? AND \"CMC_METRIC_TYPE\" = ?";
  private static final String NO_SELECT_COMPACTION_METRICS_CACHE_FOR_TYPE_QUERY =
      "\"CMC_DATABASE\", \"CMC_TABLE\", \"CMC_PARTITION\", \"CMC_METRIC_VALUE\", \"CMC_VERSION\" FROM " +
      "\"COMPACTION_METRICS_CACHE\" WHERE \"CMC_METRIC_TYPE\" = ? ORDER BY \"CMC_METRIC_VALUE\" DESC";
  private static final String UPDATE_COMPACTION_METRICS_CACHE_QUERY =
      "UPDATE \"COMPACTION_METRICS_CACHE\" SET \"CMC_METRIC_VALUE\" = ?, \"CMC_VERSION\" = ? " +
          "WHERE \"CMC_DATABASE\" = ? AND \"CMC_TABLE\" = ? AND \"CMC_METRIC_TYPE\" = ? " +
          "AND \"CMC_VERSION\" = ?";
  private static final String INSERT_COMPACTION_METRICS_CACHE_QUERY =
      "INSERT INTO \"COMPACTION_METRICS_CACHE\" ( " +
          "\"CMC_DATABASE\", \"CMC_TABLE\", \"CMC_PARTITION\", \"CMC_METRIC_TYPE\", \"CMC_METRIC_VALUE\", " +
          "\"CMC_VERSION\" ) VALUES (?, ?, ?, ?, ?, ?)";
  private static final String DELETE_COMPACTION_METRICS_CACHE_QUERY =
      "DELETE FROM \"COMPACTION_METRICS_CACHE\" WHERE \"CMC_DATABASE\" = ? AND \"CMC_TABLE\" = ? " +
          "AND \"CMC_METRIC_TYPE\" = ?";

  private static final String DELETE_FAILED_TXNS_SQL =
      "DELETE FROM \"TXNS\" WHERE \"TXN_ID\" NOT IN (SELECT \"TC_TXNID\" FROM \"TXN_COMPONENTS\") " +
          "AND (\"TXN_STATE\" = " + TxnStatus.ABORTED + " OR \"TXN_STATE\" = " + TxnStatus.COMMITTED + ") " +
          "AND \"TXN_ID\" < ?";
  public CompactionTxnHandler() {
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    synchronized (CompactionTxnHandler.class) {
      if (connPoolCompaction == null) {
        int maxPoolSize = MetastoreConf.getIntVar(conf, ConfVars.HIVE_COMPACTOR_CONNECTION_POOLING_MAX_CONNECTIONS);
        try (DataSourceProvider.DataSourceNameConfigurator configurator =
                 new DataSourceProvider.DataSourceNameConfigurator(conf, "compactor")) {
          connPoolCompaction = setupJdbcConnectionPool(conf, maxPoolSize);
        }
      }
    }
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
      long abortedTimeThreshold, long lastChecked) throws MetaException {
    Connection dbConn = null;
    Set<CompactionInfo> response = new HashSet<>();
    Statement stmt = null;
    ResultSet rs = null;
    try {
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction);
        stmt = dbConn.createStatement();

        long startedAt = System.currentTimeMillis();
        long checkInterval = (lastChecked <= 0) ? lastChecked : (startedAt - lastChecked + 500) / 1000;

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
          "  WHERE \"C1\".\"CC_STATE\" IN (" + quoteChar(DID_NOT_INITIATE) + "," + quoteChar(FAILED_STATE) + ")" +
          ") \"C\" " +
          "ON \"TC\".\"CTC_DATABASE\" = \"C\".\"CC_DATABASE\" AND \"TC\".\"CTC_TABLE\" = \"C\".\"CC_TABLE\" " +
          "  AND (\"TC\".\"CTC_PARTITION\" = \"C\".\"CC_PARTITION\" OR (\"TC\".\"CTC_PARTITION\" IS NULL AND \"C\".\"CC_PARTITION\" IS NULL)) " +
          "WHERE \"C\".\"CC_ID\" IS NOT NULL OR " + isWithinCheckInterval("\"TC\".\"CTC_TIMESTAMP\"", checkInterval) : "");

        LOG.debug("Going to execute query <{}>", s);
        rs = stmt.executeQuery(s);
        while (rs.next()) {
          CompactionInfo info = new CompactionInfo();
          info.dbname = rs.getString(1);
          info.tableName = rs.getString(2);
          info.partName = rs.getString(3);
          response.add(info);
        }
        rs.close();

        if (!MetastoreConf.getBoolVar(conf, ConfVars.COMPACTOR_CLEAN_ABORTS_USING_CLEANER)) {
          // Check for aborted txns: number of aborted txns past threshold and age of aborted txns
          // past time threshold
          boolean checkAbortedTimeThreshold = abortedTimeThreshold >= 0;
          String sCheckAborted = "SELECT \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\", " +
                  "MIN(\"TXN_STARTED\"), COUNT(*) FROM \"TXNS\", \"TXN_COMPONENTS\" " +
                  "   WHERE \"TXN_ID\" = \"TC_TXNID\" AND \"TXN_STATE\" = " + TxnStatus.ABORTED + " " +
                  "GROUP BY \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\" " +
                  (checkAbortedTimeThreshold ? "" : " HAVING COUNT(*) > " + abortedThreshold);

          LOG.debug("Going to execute query <{}>", sCheckAborted);
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
              LOG.debug("Found potential compaction: {}", info);
              response.add(info);
            }
          }
        }
      } catch (SQLException e) {
        LOG.error(DB_FAILED_TO_CONNECT + e.getMessage());
        checkRetryable(e,
            "findPotentialCompactions(maxAborted:" + abortedThreshold
                + ", abortedTimeThreshold:" + abortedTimeThreshold + ")");
      } finally {
        close(rs, stmt, dbConn);
      }
      return response;
    }
    catch (RetryException e) {
      return findPotentialCompactions(abortedThreshold, abortedTimeThreshold, lastChecked);
    }
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
  @SuppressWarnings("squid:S2095")
  public CompactionInfo findNextToCompact(FindNextCompactRequest rqst) throws MetaException {
    try {
      if (rqst == null) {
        throw new MetaException("FindNextCompactRequest is null");
      }

      Connection dbConn = null;
      PreparedStatement stmt = null;
      //need a separate stmt for executeUpdate() otherwise it will close the ResultSet(HIVE-12725)
      Statement updStmt = null;
      ResultSet rs = null;

      long poolTimeout = MetastoreConf.getTimeVar(conf, ConfVars.COMPACTOR_WORKER_POOL_TIMEOUT, TimeUnit.MILLISECONDS);

      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction);
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT \"CQ_ID\", \"CQ_DATABASE\", \"CQ_TABLE\", \"CQ_PARTITION\", " +
          "\"CQ_TYPE\", \"CQ_POOL_NAME\", \"CQ_NUMBER_OF_BUCKETS\", \"CQ_ORDER_BY\", " +
            "\"CQ_TBLPROPERTIES\" FROM \"COMPACTION_QUEUE\" WHERE \"CQ_STATE\" = '" + INITIATED_STATE + "' AND ");
        boolean hasPoolName = StringUtils.isNotBlank(rqst.getPoolName());
        if(hasPoolName) {
          sb.append("\"CQ_POOL_NAME\"=?");
        } else {
          sb.append("\"CQ_POOL_NAME\" IS NULL OR  \"CQ_ENQUEUE_TIME\" < (")
            .append(getEpochFn(dbProduct)).append(" - ").append(poolTimeout).append(")");
        }
        sb.append(" ORDER BY \"CQ_ID\" ASC");
        String query = sb.toString();
        stmt = dbConn.prepareStatement(query);
        if (hasPoolName) {
          stmt.setString(1, rqst.getPoolName());
        }

        LOG.debug("Going to execute query <{}>", query);
        rs = stmt.executeQuery();
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
          info.type = TxnUtils.dbCompactionType2ThriftType(rs.getString(5).charAt(0));
          info.poolName = rs.getString(6);
          info.numberOfBuckets = rs.getInt(7);
          info.orderByClause = rs.getString(8);
          info.properties = rs.getString(9);
          info.workerId = rqst.getWorkerId();

          String workerId = rqst.getWorkerId();
          String workerVersion = rqst.getWorkerVersion();
          String workerIdSqlValue = (workerId == null) ? "NULL" : ("'" + workerId + "'");

          // Now, update this record as being worked on by this worker.
          long now = getDbTime(dbConn);
          query = "" +
              "UPDATE " +
              "  \"COMPACTION_QUEUE\" " +
              "SET " +
              " \"CQ_WORKER_ID\" = " + workerIdSqlValue + ", " +
              " \"CQ_WORKER_VERSION\" = '" + workerVersion + "', " +
              " \"CQ_START\" = " + now + ", " +
              " \"CQ_STATE\" = '" + WORKING_STATE + "' " +
              "WHERE \"CQ_ID\" = " + info.id +
              "  AND \"CQ_STATE\"='" + INITIATED_STATE + "'";

          LOG.debug("Going to execute update <{}>", query);
          int updCount = updStmt.executeUpdate(query);
          if(updCount == 1) {
            dbConn.commit();
            return info;
          }
          if(updCount == 0) {
            LOG.debug("Worker {} (version: {}) picked up {}", workerId, workerVersion, info);
            continue;
          }
          LOG.error("Unable to set to cq_state={} for compaction record: {}. updCnt={}. workerId={}. workerVersion={}",
              WORKING_STATE, info, updCount, workerId, workerVersion);
          dbConn.rollback();
          return null;
        } while( rs.next());
        dbConn.rollback();
        return null;
      } catch (SQLException e) {
        LOG.error("Unable to select next element for compaction, " + e.getMessage());
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(e, "findNextToCompact(rqst:" + rqst + ")");
        throw new MetaException(DB_FAILED_TO_CONNECT + e.getMessage());
      } finally {
        closeStmt(updStmt);
        close(rs, stmt, dbConn);
      }
    } catch (RetryException e) {
      return findNextToCompact(rqst);
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
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction);
        stmt = dbConn.createStatement();
        String s = "UPDATE \"COMPACTION_QUEUE\" SET \"CQ_STATE\" = '" + READY_FOR_CLEANING + "', "
            + "\"CQ_WORKER_ID\" = NULL"
            + " WHERE \"CQ_ID\" = " + info.id;
        LOG.debug("Going to execute update <{}>", s);
        int updCnt = stmt.executeUpdate(s);
        if (updCnt != 1) {
          LOG.error("Unable to set cq_state={} for compaction record: {}. updCnt={}", READY_FOR_CLEANING, info, updCnt);
          LOG.debug("Going to rollback");
          dbConn.rollback();
        }
        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.error("Unable to update compaction queue " + e.getMessage());
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(e, "markCompacted(" + info + ")");
        throw new MetaException(DB_FAILED_TO_CONNECT + e.getMessage());
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
   * @return information on the entry in the queue.
   */
  @Override
  @RetrySemantics.ReadOnly
  public List<CompactionInfo> findReadyToClean(long minOpenTxnWaterMark, long retentionTime) throws MetaException {
    try {
      List<CompactionInfo> rc = new ArrayList<>();

      try (Connection dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction);
          Statement stmt = dbConn.createStatement()) {
        /*
         * By filtering on minOpenTxnWaterMark, we will only cleanup after every transaction is committed, that could see
         * the uncompacted deltas. This way the cleaner can clean up everything that was made obsolete by this compaction.
         */
        String whereClause = " WHERE \"CQ_STATE\" = " + quoteChar(READY_FOR_CLEANING) + 
          " AND (\"CQ_COMMIT_TIME\" < (" + getEpochFn(dbProduct) + " - \"CQ_RETRY_RETENTION\" - " + retentionTime + ") OR \"CQ_COMMIT_TIME\" IS NULL)";
        
        String queryStr = 
          "SELECT \"CQ_ID\", \"cq1\".\"CQ_DATABASE\", \"cq1\".\"CQ_TABLE\", \"cq1\".\"CQ_PARTITION\"," + 
          "  \"CQ_TYPE\", \"CQ_RUN_AS\", \"CQ_HIGHEST_WRITE_ID\", \"CQ_TBLPROPERTIES\", \"CQ_RETRY_RETENTION\", " +
          "  \"CQ_NEXT_TXN_ID\"";
        if (useMinHistoryWriteId) {
          queryStr += ", \"MIN_OPEN_WRITE_ID\"";
        }
        queryStr +=
          "  FROM \"COMPACTION_QUEUE\" \"cq1\" " +
          "INNER JOIN (" +
          "  SELECT MIN(\"CQ_HIGHEST_WRITE_ID\") \"MIN_WRITE_ID_HWM\", \"CQ_DATABASE\", \"CQ_TABLE\", \"CQ_PARTITION\"" +
          "  FROM \"COMPACTION_QUEUE\"" 
          + whereClause +
          "  GROUP BY \"CQ_DATABASE\", \"CQ_TABLE\", \"CQ_PARTITION\") \"cq2\" " +
          "ON \"cq1\".\"CQ_DATABASE\" = \"cq2\".\"CQ_DATABASE\""+
          "  AND \"cq1\".\"CQ_TABLE\" = \"cq2\".\"CQ_TABLE\""+
          "  AND (\"cq1\".\"CQ_PARTITION\" = \"cq2\".\"CQ_PARTITION\"" +
          "    OR \"cq1\".\"CQ_PARTITION\" IS NULL AND \"cq2\".\"CQ_PARTITION\" IS NULL)" +
          "  AND \"CQ_HIGHEST_WRITE_ID\" = \"MIN_WRITE_ID_HWM\" ";
        
        if (useMinHistoryWriteId) {
          queryStr += 
            "LEFT JOIN (" +
            "  SELECT MIN(\"MH_WRITEID\") \"MIN_OPEN_WRITE_ID\", \"MH_DATABASE\", \"MH_TABLE\"" +
            "  FROM \"MIN_HISTORY_WRITE_ID\"" +
            "  GROUP BY \"MH_DATABASE\", \"MH_TABLE\") \"hwm\" " +
            "ON \"cq1\".\"CQ_DATABASE\" = \"hwm\".\"MH_DATABASE\"" +
            "  AND \"cq1\".\"CQ_TABLE\" = \"hwm\".\"MH_TABLE\"";
          
          whereClause += " AND (\"CQ_HIGHEST_WRITE_ID\" < \"MIN_OPEN_WRITE_ID\" OR \"MIN_OPEN_WRITE_ID\" IS NULL)";
          
        } else if (minOpenTxnWaterMark > 0) {
          whereClause += " AND (\"CQ_NEXT_TXN_ID\" <= " + minOpenTxnWaterMark + " OR \"CQ_NEXT_TXN_ID\" IS NULL)";
        }
        queryStr += whereClause + " ORDER BY \"CQ_ID\"";
        LOG.debug("Going to execute query <{}>", queryStr);

        try (ResultSet rs = stmt.executeQuery(queryStr)) {
          while (rs.next()) {
            CompactionInfo info = new CompactionInfo();
            info.id = rs.getLong(1);
            info.dbname = rs.getString(2);
            info.tableName = rs.getString(3);
            info.partName = rs.getString(4);
            info.type = TxnUtils.dbCompactionType2ThriftType(rs.getString(5).charAt(0));
            info.runAs = rs.getString(6);
            info.highestWriteId = rs.getLong(7);
            info.properties = rs.getString(8);
            info.retryRetention = rs.getInt(9);
            info.nextTxnId = rs.getLong(10);
            if (useMinHistoryWriteId) {
              info.minOpenWriteId = rs.getLong(11);
            }
            LOG.debug("Found ready to clean: {}", info);
            rc.add(info);
          }
        }
        return rc;
      } catch (SQLException e) {
        LOG.error("Unable to select next element for cleaning, " + e.getMessage());
        checkRetryable(e, "findReadyToClean");
        throw new MetaException(DB_FAILED_TO_CONNECT + e.getMessage());
      }
    } catch (RetryException e) {
      return findReadyToClean(minOpenTxnWaterMark, retentionTime);
    }
  }

  @Override
  @RetrySemantics.ReadOnly
  public List<CompactionInfo> findReadyToCleanAborts(long abortedTimeThreshold, int abortedThreshold) throws MetaException {
    try {
      List<CompactionInfo> readyToCleanAborts = new ArrayList<>();
      try (Connection dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction);
           Statement stmt = dbConn.createStatement()) {
        boolean checkAbortedTimeThreshold = abortedTimeThreshold >= 0;
        String sCheckAborted = "SELECT \"tc\".\"TC_DATABASE\", \"tc\".\"TC_TABLE\", \"tc\".\"TC_PARTITION\", " +
            " \"tc\".\"MIN_TXN_START_TIME\", \"tc\".\"ABORTED_TXN_COUNT\", \"minOpenWriteTxnId\".\"MIN_OPEN_WRITE_TXNID\" FROM " +
            " ( SELECT \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\", " +
            " MIN(\"TXN_STARTED\") AS \"MIN_TXN_START_TIME\", COUNT(*) AS \"ABORTED_TXN_COUNT\" FROM \"TXNS\", \"TXN_COMPONENTS\" " +
            " WHERE \"TXN_ID\" = \"TC_TXNID\" AND \"TXN_STATE\" = " + TxnStatus.ABORTED +
            " GROUP BY \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\" " +
            (checkAbortedTimeThreshold ? "" : " HAVING COUNT(*) > " + abortedThreshold) + " ) \"tc\" " +
            " LEFT JOIN ( SELECT MIN(\"TC_TXNID\") AS \"MIN_OPEN_WRITE_TXNID\", \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\" FROM \"TXNS\", \"TXN_COMPONENTS\" " +
            " WHERE \"TXN_ID\" = \"TC_TXNID\" AND \"TXN_STATE\"=" + TxnStatus.OPEN + " GROUP BY \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\" ) \"minOpenWriteTxnId\" " +
            " ON \"tc\".\"TC_DATABASE\" = \"minOpenWriteTxnId\".\"TC_DATABASE\" AND \"tc\".\"TC_TABLE\" = \"minOpenWriteTxnId\".\"TC_TABLE\"" +
            " AND (\"tc\".\"TC_PARTITION\" = \"minOpenWriteTxnId\".\"TC_PARTITION\" OR (\"tc\".\"TC_PARTITION\" IS NULL AND \"minOpenWriteTxnId\".\"TC_PARTITION\" IS NULL))";

        LOG.debug("Going to execute query <{}>", sCheckAborted);
        try (ResultSet rs = stmt.executeQuery(sCheckAborted)) {
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
              // In this case, this field contains min open write txn ID.
              info.txnId = rs.getLong(6);
              readyToCleanAborts.add(info);
            }
          }
        }
        return readyToCleanAborts;
      } catch (SQLException e) {
        LOG.error("Unable to select next element for cleaning, " + e.getMessage());
        checkRetryable(e, "findReadyToCleanForAborts");
        throw new MetaException(DB_FAILED_TO_CONNECT + e.getMessage());
      }
    } catch (RetryException e) {
      return findReadyToCleanAborts(abortedTimeThreshold, abortedThreshold);
    }
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

    try {
      Connection dbConn = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction);
        long now = getDbTime(dbConn);
        setCleanerStart(dbConn, info, now);
      } catch (SQLException e) {
        LOG.error("Unable to set the cleaner start time for compaction record  " + e.getMessage());
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(e, "markCleanerStart(" + info + ")");
        throw new MetaException(DB_FAILED_TO_CONNECT + e.getMessage());
      } finally {
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      markCleanerStart(info);
    }
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

    try {
      Connection dbConn = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction);
        setCleanerStart(dbConn, info, -1L);
      } catch (SQLException e) {
        LOG.error("Unable to clear the cleaner start time for compaction record  " + e.getMessage());
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(e, "clearCleanerStart(" + info + ")");
        throw new MetaException(DB_FAILED_TO_CONNECT + e.getMessage());
      } finally {
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      clearCleanerStart(info);
    }
  }

  private void setCleanerStart(Connection dbConn, CompactionInfo info, Long timestamp)
      throws RetryException, SQLException {
    long id = info.id;
    PreparedStatement pStmt = null;
    ResultSet rs = null;
    try {
      String query = "" +
          " UPDATE " +
          "  \"COMPACTION_QUEUE\" " +
          " SET " +
          "  \"CQ_CLEANER_START\" = " + timestamp +
          " WHERE " +
          "  \"CQ_ID\" = " + id +
          " AND " +
          "  \"CQ_STATE\"='" + READY_FOR_CLEANING + "'";

      pStmt = dbConn.prepareStatement(query);
      LOG.debug("Going to execute update <{}> for CQ_ID={}", query, id);
      int updCount = pStmt.executeUpdate();
      if (updCount != 1) {
        LOG.error("Unable to update compaction record: {}.  Update count={}", info, updCount);
        LOG.debug("Going to rollback");
        dbConn.rollback();
      } else {
        LOG.debug("Going to commit");
        dbConn.commit();
      }
    } finally {
      close(rs);
      closeStmt(pStmt);
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
  public void markCleaned(CompactionInfo info, boolean isAbortOnly) throws MetaException {
    LOG.debug("Running markCleaned with CompactionInfo: {}", info);
    try {
      Connection dbConn = null;
      PreparedStatement pStmt = null;
      ResultSet rs = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction);
        if (!isAbortOnly) {
          String s = "INSERT INTO \"COMPLETED_COMPACTIONS\"(\"CC_ID\", \"CC_DATABASE\", "
              + "\"CC_TABLE\", \"CC_PARTITION\", \"CC_STATE\", \"CC_TYPE\", \"CC_TBLPROPERTIES\", \"CC_WORKER_ID\", "
              + "\"CC_START\", \"CC_END\", \"CC_RUN_AS\", \"CC_HIGHEST_WRITE_ID\", \"CC_META_INFO\", "
              + "\"CC_HADOOP_JOB_ID\", \"CC_ERROR_MESSAGE\", \"CC_ENQUEUE_TIME\", "
              + "\"CC_WORKER_VERSION\", \"CC_INITIATOR_ID\", \"CC_INITIATOR_VERSION\", "
              + "\"CC_NEXT_TXN_ID\", \"CC_TXN_ID\", \"CC_COMMIT_TIME\", \"CC_POOL_NAME\", \"CC_NUMBER_OF_BUCKETS\","
              + "\"CC_ORDER_BY\") "
              + "SELECT \"CQ_ID\", \"CQ_DATABASE\", \"CQ_TABLE\", \"CQ_PARTITION\", "
              + quoteChar(SUCCEEDED_STATE) + ", \"CQ_TYPE\", \"CQ_TBLPROPERTIES\", \"CQ_WORKER_ID\", \"CQ_START\", "
              + getEpochFn(dbProduct) + ", \"CQ_RUN_AS\", \"CQ_HIGHEST_WRITE_ID\", \"CQ_META_INFO\", "
              + "\"CQ_HADOOP_JOB_ID\", \"CQ_ERROR_MESSAGE\", \"CQ_ENQUEUE_TIME\", "
              + "\"CQ_WORKER_VERSION\", \"CQ_INITIATOR_ID\", \"CQ_INITIATOR_VERSION\", "
              + "\"CQ_NEXT_TXN_ID\", \"CQ_TXN_ID\", \"CQ_COMMIT_TIME\", \"CQ_POOL_NAME\", \"CQ_NUMBER_OF_BUCKETS\", "
              + "\"CQ_ORDER_BY\" "
              + "FROM \"COMPACTION_QUEUE\" WHERE \"CQ_ID\" = ?";
          pStmt = dbConn.prepareStatement(s);
          pStmt.setLong(1, info.id);
          LOG.debug("Going to execute update <{}> for CQ_ID={}", s, info.id);
          pStmt.executeUpdate();

          s = "DELETE FROM \"COMPACTION_QUEUE\" WHERE \"CQ_ID\" = ?";
          pStmt = dbConn.prepareStatement(s);
          pStmt.setLong(1, info.id);
          LOG.debug("Going to execute update <{}>", s);
          int updCount = pStmt.executeUpdate();
          if (updCount != 1) {
            LOG.error("Unable to delete compaction record: {}.  Update count={}", info, updCount);
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
          if (info.highestWriteId != 0) {
            s += " AND \"CTC_WRITEID\" <= ?";
          }
          pStmt = dbConn.prepareStatement(s);
          int paramCount = 1;
          pStmt.setString(paramCount++, info.dbname);
          pStmt.setString(paramCount++, info.tableName);
          if (info.partName != null) {
            pStmt.setString(paramCount++, info.partName);
          }
          if (info.highestWriteId != 0) {
            pStmt.setLong(paramCount, info.highestWriteId);
          }
          LOG.debug("Going to execute update <{}>", s);
          if ((updCount = pStmt.executeUpdate()) < 1) {
            LOG.warn("Expected to remove at least one row from completed_txn_components when " +
                    "marking compaction entry as clean!");
          }
          LOG.debug("Removed {} records from completed_txn_components", updCount);
        }

        // Do cleanup of metadata in TXN_COMPONENTS table.
        removeTxnComponents(dbConn, info);
        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.error("Unable to delete from compaction queue " + e.getMessage());
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(e, "markCleaned(" + info + "," + isAbortOnly + ")");
        throw new MetaException(DB_FAILED_TO_CONNECT + e.getMessage());
      } finally {
        close(rs, pStmt, dbConn);
      }
    } catch (RetryException e) {
      markCleaned(info, isAbortOnly);
    }
  }

  private void removeTxnComponents(Connection dbConn, CompactionInfo info) throws MetaException, RetryException {
    PreparedStatement pStmt = null;
    ResultSet rs = null;
    try {
      /*
       * compaction may remove data from aborted txns above tc_writeid bit it only guarantees to
       * remove it up to (inclusive) tc_writeid, so it's critical to not remove metadata about
       * aborted TXN_COMPONENTS above tc_writeid (and consequently about aborted txns).
       * See {@link ql.txn.compactor.Cleaner.removeFiles()}
       */
      String s = "DELETE FROM \"TXN_COMPONENTS\" WHERE \"TC_TXNID\" IN ( "
              + "SELECT \"TXN_ID\" FROM \"TXNS\" WHERE \"TXN_STATE\" = " + TxnStatus.ABORTED + ") "
              + "AND \"TC_DATABASE\" = ? AND \"TC_TABLE\" = ? "
              + "AND \"TC_PARTITION\" " + (info.partName != null ? "= ?" : "IS NULL");

      List<String> queries = new ArrayList<>();
      Iterator<Long> writeIdsIter = null;
      List<Integer> counts = null;

      if (info.writeIds != null && !info.writeIds.isEmpty()) {
        StringBuilder prefix = new StringBuilder(s).append(" AND ");
        List<String> questions = Collections.nCopies(info.writeIds.size(), "?");

        counts = TxnUtils.buildQueryWithINClauseStrings(conf, queries, prefix,
                new StringBuilder(), questions, "\"TC_WRITEID\"", false, false);
        writeIdsIter = info.writeIds.iterator();
      } else if (!info.hasUncompactedAborts) {
        if (info.highestWriteId != 0) {
          s += " AND \"TC_WRITEID\" <= ?";
        }
        queries.add(s);
      }

      for (int i = 0; i < queries.size(); i++) {
        String query = queries.get(i);
        int writeIdCount = (counts != null) ? counts.get(i) : 0;

        LOG.debug("Going to execute update <{}>", query);
        pStmt = dbConn.prepareStatement(query);
        int paramCount = 1;

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
        LOG.debug("Removed {} records from txn_components", rc);
      }
    } catch (SQLException e) {
      LOG.error("Unable to delete from txn components due to {}", e.getMessage());
      LOG.debug("Going to rollback");
      rollbackDBConn(dbConn);
      checkRetryable(e, "markCleanedForAborts(" + info + ")");
      throw new MetaException(DB_FAILED_TO_CONNECT + e.getMessage());
    } finally {
      close(rs);
      closeStmt(pStmt);
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
        long minTxnIdSeenOpen = findMinTxnIdSeenOpen();

        // We query for minimum values in all the queries and they can only increase by any concurrent
        // operations. So, READ COMMITTED is sufficient.
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction);
        stmt = dbConn.createStatement();

        // First need to find the min_uncommitted_txnid which is currently seen by any open transactions.
        // If there are no txns which are currently open or aborted in the system, then current value of
        // max(TXNS.txn_id) could be min_uncommitted_txnid.
        String s = "SELECT MIN(\"RES\".\"ID\") AS \"ID\" FROM (" +
          " SELECT MAX(\"TXN_ID\") + 1 AS \"ID\" FROM \"TXNS\"" +
          (useMinHistoryLevel ? "" :
          "   UNION" +
          " SELECT MIN(\"WS_TXNID\") AS \"ID\" FROM \"WRITE_SET\"") +
          "   UNION" +
          " SELECT MIN(\"TXN_ID\") AS \"ID\" FROM \"TXNS\" WHERE \"TXN_STATE\" = " + TxnStatus.ABORTED +
          (useMinHistoryLevel ? "" :
          "   OR \"TXN_STATE\" = " + TxnStatus.OPEN) +
          " ) \"RES\"";

        LOG.debug("Going to execute query <{}>", s);
        rs = stmt.executeQuery(s);
        if (!rs.next()) {
          throw new MetaException("Transaction tables not properly initialized, no record found in TXNS");
        }
        long minUncommitedTxnid = Math.min(rs.getLong(1), minTxnIdSeenOpen);

        // As all txns below min_uncommitted_txnid are either committed or empty_aborted, we are allowed
        // to cleanup the entries less than min_uncommitted_txnid from the TXN_TO_WRITE_ID table.
        s = "DELETE FROM \"TXN_TO_WRITE_ID\" WHERE \"T2W_TXNID\" < " + minUncommitedTxnid;
        LOG.debug("Going to execute delete <{}>", s);
        int rc = stmt.executeUpdate(s);
        LOG.info("Removed {} rows from TXN_TO_WRITE_ID with Txn Low-Water-Mark: {}", rc, minUncommitedTxnid);

        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.error("Unable to delete from TXN_TO_WRITE_ID table " + e.getMessage());
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(e, "cleanTxnToWriteIdTable");
        throw new MetaException(DB_FAILED_TO_CONNECT + e.getMessage());
      } finally {
        close(rs, stmt, dbConn);
      }
    } catch (RetryException e) {
      cleanTxnToWriteIdTable();
    }
  }

  @Override
  @RetrySemantics.SafeToRetry
  public void removeDuplicateCompletedTxnComponents() throws MetaException {
    try {
      Connection dbConn = null;
      Statement stmt = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction);
        stmt = dbConn.createStatement();

        String s;
        switch (dbProduct.dbType) {
          case MYSQL:
          case SQLSERVER:
            s = "DELETE \"tc\" " +
              "FROM \"COMPLETED_TXN_COMPONENTS\" \"tc\" " +
              "INNER JOIN (" +
              "  SELECT \"CTC_DATABASE\", \"CTC_TABLE\", \"CTC_PARTITION\", max(\"CTC_WRITEID\") \"highestWriteId\"" +
              "  FROM \"COMPLETED_TXN_COMPONENTS\"" +
              "  GROUP BY \"CTC_DATABASE\", \"CTC_TABLE\", \"CTC_PARTITION\") \"c\" " +
              "ON \"tc\".\"CTC_DATABASE\" = \"c\".\"CTC_DATABASE\" AND \"tc\".\"CTC_TABLE\" = \"c\".\"CTC_TABLE\"" +
              "  AND (\"tc\".\"CTC_PARTITION\" = \"c\".\"CTC_PARTITION\" OR (\"tc\".\"CTC_PARTITION\" IS NULL AND \"c\".\"CTC_PARTITION\" IS NULL)) " +
              "LEFT JOIN (" +
              "  SELECT \"CTC_DATABASE\", \"CTC_TABLE\", \"CTC_PARTITION\", max(\"CTC_WRITEID\") \"updateWriteId\"" +
              "  FROM \"COMPLETED_TXN_COMPONENTS\"" +
              "  WHERE \"CTC_UPDATE_DELETE\" = 'Y'" +
              "  GROUP BY \"CTC_DATABASE\", \"CTC_TABLE\", \"CTC_PARTITION\") \"c2\" " +
              "ON \"tc\".\"CTC_DATABASE\" = \"c2\".\"CTC_DATABASE\" AND \"tc\".\"CTC_TABLE\" = \"c2\".\"CTC_TABLE\"" +
              "  AND (\"tc\".\"CTC_PARTITION\" = \"c2\".\"CTC_PARTITION\" OR (\"tc\".\"CTC_PARTITION\" IS NULL AND \"c2\".\"CTC_PARTITION\" IS NULL)) " +
              "WHERE \"tc\".\"CTC_WRITEID\" < \"c\".\"highestWriteId\" " +
              (DbType.MYSQL == dbProduct.dbType ?
              "  AND NOT \"tc\".\"CTC_WRITEID\" <=> \"c2\".\"updateWriteId\"" :
              "  AND (\"tc\".\"CTC_WRITEID\" != \"c2\".\"updateWriteId\" OR \"c2\".\"updateWriteId\" IS NULL)");
            break;
          case DERBY:
          case ORACLE:
          case CUSTOM:
            s = "DELETE from \"COMPLETED_TXN_COMPONENTS\" \"tc\"" +
              "WHERE EXISTS (" +
              "  SELECT 1" +
              "  FROM \"COMPLETED_TXN_COMPONENTS\"" +
              "  WHERE \"CTC_DATABASE\" = \"tc\".\"CTC_DATABASE\"" +
              "    AND \"CTC_TABLE\" = \"tc\".\"CTC_TABLE\"" +
              "    AND (\"CTC_PARTITION\" = \"tc\".\"CTC_PARTITION\" OR (\"CTC_PARTITION\" IS NULL AND \"tc\".\"CTC_PARTITION\" IS NULL))" +
              "    AND (\"tc\".\"CTC_UPDATE_DELETE\"='N' OR \"CTC_UPDATE_DELETE\"='Y')" +
              "    AND \"tc\".\"CTC_WRITEID\" < \"CTC_WRITEID\")";
            break;
          case POSTGRES:
            s = "DELETE " +
              "FROM \"COMPLETED_TXN_COMPONENTS\" \"tc\" " +
              "USING (" +
              "  SELECT \"c1\".*, \"c2\".\"updateWriteId\" FROM" +
              "    (SELECT \"CTC_DATABASE\", \"CTC_TABLE\", \"CTC_PARTITION\", max(\"CTC_WRITEID\") \"highestWriteId\"" +
              "      FROM \"COMPLETED_TXN_COMPONENTS\"" +
              "      GROUP BY \"CTC_DATABASE\", \"CTC_TABLE\", \"CTC_PARTITION\") \"c1\"" +
              "  LEFT JOIN" +
              "    (SELECT \"CTC_DATABASE\", \"CTC_TABLE\", \"CTC_PARTITION\", max(\"CTC_WRITEID\") \"updateWriteId\"" +
              "      FROM \"COMPLETED_TXN_COMPONENTS\"" +
              "      WHERE \"CTC_UPDATE_DELETE\" = 'Y'" +
              "      GROUP BY \"CTC_DATABASE\", \"CTC_TABLE\", \"CTC_PARTITION\") \"c2\"" +
              "  ON \"c1\".\"CTC_DATABASE\" = \"c2\".\"CTC_DATABASE\" AND \"c1\".\"CTC_TABLE\" = \"c2\".\"CTC_TABLE\"" +
              "    AND (\"c1\".\"CTC_PARTITION\" = \"c2\".\"CTC_PARTITION\" OR (\"c1\".\"CTC_PARTITION\" IS NULL AND \"c2\".\"CTC_PARTITION\" IS NULL))" +
              ") \"c\" " +
              "WHERE \"tc\".\"CTC_DATABASE\" = \"c\".\"CTC_DATABASE\" AND \"tc\".\"CTC_TABLE\" = \"c\".\"CTC_TABLE\"" +
              "  AND (\"tc\".\"CTC_PARTITION\" = \"c\".\"CTC_PARTITION\" OR (\"tc\".\"CTC_PARTITION\" IS NULL AND \"c\".\"CTC_PARTITION\" IS NULL))" +
              "  AND \"tc\".\"CTC_WRITEID\" < \"c\".\"highestWriteId\" " +
              "  AND \"tc\".\"CTC_WRITEID\" IS DISTINCT FROM \"c\".\"updateWriteId\"";
            break;
          default:
            String msg = "Unknown database product: " + dbProduct.dbType;
            LOG.error(msg);
            throw new MetaException(msg);
        }
        LOG.debug("Going to execute delete <{}>", s);
        int rc = stmt.executeUpdate(s);
        LOG.info("Removed {} rows from COMPLETED_TXN_COMPONENTS", rc);

        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.error("Unable to delete from COMPLETED_TXN_COMPONENTS table " + e.getMessage());
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(e, "removeDuplicateCompletedTxnComponents");
        throw new MetaException(DB_FAILED_TO_CONNECT + e.getMessage());
      } finally {
        close(null, stmt, dbConn);
      }
    } catch (RetryException e) {
      removeDuplicateCompletedTxnComponents();
    }
  }

  /**
   * Clean up aborted / committed transactions from txns that have no components in txn_components.
   * The committed txns are left there for TXN_OPENTXN_TIMEOUT window period intentionally.
   * The reason such aborted txns exist can be that now work was done in this txn
   * (e.g. Streaming opened TransactionBatch and abandoned it w/o doing any work)
   * or due to {@link #markCleaned(CompactionInfo, boolean)} being called.
   */
  @Override
  @RetrySemantics.SafeToRetry
  public void cleanEmptyAbortedAndCommittedTxns() throws MetaException {
    LOG.info("Start to clean empty aborted or committed TXNS");
    try {
      try (Connection dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction)) {
        try (PreparedStatement stmt = dbConn.prepareStatement(DELETE_FAILED_TXNS_SQL)) {
          //Aborted and committed are terminal states, so nothing about the txn can change
          //after that, so READ COMMITTED is sufficient.
          /*
           * Only delete aborted / committed transaction in a way that guarantees two things:
           * 1. never deletes anything that is inside the TXN_OPENTXN_TIMEOUT window
           * 2. never deletes the maximum txnId even if it is before the TXN_OPENTXN_TIMEOUT window
           */
          long lowWaterMark = getOpenTxnTimeoutLowBoundaryTxnId(dbConn);
          stmt.setLong(1, lowWaterMark);
          LOG.debug("Going to execute query <{}>", DELETE_FAILED_TXNS_SQL);
          int rc = stmt.executeUpdate();
          LOG.debug("Removed {} empty Aborted and Committed transactions from TXNS", rc);
          LOG.debug("Going to commit");
          dbConn.commit();
        } catch (SQLException e) {
          LOG.error("Unable to delete from txns table " + e.getMessage());
          LOG.debug("Going to rollback");
          rollbackDBConn(dbConn);
          checkRetryable(e, "cleanEmptyAbortedTxns");
          throw new MetaException("Unable to delete from txns table " + e.getMessage());
        }
      } catch (SQLException e) {
        LOG.error(DB_FAILED_TO_CONNECT + e.getMessage());
        checkRetryable(e, "cleanEmptyAbortedTxns");
        throw new MetaException(DB_FAILED_TO_CONNECT + e.getMessage());
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
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction);
        stmt = dbConn.createStatement();
        String s = "UPDATE \"COMPACTION_QUEUE\" SET \"CQ_WORKER_ID\" = NULL, \"CQ_START\" = NULL, \"CQ_STATE\" = '"
          + INITIATED_STATE+ "' WHERE \"CQ_STATE\" = '" + WORKING_STATE + "' AND \"CQ_WORKER_ID\" LIKE '"
          +  hostname + "%'";
        LOG.debug("Going to execute update <{}>", s);
        // It isn't an error if the following returns no rows, as the local workers could have died
        // with  nothing assigned to them.
        int updated = stmt.executeUpdate(s);
        LOG.debug("Set {} compaction queue entries to {} state for host {}", updated, INITIATED_RESPONSE, hostname);
        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.error("Unable to change dead worker's records back to initiated state " +
          e.getMessage());
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(e, "revokeFromLocalWorkers(hostname:" + hostname +")");
        throw new MetaException(DB_FAILED_TO_CONNECT + e.getMessage());
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
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction);
        long latestValidStart = getDbTime(dbConn) - timeout;
        stmt = dbConn.createStatement();
        String s = "UPDATE \"COMPACTION_QUEUE\" SET \"CQ_WORKER_ID\" = NULL, \"CQ_START\" = NULL, \"CQ_STATE\" = '"
          + INITIATED_STATE+ "' WHERE \"CQ_STATE\" = '" + WORKING_STATE + "' AND \"CQ_START\" < "
          +  latestValidStart;
        LOG.debug("Going to execute update <{}", s);
        // It isn't an error if the following returns no rows, as the local workers could have died
        // with  nothing assigned to them.
        int updated = stmt.executeUpdate(s);
        LOG.info("{} compaction queue entries timed out, set back to {} state. Latest valid start: {}", updated,
            INITIATED_RESPONSE, latestValidStart);
        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.error("Unable to change dead worker's records back to initiated state " +
          e.getMessage());
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(e, "revokeTimedoutWorkers(timeout:" + timeout + ")");
        throw new MetaException(DB_FAILED_TO_CONNECT + e.getMessage());
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
    LOG.debug("Finding columns with statistics info for CompactionInfo: {}",  ci);
    Connection dbConn = null;
    PreparedStatement pStmt = null;
    ResultSet rs = null;
    try {
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction);
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
        LOG.debug("Going to execute <{}>", s);
        rs = pStmt.executeQuery();
        List<String> columns = new ArrayList<>();
        while (rs.next()) {
          columns.add(rs.getString(1));
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Found columns to update stats: {} on {}{}", columns, ci.tableName,
              (ci.partName == null ? "" : "/" + ci.partName));
        }
        dbConn.commit();
        return columns;
      } catch (SQLException e) {
        rollbackDBConn(dbConn);
        checkRetryable(e, "findColumnsWithStats(" + ci.tableName +
          (ci.partName == null ? "" : "/" + ci.partName) + ")");
        throw new MetaException(DB_FAILED_TO_CONNECT + e.getMessage());
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
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction);
        stmt = dbConn.createStatement();
        String sqlText = "UPDATE \"COMPACTION_QUEUE\" SET \"CQ_HIGHEST_WRITE_ID\" = " +
            ci.highestWriteId + ", \"CQ_RUN_AS\" = " + quoteString(ci.runAs) + ", \"CQ_TXN_ID\" = " + compactionTxnId +
            " WHERE \"CQ_ID\" = " + ci.id;
        LOG.debug("About to execute: {}", sqlText);
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
        LOG.debug("About to execute: {}", sqlText);
        updCount = stmt.executeUpdate(sqlText);
        if(updCount != 1) {
          throw new IllegalStateException("Could not find record in COMPACTION_QUEUE for " + ci);
        }
        dbConn.commit();
      } catch (SQLException e) {
        rollbackDBConn(dbConn);
        checkRetryable(e, "updateCompactorState(" + ci + "," + compactionTxnId +")");
        throw new MetaException(DB_FAILED_TO_CONNECT + e.getMessage());
      } finally {
        close(null, stmt, dbConn);
      }
    } catch (RetryException ex) {
      updateCompactorState(ci, compactionTxnId);
    }
  }

  private static class RetentionCounters {
    int didNotInitiateRetention;
    int failedRetention;
    int succeededRetention;
    int refusedRetention;
    boolean hasSucceededMajorCompaction = false;
    boolean hasSucceededMinorCompaction = false;

    RetentionCounters(int didNotInitiateRetention, int failedRetention, int succeededRetention, int refusedRetention) {
      this.didNotInitiateRetention = didNotInitiateRetention;
      this.failedRetention = failedRetention;
      this.succeededRetention = succeededRetention;
      this.refusedRetention = refusedRetention;
    }
  }

  private void checkForDeletion(List<Long> deleteSet, CompactionInfo ci, RetentionCounters rc, long timeoutThreshold) {
    switch (ci.state) {
      case DID_NOT_INITIATE:
        if(--rc.didNotInitiateRetention < 0 || timedOut(ci, rc, timeoutThreshold)) {
          deleteSet.add(ci.id);
        }
        break;
      case FAILED_STATE:
        if(--rc.failedRetention < 0 || timedOut(ci, rc, timeoutThreshold)) {
          deleteSet.add(ci.id);
        }
        break;
      case SUCCEEDED_STATE:
        if(--rc.succeededRetention < 0) {
          deleteSet.add(ci.id);
        }
        if (ci.type == CompactionType.MAJOR) {
          rc.hasSucceededMajorCompaction = true;
        } else {
          rc.hasSucceededMinorCompaction = true;
        }
        break;
      case REFUSED_STATE:
        if(--rc.refusedRetention < 0 || timedOut(ci, rc, timeoutThreshold)) {
          deleteSet.add(ci.id);
        }
        break;
      default:
        //do nothing to handle future RU/D where we may want to add new state types
    }
  }

  private static boolean timedOut(CompactionInfo ci, RetentionCounters rc, long pastTimeout) {
    return ci.start < pastTimeout
            && (rc.hasSucceededMajorCompaction || (rc.hasSucceededMinorCompaction && ci.type == CompactionType.MINOR));
  }

  /**
   * For any given compactable entity (partition; table if not partitioned) the history of compactions
   * may look like "sssfffaaasffss", for example.  The idea is to retain the tail (most recent) of the
   * history such that a configurable number of each type of state is present.  Any other entries
   * can be purged.  This scheme has advantage of always retaining the last failure/success even if
   * it's not recent.
   *
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
    Connection dbConn = null;
    Statement stmt = null;
    PreparedStatement pStmt = null;
    ResultSet rs = null;
    List<Long> deleteSet = new ArrayList<>();
    RetentionCounters rc = null;
    long timeoutThreshold = System.currentTimeMillis() -
            MetastoreConf.getTimeVar(conf, ConfVars.COMPACTOR_HISTORY_RETENTION_TIMEOUT, TimeUnit.MILLISECONDS);
    int didNotInitiateRetention = MetastoreConf.getIntVar(conf, ConfVars.COMPACTOR_HISTORY_RETENTION_DID_NOT_INITIATE);
    int failedRetention = getFailedCompactionRetention();
    int succeededRetention = MetastoreConf.getIntVar(conf, ConfVars.COMPACTOR_HISTORY_RETENTION_SUCCEEDED);
    int refusedRetention = MetastoreConf.getIntVar(conf, ConfVars.COMPACTOR_HISTORY_RETENTION_REFUSED);
    try {
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction);
        stmt = dbConn.createStatement();
        /* cc_id is monotonically increasing so for any entity sorts in order of compaction history,
        thus this query groups by entity and withing group sorts most recent first */
        rs = stmt.executeQuery("SELECT \"CC_ID\", \"CC_DATABASE\", \"CC_TABLE\", \"CC_PARTITION\", "
            + "\"CC_STATE\" , \"CC_START\", \"CC_TYPE\" "
            + "FROM \"COMPLETED_COMPACTIONS\" ORDER BY \"CC_DATABASE\", \"CC_TABLE\", \"CC_PARTITION\"," +
                "\"CC_ID\" DESC");
        String lastCompactedEntity = null;
        /* In each group, walk from most recent and count occurrences of each state type.  Once you
        * have counted enough (for each state) to satisfy retention policy, delete all other
        * instances of this status, plus timed-out entries (see this method's JavaDoc).
        */
        while(rs.next()) {
          CompactionInfo ci = new CompactionInfo(
              rs.getLong(1), rs.getString(2), rs.getString(3),
              rs.getString(4), rs.getString(5).charAt(0));
          ci.start = rs.getLong(6);
          ci.type = TxnUtils.dbCompactionType2ThriftType(rs.getString(7).charAt(0));
          if(!ci.getFullPartitionName().equals(lastCompactedEntity)) {
            lastCompactedEntity = ci.getFullPartitionName();
            rc = new RetentionCounters(didNotInitiateRetention, failedRetention, succeededRetention, refusedRetention);
          }
          checkForDeletion(deleteSet, ci, rc, timeoutThreshold);
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
          LOG.debug("Going to execute update <{}>", query);
          pStmt = dbConn.prepareStatement(query);
          for (int j = 0; j < insertCount; j++) {
            pStmt.setLong(j + 1, deleteSet.get(totalCount + j));
          }
          totalCount += insertCount;
          int count = pStmt.executeUpdate();
          LOG.debug("Removed {} records from COMPLETED_COMPACTIONS", count);
        }
        dbConn.commit();
      } catch (SQLException e) {
        rollbackDBConn(dbConn);
        checkRetryable(e, "purgeCompactionHistory()");
        throw new MetaException(DB_FAILED_TO_CONNECT + e.getMessage());
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
      LOG.warn("Invalid configuration {}={} < {}={}.  Will use {}={}",
          ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD.getVarname(), failedRetention,
          ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED, failedRetention,
          ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD.getVarname(), failedRetention);
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
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction);
        pStmt = dbConn.prepareStatement("SELECT \"CC_STATE\", \"CC_ENQUEUE_TIME\" FROM \"COMPLETED_COMPACTIONS\" WHERE " +
          "\"CC_DATABASE\" = ? AND " +
          "\"CC_TABLE\" = ? " +
          (ci.partName != null ? "AND \"CC_PARTITION\" = ?" : "") +
          " AND \"CC_STATE\" != " + quoteChar(DID_NOT_INITIATE) + " ORDER BY \"CC_ID\" DESC");
        pStmt.setString(1, ci.dbname);
        pStmt.setString(2, ci.tableName);
        if (ci.partName != null) {
          pStmt.setString(3, ci.partName);
        }
        rs = pStmt.executeQuery();
        int numFailed = 0;
        int numTotal = 0;
        long lastEnqueueTime = -1;
        int failedThreshold = MetastoreConf.getIntVar(conf, ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD);
        while(rs.next() && ++numTotal <= failedThreshold) {
          long enqueueTime = rs.getLong(2);
          if (enqueueTime > lastEnqueueTime) {
            lastEnqueueTime = enqueueTime;
          }
          if(rs.getString(1).charAt(0) == FAILED_STATE) {
            numFailed++;
          }
          else {
            numFailed--;
          }
        }
        // If the last attempt was too long ago, ignore the failed threshold and try compaction again
        long retryTime = MetastoreConf.getTimeVar(conf,
            ConfVars.COMPACTOR_INITIATOR_FAILED_RETRY_TIME, TimeUnit.MILLISECONDS);
        boolean needsRetry = (retryTime > 0) && (lastEnqueueTime + retryTime < System.currentTimeMillis());
        return (numFailed == failedThreshold) && !needsRetry;
      }
      catch (SQLException e) {
        LOG.error("Unable to check for failed compactions", e);
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(e, "checkFailedCompactions(" + ci + ")");
        LOG.error(DB_FAILED_TO_CONNECT, e);
        return false;//weren't able to check
      } finally {
        close(rs, pStmt, dbConn);
      }
    } catch (RetryException e) {
      return checkFailedCompactions(ci);
    }
  }


  private void updateStatus(CompactionInfo ci) throws MetaException {
    String strState = CompactionState.fromSqlConst(ci.state).toString();
    LOG.debug("Marking as {}: CompactionInfo: {}", strState, ci);
    try {
      Connection dbConn = null;
      Statement stmt = null;
      PreparedStatement pStmt = null;
      ResultSet rs = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction);
        stmt = dbConn.createStatement();
        pStmt = dbConn.prepareStatement("SELECT \"CQ_ID\", \"CQ_DATABASE\", \"CQ_TABLE\", \"CQ_PARTITION\", "
                + "\"CQ_STATE\", \"CQ_TYPE\", \"CQ_TBLPROPERTIES\", \"CQ_WORKER_ID\", \"CQ_START\", \"CQ_RUN_AS\", "
                + "\"CQ_HIGHEST_WRITE_ID\", \"CQ_META_INFO\", \"CQ_HADOOP_JOB_ID\", \"CQ_ERROR_MESSAGE\", "
                + "\"CQ_ENQUEUE_TIME\", \"CQ_WORKER_VERSION\", \"CQ_INITIATOR_ID\", \"CQ_INITIATOR_VERSION\", "
                + "\"CQ_RETRY_RETENTION\", \"CQ_NEXT_TXN_ID\", \"CQ_TXN_ID\", \"CQ_COMMIT_TIME\", \"CQ_POOL_NAME\", "
                + "\"CQ_NUMBER_OF_BUCKETS\", \"CQ_ORDER_BY\" FROM \"COMPACTION_QUEUE\" WHERE \"CQ_ID\" = ?");
        pStmt.setLong(1, ci.id);
        rs = pStmt.executeQuery();
        if (rs.next()) {
          //preserve errorMessage and state
          String errorMessage = ci.errorMessage;
          char state = ci.state;
          ci = CompactionInfo.loadFullFromCompactionQueue(rs);
          ci.errorMessage = errorMessage;
          ci.state = state;

          String s = "DELETE FROM \"COMPACTION_QUEUE\" WHERE \"CQ_ID\" = ?";
          pStmt = dbConn.prepareStatement(s);
          pStmt.setLong(1, ci.id);
          LOG.debug("Going to execute update <{}>", s);
          pStmt.executeUpdate();
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
          //this is not strictly accurate, but 'type' cannot be null.
          if(ci.type == null) {
            ci.type = CompactionType.MINOR;
          }
          ci.start = getDbTime(dbConn);
          LOG.debug("The failure occurred before we even made an entry in COMPACTION_QUEUE. Generated ID so that we "
                  + "can make an entry in COMPLETED_COMPACTIONS. New Id: {}", ci.id);
        }
        close(rs, stmt, null);
        closeStmt(pStmt);

        pStmt = dbConn.prepareStatement(TxnQueries.INSERT_INTO_COMPLETED_COMPACTION);
        CompactionInfo.insertIntoCompletedCompactions(pStmt, ci, getDbTime(dbConn));
        int updCount = pStmt.executeUpdate();
        LOG.debug("Inserted {} entries into COMPLETED_COMPACTIONS", updCount);
        closeStmt(pStmt);
        dbConn.commit();
      } catch (SQLException e) {
        LOG.error("Failed to mark compaction request as " + strState + ", rolling back transaction: " + ci, e);
        rollbackDBConn(dbConn);
        checkRetryable(e, "updateStatus(" + ci + ")");
      } finally {
        close(rs, stmt, null);
        close(null, pStmt, dbConn);
      }
    } catch (RetryException e) {
      updateStatus(ci);
    }
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
    try {
      try (Connection dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction)) {
        try (PreparedStatement stmt = dbConn.prepareStatement("UPDATE \"COMPACTION_QUEUE\" " +
                "SET \"CQ_RETRY_RETENTION\" = ?, \"CQ_ERROR_MESSAGE\"= ? WHERE \"CQ_ID\" = ?")) {
          stmt.setLong(1, info.retryRetention);
          stmt.setString(2, info.errorMessage);
          stmt.setLong(3, info.id);
          int updCnt = stmt.executeUpdate();
          if (updCnt != 1) {
            LOG.error("Unable to update compaction queue record: {}. updCnt={}", info, updCnt);
            dbConn.rollback();
            throw new MetaException("No record with CQ_ID=" + info.id + " found in COMPACTION_QUEUE");
          }
          LOG.debug("Going to commit");
          dbConn.commit();
        } catch (SQLException e) {
          LOG.error("Unable to update compaction queue: " + e.getMessage());
          rollbackDBConn(dbConn);
          checkRetryable(e, "setCleanerRetryRetentionTimeOnError(" + info + ")");
          throw new MetaException("Unable to update compaction queue: " +
                  e.getMessage());
        }
      } catch (SQLException e) {
        LOG.error(DB_FAILED_TO_CONNECT + e.getMessage());
        checkRetryable(e, "setCleanerRetryRetentionTimeOnError(" + info  + ")");
        throw new MetaException(DB_FAILED_TO_CONNECT + e.getMessage());
      }
    } catch (RetryException e) {
      setCleanerRetryRetentionTimeOnError(info);
    }
  }

  @Override
  @RetrySemantics.Idempotent
  public void setHadoopJobId(String hadoopJobId, long id) {
    try {
      Connection dbConn = null;
      Statement stmt = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction);
        stmt = dbConn.createStatement();
        String s = "UPDATE \"COMPACTION_QUEUE\" SET \"CQ_HADOOP_JOB_ID\" = " + quoteString(hadoopJobId)
            + " WHERE \"CQ_ID\" = " + id;
        LOG.debug("Going to execute <{}>  with jobId: {} and CQ id: {}", s, hadoopJobId, id);
        stmt.executeUpdate(s);
        LOG.debug("Going to commit");
        closeStmt(stmt);
        dbConn.commit();
      } catch (SQLException e) {
        LOG.warn("setHadoopJobId(" + hadoopJobId + "," + id + "):" + e.getMessage());
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(e, "setHadoopJobId(" + hadoopJobId + "," + id + ")");
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
    if (useMinHistoryWriteId) {
      return Long.MAX_VALUE;
    }
    try {
      try (Connection dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction)) {
        return getMinOpenTxnIdWaterMark(dbConn);
      } catch (SQLException e) {
        LOG.error("Unable to fetch minOpenTxnId for Cleaner", e);
        checkRetryable(e, "findMinOpenTxnIdForCleaner");
        throw new MetaException("Unable to execute findMinOpenTxnIdForCleaner() " + e.getMessage());
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
    if (!useMinHistoryLevel || useMinHistoryWriteId) {
      return Long.MAX_VALUE;
    }
    try {
      try (Connection dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction)) {
        long minOpenTxn;
        try (Statement stmt = dbConn.createStatement()) {
          try (ResultSet rs = stmt.executeQuery("SELECT MIN(\"MHL_MIN_OPEN_TXNID\") FROM \"MIN_HISTORY_LEVEL\"")) {
            if (!rs.next()) {
              throw new IllegalStateException("Scalar query returned no rows?!");
            }
            minOpenTxn = rs.getLong(1);
            if (rs.wasNull()) {
              minOpenTxn = Long.MAX_VALUE;
            }
          }
        }
        return minOpenTxn;
      } catch (SQLException e) {
        if (dbProduct.isTableNotExistsError(e)) {
          useMinHistoryLevel = false;
          return Long.MAX_VALUE;
        } else {
          LOG.error("Unable to execute findMinTxnIdSeenOpen", e);
          checkRetryable(e, "findMinTxnIdSeenOpen");
          throw new MetaException("Unable to execute findMinTxnIdSeenOpen() " + e.getMessage());
        }
      }
    } catch (RetryException e) {
      return findMinTxnIdSeenOpen();
    }
  }

  @Override
  protected void updateWSCommitIdAndCleanUpMetadata(Statement stmt, long txnid, TxnType txnType, 
      Long commitId, long tempId) throws SQLException, MetaException {
    super.updateWSCommitIdAndCleanUpMetadata(stmt, txnid, txnType, commitId, tempId);
    
    if (txnType == TxnType.SOFT_DELETE || txnType == TxnType.COMPACTION) {
      stmt.executeUpdate("UPDATE \"COMPACTION_QUEUE\" SET \"CQ_NEXT_TXN_ID\" = " + commitId + ", \"CQ_COMMIT_TIME\" = " +
          getEpochFn(dbProduct) + " WHERE \"CQ_TXN_ID\" = " + txnid);
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
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction);
        return getCompactionByTxnId(dbConn, txnId);
      } catch (SQLException e) {
        LOG.error("Unable to getCompactionByTxnId", e);
        rollbackDBConn(dbConn);
        checkRetryable(e, "getCompactionByTxnId");
        throw new MetaException("Unable to execute getCompactionByTxnId() " + e.getMessage());
      } finally {
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      return getCompactionByTxnId(txnId);
    }
  }

  @Override
  protected void createCommitNotificationEvent(Connection dbConn, long txnid, TxnType txnType)
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

  @Override
  public boolean updateCompactionMetricsData(CompactionMetricsData data) throws MetaException {
    Connection dbConn = null;
    try {
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction);
        boolean updateRes;
        CompactionMetricsData prevMetricsData = getCompactionMetricsData(data, dbConn);
        if (data.getMetricValue() >= data.getThreshold()) {
          if (prevMetricsData != null) {
            updateRes = updateCompactionMetricsData(dbConn, data, prevMetricsData);
          } else {
            updateRes = createCompactionMetricsData(dbConn, data);
          }
        } else {
          if (prevMetricsData != null) {
            updateRes =
                removeCompactionMetricsData(dbConn, data.getDbName(), data.getTblName(), data.getPartitionName(), data.getMetricType());
          } else {
            return true;
          }
        }
        return updateRes;
      } catch (SQLException e) {
        rollbackDBConn(dbConn);
        checkRetryable(e, "updateCompactionMetricsData(" + data + ")");
        throw new MetaException("Unable to execute updateCompactionMetricsData()" + e.getMessage());
      } finally {
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      updateCompactionMetricsData(data);
    }
    return false;
  }

  @Override
  public List<CompactionMetricsData> getTopCompactionMetricsDataPerType(int limit)
      throws MetaException {
    Connection dbConn = null;
    List<CompactionMetricsData> metricsDataList = new ArrayList<>();
    try {
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction);
        for (CompactionMetricsData.MetricType type : CompactionMetricsData.MetricType.values()) {
          String query = sqlGenerator.addLimitClause(limit, NO_SELECT_COMPACTION_METRICS_CACHE_FOR_TYPE_QUERY);
          try (PreparedStatement pstmt = dbConn.prepareStatement(query)) {
            pstmt.setString(1, type.toString());
            ResultSet resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
              CompactionMetricsData.Builder builder = new CompactionMetricsData.Builder();
              metricsDataList.add(builder
                  .dbName(resultSet.getString(1))
                  .tblName(resultSet.getString(2))
                  .partitionName(resultSet.getString(3))
                  .metricType(type)
                  .metricValue(resultSet.getInt(4))
                  .version(resultSet.getInt(5))
                  .build());
            }
          }
        }
      } catch (SQLException e) {
        LOG.error("Unable to getCompactionMetricsDataForType");
        checkRetryable(e, "getCompactionMetricsDataForType");
        throw new MetaException("Unable to execute getCompactionMetricsDataForType()" +
            e.getMessage());
      } finally {
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      return getTopCompactionMetricsDataPerType(limit);
    }
    return metricsDataList;
  }

  @Override
  public CompactionMetricsData getCompactionMetricsData(String dbName, String tblName, String partitionName,
      CompactionMetricsData.MetricType type) throws MetaException {
    Connection dbConn = null;
    try {
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction);
        return getCompactionMetricsData(new CompactionMetricsData.Builder().dbName(dbName).tblName(tblName)
            .partitionName(partitionName).metricType(type).build(), dbConn);
      } catch (SQLException e) {
        rollbackDBConn(dbConn);
        checkRetryable(e, "getCompactionMetricsData(" + dbName + ", " + tblName + ", " + partitionName + ", "
            + type + ")");
        throw new MetaException("Unable to execute getCompactionMetricsData()" + e.getMessage());
      } finally {
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      getCompactionMetricsData(dbName, tblName, partitionName, type);
    }
    return null;
  }

  private CompactionMetricsData getCompactionMetricsData(CompactionMetricsData data, Connection dbConn) throws SQLException {
    String query = SELECT_COMPACTION_METRICS_CACHE_QUERY;
    if (data.getPartitionName() != null) {
      query += " AND \"CMC_PARTITION\" = ?";
    } else {
      query += " AND \"CMC_PARTITION\" IS NULL";
    }
    try (PreparedStatement pstmt = dbConn.prepareStatement(query)) {
      pstmt.setString(1, data.getDbName());
      pstmt.setString(2, data.getTblName());
      pstmt.setString(3, data.getMetricType().toString());
      if (data.getPartitionName() != null) {
        pstmt.setString(4, data.getPartitionName());
      }
      ResultSet resultSet = pstmt.executeQuery();
      CompactionMetricsData.Builder builder = new CompactionMetricsData.Builder();
      if (resultSet.next()) {
        return builder.dbName(data.getDbName()).tblName(data.getTblName()).partitionName(data.getPartitionName())
            .metricType(data.getMetricType()).metricValue(resultSet.getInt(1))
            .version(resultSet.getInt(2)).build();
      } else {
        return null;
      }
    }
  }

  @Override
  public void removeCompactionMetricsData(String dbName, String tblName, String partitionName,
      CompactionMetricsData.MetricType type) throws MetaException {
    Connection dbConn = null;
    try {
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolCompaction);
        removeCompactionMetricsData(dbConn, dbName, tblName, partitionName, type);
      } catch (SQLException e) {
        rollbackDBConn(dbConn);
        checkRetryable(e, "removeCompactionMetricsData(" + dbName + ", " +  tblName + ", " + partitionName + ", " +
            type + ")");
        throw new MetaException("Unable to execute removeCompactionMetricsData()" + e.getMessage());
      } finally {
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      removeCompactionMetricsData(dbName, tblName, partitionName, type);
    }
  }

  private boolean removeCompactionMetricsData(Connection dbConn, String dbName, String tblName, String partitionName,
      CompactionMetricsData.MetricType type) throws SQLException {
    boolean removeRes;
    String query = DELETE_COMPACTION_METRICS_CACHE_QUERY;
    if (partitionName != null) {
      query += " AND \"CMC_PARTITION\" = ?";
    } else {
      query += " AND \"CMC_PARTITION\" IS NULL";
    }
    try (PreparedStatement pstmt = dbConn.prepareStatement(query)) {
      pstmt.setString(1, dbName);
      pstmt.setString(2, tblName);
      pstmt.setString(3, type.toString());
      if (partitionName != null) {
        pstmt.setString(4, partitionName);
      }
      removeRes = pstmt.executeUpdate() > 0;
      dbConn.commit();
    }
    return removeRes;
  }

  private boolean updateCompactionMetricsData(Connection dbConn, CompactionMetricsData data,
      CompactionMetricsData prevData) throws SQLException {
    boolean updateRes;
    String query = UPDATE_COMPACTION_METRICS_CACHE_QUERY;
    if (data.getPartitionName() != null) {
      query += " AND \"CMC_PARTITION\" = ?";
    } else {
      query += " AND \"CMC_PARTITION\" IS NULL";
    }
    try (PreparedStatement pstmt = dbConn.prepareStatement(query)) {
      pstmt.setInt(1, data.getMetricValue());
      pstmt.setInt(2, prevData.getVersion() + 1);
      pstmt.setString(3, data.getDbName());
      pstmt.setString(4, data.getTblName());
      pstmt.setString(5, data.getMetricType().toString());
      pstmt.setInt(6, prevData.getVersion());
      if (data.getPartitionName() != null) {
        pstmt.setString(7, data.getPartitionName());
      }
      updateRes = pstmt.executeUpdate() > 0;
      dbConn.commit();
    }
    return updateRes;
  }

  private boolean createCompactionMetricsData(Connection dbConn, CompactionMetricsData data) throws SQLException {
    boolean createRes;
    try (PreparedStatement pstmt = dbConn.prepareStatement(INSERT_COMPACTION_METRICS_CACHE_QUERY)) {
      pstmt.setString(1, data.getDbName());
      pstmt.setString(2, data.getTblName());
      pstmt.setString(3, data.getPartitionName());
      pstmt.setString(4, data.getMetricType().toString());
      pstmt.setInt(5, data.getMetricValue());
      pstmt.setInt(6, 1);
      createRes = pstmt.executeUpdate() > 0;
      dbConn.commit();
    }
    return createRes;
  }

}


