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
package org.apache.hadoop.hive.metastore.txn.impl.functions;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.ReplLastIdInfo;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.txn.OperationType;
import org.apache.hadoop.hive.metastore.txn.TxnErrorMsg;
import org.apache.hadoop.hive.metastore.txn.TxnHandlingFeatures;
import org.apache.hadoop.hive.metastore.txn.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.impl.commands.DeleteReplTxnMapEntryCommand;
import org.apache.hadoop.hive.metastore.txn.impl.commands.RemoveTxnsFromMinHistoryLevelCommand;
import org.apache.hadoop.hive.metastore.txn.impl.queries.FindTxnStateHandler;
import org.apache.hadoop.hive.metastore.txn.impl.queries.GetOpenTxnTypeAndLockHandler;
import org.apache.hadoop.hive.metastore.txn.impl.queries.TargetTxnIdListHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionContext;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hive.metastore.txn.TxnUtils.executeQueriesInBatchNoCount;
import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;
import static org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRED;

public class CommitTxnFunction implements TransactionalFunction<TxnType> {

  private static final Logger LOG = LoggerFactory.getLogger(CommitTxnFunction.class);

  private static final String COMPL_TXN_COMPONENTS_INSERT_QUERY = "INSERT INTO \"COMPLETED_TXN_COMPONENTS\" " +
      "(\"CTC_TXNID\"," + " \"CTC_DATABASE\", \"CTC_TABLE\", \"CTC_PARTITION\", \"CTC_WRITEID\", \"CTC_UPDATE_DELETE\")" +
      " VALUES (%s, ?, ?, ?, ?, %s)";

  private final CommitTxnRequest rqst;
  
  @Override
  public TxnType execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException, NoSuchTxnException, TxnAbortedException {
    char isUpdateDelete = 'N';
    long txnid = rqst.getTxnid();
    long sourceTxnId = -1;

    boolean isReplayedReplTxn = TxnType.REPL_CREATED.equals(rqst.getTxn_type());
    boolean isHiveReplTxn = rqst.isSetReplPolicy() && TxnType.DEFAULT.equals(rqst.getTxn_type());
    //start a new transaction
    jdbcResource.bindDataSource(POOL_TX);
    try (TransactionContext context = jdbcResource.getTransactionManager().getTransaction(PROPAGATION_REQUIRED)) {
      Connection dbConn = null;
      Statement stmt = null;
      Long commitId = null;
      try {
        lockInternal();
        //make sure we are using the connection bound to the transaction, so obtain it via DataSourceUtils.getConnection() 
        dbConn = jdbcResource.getConnection();
        stmt = dbConn.createStatement();

        if (rqst.isSetReplLastIdInfo()) {
          updateReplId(dbConn, rqst.getReplLastIdInfo());
        }

        if (isReplayedReplTxn) {
          assert (rqst.isSetReplPolicy());
          sourceTxnId = rqst.getTxnid();
          List<Long> targetTxnIds = jdbcResource.execute(new TargetTxnIdListHandler(rqst.getReplPolicy(), Collections.singletonList(sourceTxnId)));
          if (targetTxnIds.isEmpty()) {
            // Idempotent case where txn was already closed or commit txn event received without
            // corresponding open txn event.
            LOG.info("Target txn id is missing for source txn id : {} and repl policy {}", sourceTxnId,
                rqst.getReplPolicy());
            jdbcResource.getTransactionManager().rollback(context);
            return null;
          }
          assert targetTxnIds.size() == 1;
          txnid = targetTxnIds.get(0);
        }

        /**
         * Runs at READ_COMMITTED with S4U on TXNS row for "txnid".  S4U ensures that no other
         * operation can change this txn (such acquiring locks). While lock() and commitTxn()
         * should not normally run concurrently (for same txn) but could due to bugs in the client
         * which could then corrupt internal transaction manager state.  Also competes with abortTxn().
         */
        TxnType txnType = jdbcResource.execute(new GetOpenTxnTypeAndLockHandler(sqlGenerator, txnid));
        if (txnType == null) {
          //if here, txn was not found (in expected state)
          TxnStatus actualTxnStatus = jdbcResource.execute(new FindTxnStateHandler(txnid));
          if (actualTxnStatus == TxnStatus.COMMITTED) {
            if (isReplayedReplTxn) {
              // in case of replication, idempotent is taken care by getTargetTxnId
              LOG.warn("Invalid state COMMITTED for transactions started using replication replay task");
            }
            /**
             * This makes the operation idempotent
             * (assume that this is most likely due to retry logic)
             */
            LOG.info("Nth commitTxn({}) msg", JavaUtils.txnIdToString(txnid));
            return null;
          }
          TxnUtils.raiseTxnUnexpectedState(actualTxnStatus, txnid);
        }

        String conflictSQLSuffix = "FROM \"TXN_COMPONENTS\" WHERE \"TC_TXNID\"=" + txnid + " AND \"TC_OPERATION_TYPE\" IN (" +
            OperationType.UPDATE + "," + OperationType.DELETE + ")";
        long tempCommitId = TxnUtils.generateTemporaryId();

        if (txnType == TxnType.SOFT_DELETE || txnType == TxnType.COMPACTION) {
          acquireTxnLock(false);
          commitId = getHighWaterMark();

        } else if (txnType != TxnType.READ_ONLY && !isReplayedReplTxn) {
          String writeSetInsertSql = "INSERT INTO \"WRITE_SET\" (\"WS_DATABASE\", \"WS_TABLE\", \"WS_PARTITION\"," +
              "   \"WS_TXNID\", \"WS_COMMIT_ID\", \"WS_OPERATION_TYPE\")" +
              " SELECT DISTINCT \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\", \"TC_TXNID\", " + tempCommitId + ", \"TC_OPERATION_TYPE\" ";

          if (isUpdateOrDelete(stmt, conflictSQLSuffix)) {
            isUpdateDelete = 'Y';
            //if here it means currently committing txn performed update/delete and we should check WW conflict
            /**
             * "select distinct" is used below because
             * 1. once we get to multi-statement txns, we only care to record that something was updated once
             * 2. if {@link #addDynamicPartitions(AddDynamicPartitions)} is retried by caller it may create
             *  duplicate entries in TXN_COMPONENTS
             * but we want to add a PK on WRITE_SET which won't have unique rows w/o this distinct
             * even if it includes all of its columns
             *
             * First insert into write_set using a temporary commitID, which will be updated in a separate call,
             * see: {@link #updateWSCommitIdAndCleanUpMetadata(Statement, long, TxnType, Long, long)}}.
             * This should decrease the scope of the S4U lock on the next_txn_id table.
             */
            Object undoWriteSetForCurrentTxn = context.createSavepoint();
            stmt.executeUpdate(writeSetInsertSql + (TxnHandlingFeatures.useMinHistoryLevel() ? conflictSQLSuffix :
                "FROM \"TXN_COMPONENTS\" WHERE \"TC_TXNID\"=" + txnid + " AND \"TC_OPERATION_TYPE\" <> " + OperationType.COMPACT));

            /**
             * This S4U will mutex with other commitTxn() and openTxns().
             * -1 below makes txn intervals look like [3,3] [4,4] if all txns are serial
             * Note: it's possible to have several txns have the same commit id.  Suppose 3 txns start
             * at the same time and no new txns start until all 3 commit.
             * We could've incremented the sequence for commitId as well but it doesn't add anything functionally.
             */
            acquireTxnLock(false);
            commitId = getHighWaterMark();

            if (!rqst.isExclWriteEnabled()) {
              /**
               * see if there are any overlapping txns that wrote the same element, i.e. have a conflict
               * Since entire commit operation is mutexed wrt other start/commit ops,
               * committed.ws_commit_id <= current.ws_commit_id for all txns
               * thus if committed.ws_commit_id < current.ws_txnid, transactions do NOT overlap
               * For example, [17,20] is committed, [6,80] is being committed right now - these overlap
               * [17,20] committed and [21,21] committing now - these do not overlap.
               * [17,18] committed and [18,19] committing now - these overlap  (here 18 started while 17 was still running)
               */
              try (ResultSet rs = checkForWriteConflict(stmt, txnid)) {
                if (rs.next()) {
                  //found a conflict, so let's abort the txn
                  String committedTxn = "[" + JavaUtils.txnIdToString(rs.getLong(1)) + "," + rs.getLong(2) + "]";
                  StringBuilder resource = new StringBuilder(rs.getString(3)).append("/").append(rs.getString(4));
                  String partitionName = rs.getString(5);
                  if (partitionName != null) {
                    resource.append('/').append(partitionName);
                  }
                  String msg = "Aborting [" + JavaUtils.txnIdToString(txnid) + "," + commitId + "]" + " due to a write conflict on " + resource +
                      " committed by " + committedTxn + " " + rs.getString(7) + "/" + rs.getString(8);
                  //remove WRITE_SET info for current txn since it's about to abort
                  context.rollbackToSavepoint(undoWriteSetForCurrentTxn);
                  LOG.info(msg);
                  //todo: should make abortTxns() write something into TXNS.TXN_META_INFO about this
                  if (abortTxns(Collections.singletonList(txnid), false, isReplayedReplTxn,
                      TxnErrorMsg.ABORT_WRITE_CONFLICT) != 1) {
                    throw new IllegalStateException(msg + " FAILED!");
                  }
                  jdbcResource.getTransactionManager().commit(context);
                  throw new TxnAbortedException(msg);
                }
              }
            }
          } else if (!TxnHandlingFeatures.useMinHistoryLevel()) {
            stmt.executeUpdate(writeSetInsertSql + "FROM \"TXN_COMPONENTS\" WHERE \"TC_TXNID\"=" + txnid +
                " AND \"TC_OPERATION_TYPE\" <> " + OperationType.COMPACT);
            commitId = getHighWaterMark();
          }
        } else {
          /*
           * current txn didn't update/delete anything (may have inserted), so just proceed with commit
           *
           * We only care about commit id for write txns, so for RO (when supported) txns we don't
           * have to mutex on NEXT_TXN_ID.
           * Consider: if RO txn is after a W txn, then RO's openTxns() will be mutexed with W's
           * commitTxn() because both do S4U on NEXT_TXN_ID and thus RO will see result of W txn.
           * If RO < W, then there is no reads-from relationship.
           * In replication flow we don't expect any write write conflict as it should have been handled at source.
           */
          assert true;
        }


        if (txnType != TxnType.READ_ONLY && !isReplayedReplTxn && !MetaStoreServerUtils.isCompactionTxn(txnType)) {
          moveTxnComponentsToCompleted(stmt, txnid, isUpdateDelete);
        } else if (isReplayedReplTxn) {
          if (rqst.isSetWriteEventInfos()) {
            String sql = String.format(COMPL_TXN_COMPONENTS_INSERT_QUERY, txnid, quoteChar(isUpdateDelete));
            try (PreparedStatement pstmt = dbConn.prepareStatement(sql)) {
              int insertCounter = 0;
              for (WriteEventInfo writeEventInfo : rqst.getWriteEventInfos()) {
                pstmt.setString(1, writeEventInfo.getDatabase());
                pstmt.setString(2, writeEventInfo.getTable());
                pstmt.setString(3, writeEventInfo.getPartition());
                pstmt.setLong(4, writeEventInfo.getWriteId());

                pstmt.addBatch();
                insertCounter++;
                if (insertCounter % maxBatchSize == 0) {
                  LOG.debug("Executing a batch of <{}> queries. Batch size: {}", sql, maxBatchSize);
                  pstmt.executeBatch();
                }
              }
              if (insertCounter % maxBatchSize != 0) {
                LOG.debug("Executing a batch of <{}> queries. Batch size: {}", sql, insertCounter % maxBatchSize);
                pstmt.executeBatch();
              }
            }
          }
          jdbcResource.execute(new DeleteReplTxnMapEntryCommand(sourceTxnId, rqst.getReplPolicy()));
        }
        updateWSCommitIdAndCleanUpMetadata(stmt, txnid, txnType, commitId, tempCommitId);
        jdbcResource.execute(new RemoveTxnsFromMinHistoryLevelCommand(conf, ImmutableList.of(txnid)), maxBatchSize);
        removeWriteIdsFromMinHistory(dbConn, ImmutableList.of(txnid));
        if (rqst.isSetKeyValue()) {
          updateKeyValueAssociatedWithTxn(rqst, stmt);
        }

        if (!isHiveReplTxn) {
          createCommitNotificationEvent(jdbcResource.getConnection(), txnid , txnType);
        }

        LOG.debug("Going to commit");
        jdbcResource.getTransactionManager().commit(context);

        if (MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_EXT_ON)) {
          Metrics.getOrCreateCounter(MetricsConstants.TOTAL_NUM_COMMITTED_TXNS).inc();
        }
      } catch (SQLException e) {
        LOG.debug("Going to rollback: ", e);
        jdbcResource.getTransactionManager().rollback(context);
        checkRetryable(e, "commitTxn(" + rqst + ")");
        throw new MetaException("Unable to update transaction database "
            + StringUtils.stringifyException(e));
      } finally {
        closeStmt(stmt);
        unlockInternal();
      }
    } catch (RetryException e) {
      commitTxn(rqst);
    } finally {
      jdbcResource.unbindDataSource();
    }  
  }

  private void updateReplId(Connection dbConn, ReplLastIdInfo replLastIdInfo) throws SQLException, MetaException {
    PreparedStatement pst = null;
    PreparedStatement pstInt = null;
    ResultSet rs = null;
    ResultSet prs = null;
    Statement stmt = null;
    String query;
    List<String> params;
    String lastReplId = Long.toString(replLastIdInfo.getLastReplId());
    String catalog = replLastIdInfo.isSetCatalog() ? normalizeIdentifier(replLastIdInfo.getCatalog()) :
        MetaStoreUtils.getDefaultCatalog(conf);
    String db = normalizeIdentifier(replLastIdInfo.getDatabase());
    String table = replLastIdInfo.isSetTable() ? normalizeIdentifier(replLastIdInfo.getTable()) : null;
    List<String> partList = replLastIdInfo.isSetPartitionList() ? replLastIdInfo.getPartitionList() : null;

    try {
      stmt = dbConn.createStatement();

      String s = sqlGenerator.getDbProduct().getPrepareTxnStmt();
      if (s != null) {
        stmt.execute(s);
      }

      long dbId = getDatabaseId(dbConn, db, catalog);

      // not used select for update as it will be updated by single thread only from repl load
      updateDatabaseProp(dbConn, db, dbId, ReplConst.REPL_TARGET_TABLE_PROPERTY, lastReplId);

      if (table == null) {
        // if only database last repl id to be updated.
        return;
      }

      query = "SELECT \"TBL_ID\" FROM \"TBLS\" WHERE \"TBL_NAME\" = ? AND \"DB_ID\" = " + dbId;
      params = Arrays.asList(table);
      pst = sqlGenerator.prepareStmtWithParameters(dbConn, query, params);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to execute query <" + query.replace("?", "{}") + ">", quoteString(table));
      }

      rs = pst.executeQuery();
      if (!rs.next()) {
        throw new MetaException("Table with name " + table + " does not exist in db " + catalog + "." + db);
      }
      long tblId = rs.getLong(1);
      rs.close();
      pst.close();

      // select for update is not required as only one task will update this during repl load.
      rs = stmt.executeQuery("SELECT \"PARAM_VALUE\" FROM \"TABLE_PARAMS\" WHERE \"PARAM_KEY\" = " +
          "'repl.last.id' AND \"TBL_ID\" = " + tblId);
      if (!rs.next()) {
        query = "INSERT INTO \"TABLE_PARAMS\" VALUES ( " + tblId + " , 'repl.last.id' , ? )";
      } else {
        query = "UPDATE \"TABLE_PARAMS\" SET \"PARAM_VALUE\" = ? WHERE \"TBL_ID\" = " + tblId +
            " AND \"PARAM_KEY\" = 'repl.last.id'";
      }
      rs.close();

      params = Arrays.asList(lastReplId);
      pst = sqlGenerator.prepareStmtWithParameters(dbConn, query, params);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Updating repl id for table <" + query.replace("?", "{}") + ">", lastReplId);
      }
      if (pst.executeUpdate() != 1) {
        //only one row insert or update should happen
        throw new RuntimeException("TABLE_PARAMS is corrupted for table " + table);
      }
      pst.close();

      if (partList == null || partList.isEmpty()) {
        return;
      }

      List<String> questions = new ArrayList<>();
      for(int i = 0; i < partList.size(); ++i) {
        questions.add("?");
      }

      List<String> queries = new ArrayList<>();
      StringBuilder prefix = new StringBuilder();
      StringBuilder suffix = new StringBuilder();
      prefix.append("SELECT \"PART_ID\" FROM \"PARTITIONS\" WHERE \"TBL_ID\" = " + tblId + " and ");

      // Populate the complete query with provided prefix and suffix
      List<Integer> counts = TxnUtils.buildQueryWithINClauseStrings(conf, queries, prefix, suffix,
          questions, "\"PART_NAME\"", true, false);
      int totalCount = 0;
      assert queries.size() == counts.size();
      params = Arrays.asList(lastReplId);
      for (int i = 0; i < queries.size(); i++) {
        query = queries.get(i);
        int partCount = counts.get(i);

        LOG.debug("Going to execute query {} with partitions {}", query,
            partList.subList(totalCount, (totalCount + partCount)));
        pst = dbConn.prepareStatement(query);
        for (int j = 0; j < partCount; j++) {
          pst.setString(j + 1, partList.get(totalCount + j));
        }
        totalCount += partCount;
        prs = pst.executeQuery();
        while (prs.next()) {
          long partId = prs.getLong(1);
          rs = stmt.executeQuery("SELECT \"PARAM_VALUE\" FROM \"PARTITION_PARAMS\" WHERE \"PARAM_KEY\" " +
              " = 'repl.last.id' AND \"PART_ID\" = " + partId);
          if (!rs.next()) {
            query = "INSERT INTO \"PARTITION_PARAMS\" VALUES ( " + partId + " , 'repl.last.id' , ? )";
          } else {
            query = "UPDATE \"PARTITION_PARAMS\" SET \"PARAM_VALUE\" = ? " +
                " WHERE \"PART_ID\" = " + partId + " AND \"PARAM_KEY\" = 'repl.last.id'";
          }
          rs.close();

          pstInt = sqlGenerator.prepareStmtWithParameters(dbConn, query, params);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Updating repl id for part <" + query.replace("?", "{}") + ">", lastReplId);
          }
          if (pstInt.executeUpdate() != 1) {
            //only one row insert or update should happen
            throw new RuntimeException("PARTITION_PARAMS is corrupted for partition " + partId);
          }
          partCount--;
          pstInt.close();
        }
        if (partCount != 0) {
          throw new MetaException(partCount + " Number of partition among " + partList + " does not exist in table " +
              catalog + "." + db + "." + table);
        }
        prs.close();
        pst.close();
      }
    } finally {
      closeStmt(stmt);
      close(rs);
      close(prs);
      closeStmt(pst);
      closeStmt(pstInt);
    }
  }

  private void updateDatabaseProp(Connection dbConn, String database,
                                  long dbId, String prop, String propValue) throws SQLException {
    ResultSet rs = null;
    PreparedStatement pst = null;
    try {
      String query = "SELECT \"PARAM_VALUE\" FROM \"DATABASE_PARAMS\" WHERE \"PARAM_KEY\" = " +
          "'" + prop + "' AND \"DB_ID\" = " + dbId;
      pst = sqlGenerator.prepareStmtWithParameters(dbConn, query, null);
      rs = pst.executeQuery();
      query = null;
      if (!rs.next()) {
        query = "INSERT INTO \"DATABASE_PARAMS\" VALUES ( " + dbId + " , '" + prop + "' , ? )";
      } else if (!rs.getString(1).equals(propValue)) {
        query = "UPDATE \"DATABASE_PARAMS\" SET \"PARAM_VALUE\" = ? WHERE \"DB_ID\" = " + dbId +
            " AND \"PARAM_KEY\" = '" + prop + "'";
      }
      closeStmt(pst);
      if (query == null) {
        LOG.info("Database property: {} with value: {} already updated for db: {}", prop, propValue, database);
        return;
      }
      pst = sqlGenerator.prepareStmtWithParameters(dbConn, query, Arrays.asList(propValue));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Updating " + prop + " for db: " + database + " <" + query.replace("?", "{}") + ">", propValue);
      }
      if (pst.executeUpdate() != 1) {
        //only one row insert or update should happen
        throw new RuntimeException("DATABASE_PARAMS is corrupted for database: " + database);
      }
    } finally {
      close(rs);
      closeStmt(pst);
    }
  }

  private long getDatabaseId(Connection dbConn, String database, String catalog) throws SQLException, MetaException {
    ResultSet rs = null;
    PreparedStatement pst = null;
    try {
      String query = "select \"DB_ID\" from \"DBS\" where \"NAME\" = ?  and \"CTLG_NAME\" = ?";
      pst = sqlGenerator.prepareStmtWithParameters(dbConn, query, Arrays.asList(database, catalog));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to execute query <" + query.replace("?", "{}") + ">",
            quoteString(database), quoteString(catalog));
      }
      rs = pst.executeQuery();
      if (!rs.next()) {
        throw new MetaException("DB with name " + database + " does not exist in catalog " + catalog);
      }
      return rs.getLong(1);
    } finally {
      close(rs);
      closeStmt(pst);
    }
  }

  private ResultSet checkForWriteConflict(Statement stmt, long txnid) throws SQLException, MetaException {
    String writeConflictQuery = sqlGenerator.addLimitClause(1, "\"COMMITTED\".\"WS_TXNID\", \"COMMITTED\".\"WS_COMMIT_ID\", " +
        "\"COMMITTED\".\"WS_DATABASE\", \"COMMITTED\".\"WS_TABLE\", \"COMMITTED\".\"WS_PARTITION\", " +
        "\"CUR\".\"WS_COMMIT_ID\" \"CUR_WS_COMMIT_ID\", \"CUR\".\"WS_OPERATION_TYPE\" \"CUR_OP\", " +
        "\"COMMITTED\".\"WS_OPERATION_TYPE\" \"COMMITTED_OP\" FROM \"WRITE_SET\" \"COMMITTED\" INNER JOIN \"WRITE_SET\" \"CUR\" " +
        "ON \"COMMITTED\".\"WS_DATABASE\"=\"CUR\".\"WS_DATABASE\" AND \"COMMITTED\".\"WS_TABLE\"=\"CUR\".\"WS_TABLE\" " +
        //For partitioned table we always track writes at partition level (never at table)
        //and for non partitioned - always at table level, thus the same table should never
        //have entries with partition key and w/o
        "AND (\"COMMITTED\".\"WS_PARTITION\"=\"CUR\".\"WS_PARTITION\" OR (\"COMMITTED\".\"WS_PARTITION\" IS NULL AND \"CUR\".\"WS_PARTITION\" IS NULL)) " +
        "WHERE \"CUR\".\"WS_TXNID\" <= \"COMMITTED\".\"WS_COMMIT_ID\"" + //txns overlap; could replace ws_txnid
        // with txnid, though any decent DB should infer this
        " AND \"CUR\".\"WS_TXNID\"=" + txnid + //make sure RHS of join only has rows we just inserted as
        // part of this commitTxn() op
        " AND \"COMMITTED\".\"WS_TXNID\" <> " + txnid + //and LHS only has committed txns
        //U+U and U+D and D+D is a conflict and we don't currently track I in WRITE_SET at all
        //it may seem like D+D should not be in conflict but consider 2 multi-stmt txns
        //where each does "delete X + insert X, where X is a row with the same PK.  This is
        //equivalent to an update of X but won't be in conflict unless D+D is in conflict.
        //The same happens when Hive splits U=I+D early so it looks like 2 branches of a
        //multi-insert stmt (an Insert and a Delete branch).  It also 'feels'
        // un-serializable to allow concurrent deletes
        " and (\"COMMITTED\".\"WS_OPERATION_TYPE\" IN(" + OperationType.UPDATE +
        ", " + OperationType.DELETE +
        ") AND \"CUR\".\"WS_OPERATION_TYPE\" IN(" + OperationType.UPDATE+ ", "
        + OperationType.DELETE + "))");
    LOG.debug("Going to execute query: <{}>", writeConflictQuery);
    return stmt.executeQuery(writeConflictQuery);
  }

  private void moveTxnComponentsToCompleted(Statement stmt, long txnid, char isUpdateDelete) throws SQLException {
    // Move the record from txn_components into completed_txn_components so that the compactor
    // knows where to look to compact.
    String s = "INSERT INTO \"COMPLETED_TXN_COMPONENTS\" (\"CTC_TXNID\", \"CTC_DATABASE\", " +
        "\"CTC_TABLE\", \"CTC_PARTITION\", \"CTC_WRITEID\", \"CTC_UPDATE_DELETE\") SELECT \"TC_TXNID\"," +
        " \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\", \"TC_WRITEID\", '" + isUpdateDelete +
        "' FROM \"TXN_COMPONENTS\" WHERE \"TC_TXNID\" = " + txnid +
        //we only track compactor activity in TXN_COMPONENTS to handle the case where the
        //compactor txn aborts - so don't bother copying it to COMPLETED_TXN_COMPONENTS
        " AND \"TC_OPERATION_TYPE\" <> " + OperationType.COMPACT;
    LOG.debug("Going to execute insert <{}>", s);

    if ((stmt.executeUpdate(s)) < 1) {
      //this can be reasonable for an empty txn START/COMMIT or read-only txn
      //also an IUD with DP that didn't match any rows.
      LOG.info("Expected to move at least one record from txn_components to "
          + "completed_txn_components when committing txn! {}", JavaUtils.txnIdToString(txnid));
    }
  }

  private void updateKeyValueAssociatedWithTxn(CommitTxnRequest rqst, Statement stmt) throws SQLException {
    if (!rqst.getKeyValue().getKey().startsWith(TxnStore.TXN_KEY_START)) {
      String errorMsg = "Error updating key/value in the sql backend with"
          + " txnId=" + rqst.getTxnid() + ","
          + " tableId=" + rqst.getKeyValue().getTableId() + ","
          + " key=" + rqst.getKeyValue().getKey() + ","
          + " value=" + rqst.getKeyValue().getValue() + "."
          + " key should start with " + TXN_KEY_START + ".";
      LOG.warn(errorMsg);
      throw new IllegalArgumentException(errorMsg);
    }
    String s = "UPDATE \"TABLE_PARAMS\" SET"
        + " \"PARAM_VALUE\" = " + quoteString(rqst.getKeyValue().getValue())
        + " WHERE \"TBL_ID\" = " + rqst.getKeyValue().getTableId()
        + " AND \"PARAM_KEY\" = " + quoteString(rqst.getKeyValue().getKey());
    LOG.debug("Going to execute update <{}>", s);
    int affectedRows = stmt.executeUpdate(s);
    if (affectedRows != 1) {
      String errorMsg = "Error updating key/value in the sql backend with"
          + " txnId=" + rqst.getTxnid() + ","
          + " tableId=" + rqst.getKeyValue().getTableId() + ","
          + " key=" + rqst.getKeyValue().getKey() + ","
          + " value=" + rqst.getKeyValue().getValue() + "."
          + " Only one row should have been affected but "
          + affectedRows + " rows where affected.";
      LOG.warn(errorMsg);
      throw new IllegalStateException(errorMsg);
    }
  }

  /**
   * See overridden method in CompactionTxnHandler also.
   */
  protected void updateWSCommitIdAndCleanUpMetadata(Statement stmt, long txnid, TxnType txnType,
                                                    Long commitId, long tempId) throws SQLException, MetaException {
    List<String> queryBatch = new ArrayList<>(5);
    // update write_set with real commitId
    if (commitId != null) {
      queryBatch.add("UPDATE \"WRITE_SET\" SET \"WS_COMMIT_ID\" = " + commitId +
          " WHERE \"WS_COMMIT_ID\" = " + tempId + " AND \"WS_TXNID\" = " + txnid);
    }
    // clean up txn related metadata
    if (txnType != TxnType.READ_ONLY) {
      queryBatch.add("DELETE FROM \"TXN_COMPONENTS\" WHERE \"TC_TXNID\" = " + txnid);
    }
    queryBatch.add("DELETE FROM \"HIVE_LOCKS\" WHERE \"HL_TXNID\" = " + txnid);
    // DO NOT remove the transaction from the TXN table, the cleaner will remove it when appropriate
    queryBatch.add("UPDATE \"TXNS\" SET \"TXN_STATE\" = " + TxnStatus.COMMITTED + " WHERE \"TXN_ID\" = " + txnid);
    if (txnType == TxnType.MATER_VIEW_REBUILD) {
      queryBatch.add("DELETE FROM \"MATERIALIZATION_REBUILD_LOCKS\" WHERE \"MRL_TXN_ID\" = " + txnid);
    }
    // execute all in one batch
    executeQueriesInBatchNoCount(dbProduct, stmt, queryBatch, maxBatchSize);

    if (txnType == TxnType.SOFT_DELETE || txnType == TxnType.COMPACTION) {
      stmt.executeUpdate("UPDATE \"COMPACTION_QUEUE\" SET \"CQ_NEXT_TXN_ID\" = " + commitId + ", \"CQ_COMMIT_TIME\" = " +
          getEpochFn(dbProduct) + " WHERE \"CQ_TXN_ID\" = " + txnid);
    }
    
  }

  private boolean isUpdateOrDelete(Statement stmt, String conflictSQLSuffix) throws SQLException, MetaException {
    try (ResultSet rs = stmt.executeQuery(sqlGenerator.addLimitClause(1,
        "\"TC_OPERATION_TYPE\" " + conflictSQLSuffix))) {
      return rs.next();
    }
  }

}
