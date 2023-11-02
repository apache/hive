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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.events.AllocWriteIdEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import org.apache.hadoop.hive.metastore.txn.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.impl.queries.TargetTxnIdListHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.InClauseBatchCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionContext;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRED;

public class AllocateTableWriteIdsFunction implements TransactionalFunction<AllocateTableWriteIdsResponse> {

  private static final Logger LOG = LoggerFactory.getLogger(AbortTxnFunction.class);

  private static final String TXN_TO_WRITE_ID_INSERT_QUERY = "INSERT INTO \"TXN_TO_WRITE_ID\" (\"T2W_TXNID\", " +
      "\"T2W_DATABASE\", \"T2W_TABLE\", \"T2W_WRITEID\") VALUES (:txnId, :dbName, :tableName, :writeId)";

  private final Configuration conf;
  private final AllocateTableWriteIdsRequest rqst;
  private final SQLGenerator sqlGenerator;
  
  @Override
  public AllocateTableWriteIdsResponse execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    List<Long> txnIds;
    String dbName = rqst.getDbName().toLowerCase();
    String tblName = rqst.getTableName().toLowerCase();
    boolean shouldReallocate = rqst.isReallocate();
    try {
      Connection dbConn = null;
      PreparedStatement pStmt = null;
      ResultSet rs = null;
      List<TxnToWriteId> txnToWriteIds = new ArrayList<>();
      List<TxnToWriteId> srcTxnToWriteIds = null;
      jdbcResource.bindDataSource(POOL_TX);
      try (TransactionContext context = jdbcResource.getTransactionManager().getTransaction(PROPAGATION_REQUIRED)){
        dbConn =jdbcResource.getConnection();

        if (rqst.isSetReplPolicy()) {
          srcTxnToWriteIds = rqst.getSrcTxnToWriteIdList();
          List<Long> srcTxnIds = new ArrayList<>();
          assert (rqst.isSetSrcTxnToWriteIdList());
          assert (!rqst.isSetTxnIds());
          assert (!srcTxnToWriteIds.isEmpty());

          for (TxnToWriteId txnToWriteId : srcTxnToWriteIds) {
            srcTxnIds.add(txnToWriteId.getTxnId());
          }
          txnIds = jdbcResource.execute(new TargetTxnIdListHandler(rqst.getReplPolicy(), srcTxnIds));
          if (srcTxnIds.size() != txnIds.size()) {
            // Idempotent case where txn was already closed but gets allocate write id event.
            // So, just ignore it and return empty list.
            LOG.info("Idempotent case: Target txn id is missing for source txn id : {} and repl policy {}", srcTxnIds,
                rqst.getReplPolicy());
            return new AllocateTableWriteIdsResponse(txnToWriteIds);
          }
        } else {
          assert (!rqst.isSetSrcTxnToWriteIdList());
          assert (rqst.isSetTxnIds());
          txnIds = rqst.getTxnIds();
        }

        //Easiest check since we can't differentiate do we handle singleton list or list with multiple txn ids.
        if (txnIds.size() > 1) {
          Collections.sort(txnIds); //easier to read logs and for assumption done in replication flow
        }

        // Check if all the input txns are in valid state.
        // Write IDs should be allocated only for open and not read-only transactions.
        try (Statement stmt = dbConn.createStatement()) {
          if (!isTxnsOpenAndNotReadOnly(txnIds, stmt)) {
            String errorMsg = "Write ID allocation on " + TableName.getDbTable(dbName, tblName)
                + " failed for input txns: "
                + getAbortedAndReadOnlyTxns(txnIds, stmt)
                + getCommittedTxns(txnIds, stmt);
            LOG.error(errorMsg);

            throw new IllegalStateException("Write ID allocation failed on " + TableName.getDbTable(dbName, tblName)
                + " as not all input txns in open state or read-only");
          }
        }

        List<String> queries = new ArrayList<>();
        StringBuilder prefix = new StringBuilder();
        StringBuilder suffix = new StringBuilder();
        long writeId;
        int allocatedTxnsCount = 0;
        List<String> params = Arrays.asList(dbName, tblName);
        if (shouldReallocate) {
          // during query recompilation after lock acquistion, it is important to realloc new writeIds
          // to ensure writeIds are committed in increasing order.
          jdbcResource.execute(new DeleteFromTxnToWriteIdCommand(dbName, tblName, txnIds));
        } else {
          // Traverse the TXN_TO_WRITE_ID to see if any of the input txns already have allocated a
          // write id for the same db.table. If yes, then need to reuse it else have to allocate new one
          // The write id would have been already allocated in case of multi-statement txns where
          // first write on a table will allocate write id and rest of the writes should re-use it.
          prefix.append("SELECT \"T2W_TXNID\", \"T2W_WRITEID\" FROM \"TXN_TO_WRITE_ID\" WHERE")
              .append(" \"T2W_DATABASE\" = ? AND \"T2W_TABLE\" = ? AND ");
          TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix,
              txnIds, "\"T2W_TXNID\"", false, false);
          for (String query : queries) {
            pStmt = sqlGenerator.prepareStmtWithParameters(dbConn, query, params);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Going to execute query <" + query.replace("?", "{}") + ">",
                  quoteString(dbName), quoteString(tblName));
            }
            rs = pStmt.executeQuery();
            while (rs.next()) {
              // If table write ID is already allocated for the given transaction, then just use it
              long txnId = rs.getLong(1);
              writeId = rs.getLong(2);
              txnToWriteIds.add(new TxnToWriteId(txnId, writeId));
              allocatedTxnsCount++;
              LOG.info("Reused already allocated writeID: {} for txnId: {}", writeId, txnId);
            }
            closeStmt(pStmt);
          }
        }

        // Batch allocation should always happen atomically. Either write ids for all txns is allocated or none.
        long numOfWriteIds = txnIds.size();
        assert ((allocatedTxnsCount == 0) || (numOfWriteIds == allocatedTxnsCount));
        if (allocatedTxnsCount == numOfWriteIds) {
          // If all the txns in the list have pre-allocated write ids for the given table, then just return.
          // This is for idempotent case.
          return new AllocateTableWriteIdsResponse(txnToWriteIds);
        }

        long srcWriteId = 0;
        if (rqst.isSetReplPolicy()) {
          // In replication flow, we always need to allocate write ID equal to that of source.
          assert (srcTxnToWriteIds != null);
          srcWriteId = srcTxnToWriteIds.get(0).getWriteId();
        }


        // There are some txns in the list which does not have write id allocated and hence go ahead and do it.
        // Get the next write id for the given table and update it with new next write id.
        // This is select for update query which takes a lock if the table entry is already there in NEXT_WRITE_ID
        String s = sqlGenerator.addForUpdateClause(
            "SELECT \"NWI_NEXT\" FROM \"NEXT_WRITE_ID\" WHERE \"NWI_DATABASE\" = ? AND \"NWI_TABLE\" = ?");        
        closeStmt(pStmt);
        pStmt = sqlGenerator.prepareStmtWithParameters(dbConn, s, params);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Going to execute query <" + s.replace("?", "{}") + ">",
              quoteString(dbName), quoteString(tblName));
        }
        rs = pStmt.executeQuery();
        if (!rs.next()) {
          // First allocation of write id should add the table to the next_write_id meta table
          // The initial value for write id should be 1 and hence we add 1 with number of write ids allocated here
          // For repl flow, we need to force set the incoming write id.
          writeId = (srcWriteId > 0) ? srcWriteId : 1;
          s = "INSERT INTO \"NEXT_WRITE_ID\" (\"NWI_DATABASE\", \"NWI_TABLE\", \"NWI_NEXT\") VALUES (?, ?, "
              + (writeId + numOfWriteIds) + ")";
          closeStmt(pStmt);
          pStmt = sqlGenerator.prepareStmtWithParameters(dbConn, s, params);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Going to execute insert <" + s.replace("?", "{}") + ">",
                quoteString(dbName), quoteString(tblName));
          }
          pStmt.execute();
        } else {
          long nextWriteId = rs.getLong(1);
          writeId = (srcWriteId > 0) ? srcWriteId : nextWriteId;

          // Update the NEXT_WRITE_ID for the given table after incrementing by number of write ids allocated
          s = "UPDATE \"NEXT_WRITE_ID\" SET \"NWI_NEXT\" = " + (writeId + numOfWriteIds)
              + " WHERE \"NWI_DATABASE\" = ? AND \"NWI_TABLE\" = ?";
          closeStmt(pStmt);
          pStmt = sqlGenerator.prepareStmtWithParameters(dbConn, s, params);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Going to execute update <" + s.replace("?", "{}") + ">",
                quoteString(dbName), quoteString(tblName));
          }
          pStmt.executeUpdate();

          // For repl flow, if the source write id is mismatching with target next write id, then current
          // metadata in TXN_TO_WRITE_ID is stale for this table and hence need to clean-up TXN_TO_WRITE_ID.
          // This is possible in case of first incremental repl after bootstrap where concurrent write
          // and drop table was performed at source during bootstrap dump.
          if ((srcWriteId > 0) && (srcWriteId != nextWriteId)) {
            s = "DELETE FROM \"TXN_TO_WRITE_ID\" WHERE \"T2W_DATABASE\" = ? AND \"T2W_TABLE\" = ?";
            closeStmt(pStmt);
            pStmt = sqlGenerator.prepareStmtWithParameters(dbConn, s, params);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Going to execute delete <" + s.replace("?", "{}") + ">",
                  quoteString(dbName), quoteString(tblName));
            }
            pStmt.executeUpdate();
          }
        }

        // Map the newly allocated write ids against the list of txns which doesn't have pre-allocated write ids
        try (PreparedStatement pstmt = dbConn.prepareStatement(TXN_TO_WRITE_ID_INSERT_QUERY)) {
          for (long txnId : txnIds) {
            pstmt.setLong(1, txnId);
            pstmt.setString(2, dbName);
            pstmt.setString(3, tblName);
            pstmt.setLong(4, writeId);
            pstmt.addBatch();

            txnToWriteIds.add(new TxnToWriteId(txnId, writeId));
            LOG.info("Allocated writeId: {} for txnId: {}", writeId, txnId);
            writeId++;
            if (txnToWriteIds.size() % maxBatchSize == 0) {
              LOG.debug("Executing a batch of <{}> queries. Batch size: {}", TXN_TO_WRITE_ID_INSERT_QUERY,
                  maxBatchSize);
              pstmt.executeBatch();
            }
          }
          if (txnToWriteIds.size() % maxBatchSize != 0) {
            LOG.debug("Executing a batch of <{}> queries. Batch size: {}", TXN_TO_WRITE_ID_INSERT_QUERY,
                txnToWriteIds.size() % maxBatchSize);
            pstmt.executeBatch();
          }
        }

        if (transactionalListeners != null) {
          MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners,
              EventMessage.EventType.ALLOC_WRITE_ID,
              new AllocWriteIdEvent(txnToWriteIds, dbName, tblName),
              dbConn, sqlGenerator);
        }

        LOG.info("Allocated write ids for dbName={}, tblName={} (txnIds: {})", dbName, tblName, rqst.getTxnIds());
        jdbcResource.getTransactionManager().commit(context);
        return new AllocateTableWriteIdsResponse(txnToWriteIds);
      } catch (SQLException e) {
        LOG.error("Exception during write ids allocation for request={}. Will retry if possible.", rqst, e);
        checkRetryable(e, "allocateTableWriteIds(" + rqst + ")", true);
        throw new MetaException("Unable to update transaction database "
            + StringUtils.stringifyException(e));
      } finally {
        close(rs, pStmt, null);
        unlockInternal();
        jdbcResource.unbindDataSource();
      }
    } catch (RetryException e) {
      return allocateTableWriteIds(rqst);
    }  
  }

  /**
   * Checks if all the txns in the list are in open state and not read-only.
   * @param txnIds list of txns to be evaluated for open state/read-only status
   * @param stmt db statement
   * @return If all the txns in open state and not read-only, then return true else false
   */
  private boolean isTxnsOpenAndNotReadOnly(List<Long> txnIds, Statement stmt) throws SQLException {
    List<String> queries = new ArrayList<>();
    StringBuilder prefix = new StringBuilder();

    // Get the count of txns from the given list that are in open state and not read-only.
    // If the returned count is same as the input number of txns, then all txns are in open state and not read-only.
    prefix.append("SELECT COUNT(*) FROM \"TXNS\" WHERE \"TXN_STATE\" = " + TxnStatus.OPEN
        + " AND \"TXN_TYPE\" != " + TxnType.READ_ONLY.getValue() + " AND ");

    TxnUtils.buildQueryWithINClause(conf, queries, prefix, new StringBuilder(),
        txnIds, "\"TXN_ID\"", false, false);

    long count = 0;
    for (String query : queries) {
      LOG.debug("Going to execute query <{}>", query);
      try (ResultSet rs = stmt.executeQuery(query)) {
        if (rs.next()) {
          count += rs.getLong(1);
        }
      }
    }
    return count == txnIds.size();
  }

  /**
   * Get txns from the list that are either aborted or read-only.
   * @param txnIds list of txns to be evaluated for aborted state/read-only status
   * @param stmt db statement
   */
  private String getAbortedAndReadOnlyTxns(List<Long> txnIds, Statement stmt) throws SQLException {
    List<String> queries = new ArrayList<>();
    StringBuilder prefix = new StringBuilder();

    // Check if any of the txns in the list are either aborted or read-only.
    prefix.append("SELECT \"TXN_ID\", \"TXN_STATE\", \"TXN_TYPE\" FROM \"TXNS\" WHERE ");
    TxnUtils.buildQueryWithINClause(conf, queries, prefix, new StringBuilder(),
        txnIds, "\"TXN_ID\"", false, false);
    StringBuilder txnInfo = new StringBuilder();

    for (String query : queries) {
      LOG.debug("Going to execute query <{}>", query);
      try (ResultSet rs = stmt.executeQuery(query)) {
        while (rs.next()) {
          long txnId = rs.getLong(1);
          TxnStatus txnState = TxnStatus.fromString(rs.getString(2));
          TxnType txnType = TxnType.findByValue(rs.getInt(3));

          if (txnState != TxnStatus.OPEN) {
            txnInfo.append("{").append(txnId).append(",").append(txnState).append("}");
          } else if (txnType == TxnType.READ_ONLY) {
            txnInfo.append("{").append(txnId).append(",read-only}");
          }
        }
      }
    }
    return txnInfo.toString();
  }

  /**
   * Get txns from the list that are committed.
   * @param txnIds list of txns to be evaluated for committed state
   * @param stmt db statement
   */
  private String getCommittedTxns(List<Long> txnIds, Statement stmt) throws SQLException {
    List<String> queries = new ArrayList<>();
    StringBuilder prefix = new StringBuilder();

    // Check if any of the txns in the list are committed.
    prefix.append("SELECT \"CTC_TXNID\" FROM \"COMPLETED_TXN_COMPONENTS\" WHERE ");
    TxnUtils.buildQueryWithINClause(conf, queries, prefix, new StringBuilder(),
        txnIds, "\"CTC_TXNID\"", false, false);
    StringBuilder txnInfo = new StringBuilder();

    for (String query : queries) {
      LOG.debug("Going to execute query <{}>", query);
      try (ResultSet rs = stmt.executeQuery(query)) {
        while (rs.next()) {
          long txnId = rs.getLong(1);
          txnInfo.append("{").append(txnId).append(",c}");
        }
      }
    }
    return txnInfo.toString();
  }
  
  private static class DeleteFromTxnToWriteIdCommand implements InClauseBatchCommand<Long> {
    
    private final String dbName;
    private final String tblName;
    private final List<Long> txnIds;

    public DeleteFromTxnToWriteIdCommand(String dbName, String tblName, List<Long> txnIds) {
      this.dbName = dbName;
      this.tblName = tblName;
      this.txnIds = txnIds;
    }

    @Override
    public String getParameterizedQueryString(DatabaseProduct databaseProduct) {
      return "DELETE FROM \"TXN_TO_WRITE_ID\" WHERE \"T2W_DATABASE\" = :dbName AND \"T2W_TABLE\" = :tableName AND " +
          "\"T2W_TXNID\" IN (:txnIds)";
    }

    @Override
    public SqlParameterSource getQueryParameters() {
      return new MapSqlParameterSource()
          .addValue("dbName", dbName)
          .addValue("tableName", tblName);
    }

    @Override
    public List<Long> getInClauseParameters() {
      return txnIds;
    }

    @Override
    public String getInClauseParameterName() {
      return "txnIds";
    }

    @Override
    public Comparator<Long> getParameterLengthComparator() {
      return Long::compareTo;
    }
  }

}