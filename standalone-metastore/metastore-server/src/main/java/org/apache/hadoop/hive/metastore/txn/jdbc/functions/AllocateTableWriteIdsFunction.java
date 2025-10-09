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
package org.apache.hadoop.hive.metastore.txn.jdbc.functions;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.TransactionalMetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.events.AllocWriteIdEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.commands.AddWriteIdsToTxnToWriteIdCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.TargetTxnIdListHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.InClauseBatchCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class AllocateTableWriteIdsFunction implements TransactionalFunction<AllocateTableWriteIdsResponse> {

  private static final Logger LOG = LoggerFactory.getLogger(AbortTxnFunction.class);

  private final AllocateTableWriteIdsRequest rqst;
  private final List<TransactionalMetaStoreEventListener> transactionalListeners;

  public AllocateTableWriteIdsFunction(AllocateTableWriteIdsRequest rqst, List<TransactionalMetaStoreEventListener> transactionalListeners) {
    this.rqst = rqst;
    this.transactionalListeners = transactionalListeners;
  }

  @Override
  public AllocateTableWriteIdsResponse execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    List<Long> txnIds;
    String dbName = rqst.getDbName().toLowerCase();
    String tblName = rqst.getTableName().toLowerCase();
    boolean shouldReallocate = rqst.isReallocate();
    Connection dbConn = jdbcResource.getConnection();
    List<TxnToWriteId> txnToWriteIds = new ArrayList<>();
    List<TxnToWriteId> srcTxnToWriteIds = null;

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
    if (!isTxnsOpenAndNotReadOnly(jdbcResource, txnIds)) {
      String errorMsg = "Write ID allocation on " + TableName.getDbTable(dbName, tblName)
          + " failed for input txns: "
          + getAbortedAndReadOnlyTxns(jdbcResource, txnIds)
          + getCommittedTxns(jdbcResource, txnIds);
      LOG.error(errorMsg);

      throw new IllegalStateException("Write ID allocation failed on " + TableName.getDbTable(dbName, tblName)
          + " as not all input txns in open state or read-only");
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
      jdbcResource.execute(new InClauseBatchCommand<>(
          "DELETE FROM \"TXN_TO_WRITE_ID\" WHERE \"T2W_DATABASE\" = :dbName AND \"T2W_TABLE\" = :tableName AND " +
              "\"T2W_TXNID\" IN (:txnIds)", 
          new MapSqlParameterSource()
              .addValue("dbName", dbName)
              .addValue("tableName", tblName)
              .addValue("txnIds", txnIds),
          "txnIds", Long::compareTo));
    } else {
      // Traverse the TXN_TO_WRITE_ID to see if any of the input txns already have allocated a
      // write id for the same db.table. If yes, then need to reuse it else have to allocate new one
      // The write id would have been already allocated in case of multi-statement txns where
      // first write on a table will allocate write id and rest of the writes should re-use it.
      prefix.append("SELECT \"T2W_TXNID\", \"T2W_WRITEID\" FROM \"TXN_TO_WRITE_ID\" WHERE")
          .append(" \"T2W_DATABASE\" = ? AND \"T2W_TABLE\" = ? AND ");
      TxnUtils.buildQueryWithINClause(jdbcResource.getConf(), queries, prefix, suffix,
          txnIds, "\"T2W_TXNID\"", false, false);
      for (String query : queries) {
        try (PreparedStatement pStmt = jdbcResource.getSqlGenerator().prepareStmtWithParameters(dbConn, query, params)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Going to execute query <" + query.replace("?", "'{}'") + ">", dbName, tblName);
          }
          try (ResultSet rs = pStmt.executeQuery()) {
            while (rs.next()) {
              // If table write ID is already allocated for the given transaction, then just use it
              long txnId = rs.getLong(1);
              writeId = rs.getLong(2);
              txnToWriteIds.add(new TxnToWriteId(txnId, writeId));
              allocatedTxnsCount++;
              LOG.info("Reused already allocated writeID: {} for txnId: {}", writeId, txnId);
            }            
          }          
        } catch (SQLException e) {
          throw new UncategorizedSQLException(null, null, e);
        }
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
    String query = jdbcResource.getSqlGenerator().addForUpdateClause(
        "SELECT \"NWI_NEXT\" FROM \"NEXT_WRITE_ID\" WHERE \"NWI_DATABASE\" = :dbName AND \"NWI_TABLE\" = :tableName");
    if (LOG.isDebugEnabled()) {
      LOG.debug("Going to execute query {}", query);
    }
    
    Long nextWriteId = jdbcResource.getJdbcTemplate().query(query, 
        new MapSqlParameterSource()
            .addValue("dbName", dbName)
            .addValue("tableName", tblName),
        (ResultSet rs) -> rs.next() ? rs.getLong(1) : null);
    
    if (nextWriteId == null) {
      query = "INSERT INTO \"NEXT_WRITE_ID\" (\"NWI_DATABASE\", \"NWI_TABLE\", \"NWI_NEXT\") " +
          "VALUES (:dbName, :tableName, :nextId)";
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to execute query {}", query);
      }
      
      // First allocation of write id should add the table to the next_write_id meta table
      // The initial value for write id should be 1 and hence we add 1 with number of write ids allocated here
      // For repl flow, we need to force set the incoming write id.
      writeId = (srcWriteId > 0) ? srcWriteId : 1;
      jdbcResource.getJdbcTemplate().update(query,
          new MapSqlParameterSource()
              .addValue("dbName", dbName)
              .addValue("tableName", tblName)
              .addValue("nextId", writeId + numOfWriteIds));      
    } else {
      query = "UPDATE \"NEXT_WRITE_ID\" SET \"NWI_NEXT\" = :nextId WHERE \"NWI_DATABASE\" = :dbName AND \"NWI_TABLE\" = :tableName";
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to execute query {}", query);
      }

      writeId = (srcWriteId > 0) ? srcWriteId : nextWriteId;
      // Update the NEXT_WRITE_ID for the given table after incrementing by number of write ids allocated
      jdbcResource.getJdbcTemplate().update(query,
          new MapSqlParameterSource()
              .addValue("dbName", dbName)
              .addValue("tableName", tblName)
              .addValue("nextId", writeId + numOfWriteIds));

      // For repl flow, if the source write id is mismatching with target next write id, then current
      // metadata in TXN_TO_WRITE_ID is stale for this table and hence need to clean-up TXN_TO_WRITE_ID.
      // This is possible in case of first incremental repl after bootstrap where concurrent write
      // and drop table was performed at source during bootstrap dump.
      if ((srcWriteId > 0) && (srcWriteId != nextWriteId)) {
        query = "DELETE FROM \"TXN_TO_WRITE_ID\" WHERE \"T2W_DATABASE\" = :dbName AND \"T2W_TABLE\" = :tableName";
        if (LOG.isDebugEnabled()) {
          LOG.debug("Going to execute query {}", query);
        }

        jdbcResource.getJdbcTemplate().update(query,
            new MapSqlParameterSource()
                .addValue("dbName", dbName)
                .addValue("tableName", tblName));
      }
    }

    // Map the newly allocated write ids against the list of txns which doesn't have pre-allocated write ids
    jdbcResource.execute(new AddWriteIdsToTxnToWriteIdCommand(dbName, tblName, writeId, txnIds, txnToWriteIds));

    if (transactionalListeners != null) {
      MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners,
          EventMessage.EventType.ALLOC_WRITE_ID,
          new AllocWriteIdEvent(txnToWriteIds, dbName, tblName),
          dbConn, jdbcResource.getSqlGenerator());
    }

    LOG.info("Allocated write ids for dbName={}, tblName={} (txnIds: {})", dbName, tblName, rqst.getTxnIds());
    return new AllocateTableWriteIdsResponse(txnToWriteIds);
  }

  /**
   * Checks if all the txns in the list are in open state and not read-only.
   * @param txnIds list of txns to be evaluated for open state/read-only status
   * @return If all the txns in open state and not read-only, then return true else false
   */
  private boolean isTxnsOpenAndNotReadOnly(MultiDataSourceJdbcResource jdbcResource, List<Long> txnIds) {
    List<String> queries = new ArrayList<>();
    StringBuilder prefix = new StringBuilder();

    // Get the count of txns from the given list that are in open state and not read-only.
    // If the returned count is same as the input number of txns, then all txns are in open state and not read-only.
    prefix.append("SELECT COUNT(*) FROM \"TXNS\" WHERE \"TXN_STATE\" = ").append(TxnStatus.OPEN)
        .append(" AND \"TXN_TYPE\" != ").append(TxnType.READ_ONLY.getValue()).append(" AND ");

    TxnUtils.buildQueryWithINClause(jdbcResource.getConf(), queries, prefix, new StringBuilder(),
        txnIds, "\"TXN_ID\"", false, false);

    AtomicLong count = new AtomicLong(0);
    for (String query : queries) {
      LOG.debug("Going to execute query <{}>", query);
      jdbcResource.getJdbcTemplate().query(query, rs -> {
        while (rs.next()) {
          count.set(count.get() + rs.getLong(1));
        }
        return null;
      });
    }
    return count.get() == txnIds.size();
  }

  /**
   * Get txns from the list that are either aborted or read-only.
   * @param txnIds list of txns to be evaluated for aborted state/read-only status
   */
  private String getAbortedAndReadOnlyTxns(MultiDataSourceJdbcResource jdbcResource, List<Long> txnIds) {
    List<String> queries = new ArrayList<>();
    StringBuilder prefix = new StringBuilder();

    // Check if any of the txns in the list are either aborted or read-only.
    prefix.append("SELECT \"TXN_ID\", \"TXN_STATE\", \"TXN_TYPE\" FROM \"TXNS\" WHERE ");
    TxnUtils.buildQueryWithINClause(jdbcResource.getConf(), queries, prefix, new StringBuilder(),
        txnIds, "\"TXN_ID\"", false, false);
    StringBuilder txnInfo = new StringBuilder();

    for (String query : queries) {
      LOG.debug("Going to execute query <{}>", query);
      jdbcResource.getJdbcTemplate().query(query, rs -> {
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
        return null;
      });
    }
    return txnInfo.toString();
  }

  /**
   * Get txns from the list that are committed.
   * @param txnIds list of txns to be evaluated for committed state
   */
  private String getCommittedTxns(MultiDataSourceJdbcResource jdbcResource, List<Long> txnIds) {
    List<String> queries = new ArrayList<>();
    StringBuilder prefix = new StringBuilder();

    // Check if any of the txns in the list are committed.
    prefix.append("SELECT \"CTC_TXNID\" FROM \"COMPLETED_TXN_COMPONENTS\" WHERE ");
    TxnUtils.buildQueryWithINClause(jdbcResource.getConf(), queries, prefix, new StringBuilder(),
        txnIds, "\"CTC_TXNID\"", false, false);
    StringBuilder txnInfo = new StringBuilder();

    for (String query : queries) {
      LOG.debug("Going to execute query <{}>", query);
      jdbcResource.getJdbcTemplate().query(query, rs -> {
        while (rs.next()) {
          long txnId = rs.getLong(1);
          txnInfo.append("{").append(txnId).append(",c}");
        }          
        return null;
      });
    }
    return txnInfo.toString();
  }
  
}