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

import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.TransactionalMetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.AbortTxnEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.txn.TxnErrorMsg;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionContext;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.GetTxnDbsUpdatedHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;

public class PerformTimeoutsFunction implements TransactionalFunction<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(PerformTimeoutsFunction.class);

  private static final String SELECT_TIMED_OUT_LOCKS_QUERY = "SELECT DISTINCT \"HL_LOCK_EXT_ID\" FROM \"HIVE_LOCKS\" " +
      "WHERE \"HL_LAST_HEARTBEAT\" < %s - :timeout AND \"HL_TXNID\" = 0";

  public static int TIMED_OUT_TXN_ABORT_BATCH_SIZE = 50000;

  private final long timeout;
  private final long replicationTxnTimeout;
  private final List<TransactionalMetaStoreEventListener> transactionalListeners;  

  public PerformTimeoutsFunction(long timeout, long replicationTxnTimeout, List<TransactionalMetaStoreEventListener> transactionalListeners) {
    this.timeout = timeout;
    this.replicationTxnTimeout = replicationTxnTimeout;
    this.transactionalListeners = transactionalListeners;
  }

  @Override
  public Void execute(MultiDataSourceJdbcResource jdbcResource) {
    DatabaseProduct dbProduct = jdbcResource.getDatabaseProduct();
    try {
      //We currently commit after selecting the TXNS to abort.  So whether SERIALIZABLE
      //READ_COMMITTED, the effect is the same.  We could use FOR UPDATE on Select from TXNS
      //and do the whole performTimeOuts() in a single huge transaction, but the only benefit
      //would be to make sure someone cannot heartbeat one of these txns at the same time.
      //The attempt to heartbeat would block and fail immediately after it's unblocked.
      //With current (RC + multiple txns) implementation it is possible for someone to send
      //heartbeat at the very end of the expiry interval, and just after the Select from TXNS
      //is made, in which case heartbeat will succeed but txn will still be Aborted.
      //Solving this corner case is not worth the perf penalty.  The client should heartbeat in a
      //timely way.
      timeOutLocks(jdbcResource, dbProduct);
      while (true) {
        String s = " \"TXN_ID\", \"TXN_TYPE\" FROM \"TXNS\" WHERE \"TXN_STATE\" = " + TxnStatus.OPEN +
            " AND (" +
            "\"TXN_TYPE\" != " + TxnType.REPL_CREATED.getValue() +
            " AND \"TXN_LAST_HEARTBEAT\" <  " + getEpochFn(dbProduct) + "-" + timeout +
            " OR " +
            " \"TXN_TYPE\" = " + TxnType.REPL_CREATED.getValue() +
            " AND \"TXN_LAST_HEARTBEAT\" <  " + getEpochFn(dbProduct) + "-" + replicationTxnTimeout +
            ")";
        //safety valve for extreme cases
        s = jdbcResource.getSqlGenerator().addLimitClause(10 * TIMED_OUT_TXN_ABORT_BATCH_SIZE, s);

        LOG.debug("Going to execute query <{}>", s);
        List<Map<Long, TxnType>> timedOutTxns = Objects.requireNonNull(jdbcResource.getJdbcTemplate().query(s, rs -> {
          List<Map<Long, TxnType>> txnbatch = new ArrayList<>();
          Map<Long, TxnType> currentBatch = new HashMap<>(TIMED_OUT_TXN_ABORT_BATCH_SIZE);
          while (rs.next()) {
            currentBatch.put(rs.getLong(1),TxnType.findByValue(rs.getInt(2)));
            if (currentBatch.size() == TIMED_OUT_TXN_ABORT_BATCH_SIZE) {
              txnbatch.add(currentBatch);
              currentBatch = new HashMap<>(TIMED_OUT_TXN_ABORT_BATCH_SIZE);
            }
          }
          if (!currentBatch.isEmpty()) {
            txnbatch.add(currentBatch);
          }
          return txnbatch;
        }), "This never should be null, it's just to suppress warnings");
        if (timedOutTxns.isEmpty()) {
          return null;
        }

        TransactionContext context = jdbcResource.getTransactionManager().getActiveTransaction();
        Object savePoint = context.createSavepoint();

        int numTxnsAborted = 0;
        for (Map<Long, TxnType> batchToAbort : timedOutTxns) {
          context.releaseSavepoint(savePoint);
          savePoint = context.createSavepoint();
          int abortedTxns = new AbortTxnsFunction(new ArrayList<>(batchToAbort.keySet()), true, false, false, 
              TxnErrorMsg.ABORT_TIMEOUT).execute(jdbcResource);
          
          if (abortedTxns == batchToAbort.size()) {
            numTxnsAborted += batchToAbort.size();
            //todo: add TXNS.COMMENT filed and set it to 'aborted by system due to timeout'
            LOG.info("Aborted the following transactions due to timeout: {}", batchToAbort);
            if (transactionalListeners != null) {
              for (Map.Entry<Long, TxnType> txnEntry : batchToAbort.entrySet()) {
                List<String> dbsUpdated = jdbcResource.execute(new GetTxnDbsUpdatedHandler(txnEntry.getKey()));
                MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners,
                    EventMessage.EventType.ABORT_TXN,
                    new AbortTxnEvent(txnEntry.getKey(), txnEntry.getValue(), null, dbsUpdated),
                    jdbcResource.getConnection(), jdbcResource.getSqlGenerator());
              }
              LOG.debug("Added Notifications for the transactions that are aborted due to timeout: {}", batchToAbort);
            }
          } else {
            //could not abort all txns in this batch - this may happen because in parallel with this
            //operation there was activity on one of the txns in this batch (commit/abort/heartbeat)
            //This is not likely but may happen if client experiences long pause between heartbeats or
            //unusually long/extreme pauses between heartbeat() calls and other logic in checkLock(),
            //lock(), etc.
            context.rollbackToSavepoint(savePoint);
          }
        }
        LOG.info("Aborted {} transaction(s) due to timeout", numTxnsAborted);
        if (MetastoreConf.getBoolVar(jdbcResource.getConf(), MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_EXT_ON)) {
          Metrics.getOrCreateCounter(MetricsConstants.TOTAL_NUM_TIMED_OUT_TXNS).inc(numTxnsAborted);
        }
      }
    } catch (Exception e) {
      LOG.warn("Aborting timed out transactions failed due to " + e.getMessage(), e);
    }
    return null;
  }

  // Clean time out locks from the database not associated with a transactions, i.e. locks
  // for read-only autoCommit=true statements.  This does a commit,
  // and thus should be done before any calls to heartbeat that will leave
  // open transactions.
  private void timeOutLocks(MultiDataSourceJdbcResource jdbcResource, DatabaseProduct dbProduct) {
    //doing a SELECT first is less efficient but makes it easier to debug things
    //when txnid is <> 0, the lock is associated with a txn and is handled by performTimeOuts()
    //want to avoid expiring locks for a txn w/o expiring the txn itself
    try {
      Set<Long> timedOutLockIds = new TreeSet<>(
          jdbcResource.getJdbcTemplate().query(String.format(SELECT_TIMED_OUT_LOCKS_QUERY, getEpochFn(dbProduct)),
              new MapSqlParameterSource().addValue("timeout", timeout),
              (rs, rowNum) -> rs.getLong(1)));
      if (timedOutLockIds.isEmpty()) {
        LOG.debug("Did not find any timed-out locks, therefore retuning.");
        return;
      }

      List<String> queries = new ArrayList<>();
      StringBuilder prefix = new StringBuilder();
      StringBuilder suffix = new StringBuilder();

      //include same hl_last_heartbeat condition in case someone heartbeated since the select
      prefix.append("DELETE FROM \"HIVE_LOCKS\" WHERE \"HL_LAST_HEARTBEAT\" < ");
      prefix.append(getEpochFn(dbProduct)).append("-").append(timeout);
      prefix.append(" AND \"HL_TXNID\" = 0 AND ");

      TxnUtils.buildQueryWithINClause(jdbcResource.getConf(), queries, prefix, suffix, timedOutLockIds,
          "\"HL_LOCK_EXT_ID\"", true, false);

      int deletedLocks = 0;
      for (String query : queries) {
        LOG.debug("Going to execute update: <{}>", query);
        deletedLocks += jdbcResource.getJdbcTemplate().update(query, new MapSqlParameterSource());
      }
      if (deletedLocks > 0) {
        LOG.info("Deleted {} locks due to timed-out. Lock ids: {}", deletedLocks, timedOutLockIds);
      }
    } catch (Exception ex) {
      LOG.error("Failed to purge timed-out locks: " + ex.getMessage(), ex);
    }
  }
  
}
