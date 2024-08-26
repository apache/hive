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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.classification.RetrySemantics;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.TransactionalMetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.AbortCompactResponse;
import org.apache.hadoop.hive.metastore.api.AbortCompactionRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoRequest;
import org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.MaxAllocatedTableWriteIdRequest;
import org.apache.hadoop.hive.metastore.api.MaxAllocatedTableWriteIdResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchCompactionException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ReplTblWriteIdStateRequest;
import org.apache.hadoop.hive.metastore.api.SeedTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.SeedTxnIdRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.api.UpdateTransactionalStatsRequest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.datasource.DataSourceProvider;
import org.apache.hadoop.hive.metastore.datasource.DataSourceProviderFactory;
import org.apache.hadoop.hive.metastore.events.AbortTxnEvent;
import org.apache.hadoop.hive.metastore.events.AcidWriteEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionState;
import org.apache.hadoop.hive.metastore.txn.entities.LockInfo;
import org.apache.hadoop.hive.metastore.txn.entities.MetricsInfo;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.jdbc.commands.*;
import org.apache.hadoop.hive.metastore.txn.jdbc.functions.*;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.*;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.NoPoolConnectionPool;
import org.apache.hadoop.hive.metastore.txn.jdbc.ParameterizedCommand;
import org.apache.hadoop.hive.metastore.txn.retry.SqlRetryCallProperties;
import org.apache.hadoop.hive.metastore.txn.retry.SqlRetryException;
import org.apache.hadoop.hive.metastore.txn.retry.SqlRetryHandler;
import org.apache.hadoop.hive.metastore.txn.service.AcidHouseKeeperService;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;

import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

/**
 * A handler to answer transaction related calls that come into the metastore
 * server.
 * <p>
 * Note on log messages:  Please include txnid:X and lockid info using
 * {@link JavaUtils#txnIdToString(long)}
 * and {@link JavaUtils#lockIdToString(long)} in all messages.
 * The txnid:X and lockid:Y matches how Thrift object toString() methods are generated,
 * so keeping the format consistent makes grep'ing the logs much easier.
 * <p>
 * Note on HIVE_LOCKS.hl_last_heartbeat.
 * For locks that are part of transaction, we set this 0 (would rather set it to NULL but
 * Currently the DB schema has this NOT NULL) and only update/read heartbeat from corresponding
 * transaction in TXNS.
 * <p>
 * In general there can be multiple metastores where this logic can execute, thus the DB is
 * used to ensure proper mutexing of operations.
 * Select ... For Update (or equivalent: either MsSql with(updlock) or actual Update stmt) is
 * used to properly sequence operations.  Most notably:
 * 1. various sequence IDs are generated with aid of this mutex
 * 2. ensuring that each (Hive) Transaction state is transitioned atomically.  Transaction state
 *  includes its actual state (Open, Aborted) as well as it's lock list/component list.  Thus all
 *  per transaction ops, either start by update/delete of the relevant TXNS row or do S4U on that row.
 *  This allows almost all operations to run at READ_COMMITTED and minimizes DB deadlocks.
 * 3. checkLock() - this is mutexted entirely since we must ensure that while we check if some lock
 *  can be granted, no other (strictly speaking "earlier") lock can change state.
 * <p>
 * The exception to this is Derby which doesn't support proper S4U.  Derby is always running embedded
 * (this is the only supported configuration for Derby)
 * in the same JVM as HiveMetaStoreHandler thus we use JVM wide lock to properly sequnce the operations.
 * <p>

 * If we ever decide to run remote Derby server, according to
 * https://db.apache.org/derby/docs/10.0/manuals/develop/develop78.html all transactions will be
 * seriazlied, so that would also work though has not been tested.
 * <p>
 * General design note:
 * It's imperative that any operation on a txn (e.g. commit), ensure (atomically) that this txn is
 * still valid and active.  In the code this is usually achieved at the same time the txn record
 * is locked for some operation.
 * <p>
 * Note on retry logic:
 * Metastore has retry logic in both {@link org.apache.hadoop.hive.metastore.RetryingMetaStoreClient}
 * and {@link org.apache.hadoop.hive.metastore.RetryingHMSHandler}.  The retry logic there is very
 * generic and is not aware whether the operations are idempotent or not.  (This is separate from
 * retry logic here in TxnHander which can/does retry DB errors intelligently).  The worst case is
 * when an op here issues a successful commit against the RDBMS but the calling stack doesn't
 * receive the ack and retries.  (If an op fails before commit, it's trivially idempotent)
 * Thus the ops here need to be made idempotent as much as possible or
 * the metstore call stack should have logic not to retry.  There are {@link RetrySemantics}
 * annotations to document the behavior.
 */
@SuppressWarnings("SqlSourceToSinkFlow")
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class TxnHandler implements TxnStore, TxnStore.MutexAPI {

  
  public final static class ConfVars {
    
    private ConfVars() {}

    // Whether to use min_history_level table or not.
    // At startup we read it from the config, but set it to false if min_history_level does nto exists.
    private boolean useMinHistoryLevel;
    private boolean useMinHistoryWriteId;

    public boolean useMinHistoryLevel() {
      return useMinHistoryLevel;
    }

    public void setUseMinHistoryLevel(boolean useMinHistoryLevel) {
      this.useMinHistoryLevel = useMinHistoryLevel;
    }

    public boolean useMinHistoryWriteId() {
      return useMinHistoryWriteId;
    }

    public void setUseMinHistoryWriteId(boolean useMinHistoryWriteId) {
      this.useMinHistoryWriteId = useMinHistoryWriteId;
    }

    public void init(BiPredicate<String, Boolean> tableCheck, Configuration conf){
      useMinHistoryWriteId = tableCheck.test("MIN_HISTORY_WRITE_ID",
          MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.TXN_USE_MIN_HISTORY_WRITE_ID));
      useMinHistoryLevel = tableCheck.test("MIN_HISTORY_LEVEL",
          MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.TXN_USE_MIN_HISTORY_LEVEL));
      
    }
  }
  
  private static final Logger LOG = LoggerFactory.getLogger(TxnHandler.class.getName());
  public static final TxnHandler.ConfVars ConfVars = new ConfVars();

  // Maximum number of open transactions that's allowed
  private static volatile int maxOpenTxns = 0;
  // Whether number of open transactions reaches the threshold
  private static volatile boolean tooManyOpenTxns = false;
  // Current number of open txns
  private static AtomicInteger numOpenTxns;

  private static volatile boolean initialized = false;
  private static DataSource connPool;
  private static DataSource connPoolMutex;
  protected static DataSource connPoolCompactor;

  protected static DatabaseProduct dbProduct;
  protected static SQLGenerator sqlGenerator;
  protected static long openTxnTimeOutMillis;

  /**
   * Number of consecutive deadlocks we have seen
   */
  protected Configuration conf;

  protected List<TransactionalMetaStoreEventListener> transactionalListeners;
  // (End user) Transaction timeout, in milliseconds.
  private long timeout;
  private long replicationTxnTimeout;

  private MutexAPI mutexAPI;
  private TxnLockManager txnLockManager;
  private SqlRetryHandler sqlRetryHandler;
  protected MultiDataSourceJdbcResource jdbcResource;

  private static final String hostname = JavaUtils.hostname();

  public TxnHandler() {
  }

  /**
   * This is logically part of c'tor and must be called prior to any other method.
   * Not physically part of c'tor due to use of reflection
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;

    if (!initialized) {
      synchronized (TxnHandler.class) {
        if (!initialized) {
          try (DataSourceProvider.DataSourceNameConfigurator configurator =
                   new DataSourceProvider.DataSourceNameConfigurator(conf, POOL_TX)) {
            int maxPoolSize = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.CONNECTION_POOLING_MAX_CONNECTIONS);
            if (connPool == null) {
              connPool = setupJdbcConnectionPool(conf, maxPoolSize);
            }
            if (connPoolMutex == null) {
              configurator.resetName(POOL_MUTEX);
              connPoolMutex = setupJdbcConnectionPool(conf, maxPoolSize);
            }
            if (connPoolCompactor == null) {
              configurator.resetName(POOL_COMPACTOR);
              connPoolCompactor = setupJdbcConnectionPool(conf,
                  MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.HIVE_COMPACTOR_CONNECTION_POOLING_MAX_CONNECTIONS));
            }
          }
          if (dbProduct == null) {
            try (Connection dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPool)) {
              determineDatabaseProduct(dbConn);
            } catch (SQLException e) {
              LOG.error("Unable to determine database product", e);
              throw new RuntimeException(e);
            }
          }
          if (sqlGenerator == null) {
            sqlGenerator = new SQLGenerator(dbProduct, conf);
          }
          
          initJdbcResource();

          try {
            TxnHandler.ConfVars.init(this::checkIfTableIsUsable, conf);
          } catch (Exception e) {
            String msg = "Error during TxnHandler initialization, " + e.getMessage();
            LOG.error(msg);
            throw e;
          }          
          initialized = true;
        }
      }
    }

    initJdbcResource();

    mutexAPI = new TxnStoreMutex(sqlGenerator, jdbcResource);

    numOpenTxns = Metrics.getOrCreateGauge(MetricsConstants.NUM_OPEN_TXNS);

    timeout = MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.TXN_TIMEOUT, TimeUnit.MILLISECONDS);
    replicationTxnTimeout = MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.REPL_TXN_TIMEOUT, TimeUnit.MILLISECONDS);
    maxOpenTxns = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.MAX_OPEN_TXNS);
    openTxnTimeOutMillis = MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.TXN_OPENTXN_TIMEOUT, TimeUnit.MILLISECONDS);

    try {
      transactionalListeners = MetaStoreServerUtils.getMetaStoreListeners(
          TransactionalMetaStoreEventListener.class,
          conf, MetastoreConf.getVar(conf, MetastoreConf.ConfVars.TRANSACTIONAL_EVENT_LISTENERS));
    } catch (MetaException e) {
      String msg = "Unable to get transaction listeners, " + e.getMessage();
      LOG.error(msg);
      throw new RuntimeException(e);
    }

    sqlRetryHandler = new SqlRetryHandler(conf, jdbcResource.getDatabaseProduct());
    txnLockManager = TransactionalRetryProxy.getProxy(sqlRetryHandler, jdbcResource, new DefaultTxnLockManager(jdbcResource));
  }
 
  @Override
  public SqlRetryHandler getRetryHandler() {
    return sqlRetryHandler;
  }

  @Override
  public MultiDataSourceJdbcResource getJdbcResourceHolder() {
    return jdbcResource;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public GetOpenTxnsInfoResponse getOpenTxnsInfo() throws MetaException {
    return jdbcResource.execute(new GetOpenTxnsListHandler(true, openTxnTimeOutMillis))
        .toOpenTxnsInfoResponse();
  }

  @Override
  public GetOpenTxnsResponse getOpenTxns() throws MetaException {
    return jdbcResource.execute(new GetOpenTxnsListHandler(false, openTxnTimeOutMillis))
        .toOpenTxnsResponse(Collections.singletonList(TxnType.READ_ONLY));
  }

  @Override
  public GetOpenTxnsResponse getOpenTxns(List<TxnType> excludeTxnTypes) throws MetaException {
    return jdbcResource.execute(new GetOpenTxnsListHandler(false, openTxnTimeOutMillis))
        .toOpenTxnsResponse(excludeTxnTypes);
  }

  /**
   * Retry-by-caller note:
   * Worst case, it will leave an open txn which will timeout.
   */
  @Override
  public OpenTxnsResponse openTxns(OpenTxnRequest rqst) throws MetaException {
    if (!tooManyOpenTxns && numOpenTxns.get() >= maxOpenTxns) {
      tooManyOpenTxns = true;
    }
    if (tooManyOpenTxns) {
      if (numOpenTxns.get() < maxOpenTxns * 0.9) {
        tooManyOpenTxns = false;
      } else {
        LOG.warn("Maximum allowed number of open transactions ({}) has been "
            + "reached. Current number of open transactions: {}", maxOpenTxns, numOpenTxns);
        throw new MetaException("Maximum allowed number of open transactions has been reached. " +
            "See hive.max.open.txns.");
      }
    }

    int numTxns = rqst.getNum_txns();
    if (numTxns <= 0) {
      throw new MetaException("Invalid input for number of txns: " + numTxns);
    }

    /*
     * To make {@link #getOpenTxns()}/{@link #getOpenTxnsInfo()} work correctly, this operation must ensure
     * that looking at the TXNS table every open transaction could be identified below a given High Water Mark.
     * One way to do it, would be to serialize the openTxns call with a S4U lock, but that would cause
     * performance degradation with high transaction load.
     * To enable parallel openTxn calls, we define a time period (TXN_OPENTXN_TIMEOUT) and consider every
     * transaction missing from the TXNS table in that period open, and prevent opening transaction outside
     * the period.
     * Example: At t[0] there is one open transaction in the TXNS table, T[1].
     * T[2] acquires the next sequence at t[1] but only commits into the TXNS table at t[10].
     * T[3] acquires its sequence at t[2], and commits into the TXNS table at t[3].
     * Then T[3] calculates it’s snapshot at t[4] and puts T[1] and also T[2] in the snapshot’s
     * open transaction list. T[1] because it is presented as open in TXNS,
     * T[2] because it is a missing sequence.
     *
     * In the current design, there can be several metastore instances running in a given Warehouse.
     * This makes ideas like reserving a range of IDs to save trips to DB impossible.  For example,
     * a client may go to MS1 and start a transaction with ID 500 to update a particular row.
     * Now the same client will start another transaction, except it ends up on MS2 and may get
     * transaction ID 400 and update the same row.  Now the merge that happens to materialize the snapshot
     * on read will thing the version of the row from transaction ID 500 is the latest one.
     *
     * Longer term we can consider running Active-Passive MS (at least wrt to ACID operations).  This
     * set could support a write-through cache for added performance.
     */
    /*
     * The openTxn and commitTxn must be mutexed, when committing a not read only transaction.
     * This is achieved by requesting a shared table lock here, and an exclusive one at commit.
     * Since table locks are working in Derby, we don't need the lockInternal call here.
     * Example: Suppose we have two transactions with update like x = x+1.
     * We have T[3,3] that was using a value from a snapshot with T[2,2]. If we allow committing T[3,3]
     * and opening T[4] parallel it is possible, that T[4] will be using the value from a snapshot with T[2,2],
     * and we will have a lost update problem
     */
    acquireTxnLock(true);
    // Measure the time from acquiring the sequence value, till committing in the TXNS table
    StopWatch generateTransactionWatch = new StopWatch();
    generateTransactionWatch.start();

    List<Long> txnIds = new OpenTxnsFunction(rqst, openTxnTimeOutMillis, transactionalListeners).execute(jdbcResource);

    LOG.debug("Going to commit");
    jdbcResource.getTransactionManager().getActiveTransaction().createSavepoint();
    generateTransactionWatch.stop();
    long elapsedMillis = generateTransactionWatch.getTime(TimeUnit.MILLISECONDS);
    TxnType txnType = rqst.isSetTxn_type() ? rqst.getTxn_type() : TxnType.DEFAULT;
    if (txnType != TxnType.READ_ONLY && elapsedMillis >= openTxnTimeOutMillis) {
      /*
       * The commit was too slow, we can not allow this to continue (except if it is read only,
       * since that can not cause dirty reads).
       * When calculating the snapshot for a given transaction, we look back for possible open transactions
       * (that are not yet committed in the TXNS table), for TXN_OPENTXN_TIMEOUT period.
       * We can not allow a write transaction, that was slower than TXN_OPENTXN_TIMEOUT to continue,
       * because there can be other transactions running, that didn't considered this transactionId open,
       * this could cause dirty reads.
       */
      LOG.error("OpenTxnTimeOut exceeded commit duration {}, deleting transactionIds: {}", elapsedMillis, txnIds);

      if (!txnIds.isEmpty()) {
        deleteInvalidOpenTransactions(txnIds);
      }

      /*
       * We cannot throw SQLException directly, as it is not in the throws clause
       */
      throw new SqlRetryException("OpenTxnTimeOut exceeded");
    }

    return new OpenTxnsResponse(txnIds);
  }

  @Override
  public long getOpenTxnTimeOutMillis() {
    return openTxnTimeOutMillis;
  }

  @Override
  public void setOpenTxnTimeOutMillis(long openTxnTimeOutMillis) {
    TxnHandler.openTxnTimeOutMillis = openTxnTimeOutMillis;
  }

  @Override
  public long getTargetTxnId(String replPolicy, long sourceTxnId) throws MetaException {
    List<Long> targetTxnIds =jdbcResource.execute(new TargetTxnIdListHandler(replPolicy, Collections.singletonList(sourceTxnId)));
    if (targetTxnIds.isEmpty()) {
      LOG.info("Txn {} not present for repl policy {}", sourceTxnId, replPolicy);
      return -1;
    }
    assert (targetTxnIds.size() == 1);
    return targetTxnIds.get(0);
  }

  @Override
  public void abortTxn(AbortTxnRequest rqst) throws NoSuchTxnException, MetaException, TxnAbortedException {
    TxnType txnType = new AbortTxnFunction(rqst).execute(jdbcResource); 
    if (txnType != null) {
      if (transactionalListeners != null && (!rqst.isSetReplPolicy() || !TxnType.DEFAULT.equals(rqst.getTxn_type()))) {
        List<String> dbsUpdated = getTxnDbsUpdated(rqst.getTxnid());
        MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners, EventMessage.EventType.ABORT_TXN,
            new AbortTxnEvent(rqst.getTxnid(), txnType, null, dbsUpdated), jdbcResource.getConnection(), sqlGenerator);
      }
    }
  }

  @Override
  public void abortTxns(AbortTxnsRequest rqst) throws MetaException {
    List<Long> txnIds = rqst.getTxn_ids();
    TxnErrorMsg txnErrorMsg = TxnErrorMsg.NONE;
    if (rqst.isSetErrorCode()) {
      txnErrorMsg = TxnErrorMsg.getTxnErrorMsg(rqst.getErrorCode());
    }

    List<String> queries = new ArrayList<>();
    StringBuilder prefix =
        new StringBuilder("SELECT \"TXN_ID\", \"TXN_TYPE\" from \"TXNS\" where \"TXN_STATE\" = ")
            .append(TxnStatus.OPEN)
            .append(" and \"TXN_TYPE\" != ").append(TxnType.READ_ONLY.getValue()).append(" and ");

    TxnUtils.buildQueryWithINClause(conf, queries, prefix, new StringBuilder(),
        txnIds, "\"TXN_ID\"", false, false);

    Connection dbConn = jdbcResource.getConnection();
    try {
      Map<Long, TxnType> nonReadOnlyTxns = new HashMap<>();
      for (String query : queries) {
        LOG.debug("Going to execute query <{}>", query);
        try (Statement stmt = dbConn.createStatement(); ResultSet rs = stmt.executeQuery(sqlGenerator.addForUpdateClause(query))) {
          while (rs.next()) {
            TxnType txnType = TxnType.findByValue(rs.getInt(2));
            nonReadOnlyTxns.put(rs.getLong(1), txnType);
          }
        }
      }
      int numAborted = new AbortTxnsFunction(txnIds, false, false, false, txnErrorMsg).execute(jdbcResource); 
      if (numAborted != txnIds.size()) {
        LOG.warn(
            "Abort Transactions command only aborted {} out of {} transactions. It's possible that the other"
                + " {} transactions have been aborted or committed, or the transaction ids are invalid.",
            numAborted, txnIds.size(), (txnIds.size() - numAborted));
      }

      if (transactionalListeners != null) {
        for (Long txnId : txnIds) {
          List<String> dbsUpdated = getTxnDbsUpdated(txnId);
          MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners,
              EventMessage.EventType.ABORT_TXN, new AbortTxnEvent(txnId,
                  nonReadOnlyTxns.getOrDefault(txnId, TxnType.READ_ONLY), null, dbsUpdated), dbConn, sqlGenerator);
        }
      }
    } catch (SQLException e) {
      throw new UncategorizedSQLException(null, null, e);
    }
  }

  /**
   * Concurrency/isolation notes:
   * This is mutexed with {@link #openTxns(OpenTxnRequest)} and other commitTxn(CommitTxnRequest)
   * operations using select4update on NEXT_TXN_ID. Also, mutexes on TXNS table for specific txnid:X
   * see more notes below.
   * In order to prevent lost updates, we need to determine if any 2 transactions overlap.  Each txn
   * is viewed as an interval [M,N]. M is the txnid and N is taken from the same NEXT_TXN_ID sequence
   * so that we can compare commit time of txn T with start time of txn S.  This sequence can be thought of
   * as a logical time counter. If S.commitTime &lt; T.startTime, T and S do NOT overlap.
   * <p>
   * Motivating example:
   * Suppose we have multi-statement transactions T and S both of which are attempting x = x + 1
   * In order to prevent lost update problem, then the non-overlapping txns must lock in the snapshot
   * that they read appropriately. In particular, if txns do not overlap, then one follows the other
   * (assuming they write the same entity), and thus the 2nd must see changes of the 1st.  We ensure
   * this by locking in snapshot after
   * {@link #openTxns(OpenTxnRequest)} call is made (see org.apache.hadoop.hive.ql.Driver.acquireLocksAndOpenTxn)
   * and mutexing openTxn() with commit(). In other words, once a S.commit() starts we must ensure
   * that txn T which will be considered a later txn, locks in a snapshot that includes the result
   * of S's commit (assuming no other txns).
   * As a counter example, suppose we have S[3,3] and T[4,4] (commitId=txnid means no other transactions
   * were running in parallel). If T and S both locked in the same snapshot (for example commit of
   * txnid:2, which is possible if commitTxn() and openTxnx() is not mutexed)
   * 'x' would be updated to the same value by both, i.e. lost update.
   */
  @Override
  public void commitTxn(CommitTxnRequest rqst) throws NoSuchTxnException, TxnAbortedException, MetaException {
    new CommitTxnFunction(rqst, transactionalListeners).execute(jdbcResource);
  }

  /**
   * Checks if there are transactions that are overlapping, i.e. conflicting with the current one.
   * Valid only if the caller txn holds an exclusive lock that prevents other txns to make changes to the same tables/partitions.
   * Only considers txn performing update/delete and intentionally ignores inserts.
   * @param txnid
   * @return max Id for the conflicting transaction, if any, otherwise -1
   * @throws MetaException
   */
  @Override
  public long getLatestTxnIdInConflict(long txnid) throws MetaException {
    return jdbcResource.execute(new LatestTxnIdInConflictHandler(txnid));
  }

  /**
   * Replicate Table Write Ids state to mark aborted write ids and writeid high watermark.
   * @param rqst info on table/partitions and writeid snapshot to replicate.
   * @throws MetaException
   */
  @Override
  public void replTableWriteIdState(ReplTblWriteIdStateRequest rqst) throws MetaException {
    new ReplTableWriteIdStateFunction(rqst, mutexAPI, transactionalListeners).execute(jdbcResource);
  }

  @Override
  public GetValidWriteIdsResponse getValidWriteIds(GetValidWriteIdsRequest rqst) throws MetaException {
    return new GetValidWriteIdsFunction(rqst, openTxnTimeOutMillis).execute(jdbcResource);
  }
  
  @Override
  public AllocateTableWriteIdsResponse allocateTableWriteIds(AllocateTableWriteIdsRequest rqst) throws MetaException {
    return new AllocateTableWriteIdsFunction(rqst, transactionalListeners).execute(jdbcResource);
  }

  @Override
  public MaxAllocatedTableWriteIdResponse getMaxAllocatedTableWrited(MaxAllocatedTableWriteIdRequest rqst) throws MetaException {
    return jdbcResource.execute(new GetMaxAllocatedTableWriteIdHandler(rqst));
  }

  @Override
  public void seedWriteId(SeedTableWriteIdsRequest rqst) throws MetaException {
    //since this is on conversion from non-acid to acid, NEXT_WRITE_ID should not have an entry
    //for this table.  It also has a unique index in case 'should not' is violated

    // First allocation of write id should add the table to the next_write_id meta table
    // The initial value for write id should be 1 and hence we add 1 with number of write ids
    // allocated here
    jdbcResource.getJdbcTemplate().update(
        "INSERT INTO \"NEXT_WRITE_ID\" (\"NWI_DATABASE\", \"NWI_TABLE\", \"NWI_NEXT\") VALUES (:db, :table, :writeId)",
        new MapSqlParameterSource()
            .addValue("db", rqst.getDbName())
            .addValue("table", rqst.getTableName())
            .addValue("writeId", rqst.getSeedWriteId() + 1));
  }

  @Override
  public void seedTxnId(SeedTxnIdRequest rqst) throws MetaException {
    /*
     * Locking the txnLock an exclusive way, we do not want to set the txnId backward accidentally
     * if there are concurrent open transactions
     */
    acquireTxnLock(false);
    long highWaterMark = jdbcResource.execute(new GetHighWaterMarkHandler());
    if (highWaterMark >= rqst.getSeedTxnId()) {
      throw new MetaException(MessageFormat
          .format("Invalid txnId seed {}, the highWaterMark is {}", rqst.getSeedTxnId(), highWaterMark));
    }
    jdbcResource.getJdbcTemplate().getJdbcTemplate()
        .execute((Statement stmt) -> stmt.execute(dbProduct.getTxnSeedFn(rqst.getSeedTxnId())));
  }

  @Override
  public void addWriteNotificationLog(ListenerEvent acidWriteEvent) throws MetaException {
      MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners,
          acidWriteEvent instanceof AcidWriteEvent ? EventMessage.EventType.ACID_WRITE
              : EventMessage.EventType.BATCH_ACID_WRITE,
          acidWriteEvent, jdbcResource.getConnection(), sqlGenerator);
  }

  @Override
  public void performWriteSetGC() throws MetaException {
    long commitHighWaterMark = new MinOpenTxnIdWaterMarkFunction(openTxnTimeOutMillis).execute(jdbcResource);
    jdbcResource.getJdbcTemplate().update(
        "DELETE FROM \"WRITE_SET\" WHERE \"WS_COMMIT_ID\" < :hwm",
        new MapSqlParameterSource()
            .addValue("hwm", commitHighWaterMark));
  }

  @Override
  public void updateTransactionStatistics(UpdateTransactionalStatsRequest req) throws MetaException {
    jdbcResource.execute(
        "UPDATE \"MV_TABLES_USED\" " +
        "SET \"INSERTED_COUNT\"=\"INSERTED_COUNT\"+ :insertCount" +
        ",\"UPDATED_COUNT\"=\"UPDATED_COUNT\"+ :updateCount" +
        ",\"DELETED_COUNT\"=\"DELETED_COUNT\"+ :deleteCount" +
        " WHERE \"TBL_ID\"= :tableId",
        new MapSqlParameterSource()
            .addValue("insertCount", req.getInsertCount())
            .addValue("updateCount", req.getUpdatedCount())
            .addValue("deleteCount", req.getDeletedCount())
            .addValue("tableId", req.getTableId()), null);
  }

  /**
   * Get invalidation info for the materialization. Materialization information
   * contains information about whether there was update/delete/compaction operations on the source
   * tables used by the materialization since it was created.
   */
  @Override
  public Materialization getMaterializationInvalidationInfo(
          CreationMetadata creationMetadata, String validTxnListStr) throws MetaException {
    return new GetMaterializationInvalidationInfoFunction(creationMetadata, validTxnListStr).execute(jdbcResource);
  }

  @Override
  public LockResponse lockMaterializationRebuild(String dbName, String tableName, long txnId) throws MetaException {
    return new LockMaterializationRebuildFunction(dbName, tableName, txnId, mutexAPI).execute(jdbcResource);
  }

  @Override
  public boolean heartbeatLockMaterializationRebuild(String dbName, String tableName, long txnId) throws MetaException {
    int result = jdbcResource.execute(
            "UPDATE \"MATERIALIZATION_REBUILD_LOCKS\"" +
                    " SET \"MRL_LAST_HEARTBEAT\" = :lastHeartbeat" +
                    " WHERE \"MRL_TXN_ID\" = :txnId" +
                    " AND \"MRL_DB_NAME\" = :dbName" +
                    " AND \"MRL_TBL_NAME\" = :tblName",
            new MapSqlParameterSource()
                    .addValue("lastHeartbeat", Instant.now().toEpochMilli())
                    .addValue("txnId", txnId)
                    .addValue("dbName", dbName)
                    .addValue("tblName", tableName),
        ParameterizedCommand.AT_LEAST_ONE_ROW);
    return result >= 1;
  }

  @Override
  public long cleanupMaterializationRebuildLocks(ValidTxnList validTxnList, long timeout) throws MetaException {
    return new ReleaseMaterializationRebuildLocks(validTxnList, timeout).execute(jdbcResource);
  }

  /**
   * As much as possible (i.e. in absence of retries) we want both operations to be done on the same
   * connection (but separate transactions).
   * Retry-by-caller note: If the call to lock is from a transaction, then in the worst case
   * there will be a duplicate set of locks but both sets will belong to the same txn so they
   * will not conflict with each other.  For locks w/o txn context (i.e. read-only query), this
   * may lead to deadlock (at least a long wait).  (e.g. 1st call creates locks in {@code LOCK_WAITING}
   * mode and response gets lost.  Then {@link org.apache.hadoop.hive.metastore.RetryingMetaStoreClient}
   * retries, and enqueues another set of locks in LOCK_WAITING.  The 2nd LockResponse is delivered
   * to the DbLockManager, which will keep dong {@link #checkLock(CheckLockRequest)} until the 1st
   * set of locks times out.
   */
  @Override
  public LockResponse lock(LockRequest rqst) throws NoSuchTxnException, TxnAbortedException, MetaException {
    long lockId = txnLockManager.enqueueLock(rqst);
    try {
      return txnLockManager.checkLock(lockId, rqst.getTxnid(), rqst.isZeroWaitReadEnabled(), rqst.isExclusiveCTAS());
    } catch (NoSuchLockException e) {
      // This should never happen, as we just added the lock id
      throw new MetaException("Couldn't find a lock we just created! " + e.getMessage());      
    }
  }
  
  /**
   * Why doesn't this get a txnid as parameter?  The caller should either know the txnid or know there isn't one.
   * Either way getTxnIdFromLockId() will not be needed.  This would be a Thrift change.
   * <p>
   * Also, when lock acquisition returns WAITING, it's retried every 15 seconds (best case, see DbLockManager.backoff(),
   * in practice more often)
   * which means this is heartbeating way more often than hive.txn.timeout and creating extra load on DB.
   * <p>
   * The clients that operate in blocking mode, can't heartbeat a lock until the lock is acquired.
   * We should make CheckLockRequest include timestamp or last request to skip unnecessary heartbeats. Thrift change.
   * <p>
   * {@link #checkLock(CheckLockRequest)}  must run at SERIALIZABLE
   * (make sure some lock we are checking against doesn't move from W to A in another txn)
   * but this method can heartbeat in separate txn at READ_COMMITTED.
   * <p>
   * Retry-by-caller note:
   * Retryable because {@link #checkLock(CheckLockRequest)} is
   */
  @Override
  public LockResponse checkLock(CheckLockRequest rqst)
      throws NoSuchTxnException, NoSuchLockException, TxnAbortedException, MetaException {
    long extLockId = rqst.getLockid();
    // Heartbeat on the lockid first, to assure that our lock is still valid.
    // Then look up the lock info (hopefully in the cache).  If these locks
    // are associated with a transaction then heartbeat on that as well.
    List<LockInfo> lockInfos = jdbcResource.execute(new GetLocksByLockId(extLockId, 1, sqlGenerator));
    if (CollectionUtils.isEmpty(lockInfos)) {
      throw new NoSuchLockException("No such lock " + JavaUtils.lockIdToString(extLockId));
    }
    LockInfo lockInfo = lockInfos.get(0);
    if (lockInfo.getTxnId() > 0) {
      new HeartbeatTxnFunction(lockInfo.getTxnId()).execute(jdbcResource);
    } else {
      new HeartbeatLockFunction(rqst.getLockid()).execute(jdbcResource);
    }
    return txnLockManager.checkLock(extLockId, lockInfo.getTxnId(), false, false);
  }

  /**
   * This would have been made simpler if all locks were associated with a txn.  Then only txn needs to
   * be heartbeated, committed, etc.  no need for client to track individual locks.
   * When removing locks not associated with txn this potentially conflicts with
   * heartbeat/performTimeout which are update/delete of HIVE_LOCKS thus will be locked as needed by db.
   * since this only removes from HIVE_LOCKS at worst some lock acquire is delayed
   */
  @Override
  public void unlock(UnlockRequest rqst) throws TxnOpenException, MetaException {
    txnLockManager.unlock(rqst);
  }

  @Override
  public ShowLocksResponse showLocks(ShowLocksRequest rqst) throws MetaException {
    return txnLockManager.showLocks(rqst);
  }

  /**
   * {@code ids} should only have txnid or lockid but not both, ideally.
   * Currently DBTxnManager.heartbeat() enforces this.
   */
  @Override
  public void heartbeat(HeartbeatRequest ids)
      throws NoSuchTxnException,  NoSuchLockException, TxnAbortedException, MetaException {
    new HeartbeatTxnFunction(ids.getTxnid()).execute(jdbcResource);
    new HeartbeatLockFunction(ids.getLockid()).execute(jdbcResource);
  }
  
  @Override
  public HeartbeatTxnRangeResponse heartbeatTxnRange(HeartbeatTxnRangeRequest rqst) throws MetaException {
    return new HeartbeatTxnRangeFunction(rqst).execute(jdbcResource);
  }

  @Override
  public long getTxnIdForWriteId(String dbName, String tblName, long writeId) throws MetaException {
    return jdbcResource.execute(new TxnIdForWriteIdHandler(writeId, dbName, tblName));
  }

  @Override
  public CompactionResponse compact(CompactionRequest rqst) throws MetaException {
    return new CompactFunction(rqst, openTxnTimeOutMillis, getMutexAPI()).execute(jdbcResource);
  }

  @Override
  public boolean submitForCleanup(CompactionRequest rqst, long highestWriteId, long txnId) throws MetaException {
    // Put a compaction request in the queue.
    long id = new GenerateCompactionQueueIdFunction().execute(jdbcResource);
    jdbcResource.execute(new InsertCompactionRequestCommand(id, CompactionState.READY_FOR_CLEANING, rqst).withTxnDetails(highestWriteId, txnId));
    return true;
  }

  @Override
  public ShowCompactResponse showCompact(ShowCompactRequest rqst) throws MetaException {
    return jdbcResource.execute(new ShowCompactHandler(rqst, sqlGenerator));
  }

  /**
   * We assume this is only called by metadata cache server to know if there are new base/delta files should be read.
   * The query filters compactions by state and only returns SUCCEEDED or READY_FOR_CLEANING compactions because
   * only these two states means there are new files ready to be read.
   */
  @Override
  public GetLatestCommittedCompactionInfoResponse getLatestCommittedCompactionInfo(
      GetLatestCommittedCompactionInfoRequest rqst) throws MetaException {
    return jdbcResource.execute(new GetLatestCommittedCompactionInfoHandler(rqst));
  }

  @Override
  public MetricsInfo getMetricsInfo() throws MetaException {
    int threshold = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_TABLES_WITH_ABORTED_TXNS_THRESHOLD);
    MetricsInfo metrics = jdbcResource.execute(MetricsInfoHandler.INSTANCE);
    Set<String> resourceNames = jdbcResource.execute(new TablesWithAbortedTxnsHandler(threshold));
    metrics.setTablesWithXAbortedTxnsCount(resourceNames.size());
    metrics.setTablesWithXAbortedTxns(resourceNames);    
    return metrics;
  }

  /**
   * Retry-by-caller note:
   * This may be retried after dbConn.commit.  At worst, it will create duplicate entries in
   * TXN_COMPONENTS which won't affect anything.  See more comments in {@link #commitTxn(CommitTxnRequest)}
   */
  @Override
  public void addDynamicPartitions(AddDynamicPartitions rqst) throws NoSuchTxnException,  TxnAbortedException, MetaException {
    TxnType txnType = jdbcResource.execute(new GetOpenTxnTypeAndLockHandler(sqlGenerator, rqst.getTxnid()));
    if (txnType == null) {
      //ensures txn is still there and in expected state
      new EnsureValidTxnFunction(rqst.getTxnid()).execute(jdbcResource);
      shouldNeverHappen(rqst.getTxnid());
    }
    jdbcResource.execute(new InsertTxnComponentsCommand(rqst));
    jdbcResource.getJdbcTemplate().update("DELETE FROM \"TXN_COMPONENTS\" " +
            "WHERE \"TC_TXNID\" = :txnId AND \"TC_DATABASE\" = :dbName AND \"TC_TABLE\" = :tableName AND \"TC_PARTITION\" IS NULL",
        new MapSqlParameterSource()
            .addValue("txnId", rqst.getTxnid())
            .addValue("dbName", org.apache.commons.lang3.StringUtils.lowerCase(rqst.getDbname()))
            .addValue("tableName", org.apache.commons.lang3.StringUtils.lowerCase(rqst.getTablename())));
  }

  /**
   * Clean up corresponding records in metastore tables when corresponding object is dropped,
   * specifically: TXN_COMPONENTS, COMPLETED_TXN_COMPONENTS, COMPACTION_QUEUE, COMPLETED_COMPACTIONS
   * Retry-by-caller note: this is only idempotent assuming it's only called by dropTable/Db/etc
   * operations.
   * <p>
   * HIVE_LOCKS and WS_SET are cleaned up by {@link AcidHouseKeeperService}, if turned on
   */
  @Override
  public void cleanupRecords(HiveObjectType type, Database db, Table table,
        Iterator<Partition> partitionIterator, boolean keepTxnToWriteIdMetaData) throws MetaException {
    new CleanupRecordsFunction(type, db, table, partitionIterator, getDefaultCatalog(conf), keepTxnToWriteIdMetaData, null)
        .execute(jdbcResource);
  }

  @Override
  public void cleanupRecords(HiveObjectType type, Database db, Table table,
        Iterator<Partition> partitionIterator, long txnId) throws MetaException {
    new CleanupRecordsFunction(type, db, table, partitionIterator, getDefaultCatalog(conf), false, txnId)
        .execute(jdbcResource);
  }
  
  /**
   * Catalog hasn't been added to transactional tables yet, so it's passed in but not used.
   */
  @Override
  public void onRename(String oldCatName, String oldDbName, String oldTabName, String oldPartName,
      String newCatName, String newDbName, String newTabName, String newPartName)
      throws MetaException {
    new OnRenameFunction(oldCatName, oldDbName, oldTabName, oldPartName, newCatName, newDbName, newTabName, newPartName).execute(jdbcResource);
  }
  
  /**
   * TODO: remove in future, for testing only, do not use.
   */
  @VisibleForTesting
  @Override
  public int getNumLocks() {
    return Objects.requireNonNull(
        jdbcResource.getJdbcTemplate().queryForObject("SELECT COUNT(*) FROM \"HIVE_LOCKS\"", new MapSqlParameterSource(), Integer.TYPE),
        "This never should be null, it's just to suppress warnings");
  }

  /**
   * TODO: remove in future, for testing only, do not use.
   */
  @VisibleForTesting
  @Override
  public long setTimeout(long milliseconds) {
    long previous_timeout = timeout;
    timeout = milliseconds;
    return previous_timeout;
  }

  protected Connection getDbConn(int isolationLevel, DataSource connPool) throws SQLException {
    Connection dbConn = null;
    try {
      dbConn = connPool.getConnection();
      dbConn.setAutoCommit(false);
      dbConn.setTransactionIsolation(isolationLevel);
      return dbConn;
    } catch (SQLException e) {
      if (dbConn != null) {
        dbConn.close();
      }
      throw e;
    }
  }

  /**
   * Isolation Level Notes
   * Plain: RC is OK
   * This will find transactions that have timed out and abort them.
   * Will also delete locks which are not associated with a transaction and have timed out
   * Tries to keep transactions (against metastore db) small to reduce lock contention.
   */
  @Override
  public void performTimeOuts() {
    new PerformTimeoutsFunction(timeout, replicationTxnTimeout, transactionalListeners).execute(jdbcResource);
  }

  @Override
  public void countOpenTxns() throws MetaException {
    int openTxns = jdbcResource.execute(new CountOpenTxnsHandler());
    if (openTxns > -1) {
      numOpenTxns.set(openTxns);
    }
  }

  @Override
  public void addWriteIdsToMinHistory(long txnid, Map<String, Long> minOpenWriteIds) throws MetaException {
    jdbcResource.execute(new AddWriteIdsToMinHistoryCommand(txnid, minOpenWriteIds));
  }

  protected synchronized static DataSource setupJdbcConnectionPool(Configuration conf, int maxPoolSize) {
    DataSourceProvider dsp = DataSourceProviderFactory.tryGetDataSourceProviderOrNull(conf);
    if (dsp != null) {
      try {
        return dsp.create(conf, maxPoolSize);
      } catch (SQLException e) {
        LOG.error("Unable to instantiate JDBC connection pooling", e);
        throw new RuntimeException(e);
      }
    } else {
      String connectionPooler = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CONNECTION_POOLING_TYPE).toLowerCase();
      if ("none".equals(connectionPooler)) {
        LOG.info("Choosing not to pool JDBC connections");
        return new NoPoolConnectionPool(conf, dbProduct);
      } else {
        throw new RuntimeException("Unknown JDBC connection pooling " + connectionPooler);
      }
    }
  }

  @Override
  public MutexAPI getMutexAPI() {
    return mutexAPI;
  }

  @Override
  public LockHandle acquireLock(String key) throws MetaException {
    return mutexAPI.acquireLock(key);
  }

  @Override
  public void acquireLock(String key, LockHandle handle) throws MetaException {
    mutexAPI.acquireLock(key, handle);
  }

  @Override
  public AbortCompactResponse abortCompactions(AbortCompactionRequest reqst) throws MetaException, NoSuchCompactionException {
    if (reqst.getCompactionIds().isEmpty()) {
      LOG.info("Compaction ids are missing in request. No compactions to abort");
      throw new NoSuchCompactionException("Compaction ids missing in request. No compactions to abort");
    }
    return new AbortCompactionFunction(reqst, sqlRetryHandler).execute(jdbcResource);    
  }

  private static void shouldNeverHappen(long txnid) {
    throw new RuntimeException("This should never happen: " + JavaUtils.txnIdToString(txnid));
  }  
  
  private void deleteInvalidOpenTransactions(List<Long> txnIds) throws MetaException {
    try {
      sqlRetryHandler.executeWithRetry(new SqlRetryCallProperties().withCallerId("deleteInvalidOpenTransactions"),
          () -> {
            jdbcResource.execute(new DeleteInvalidOpenTxnsCommand(txnIds));
            LOG.info("Removed transactions: ({}) from TXNS", txnIds);
            jdbcResource.execute(new RemoveTxnsFromMinHistoryLevelCommand(txnIds));
            return null;
          });
    } catch (TException e) {
      throw new MetaException(e.getMessage());
    }    
  }

  /**
   * Acquire the global txn lock, used to mutex the openTxn and commitTxn.
   * @param shared either SHARED_READ or EXCLUSIVE
   */
  private void acquireTxnLock(boolean shared) throws MetaException {
    String sqlStmt = sqlGenerator.createTxnLockStatement(shared);
    jdbcResource.getJdbcTemplate().getJdbcTemplate().execute((Statement stmt) -> {
      stmt.execute(sqlStmt);
      return null;
    });
    LOG.debug("TXN lock locked by '{}' in mode {}", TxnHandler.hostname, shared);
  }

  /**
   * Determine the current time, using the RDBMS as a source of truth
   * @return current time in milliseconds
   * @throws org.apache.hadoop.hive.metastore.api.MetaException if the time cannot be determined
   */
  protected Timestamp getDbTime() throws MetaException {
    return jdbcResource.getJdbcTemplate().queryForObject(
        dbProduct.getDBTime(),
        new MapSqlParameterSource(),
        (ResultSet rs, int rowNum) -> rs.getTimestamp(1));
  }

  private void determineDatabaseProduct(Connection conn) {
    try {
      String s = conn.getMetaData().getDatabaseProductName();
      dbProduct = DatabaseProduct.determineDatabaseProduct(s, conf);
      if (dbProduct.isUNDEFINED()) {
        String msg = "Unrecognized database product name <" + s + ">";
        LOG.error(msg);
        throw new IllegalStateException(msg);
      }
    } catch (SQLException e) {
      String msg = "Unable to get database product name";
      LOG.error(msg, e);
      throw new IllegalStateException(msg, e);
    }
  }
  
  private void initJdbcResource() {
    if (jdbcResource == null) {
      jdbcResource = new MultiDataSourceJdbcResource(dbProduct, conf, sqlGenerator);
      jdbcResource.registerDataSource(POOL_TX, connPool);
      jdbcResource.registerDataSource(POOL_MUTEX, connPoolMutex);
      jdbcResource.registerDataSource(POOL_COMPACTOR, connPoolCompactor);
    }
  }

  /**
   * Check if provided table is usable
   * @return
   */
  private boolean checkIfTableIsUsable(String tableName, boolean configValue) {
    if (!configValue) {
      // don't check it if disabled
      return false;
    }
    jdbcResource.bindDataSource(POOL_TX);
    try {
      jdbcResource.getJdbcTemplate().query("SELECT 1 FROM \"" + tableName + "\"",
          new MapSqlParameterSource(), ResultSet::next);
    } catch (DataAccessException e) {
      LOG.debug("Catching sql exception in " + tableName + " check", e);
      if (e.getCause() instanceof SQLException) {
        if (dbProduct.isTableNotExistsError(e)) {
          return false;
        } else {
          throw new RuntimeException(
              "Unable to select from transaction database: " + SqlRetryHandler.getMessage(e) + StringUtils.stringifyException(e));
        }
      }
    } finally {
      jdbcResource.unbindDataSource();
    }
    return true;
  }

  /**
   * Returns the databases updated by txnId.
   * Queries TXN_TO_WRITE_ID using txnId.
   *
   * @param txnId
   * @throws MetaException
   */
  private List<String> getTxnDbsUpdated(long txnId) throws MetaException {
    try {
      return sqlRetryHandler.executeWithRetry(
          new SqlRetryCallProperties().withCallerId("GetTxnDbsUpdatedHandler"),
          () -> jdbcResource.execute(new GetTxnDbsUpdatedHandler(txnId)));
    } catch (MetaException e) {
      throw e;
    } catch (TException e) {
      throw new MetaException(e.getMessage());
    }
  }

}
