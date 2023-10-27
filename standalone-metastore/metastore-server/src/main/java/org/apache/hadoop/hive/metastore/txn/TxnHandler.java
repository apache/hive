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
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.MaterializationSnapshot;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.classification.RetrySemantics;
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.TransactionalMetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.AbortCompactResponse;
import org.apache.hadoop.hive.metastore.api.AbortCompactionRequest;
import org.apache.hadoop.hive.metastore.api.AbortCompactionResponseElement;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionInfoStruct;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
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
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
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
import org.apache.hadoop.hive.metastore.api.ReplLastIdInfo;
import org.apache.hadoop.hive.metastore.api.ReplTblWriteIdStateRequest;
import org.apache.hadoop.hive.metastore.api.SeedTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.SeedTxnIdRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.api.UpdateTransactionalStatsRequest;
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.datasource.DataSourceProvider;
import org.apache.hadoop.hive.metastore.datasource.DataSourceProviderFactory;
import org.apache.hadoop.hive.metastore.events.AbortTxnEvent;
import org.apache.hadoop.hive.metastore.events.AcidWriteEvent;
import org.apache.hadoop.hive.metastore.events.AllocWriteIdEvent;
import org.apache.hadoop.hive.metastore.events.CommitTxnEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import org.apache.hadoop.hive.metastore.txn.entities.LockInfo;
import org.apache.hadoop.hive.metastore.txn.impl.HiveMutex;
import org.apache.hadoop.hive.metastore.txn.impl.commands.AddWriteIdsToMinHistoryCommand;
import org.apache.hadoop.hive.metastore.txn.impl.commands.DeleteInvalidOpenTxnsCommand;
import org.apache.hadoop.hive.metastore.txn.impl.commands.InsertCompactionInfoCommand;
import org.apache.hadoop.hive.metastore.txn.impl.commands.InsertTxnComponentsCommand;
import org.apache.hadoop.hive.metastore.txn.impl.commands.RemoveTxnsFromMinHistoryLevelCommand;
import org.apache.hadoop.hive.metastore.txn.impl.commands.RemoveWriteIdsFromMinHistoryCommand;
import org.apache.hadoop.hive.metastore.txn.impl.functions.AbortTxnsFunction;
import org.apache.hadoop.hive.metastore.txn.impl.functions.CleanupRecordsFunction;
import org.apache.hadoop.hive.metastore.txn.impl.functions.CompactFunction;
import org.apache.hadoop.hive.metastore.txn.impl.functions.GetValidWriteIdsForTableFunction;
import org.apache.hadoop.hive.metastore.txn.impl.functions.GetValidWriteIdsFunction;
import org.apache.hadoop.hive.metastore.txn.impl.functions.HeartBeatLockFunction;
import org.apache.hadoop.hive.metastore.txn.impl.functions.HeartBeatTxnFunction;
import org.apache.hadoop.hive.metastore.txn.impl.functions.MinOpenTxnIdWaterMarkFunction;
import org.apache.hadoop.hive.metastore.txn.impl.functions.OnRenameFunction;
import org.apache.hadoop.hive.metastore.txn.impl.functions.OpenTxnsFunction;
import org.apache.hadoop.hive.metastore.txn.impl.functions.PerformTimeoutsFunction;
import org.apache.hadoop.hive.metastore.txn.impl.queries.CountOpenTxnsHandler;
import org.apache.hadoop.hive.metastore.txn.impl.queries.GetLocksByLockId;
import org.apache.hadoop.hive.metastore.txn.impl.queries.GetOpenTxnTypeAndLockHandler;
import org.apache.hadoop.hive.metastore.txn.impl.queries.GetOpenTxnsListHandler;
import org.apache.hadoop.hive.metastore.txn.impl.queries.LatestTxnIdInConflictHandler;
import org.apache.hadoop.hive.metastore.txn.impl.queries.MetricsInfoHandler;
import org.apache.hadoop.hive.metastore.txn.impl.queries.OpenTxnTimeoutLowBoundaryTxnIdHandler;
import org.apache.hadoop.hive.metastore.txn.impl.queries.ShowCompactHandler;
import org.apache.hadoop.hive.metastore.txn.impl.queries.TablesWithAbortedTxnsHandler;
import org.apache.hadoop.hive.metastore.txn.impl.queries.TargetTxnIdListHandler;
import org.apache.hadoop.hive.metastore.txn.impl.queries.TxnIdForWriteIdHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.ParameterizedCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionContext;
import org.apache.hadoop.hive.metastore.txn.retryhandling.SqlRetryCallProperties;
import org.apache.hadoop.hive.metastore.txn.retryhandling.SqlRetryFunction;
import org.apache.hadoop.hive.metastore.txn.retryhandling.SqlRetryHandler;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.StringableMap;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.lang3.StringUtils.repeat;
import static org.apache.hadoop.hive.metastore.txn.TxnUtils.executeQueriesInBatchNoCount;
import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;
import static org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRED;

/**
 * A handler to answer transaction related calls that come into the metastore
 * server.
 *
 * Note on log messages:  Please include txnid:X and lockid info using
 * {@link JavaUtils#txnIdToString(long)}
 * and {@link JavaUtils#lockIdToString(long)} in all messages.
 * The txnid:X and lockid:Y matches how Thrift object toString() methods are generated,
 * so keeping the format consistent makes grep'ing the logs much easier.
 *
 * Note on HIVE_LOCKS.hl_last_heartbeat.
 * For locks that are part of transaction, we set this 0 (would rather set it to NULL but
 * Currently the DB schema has this NOT NULL) and only update/read heartbeat from corresponding
 * transaction in TXNS.
 *
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
 *
 * The exception to his is Derby which doesn't support proper S4U.  Derby is always running embedded
 * (this is the only supported configuration for Derby)
 * in the same JVM as HiveMetaStoreHandler thus we use JVM wide lock to properly sequnce the operations.
 *
 * {@link #derbyLock}

 * If we ever decide to run remote Derby server, according to
 * https://db.apache.org/derby/docs/10.0/manuals/develop/develop78.html all transactions will be
 * seriazlied, so that would also work though has not been tested.
 *
 * General design note:
 * It's imperative that any operation on a txn (e.g. commit), ensure (atomically) that this txn is
 * still valid and active.  In the code this is usually achieved at the same time the txn record
 * is locked for some operation.
 *
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
@InterfaceAudience.Private
@InterfaceStability.Evolving
abstract class TxnHandler implements TxnStore, TxnStore.MutexAPI {
  
  private static final String TXN_TMP_STATE = "_";

  private static final int ALLOWED_REPEATED_DEADLOCKS = 10;
  private static final Logger LOG = LoggerFactory.getLogger(TxnHandler.class.getName());

  private static DataSource connPool;
  private static DataSource connPoolMutex;
  private static DataSource connPoolCompaction;

  private static final String MANUAL_RETRY = "ManualRetry";

  // Query definitions
  private static final String TXN_COMPONENTS_DP_DELETE_QUERY = "DELETE FROM \"TXN_COMPONENTS\" " +
      "WHERE \"TC_TXNID\" = ? AND \"TC_DATABASE\" = ? AND \"TC_TABLE\" = ? AND \"TC_PARTITION\" IS NULL";
  private static final String COMPL_TXN_COMPONENTS_INSERT_QUERY = "INSERT INTO \"COMPLETED_TXN_COMPONENTS\" " +
      "(\"CTC_TXNID\"," + " \"CTC_DATABASE\", \"CTC_TABLE\", \"CTC_PARTITION\", \"CTC_WRITEID\", \"CTC_UPDATE_DELETE\")" +
      " VALUES (%s, ?, ?, ?, ?, %s)";
  private static final String TXNS_INSERT_QRY = "INSERT INTO \"TXNS\" " +
      "(\"TXN_STATE\", \"TXN_STARTED\", \"TXN_LAST_HEARTBEAT\", \"TXN_USER\", \"TXN_HOST\", \"TXN_TYPE\") " +
      "VALUES(?,%s,%s,?,?,?)";
  private static final String TXN_TO_WRITE_ID_INSERT_QUERY = "INSERT INTO \"TXN_TO_WRITE_ID\" (\"T2W_TXNID\", " +
      "\"T2W_DATABASE\", \"T2W_TABLE\", \"T2W_WRITEID\") VALUES (?, ?, ?, ?)";
  private static final String SELECT_NWI_NEXT_FROM_NEXT_WRITE_ID =
      "SELECT \"NWI_NEXT\" FROM \"NEXT_WRITE_ID\" WHERE \"NWI_DATABASE\" = ? AND \"NWI_TABLE\" = ?";
  

  protected List<TransactionalMetaStoreEventListener> transactionalListeners;

  // Maximum number of open transactions that's allowed
  private static volatile int maxOpenTxns = 0;
  // Whether number of open transactions reaches the threshold
  private static volatile boolean tooManyOpenTxns = false;
  // Current number of open txns
  private static AtomicInteger numOpenTxns;

  /**
   * Number of consecutive deadlocks we have seen
   */
  private int deadlockCnt;
  private long deadlockRetryInterval;
  protected Configuration conf;
  protected static DatabaseProduct dbProduct;
  protected static SQLGenerator sqlGenerator;
  protected static long openTxnTimeOutMillis;

  // (End user) Transaction timeout, in milliseconds.
  private long timeout;
  private long replicationTxnTimeout;

  private int maxBatchSize;
  private String identifierQuoteString; // quotes to use for quoting tables, where necessary
  private long retryInterval;
  private int retryLimit;
  private int retryNum;

  private MutexAPI mutexAPI;
  private static TxnLockHandler txnLockHandler;
  private static SqlRetryHandler sqlRetryHandler;
  protected static MultiDataSourceJdbcResource jdbcResource;

  /**
   * Derby specific concurrency control
   */
  private static final ReentrantLock derbyLock = new ReentrantLock(true);
  /**
   * must be static since even in UT there may be > 1 instance of TxnHandler
   * (e.g. via Compactor services)
   */
  private final static ConcurrentHashMap<String, Semaphore> derbyKey2Lock = new ConcurrentHashMap<>();
  private static final String hostname = JavaUtils.hostname();

  // Private methods should never catch SQLException and then throw MetaException.  The public
  // methods depend on SQLException coming back so they can detect and handle deadlocks.  Private
  // methods should only throw MetaException when they explicitly know there's a logic error and
  // they want to throw past the public methods.
  //
  // All public methods that write to the database have to check for deadlocks when a SQLException
  // comes back and handle it if they see one.  This has to be done with the connection pooling
  // in mind.  To do this they should call checkRetryable() AFTER rolling back the db transaction,
  // and then they should catch RetryException and call themselves recursively. See commitTxn for an example.

  public TxnHandler() {
  }

  /**
   * This is logically part of c'tor and must be called prior to any other method.
   * Not physically part of c'tor due to use of reflection
   */
  public void setConf(Configuration conf){
    this.conf = conf;

    int maxPoolSize = MetastoreConf.getIntVar(conf, ConfVars.CONNECTION_POOLING_MAX_CONNECTIONS);
    synchronized (TxnHandler.class) {
      try (DataSourceProvider.DataSourceNameConfigurator configurator =
               new DataSourceProvider.DataSourceNameConfigurator(conf, POOL_TX)) {
        if (connPool == null) {
          connPool = setupJdbcConnectionPool(conf, maxPoolSize);
        }
        if (connPoolMutex == null) {
          configurator.resetName(POOL_MUTEX);
          connPoolMutex = setupJdbcConnectionPool(conf, maxPoolSize);
        }
      }
      if (dbProduct == null) {
        try (Connection dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED)) {
          determineDatabaseProduct(dbConn);
        } catch (SQLException e) {
          LOG.error("Unable to determine database product", e);
          throw new RuntimeException(e);
        }
      }

      if (sqlGenerator == null) {
        sqlGenerator = new SQLGenerator(dbProduct, conf);
      }
      
      if (jdbcResource == null) {
        jdbcResource = new MultiDataSourceJdbcResource(dbProduct);
        jdbcResource.registerDataSource(POOL_TX, connPool);
        jdbcResource.registerDataSource(POOL_MUTEX, connPoolMutex);
      }      
    }
    mutexAPI = new HiveMutex(sqlGenerator, jdbcResource);

    numOpenTxns = Metrics.getOrCreateGauge(MetricsConstants.NUM_OPEN_TXNS);

    timeout = MetastoreConf.getTimeVar(conf, ConfVars.TXN_TIMEOUT, TimeUnit.MILLISECONDS);
    replicationTxnTimeout = MetastoreConf.getTimeVar(conf, ConfVars.REPL_TXN_TIMEOUT, TimeUnit.MILLISECONDS);
    retryInterval = MetastoreConf.getTimeVar(conf, ConfVars.HMS_HANDLER_INTERVAL,
        TimeUnit.MILLISECONDS);
    retryLimit = MetastoreConf.getIntVar(conf, ConfVars.HMS_HANDLER_ATTEMPTS);
    deadlockRetryInterval = retryInterval / 10;
    maxOpenTxns = MetastoreConf.getIntVar(conf, ConfVars.MAX_OPEN_TXNS);
    maxBatchSize = MetastoreConf.getIntVar(conf, ConfVars.JDBC_MAX_BATCH_SIZE);

    openTxnTimeOutMillis = MetastoreConf.getTimeVar(conf, ConfVars.TXN_OPENTXN_TIMEOUT, TimeUnit.MILLISECONDS);
    
    try {
      TxnHandlingFeatures.setUseMinHistoryWriteId(checkIfTableIsUsable("MIN_HISTORY_WRITE_ID", 
              MetastoreConf.getBoolVar(conf, ConfVars.TXN_USE_MIN_HISTORY_WRITE_ID))
      );      
      // override the config if table does not exists anymore
      // this helps to roll out his feature when multiple HMS is accessing the same backend DB
      TxnHandlingFeatures.setUseMinHistoryLevel(checkIfTableIsUsable("MIN_HISTORY_LEVEL",
          MetastoreConf.getBoolVar(conf, ConfVars.TXN_USE_MIN_HISTORY_LEVEL)));
    } catch (MetaException e) {
      String msg = "Error during TxnHandler startup, " + e.getMessage();
      LOG.error(msg);
      throw new RuntimeException(e);
    }

    try {
      transactionalListeners = MetaStoreServerUtils.getMetaStoreListeners(
              TransactionalMetaStoreEventListener.class,
                      conf, MetastoreConf.getVar(conf, ConfVars.TRANSACTIONAL_EVENT_LISTENERS));
    } catch(MetaException e) {
      String msg = "Unable to get transaction listeners, " + e.getMessage();
      LOG.error(msg);
      throw new RuntimeException(e);
    }

    sqlRetryHandler = new SqlRetryHandler(conf, jdbcResource.getDatabaseProduct());
    txnLockHandler = TransactionalRetryProxy.getProxy(new TxnLockHandlerImpl(jdbcResource, sqlGenerator, conf), sqlRetryHandler, jdbcResource);
  }

  /**
   * Check if provided table is usable
   * @return
   * @throws MetaException
   */
  private boolean checkIfTableIsUsable(String tableName, boolean configValue) throws MetaException {
    if (!configValue) {
      // don't check it if disabled
      return false;
    }
    jdbcResource.bindDataSource(POOL_TX);
    try {
      int i = jdbcResource.getJdbcTemplate().query("SELECT 1 FROM \"" + tableName + "\"", new MapSqlParameterSource(),
          new ResultSetExtractor<Integer>() {
            @Override
            public Integer extractData(ResultSet rs) throws SQLException, DataAccessException {
              if (rs.next()) {
                return rs.getInt(1);
              }
              return -1;
            }
          }); 
    } catch (DataAccessException e) {
      LOG.debug("Catching sql exception in " + tableName + " check", e);
      if (e.getCause() instanceof SQLException)
      if (dbProduct.isTableNotExistsError((SQLException) e.getCause())) {
        return false;
      } else {
        throw new MetaException(
            "Unable to select from transaction database: " + SqlRetryHandler.getMessage(e) + StringUtils.stringifyException(e));
      }
    } finally {
      jdbcResource.unbindDataSource();
    }
    return true;
  }
  
  @Override
  @RetrySemantics.ReadOnly
  public SqlRetryHandler getRetryHandler() {
    return sqlRetryHandler;
  }

  @Override
  @RetrySemantics.ReadOnly
  public MultiDataSourceJdbcResource getJdbcResourceHolder() {
    return jdbcResource;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  @RetrySemantics.ReadOnly
  public GetOpenTxnsInfoResponse getOpenTxnsInfo() throws MetaException {
    return getOpenTxnsList(true).toOpenTxnsInfoResponse();
  }

  @Override
  @RetrySemantics.ReadOnly
  public GetOpenTxnsResponse getOpenTxns() throws MetaException {
    return getOpenTxnsList(false).toOpenTxnsResponse(Arrays.asList(TxnType.READ_ONLY));
  }

  @Override
  @RetrySemantics.ReadOnly
  public GetOpenTxnsResponse getOpenTxns(List<TxnType> excludeTxnTypes) throws MetaException {
    return getOpenTxnsList(false).toOpenTxnsResponse(excludeTxnTypes);
  }

  private OpenTxnList getOpenTxnsList(boolean infoFields) throws MetaException {
    // We need to figure out the HighWaterMark and the list of open transactions.
    /*
     * This method need guarantees from
     * {@link #openTxns(OpenTxnRequest)} and  {@link #commitTxn(CommitTxnRequest)}.
     * It will look at the TXNS table and find each transaction between the max(txn_id) as HighWaterMark
     * and the max(txn_id) before the TXN_OPENTXN_TIMEOUT period as LowWaterMark.
     * Every transaction that is not found between these will be considered as open, since it may appear later.
     * openTxns must ensure, that no new transaction will be opened with txn_id below LWM and
     * commitTxn must ensure, that no committed transaction will be removed before the time period expires.
     */
    return jdbcResource.execute(new GetOpenTxnsListHandler(infoFields, openTxnTimeOutMillis));
  }

  /**
   * Retry-by-caller note:
   * Worst case, it will leave an open txn which will timeout.
   */
  @Override
  @RetrySemantics.Idempotent
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
    TransactionContext context = jdbcResource.getTransactionManager().getTransaction(PROPAGATION_REQUIRED);
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

    List<Long> txnIds = new OpenTxnsFunction(rqst, conf, openTxnTimeOutMillis, sqlGenerator, transactionalListeners).execute(jdbcResource);

    LOG.debug("Going to commit");
    context.createSavepoint();
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

      if (txnIds.size() > 0) {
        try {
          sqlRetryHandler.executeWithRetry(new SqlRetryCallProperties().withCallerId("deleteInvalidOpenTransactions"),
              () -> {
                jdbcResource.execute(new DeleteInvalidOpenTxnsCommand(conf, txnIds), maxBatchSize);
                LOG.info("Removed transactions: ({}) from TXNS", txnIds);
                jdbcResource.execute(new RemoveTxnsFromMinHistoryLevelCommand(conf, txnIds), maxBatchSize);
                return null;
              });
        } catch (TException e) {
          throw new MetaException(e.getMessage());
        }
      }

      jdbcResource.getTransactionManager().commit(context);
      /*
       * We cannot throw SQLException directly, as it is not in the throws clause
       */
      throw new UncategorizedSQLException(null, null, new SQLException("OpenTxnTimeOut exceeded", MANUAL_RETRY));
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

  private long getHighWaterMark() throws MetaException {
    try {
      return jdbcResource.getJdbcTemplate().queryForObject("SELECT MAX(\"TXN_ID\") FROM \"TXNS\"",
          new MapSqlParameterSource(), Long.class);
    } catch (EmptyResultDataAccessException e) {
      throw new MetaException("Transaction tables not properly " + "initialized, null record found in MAX(TXN_ID)");
    }
  }

  private List<Long> getTargetTxnIdList(String replPolicy, List<Long> sourceTxnIdList) throws MetaException {
    return jdbcResource.execute(new TargetTxnIdListHandler(replPolicy, sourceTxnIdList));
  }

  @Override
  @RetrySemantics.Idempotent
  public long getTargetTxnId(String replPolicy, long sourceTxnId) throws MetaException {
    List<Long> targetTxnIds = getTargetTxnIdList(replPolicy, Collections.singletonList(sourceTxnId));
    if (targetTxnIds.isEmpty()) {
      LOG.info("Txn {} not present for repl policy {}", sourceTxnId, replPolicy);
      return -1;
    }
    assert (targetTxnIds.size() == 1);
    return targetTxnIds.get(0);
  }

  private void deleteReplTxnMapEntry(Connection dbConn, long sourceTxnId, String replPolicy) throws SQLException {
    String s = "DELETE FROM \"REPL_TXN_MAP\" WHERE \"RTM_SRC_TXN_ID\" = " + sourceTxnId + " AND \"RTM_REPL_POLICY\" = ?";
    try (PreparedStatement pst = sqlGenerator.prepareStmtWithParameters(dbConn, s, Arrays.asList(replPolicy))) {
      LOG.info("Going to execute  <" + s.replace("?", "{}") + ">", quoteString(replPolicy));
      pst.executeUpdate();
    }
  }

  @Override
  @RetrySemantics.Idempotent
  public void abortTxn(AbortTxnRequest rqst) throws NoSuchTxnException, MetaException, TxnAbortedException {
    long txnid = rqst.getTxnid();
    TxnErrorMsg txnErrorMsg = TxnErrorMsg.NONE;
    long sourceTxnId = -1;
    boolean isReplayedReplTxn = TxnType.REPL_CREATED.equals(rqst.getTxn_type());
    boolean isHiveReplTxn = rqst.isSetReplPolicy() && TxnType.DEFAULT.equals(rqst.getTxn_type());
    Connection dbConn = null;
    Statement stmt = null;
    try {
      dbConn = jdbcResource.getConnection();
      stmt = dbConn.createStatement();

      if (isReplayedReplTxn) {
        assert (rqst.isSetReplPolicy());
        sourceTxnId = rqst.getTxnid();
        List<Long> targetTxnIds = getTargetTxnIdList(rqst.getReplPolicy(), Collections.singletonList(sourceTxnId));
        if (targetTxnIds.isEmpty()) {
          // Idempotent case where txn was already closed or abort txn event received without
          // corresponding open txn event.
          LOG.info("Target txn id is missing for source txn id : {} and repl policy {}", sourceTxnId,
              rqst.getReplPolicy());
          return;
        }
        assert targetTxnIds.size() == 1;
        txnid = targetTxnIds.get(0);
      }

      TxnType txnType = jdbcResource.execute(new GetOpenTxnTypeAndLockHandler(sqlGenerator, txnid));
      if (txnType == null) {
        TxnStatus status = findTxnState(txnid, stmt);
        if (status == TxnStatus.ABORTED) {
          if (isReplayedReplTxn) {
            // in case of replication, idempotent is taken care by getTargetTxnId
            LOG.warn("Invalid state ABORTED for transactions started using replication replay task");
            deleteReplTxnMapEntry(dbConn, sourceTxnId, rqst.getReplPolicy());
          }
          LOG.info("abortTxn({}) requested by it is already {}", JavaUtils.txnIdToString(txnid), TxnStatus.ABORTED);
          return;
        }
        raiseTxnUnexpectedState(status, txnid);
      }

      if (isReplayedReplTxn) {
        txnErrorMsg = TxnErrorMsg.ABORT_REPLAYED_REPL_TXN;
      } else if (isHiveReplTxn) {
        txnErrorMsg = TxnErrorMsg.ABORT_DEFAULT_REPL_TXN;
      } else if (rqst.isSetErrorCode()) {
        txnErrorMsg = TxnErrorMsg.getTxnErrorMsg(rqst.getErrorCode());
      }

      abortTxns(Collections.singletonList(txnid), true, isReplayedReplTxn, txnErrorMsg);

      if (isReplayedReplTxn) {
        deleteReplTxnMapEntry(dbConn, sourceTxnId, rqst.getReplPolicy());
      }

      if (transactionalListeners != null && !isHiveReplTxn) {
        List<String> dbsUpdated = getTxnDbsUpdated(txnid, dbConn);
        MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners,
            EventMessage.EventType.ABORT_TXN,
            new AbortTxnEvent(txnid, txnType, null, dbsUpdated), dbConn, sqlGenerator);
      }
    } catch (SQLException e) {
      throw new UncategorizedSQLException(null, null, e);
    } finally {
      closeStmt(stmt);
    }
  }

  @Override
  @RetrySemantics.Idempotent
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
      int numAborted = abortTxns(txnIds, false, false, txnErrorMsg);
      if (numAborted != txnIds.size()) {
        LOG.warn(
            "Abort Transactions command only aborted {} out of {} transactions. It's possible that the other"
                + " {} transactions have been aborted or committed, or the transaction ids are invalid.",
            numAborted, txnIds.size(), (txnIds.size() - numAborted));
      }

      if (transactionalListeners != null) {
        for (Long txnId : txnIds) {
          List<String> dbsUpdated = getTxnDbsUpdated(txnId, dbConn);
          MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners,
              EventMessage.EventType.ABORT_TXN, new AbortTxnEvent(txnId,
                  nonReadOnlyTxns.getOrDefault(txnId, TxnType.READ_ONLY), null, dbsUpdated), dbConn, sqlGenerator);
        }
      }
    } catch (SQLException e) {
      throw new UncategorizedSQLException(null, null, e);
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

  /**
   * Concurrency/isolation notes:
   * This is mutexed with {@link #openTxns(OpenTxnRequest)} and other {@link #commitTxn(CommitTxnRequest)}
   * operations using select4update on NEXT_TXN_ID. Also, mutexes on TXNS table for specific txnid:X
   * see more notes below.
   * In order to prevent lost updates, we need to determine if any 2 transactions overlap.  Each txn
   * is viewed as an interval [M,N]. M is the txnid and N is taken from the same NEXT_TXN_ID sequence
   * so that we can compare commit time of txn T with start time of txn S.  This sequence can be thought of
   * as a logical time counter. If S.commitTime < T.startTime, T and S do NOT overlap.
   *
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
  @RetrySemantics.Idempotent("No-op if already committed")
  public void commitTxn(CommitTxnRequest rqst) throws NoSuchTxnException, TxnAbortedException, MetaException {
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
          List<Long> targetTxnIds = getTargetTxnIdList(rqst.getReplPolicy(), Collections.singletonList(sourceTxnId));
          if (targetTxnIds.isEmpty()) {
            // Idempotent case where txn was already closed or commit txn event received without
            // corresponding open txn event.
            LOG.info("Target txn id is missing for source txn id : {} and repl policy {}", sourceTxnId,
                rqst.getReplPolicy());
            jdbcResource.getTransactionManager().rollback(context);
            return;
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
          TxnStatus actualTxnStatus = findTxnState(txnid, stmt);
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
            return;
          }
          raiseTxnUnexpectedState(actualTxnStatus, txnid);
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
          deleteReplTxnMapEntry(dbConn, sourceTxnId, rqst.getReplPolicy());
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

  /**
   * Create Notifiaction Events on txn commit
   * @param txnid committed txn
   * @param txnType transaction type
   * @throws MetaException ex
   */
  protected void createCommitNotificationEvent(Connection conn, long txnid, TxnType txnType)
      throws MetaException, SQLException {
    if (transactionalListeners != null) {
      MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners,
          EventMessage.EventType.COMMIT_TXN, new CommitTxnEvent(txnid, txnType), conn, sqlGenerator);
    }
  }

  private boolean isUpdateOrDelete(Statement stmt, String conflictSQLSuffix) throws SQLException, MetaException {
    try (ResultSet rs = stmt.executeQuery(sqlGenerator.addLimitClause(1,
            "\"TC_OPERATION_TYPE\" " + conflictSQLSuffix))) {
      return rs.next();
    }
  }

  /**
   * Checks if there are transactions that are overlapping, i.e. conflicting with the current one.
   * Valid only if the caller txn holds an exclusive lock that prevents other txns to make changes to the same tables/partitions.
   * Only considers txn performing update/delete and intentionally ignores inserts.
   * @param txnid
   * @return max Id for the conflicting transaction, if any, otherwise -1
   * @throws MetaException
   */
  @RetrySemantics.ReadOnly
  public long getLatestTxnIdInConflict(long txnid) throws MetaException {
    return jdbcResource.execute(new LatestTxnIdInConflictHandler(txnid));
  }

  /**
   * Returns the databases updated by txnId.
   * Queries TXN_TO_WRITE_ID using txnId.
   *
   * @param txnId
   * @throws MetaException
   */
    private List<String> getTxnDbsUpdated(long txnId, Connection dbConn) throws MetaException {
    try {
      try (Statement stmt = dbConn.createStatement()) {

        String query = "SELECT DISTINCT \"T2W_DATABASE\" " +
                " FROM \"TXN_TO_WRITE_ID\" \"COMMITTED\"" +
                "   WHERE \"T2W_TXNID\" = " + txnId;

        LOG.debug("Going to execute query: <{}>", query);
        try (ResultSet rs = stmt.executeQuery(query)) {
          List<String> dbsUpdated = new ArrayList<String>();
          while (rs.next()) {
            dbsUpdated.add(rs.getString(1));
          }
          return dbsUpdated;
        }
      } catch (SQLException e) {
        checkRetryable(e, "getTxnDbsUpdated");
        throw new MetaException(StringUtils.stringifyException(e));
      }
    } catch (RetryException e) {
      return getTxnDbsUpdated(txnId, dbConn);
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
   * Replicate Table Write Ids state to mark aborted write ids and writeid high water mark.
   * @param rqst info on table/partitions and writeid snapshot to replicate.
   * @throws MetaException
   */
  @Override
  @RetrySemantics.Idempotent("No-op if already replicated the writeid state")
  public void replTableWriteIdState(ReplTblWriteIdStateRequest rqst) throws MetaException {
    String dbName = rqst.getDbName().toLowerCase();
    String tblName = rqst.getTableName().toLowerCase();
    ValidWriteIdList validWriteIdList = new ValidReaderWriteIdList(rqst.getValidWriteIdlist());

    NamedParameterJdbcTemplate npjdbcTemplate = jdbcResource.getJdbcTemplate();
    // Check if this txn state is already replicated for this given table. If yes, then it is
    // idempotent case and just return.
    String sql = "SELECT \"NWI_NEXT\" FROM \"NEXT_WRITE_ID\" WHERE \"NWI_DATABASE\" = :dbName AND \"NWI_TABLE\" = :tableName";
    boolean found = npjdbcTemplate.query(sql, new MapSqlParameterSource()
            .addValue("dbName", dbName)
            .addValue("tableName", tblName), rs -> {
          return rs.next();
        }
    );

    if (found) {
      LOG.info("Idempotent flow: WriteId state <{}> is already applied for the table: {}.{}", 
          validWriteIdList, dbName, tblName);
      return;
    }

    // Get the abortedWriteIds which are already sorted in ascending order.
    List<Long> abortedWriteIds = getAbortedWriteIds(validWriteIdList);
    int numAbortedWrites = abortedWriteIds.size();
    if (numAbortedWrites > 0) {
      // Allocate/Map one txn per aborted writeId and abort the txn to mark writeid as aborted.
      // We don't use the txnLock, all of these transactions will be aborted in this one rdbm transaction
      // So they will not effect the commitTxn in any way
      
      try {
        List<Long> txnIds = new OpenTxnsFunction(
            new OpenTxnRequest(numAbortedWrites, rqst.getUser(), rqst.getHostName()),
            conf, openTxnTimeOutMillis, sqlGenerator, transactionalListeners).execute(jdbcResource);
        assert (numAbortedWrites == txnIds.size());

        // Map each aborted write id with each allocated txn.
        List<SqlParameterSource> params = new ArrayList<>(txnIds.size());
        for (int i = 0; i < txnIds.size(); i++) {
          params.add(new MapSqlParameterSource()
              .addValue("txnId", txnIds.get(i))
              .addValue("dbName", dbName)
              .addValue("tableName", tblName)
              .addValue("writeId", abortedWriteIds.get(i)));
          LOG.info("Allocated writeID: {} for txnId: {}", abortedWriteIds.get(i), txnIds.get(i));
        }
        jdbcResource.getJdbcTemplate().getJdbcTemplate().batchUpdate(
            "INSERT INTO \"TXN_TO_WRITE_ID\" (\"T2W_TXNID\", \"T2W_DATABASE\", \"T2W_TABLE\", \"T2W_WRITEID\") VALUES (?, ?, ?, ?)", 
            params, maxBatchSize, (PreparedStatement ps, SqlParameterSource sps) -> {
            ps.setLong(1, (Long)sps.getValue("txnId"));
            ps.setString(2, sps.getValue("dbName").toString());
            ps.setString(3, sps.getValue("tableName").toString());
            ps.setLong(4, (Long)sps.getValue("writeId"));
          });

        // Abort all the allocated txns so that the mapped write ids are referred as aborted ones.
        int numAborts = abortTxns(txnIds, false, false, TxnErrorMsg.ABORT_REPL_WRITEID_TXN);
        assert (numAborts == numAbortedWrites);
      } catch (SQLException e) {
        throw new UncategorizedSQLException(null, null, e);
      }
    }

    // There are some txns in the list which has no write id allocated and hence go ahead and do it.
    // Get the next write id for the given table and update it with new next write id.
    // It is expected NEXT_WRITE_ID doesn't have entry for this table and hence directly insert it.
    long nextWriteId = validWriteIdList.getHighWatermark() + 1;

    // First allocation of write id (hwm+1) should add the table to the next_write_id meta table.
    npjdbcTemplate.update(
        "INSERT INTO \"NEXT_WRITE_ID\" (\"NWI_DATABASE\", \"NWI_TABLE\", \"NWI_NEXT\") VALUES (:dbName, :tableName, :nextWriteId)",
        new MapSqlParameterSource()
            .addValue("dbName", dbName)
            .addValue("tableName", tblName)
            .addValue("nextWriteId", nextWriteId));
    LOG.info("WriteId state <{}> is applied for the table: {}.{}", validWriteIdList, dbName, tblName);

    // Schedule Major compaction on all the partitions/table to clean aborted data
    if (numAbortedWrites > 0) {
      CompactionRequest compactRqst = new CompactionRequest(rqst.getDbName(), rqst.getTableName(),
          CompactionType.MAJOR);
      if (rqst.isSetPartNames()) {
        for (String partName : rqst.getPartNames()) {
          compactRqst.setPartitionname(partName);
          compact(compactRqst);
        }
      } else {
        compact(compactRqst);
      }
    }
  }

  private List<Long> getAbortedWriteIds(ValidWriteIdList validWriteIdList) {
    return Arrays.stream(validWriteIdList.getInvalidWriteIds())
        .filter(validWriteIdList::isWriteIdAborted)
        .boxed()
        .collect(Collectors.toList());
  }

  @Override
  @RetrySemantics.ReadOnly
  public GetValidWriteIdsResponse getValidWriteIds(GetValidWriteIdsRequest rqst) throws MetaException {
    return new GetValidWriteIdsFunction(rqst, openTxnTimeOutMillis).execute(jdbcResource);
  }
  
  @Override
  @RetrySemantics.Idempotent
  public AllocateTableWriteIdsResponse allocateTableWriteIds(AllocateTableWriteIdsRequest rqst)
          throws MetaException {
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
        lockInternal();
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
          txnIds = getTargetTxnIdList(rqst.getReplPolicy(), srcTxnIds);
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
          prefix.append("DELETE FROM \"TXN_TO_WRITE_ID\" WHERE")
                .append(" \"T2W_DATABASE\" = ? AND \"T2W_TABLE\" = ? AND ");
          TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix,
              txnIds, "\"T2W_TXNID\"", false, false);
          for (String query : queries) {
            pStmt = sqlGenerator.prepareStmtWithParameters(dbConn, query, params);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Going to execute delete <" + query.replace("?", "{}") + ">",
                  quoteString(dbName), quoteString(tblName));
            }
            int numRowsDeleted = pStmt.executeUpdate();
            LOG.info("Removed {} prior writeIds during reallocation", numRowsDeleted);
            closeStmt(pStmt);
          }
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

  @Override
  public MaxAllocatedTableWriteIdResponse getMaxAllocatedTableWrited(MaxAllocatedTableWriteIdRequest rqst) throws MetaException {
    String dbName = rqst.getDbName();
    String tableName = rqst.getTableName();
    try {
      Connection dbConn = null;
      PreparedStatement pStmt = null;
      ResultSet rs = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        pStmt = sqlGenerator.prepareStmtWithParameters(dbConn, SELECT_NWI_NEXT_FROM_NEXT_WRITE_ID,
            Arrays.asList(dbName, tableName));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Going to execute query <" + SELECT_NWI_NEXT_FROM_NEXT_WRITE_ID.replace("?", "{}") + ">",
              quoteString(dbName), quoteString(tableName));
        }
        rs = pStmt.executeQuery();
        // If there is no record, we never allocated anything
        long maxWriteId = 0l;
        if (rs.next()) {
          // The row contains the nextId not the previously allocated
          maxWriteId = rs.getLong(1) - 1;
        }
        return new MaxAllocatedTableWriteIdResponse(maxWriteId);
      } catch (SQLException e) {
        LOG.error(
            "Exception during reading the max allocated writeId for dbName={}, tableName={}. Will retry if possible.",
            dbName, tableName, e);
        checkRetryable(e, "getMaxAllocatedTableWrited(" + rqst + ")");
        throw new MetaException("Unable to update transaction database " + StringUtils.stringifyException(e));
      } finally {
        close(rs, pStmt, dbConn);
      }
    } catch (RetryException e) {
      return getMaxAllocatedTableWrited(rqst);
    }
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
    long highWaterMark = getHighWaterMark();
    if (highWaterMark >= rqst.getSeedTxnId()) {
      throw new MetaException(MessageFormat
          .format("Invalid txnId seed {}, the highWaterMark is {}", rqst.getSeedTxnId(), highWaterMark));
    }
    jdbcResource.getJdbcTemplate().getJdbcTemplate().execute((Statement stmt) -> {
      return stmt.execute(dbProduct.getTxnSeedFn(rqst.getSeedTxnId()));
    });
  }

  @Override
  @RetrySemantics.Idempotent
  public void addWriteNotificationLog(ListenerEvent acidWriteEvent) throws MetaException {
      MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners,
          acidWriteEvent instanceof AcidWriteEvent ? EventMessage.EventType.ACID_WRITE
              : EventMessage.EventType.BATCH_ACID_WRITE,
          acidWriteEvent, jdbcResource.getConnection(), sqlGenerator);
  }

  @Override
  @RetrySemantics.SafeToRetry
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
  @RetrySemantics.ReadOnly
  public Materialization getMaterializationInvalidationInfo(
          CreationMetadata creationMetadata, String validTxnListStr) throws MetaException {
    if (creationMetadata.getTablesUsed().isEmpty()) {
      // Bail out
      LOG.warn("Materialization creation metadata does not contain any table");
      return null;
    }

    // We are composing a query that returns a single row if an update happened after
    // the materialization was created. Otherwise, query returns 0 rows.

    // Parse validReaderWriteIdList from creation metadata
    MaterializationSnapshot mvSnapshot = MaterializationSnapshot.fromJson(creationMetadata.getValidTxnList());
    if (mvSnapshot.getTableSnapshots() != null && !mvSnapshot.getTableSnapshots().isEmpty()) {
      // Incremental rebuild of MVs on Iceberg sources is not supported.
      return null;
    }
    final ValidTxnWriteIdList validReaderWriteIdList = new ValidTxnWriteIdList(mvSnapshot.getValidTxnList());

    // Parse validTxnList
    final ValidReadTxnList currentValidTxnList = new ValidReadTxnList(validTxnListStr);
    // Get the valid write id list for the tables in current state
    final List<TableValidWriteIds> currentTblValidWriteIdsList = new ArrayList<>();
    for (String fullTableName : creationMetadata.getTablesUsed()) {
      currentTblValidWriteIdsList.add(new GetValidWriteIdsForTableFunction(currentValidTxnList, fullTableName).execute(jdbcResource));
    }
    final ValidTxnWriteIdList currentValidReaderWriteIdList = TxnCommonUtils.createValidTxnWriteIdList(
            currentValidTxnList.getHighWatermark(), currentTblValidWriteIdsList);

    List<String> params = new ArrayList<>();
    StringBuilder queryUpdateDelete = new StringBuilder();
    StringBuilder queryCompletedCompactions = new StringBuilder();
    StringBuilder queryCompactionQueue = new StringBuilder();
    // compose a query that select transactions containing an update...
    queryUpdateDelete.append("SELECT \"CTC_UPDATE_DELETE\" FROM \"COMPLETED_TXN_COMPONENTS\" WHERE \"CTC_UPDATE_DELETE\" ='Y' AND (");
    queryCompletedCompactions.append("SELECT 1 FROM \"COMPLETED_COMPACTIONS\" WHERE (");
    queryCompactionQueue.append("SELECT 1 FROM \"COMPACTION_QUEUE\" WHERE (");
    int i = 0;
    for (String fullyQualifiedName : creationMetadata.getTablesUsed()) {
      ValidWriteIdList tblValidWriteIdList =
              validReaderWriteIdList.getTableValidWriteIdList(fullyQualifiedName);
      if (tblValidWriteIdList == null) {
        LOG.warn("ValidWriteIdList for table {} not present in creation metadata, this should not happen", fullyQualifiedName);
        return null;
      }

      // First, we check whether the low watermark has moved for any of the tables.
      // If it has, we return true, since it is not incrementally refreshable, e.g.,
      // one of the commits that are not available may be an update/delete.
      ValidWriteIdList currentTblValidWriteIdList =
              currentValidReaderWriteIdList.getTableValidWriteIdList(fullyQualifiedName);
      if (currentTblValidWriteIdList == null) {
        LOG.warn("Current ValidWriteIdList for table {} not present in creation metadata, this should not happen", fullyQualifiedName);
        return null;
      }
      if (!Objects.equals(currentTblValidWriteIdList.getMinOpenWriteId(), tblValidWriteIdList.getMinOpenWriteId())) {
        LOG.debug("Minimum open write id do not match for table {}", fullyQualifiedName);
        return null;
      }

      // ...for each of the tables that are part of the materialized view,
      // where the transaction had to be committed after the materialization was created...
      if (i != 0) {
        queryUpdateDelete.append("OR");
        queryCompletedCompactions.append("OR");
        queryCompactionQueue.append("OR");
      }
      String[] names = TxnUtils.getDbTableName(fullyQualifiedName);
      assert (names.length == 2);
      queryUpdateDelete.append(" (\"CTC_DATABASE\"=? AND \"CTC_TABLE\"=?");
      queryCompletedCompactions.append(" (\"CC_DATABASE\"=? AND \"CC_TABLE\"=?");
      queryCompactionQueue.append(" (\"CQ_DATABASE\"=? AND \"CQ_TABLE\"=?");
      params.add(names[0]);
      params.add(names[1]);
      queryUpdateDelete.append(" AND (\"CTC_WRITEID\" > " + tblValidWriteIdList.getHighWatermark());
      queryCompletedCompactions.append(" AND (\"CC_HIGHEST_WRITE_ID\" > " + tblValidWriteIdList.getHighWatermark());
      queryUpdateDelete.append(tblValidWriteIdList.getInvalidWriteIds().length == 0 ? ") " :
              " OR \"CTC_WRITEID\" IN(" + StringUtils.join(",",
                      Arrays.asList(ArrayUtils.toObject(tblValidWriteIdList.getInvalidWriteIds()))) + ") ) ");
      queryCompletedCompactions.append(tblValidWriteIdList.getInvalidWriteIds().length == 0 ? ") " :
              " OR \"CC_HIGHEST_WRITE_ID\" IN(" + StringUtils.join(",",
                      Arrays.asList(ArrayUtils.toObject(tblValidWriteIdList.getInvalidWriteIds()))) + ") ) ");
      queryUpdateDelete.append(") ");
      queryCompletedCompactions.append(") ");
      queryCompactionQueue.append(") ");
      i++;
    }
    // ... and where the transaction has already been committed as per snapshot taken
    // when we are running current query
    queryUpdateDelete.append(") AND \"CTC_TXNID\" <= " + currentValidTxnList.getHighWatermark());
    queryUpdateDelete.append(currentValidTxnList.getInvalidTransactions().length == 0 ? " " :
            " AND \"CTC_TXNID\" NOT IN(" + StringUtils.join(",",
                    Arrays.asList(ArrayUtils.toObject(currentValidTxnList.getInvalidTransactions()))) + ") ");
    queryCompletedCompactions.append(")");
    queryCompactionQueue.append(") ");

    boolean hasUpdateDelete = executeBoolean(queryUpdateDelete.toString(), params,
            "Unable to retrieve materialization invalidation information: completed transaction components.");

    // Execute query
    queryCompletedCompactions.append(" UNION ");
    queryCompletedCompactions.append(queryCompactionQueue.toString());
    List<String> paramsTwice = new ArrayList<>(params);
    paramsTwice.addAll(params);
    boolean hasCompaction = executeBoolean(queryCompletedCompactions.toString(), paramsTwice,
            "Unable to retrieve materialization invalidation information: compactions");

    return new Materialization(hasUpdateDelete, hasCompaction);
  }

  private boolean executeBoolean(String queryText, List<String> params, String errorMessage) throws MetaException {
    PreparedStatement pst = null;
    ResultSet rs = null;
    try {
      LOG.debug("Going to execute query <{}>", queryText);
      pst = sqlGenerator.prepareStmtWithParameters(jdbcResource.getConnection(), queryText, params);
      pst.setMaxRows(1);
      rs = pst.executeQuery();

      return rs.next();
    } catch (SQLException ex) {
      LOG.warn(errorMessage, ex);
      throw new MetaException(errorMessage + " " + StringUtils.stringifyException(ex));
    } finally {
      close(rs, pst, null);
    }
  }

  @Override
  public LockResponse lockMaterializationRebuild(String dbName, String tableName, long txnId)
      throws MetaException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Acquiring lock for materialization rebuild with {} for {}",
          JavaUtils.txnIdToString(txnId), TableName.getDbTable(dbName, tableName));
    }

    TxnStore.MutexAPI.LockHandle handle = null;
    Connection dbConn = null;
    PreparedStatement pst = null;
    ResultSet rs = null;
    try {
      lockInternal();
      /**
       * MUTEX_KEY.MaterializationRebuild lock ensures that there is only 1 entry in
       * Initiated/Working state for any resource. This ensures we do not run concurrent
       * rebuild operations on any materialization.
       */
      handle = getMutexAPI().acquireLock(MUTEX_KEY.MaterializationRebuild.name());
      dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);

      List<String> params = Arrays.asList(dbName, tableName);
      String selectQ = "SELECT \"MRL_TXN_ID\" FROM \"MATERIALIZATION_REBUILD_LOCKS\" WHERE" +
          " \"MRL_DB_NAME\" = ? AND \"MRL_TBL_NAME\" = ?";
      pst = sqlGenerator.prepareStmtWithParameters(dbConn, selectQ, params);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to execute query <" + selectQ.replace("?", "{}") + ">",
            quoteString(dbName), quoteString(tableName));
      }
      rs = pst.executeQuery();
      if(rs.next()) {
        LOG.info("Ignoring request to rebuild {}/{} since it is already being rebuilt", dbName, tableName);
        return new LockResponse(txnId, LockState.NOT_ACQUIRED);
      }
      String insertQ = "INSERT INTO \"MATERIALIZATION_REBUILD_LOCKS\" " +
          "(\"MRL_TXN_ID\", \"MRL_DB_NAME\", \"MRL_TBL_NAME\", \"MRL_LAST_HEARTBEAT\") VALUES (" + txnId +
          ", ?, ?, " + Instant.now().toEpochMilli() + ")";
      closeStmt(pst);
      pst = sqlGenerator.prepareStmtWithParameters(dbConn, insertQ, params);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to execute update <" + insertQ.replace("?", "{}") + ">",
            quoteString(dbName), quoteString(tableName));
      }
      pst.executeUpdate();
      LOG.debug("Going to commit");
      dbConn.commit();
      return new LockResponse(txnId, LockState.ACQUIRED);
    } catch (SQLException ex) {
      LOG.warn("lockMaterializationRebuild failed due to " + getMessage(ex), ex);
      throw new MetaException("Unable to retrieve materialization invalidation information due to " +
          StringUtils.stringifyException(ex));
    } finally {
      close(rs, pst, dbConn);
      if(handle != null) {
        handle.releaseLocks();
      }
      unlockInternal();
    }
  }

  @Override
  public boolean heartbeatLockMaterializationRebuild(String dbName, String tableName, long txnId) throws MetaException {
    int result = jdbcResource.execute(
        "UPDATE \"MATERIALIZATION_REBUILD_LOCKS\"" +
            " SET \"MRL_LAST_HEARTBEAT\" = " + Instant.now().toEpochMilli() +
            " WHERE \"MRL_TXN_ID\" = " + txnId +
            " AND \"MRL_DB_NAME\" = ?" +
            " AND \"MRL_TBL_NAME\" = ?",
        new MapSqlParameterSource()
            .addValue("now", Instant.now().toEpochMilli())
            .addValue("txnId", txnId)
            .addValue("dbName", dbName)
            .addValue("tableNane", tableName),
        ParameterizedCommand.AT_LEAST_ONE_ROW);
    return result >= 1;
  }

  @Override
  public long cleanupMaterializationRebuildLocks(ValidTxnList validTxnList, long timeout) throws MetaException {
    try {
      // Aux values
      long cnt = 0L;
      List<Long> txnIds = new ArrayList<>();
      long timeoutTime = Instant.now().toEpochMilli() - timeout;

      Connection dbConn = null;
      Statement stmt = null;
      ResultSet rs = null;
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();

        String selectQ = "SELECT \"MRL_TXN_ID\", \"MRL_LAST_HEARTBEAT\" FROM \"MATERIALIZATION_REBUILD_LOCKS\"";
        LOG.debug("Going to execute query <{}>", selectQ);
        rs = stmt.executeQuery(selectQ);
        while(rs.next()) {
          long lastHeartbeat = rs.getLong(2);
          if (lastHeartbeat < timeoutTime) {
            // The heartbeat has timeout, double check whether we can remove it
            long txnId = rs.getLong(1);
            if (validTxnList.isTxnValid(txnId) || validTxnList.isTxnAborted(txnId)) {
              // Txn was committed (but notification was not received) or it was aborted.
              // Either case, we can clean it up
              txnIds.add(txnId);
            }
          }
        }
        if (!txnIds.isEmpty()) {
          String deleteQ = "DELETE FROM \"MATERIALIZATION_REBUILD_LOCKS\" WHERE" +
              " \"MRL_TXN_ID\" IN(" + StringUtils.join(",", txnIds) + ") ";
          LOG.debug("Going to execute update <{}>", deleteQ);
          cnt = stmt.executeUpdate(deleteQ);
        }
        LOG.debug("Going to commit");
        dbConn.commit();
        return cnt;
      } catch (SQLException e) {
        LOG.debug("Going to rollback: ", e);
        rollbackDBConn(dbConn);
        checkRetryable(e, "cleanupMaterializationRebuildLocks");
        throw new MetaException("Unable to clean rebuild locks due to " +
            StringUtils.stringifyException(e));
      } finally {
        close(rs, stmt, dbConn);
        unlockInternal();
      }
    } catch (RetryException e) {
      return cleanupMaterializationRebuildLocks(validTxnList, timeout);
    }
  }

  /**
   * As much as possible (i.e. in absence of retries) we want both operations to be done on the same
   * connection (but separate transactions).
   *
   * Retry-by-caller note: If the call to lock is from a transaction, then in the worst case
   * there will be a duplicate set of locks but both sets will belong to the same txn so they
   * will not conflict with each other.  For locks w/o txn context (i.e. read-only query), this
   * may lead to deadlock (at least a long wait).  (e.g. 1st call creates locks in {@code LOCK_WAITING}
   * mode and response gets lost.  Then {@link org.apache.hadoop.hive.metastore.RetryingMetaStoreClient}
   * retries, and enqueues another set of locks in LOCK_WAITING.  The 2nd LockResponse is delivered
   * to the DbLockManager, which will keep dong {@link #checkLock(CheckLockRequest)} until the 1st
   * set of locks times out.
   */
  @RetrySemantics.CannotRetry
  public LockResponse lock(LockRequest rqst) throws NoSuchTxnException, TxnAbortedException, MetaException {
    try {
      return txnLockHandler.checkLock(txnLockHandler.enqueueLock(rqst), rqst.getTxnid(), rqst.isZeroWaitReadEnabled(),
          rqst.isExclusiveCTAS());
    } catch (NoSuchLockException e) {
      // This should never happen, as we just added the lock id
      throw new MetaException("Couldn't find a lock we just created! " + e.getMessage());      
    }
  }
  
  private static String normalizeCase(String s) {
    return s == null ? null : s.toLowerCase();
  }

  /**
   * Why doesn't this get a txnid as parameter?  The caller should either know the txnid or know there isn't one.
   * Either way getTxnIdFromLockId() will not be needed.  This would be a Thrift change.
   *
   * Also, when lock acquisition returns WAITING, it's retried every 15 seconds (best case, see DbLockManager.backoff(),
   * in practice more often)
   * which means this is heartbeating way more often than hive.txn.timeout and creating extra load on DB.
   *
   * The clients that operate in blocking mode, can't heartbeat a lock until the lock is acquired.
   * We should make CheckLockRequest include timestamp or last request to skip unnecessary heartbeats. Thrift change.
   *
   * {@link #checkLock(java.sql.Connection, long, long, boolean, boolean)}  must run at SERIALIZABLE
   * (make sure some lock we are checking against doesn't move from W to A in another txn)
   * but this method can heartbeat in separate txn at READ_COMMITTED.
   *
   * Retry-by-caller note:
   * Retryable because {@link #checkLock(Connection, long, long, boolean, boolean)} is
   */
  @Override
  @RetrySemantics.SafeToRetry
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
      new HeartBeatTxnFunction(lockInfo.getTxnId()).execute(jdbcResource);
    } else {
      new HeartBeatLockFunction(rqst.getLockid()).execute(jdbcResource);
    }
    return txnLockHandler.checkLock(extLockId, lockInfo.getTxnId(), false, false);
  }

  /**
   * This would have been made simpler if all locks were associated with a txn.  Then only txn needs to
   * be heartbeated, committed, etc.  no need for client to track individual locks.
   * When removing locks not associated with txn this potentially conflicts with
   * heartbeat/performTimeout which are update/delete of HIVE_LOCKS thus will be locked as needed by db.
   * since this only removes from HIVE_LOCKS at worst some lock acquire is delayed
   */
  @RetrySemantics.Idempotent
  public void unlock(UnlockRequest rqst) throws TxnOpenException, MetaException {
    txnLockHandler.unlock(rqst);
  }

  @RetrySemantics.ReadOnly
  public ShowLocksResponse showLocks(ShowLocksRequest rqst) throws MetaException {
    return txnLockHandler.showLocks(rqst);
  }

  /**
   * {@code ids} should only have txnid or lockid but not both, ideally.
   * Currently DBTxnManager.heartbeat() enforces this.
   */
  @Override
  @RetrySemantics.SafeToRetry
  public void heartbeat(HeartbeatRequest ids)
    throws NoSuchTxnException,  NoSuchLockException, TxnAbortedException, MetaException {
    new HeartBeatTxnFunction(ids.getTxnid()).execute(jdbcResource);
    new HeartBeatLockFunction(ids.getLockid()).execute(jdbcResource);
  }
  
  @Override
  @RetrySemantics.SafeToRetry
  public HeartbeatTxnRangeResponse heartbeatTxnRange(HeartbeatTxnRangeRequest rqst)
    throws MetaException {
    Connection dbConn = null;
    Statement stmt = null;
    HeartbeatTxnRangeResponse rsp = new HeartbeatTxnRangeResponse();
    Set<Long> nosuch = new HashSet<>();
    Set<Long> aborted = new HashSet<>();
    rsp.setNosuch(nosuch);
    rsp.setAborted(aborted);
    try {
      /**
       * READ_COMMITTED is sufficient since {@link #heartbeatTxn(java.sql.Connection, long)}
       * only has 1 update statement in it and
       * we only update existing txns, i.e. nothing can add additional txns that this operation
       * would care about (which would have required SERIALIZABLE)
       */
      dbConn = jdbcResource.getConnection();
      /*do fast path first (in 1 statement) if doesn't work, rollback and do the long version*/
      stmt = dbConn.createStatement();
      List<String> queries = new ArrayList<>();
      int numTxnsToHeartbeat = (int) (rqst.getMax() - rqst.getMin() + 1);
      List<Long> txnIds = new ArrayList<>(numTxnsToHeartbeat);
      for (long txn = rqst.getMin(); txn <= rqst.getMax(); txn++) {
        txnIds.add(txn);
      }
      TransactionContext context = jdbcResource.getTransactionManager().getTransaction(PROPAGATION_REQUIRED);
      Object savePoint = context.createSavepoint();
      TxnUtils.buildQueryWithINClause(conf, queries,
          new StringBuilder("UPDATE \"TXNS\" SET \"TXN_LAST_HEARTBEAT\" = " + getEpochFn(dbProduct) +
              " WHERE \"TXN_STATE\" = " + TxnStatus.OPEN + " AND "),
          new StringBuilder(""), txnIds, "\"TXN_ID\"", true, false);
      int updateCnt = 0;
      for (String query : queries) {
        LOG.debug("Going to execute update <{}>", query);
        updateCnt += stmt.executeUpdate(query);
      }
      if (updateCnt == numTxnsToHeartbeat) {
        //fast pass worked, i.e. all txns we were asked to heartbeat were Open as expected
        context.rollbackToSavepoint(savePoint);
        return rsp;
      }
      //if here, do the slow path so that we can return info txns which were not in expected state
      context.rollbackToSavepoint(savePoint);
      for (long txn = rqst.getMin(); txn <= rqst.getMax(); txn++) {
        try {
          new HeartBeatTxnFunction(txn).execute(jdbcResource);
        } catch (NoSuchTxnException e) {
          nosuch.add(txn);
        } catch (TxnAbortedException e) {
          aborted.add(txn);
        } catch (NoSuchLockException e) {
          throw new RuntimeException(e);
        }
      }
      return rsp;
    } catch (SQLException e) {
      throw new UncategorizedSQLException(null, null, e);
    } finally {
      closeStmt(stmt);
    }
  }

  @Deprecated
  long generateCompactionQueueId(Statement stmt) throws SQLException, MetaException {
    // Get the id for the next entry in the queue
    String s = sqlGenerator.addForUpdateClause("SELECT \"NCQ_NEXT\" FROM \"NEXT_COMPACTION_QUEUE_ID\"");
    LOG.debug("going to execute query <{}>", s);
    try (ResultSet rs = stmt.executeQuery(s)) {
      if (!rs.next()) {
        throw new IllegalStateException("Transaction tables not properly initiated, "
            + "no record found in next_compaction_queue_id");
      }
      long id = rs.getLong(1);
      s = "UPDATE \"NEXT_COMPACTION_QUEUE_ID\" SET \"NCQ_NEXT\" = " + (id + 1) + " WHERE \"NCQ_NEXT\" = " + id;
      LOG.debug("Going to execute update <{}>", s);
      if (stmt.executeUpdate(s) != 1) {
        //TODO: Eliminate this id generation by implementing: https://issues.apache.org/jira/browse/HIVE-27121
        LOG.info("The returned compaction ID ({}) already taken, obtaining new", id);
        return generateCompactionQueueId(stmt);
      }
      return id;
    }
  }

  long generateCompactionQueueId() throws MetaException {
    // Get the id for the next entry in the queue
    String sql = sqlGenerator.addForUpdateClause("SELECT \"NCQ_NEXT\" FROM \"NEXT_COMPACTION_QUEUE_ID\"");
    LOG.debug("going to execute SQL <{}>", sql);
    
    Long allocatedId = jdbcResource.getJdbcTemplate().query(sql, rs -> {
      if (!rs.next()) {
        throw new IllegalStateException("Transaction tables not properly initiated, "
            + "no record found in next_compaction_queue_id");
      }
      long id = rs.getLong(1);
      
      int count = jdbcResource.getJdbcTemplate().update("UPDATE \"NEXT_COMPACTION_QUEUE_ID\" SET \"NCQ_NEXT\" = :newId WHERE \"NCQ_NEXT\" = :id",
          new MapSqlParameterSource()
              .addValue("id", id)
              .addValue("newId", id + 1));
      
      if (count != 1) {
        //TODO: Eliminate this id generation by implementing: https://issues.apache.org/jira/browse/HIVE-27121
        LOG.info("The returned compaction ID ({}) already taken, obtaining new", id);
        return null;
      }
      return id;
    });
    if (allocatedId == null) {
      return generateCompactionQueueId();
    } else {
      return allocatedId;
    }
  }


  @Override
  @RetrySemantics.ReadOnly
  public long getTxnIdForWriteId(String dbName, String tblName, long writeId) throws MetaException {
    return jdbcResource.execute(new TxnIdForWriteIdHandler(writeId, dbName, tblName));
  }

  @Override
  @RetrySemantics.Idempotent
  public CompactionResponse compact(CompactionRequest rqst) throws MetaException {
    return new CompactFunction(rqst, openTxnTimeOutMillis, sqlGenerator, getMutexAPI()).execute(jdbcResource);
  }

  @Override
  @RetrySemantics.SafeToRetry
  public boolean submitForCleanup(CompactionRequest rqst, long highestWriteId, long txnId) throws MetaException {
    // Put a compaction request in the queue.
    try {
      Connection dbConn = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        lockInternal();

        List<String> params = new ArrayList<String>() {{
          add(rqst.getDbname());
          add(rqst.getTablename());
        }};
        long cqId;
        try (Statement stmt = dbConn.createStatement()) {
          cqId = generateCompactionQueueId(stmt);
        }
        StringBuilder buf = new StringBuilder(
            "INSERT INTO \"COMPACTION_QUEUE\" (\"CQ_ID\", \"CQ_HIGHEST_WRITE_ID\", \"CQ_TXN_ID\", \"CQ_ENQUEUE_TIME\", \"CQ_DATABASE\", \"CQ_TABLE\", ");
        String partName = rqst.getPartitionname();
        if (partName != null) {
          buf.append("\"CQ_PARTITION\", ");
          params.add(partName);
        }
        buf.append("\"CQ_STATE\", \"CQ_TYPE\"");
        params.add(String.valueOf(READY_FOR_CLEANING));
        params.add(TxnUtils.thriftCompactionType2DbType(rqst.getType()).toString());

        if (rqst.getProperties() != null) {
          buf.append(", \"CQ_TBLPROPERTIES\"");
          params.add(new StringableMap(rqst.getProperties()).toString());
        }
        if (rqst.getRunas() != null) {
          buf.append(", \"CQ_RUN_AS\"");
          params.add(rqst.getRunas());
        }
        buf.append(") values (")
          .append(
            Stream.of(cqId, highestWriteId, txnId, getEpochFn(dbProduct))
              .map(Object::toString)
              .collect(Collectors.joining(", ")))
          .append(repeat(", ?", params.size()))
          .append(")");

        String s = buf.toString();
        try (PreparedStatement pst = sqlGenerator.prepareStmtWithParameters(dbConn, s, params)) {
          LOG.debug("Going to execute update <{}>", s);
          pst.executeUpdate();
        }
        LOG.debug("Going to commit");
        dbConn.commit();
        return true;
      } catch (SQLException e) {
        LOG.debug("Going to rollback: ", e);
        rollbackDBConn(dbConn);
        checkRetryable(e, "submitForCleanup(" + rqst + ")");
        throw new MetaException("Failed to submit cleanup request: " +
          StringUtils.stringifyException(e));
      } finally {
        closeDbConn(dbConn);
        unlockInternal();
      }
    } catch (RetryException e) {
      return submitForCleanup(rqst, highestWriteId, txnId);
    }
  }
  
  @RetrySemantics.ReadOnly
  public ShowCompactResponse showCompact(ShowCompactRequest rqst) throws MetaException {
    return jdbcResource.execute(new ShowCompactHandler(rqst, sqlGenerator));
  }

  /**
   * We assume this is only called by metadata cache server to know if there are new base/delta files should be read.
   * The query filters compactions by state and only returns SUCCEEDED or READY_FOR_CLEANING compactions because
   * only these two states means there are new files ready to be read.
   */
  @RetrySemantics.ReadOnly
  public GetLatestCommittedCompactionInfoResponse getLatestCommittedCompactionInfo(
      GetLatestCommittedCompactionInfoRequest rqst) throws MetaException {
    GetLatestCommittedCompactionInfoResponse response = new GetLatestCommittedCompactionInfoResponse(new ArrayList<>());
    Connection dbConn = null;
    PreparedStatement pst = null;
    ResultSet rs = null;
    try {
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);

        List<String> params = new ArrayList<>();
        // This query combines the result sets of SUCCEEDED compactions and READY_FOR_CLEANING compactions
        // We also sort the result by CC_ID in descending order so that we can keep only the latest record
        // according to the order in result set
        StringBuilder sb = new StringBuilder()
            .append("SELECT * FROM (")
            .append("   SELECT")
            .append("   \"CC_ID\", \"CC_DATABASE\", \"CC_TABLE\", \"CC_PARTITION\", \"CC_TYPE\"")
            .append("   FROM \"COMPLETED_COMPACTIONS\"")
            .append("     WHERE \"CC_STATE\" = " + quoteChar(SUCCEEDED_STATE))
            .append("   UNION ALL")
            .append("   SELECT")
            .append("   \"CQ_ID\" AS \"CC_ID\", \"CQ_DATABASE\" AS \"CC_DATABASE\"")
            .append("   ,\"CQ_TABLE\" AS \"CC_TABLE\", \"CQ_PARTITION\" AS \"CC_PARTITION\"")
            .append("   ,\"CQ_TYPE\" AS \"CC_TYPE\"")
            .append("   FROM \"COMPACTION_QUEUE\"")
            .append("     WHERE \"CQ_STATE\" = " + quoteChar(READY_FOR_CLEANING))
            .append(") AS compactions ")
            .append(" WHERE \"CC_DATABASE\" = ? AND \"CC_TABLE\" = ?");
        params.add(rqst.getDbname());
        params.add(rqst.getTablename());
        if (rqst.getPartitionnamesSize() > 0) {
          sb.append(" AND \"CC_PARTITION\" IN (");
          sb.append(String.join(",",
              Collections.nCopies(rqst.getPartitionnamesSize(), "?")));
          sb.append(")");
          params.addAll(rqst.getPartitionnames());
        }
        if (rqst.isSetLastCompactionId()) {
          sb.append(" AND \"CC_ID\" > ?");
        }
        sb.append(" ORDER BY \"CC_ID\" DESC");

        pst = sqlGenerator.prepareStmtWithParameters(dbConn, sb.toString(), params);
        if (rqst.isSetLastCompactionId()) {
          pst.setLong(params.size() + 1, rqst.getLastCompactionId());
        }
        LOG.debug("Going to execute query <{}>", sb);
        rs = pst.executeQuery();
        Set<String> partitionSet = new HashSet<>();
        while (rs.next()) {
          CompactionInfoStruct lci = new CompactionInfoStruct();
          lci.setId(rs.getLong(1));
          lci.setDbname(rs.getString(2));
          lci.setTablename(rs.getString(3));
          String partition = rs.getString(4);
          if (!rs.wasNull()) {
            lci.setPartitionname(partition);
          }
          lci.setType(TxnUtils.dbCompactionType2ThriftType(rs.getString(5).charAt(0)));
          // Only put the latest record of each partition into response
          if (!partitionSet.contains(partition)) {
            response.addToCompactions(lci);
            partitionSet.add(partition);
          }
        }
      } catch (SQLException e) {
        LOG.error("Unable to execute query", e);
        checkRetryable(e, "getLatestCommittedCompactionInfo");
      } finally {
        close(rs, pst, dbConn);
      }
      return response;
    } catch (RetryException e) {
      return getLatestCommittedCompactionInfo(rqst);
    }
  }

  public MetricsInfo getMetricsInfo() throws MetaException {
    int threshold = MetastoreConf.getIntVar(conf, ConfVars.METASTORE_ACIDMETRICS_TABLES_WITH_ABORTED_TXNS_THRESHOLD);
    MetricsInfo metrics = jdbcResource.execute(MetricsInfoHandler.INSTANCE);
    Set<String> resourceNames = jdbcResource.execute(new TablesWithAbortedTxnsHandler(threshold));
    metrics.setTablesWithXAbortedTxnsCount(resourceNames.size());
    metrics.setTablesWithXAbortedTxns(resourceNames);    
    return metrics;
  }


  private static void shouldNeverHappen(long txnid) {
    throw new RuntimeException("This should never happen: " + JavaUtils.txnIdToString(txnid));
  }
  private static void shouldNeverHappen(long txnid, long extLockId, long intLockId) {
    throw new RuntimeException("This should never happen: " + JavaUtils.txnIdToString(txnid) + " "
      + JavaUtils.lockIdToString(extLockId) + " " + intLockId);
  }

  /**
   * Retry-by-caller note:
   * This may be retried after dbConn.commit.  At worst, it will create duplicate entries in
   * TXN_COMPONENTS which won't affect anything.  See more comments in {@link #commitTxn(CommitTxnRequest)}
   */
  @Override
  @RetrySemantics.SafeToRetry
  public void addDynamicPartitions(AddDynamicPartitions rqst) throws NoSuchTxnException,  TxnAbortedException, MetaException {
    Connection dbConn = null;
    Statement stmt = null;
    try {
      dbConn = jdbcResource.getConnection();
      stmt = dbConn.createStatement();
      TxnType txnType = jdbcResource.execute(new GetOpenTxnTypeAndLockHandler(sqlGenerator, rqst.getTxnid()));
      if (txnType == null) {
        //ensures txn is still there and in expected state
        ensureValidTxn(dbConn, rqst.getTxnid(), stmt);
        shouldNeverHappen(rqst.getTxnid());
      }
      //for RU this may be null so we should default it to 'u' which is most restrictive
      OperationType ot = OperationType.UPDATE;
      if (rqst.isSetOperationType()) {
        ot = OperationType.fromDataOperationType(rqst.getOperationType());
      }

      Long writeId = rqst.getWriteid();
      jdbcResource.execute(new InsertTxnComponentsCommand(rqst), maxBatchSize);
      
      try (PreparedStatement pstmt = dbConn.prepareStatement(TXN_COMPONENTS_DP_DELETE_QUERY)) {
        pstmt.setLong(1, rqst.getTxnid());
        pstmt.setString(2, normalizeCase(rqst.getDbname()));
        pstmt.setString(3, normalizeCase(rqst.getTablename()));
        pstmt.execute();
      }
    } catch (SQLException e) {
      LOG.debug("Going to rollback: ", e);
      throw new MetaException("Unable to insert into from transaction database " +
          StringUtils.stringifyException(e));
    } finally {
      closeStmt(stmt);
    }
  }

  /**
   * Clean up corresponding records in metastore tables when corresponding object is dropped,
   * specifically: TXN_COMPONENTS, COMPLETED_TXN_COMPONENTS, COMPACTION_QUEUE, COMPLETED_COMPACTIONS
   * Retry-by-caller note: this is only idempotent assuming it's only called by dropTable/Db/etc
   * operations.
   *
   * HIVE_LOCKS and WS_SET are cleaned up by {@link AcidHouseKeeperService}, if turned on
   */
  @Override
  @RetrySemantics.Idempotent
  public void cleanupRecords(HiveObjectType type, Database db, Table table,
        Iterator<Partition> partitionIterator, boolean keepTxnToWriteIdMetaData) throws MetaException {
    new CleanupRecordsFunction(type, db, table, partitionIterator, getDefaultCatalog(conf), keepTxnToWriteIdMetaData, null)
        .execute(jdbcResource);
  }

  @Override
  @RetrySemantics.Idempotent
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
   * For testing only, do not use.
   */
  @VisibleForTesting
  public int numLocksInLockTable() throws SQLException, MetaException {
    int count = jdbcResource.getJdbcTemplate().queryForObject("SELECT COUNT(*) FROM \"HIVE_LOCKS\"", new MapSqlParameterSource(), Integer.TYPE);
    jdbcResource.getTransactionManager().getTransaction(PROPAGATION_REQUIRED).setRollbackOnly();
    return count;
  }

  /**
   * For testing only, do not use.
   */
  public long setTimeout(long milliseconds) {
    long previous_timeout = timeout;
    timeout = milliseconds;
    return previous_timeout;
  }

  protected class RetryException extends Exception {

  }

  Connection getDbConn(int isolationLevel) throws SQLException {
    return getDbConn(isolationLevel, connPool);
  }

  protected Connection getDbConn(int isolationLevel, DataSource connPool) throws SQLException {
    Connection dbConn = null;
    try {
      dbConn = connPool.getConnection();
      dbConn.setAutoCommit(false);
      dbConn.setTransactionIsolation(isolationLevel);
      return dbConn;
    } catch (SQLException e) {
      closeDbConn(dbConn);
      throw e;
    }
  }

  static void rollbackDBConn(Connection dbConn) {
    try {
      if (dbConn != null && !dbConn.isClosed()) dbConn.rollback();
    } catch (SQLException e) {
      LOG.warn("Failed to rollback db connection " + getMessage(e));
    }
  }
  protected static void closeDbConn(Connection dbConn) {
    try {
      if (dbConn != null && !dbConn.isClosed()) {
        dbConn.close();
      }
    } catch (SQLException e) {
      LOG.warn("Failed to close db connection " + getMessage(e));
    }
  }

  /**
   * Close statement instance.
   * @param stmt statement instance.
   */
  protected static void closeStmt(Statement stmt) {
    try {
      if (stmt != null && !stmt.isClosed()) stmt.close();
    } catch (SQLException e) {
      LOG.warn("Failed to close statement " + getMessage(e));
    }
  }

  /**
   * Close the ResultSet.
   * @param rs may be {@code null}
   */
  static void close(ResultSet rs) {
    try {
      if (rs != null && !rs.isClosed()) {
        rs.close();
      }
    }
    catch(SQLException ex) {
      LOG.warn("Failed to close statement " + getMessage(ex));
    }
  }

  /**
   * Close all 3 JDBC artifacts in order: {@code rs stmt dbConn}
   */
  static void close(ResultSet rs, Statement stmt, Connection dbConn) {
    close(rs);
    closeStmt(stmt);
    closeDbConn(dbConn);
  }

  private boolean waitForRetry(String caller, String errMsg) {
    if (retryNum++ < retryLimit) {
      LOG.warn("Retryable error detected in {}. Will wait {} ms and retry up to {} times. Error: {}", caller,
          retryInterval, (retryLimit - retryNum + 1), errMsg);
      try {
        Thread.sleep(retryInterval);
      } catch (InterruptedException ex) {
        //
      }
      return true;
    } else {
      LOG.error("Fatal error in {}. Retry limit ({}) reached. Last error: {}", caller, retryLimit, errMsg);
    }
    return false;
  }

  /**
   * See {@link #checkRetryable(SQLException, String, boolean)}.
   */
  void checkRetryable(SQLException e, String caller) throws RetryException {
    checkRetryable(e, caller, false);
  }

  /**
   * Determine if an exception was such that it makes sense to retry.  Unfortunately there is no standard way to do
   * this, so we have to inspect the error messages and catch the telltale signs for each
   * different database.  This method will throw {@code RetryException}
   * if the error is retry-able.
   * @param e exception that was thrown.
   * @param caller name of the method calling this (and other info useful to log)
   * @param retryOnDuplicateKey whether to retry on unique key constraint violation
   * @throws org.apache.hadoop.hive.metastore.txn.TxnHandler.RetryException when the operation should be retried
   */
  void checkRetryable(SQLException e, String caller, boolean retryOnDuplicateKey)
      throws RetryException {

    // If you change this function, remove the @Ignore from TestTxnHandler.deadlockIsDetected()
    // to test these changes.
    // MySQL and MSSQL use 40001 as the state code for rollback.  Postgres uses 40001 and 40P01.
    // Oracle seems to return different SQLStates and messages each time,
    // so I've tried to capture the different error messages (there appear to be fewer different
    // error messages than SQL states).
    // Derby and newer MySQL driver use the new SQLTransactionRollbackException
    boolean sendRetrySignal = false;
    try {
      if(dbProduct == null) {
        throw new IllegalStateException("DB Type not determined yet.");
      }
      if (dbProduct.isDeadlock(e)) {
        if (deadlockCnt++ < ALLOWED_REPEATED_DEADLOCKS) {
          long waitInterval = deadlockRetryInterval * deadlockCnt;
          LOG.warn("Deadlock detected in {}. Will wait {} ms try again up to {} times.", caller, waitInterval,
              (ALLOWED_REPEATED_DEADLOCKS - deadlockCnt + 1));
          // Pause for a just a bit for retrying to avoid immediately jumping back into the deadlock.
          try {
            Thread.sleep(waitInterval);
          } catch (InterruptedException ie) {
            // NOP
          }
          sendRetrySignal = true;
        } else {
          LOG.error("Too many repeated deadlocks in {}, giving up.", caller);
        }
      } else if (isRetryable(conf, e)) {
        //in MSSQL this means Communication Link Failure
        sendRetrySignal = waitForRetry(caller, e.getMessage());
      } else if (retryOnDuplicateKey && isDuplicateKeyError(e)) {
        sendRetrySignal = waitForRetry(caller, e.getMessage());
      }
      else {
        //make sure we know we saw an error that we don't recognize
        LOG.info("Non-retryable error in {} : {}", caller, getMessage(e));
      }
    }
    finally {
      /*if this method ends with anything except a retry signal, the caller should fail the operation
      and propagate the error up to the its caller (Metastore client); thus must reset retry counters*/
      if(!sendRetrySignal) {
        deadlockCnt = 0;
        retryNum = 0;
      }
    }
    if(sendRetrySignal) {
      throw new RetryException();
    }
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
  

  protected String isWithinCheckInterval(String expr, long interval) throws MetaException {
    return dbProduct.isWithinCheckInterval(expr, interval);
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

  private enum LockAction {ACQUIRE, WAIT, KEEP_LOOKING}

  // A jump table to figure out whether to wait, acquire,
  // or keep looking .  Since
  // java doesn't have function pointers (grumble grumble) we store a
  // character that we'll use to determine which function to call.
  // The table maps the lock type of the lock we are looking to acquire to
  // the lock type of the lock we are checking to the lock state of the lock
  // we are checking to the desired action.
  private static Map<LockType, Map<LockType, Map<LockState, LockAction>>> jumpTable;

  private int abortTxns(List<Long> txnids,
                        boolean skipCount, boolean isReplReplayed, TxnErrorMsg txnErrorMsg) throws SQLException, MetaException {
    return new AbortTxnsFunction(conf, sqlGenerator, txnids, false, skipCount, isReplReplayed, txnErrorMsg).execute(jdbcResource);
  }
  /**
   * TODO: expose this as an operation to client.  Useful for streaming API to abort all remaining
   * transactions in a batch on IOExceptions.
   * Caller must rollback the transaction if not all transactions were aborted since this will not
   * attempt to delete associated locks in this case.
   *
   * @param dbConn An active connection
   * @param txnids list of transactions to abort
   * @param checkHeartbeat value used by {@link #performTimeOuts()} to ensure this doesn't Abort txn which were
   *                      heartbeated after #performTimeOuts() select and this operation.
   * @param skipCount If true, the method always returns 0, otherwise returns the number of actually aborted txns
   * @return 0 if skipCount is true, the number of aborted transactions otherwise
   * @throws SQLException
   */
  private int abortTxns(List<Long> txnids, boolean checkHeartbeat,
                        boolean skipCount, boolean isReplReplayed, TxnErrorMsg txnErrorMsg)
      throws SQLException, MetaException {
    return new AbortTxnsFunction(conf, sqlGenerator, txnids, checkHeartbeat, skipCount, isReplReplayed, txnErrorMsg).execute(jdbcResource);
  }

  /**
   * Returns the state of the transaction if it's able to determine it. Some cases where it cannot:
   * 1. txnid was Aborted/Committed and then GC'd (compacted)
   * 2. txnid was committed but it didn't modify anything (nothing in COMPLETED_TXN_COMPONENTS)
   */
  private TxnStatus findTxnState(long txnid, Statement stmt) throws SQLException, MetaException {
    String s = "SELECT \"TXN_STATE\" FROM \"TXNS\" WHERE \"TXN_ID\" = " + txnid;
    LOG.debug("Going to execute query <{}>", s);
    try (ResultSet rs = stmt.executeQuery(s)) {
      if (!rs.next()) {
        s =
            sqlGenerator.addLimitClause(1, "1 FROM \"COMPLETED_TXN_COMPONENTS\" WHERE \"CTC_TXNID\" = "
                + txnid);
        LOG.debug("Going to execute query <{}>", s);
        try (ResultSet rs2 = stmt.executeQuery(s)) {
          if (rs2.next()) {
            return TxnStatus.COMMITTED;
          }
        }
        // could also check WRITE_SET but that seems overkill
        return TxnStatus.UNKNOWN;
      }
      return TxnStatus.fromString(rs.getString(1));
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

  /**
   * Used to raise an informative error when the caller expected a txn in a particular TxnStatus
   * but found it in some other status
   */
  private static void raiseTxnUnexpectedState(TxnStatus actualStatus, long txnid)
    throws NoSuchTxnException, TxnAbortedException {
    switch (actualStatus) {
      case ABORTED:
        throw new TxnAbortedException("Transaction " + JavaUtils.txnIdToString(txnid) + " already aborted");
      case COMMITTED:
        throw new NoSuchTxnException("Transaction " + JavaUtils.txnIdToString(txnid) + " is already committed.");
      case UNKNOWN:
        throw new NoSuchTxnException("No such transaction " + JavaUtils.txnIdToString(txnid));
      case OPEN:
        throw new NoSuchTxnException(JavaUtils.txnIdToString(txnid) + " is " + TxnStatus.OPEN);
      default:
        throw new IllegalArgumentException("Unknown TxnStatus " + actualStatus);
    }
  }
  /**
   * Returns the state of the transaction with {@code txnid} or throws if {@code raiseError} is true.
   */
  private static void ensureValidTxn(Connection dbConn, long txnid, Statement stmt)
      throws SQLException, NoSuchTxnException, TxnAbortedException {
    // We need to check whether this transaction is valid and open
    String s = "SELECT \"TXN_STATE\" FROM \"TXNS\" WHERE \"TXN_ID\" = " + txnid;
    LOG.debug("Going to execute query <{}>", s);
    try (ResultSet rs = stmt.executeQuery(s)) {
      if (!rs.next()) {
        // todo: add LIMIT 1 instead of count - should be more efficient
        s = "SELECT COUNT(*) FROM \"COMPLETED_TXN_COMPONENTS\" WHERE \"CTC_TXNID\" = " + txnid;
        try (ResultSet rs2 = stmt.executeQuery(s)) {
          // todo: strictly speaking you can commit an empty txn, thus 2nd conjunct is wrong but
          // only
          // possible for for multi-stmt txns
          boolean alreadyCommitted = rs2.next() && rs2.getInt(1) > 0;
          LOG.debug("Going to rollback");
          rollbackDBConn(dbConn);
          if (alreadyCommitted) {
            // makes the message more informative - helps to find bugs in client code
            throw new NoSuchTxnException("Transaction " + JavaUtils.txnIdToString(txnid)
                + " is already committed.");
          }
          throw new NoSuchTxnException("No such transaction " + JavaUtils.txnIdToString(txnid));
        }
      }
      if (TxnStatus.fromString(rs.getString(1)) == TxnStatus.ABORTED) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        throw new TxnAbortedException("Transaction " + JavaUtils.txnIdToString(txnid)
            + " already aborted");// todo: add time of abort, which is not currently tracked.
                                  // Requires schema change
      }
    }
  }

  /**
   * Isolation Level Notes
   * Plain: RC is OK
   * This will find transactions that have timed out and abort them.
   * Will also delete locks which are not associated with a transaction and have timed out
   * Tries to keep transactions (against metastore db) small to reduce lock contention.
   */
  @RetrySemantics.Idempotent
  public void performTimeOuts() {
    new PerformTimeoutsFunction(conf, sqlGenerator, timeout, replicationTxnTimeout).execute(jdbcResource);
  }

  @Override
  @RetrySemantics.ReadOnly
  public void countOpenTxns() throws MetaException {
    int openTxns = jdbcResource.execute(new CountOpenTxnsHandler());
    if (openTxns > -1) {
      numOpenTxns.set(openTxns);
    }
  }

  @Override
  @RetrySemantics.SafeToRetry
  public void addWriteIdsToMinHistory(long txnid, Map<String, Long> minOpenWriteIds) throws MetaException {
    jdbcResource.execute(new AddWriteIdsToMinHistoryCommand(txnid, minOpenWriteIds), maxBatchSize);
  }

  /**
   * Remove minOpenWriteIds from min_history_write_id tables
   * @param dbConn connection
   * @param txnids transactions
   */
  private void removeWriteIdsFromMinHistory(Connection dbConn, List<Long> txnids) throws SQLException {
    jdbcResource.execute(new RemoveWriteIdsFromMinHistoryCommand(conf, txnids), maxBatchSize);
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
      String connectionPooler = MetastoreConf.getVar(conf, ConfVars.CONNECTION_POOLING_TYPE).toLowerCase();
      if ("none".equals(connectionPooler)) {
        LOG.info("Choosing not to pool JDBC connections");
        return new NoPoolConnectionPool(conf);
      } else {
        throw new RuntimeException("Unknown JDBC connection pooling " + connectionPooler);
      }
    }
  }

  /**
   * Returns true if {@code ex} should be retried
   */
  static boolean isRetryable(Configuration conf, Exception ex) {
    if(ex instanceof SQLException) {
      SQLException sqlException = (SQLException)ex;
      if (MANUAL_RETRY.equalsIgnoreCase(sqlException.getSQLState())) {
        // Manual retry exception was thrown
        return true;
      }
      if ("08S01".equalsIgnoreCase(sqlException.getSQLState())) {
        //in MSSQL this means Communication Link Failure
        return true;
      }
      if ("ORA-08176".equalsIgnoreCase(sqlException.getSQLState()) ||
        sqlException.getMessage().contains("consistent read failure; rollback data not available")) {
        return true;
      }

      String regex = MetastoreConf.getVar(conf, ConfVars.TXN_RETRYABLE_SQLEX_REGEX);
      if (regex != null && !regex.isEmpty()) {
        String[] patterns = regex.split(",(?=\\S)");
        String message = getMessage((SQLException)ex);
        for (String p : patterns) {
          if (Pattern.matches(p, message)) {
            return true;
          }
        }
      }
      //see also https://issues.apache.org/jira/browse/HIVE-9938
    }
    return false;
  }

  private boolean isDuplicateKeyError(SQLException ex) {
    return dbProduct.isDuplicateKeyError(ex);
  }

  private static String getMessage(SQLException ex) {
    return ex.getMessage() + " (SQLState=" + ex.getSQLState() + ", ErrorCode=" + ex.getErrorCode() + ")";
  }
  static String quoteString(String input) {
    return "'" + input + "'";
  }
  static String quoteChar(char c) {
    return "'" + c + "'";
  }

  /**
   * {@link #lockInternal()} and {@link #unlockInternal()} are used to serialize those operations that require
   * Select ... For Update to sequence operations properly.  In practice that means when running
   * with Derby database.  See more notes at class level.
   */
  protected void lockInternal() {
    if(dbProduct.isDERBY()) {
      derbyLock.lock();
    }
  }
  protected void unlockInternal() {
    if(dbProduct.isDERBY()) {
      derbyLock.unlock();
    }
  }
  
  @Override
  @RetrySemantics.Idempotent
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

  /**
   * Acquire the global txn lock, used to mutex the openTxn and commitTxn.
   * @param shared either SHARED_READ or EXCLUSIVE
   * @throws SQLException
   */
  private void acquireTxnLock(boolean shared) throws MetaException {
    String sqlStmt = sqlGenerator.createTxnLockStatement(shared);
    jdbcResource.getJdbcTemplate().getJdbcTemplate().execute((Statement stmt) -> {
        stmt.execute(sqlStmt);
        return null;
    });
    LOG.debug("TXN lock locked by {} in mode {}", quoteString(TxnHandler.hostname), shared);
  }

  public static class NoPoolConnectionPool implements DataSource {
    // Note that this depends on the fact that no-one in this class calls anything but
    // getConnection.  If you want to use any of the Logger or wrap calls you'll have to
    // implement them.
    private final Configuration conf;
    private Driver driver;
    private String connString;
    private String user;
    private String passwd;

    public NoPoolConnectionPool(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public Connection getConnection() throws SQLException {
      if (user == null) {
        user = DataSourceProvider.getMetastoreJdbcUser(conf);
        passwd = DataSourceProvider.getMetastoreJdbcPasswd(conf);
      }
      return getConnection(user, passwd);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      // Find the JDBC driver
      if (driver == null) {
        String driverName = MetastoreConf.getVar(conf, ConfVars.CONNECTION_DRIVER);
        if (driverName == null || driverName.equals("")) {
          String msg = "JDBC driver for transaction db not set in configuration " +
              "file, need to set " + ConfVars.CONNECTION_DRIVER.getVarname();
          LOG.error(msg);
          throw new RuntimeException(msg);
        }
        try {
          LOG.info("Going to load JDBC driver {}", driverName);
          driver = (Driver) Class.forName(driverName).newInstance();
        } catch (InstantiationException e) {
          throw new RuntimeException("Unable to instantiate driver " + driverName + ", " +
              e.getMessage(), e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(
              "Unable to access driver " + driverName + ", " + e.getMessage(),
              e);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException("Unable to find driver " + driverName + ", " + e.getMessage(),
              e);
        }
        connString = MetastoreConf.getVar(conf, ConfVars.CONNECT_URL_KEY);
      }

      try {
        LOG.info("Connecting to transaction db with connection string {}", connString);
        Properties connectionProps = new Properties();
        connectionProps.setProperty("user", username);
        connectionProps.setProperty("password", password);
        Connection conn = driver.connect(connString, connectionProps);
        String prepareStmt = dbProduct != null ? dbProduct.getPrepareTxnStmt() : null;
        if (prepareStmt != null) {
          try (Statement stmt = conn.createStatement()) {
            stmt.execute(prepareStmt);
          }
        }
        conn.setAutoCommit(false);
        return conn;
      } catch (SQLException e) {
        throw new RuntimeException("Unable to connect to transaction manager using " + connString
            + ", " + e.getMessage(), e);
      }
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getLoginTimeout() throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  @RetrySemantics.SafeToRetry
  public AbortCompactResponse abortCompactions(AbortCompactionRequest reqst) throws MetaException, NoSuchCompactionException {
    Map<Long, AbortCompactionResponseElement> abortCompactionResponseElements = new HashMap<>();
    AbortCompactResponse response = new AbortCompactResponse(new HashMap<>());
    response.setAbortedcompacts(abortCompactionResponseElements);

    List<Long> compactionIdsToAbort = reqst.getCompactionIds();
    if (compactionIdsToAbort.isEmpty()) {
      LOG.info("Compaction ids are missing in request. No compactions to abort");
      throw new NoSuchCompactionException("Compaction ids missing in request. No compactions to abort");
    }
    reqst.getCompactionIds().forEach(x -> abortCompactionResponseElements.put(x, getAbortCompactionResponseElement(x,"Error","No Such Compaction Id Available")));

    List<CompactionInfo> eligibleCompactionsToAbort = findEligibleCompactionsToAbort(abortCompactionResponseElements,
            compactionIdsToAbort);
    for (CompactionInfo compactionInfo : eligibleCompactionsToAbort) {
      abortCompactionResponseElements.put(compactionInfo.id, abortCompaction(compactionInfo));
    }
    return response;
  }

  private AbortCompactionResponseElement getAbortCompactionResponseElement(long compactionId, String status, String message) {
    AbortCompactionResponseElement resEle = new AbortCompactionResponseElement(compactionId);
    resEle.setMessage(message);
    resEle.setStatus(status);
    return resEle;
  }

  @RetrySemantics.SafeToRetry
  private AbortCompactionResponseElement abortCompaction(CompactionInfo compactionInfo) throws MetaException {
    SqlRetryFunction<AbortCompactionResponseElement> function = () -> {
      jdbcResource.bindDataSource(POOL_TX);
      try (TransactionContext context = jdbcResource.getTransactionManager().getTransaction(PROPAGATION_REQUIRED)) {
        compactionInfo.state = TxnStore.ABORTED_STATE;
        compactionInfo.errorMessage = "Compaction Aborted by Abort Comapction request.";
        int updCount;
        try {
          updCount = jdbcResource.execute(new InsertCompactionInfoCommand(compactionInfo, getDbTime().getTime()));
        } catch (Exception e) {
          LOG.error("Unable to update compaction record: {}.", compactionInfo);
          return getAbortCompactionResponseElement(compactionInfo.id, "Error",
              "Error while aborting compaction:Unable to update compaction record in COMPLETED_COMPACTIONS");
        }
        LOG.debug("Inserted {} entries into COMPLETED_COMPACTIONS", updCount);
        try {
          updCount = jdbcResource.getJdbcTemplate().update("DELETE FROM \"COMPACTION_QUEUE\" WHERE \"CQ_ID\" = :id",
              new MapSqlParameterSource().addValue("id", compactionInfo.id));
          if (updCount != 1) {
            LOG.error("Unable to update compaction record: {}. updCnt={}", compactionInfo, updCount);
            return getAbortCompactionResponseElement(compactionInfo.id, "Error",
                "Error while aborting compaction: Unable to update compaction record in COMPACTION_QUEUE");
          } else {
            jdbcResource.getTransactionManager().commit(context);
            return getAbortCompactionResponseElement(compactionInfo.id, "Success",
                "Successfully aborted compaction");
          }
        } catch (DataAccessException e) {
          return getAbortCompactionResponseElement(compactionInfo.id, "Error",
              "Error while aborting compaction:" + e.getMessage());
        }
      } finally {
        jdbcResource.unbindDataSource();
      }
    };
    try {
      return sqlRetryHandler.executeWithRetry(
          new SqlRetryCallProperties().withCallerId("abortCompaction(" + compactionInfo + ")"), function);
    } catch (TException e) {
      throw (MetaException) e;
    }
  }
  
  private List<CompactionInfo> findEligibleCompactionsToAbort(Map<Long,
          AbortCompactionResponseElement> abortCompactionResponseElements, List<Long> requestedCompId) throws MetaException {

    List<CompactionInfo> compactionInfoList = new ArrayList<>();
    String queryText = TxnQueries.SELECT_COMPACTION_QUEUE_BY_COMPID + "  WHERE \"CC_ID\" IN (?) " ;
    String sqlIN = requestedCompId.stream()
            .map(x -> String.valueOf(x))
            .collect(Collectors.joining(",", "(", ")"));
    queryText = queryText.replace("(?)", sqlIN);
    try (Connection dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
         Statement pStmt = dbConn.createStatement()) {
      try (ResultSet rs = pStmt.executeQuery(queryText)) {
        while (rs.next()) {
          char compState = rs.getString(5).charAt(0);
          long compID = rs.getLong(1);
          if (CompactionState.INITIATED.equals(CompactionState.fromSqlConst(compState))) {
            compactionInfoList.add(CompactionInfo.loadFullFromCompactionQueue(rs));
          } else {
            abortCompactionResponseElements.put(compID, getAbortCompactionResponseElement(compID,"Error",
                    "Error while aborting compaction as compaction is in state-" + CompactionState.fromSqlConst(compState)));
          }
        }
      }
    } catch (SQLException e) {
      throw new MetaException("Unable to select from transaction database-" + StringUtils.stringifyException(e));
    }
    return compactionInfoList;
  }

}
