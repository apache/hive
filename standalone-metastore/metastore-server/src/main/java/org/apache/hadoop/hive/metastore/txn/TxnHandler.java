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

import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.classification.RetrySemantics;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.TransactionalMetaStoreEventListener;
import org.apache.hadoop.hive.metastore.LockTypeComparator;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.MaxAllocatedTableWriteIdRequest;
import org.apache.hadoop.hive.metastore.api.MaxAllocatedTableWriteIdResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
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
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.datasource.DataSourceProvider;
import org.apache.hadoop.hive.metastore.datasource.DataSourceProviderFactory;
import org.apache.hadoop.hive.metastore.events.AbortTxnEvent;
import org.apache.hadoop.hive.metastore.events.AllocWriteIdEvent;
import org.apache.hadoop.hive.metastore.events.CommitCompactionEvent;
import org.apache.hadoop.hive.metastore.events.CommitTxnEvent;
import org.apache.hadoop.hive.metastore.events.OpenTxnEvent;
import org.apache.hadoop.hive.metastore.events.AcidWriteEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.LockTypeUtil;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.StringableMap;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;

import static org.apache.hadoop.hive.metastore.txn.TxnDbUtil.executeQueriesInBatch;
import static org.apache.hadoop.hive.metastore.txn.TxnDbUtil.executeQueriesInBatchNoCount;
import static org.apache.hadoop.hive.metastore.txn.TxnDbUtil.getEpochFn;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;


import com.google.common.annotations.VisibleForTesting;

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

  static final protected char INITIATED_STATE = 'i';
  static final protected char WORKING_STATE = 'w';
  static final protected char READY_FOR_CLEANING = 'r';
  static final char FAILED_STATE = 'f';
  static final char SUCCEEDED_STATE = 's';
  static final char ATTEMPTED_STATE = 'a';

  // Compactor types
  static final protected char MAJOR_TYPE = 'a';
  static final protected char MINOR_TYPE = 'i';


  private static final String TXN_TMP_STATE = "_";

  // Lock states
  static final protected char LOCK_ACQUIRED = 'a';
  static final protected char LOCK_WAITING = 'w';

  private static final int ALLOWED_REPEATED_DEADLOCKS = 10;
  private static final Logger LOG = LoggerFactory.getLogger(TxnHandler.class.getName());


  private static DataSource connPool;
  private static DataSource connPoolMutex;

  private static final String MANUAL_RETRY = "ManualRetry";

  // Query definitions
  private static final String HIVE_LOCKS_INSERT_QRY = "INSERT INTO \"HIVE_LOCKS\" ( " +
      "\"HL_LOCK_EXT_ID\", \"HL_LOCK_INT_ID\", \"HL_TXNID\", \"HL_DB\", \"HL_TABLE\", \"HL_PARTITION\", " +
      "\"HL_LOCK_STATE\", \"HL_LOCK_TYPE\", \"HL_LAST_HEARTBEAT\", \"HL_USER\", \"HL_HOST\", \"HL_AGENT_INFO\") " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, %s, ?, ?, ?)";
  private static final String TXN_COMPONENTS_INSERT_QUERY = "INSERT INTO \"TXN_COMPONENTS\" (" +
      "\"TC_TXNID\", \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\", \"TC_OPERATION_TYPE\", \"TC_WRITEID\")" +
      " VALUES (?, ?, ?, ?, ?, ?)";
  private static final String TXN_COMPONENTS_DP_DELETE_QUERY = "DELETE FROM \"TXN_COMPONENTS\" " +
      "WHERE \"TC_TXNID\" = ? AND \"TC_DATABASE\" = ? AND \"TC_TABLE\" = ? AND \"TC_PARTITION\" IS NULL";
  private static final String INCREMENT_NEXT_LOCK_ID_QUERY = "UPDATE \"NEXT_LOCK_ID\" SET \"NL_NEXT\" = %s";
  private static final String UPDATE_HIVE_LOCKS_EXT_ID_QUERY = "UPDATE \"HIVE_LOCKS\" SET \"HL_LOCK_EXT_ID\" = %s " +
      "WHERE \"HL_LOCK_EXT_ID\" = %s";
  private static final String SELECT_WRITE_ID_QUERY = "SELECT \"T2W_WRITEID\" FROM \"TXN_TO_WRITE_ID\" WHERE" +
      " \"T2W_DATABASE\" = ? AND \"T2W_TABLE\" = ? AND \"T2W_TXNID\" = ?";
  private static final String COMPL_TXN_COMPONENTS_INSERT_QUERY = "INSERT INTO \"COMPLETED_TXN_COMPONENTS\" " +
      "(\"CTC_TXNID\"," + " \"CTC_DATABASE\", \"CTC_TABLE\", \"CTC_PARTITION\", \"CTC_WRITEID\", \"CTC_UPDATE_DELETE\")" +
      " VALUES (%s, ?, ?, ?, ?, %s)";
  private static final String TXNS_INSERT_QRY = "INSERT INTO \"TXNS\" " +
      "(\"TXN_STATE\", \"TXN_STARTED\", \"TXN_LAST_HEARTBEAT\", \"TXN_USER\", \"TXN_HOST\", \"TXN_TYPE\") " +
      "VALUES(?,%s,%s,?,?,?)";
  private static final String SELECT_LOCKS_FOR_LOCK_ID_QUERY = "SELECT \"HL_LOCK_EXT_ID\", \"HL_LOCK_INT_ID\", " +
      "\"HL_DB\", \"HL_TABLE\", \"HL_PARTITION\", \"HL_LOCK_STATE\", \"HL_LOCK_TYPE\", \"HL_TXNID\" " +
      "FROM \"HIVE_LOCKS\" WHERE \"HL_LOCK_EXT_ID\" = ?";
  private static final String SELECT_TIMED_OUT_LOCKS_QUERY = "SELECT DISTINCT \"HL_LOCK_EXT_ID\" FROM \"HIVE_LOCKS\" " +
      "WHERE \"HL_LAST_HEARTBEAT\" < %s - ? AND \"HL_TXNID\" = 0";
  private static final String TXN_TO_WRITE_ID_INSERT_QUERY = "INSERT INTO \"TXN_TO_WRITE_ID\" (\"T2W_TXNID\", " +
      "\"T2W_DATABASE\", \"T2W_TABLE\", \"T2W_WRITEID\") VALUES (?, ?, ?, ?)";
  private static final String SELECT_NWI_NEXT_FROM_NEXT_WRITE_ID =
      "SELECT \"NWI_NEXT\" FROM \"NEXT_WRITE_ID\" WHERE \"NWI_DATABASE\" = ? AND \"NWI_TABLE\" = ?";


  protected List<TransactionalMetaStoreEventListener> transactionalListeners;

  // Maximum number of open transactions that's allowed
  private static volatile int maxOpenTxns = 0;
  // Whether number of open transactions reaches the threshold
  private static volatile boolean tooManyOpenTxns = false;

  /**
   * Number of consecutive deadlocks we have seen
   */
  private int deadlockCnt;
  private long deadlockRetryInterval;
  protected Configuration conf;
  protected static DatabaseProduct dbProduct;
  protected static SQLGenerator sqlGenerator;
  private static long openTxnTimeOutMillis;

  // (End user) Transaction timeout, in milliseconds.
  private long timeout;
  // Timeout for opening a transaction

  private int maxBatchSize;
  private String identifierQuoteString; // quotes to use for quoting tables, where necessary
  private long retryInterval;
  private int retryLimit;
  private int retryNum;
  // Current number of open txns
  private AtomicInteger numOpenTxns;
  // Whether to use min_history_level table or not.
  // At startup we read it from the config, but set it to false if min_history_level does nto exists.
  static boolean useMinHistoryLevel;

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

    synchronized (TxnHandler.class) {
      if (connPool == null) {
        Connection dbConn = null;
        // Set up the JDBC connection pool
        try {
          int maxPoolSize = MetastoreConf.getIntVar(conf, ConfVars.CONNECTION_POOLING_MAX_CONNECTIONS);
          long getConnectionTimeoutMs = 30000;
          connPool = setupJdbcConnectionPool(conf, maxPoolSize, getConnectionTimeoutMs);
          /*the mutex pools should ideally be somewhat larger since some operations require 1
           connection from each pool and we want to avoid taking a connection from primary pool
           and then blocking because mutex pool is empty.  There is only 1 thread in any HMS trying
           to mutex on each MUTEX_KEY except MUTEX_KEY.CheckLock.  The CheckLock operation gets a
           connection from connPool first, then connPoolMutex.  All others, go in the opposite
           order (not very elegant...).  So number of connection requests for connPoolMutex cannot
           exceed (size of connPool + MUTEX_KEY.values().length - 1).*/
          connPoolMutex = setupJdbcConnectionPool(conf, maxPoolSize + MUTEX_KEY.values().length, getConnectionTimeoutMs);
          dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
          determineDatabaseProduct(dbConn);
          sqlGenerator = new SQLGenerator(dbProduct, conf);
        } catch (SQLException e) {
          String msg = "Unable to instantiate JDBC connection pooling, " + e.getMessage();
          LOG.error(msg);
          throw new RuntimeException(e);
        } finally {
          closeDbConn(dbConn);
        }
      }
    }

    numOpenTxns = Metrics.getOrCreateGauge(MetricsConstants.NUM_OPEN_TXNS);

    timeout = MetastoreConf.getTimeVar(conf, ConfVars.TXN_TIMEOUT, TimeUnit.MILLISECONDS);
    retryInterval = MetastoreConf.getTimeVar(conf, ConfVars.HMS_HANDLER_INTERVAL,
        TimeUnit.MILLISECONDS);
    retryLimit = MetastoreConf.getIntVar(conf, ConfVars.HMS_HANDLER_ATTEMPTS);
    deadlockRetryInterval = retryInterval / 10;
    maxOpenTxns = MetastoreConf.getIntVar(conf, ConfVars.MAX_OPEN_TXNS);
    maxBatchSize = MetastoreConf.getIntVar(conf, ConfVars.JDBC_MAX_BATCH_SIZE);

    openTxnTimeOutMillis = MetastoreConf.getTimeVar(conf, ConfVars.TXN_OPENTXN_TIMEOUT, TimeUnit.MILLISECONDS);

    try {
      boolean minHistoryConfig = MetastoreConf.getBoolVar(conf, ConfVars.TXN_USE_MIN_HISTORY_LEVEL);
      // override the config if table does not exists anymore
      // this helps to roll out his feature when multiple HMS is accessing the same backend DB
      useMinHistoryLevel = checkMinHistoryLevelTable(minHistoryConfig);
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
  }

  /**
   * Check if min_history_level table is usable
   * @return
   * @throws MetaException
   */
  private boolean checkMinHistoryLevelTable(boolean configValue) throws MetaException {
    if (!configValue) {
      // don't check it if disabled
      return false;
    }
    Connection dbConn = null;
    boolean tableExists = true;
    try {
      dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
      try (Statement stmt = dbConn.createStatement()) {
        // Dummy query to see if table exists
        try (ResultSet rs = stmt.executeQuery("SELECT 1 FROM \"MIN_HISTORY_LEVEL\"")) {
          rs.next();
        }
      }
      dbConn.rollback();
    } catch (SQLException e) {
      rollbackDBConn(dbConn);
      LOG.debug("Catching sql exception in min history level check", e);
      if (dbProduct.isTableNotExistsError(e)) {
        tableExists = false;
      } else {
        throw new MetaException(
            "Unable to select from transaction database: " + getMessage(e) + StringUtils.stringifyException(e));
      }
    } finally {
      closeDbConn(dbConn);
    }
    return tableExists;
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
    try {
      // We need to figure out the HighWaterMark and the list of open transactions.
      Connection dbConn = null;
      Statement stmt = null;
      ResultSet rs = null;
      try {
        /*
         * This method need guarantees from
         * {@link #openTxns(OpenTxnRequest)} and  {@link #commitTxn(CommitTxnRequest)}.
         * It will look at the TXNS table and find each transaction between the max(txn_id) as HighWaterMark
         * and the max(txn_id) before the TXN_OPENTXN_TIMEOUT period as LowWaterMark.
         * Every transaction that is not found between these will be considered as open, since it may appear later.
         * openTxns must ensure, that no new transaction will be opened with txn_id below LWM and
         * commitTxn must ensure, that no committed transaction will be removed before the time period expires.
         */
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        List<OpenTxn> txnInfos = new ArrayList<>();
        String txnsQuery = String.format(infoFields ? OpenTxn.OPEN_TXNS_INFO_QUERY : OpenTxn.OPEN_TXNS_QUERY,
            TxnDbUtil.getEpochFn(dbProduct));
        LOG.debug("Going to execute query<" + txnsQuery + ">");
        rs = stmt.executeQuery(txnsQuery);
        /*
         * We can use the maximum txn_id from the TXNS table as high water mark, since the commitTxn and the Initiator
         * guarantees, that the transaction with the highest txn_id will never be removed from the TXNS table.
         * If there is a pending openTxns, that is already acquired it's sequenceId but not yet committed the insert
         * into the TXNS table, will have either a lower txn_id than HWM and will be listed in the openTxn list,
         * or will have a higher txn_id and don't effect this getOpenTxns() call.
         */
        long hwm = 0;
        long openTxnLowBoundary = 0;

        while (rs.next()) {
          long txnId = rs.getLong(1);
          long age = rs.getLong(4);
          hwm = txnId;
          if (age < getOpenTxnTimeOutMillis()) {
            // We will consider every gap as an open transaction from the previous txnId
            openTxnLowBoundary++;
            while (txnId > openTxnLowBoundary) {
              // Add an empty open transaction for every missing value
              txnInfos.add(new OpenTxn(openTxnLowBoundary, TxnStatus.OPEN, TxnType.DEFAULT));
              LOG.debug("Open transaction added for missing value in TXNS {}",
                  JavaUtils.txnIdToString(openTxnLowBoundary));
              openTxnLowBoundary++;
            }
          } else {
            openTxnLowBoundary = txnId;
          }
          TxnStatus state = TxnStatus.fromString(rs.getString(2));
          if (state == TxnStatus.COMMITTED) {
            // This is only here, to avoid adding this txnId as possible gap
            continue;
          }
          OpenTxn txnInfo = new OpenTxn(txnId, state, TxnType.findByValue(rs.getInt(3)));
          if (infoFields) {
            txnInfo.setUser(rs.getString(5));
            txnInfo.setHost(rs.getString(6));
            txnInfo.setStartedTime(rs.getLong(7));
            txnInfo.setLastHeartBeatTime(rs.getLong(8));
          }
          txnInfos.add(txnInfo);
        }
        dbConn.rollback();
        LOG.debug("Got OpenTxnList with hwm: {} and openTxnList size {}.", hwm, txnInfos.size());
        return new OpenTxnList(hwm, txnInfos);
      } catch (SQLException e) {
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "getOpenTxnsList");
        throw new MetaException(
            "Unable to select from transaction database: " + getMessage(e) + StringUtils.stringifyException(e));
      } finally {
        close(rs, stmt, dbConn);
      }
    } catch (RetryException e) {
      return getOpenTxnsList(infoFields);
    }
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
        LOG.warn("Maximum allowed number of open transactions (" + maxOpenTxns + ") has been " +
            "reached. Current number of open transactions: " + numOpenTxns);
        throw new MetaException("Maximum allowed number of open transactions has been reached. " +
            "See hive.max.open.txns.");
      }
    }

    int numTxns = rqst.getNum_txns();
    if (numTxns <= 0) {
      throw new MetaException("Invalid input for number of txns: " + numTxns);
    }

    try {
      Connection dbConn = null;
      Statement stmt = null;
      try {
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
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        /*
         * The openTxn and commitTxn must be mutexed, when committing a not read only transaction.
         * This is achieved by requesting a shared table lock here, and an exclusive one at commit.
         * Since table locks are working in Derby, we don't need the lockInternal call here.
         * Example: Suppose we have two transactions with update like x = x+1.
         * We have T[3,3] that was using a value from a snapshot with T[2,2]. If we allow committing T[3,3]
         * and opening T[4] parallel it is possible, that T[4] will be using the value from a snapshot with T[2,2],
         * and we will have a lost update problem
         */
        acquireTxnLock(stmt, true);
        // Measure the time from acquiring the sequence value, till committing in the TXNS table
        StopWatch generateTransactionWatch = new StopWatch();
        generateTransactionWatch.start();

        List<Long> txnIds = openTxns(dbConn, rqst);

        LOG.debug("Going to commit");
        dbConn.commit();
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
          deleteInvalidOpenTransactions(dbConn, txnIds);
          dbConn.commit();
          /*
           * We do not throw RetryException directly, to not circumvent the max retry limit
           */
          throw new SQLException("OpenTxnTimeOut exceeded", MANUAL_RETRY);
        }
        return new OpenTxnsResponse(txnIds);
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "openTxns(" + rqst + ")");
        throw new MetaException("Unable to select from transaction database " + StringUtils.stringifyException(e));
      } finally {
        close(null, stmt, dbConn);
      }
    } catch (RetryException e) {
      return openTxns(rqst);
    }
  }

  private List<Long> openTxns(Connection dbConn, OpenTxnRequest rqst)
          throws SQLException, MetaException {
    int numTxns = rqst.getNum_txns();
    // Make sure the user has not requested an insane amount of txns.
    int maxTxns = MetastoreConf.getIntVar(conf, ConfVars.TXN_MAX_OPEN_BATCH);
    if (numTxns > maxTxns) {
      numTxns = maxTxns;
    }
    List<PreparedStatement> insertPreparedStmts = null;
    TxnType txnType = rqst.isSetTxn_type() ? rqst.getTxn_type() : TxnType.DEFAULT;
    try {
      if (rqst.isSetReplPolicy()) {
        List<Long> targetTxnIdList = getTargetTxnIdList(rqst.getReplPolicy(), rqst.getReplSrcTxnIds(), dbConn);

        if (!targetTxnIdList.isEmpty()) {
          if (targetTxnIdList.size() != rqst.getReplSrcTxnIds().size()) {
            LOG.warn("target txn id number " + targetTxnIdList.toString() +
                    " is not matching with source txn id number " + rqst.getReplSrcTxnIds().toString());
          }
          LOG.info("Target transactions " + targetTxnIdList.toString() + " are present for repl policy :" +
                  rqst.getReplPolicy() + " and Source transaction id : " + rqst.getReplSrcTxnIds().toString());
          return targetTxnIdList;
        }
        txnType = TxnType.REPL_CREATED;
      }

      long minOpenTxnId = 0;
      if (useMinHistoryLevel) {
        minOpenTxnId = getMinOpenTxnIdWaterMark(dbConn);
      }

      List<Long> txnIds = new ArrayList<>(numTxns);
      /*
       * The getGeneratedKeys are not supported in every dbms, after executing a multi line insert.
       * But it is supported in every used dbms for single line insert, even if the metadata says otherwise.
       * If the getGeneratedKeys are not supported first we insert a random batchId in the TXN_META_INFO field,
       * then the keys are selected beck with that batchid.
       */
      boolean genKeySupport = dbProduct.supportsGetGeneratedKeys();
      genKeySupport = genKeySupport || (numTxns == 1);

      String insertQuery = String.format(TXNS_INSERT_QRY, TxnDbUtil.getEpochFn(dbProduct),
          TxnDbUtil.getEpochFn(dbProduct));
      LOG.debug("Going to execute insert <" + insertQuery + ">");
      try (PreparedStatement ps = dbConn.prepareStatement(insertQuery, new String[] {"TXN_ID"})) {
        String state = genKeySupport ? TxnStatus.OPEN.getSqlConst() : TXN_TMP_STATE;
        if (numTxns == 1) {
          ps.setString(1, state);
          ps.setString(2, rqst.getUser());
          ps.setString(3, rqst.getHostname());
          ps.setInt(4, txnType.getValue());
          txnIds.addAll(executeTxnInsertBatchAndExtractGeneratedKeys(dbConn, genKeySupport, ps, false));
        } else {
          for (int i = 0; i < numTxns; ++i) {
            ps.setString(1, state);
            ps.setString(2, rqst.getUser());
            ps.setString(3, rqst.getHostname());
            ps.setInt(4, txnType.getValue());
            ps.addBatch();

            if ((i + 1) % maxBatchSize == 0) {
              txnIds.addAll(executeTxnInsertBatchAndExtractGeneratedKeys(dbConn, genKeySupport, ps, true));
            }
          }
          if (numTxns % maxBatchSize != 0) {
            txnIds.addAll(executeTxnInsertBatchAndExtractGeneratedKeys(dbConn, genKeySupport, ps, true));
          }
        }
      }

      assert txnIds.size() == numTxns;

      addTxnToMinHistoryLevel(dbConn, txnIds, minOpenTxnId);

      if (rqst.isSetReplPolicy()) {
        List<String> rowsRepl = new ArrayList<>(numTxns);
        List<String> params = Collections.singletonList(rqst.getReplPolicy());
        List<List<String>> paramsList = new ArrayList<>(numTxns);
        for (int i = 0; i < numTxns; i++) {
          rowsRepl.add("?," + rqst.getReplSrcTxnIds().get(i) + "," + txnIds.get(i));
          paramsList.add(params);
        }

        insertPreparedStmts = sqlGenerator.createInsertValuesPreparedStmt(dbConn,
                "\"REPL_TXN_MAP\" (\"RTM_REPL_POLICY\", \"RTM_SRC_TXN_ID\", \"RTM_TARGET_TXN_ID\")", rowsRepl,
                paramsList);
        for (PreparedStatement pst : insertPreparedStmts) {
          pst.execute();
        }
      }

      if (transactionalListeners != null) {
        MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners,
                EventMessage.EventType.OPEN_TXN, new OpenTxnEvent(txnIds, txnType), dbConn, sqlGenerator);
      }
      return txnIds;
    } finally {
      if (insertPreparedStmts != null) {
        for (PreparedStatement pst : insertPreparedStmts) {
          pst.close();
        }
      }
    }
  }

  private List<Long> executeTxnInsertBatchAndExtractGeneratedKeys(Connection dbConn, boolean genKeySupport,
      PreparedStatement ps, boolean batch) throws SQLException {
    List<Long> txnIds = new ArrayList<>();
    if (batch) {
      ps.executeBatch();
    } else {
      // For slight performance advantage we do not use the executeBatch, when we only have one row
      ps.execute();
    }
    if (genKeySupport) {
      try (ResultSet generatedKeys = ps.getGeneratedKeys()) {
        while (generatedKeys.next()) {
          txnIds.add(generatedKeys.getLong(1));
        }
      }
    } else {
      try (PreparedStatement pstmt =
          dbConn.prepareStatement("SELECT \"TXN_ID\" FROM \"TXNS\" WHERE \"TXN_STATE\" = ?")) {
        pstmt.setString(1, TXN_TMP_STATE);
        try (ResultSet rs = pstmt.executeQuery()) {
          while (rs.next()) {
            txnIds.add(rs.getLong(1));
          }
        }
      }
      try (PreparedStatement pstmt = dbConn
          .prepareStatement("UPDATE \"TXNS\" SET \"TXN_STATE\" = ? WHERE \"TXN_STATE\" = ?")) {
        pstmt.setString(1, TxnStatus.OPEN.getSqlConst());
        pstmt.setString(2, TXN_TMP_STATE);
        pstmt.executeUpdate();
      }
    }
    return txnIds;
  }

  private void deleteInvalidOpenTransactions(Connection dbConn, List<Long> txnIds) throws MetaException {
    if (txnIds.size() == 0) {
      return;
    }
    try {
      Statement stmt = null;
      try {
        stmt = dbConn.createStatement();

        List<String> queries = new ArrayList<>();
        StringBuilder prefix = new StringBuilder();
        StringBuilder suffix = new StringBuilder();
        prefix.append("DELETE FROM \"TXNS\" WHERE ");
        TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix, txnIds, "\"TXN_ID\"", false, false);
        for (String s : queries) {
          LOG.debug("Going to execute update <" + s + ">");
          stmt.executeUpdate(s);
        }
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "deleteInvalidOpenTransactions(" + txnIds + ")");
        throw new MetaException("Unable to select from transaction database " + StringUtils.stringifyException(e));
      } finally {
        closeStmt(stmt);
      }
    } catch (RetryException ex) {
      deleteInvalidOpenTransactions(dbConn, txnIds);
    }
  }

  @Override
  public long getOpenTxnTimeOutMillis() {
    return openTxnTimeOutMillis;
  }

  @Override
  public void setOpenTxnTimeOutMillis(long openTxnTimeOutMillis) {
    this.openTxnTimeOutMillis = openTxnTimeOutMillis;
  }

  protected long getOpenTxnTimeoutLowBoundaryTxnId(Connection dbConn) throws MetaException, SQLException {
    long maxTxnId;
    String s =
        "SELECT MAX(\"TXN_ID\") FROM \"TXNS\" WHERE \"TXN_STARTED\" < (" + TxnDbUtil.getEpochFn(dbProduct) + " - "
            + openTxnTimeOutMillis + ")";
    try (Statement stmt = dbConn.createStatement()) {
      LOG.debug("Going to execute query <" + s + ">");
      try (ResultSet maxTxnIdRs = stmt.executeQuery(s)) {
        maxTxnIdRs.next();
        maxTxnId = maxTxnIdRs.getLong(1);
        if (maxTxnIdRs.wasNull()) {
          /*
           * TXNS always contains at least one transaction,
           * the row where txnid = (select max(txnid) where txn_started < epoch - TXN_OPENTXN_TIMEOUT) is never deleted
           */
          throw new MetaException("Transaction tables not properly " + "initialized, null record found in MAX(TXN_ID)");
        }
      }
    }
    return maxTxnId;
  }

  private long getHighWaterMark(Statement stmt) throws SQLException, MetaException {
    String s = "SELECT MAX(\"TXN_ID\") FROM \"TXNS\"";
    LOG.debug("Going to execute query <" + s + ">");
    long maxOpenTxnId;
    try (ResultSet maxOpenTxnIdRs = stmt.executeQuery(s)) {
      maxOpenTxnIdRs.next();
      maxOpenTxnId = maxOpenTxnIdRs.getLong(1);
      if (maxOpenTxnIdRs.wasNull()) {
        /*
         * TXNS always contains at least one transaction,
         * the row where txnid = (select max(txnid) where txn_started < epoch - TXN_OPENTXN_TIMEOUT) is never deleted
         */
        throw new MetaException("Transaction tables not properly " + "initialized, null record found in MAX(TXN_ID)");
      }
    }
    return maxOpenTxnId;
  }

  private List<Long> getTargetTxnIdList(String replPolicy, List<Long> sourceTxnIdList, Connection dbConn)
          throws SQLException {
    PreparedStatement pst = null;
    ResultSet rs = null;
    try {
      List<String> inQueries = new ArrayList<>();
      StringBuilder prefix = new StringBuilder();
      StringBuilder suffix = new StringBuilder();
      List<Long> targetTxnIdList = new ArrayList<>();
      prefix.append("SELECT \"RTM_TARGET_TXN_ID\" FROM \"REPL_TXN_MAP\" WHERE ");
      suffix.append(" AND \"RTM_REPL_POLICY\" = ?");
      TxnUtils.buildQueryWithINClause(conf, inQueries, prefix, suffix, sourceTxnIdList,
              "\"RTM_SRC_TXN_ID\"", false, false);
      List<String> params = Arrays.asList(replPolicy);
      for (String query : inQueries) {
        LOG.debug("Going to execute select <" + query.replaceAll("\\?", "{}") + ">", quoteString(replPolicy));
        pst = sqlGenerator.prepareStmtWithParameters(dbConn, query, params);
        rs = pst.executeQuery();
        while (rs.next()) {
          targetTxnIdList.add(rs.getLong(1));
        }
        closeStmt(pst);
      }
      LOG.debug("targetTxnid for srcTxnId " + sourceTxnIdList.toString() + " is " + targetTxnIdList.toString());
      return targetTxnIdList;
    } catch (SQLException e) {
      LOG.warn("failed to get target txn ids " + e.getMessage());
      throw e;
    } finally {
      closeStmt(pst);
      close(rs);
    }
  }

  @Override
  @RetrySemantics.Idempotent
  public long getTargetTxnId(String replPolicy, long sourceTxnId) throws MetaException {
    try {
      Connection dbConn = null;
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        List<Long> targetTxnIds = getTargetTxnIdList(replPolicy, Collections.singletonList(sourceTxnId), dbConn);
        if (targetTxnIds.isEmpty()) {
          LOG.info("Txn {} not present for repl policy {}", sourceTxnId, replPolicy);
          return -1;
        }
        assert (targetTxnIds.size() == 1);
        return targetTxnIds.get(0);
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "getTargetTxnId(" + replPolicy + sourceTxnId + ")");
        throw new MetaException("Unable to get target transaction id "
                + StringUtils.stringifyException(e));
      } finally {
        closeDbConn(dbConn);
        unlockInternal();
      }
    } catch (RetryException e) {
      return getTargetTxnId(replPolicy, sourceTxnId);
    }
  }

  private void deleteReplTxnMapEntry(Connection dbConn, long sourceTxnId, String replPolicy) throws SQLException {
    String s = "DELETE FROM \"REPL_TXN_MAP\" WHERE \"RTM_SRC_TXN_ID\" = " + sourceTxnId + " AND \"RTM_REPL_POLICY\" = ?";
    try (PreparedStatement pst = sqlGenerator.prepareStmtWithParameters(dbConn, s, Arrays.asList(replPolicy))) {
      LOG.info("Going to execute  <" + s.replaceAll("\\?", "{}") + ">", quoteString(replPolicy));
      pst.executeUpdate();
    }
  }

  @Override
  @RetrySemantics.Idempotent
  public void abortTxn(AbortTxnRequest rqst) throws NoSuchTxnException, MetaException, TxnAbortedException {
    long txnid = rqst.getTxnid();
    long sourceTxnId = -1;
    try {
      Connection dbConn = null;
      Statement stmt = null;
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();

        if (rqst.isSetReplPolicy()) {
          sourceTxnId = rqst.getTxnid();
          List<Long> targetTxnIds = getTargetTxnIdList(rqst.getReplPolicy(),
                  Collections.singletonList(sourceTxnId), dbConn);
          if (targetTxnIds.isEmpty()) {
            // Idempotent case where txn was already closed or abort txn event received without
            // corresponding open txn event.
            LOG.info("Target txn id is missing for source txn id : " + sourceTxnId +
                    " and repl policy " + rqst.getReplPolicy());
            return;
          }
          assert targetTxnIds.size() == 1;
          txnid = targetTxnIds.get(0);
        }

        Optional<TxnType> txnType = getOpenTxnTypeAndLock(stmt, txnid);
        if (!txnType.isPresent()) {
          TxnStatus status = findTxnState(txnid, stmt);
          if (status == TxnStatus.ABORTED) {
            if (rqst.isSetReplPolicy()) {
              // in case of replication, idempotent is taken care by getTargetTxnId
              LOG.warn("Invalid state ABORTED for transactions started using replication replay task");
              deleteReplTxnMapEntry(dbConn, sourceTxnId, rqst.getReplPolicy());
            }
            LOG.info("abortTxn(" + JavaUtils.txnIdToString(txnid) +
              ") requested by it is already " + TxnStatus.ABORTED);
            return;
          }
          raiseTxnUnexpectedState(status, txnid);
        }
        abortTxns(dbConn, Collections.singletonList(txnid), true);

        if (rqst.isSetReplPolicy()) {
          deleteReplTxnMapEntry(dbConn, sourceTxnId, rqst.getReplPolicy());
        }

        if (transactionalListeners != null) {
          MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners,
                  EventMessage.EventType.ABORT_TXN, new AbortTxnEvent(txnid, txnType.get()), dbConn, sqlGenerator);
        }

        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "abortTxn(" + rqst + ")");
        throw new MetaException("Unable to update transaction database "
          + StringUtils.stringifyException(e));
      } finally {
        close(null, stmt, dbConn);
        unlockInternal();
      }
    } catch (RetryException e) {
      abortTxn(rqst);
    }
  }

  @Override
  @RetrySemantics.Idempotent
  public void abortTxns(AbortTxnsRequest rqst) throws MetaException {
    List<Long> txnIds = rqst.getTxn_ids();
    try {
      Connection dbConn = null;
      Statement stmt = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();

        List<String> queries = new ArrayList<>();
        StringBuilder prefix =
            new StringBuilder("SELECT \"TXN_ID\", \"TXN_TYPE\" from \"TXNS\" where \"TXN_STATE\" = ")
                .append(TxnStatus.OPEN)
                .append(" and \"TXN_TYPE\" != ").append(TxnType.READ_ONLY.getValue()).append(" and ");

        TxnUtils.buildQueryWithINClause(conf, queries, prefix, new StringBuilder(),
            txnIds, "\"TXN_ID\"", false, false);

        Map<Long, TxnType> nonReadOnlyTxns = new HashMap<>();
        for (String query : queries) {
          LOG.debug("Going to execute query<" + query + ">");
          try (ResultSet rs = stmt.executeQuery(sqlGenerator.addForUpdateClause(query))) {
            while (rs.next()) {
              TxnType txnType = TxnType.findByValue(rs.getInt(2));
              nonReadOnlyTxns.put(rs.getLong(1), txnType);
            }
          }
        }
        int numAborted = abortTxns(dbConn, txnIds, false);
        if (numAborted != txnIds.size()) {
          LOG.warn("Abort Transactions command only aborted " + numAborted + " out of " +
              txnIds.size() + " transactions. It's possible that the other " +
              (txnIds.size() - numAborted) +
              " transactions have been aborted or committed, or the transaction ids are invalid.");
        }

        if (transactionalListeners != null){
          for (Long txnId : txnIds) {
            MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners,
                    EventMessage.EventType.ABORT_TXN, new AbortTxnEvent(txnId,
                nonReadOnlyTxns.getOrDefault(txnId, TxnType.READ_ONLY)), dbConn, sqlGenerator);
          }
        }
        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "abortTxns(" + rqst + ")");
        throw new MetaException("Unable to update transaction database "
            + StringUtils.stringifyException(e));
      } finally {
        closeStmt(stmt);
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      abortTxns(rqst);
    }
  }

  private void updateReplId(Connection dbConn, ReplLastIdInfo replLastIdInfo) throws SQLException, MetaException {
    PreparedStatement pst = null;
    PreparedStatement pstInt = null;
    ResultSet rs = null;
    ResultSet prs = null;
    Statement stmt = null;
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

      String query = "select \"DB_ID\" from \"DBS\" where \"NAME\" = ?  and \"CTLG_NAME\" = ?";
      List<String> params = Arrays.asList(db, catalog);
      pst = sqlGenerator.prepareStmtWithParameters(dbConn, query, params);
      LOG.debug("Going to execute query <" + query.replaceAll("\\?", "{}") + ">",
              quoteString(db), quoteString(catalog));
      rs = pst.executeQuery();
      if (!rs.next()) {
        throw new MetaException("DB with name " + db + " does not exist in catalog " + catalog);
      }
      long dbId = rs.getLong(1);
      rs.close();
      pst.close();

      // not used select for update as it will be updated by single thread only from repl load
      rs = stmt.executeQuery("SELECT \"PARAM_VALUE\" FROM \"DATABASE_PARAMS\" WHERE \"PARAM_KEY\" = " +
              "'repl.last.id' AND \"DB_ID\" = " + dbId);
      if (!rs.next()) {
        query = "INSERT INTO \"DATABASE_PARAMS\" VALUES ( " + dbId + " , 'repl.last.id' , ? )";
      } else {
        query = "UPDATE \"DATABASE_PARAMS\" SET \"PARAM_VALUE\" = ? WHERE \"DB_ID\" = " + dbId +
                " AND \"PARAM_KEY\" = 'repl.last.id'";
      }
      close(rs);
      params = Arrays.asList(lastReplId);
      pst = sqlGenerator.prepareStmtWithParameters(dbConn, query, params);
      LOG.debug("Updating repl id for db <" + query.replaceAll("\\?", "{}") + ">", lastReplId);
      if (pst.executeUpdate() != 1) {
        //only one row insert or update should happen
        throw new RuntimeException("DATABASE_PARAMS is corrupted for db " + db);
      }
      pst.close();

      if (table == null) {
        // if only database last repl id to be updated.
        return;
      }

      query = "SELECT \"TBL_ID\" FROM \"TBLS\" WHERE \"TBL_NAME\" = ? AND \"DB_ID\" = " + dbId;
      params = Arrays.asList(table);
      pst = sqlGenerator.prepareStmtWithParameters(dbConn, query, params);
      LOG.debug("Going to execute query <" + query.replaceAll("\\?", "{}") + ">", quoteString(table));

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
      LOG.debug("Updating repl id for table <" + query.replaceAll("\\?", "{}") + ">", lastReplId);
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

        LOG.debug("Going to execute query " + query + " with partitions " +
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
          LOG.debug("Updating repl id for part <" + query.replaceAll("\\?", "{}") + ">", lastReplId);
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
   * Suppose we have multi-statment transactions T and S both of which are attempting x = x + 1
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
  public void commitTxn(CommitTxnRequest rqst)
    throws NoSuchTxnException, TxnAbortedException, MetaException {
    char isUpdateDelete = 'N';
    long txnid = rqst.getTxnid();
    long sourceTxnId = -1;

    try {
      Connection dbConn = null;
      Statement stmt = null;
      Long commitId = null;
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();

        if (rqst.isSetReplLastIdInfo()) {
          updateReplId(dbConn, rqst.getReplLastIdInfo());
        }

        if (rqst.isSetReplPolicy()) {
          sourceTxnId = rqst.getTxnid();
          List<Long> targetTxnIds = getTargetTxnIdList(rqst.getReplPolicy(),
                  Collections.singletonList(sourceTxnId), dbConn);
          if (targetTxnIds.isEmpty()) {
            // Idempotent case where txn was already closed or commit txn event received without
            // corresponding open txn event.
            LOG.info("Target txn id is missing for source txn id : " + sourceTxnId +
                    " and repl policy " + rqst.getReplPolicy());
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
        Optional<TxnType> txnType = getOpenTxnTypeAndLock(stmt, txnid);
        if (!txnType.isPresent()) {
          //if here, txn was not found (in expected state)
          TxnStatus actualTxnStatus = findTxnState(txnid, stmt);
          if (actualTxnStatus == TxnStatus.COMMITTED) {
            if (rqst.isSetReplPolicy()) {
              // in case of replication, idempotent is taken care by getTargetTxnId
              LOG.warn("Invalid state COMMITTED for transactions started using replication replay task");
            }
            /**
             * This makes the operation idempotent
             * (assume that this is most likely due to retry logic)
             */
            LOG.info("Nth commitTxn(" + JavaUtils.txnIdToString(txnid) + ") msg");
            return;
          }
          raiseTxnUnexpectedState(actualTxnStatus, txnid);
        }

        String conflictSQLSuffix = "FROM \"TXN_COMPONENTS\" WHERE \"TC_TXNID\"=" + txnid + " AND \"TC_OPERATION_TYPE\" IN (" +
                OperationType.UPDATE + "," + OperationType.DELETE + ")";

        long tempCommitId = generateTemporaryId();
        if (txnType.get() != TxnType.READ_ONLY
                && !rqst.isSetReplPolicy()
                && isUpdateOrDelete(stmt, conflictSQLSuffix)) {

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
          Savepoint undoWriteSetForCurrentTxn = dbConn.setSavepoint();
          stmt.executeUpdate("INSERT INTO \"WRITE_SET\" (\"WS_DATABASE\", \"WS_TABLE\", \"WS_PARTITION\", \"WS_TXNID\", \"WS_COMMIT_ID\", \"WS_OPERATION_TYPE\")" +
                          " SELECT DISTINCT \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\", \"TC_TXNID\", " + tempCommitId + ", \"TC_OPERATION_TYPE\" " + conflictSQLSuffix);

          /**
           * This S4U will mutex with other commitTxn() and openTxns().
           * -1 below makes txn intervals look like [3,3] [4,4] if all txns are serial
           * Note: it's possible to have several txns have the same commit id.  Suppose 3 txns start
           * at the same time and no new txns start until all 3 commit.
           * We could've incremented the sequence for commitId as well but it doesn't add anything functionally.
           */
          acquireTxnLock(stmt, false);
          commitId = getHighWaterMark(stmt);

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
                dbConn.rollback(undoWriteSetForCurrentTxn);
                LOG.info(msg);
                //todo: should make abortTxns() write something into TXNS.TXN_META_INFO about this
                if (abortTxns(dbConn, Collections.singletonList(txnid), false) != 1) {
                  throw new IllegalStateException(msg + " FAILED!");
                }
                dbConn.commit();
                throw new TxnAbortedException(msg);
              }
            }
          }
        } else if (txnType.get() == TxnType.COMPACTION) {
          acquireTxnLock(stmt, false);
          commitId = getHighWaterMark(stmt);
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

        if (txnType.get() != TxnType.READ_ONLY && !rqst.isSetReplPolicy()) {
          moveTxnComponentsToCompleted(stmt, txnid, isUpdateDelete);
        } else if (rqst.isSetReplPolicy()) {
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
                  LOG.debug("Executing a batch of <" + sql + "> queries. Batch size: " + maxBatchSize);
                  pstmt.executeBatch();
                }
              }
              if (insertCounter % maxBatchSize != 0) {
                LOG.debug("Executing a batch of <" + sql + "> queries. Batch size: " + insertCounter % maxBatchSize);
                pstmt.executeBatch();
              }
            }
          }
          deleteReplTxnMapEntry(dbConn, sourceTxnId, rqst.getReplPolicy());
        }
        updateWSCommitIdAndCleanUpMetadata(stmt, txnid, txnType.get(), commitId, tempCommitId);
        removeCommittedTxnFromMinHistoryLevel(dbConn, txnid);
        if (rqst.isSetKeyValue()) {
          updateKeyValueAssociatedWithTxn(rqst, stmt);
        }

        createCommitNotificationEvent(dbConn, txnid , txnType);

        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "commitTxn(" + rqst + ")");
        throw new MetaException("Unable to update transaction database "
          + StringUtils.stringifyException(e));
      } finally {
        close(null, stmt, dbConn);
        unlockInternal();
      }
    } catch (RetryException e) {
      commitTxn(rqst);
    }
  }

  /**
   * Create Notifiaction Events on txn commit
   * @param dbConn DatabaseConnection
   * @param txnid committed txn
   * @param txnType transaction type
   * @throws MetaException ex
   */
  protected void createCommitNotificationEvent(Connection dbConn, long txnid, Optional<TxnType> txnType)
      throws MetaException, SQLException {
    if (transactionalListeners != null) {
      MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners,
              EventMessage.EventType.COMMIT_TXN, new CommitTxnEvent(txnid, txnType.get()), dbConn, sqlGenerator);
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
  public long getLatestTxnIdInConflict(long txnid) throws MetaException {
    Connection dbConn = null;
    Statement stmt = null;

    try {
      dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
      stmt = dbConn.createStatement();

      String writeConflictQuery = "SELECT MAX(\"COMMITTED\".\"WS_TXNID\")" +
        " FROM \"WRITE_SET\" \"COMMITTED\" " +
        "   INNER JOIN (" +
        "SELECT DISTINCT \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\", \"TC_TXNID\" " +
        " FROM \"TXN_COMPONENTS\"  " +
        "   WHERE \"TC_TXNID\" = " + txnid +
        "     AND \"TC_OPERATION_TYPE\" IN (" + OperationType.UPDATE + "," + OperationType.DELETE + ")) \"CUR\" " +
        "   ON \"COMMITTED\".\"WS_DATABASE\" = \"CUR\".\"TC_DATABASE\" " +
        "     AND \"COMMITTED\".\"WS_TABLE\" = \"CUR\".\"TC_TABLE\" " +
        //For partitioned table we always track writes at partition level (never at table)
        //and for non partitioned - always at table level, thus the same table should never
        //have entries with partition key and w/o
        "     AND (\"COMMITTED\".\"WS_PARTITION\" = \"CUR\".\"TC_PARTITION\" OR " +
        "       \"CUR\".\"TC_PARTITION\" IS NULL) " +
        " WHERE \"CUR\".\"TC_TXNID\" <= \"COMMITTED\".\"WS_COMMIT_ID\""; //txns overlap

      LOG.debug("Going to execute query: <" + writeConflictQuery + ">");
      ResultSet rs = stmt.executeQuery(writeConflictQuery);
      return rs.next() ? rs.getLong(1) : -1;

    } catch (Exception e) {
      throw new MetaException(StringUtils.stringifyException(e));

    } finally {
      closeStmt(stmt);
      closeDbConn(dbConn);
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
    LOG.debug("Going to execute query: <" + writeConflictQuery + ">");
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
    LOG.debug("Going to execute insert <" + s + ">");

    if ((stmt.executeUpdate(s)) < 1) {
      //this can be reasonable for an empty txn START/COMMIT or read-only txn
      //also an IUD with DP that didn't match any rows.
      LOG.info("Expected to move at least one record from txn_components to " +
              "completed_txn_components when committing txn! " + JavaUtils.txnIdToString(txnid));
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
    LOG.debug("Going to execute update <" + s + ">");
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

    // Get the abortedWriteIds which are already sorted in ascending order.
    List<Long> abortedWriteIds = getAbortedWriteIds(validWriteIdList);
    int numAbortedWrites = abortedWriteIds.size();
    try {
      Connection dbConn = null;
      Statement stmt = null;
      PreparedStatement pStmt = null;
      List<PreparedStatement> insertPreparedStmts = null;
      ResultSet rs = null;
      List<String> params = Arrays.asList(dbName, tblName);
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();

        // Check if this txn state is already replicated for this given table. If yes, then it is
        // idempotent case and just return.
        String sql = "SELECT \"NWI_NEXT\" FROM \"NEXT_WRITE_ID\" WHERE \"NWI_DATABASE\" = ? AND \"NWI_TABLE\" = ?";
        pStmt = sqlGenerator.prepareStmtWithParameters(dbConn, sql, params);
        LOG.debug("Going to execute query <" + sql.replaceAll("\\?", "{}") + ">",
                quoteString(dbName), quoteString(tblName));
        rs = pStmt.executeQuery();
        if (rs.next()) {
          LOG.info("Idempotent flow: WriteId state <" + validWriteIdList + "> is already applied for the table: "
                  + dbName + "." + tblName);
          rollbackDBConn(dbConn);
          return;
        }

        if (numAbortedWrites > 0) {
          // Allocate/Map one txn per aborted writeId and abort the txn to mark writeid as aborted.
          // We don't use the txnLock, all of these transactions will be aborted in this one rdbm transaction
          // So they will not effect the commitTxn in any way
          List<Long> txnIds = openTxns(dbConn,
                  new OpenTxnRequest(numAbortedWrites, rqst.getUser(), rqst.getHostName()));
          assert(numAbortedWrites == txnIds.size());

          // Map each aborted write id with each allocated txn.
          List<String> rows = new ArrayList<>();
          List<List<String>> paramsList = new ArrayList<>();
          int i = 0;
          for (long txn : txnIds) {
            long writeId = abortedWriteIds.get(i++);
            rows.add(txn + ", ?, ?, " + writeId);
            paramsList.add(params);
            LOG.info("Allocated writeID: " + writeId + " for txnId: " + txn);
          }

          // Insert entries to TXN_TO_WRITE_ID for aborted write ids
          insertPreparedStmts = sqlGenerator.createInsertValuesPreparedStmt(dbConn,
                  "\"TXN_TO_WRITE_ID\" (\"T2W_TXNID\", \"T2W_DATABASE\", \"T2W_TABLE\", \"T2W_WRITEID\")", rows,
                  paramsList);
          for (PreparedStatement pst : insertPreparedStmts) {
            pst.execute();
          }

          // Abort all the allocated txns so that the mapped write ids are referred as aborted ones.
          int numAborts = abortTxns(dbConn, txnIds, false);
          assert(numAborts == numAbortedWrites);
        }

        // There are some txns in the list which has no write id allocated and hence go ahead and do it.
        // Get the next write id for the given table and update it with new next write id.
        // It is expected NEXT_WRITE_ID doesn't have entry for this table and hence directly insert it.
        long nextWriteId = validWriteIdList.getHighWatermark() + 1;

        // First allocation of write id (hwm+1) should add the table to the next_write_id meta table.
        sql = "INSERT INTO \"NEXT_WRITE_ID\" (\"NWI_DATABASE\", \"NWI_TABLE\", \"NWI_NEXT\") VALUES (?, ?, "
                + Long.toString(nextWriteId) + ")";
        closeStmt(pStmt);
        pStmt = sqlGenerator.prepareStmtWithParameters(dbConn, sql, params);
        LOG.debug("Going to execute insert <" + sql.replaceAll("\\?", "{}") + ">",
                quoteString(dbName), quoteString(tblName));
        pStmt.execute();

        LOG.info("WriteId state <" + validWriteIdList + "> is applied for the table: " + dbName + "." + tblName);
        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "replTableWriteIdState(" + rqst + ")", true);
        throw new MetaException("Unable to update transaction database "
                + StringUtils.stringifyException(e));
      } finally {
        if (insertPreparedStmts != null) {
          for (PreparedStatement pst : insertPreparedStmts) {
            closeStmt(pst);
          }
        }
        closeStmt(pStmt);
        close(rs, stmt, dbConn);
        unlockInternal();
      }
    } catch (RetryException e) {
      replTableWriteIdState(rqst);
    }

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

  private ValidTxnList getValidTxnList(Connection dbConn, String fullTableName, Long writeId) throws MetaException,
          SQLException {
    PreparedStatement pst = null;
    ResultSet rs = null;
    try {
      String[] names = TxnUtils.getDbTableName(fullTableName);
      assert names.length == 2;
      List<String> params = Arrays.asList(names[0], names[1]);
      String s =
          "SELECT \"T2W_TXNID\" FROM \"TXN_TO_WRITE_ID\" WHERE \"T2W_DATABASE\" = ? AND "
              + "\"T2W_TABLE\" = ? AND \"T2W_WRITEID\" = "+ writeId;
      pst = sqlGenerator.prepareStmtWithParameters(dbConn, s, params);
      LOG.debug("Going to execute query <" + s.replaceAll("\\?", "{}") + ">", quoteString(names[0]),
              quoteString(names[1]));
      rs = pst.executeQuery();
      if (rs.next()) {
        return TxnCommonUtils.createValidReadTxnList(getOpenTxns(), rs.getLong(1));
      }
      throw new MetaException("invalid write id " + writeId + " for table " + fullTableName);
    } finally {
      close(rs, pst, null);
    }
  }

  @Override
  @RetrySemantics.ReadOnly
  public GetValidWriteIdsResponse getValidWriteIds(GetValidWriteIdsRequest rqst) throws MetaException {
    try {
      Connection dbConn = null;
      ValidTxnList validTxnList;

      try {
        /**
         * This runs at READ_COMMITTED for exactly the same reason as {@link #getOpenTxnsInfo()}
         */
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);

        // We should prepare the valid write ids list based on validTxnList of current txn.
        // If no txn exists in the caller, then they would pass null for validTxnList and so it is
        // required to get the current state of txns to make validTxnList
        if (rqst.isSetValidTxnList()) {
          assert rqst.isSetWriteId() == false;
          validTxnList = new ValidReadTxnList(rqst.getValidTxnList());
        } else if (rqst.isSetWriteId()) {
          validTxnList = getValidTxnList(dbConn, rqst.getFullTableNames().get(0), rqst.getWriteId());
        } else {
          // Passing 0 for currentTxn means, this validTxnList is not wrt to any txn
          validTxnList = TxnCommonUtils.createValidReadTxnList(getOpenTxns(), 0);
        }

        // Get the valid write id list for all the tables read by the current txn
        List<TableValidWriteIds> tblValidWriteIdsList = new ArrayList<>();
        for (String fullTableName : rqst.getFullTableNames()) {
          tblValidWriteIdsList.add(getValidWriteIdsForTable(dbConn, fullTableName, validTxnList));
        }

        LOG.debug("Going to rollback");
        dbConn.rollback();
        GetValidWriteIdsResponse owr = new GetValidWriteIdsResponse(tblValidWriteIdsList);
        return owr;
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "getValidWriteIds");
        throw new MetaException("Unable to select from transaction database, "
                + StringUtils.stringifyException(e));
      } finally {
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      return getValidWriteIds(rqst);
    }
  }

  // Method to get the Valid write ids list for the given table
  // Input fullTableName is expected to be of format <db_name>.<table_name>
  private TableValidWriteIds getValidWriteIdsForTable(Connection dbConn, String fullTableName,
                                               ValidTxnList validTxnList) throws SQLException {
    PreparedStatement pst = null;
    ResultSet rs = null;
    String[] names = TxnUtils.getDbTableName(fullTableName);
    assert(names.length == 2);
    List<String> params = Arrays.asList(names[0], names[1]);
    try {
      // Need to initialize to 0 to make sure if nobody modified this table, then current txn
      // shouldn't read any data.
      // If there is a conversion from non-acid to acid table, then by default 0 would be assigned as
      // writeId for data from non-acid table and so writeIdHwm=0 would ensure those data are readable by any txns.
      long writeIdHwm = 0;
      List<Long> invalidWriteIdList = new ArrayList<>();
      long minOpenWriteId = Long.MAX_VALUE;
      BitSet abortedBits = new BitSet();
      long txnHwm = validTxnList.getHighWatermark();

      // Find the writeId high water mark based upon txnId high water mark. If found, then, need to
      // traverse through all write Ids less than writeId HWM to make exceptions list.
      // The writeHWM = min(NEXT_WRITE_ID.nwi_next-1, max(TXN_TO_WRITE_ID.t2w_writeid under txnHwm))
      String s = "SELECT MAX(\"T2W_WRITEID\") FROM \"TXN_TO_WRITE_ID\" WHERE \"T2W_TXNID\" <= " + Long.toString(txnHwm)
              + " AND \"T2W_DATABASE\" = ? AND \"T2W_TABLE\" = ?";
      pst = sqlGenerator.prepareStmtWithParameters(dbConn, s, params);
      LOG.debug("Going to execute query<" + s.replaceAll("\\?", "{}") + ">",
              quoteString(names[0]), quoteString(names[1]));
      rs = pst.executeQuery();
      if (rs.next()) {
        writeIdHwm = rs.getLong(1);
      }

      // If no writeIds allocated by txns under txnHwm, then find writeHwm from NEXT_WRITE_ID.
      if (writeIdHwm <= 0) {
        // Need to subtract 1 as nwi_next would be the next write id to be allocated but we need highest
        // allocated write id.
        s = "SELECT \"NWI_NEXT\"-1 FROM \"NEXT_WRITE_ID\" WHERE \"NWI_DATABASE\" = ? AND \"NWI_TABLE\" = ?";
        closeStmt(pst);
        pst = sqlGenerator.prepareStmtWithParameters(dbConn, s, params);
        LOG.debug("Going to execute query<" + s.replaceAll("\\?", "{}") + ">",
                quoteString(names[0]), quoteString(names[1]));
        rs = pst.executeQuery();
        if (rs.next()) {
          writeIdHwm = rs.getLong(1);
        }
      }
      boolean foundValidUncompactedWrite = false;
      // As writeIdHwm is known, query all writeIds under the writeId HWM.
      // If any writeId under HWM is allocated by txn > txnId HWM or belongs to open/aborted txns,
      // then will be added to invalid list. The results should be sorted in ascending order based
      // on write id. The sorting is needed as exceptions list in ValidWriteIdList would be looked-up
      // using binary search.
      s = "SELECT \"T2W_TXNID\", \"T2W_WRITEID\" FROM \"TXN_TO_WRITE_ID\" WHERE \"T2W_WRITEID\" <= " + Long.toString(writeIdHwm)
              + " AND \"T2W_DATABASE\" = ? AND \"T2W_TABLE\" = ? ORDER BY \"T2W_WRITEID\" ASC";
      closeStmt(pst);
      pst = sqlGenerator.prepareStmtWithParameters(dbConn, s, params);
      LOG.debug("Going to execute query<" + s.replaceAll("\\?", "{}") + ">",
              quoteString(names[0]), quoteString(names[1]));
      rs = pst.executeQuery();
      while (rs.next()) {
        long txnId = rs.getLong(1);
        long writeId = rs.getLong(2);
        if (validTxnList.isTxnValid(txnId)) {
          // Skip if the transaction under evaluation is already committed.
          foundValidUncompactedWrite = true;
          continue;
        }
        // The current txn is either in open or aborted state.
        // Mark the write ids state as per the txn state.
        invalidWriteIdList.add(writeId);
        if (validTxnList.isTxnAborted(txnId)) {
          abortedBits.set(invalidWriteIdList.size() - 1);
        } else {
          minOpenWriteId = Math.min(minOpenWriteId, writeId);
        }
      }
      // If we have compacted writes and some invalid writes on the table,
      // return the lowest invalid write as a writeIdHwm and set it as invalid.
      if (!foundValidUncompactedWrite) {
        long writeId = invalidWriteIdList.isEmpty() ? -1 : invalidWriteIdList.get(0);
        invalidWriteIdList = new ArrayList<>();
        abortedBits = new BitSet();

        if (writeId != -1) {
          invalidWriteIdList.add(writeId);
          writeIdHwm = writeId;
          if (writeId != minOpenWriteId) {
            abortedBits.set(0);
          }
        }
      }
      ByteBuffer byteBuffer = ByteBuffer.wrap(abortedBits.toByteArray());
      TableValidWriteIds owi = new TableValidWriteIds(fullTableName, writeIdHwm, invalidWriteIdList, byteBuffer);
      if (minOpenWriteId < Long.MAX_VALUE) {
        owi.setMinOpenWriteId(minOpenWriteId);
      }
      return owi;
    } finally {
      closeStmt(pst);
      close(rs);
    }
  }

  @Override
  @RetrySemantics.Idempotent
  public AllocateTableWriteIdsResponse allocateTableWriteIds(AllocateTableWriteIdsRequest rqst)
          throws MetaException {
    List<Long> txnIds;
    String dbName = rqst.getDbName().toLowerCase();
    String tblName = rqst.getTableName().toLowerCase();
    try {
      Connection dbConn = null;
      PreparedStatement pStmt = null;
      ResultSet rs = null;
      List<TxnToWriteId> txnToWriteIds = new ArrayList<>();
      List<TxnToWriteId> srcTxnToWriteIds = null;
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);

        if (rqst.isSetReplPolicy()) {
          srcTxnToWriteIds = rqst.getSrcTxnToWriteIdList();
          List<Long> srcTxnIds = new ArrayList<>();
          assert (rqst.isSetSrcTxnToWriteIdList());
          assert (!rqst.isSetTxnIds());
          assert (!srcTxnToWriteIds.isEmpty());

          for (TxnToWriteId txnToWriteId : srcTxnToWriteIds) {
            srcTxnIds.add(txnToWriteId.getTxnId());
          }
          txnIds = getTargetTxnIdList(rqst.getReplPolicy(), srcTxnIds, dbConn);
          if (srcTxnIds.size() != txnIds.size()) {
            // Idempotent case where txn was already closed but gets allocate write id event.
            // So, just ignore it and return empty list.
            LOG.info("Idempotent case: Target txn id is missing for source txn id : " + srcTxnIds.toString() +
                    " and repl policy " + rqst.getReplPolicy());
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

        // Traverse the TXN_TO_WRITE_ID to see if any of the input txns already have allocated a
        // write id for the same db.table. If yes, then need to reuse it else have to allocate new one
        // The write id would have been already allocated in case of multi-statement txns where
        // first write on a table will allocate write id and rest of the writes should re-use it.
        prefix.append("SELECT \"T2W_TXNID\", \"T2W_WRITEID\" FROM \"TXN_TO_WRITE_ID\" WHERE")
              .append(" \"T2W_DATABASE\" = ? AND \"T2W_TABLE\" = ? AND ");
        TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix,
                txnIds, "\"T2W_TXNID\"", false, false);

        long allocatedTxnsCount = 0;
        long writeId;
        List<String> params = Arrays.asList(dbName, tblName);
        for (String query : queries) {
          pStmt = sqlGenerator.prepareStmtWithParameters(dbConn, query, params);
          LOG.debug("Going to execute query <" + query.replaceAll("\\?", "{}") + ">",
                  quoteString(dbName), quoteString(tblName));
          rs = pStmt.executeQuery();
          while (rs.next()) {
            // If table write ID is already allocated for the given transaction, then just use it
            long txnId = rs.getLong(1);
            writeId = rs.getLong(2);
            txnToWriteIds.add(new TxnToWriteId(txnId, writeId));
            allocatedTxnsCount++;
            LOG.info("Reused already allocated writeID: " + writeId + " for txnId: " + txnId);
          }
          closeStmt(pStmt);
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
        LOG.debug("Going to execute query <" + s.replaceAll("\\?", "{}") + ">",
                quoteString(dbName), quoteString(tblName));
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
          LOG.debug("Going to execute insert <" + s.replaceAll("\\?", "{}") + ">",
                  quoteString(dbName), quoteString(tblName));
          pStmt.execute();
        } else {
          long nextWriteId = rs.getLong(1);
          writeId = (srcWriteId > 0) ? srcWriteId : nextWriteId;

          // Update the NEXT_WRITE_ID for the given table after incrementing by number of write ids allocated
          s = "UPDATE \"NEXT_WRITE_ID\" SET \"NWI_NEXT\" = " + (writeId + numOfWriteIds)
                  + " WHERE \"NWI_DATABASE\" = ? AND \"NWI_TABLE\" = ?";
          closeStmt(pStmt);
          pStmt = sqlGenerator.prepareStmtWithParameters(dbConn, s, params);
          LOG.debug("Going to execute update <" + s.replaceAll("\\?", "{}") + ">",
                  quoteString(dbName), quoteString(tblName));
          pStmt.executeUpdate();

          // For repl flow, if the source write id is mismatching with target next write id, then current
          // metadata in TXN_TO_WRITE_ID is stale for this table and hence need to clean-up TXN_TO_WRITE_ID.
          // This is possible in case of first incremental repl after bootstrap where concurrent write
          // and drop table was performed at source during bootstrap dump.
          if ((srcWriteId > 0) && (srcWriteId != nextWriteId)) {
            s = "DELETE FROM \"TXN_TO_WRITE_ID\" WHERE \"T2W_DATABASE\" = ? AND \"T2W_TABLE\" = ?";
            closeStmt(pStmt);
            pStmt = sqlGenerator.prepareStmtWithParameters(dbConn, s, params);
            LOG.debug("Going to execute delete <" + s.replaceAll("\\?", "{}") + ">",
                    quoteString(dbName), quoteString(tblName));
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
            LOG.info("Allocated writeId: " + writeId + " for txnId: " + txnId);
            writeId++;
            if (txnToWriteIds.size() % maxBatchSize == 0) {
              LOG.debug("Executing a batch of <" + TXN_TO_WRITE_ID_INSERT_QUERY + "> queries. " +
                  "Batch size: " + maxBatchSize);
              pstmt.executeBatch();
            }
          }
          if (txnToWriteIds.size() % maxBatchSize != 0) {
            LOG.debug("Executing a batch of <" + TXN_TO_WRITE_ID_INSERT_QUERY + "> queries. " +
                "Batch size: " + txnToWriteIds.size() % maxBatchSize);
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
        dbConn.commit();
        return new AllocateTableWriteIdsResponse(txnToWriteIds);
      } catch (SQLException e) {
        LOG.error("Exception during write ids allocation for request={}. Will retry if possible.", rqst, e);
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "allocateTableWriteIds(" + rqst + ")", true);
        throw new MetaException("Unable to update transaction database "
                + StringUtils.stringifyException(e));
      } finally {
        close(rs, pStmt, dbConn);
        unlockInternal();
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
        LOG.debug("Going to execute query <" + SELECT_NWI_NEXT_FROM_NEXT_WRITE_ID.replaceAll("\\?", "{}") + ">",
            quoteString(dbName), quoteString(tableName));
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
        checkRetryable(dbConn, e, "getMaxAllocatedTableWrited(" + rqst + ")");
        throw new MetaException("Unable to update transaction database " + StringUtils.stringifyException(e));
      } finally {
        close(rs, pStmt, dbConn);
      }
    } catch (RetryException e) {
      return getMaxAllocatedTableWrited(rqst);
    }
  }

  @Override
  public void seedWriteId(SeedTableWriteIdsRequest rqst)
      throws MetaException {
    try {
      Connection dbConn = null;
      PreparedStatement pst = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);

        //since this is on conversion from non-acid to acid, NEXT_WRITE_ID should not have an entry
        //for this table.  It also has a unique index in case 'should not' is violated

        // First allocation of write id should add the table to the next_write_id meta table
        // The initial value for write id should be 1 and hence we add 1 with number of write ids
        // allocated here
        String s = "INSERT INTO \"NEXT_WRITE_ID\" (\"NWI_DATABASE\", \"NWI_TABLE\", \"NWI_NEXT\") VALUES (?, ?, "
                + Long.toString(rqst.getSeedWriteId() + 1) + ")";
        pst = sqlGenerator.prepareStmtWithParameters(dbConn, s, Arrays.asList(rqst.getDbName(), rqst.getTableName()));
        LOG.debug("Going to execute insert <" + s.replaceAll("\\?", "{}") + ">",
                quoteString(rqst.getDbName()), quoteString(rqst.getTableName()));
        pst.execute();
        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "seedWriteId(" + rqst + ")");
        throw new MetaException("Unable to update transaction database " + StringUtils.stringifyException(e));
      } finally {
        close(null, pst, dbConn);
      }
    } catch (RetryException e) {
      seedWriteId(rqst);
    }
  }

  @Override
  public void seedTxnId(SeedTxnIdRequest rqst) throws MetaException {
    try {
      Connection dbConn = null;
      Statement stmt = null;
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        /*
         * Locking the txnLock an exclusive way, we do not want to set the txnId backward accidentally
         * if there are concurrent open transactions
         */
        acquireTxnLock(stmt, false);
        long highWaterMark = getHighWaterMark(stmt);
        if (highWaterMark >= rqst.getSeedTxnId()) {
          throw new MetaException(MessageFormat
              .format("Invalid txnId seed {}, the highWaterMark is {}", rqst.getSeedTxnId(), highWaterMark));
        }
        TxnDbUtil.seedTxnSequence(dbConn, conf, stmt, rqst.getSeedTxnId());
        dbConn.commit();

      } catch (SQLException e) {
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "seedTxnId(" + rqst + ")");
        throw new MetaException("Unable to update transaction database " + StringUtils.stringifyException(e));
      } finally {
        close(null, stmt, dbConn);
        unlockInternal();
      }
    } catch (RetryException e) {
      seedTxnId(rqst);
    }
  }

  @Override
  @RetrySemantics.Idempotent
  public void addWriteNotificationLog(AcidWriteEvent acidWriteEvent)
          throws MetaException {
    Connection dbConn = null;
    try {
      try {
        //Idempotent case is handled by notify Event
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners,
                EventMessage.EventType.ACID_WRITE, acidWriteEvent, dbConn, sqlGenerator);
        LOG.debug("Going to commit");
        dbConn.commit();
        return;
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        if (isDuplicateKeyError(e)) {
          // in case of key duplicate error, retry as it might be because of race condition
          if (waitForRetry("addWriteNotificationLog(" + acidWriteEvent + ")", e.getMessage())) {
            throw new RetryException();
          }
          retryNum = 0;
          throw new MetaException(e.getMessage());
        }
        checkRetryable(dbConn, e, "addWriteNotificationLog(" + acidWriteEvent + ")");
        throw new MetaException("Unable to add write notification event " + StringUtils.stringifyException(e));
      } finally{
        closeDbConn(dbConn);
        unlockInternal();
      }
    } catch (RetryException e) {
      addWriteNotificationLog(acidWriteEvent);
    }
  }

  @Override
  @RetrySemantics.SafeToRetry
  public void performWriteSetGC() throws MetaException {
    Connection dbConn = null;
    Statement stmt = null;
    try {
      dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
      stmt = dbConn.createStatement();
      long commitHighWaterMark = getMinOpenTxnIdWaterMark(dbConn);
      int delCnt = stmt.executeUpdate("DELETE FROM \"WRITE_SET\" WHERE \"WS_COMMIT_ID\" < " + commitHighWaterMark);
      LOG.info("Deleted {} obsolete rows from WRITE_SET", delCnt);
      dbConn.commit();
    } catch (SQLException ex) {
      LOG.warn("WriteSet GC failed due to " + getMessage(ex), ex);
    } finally {
      close(null, stmt, dbConn);
    }
  }

  protected long getMinOpenTxnIdWaterMark(Connection dbConn) throws MetaException, SQLException {
    /**
     * We try to find the highest transactionId below everything was committed or aborted.
     * For that we look for the lowest open transaction in the TXNS and the TxnMinTimeout boundary,
     * because it is guaranteed there won't be open transactions below that.
     */
    long minOpenTxn;
    try (Statement stmt = dbConn.createStatement()) {
      try (ResultSet rs = stmt
          .executeQuery("SELECT MIN(\"TXN_ID\") FROM \"TXNS\" WHERE \"TXN_STATE\"=" + TxnStatus.OPEN)) {
        if (!rs.next()) {
          throw new IllegalStateException("Scalar query returned no rows?!?!!");
        }
        minOpenTxn = rs.getLong(1);
        if (rs.wasNull()) {
          minOpenTxn = Long.MAX_VALUE;
        }
      }
    }
    long lowWaterMark = getOpenTxnTimeoutLowBoundaryTxnId(dbConn);

    LOG.debug("MinOpenTxnIdWaterMark calculated with minOpenTxn {}, lowWaterMark {}", minOpenTxn, lowWaterMark);
    return Long.min(minOpenTxn, lowWaterMark + 1);
  }

  /**
   * Get invalidation info for the materialization. Currently, the materialization information
   * only contains information about whether there was update/delete operations on the source
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
    Connection dbConn = null;
    PreparedStatement pst = null;
    ResultSet rs = null;
    try {
      dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);

      // Parse validReaderWriteIdList from creation metadata
      final ValidTxnWriteIdList validReaderWriteIdList =
          new ValidTxnWriteIdList(creationMetadata.getValidTxnList());

      // Parse validTxnList
      final ValidReadTxnList currentValidTxnList = new ValidReadTxnList(validTxnListStr);
      // Get the valid write id list for the tables in current state
      final List<TableValidWriteIds> currentTblValidWriteIdsList = new ArrayList<>();
      for (String fullTableName : creationMetadata.getTablesUsed()) {
        currentTblValidWriteIdsList.add(getValidWriteIdsForTable(dbConn, fullTableName, currentValidTxnList));
      }
      final ValidTxnWriteIdList currentValidReaderWriteIdList = TxnCommonUtils.createValidTxnWriteIdList(
          currentValidTxnList.getHighWatermark(), currentTblValidWriteIdsList);

      List<String> params = new ArrayList<>();
      StringBuilder query = new StringBuilder();
      // compose a query that select transactions containing an update...
      query.append("SELECT \"CTC_UPDATE_DELETE\" FROM \"COMPLETED_TXN_COMPONENTS\" WHERE \"CTC_UPDATE_DELETE\" ='Y' AND (");
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
          query.append("OR");
        }
        String[] names = TxnUtils.getDbTableName(fullyQualifiedName);
        assert(names.length == 2);
        query.append(" (\"CTC_DATABASE\"=? AND \"CTC_TABLE\"=?");
        params.add(names[0]);
        params.add(names[1]);
        query.append(" AND (\"CTC_WRITEID\" > " + tblValidWriteIdList.getHighWatermark());
        query.append(tblValidWriteIdList.getInvalidWriteIds().length == 0 ? ") " :
            " OR \"CTC_WRITEID\" IN(" + StringUtils.join(",",
                Arrays.asList(ArrayUtils.toObject(tblValidWriteIdList.getInvalidWriteIds()))) + ") ");
        query.append(") ");
        i++;
      }
      // ... and where the transaction has already been committed as per snapshot taken
      // when we are running current query
      query.append(") AND \"CTC_TXNID\" <= " + currentValidTxnList.getHighWatermark());
      query.append(currentValidTxnList.getInvalidTransactions().length == 0 ? " " :
          " AND \"CTC_TXNID\" NOT IN(" + StringUtils.join(",",
              Arrays.asList(ArrayUtils.toObject(currentValidTxnList.getInvalidTransactions()))) + ") ");

      // Execute query
      String s = query.toString();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to execute query <" + s + ">");
      }
      pst = sqlGenerator.prepareStmtWithParameters(dbConn, s, params);
      pst.setMaxRows(1);
      rs = pst.executeQuery();

      return new Materialization(rs.next());
    } catch (SQLException ex) {
      LOG.warn("getMaterializationInvalidationInfo failed due to " + getMessage(ex), ex);
      throw new MetaException("Unable to retrieve materialization invalidation information due to " +
          StringUtils.stringifyException(ex));
    } finally {
      close(rs, pst, dbConn);
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
      LOG.debug("Going to execute query <" + selectQ.replaceAll("\\?", "{}") + ">",
              quoteString(dbName), quoteString(tableName));
      rs = pst.executeQuery();
      if(rs.next()) {
        LOG.info("Ignoring request to rebuild " + dbName + "/" + tableName +
            " since it is already being rebuilt");
        return new LockResponse(txnId, LockState.NOT_ACQUIRED);
      }
      String insertQ = "INSERT INTO \"MATERIALIZATION_REBUILD_LOCKS\" " +
          "(\"MRL_TXN_ID\", \"MRL_DB_NAME\", \"MRL_TBL_NAME\", \"MRL_LAST_HEARTBEAT\") VALUES (" + txnId +
          ", ?, ?, " + Instant.now().toEpochMilli() + ")";
      closeStmt(pst);
      pst = sqlGenerator.prepareStmtWithParameters(dbConn, insertQ, params);
      LOG.debug("Going to execute update <" + insertQ.replaceAll("\\?", "{}") + ">",
              quoteString(dbName), quoteString(tableName));
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
  public boolean heartbeatLockMaterializationRebuild(String dbName, String tableName, long txnId)
      throws MetaException {
    try {
      Connection dbConn = null;
      PreparedStatement pst = null;
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        String s = "UPDATE \"MATERIALIZATION_REBUILD_LOCKS\"" +
            " SET \"MRL_LAST_HEARTBEAT\" = " + Instant.now().toEpochMilli() +
            " WHERE \"MRL_TXN_ID\" = " + txnId +
            " AND \"MRL_DB_NAME\" = ?" +
            " AND \"MRL_TBL_NAME\" = ?";
        pst = sqlGenerator.prepareStmtWithParameters(dbConn, s, Arrays.asList(dbName, tableName));
        LOG.debug("Going to execute update <" + s.replaceAll("\\?", "{}") + ">",
                quoteString(dbName), quoteString(tableName));
        int rc = pst.executeUpdate();
        if (rc < 1) {
          LOG.debug("Going to rollback");
          dbConn.rollback();
          LOG.info("No lock found for rebuild of " + TableName.getDbTable(dbName, tableName) +
              " when trying to heartbeat");
          // It could not be renewed, return that information
          return false;
        }
        LOG.debug("Going to commit");
        dbConn.commit();
        // It could be renewed, return that information
        return true;
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e,
            "heartbeatLockMaterializationRebuild(" + TableName.getDbTable(dbName, tableName) + ", " + txnId + ")");
        throw new MetaException("Unable to heartbeat rebuild lock due to " +
            StringUtils.stringifyException(e));
      } finally {
        close(null, pst, dbConn);
        unlockInternal();
      }
    } catch (RetryException e) {
      return heartbeatLockMaterializationRebuild(dbName, tableName ,txnId);
    }
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
        LOG.debug("Going to execute query <" + selectQ + ">");
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
          LOG.debug("Going to execute update <" + deleteQ + ">");
          cnt = stmt.executeUpdate(deleteQ);
        }
        LOG.debug("Going to commit");
        dbConn.commit();
        return cnt;
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "cleanupMaterializationRebuildLocks");
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
    ConnectionLockIdPair connAndLockId = enqueueLockWithRetry(rqst);
    try {
      return checkLockWithRetry(connAndLockId.dbConn, connAndLockId.extLockId, rqst.getTxnid(),
          rqst.isZeroWaitReadEnabled());
    }
    catch(NoSuchLockException e) {
      // This should never happen, as we just added the lock id
      throw new MetaException("Couldn't find a lock we just created! " + e.getMessage());
    }
  }
  private static final class ConnectionLockIdPair {
    private final Connection dbConn;
    private final long extLockId;
    private ConnectionLockIdPair(Connection dbConn, long extLockId) {
      this.dbConn = dbConn;
      this.extLockId = extLockId;
    }
  }

  /**
   * Note that by definition select for update is divorced from update, i.e. you executeQuery() to read
   * and then executeUpdate().  One other alternative would be to actually update the row in TXNS but
   * to the same value as before thus forcing db to acquire write lock for duration of the transaction.
   *
   * SELECT ... FOR UPDATE locks the row until the transaction commits or rolls back.
   * Second connection using `SELECT ... FOR UPDATE` will suspend until the lock is released.
   * @return the txnType wrapped in an {@link Optional}
   * @throws SQLException
   * @throws MetaException
   */
  private Optional<TxnType> getOpenTxnTypeAndLock(Statement stmt, long txnId) throws SQLException, MetaException {
    String query = "SELECT \"TXN_TYPE\" FROM \"TXNS\" WHERE \"TXN_ID\" = " + txnId
        + " AND \"TXN_STATE\" = " + TxnStatus.OPEN;
    try (ResultSet rs = stmt.executeQuery(sqlGenerator.addForUpdateClause(query))) {
      return rs.next() ? Optional.ofNullable(
          TxnType.findByValue(rs.getInt(1))) : Optional.empty();
    }
  }

  /**
   * This enters locks into the queue in {@link #LOCK_WAITING} mode.
   *
   * Isolation Level Notes:
   * 1. We use S4U (withe read_committed) to generate the next (ext) lock id.  This serializes
   * any 2 {@code enqueueLockWithRetry()} calls.
   * 2. We use S4U on the relevant TXNS row to block any concurrent abort/commit/etc operations
   * @see #checkLockWithRetry(Connection, long, long, boolean)
   */
  private ConnectionLockIdPair enqueueLockWithRetry(LockRequest rqst)
      throws NoSuchTxnException, TxnAbortedException, MetaException {
    boolean success = false;
    Connection dbConn = null;
    try {
      Statement stmt = null;
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        long txnid = rqst.getTxnid();
        stmt = dbConn.createStatement();
        if (isValidTxn(txnid)) {
          //this also ensures that txn is still there in expected state
          Optional<TxnType> txnType = getOpenTxnTypeAndLock(stmt, txnid);
          if (!txnType.isPresent()) {
            ensureValidTxn(dbConn, txnid, stmt);
            shouldNeverHappen(txnid);
          }
        }
        /* Insert txn components and hive locks (with a temp extLockId) first, before getting the next lock ID in a select-for-update.
           This should minimize the scope of the S4U and decrease the table lock duration. */
        insertTxnComponents(txnid, rqst, dbConn);
        long tempExtLockId = insertHiveLocksWithTemporaryExtLockId(txnid, dbConn, rqst);

        /** Get the next lock id.
         * This has to be atomic with adding entries to HIVE_LOCK entries (1st add in W state) to prevent a race.
         * Suppose ID gen is a separate txn and 2 concurrent lock() methods are running.  1st one generates nl_next=7,
         * 2nd nl_next=8.  Then 8 goes first to insert into HIVE_LOCKS and acquires the locks.  Then 7 unblocks,
         * and add it's W locks but it won't see locks from 8 since to be 'fair' {@link #checkLock(java.sql.Connection, long)}
         * doesn't block on locks acquired later than one it's checking*/
        long extLockId = getNextLockIdForUpdate(dbConn, stmt);
        incrementLockIdAndUpdateHiveLocks(stmt, extLockId, tempExtLockId);

        dbConn.commit();
        success = true;
        return new ConnectionLockIdPair(dbConn, extLockId);
      } catch (SQLException e) {
        LOG.error("enqueueLock failed for request: {}. Exception msg: {}", rqst, getMessage(e));
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "enqueueLockWithRetry(" + rqst + ")");
        throw new MetaException("Unable to update transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        closeStmt(stmt);
        if (!success) {
          /* This needs to return a "live" connection to be used by operation that follows it.
          Thus it only closes Connection on failure/retry. */
          closeDbConn(dbConn);
        }
        unlockInternal();
      }
    }
    catch(RetryException e) {
      LOG.debug("Going to retry enqueueLock for request: {}, after catching RetryException with message: {}",
              rqst, e.getMessage());
      return enqueueLockWithRetry(rqst);
    }
  }

  private long getNextLockIdForUpdate(Connection dbConn, Statement stmt) throws SQLException, MetaException {
    String s = sqlGenerator.addForUpdateClause("SELECT \"NL_NEXT\" FROM \"NEXT_LOCK_ID\"");
    LOG.debug("Going to execute query <" + s + ">");
    try (ResultSet rs = stmt.executeQuery(s)) {
      if (!rs.next()) {
        LOG.error("Failure to get next lock ID for update! SELECT query returned empty ResultSet.");
        dbConn.rollback();
        throw new MetaException("Transaction tables not properly " +
                "initialized, no record found in next_lock_id");
      }
      return rs.getLong(1);
    }
  }

  private void incrementLockIdAndUpdateHiveLocks(Statement stmt, long extLockId, long tempId) throws SQLException {
    String incrCmd = String.format(INCREMENT_NEXT_LOCK_ID_QUERY, (extLockId + 1));
    // update hive locks entries with the real EXT_LOCK_ID (replace temp ID)
    String updateLocksCmd = String.format(UPDATE_HIVE_LOCKS_EXT_ID_QUERY, extLockId, tempId);
    LOG.debug("Going to execute updates in batch: <" + incrCmd + ">, and <" + updateLocksCmd + ">");
    stmt.addBatch(incrCmd);
    stmt.addBatch(updateLocksCmd);
    stmt.executeBatch();
  }

  private void insertTxnComponents(long txnid, LockRequest rqst, Connection dbConn) throws SQLException {
    if (txnid > 0) {
      Map<Pair<String, String>, Optional<Long>> writeIdCache = new HashMap<>();
      try (PreparedStatement pstmt = dbConn.prepareStatement(TXN_COMPONENTS_INSERT_QUERY)) {
        // For each component in this lock request,
        // add an entry to the txn_components table
        int insertCounter = 0;

        Predicate<LockComponent> isDynPart = lc -> lc.isSetIsDynamicPartitionWrite() && lc.isIsDynamicPartitionWrite();
        Function<LockComponent, Pair<String, String>> groupKey = lc ->
            Pair.of(normalizeCase(lc.getDbname()), normalizeCase(lc.getTablename()));

        Set<Pair<String, String>> isDynPartUpdate = rqst.getComponent().stream().filter(isDynPart)
          .filter(lc -> lc.getOperationType() == DataOperationType.UPDATE || lc.getOperationType() == DataOperationType.DELETE)
          .map(groupKey)
        .collect(Collectors.toSet());

        for (LockComponent lc : rqst.getComponent()) {
          if (lc.isSetIsTransactional() && !lc.isIsTransactional()) {
            //we don't prevent using non-acid resources in a txn but we do lock them
            continue;
          }
          if (!shouldUpdateTxnComponent(txnid, rqst, lc)) {
            continue;
          }
          String dbName = normalizeCase(lc.getDbname());
          String tblName = normalizeCase(lc.getTablename());
          String partName = normalizePartitionCase(lc.getPartitionname());
          OperationType opType = OperationType.fromDataOperationType(lc.getOperationType());

          if (isDynPart.test(lc)) {
            partName = null;
            if (writeIdCache.containsKey(groupKey.apply(lc))) {
              continue;
            }
            opType = isDynPartUpdate.contains(groupKey.apply(lc)) ? OperationType.UPDATE : OperationType.INSERT;
          }
          Optional<Long> writeId = getWriteId(writeIdCache, dbName, tblName, txnid, dbConn);

          pstmt.setLong(1, txnid);
          pstmt.setString(2, dbName);
          pstmt.setString(3, tblName);
          pstmt.setString(4, partName);
          pstmt.setString(5, opType.getSqlConst());
          pstmt.setObject(6, writeId.orElse(null));

          pstmt.addBatch();
          insertCounter++;
          if (insertCounter % maxBatchSize == 0) {
            LOG.debug("Executing a batch of <" + TXN_COMPONENTS_INSERT_QUERY + "> queries. Batch size: " + maxBatchSize);
            pstmt.executeBatch();
          }
        }
        if (insertCounter % maxBatchSize != 0) {
          LOG.debug("Executing a batch of <" + TXN_COMPONENTS_INSERT_QUERY + "> queries. Batch size: " + insertCounter % maxBatchSize);
          pstmt.executeBatch();
        }
      }
    }
  }

  private Optional<Long> getWriteId(Map<Pair<String, String>, Optional<Long>> writeIdCache, String dbName, String tblName, long txnid, Connection dbConn) throws SQLException {
    /* we can cache writeIDs based on dbName and tblName because txnid is invariant and
    partitionName is not part of the writeID select query */
    Pair<String, String> dbAndTable = Pair.of(dbName, tblName);
    if (writeIdCache.containsKey(dbAndTable)) {
      return writeIdCache.get(dbAndTable);
    } else {
      Optional<Long> writeId = getWriteIdFromDb(txnid, dbConn, dbName, tblName);
      writeIdCache.put(dbAndTable, writeId);
      return writeId;
    }
  }

  private Optional<Long> getWriteIdFromDb(long txnid, Connection dbConn, String dbName, String tblName) throws SQLException {
    if (tblName != null) {
      // It is assumed the caller have already allocated write id for adding/updating data to
      // the acid tables. However, DDL operatons won't allocate write id and hence this query
      // may return empty result sets.
      // Get the write id allocated by this txn for the given table writes
      try (PreparedStatement pstmt = dbConn.prepareStatement(SELECT_WRITE_ID_QUERY)) {
        pstmt.setString(1, dbName);
        pstmt.setString(2, tblName);
        pstmt.setLong(3, txnid);
        LOG.debug("Going to execute query <" + SELECT_WRITE_ID_QUERY + ">");
        try (ResultSet rs = pstmt.executeQuery()) {
          if (rs.next()) {
            return Optional.of(rs.getLong(1));
          }
        }
      }
    }
    return Optional.empty();
  }

  private boolean shouldUpdateTxnComponent(long txnid, LockRequest rqst, LockComponent lc) {
    if(!lc.isSetOperationType()) {
      //request came from old version of the client
      return true; //this matches old behavior
    }
    else {
      switch (lc.getOperationType()) {
        case INSERT:
        case UPDATE:
        case DELETE:
          return true;
        case SELECT:
          return false;
        case NO_TXN:
              /*this constant is a bit of a misnomer since we now always have a txn context.  It
               just means the operation is such that we don't care what tables/partitions it
               affected as it doesn't trigger a compaction or conflict detection.  A better name
               would be NON_TRANSACTIONAL.*/
          return false;
        default:
          //since we have an open transaction, only 4 values above are expected
          throw new IllegalStateException("Unexpected DataOperationType: " + lc.getOperationType()
                  + " agentInfo=" + rqst.getAgentInfo() + " " + JavaUtils.txnIdToString(txnid));
      }
    }
  }

  private long insertHiveLocksWithTemporaryExtLockId(long txnid, Connection dbConn, LockRequest rqst) throws MetaException, SQLException {

    String lastHB = isValidTxn(txnid) ? "0" : TxnDbUtil.getEpochFn(dbProduct);
    String insertLocksQuery = String.format(HIVE_LOCKS_INSERT_QRY, lastHB);
    long intLockId = 0;
    long tempExtLockId = generateTemporaryId();

    try (PreparedStatement pstmt = dbConn.prepareStatement(insertLocksQuery)) {
      for (LockComponent lc : rqst.getComponent()) {
        if (lc.isSetOperationType() && lc.getOperationType() == DataOperationType.UNSET &&
                (MetastoreConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST) || MetastoreConf.getBoolVar(conf, ConfVars.HIVE_IN_TEZ_TEST))) {
          //Old version of thrift client should have (lc.isSetOperationType() == false), but they do not
          //If you add a default value to a variable, isSet() for that variable is true regardless of the where the
          //message was created (for object variables).
          //It works correctly for boolean vars, e.g. LockComponent.isTransactional).
          //in test mode, upgrades are not tested, so client version and server version of thrift always matches so
          //we see UNSET here it means something didn't set the appropriate value.
          throw new IllegalStateException("Bug: operationType=" + lc.getOperationType() + " for component "
                  + lc + " agentInfo=" + rqst.getAgentInfo());
        }
        intLockId++;
        String lockType = LockTypeUtil.getEncodingAsStr(lc.getType());

        pstmt.setLong(1, tempExtLockId);
        pstmt.setLong(2, intLockId);
        pstmt.setLong(3, txnid);
        pstmt.setString(4, normalizeCase(lc.getDbname()));
        pstmt.setString(5, normalizeCase(lc.getTablename()));
        pstmt.setString(6, normalizePartitionCase(lc.getPartitionname()));
        pstmt.setString(7, Character.toString(LOCK_WAITING));
        pstmt.setString(8, lockType);
        pstmt.setString(9, rqst.getUser());
        pstmt.setString(10, rqst.getHostname());
        pstmt.setString(11, rqst.getAgentInfo());

        pstmt.addBatch();
        if (intLockId % maxBatchSize == 0) {
          LOG.debug("Executing a batch of <" + insertLocksQuery + "> queries. Batch size: " + maxBatchSize);
          pstmt.executeBatch();
        }
      }
      if (intLockId % maxBatchSize != 0) {
        LOG.debug("Executing a batch of <" + insertLocksQuery + "> queries. Batch size: " + intLockId % maxBatchSize);
        pstmt.executeBatch();
      }
    }
    return tempExtLockId;
  }

  private long generateTemporaryId() {
    return -1 * ThreadLocalRandom.current().nextLong();
  }

  private static String normalizeCase(String s) {
    return s == null ? null : s.toLowerCase();
  }

  private static String normalizePartitionCase(String s) {
    if (s == null) {
      return null;
    }
    Map<String, String> map = Splitter.on(Path.SEPARATOR).withKeyValueSeparator('=').split(s);
    return FileUtils.makePartName(new ArrayList<>(map.keySet()), new ArrayList<>(map.values()));
  }

  private LockResponse checkLockWithRetry(Connection dbConn, long extLockId, long txnId, boolean zeroWaitReadEnabled)
    throws NoSuchLockException, TxnAbortedException, MetaException {
    try {
      try {
        lockInternal();
        if(dbConn.isClosed()) {
          //should only get here if retrying this op
          dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        }
        return checkLock(dbConn, extLockId, txnId, zeroWaitReadEnabled);
      } catch (SQLException e) {
        LOG.error("checkLock failed for extLockId={}/txnId={}. Exception msg: {}", extLockId, txnId, getMessage(e));
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "checkLockWithRetry(" + extLockId + "," + txnId + ")");
        throw new MetaException("Unable to update transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        unlockInternal();
        closeDbConn(dbConn);
      }
    }
    catch(RetryException e) {
      LOG.debug("Going to retry checkLock for extLockId={}/txnId={} after catching RetryException with message: {}",
              extLockId, txnId, e.getMessage());
      return checkLockWithRetry(dbConn, extLockId, txnId, zeroWaitReadEnabled);
    }
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
   * {@link #checkLock(java.sql.Connection, long, long, boolean)}  must run at SERIALIZABLE
   * (make sure some lock we are checking against doesn't move from W to A in another txn)
   * but this method can heartbeat in separate txn at READ_COMMITTED.
   *
   * Retry-by-caller note:
   * Retryable because {@link #checkLock(Connection, long, long, boolean)} is
   */
  @Override
  @RetrySemantics.SafeToRetry
  public LockResponse checkLock(CheckLockRequest rqst)
    throws NoSuchTxnException, NoSuchLockException, TxnAbortedException, MetaException {
    try {
      Connection dbConn = null;
      long extLockId = rqst.getLockid();
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        // Heartbeat on the lockid first, to assure that our lock is still valid.
        // Then look up the lock info (hopefully in the cache).  If these locks
        // are associated with a transaction then heartbeat on that as well.
        LockInfo lockInfo = getLockFromLockId(dbConn, extLockId)
                .orElseThrow(() -> new NoSuchLockException("No such lock " + JavaUtils.lockIdToString(extLockId)));
        if (lockInfo.txnId > 0) {
          heartbeatTxn(dbConn, lockInfo.txnId);
        }
        else {
          heartbeatLock(dbConn, extLockId);
        }
        //todo: strictly speaking there is a bug here.  heartbeat*() commits but both heartbeat and
        //checkLock() are in the same retry block, so if checkLock() throws, heartbeat is also retired
        //extra heartbeat is logically harmless, but ...
        return checkLock(dbConn, extLockId, lockInfo.txnId, false);
      } catch (SQLException e) {
        LOG.error("checkLock failed for request={}. Exception msg: {}", rqst, getMessage(e));
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "checkLock(" + rqst + " )");
        throw new MetaException("Unable to update transaction database " +
          JavaUtils.lockIdToString(extLockId) + " " + StringUtils.stringifyException(e));
      } finally {
        closeDbConn(dbConn);
        unlockInternal();
      }
    } catch (RetryException e) {
      LOG.debug("Going to retry checkLock for request={} after catching RetryException with message: {}",
              rqst, e.getMessage());
      return checkLock(rqst);
    }

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
    try {
      Connection dbConn = null;
      Statement stmt = null;
      long extLockId = rqst.getLockid();
      try {
        /**
         * This method is logically like commit for read-only auto commit queries.
         * READ_COMMITTED since this only has 1 delete statement and no new entries with the
         * same hl_lock_ext_id can be added, i.e. all rows with a given hl_lock_ext_id are
         * created in a single atomic operation.
         * Theoretically, this competes with {@link #lock(org.apache.hadoop.hive.metastore.api.LockRequest)}
         * but hl_lock_ext_id is not known until that method returns.
         * Also competes with {@link #checkLock(org.apache.hadoop.hive.metastore.api.CheckLockRequest)}
         * but using SERIALIZABLE doesn't materially change the interaction.
         * If "delete" stmt misses, additional logic is best effort to produce meaningful error msg.
         */
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        //hl_txnid <> 0 means it's associated with a transaction
        String s = "DELETE FROM \"HIVE_LOCKS\" WHERE \"HL_LOCK_EXT_ID\" = " + extLockId + " AND (\"HL_TXNID\" = 0 OR" +
          " (\"HL_TXNID\" <> 0 AND \"HL_LOCK_STATE\" = '" + LOCK_WAITING + "'))";
        //(hl_txnid <> 0 AND hl_lock_state = '" + LOCK_WAITING + "') is for multi-statement txns where
        //some query attempted to lock (thus LOCK_WAITING state) but is giving up due to timeout for example
        LOG.debug("Going to execute update <" + s + ">");
        int rc = stmt.executeUpdate(s);
        if (rc < 1) {
          LOG.info("Failure to unlock any locks with extLockId={}.", extLockId);
          dbConn.rollback();
          Optional<LockInfo> optLockInfo = getLockFromLockId(dbConn, extLockId);
          if (!optLockInfo.isPresent()) {
            //didn't find any lock with extLockId but at ReadCommitted there is a possibility that
            //it existed when above delete ran but it didn't have the expected state.
            LOG.info("No lock in " + LOCK_WAITING + " mode found for unlock(" +
              JavaUtils.lockIdToString(rqst.getLockid()) + ")");
            //bail here to make the operation idempotent
            return;
          }
          LockInfo lockInfo = optLockInfo.get();
          if (isValidTxn(lockInfo.txnId)) {
            String msg = "Unlocking locks associated with transaction not permitted.  " + lockInfo;
            //if a lock is associated with a txn we can only "unlock" if if it's in WAITING state
            // which really means that the caller wants to give up waiting for the lock
            LOG.error(msg);
            throw new TxnOpenException(msg);
          } else {
            //we didn't see this lock when running DELETE stmt above but now it showed up
            //so should "should never happen" happened...
            String msg = "Found lock in unexpected state " + lockInfo;
            LOG.error(msg);
            throw new MetaException(msg);
          }
        }
        LOG.debug("Successfully unlocked at least 1 lock with extLockId={}", extLockId);
        dbConn.commit();
      } catch (SQLException e) {
        LOG.error("Unlock failed for request={}. Exception msg: {}", rqst, getMessage(e));
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "unlock(" + rqst + ")");
        throw new MetaException("Unable to update transaction database " +
          JavaUtils.lockIdToString(extLockId) + " " + StringUtils.stringifyException(e));
      } finally {
        closeStmt(stmt);
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      unlock(rqst);
    }
  }

  /**
   * used to sort entries in {@link org.apache.hadoop.hive.metastore.api.ShowLocksResponse}
   */
  private static class LockInfoExt extends LockInfo {
    private final ShowLocksResponseElement e;
    LockInfoExt(ShowLocksResponseElement e) {
      super(e);
      this.e = e;
    }
  }
  @RetrySemantics.ReadOnly
  public ShowLocksResponse showLocks(ShowLocksRequest rqst) throws MetaException {
    try {
      Connection dbConn = null;
      ShowLocksResponse rsp = new ShowLocksResponse();
      List<ShowLocksResponseElement> elems = new ArrayList<>();
      List<LockInfoExt> sortedList = new ArrayList<>();
      PreparedStatement pst = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);

        String s = "SELECT \"HL_LOCK_EXT_ID\", \"HL_TXNID\", \"HL_DB\", \"HL_TABLE\", \"HL_PARTITION\", \"HL_LOCK_STATE\", " +
          "\"HL_LOCK_TYPE\", \"HL_LAST_HEARTBEAT\", \"HL_ACQUIRED_AT\", \"HL_USER\", \"HL_HOST\", \"HL_LOCK_INT_ID\"," +
          "\"HL_BLOCKEDBY_EXT_ID\", \"HL_BLOCKEDBY_INT_ID\", \"HL_AGENT_INFO\" FROM \"HIVE_LOCKS\"";

        // Some filters may have been specified in the SHOW LOCKS statement. Add them to the query.
        String dbName = rqst.getDbname();
        String tableName = rqst.getTablename();
        String partName = rqst.getPartname();
        String txnId = rqst.isSetTxnid() ? String.valueOf(rqst.getTxnid()) : null;
        List<String> params = new ArrayList<>();

        StringBuilder filter = new StringBuilder();
        if (dbName != null && !dbName.isEmpty()) {
          filter.append("\"HL_DB\"=?");
          params.add(dbName);
        }
        if (tableName != null && !tableName.isEmpty()) {
          if (filter.length() > 0) {
            filter.append(" and ");
          }
          filter.append("\"HL_TABLE\"=?");
          params.add(tableName);
        }
        if (partName != null && !partName.isEmpty()) {
          if (filter.length() > 0) {
            filter.append(" and ");
          }
          filter.append("\"HL_PARTITION\"=?");
          params.add(partName);
        }
        if (txnId != null && !txnId.isEmpty()) {
          if (filter.length() > 0) {
            filter.append(" and ");
          }
          filter.append("\"HL_TXNID\"=?");
          params.add(txnId);
        }
        String whereClause = filter.toString();

        if (!whereClause.isEmpty()) {
          s = s + " where " + whereClause;
        }

        pst = sqlGenerator.prepareStmtWithParameters(dbConn, s, params);
        LOG.debug("Going to execute query <" + s + ">");
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
          ShowLocksResponseElement e = new ShowLocksResponseElement();
          e.setLockid(rs.getLong(1));
          long txnid = rs.getLong(2);
          if (!rs.wasNull()) e.setTxnid(txnid);
          e.setDbname(rs.getString(3));
          e.setTablename(rs.getString(4));
          String partition = rs.getString(5);
          if (partition != null) e.setPartname(partition);
          switch (rs.getString(6).charAt(0)) {
            case LOCK_ACQUIRED: e.setState(LockState.ACQUIRED); break;
            case LOCK_WAITING: e.setState(LockState.WAITING); break;
            default: throw new MetaException("Unknown lock state " + rs.getString(6).charAt(0));
          }

          char lockChar = rs.getString(7).charAt(0);
          LockType lockType = LockTypeUtil.getLockTypeFromEncoding(lockChar)
                  .orElseThrow(() -> new MetaException("Unknown lock type: " + lockChar));
          e.setType(lockType);

          e.setLastheartbeat(rs.getLong(8));
          long acquiredAt = rs.getLong(9);
          if (!rs.wasNull()) e.setAcquiredat(acquiredAt);
          e.setUser(rs.getString(10));
          e.setHostname(rs.getString(11));
          e.setLockIdInternal(rs.getLong(12));
          long id = rs.getLong(13);
          if(!rs.wasNull()) {
            e.setBlockedByExtId(id);
          }
          id = rs.getLong(14);
          if(!rs.wasNull()) {
            e.setBlockedByIntId(id);
          }
          e.setAgentInfo(rs.getString(15));
          sortedList.add(new LockInfoExt(e));
        }
        LOG.debug("Going to rollback");
        dbConn.rollback();
      } catch (SQLException e) {
        checkRetryable(dbConn, e, "showLocks(" + rqst + ")");
        throw new MetaException("Unable to select from transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        closeStmt(pst);
        closeDbConn(dbConn);
      }
      //this ensures that "SHOW LOCKS" prints the locks in the same order as they are examined
      //by checkLock() - makes diagnostics easier.
      Collections.sort(sortedList, new LockInfoComparator());
      for(LockInfoExt lockInfoExt : sortedList) {
        elems.add(lockInfoExt.e);
      }
      rsp.setLocks(elems);
      return rsp;
    } catch (RetryException e) {
      return showLocks(rqst);
    }
  }

  /**
   * {@code ids} should only have txnid or lockid but not both, ideally.
   * Currently DBTxnManager.heartbeat() enforces this.
   */
  @Override
  @RetrySemantics.SafeToRetry
  public void heartbeat(HeartbeatRequest ids)
    throws NoSuchTxnException,  NoSuchLockException, TxnAbortedException, MetaException {
    try {
      Connection dbConn = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        heartbeatLock(dbConn, ids.getLockid());
        heartbeatTxn(dbConn, ids.getTxnid());
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "heartbeat(" + ids + ")");
        throw new MetaException("Unable to select from transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      heartbeat(ids);
    }
  }
  @Override
  @RetrySemantics.SafeToRetry
  public HeartbeatTxnRangeResponse heartbeatTxnRange(HeartbeatTxnRangeRequest rqst)
    throws MetaException {
    try {
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
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        /*do fast path first (in 1 statement) if doesn't work, rollback and do the long version*/
        stmt = dbConn.createStatement();
        List<String> queries = new ArrayList<>();
        int numTxnsToHeartbeat = (int) (rqst.getMax() - rqst.getMin() + 1);
        List<Long> txnIds = new ArrayList<>(numTxnsToHeartbeat);
        for (long txn = rqst.getMin(); txn <= rqst.getMax(); txn++) {
          txnIds.add(txn);
        }
        TxnUtils.buildQueryWithINClause(conf, queries,
          new StringBuilder("UPDATE \"TXNS\" SET \"TXN_LAST_HEARTBEAT\" = " + TxnDbUtil.getEpochFn(dbProduct) +
            " WHERE \"TXN_STATE\" = " + TxnStatus.OPEN + " AND "),
          new StringBuilder(""), txnIds, "\"TXN_ID\"", true, false);
        int updateCnt = 0;
        for (String query : queries) {
          LOG.debug("Going to execute update <" + query + ">");
          updateCnt += stmt.executeUpdate(query);
        }
        if (updateCnt == numTxnsToHeartbeat) {
          //fast pass worked, i.e. all txns we were asked to heartbeat were Open as expected
          dbConn.commit();
          return rsp;
        }
        //if here, do the slow path so that we can return info txns which were not in expected state
        dbConn.rollback();
        for (long txn = rqst.getMin(); txn <= rqst.getMax(); txn++) {
          try {
            heartbeatTxn(dbConn, txn);
          } catch (NoSuchTxnException e) {
            nosuch.add(txn);
          } catch (TxnAbortedException e) {
            aborted.add(txn);
          }
        }
        return rsp;
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "heartbeatTxnRange(" + rqst + ")");
        throw new MetaException("Unable to select from transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        close(null, stmt, dbConn);
      }
    } catch (RetryException e) {
      return heartbeatTxnRange(rqst);
    }
  }

  long generateCompactionQueueId(Statement stmt) throws SQLException, MetaException {
    // Get the id for the next entry in the queue
    String s = sqlGenerator.addForUpdateClause("SELECT \"NCQ_NEXT\" FROM \"NEXT_COMPACTION_QUEUE_ID\"");
    LOG.debug("going to execute query <" + s + ">");
    try (ResultSet rs = stmt.executeQuery(s)) {
      if (!rs.next()) {
        throw new IllegalStateException("Transaction tables not properly initiated, "
            + "no record found in next_compaction_queue_id");
      }
      long id = rs.getLong(1);
      s = "UPDATE \"NEXT_COMPACTION_QUEUE_ID\" SET \"NCQ_NEXT\" = " + (id + 1);
      LOG.debug("Going to execute update <" + s + ">");
      stmt.executeUpdate(s);
      return id;
    }
  }

  @Override
  @RetrySemantics.ReadOnly
  public long getTxnIdForWriteId(
      String dbName, String tblName, long writeId) throws MetaException {
    try {
      Connection dbConn = null;
      PreparedStatement pst = null;
      try {
        /**
         * This runs at READ_COMMITTED for exactly the same reason as {@link #getOpenTxnsInfo()}
         */
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);

        String query = "SELECT \"T2W_TXNID\" FROM \"TXN_TO_WRITE_ID\" WHERE"
            + " \"T2W_DATABASE\" = ? AND \"T2W_TABLE\" = ? AND \"T2W_WRITEID\" = " + writeId;
        pst = sqlGenerator.prepareStmtWithParameters(dbConn, query, Arrays.asList(dbName, tblName));
        LOG.debug("Going to execute query <" + query.replaceAll("\\?", "{}") + ">",
                quoteString(dbName), quoteString(tblName));
        ResultSet rs  = pst.executeQuery();
        long txnId = -1;
        if (rs.next()) {
          txnId = rs.getLong(1);
        }
        dbConn.rollback();
        return txnId;
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "getTxnIdForWriteId");
        throw new MetaException("Unable to select from transaction database, "
                + StringUtils.stringifyException(e));
      } finally {
        close(null, pst, dbConn);
      }
    } catch (RetryException e) {
      return getTxnIdForWriteId(dbName, tblName, writeId);
    }
  }

  @Override
  @RetrySemantics.Idempotent
  public CompactionResponse compact(CompactionRequest rqst) throws MetaException {
    // Put a compaction request in the queue.
    try {
      Connection dbConn = null;
      Statement stmt = null;
      PreparedStatement pst = null;
      TxnStore.MutexAPI.LockHandle handle = null;
      try {
        lockInternal();
        /**
         * MUTEX_KEY.CompactionScheduler lock ensures that there is only 1 entry in
         * Initiated/Working state for any resource.  This ensures that we don't run concurrent
         * compactions for any resource.
         */
        handle = getMutexAPI().acquireLock(MUTEX_KEY.CompactionScheduler.name());
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();

        long id = generateCompactionQueueId(stmt);

        List<String> params = new ArrayList<>();
        StringBuilder sb = new StringBuilder("SELECT \"CQ_ID\", \"CQ_STATE\" FROM \"COMPACTION_QUEUE\" WHERE").
          append(" \"CQ_STATE\" IN(").append(quoteChar(INITIATED_STATE)).
            append(",").append(quoteChar(WORKING_STATE)).
          append(") AND \"CQ_DATABASE\"=?").
          append(" AND \"CQ_TABLE\"=?").append(" AND ");
        params.add(rqst.getDbname());
        params.add(rqst.getTablename());
        if(rqst.getPartitionname() == null) {
          sb.append("\"CQ_PARTITION\" is null");
        } else {
          sb.append("\"CQ_PARTITION\"=?");
          params.add(rqst.getPartitionname());
        }

        pst = sqlGenerator.prepareStmtWithParameters(dbConn, sb.toString(), params);
        LOG.debug("Going to execute query <" + sb.toString() + ">");
        ResultSet rs = pst.executeQuery();
        if(rs.next()) {
          long enqueuedId = rs.getLong(1);
          String state = compactorStateToResponse(rs.getString(2).charAt(0));
          LOG.info("Ignoring request to compact " + rqst.getDbname() + "/" + rqst.getTablename() +
            "/" + rqst.getPartitionname() + " since it is already " + quoteString(state) +
            " with id=" + enqueuedId);
          return new CompactionResponse(enqueuedId, state, false);
        }
        close(rs);
        closeStmt(pst);
        params.clear();
        StringBuilder buf = new StringBuilder("INSERT INTO \"COMPACTION_QUEUE\" (\"CQ_ID\", \"CQ_DATABASE\", " +
          "\"CQ_TABLE\", ");
        String partName = rqst.getPartitionname();
        if (partName != null) buf.append("\"CQ_PARTITION\", ");
        buf.append("\"CQ_STATE\", \"CQ_TYPE\", \"CQ_ENQUEUE_TIME\"");
        if (rqst.getProperties() != null) {
          buf.append(", \"CQ_TBLPROPERTIES\"");
        }
        if (rqst.getRunas() != null) buf.append(", \"CQ_RUN_AS\"");
        buf.append(") values (");
        buf.append(id);
        buf.append(", ?");
        buf.append(", ?");
        buf.append(", ");
        params.add(rqst.getDbname());
        params.add(rqst.getTablename());
        if (partName != null) {
          buf.append("?, '");
          params.add(partName);
        } else {
          buf.append("'");
        }
        buf.append(INITIATED_STATE);
        buf.append("', '");
        buf.append(thriftCompactionType2DbType(rqst.getType()));
        buf.append("',");
        buf.append(getEpochFn(dbProduct));
        if (rqst.getProperties() != null) {
          buf.append(", ?");
          params.add(new StringableMap(rqst.getProperties()).toString());
        }
        if (rqst.getRunas() != null) {
          buf.append(", ?");
          params.add(rqst.getRunas());
        }
        buf.append(")");
        String s = buf.toString();
        pst = sqlGenerator.prepareStmtWithParameters(dbConn, s, params);
        LOG.debug("Going to execute update <" + s + ">");
        pst.executeUpdate();
        LOG.debug("Going to commit");
        dbConn.commit();
        return new CompactionResponse(id, INITIATED_RESPONSE, true);
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "COMPACT(" + rqst + ")");
        throw new MetaException("Unable to select from transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        closeStmt(pst);
        closeStmt(stmt);
        closeDbConn(dbConn);
        if(handle != null) {
          handle.releaseLocks();
        }
        unlockInternal();
      }
    } catch (RetryException e) {
      return compact(rqst);
    }
  }

  private static String compactorStateToResponse(char s) {
    switch (s) {
      case INITIATED_STATE: return INITIATED_RESPONSE;
      case WORKING_STATE: return WORKING_RESPONSE;
      case READY_FOR_CLEANING: return CLEANING_RESPONSE;
      case FAILED_STATE: return FAILED_RESPONSE;
      case SUCCEEDED_STATE: return SUCCEEDED_RESPONSE;
      case ATTEMPTED_STATE: return ATTEMPTED_RESPONSE;
      default:
        return Character.toString(s);
    }
  }
  @RetrySemantics.ReadOnly
  public ShowCompactResponse showCompact(ShowCompactRequest rqst) throws MetaException {
    ShowCompactResponse response = new ShowCompactResponse(new ArrayList<>());
    Connection dbConn = null;
    Statement stmt = null;
    try {
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        String s = "SELECT \"CQ_DATABASE\", \"CQ_TABLE\", \"CQ_PARTITION\", \"CQ_STATE\", \"CQ_TYPE\", \"CQ_WORKER_ID\", " +
          //-1 because 'null' literal doesn't work for all DBs...
          "\"CQ_START\", -1 \"CC_END\", \"CQ_RUN_AS\", \"CQ_HADOOP_JOB_ID\", \"CQ_ID\", \"CQ_ERROR_MESSAGE\", " +
          "\"CQ_ENQUEUE_TIME\" " +
          "FROM \"COMPACTION_QUEUE\" UNION ALL " +
          "SELECT \"CC_DATABASE\", \"CC_TABLE\", \"CC_PARTITION\", \"CC_STATE\", \"CC_TYPE\", \"CC_WORKER_ID\", " +
          "\"CC_START\", \"CC_END\", \"CC_RUN_AS\", \"CC_HADOOP_JOB_ID\", \"CC_ID\", \"CC_ERROR_MESSAGE\", " +
          "\"CC_ENQUEUE_TIME\" " +
          " FROM \"COMPLETED_COMPACTIONS\""; //todo: sort by cq_id?
        //what I want is order by cc_end desc, cc_start asc (but derby has a bug https://issues.apache.org/jira/browse/DERBY-6013)
        //to sort so that currently running jobs are at the end of the list (bottom of screen)
        //and currently running ones are in sorted by start time
        //w/o order by likely currently running compactions will be first (LHS of Union)
        LOG.debug("Going to execute query <" + s + ">");
        ResultSet rs = stmt.executeQuery(s);
        while (rs.next()) {
          ShowCompactResponseElement e = new ShowCompactResponseElement();
          e.setDbname(rs.getString(1));
          e.setTablename(rs.getString(2));
          e.setPartitionname(rs.getString(3));
          e.setState(compactorStateToResponse(rs.getString(4).charAt(0)));
          try {
            e.setType(dbCompactionType2ThriftType(rs.getString(5).charAt(0)));
          } catch (MetaException ex) {
            //do nothing to handle RU/D if we add another status
          }
          e.setWorkerid(rs.getString(6));
          long start = rs.getLong(7);
          if(!rs.wasNull()) {
            e.setStart(start);
          }
          long endTime = rs.getLong(8);
          if(endTime != -1) {
            e.setEndTime(endTime);
          }
          e.setRunAs(rs.getString(9));
          e.setHadoopJobId(rs.getString(10));
          e.setId(rs.getLong(11));
          e.setErrorMessage(rs.getString(12));
          long enqueueTime = rs.getLong(13);
          if(!rs.wasNull()) {
            e.setEnqueueTime(enqueueTime);
          }
          response.addToCompacts(e);
        }
        LOG.debug("Going to rollback");
        dbConn.rollback();
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "showCompact(" + rqst + ")");
        throw new MetaException("Unable to select from transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        closeStmt(stmt);
        closeDbConn(dbConn);
      }
      return response;
    } catch (RetryException e) {
      return showCompact(rqst);
    }
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
  public void addDynamicPartitions(AddDynamicPartitions rqst)
      throws NoSuchTxnException,  TxnAbortedException, MetaException {
    Connection dbConn = null;
    Statement stmt = null;
    try {
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        Optional<TxnType> txnType = getOpenTxnTypeAndLock(stmt, rqst.getTxnid());
        if (!txnType.isPresent()) {
          //ensures txn is still there and in expected state
          ensureValidTxn(dbConn, rqst.getTxnid(), stmt);
          shouldNeverHappen(rqst.getTxnid());
        }
        //for RU this may be null so we should default it to 'u' which is most restrictive
        OperationType ot = OperationType.UPDATE;
        if(rqst.isSetOperationType()) {
          ot = OperationType.fromDataOperationType(rqst.getOperationType());
        }

        Long writeId = rqst.getWriteid();
        try (PreparedStatement pstmt = dbConn.prepareStatement(TXN_COMPONENTS_INSERT_QUERY)) {
          int insertCounter = 0;
          for (String partName : rqst.getPartitionnames()) {
            pstmt.setLong(1, rqst.getTxnid());
            pstmt.setString(2, normalizeCase(rqst.getDbname()));
            pstmt.setString(3, normalizeCase(rqst.getTablename()));
            pstmt.setString(4, partName);
            pstmt.setString(5, ot.getSqlConst());
            pstmt.setObject(6, writeId);

            pstmt.addBatch();
            insertCounter++;
            if (insertCounter % maxBatchSize == 0) {
              LOG.debug("Executing a batch of <" + TXN_COMPONENTS_INSERT_QUERY + "> queries. Batch size: " + maxBatchSize);
              pstmt.executeBatch();
            }
          }
          if (insertCounter % maxBatchSize != 0) {
            LOG.debug("Executing a batch of <" + TXN_COMPONENTS_INSERT_QUERY + "> queries. Batch size: " + insertCounter % maxBatchSize);
            pstmt.executeBatch();
          }
        }
        try (PreparedStatement pstmt = dbConn.prepareStatement(TXN_COMPONENTS_DP_DELETE_QUERY)) {
          pstmt.setLong(1, rqst.getTxnid());
          pstmt.setString(2, normalizeCase(rqst.getDbname()));
          pstmt.setString(3, normalizeCase(rqst.getTablename()));
          pstmt.execute();
        }
        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "addDynamicPartitions(" + rqst + ")");
        throw new MetaException("Unable to insert into from transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        close(null, stmt, dbConn);
        unlockInternal();
      }
    } catch (RetryException e) {
      addDynamicPartitions(rqst);
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
                             Iterator<Partition> partitionIterator) throws MetaException {

    // cleanup should be done only for objects belonging to default catalog
    final String defaultCatalog = getDefaultCatalog(conf);

    try {
      Connection dbConn = null;
      Statement stmt = null;

      try {
        String dbName;
        String tblName;
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        List<String> queries = new ArrayList<>();
        StringBuilder buff = new StringBuilder();

        switch (type) {
          case DATABASE: {
            dbName = db.getName();
            if(!defaultCatalog.equals(db.getCatalogName())) {
              LOG.debug("Skipping cleanup because db: " + dbName + " belongs to catalog "
                  + "other than default catalog: " + db.getCatalogName());
              return;
            }

            buff.append("DELETE FROM \"TXN_COMPONENTS\" WHERE \"TC_DATABASE\"='");
            buff.append(dbName);
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("DELETE FROM \"COMPLETED_TXN_COMPONENTS\" WHERE \"CTC_DATABASE\"='");
            buff.append(dbName);
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("DELETE FROM \"COMPACTION_QUEUE\" WHERE \"CQ_DATABASE\"='");
            buff.append(dbName);
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("DELETE FROM \"COMPLETED_COMPACTIONS\" WHERE \"CC_DATABASE\"='");
            buff.append(dbName);
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("DELETE FROM \"TXN_TO_WRITE_ID\" WHERE \"T2W_DATABASE\"='");
            buff.append(dbName.toLowerCase());
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("DELETE FROM \"NEXT_WRITE_ID\" WHERE \"NWI_DATABASE\"='");
            buff.append(dbName.toLowerCase());
            buff.append("'");
            queries.add(buff.toString());

            break;
          }
          case TABLE: {
            dbName = table.getDbName();
            tblName = table.getTableName();
            if(!defaultCatalog.equals(table.getCatName())) {
              LOG.debug("Skipping cleanup because table: " + tblName + " belongs to catalog "
                  + "other than default catalog: " + table.getCatName());
              return;
            }

            buff.append("DELETE FROM \"TXN_COMPONENTS\" WHERE \"TC_DATABASE\"='");
            buff.append(dbName);
            buff.append("' AND \"TC_TABLE\"='");
            buff.append(tblName);
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("DELETE FROM \"COMPLETED_TXN_COMPONENTS\" WHERE \"CTC_DATABASE\"='");
            buff.append(dbName);
            buff.append("' AND \"CTC_TABLE\"='");
            buff.append(tblName);
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("DELETE FROM \"COMPACTION_QUEUE\" WHERE \"CQ_DATABASE\"='");
            buff.append(dbName);
            buff.append("' AND \"CQ_TABLE\"='");
            buff.append(tblName);
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("DELETE FROM \"COMPLETED_COMPACTIONS\" WHERE \"CC_DATABASE\"='");
            buff.append(dbName);
            buff.append("' AND \"CC_TABLE\"='");
            buff.append(tblName);
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("DELETE FROM \"TXN_TO_WRITE_ID\" WHERE \"T2W_DATABASE\"='");
            buff.append(dbName.toLowerCase());
            buff.append("' AND \"T2W_TABLE\"='");
            buff.append(tblName.toLowerCase());
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("DELETE FROM \"NEXT_WRITE_ID\" WHERE \"NWI_DATABASE\"='");
            buff.append(dbName.toLowerCase());
            buff.append("' AND \"NWI_TABLE\"='");
            buff.append(tblName.toLowerCase());
            buff.append("'");
            queries.add(buff.toString());

            break;
          }
          case PARTITION: {
            dbName = table.getDbName();
            tblName = table.getTableName();
            if(!defaultCatalog.equals(table.getCatName())) {
              LOG.debug("Skipping cleanup because partitions belong to catalog "
                  + "other than default catalog: " + table.getCatName());
              return;
            }

            List<FieldSchema> partCols = table.getPartitionKeys();  // partition columns
            List<String> partVals;                                  // partition values
            String partName;

            while (partitionIterator.hasNext()) {
              Partition p = partitionIterator.next();
              partVals = p.getValues();
              partName = Warehouse.makePartName(partCols, partVals);

              buff.append("DELETE FROM \"TXN_COMPONENTS\" WHERE \"TC_DATABASE\"='");
              buff.append(dbName);
              buff.append("' AND \"TC_TABLE\"='");
              buff.append(tblName);
              buff.append("' AND \"TC_PARTITION\"='");
              buff.append(partName);
              buff.append("'");
              queries.add(buff.toString());

              buff.setLength(0);
              buff.append("DELETE FROM \"COMPLETED_TXN_COMPONENTS\" WHERE \"CTC_DATABASE\"='");
              buff.append(dbName);
              buff.append("' AND \"CTC_TABLE\"='");
              buff.append(tblName);
              buff.append("' AND \"CTC_PARTITION\"='");
              buff.append(partName);
              buff.append("'");
              queries.add(buff.toString());

              buff.setLength(0);
              buff.append("DELETE FROM \"COMPACTION_QUEUE\" WHERE \"CQ_DATABASE\"='");
              buff.append(dbName);
              buff.append("' AND \"CQ_TABLE\"='");
              buff.append(tblName);
              buff.append("' AND \"CQ_PARTITION\"='");
              buff.append(partName);
              buff.append("'");
              queries.add(buff.toString());

              buff.setLength(0);
              buff.append("DELETE FROM \"COMPLETED_COMPACTIONS\" WHERE \"CC_DATABASE\"='");
              buff.append(dbName);
              buff.append("' AND \"CC_TABLE\"='");
              buff.append(tblName);
              buff.append("' AND \"CC_PARTITION\"='");
              buff.append(partName);
              buff.append("'");
              queries.add(buff.toString());
            }

            break;
          }
          default: {
            throw new MetaException("Invalid object type for cleanup: " + type);
          }
        }

        for (String query : queries) {
          LOG.debug("Going to execute update <" + query + ">");
          stmt.executeUpdate(query);
        }

        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "cleanupRecords");
        if (e.getMessage().contains("does not exist")) {
          LOG.warn("Cannot perform cleanup since metastore table does not exist");
        } else {
          throw new MetaException("Unable to clean up " + StringUtils.stringifyException(e));
        }
      } finally {
        closeStmt(stmt);
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      cleanupRecords(type, db, table, partitionIterator);
    }
  }
  /**
   * Catalog hasn't been added to transactional tables yet, so it's passed in but not used.
   */
  @Override
  public void onRename(String oldCatName, String oldDbName, String oldTabName, String oldPartName,
      String newCatName, String newDbName, String newTabName, String newPartName)
      throws MetaException {
    String callSig = "onRename(" +
        oldCatName + "," + oldDbName + "," + oldTabName + "," + oldPartName + "," +
        newCatName + "," + newDbName + "," + newTabName + "," + newPartName + ")";

    if(newPartName != null) {
      assert oldPartName != null && oldTabName != null && oldDbName != null && oldCatName != null :
      callSig;
    }
    if(newTabName != null) {
      assert oldTabName != null && oldDbName != null && oldCatName != null : callSig;
    }
    if(newDbName != null) {
      assert oldDbName != null && oldCatName != null : callSig;
    }

    try {
      Connection dbConn = null;
      Statement stmt = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        List<String> queries = new ArrayList<>();

        String update = "UPDATE \"TXN_COMPONENTS\" SET ";
        String where = " WHERE ";
        if(oldPartName != null) {
          update += "\"TC_PARTITION\" = " + quoteString(newPartName) + ", ";
          where += "\"TC_PARTITION\" = " + quoteString(oldPartName) + " AND ";
        }
        if(oldTabName != null) {
          update += "\"TC_TABLE\" = " + quoteString(normalizeCase(newTabName)) + ", ";
          where += "\"TC_TABLE\" = " + quoteString(normalizeCase(oldTabName)) + " AND ";
        }
        if(oldDbName != null) {
          update += "\"TC_DATABASE\" = " + quoteString(normalizeCase(newDbName));
          where += "\"TC_DATABASE\" = " + quoteString(normalizeCase(oldDbName));
        }
        queries.add(update + where);

        update = "UPDATE \"COMPLETED_TXN_COMPONENTS\" SET ";
        where = " WHERE ";
        if(oldPartName != null) {
          update += "\"CTC_PARTITION\" = " + quoteString(newPartName) + ", ";
          where += "\"CTC_PARTITION\" = " + quoteString(oldPartName) + " AND ";
        }
        if(oldTabName != null) {
          update += "\"CTC_TABLE\" = " + quoteString(normalizeCase(newTabName)) + ", ";
          where += "\"CTC_TABLE\" = " + quoteString(normalizeCase(oldTabName)) + " AND ";
        }
        if(oldDbName != null) {
          update += "\"CTC_DATABASE\" = " + quoteString(normalizeCase(newDbName));
          where += "\"CTC_DATABASE\" = " + quoteString(normalizeCase(oldDbName));
        }
        queries.add(update + where);

        update = "UPDATE \"HIVE_LOCKS\" SET ";
        where = " WHERE ";
        if(oldPartName != null) {
          update += "\"HL_PARTITION\" = " + quoteString(newPartName) + ", ";
          where += "\"HL_PARTITION\" = " + quoteString(oldPartName) + " AND ";
        }
        if(oldTabName != null) {
          update += "\"HL_TABLE\" = " + quoteString(normalizeCase(newTabName)) + ", ";
          where += "\"HL_TABLE\" = " + quoteString(normalizeCase(oldTabName)) + " AND ";
        }
        if(oldDbName != null) {
          update += "\"HL_DB\" = " + quoteString(normalizeCase(newDbName));
          where += "\"HL_DB\" = " + quoteString(normalizeCase(oldDbName));
        }
        queries.add(update + where);

        update = "UPDATE \"COMPACTION_QUEUE\" SET ";
        where = " WHERE ";
        if(oldPartName != null) {
          update += "\"CQ_PARTITION\" = " + quoteString(newPartName) + ", ";
          where += "\"CQ_PARTITION\" = " + quoteString(oldPartName) + " AND ";
        }
        if(oldTabName != null) {
          update += "\"CQ_TABLE\" = " + quoteString(normalizeCase(newTabName)) + ", ";
          where += "\"CQ_TABLE\" = " + quoteString(normalizeCase(oldTabName)) + " AND ";
        }
        if(oldDbName != null) {
          update += "\"CQ_DATABASE\" = " + quoteString(normalizeCase(newDbName));
          where += "\"CQ_DATABASE\" = " + quoteString(normalizeCase(oldDbName));
        }
        queries.add(update + where);

        update = "UPDATE \"COMPLETED_COMPACTIONS\" SET ";
        where = " WHERE ";
        if(oldPartName != null) {
          update += "\"CC_PARTITION\" = " + quoteString(newPartName) + ", ";
          where += "\"CC_PARTITION\" = " + quoteString(oldPartName) + " AND ";
        }
        if(oldTabName != null) {
          update += "\"CC_TABLE\" = " + quoteString(normalizeCase(newTabName)) + ", ";
          where += "\"CC_TABLE\" = " + quoteString(normalizeCase(oldTabName)) + " AND ";
        }
        if(oldDbName != null) {
          update += "\"CC_DATABASE\" = " + quoteString(normalizeCase(newDbName));
          where += "\"CC_DATABASE\" = " + quoteString(normalizeCase(oldDbName));
        }
        queries.add(update + where);

        update = "UPDATE \"WRITE_SET\" SET ";
        where = " WHERE ";
        if(oldPartName != null) {
          update += "\"WS_PARTITION\" = " + quoteString(newPartName) + ", ";
          where += "\"WS_PARTITION\" = " + quoteString(oldPartName) + " AND ";
        }
        if(oldTabName != null) {
          update += "\"WS_TABLE\" = " + quoteString(normalizeCase(newTabName)) + ", ";
          where += "\"WS_TABLE\" = " + quoteString(normalizeCase(oldTabName)) + " AND ";
        }
        if(oldDbName != null) {
          update += "\"WS_DATABASE\" = " + quoteString(normalizeCase(newDbName));
          where += "\"WS_DATABASE\" = " + quoteString(normalizeCase(oldDbName));
        }
        queries.add(update + where);

        update = "UPDATE \"TXN_TO_WRITE_ID\" SET ";
        where = " WHERE ";
        if(oldTabName != null) {
          update += "\"T2W_TABLE\" = " + quoteString(normalizeCase(newTabName)) + ", ";
          where += "\"T2W_TABLE\" = " + quoteString(normalizeCase(oldTabName)) + " AND ";
        }
        if(oldDbName != null) {
          update += "\"T2W_DATABASE\" = " + quoteString(normalizeCase(newDbName));
          where += "\"T2W_DATABASE\" = " + quoteString(normalizeCase(oldDbName));
        }
        queries.add(update + where);

        update = "UPDATE \"NEXT_WRITE_ID\" SET ";
        where = " WHERE ";
        if(oldTabName != null) {
          update += "\"NWI_TABLE\" = " + quoteString(normalizeCase(newTabName)) + ", ";
          where += "\"NWI_TABLE\" = " + quoteString(normalizeCase(oldTabName)) + " AND ";
        }
        if(oldDbName != null) {
          update += "\"NWI_DATABASE\" = " + quoteString(normalizeCase(newDbName));
          where += "\"NWI_DATABASE\" = " + quoteString(normalizeCase(oldDbName));
        }
        queries.add(update + where);

        for (String query : queries) {
          LOG.debug("Going to execute update <" + query + ">");
          stmt.executeUpdate(query);
        }

        LOG.debug("Going to commit: " + callSig);
        dbConn.commit();
      } catch (SQLException e) {
        LOG.debug("Going to rollback: " + callSig);
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, callSig);
        if (e.getMessage().contains("does not exist")) {
          LOG.warn("Cannot perform " + callSig + " since metastore table does not exist");
        } else {
          throw new MetaException("Unable to " + callSig + ":" + StringUtils.stringifyException(e));
        }
      } finally {
        closeStmt(stmt);
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      onRename(oldCatName, oldDbName, oldTabName, oldPartName,
          newCatName, newDbName, newTabName, newPartName);
    }
  }
  /**
   * For testing only, do not use.
   */
  @VisibleForTesting
  public int numLocksInLockTable() throws SQLException, MetaException {
    Connection dbConn = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
      stmt = dbConn.createStatement();
      String s = "SELECT COUNT(*) FROM \"HIVE_LOCKS\"";
      LOG.debug("Going to execute query <" + s + ">");
      rs = stmt.executeQuery(s);
      rs.next();
      int rc = rs.getInt(1);
      // Necessary to clean up the transaction in the db.
      dbConn.rollback();
      return rc;
    } finally {
      close(rs, stmt, dbConn);
    }
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

  private Connection getDbConn(int isolationLevel, DataSource connPool) throws SQLException {
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
      LOG.warn("Retryable error detected in " + caller + ".  Will wait " + retryInterval +
              "ms and retry up to " + (retryLimit - retryNum + 1) + " times.  Error: " + errMsg);
      try {
        Thread.sleep(retryInterval);
      } catch (InterruptedException ex) {
        //
      }
      return true;
    } else {
      LOG.error("Fatal error in " + caller + ". Retry limit (" + retryLimit + ") reached. Last error: " + errMsg);
    }
    return false;
  }

  /**
   * See {@link #checkRetryable(Connection, SQLException, String, boolean)}.
   */
  protected void checkRetryable(Connection conn, SQLException e, String caller) throws RetryException {
    checkRetryable(conn, e, caller, false);
  }

  /**
   * Determine if an exception was such that it makes sense to retry.  Unfortunately there is no standard way to do
   * this, so we have to inspect the error messages and catch the telltale signs for each
   * different database.  This method will throw {@code RetryException}
   * if the error is retry-able.
   * @param conn database connection
   * @param e exception that was thrown.
   * @param caller name of the method calling this (and other info useful to log)
   * @param retryOnDuplicateKey whether to retry on unique key constraint violation
   * @throws org.apache.hadoop.hive.metastore.txn.TxnHandler.RetryException when the operation should be retried
   */
  protected void checkRetryable(Connection conn, SQLException e, String caller, boolean retryOnDuplicateKey)
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
          LOG.warn("Deadlock detected in " + caller + ". Will wait " + waitInterval +
            "ms try again up to " + (ALLOWED_REPEATED_DEADLOCKS - deadlockCnt + 1) + " times.");
          // Pause for a just a bit for retrying to avoid immediately jumping back into the deadlock.
          try {
            Thread.sleep(waitInterval);
          } catch (InterruptedException ie) {
            // NOP
          }
          sendRetrySignal = true;
        } else {
          LOG.error("Too many repeated deadlocks in " + caller + ", giving up.");
        }
      } else if (isRetryable(conf, e)) {
        //in MSSQL this means Communication Link Failure
        sendRetrySignal = waitForRetry(caller, e.getMessage());
      } else if (retryOnDuplicateKey && isDuplicateKeyError(e)) {
        sendRetrySignal = waitForRetry(caller, e.getMessage());
      }
      else {
        //make sure we know we saw an error that we don't recognize
        LOG.info("Non-retryable error in " + caller + " : " + getMessage(e));
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
   * @param conn database connection
   * @return current time in milliseconds
   * @throws org.apache.hadoop.hive.metastore.api.MetaException if the time cannot be determined
   */
  protected long getDbTime(Connection conn) throws MetaException {
    Statement stmt = null;
    try {
      stmt = conn.createStatement();
      String s = dbProduct.getDBTime();

      LOG.debug("Going to execute query <" + s + ">");
      ResultSet rs = stmt.executeQuery(s);
      if (!rs.next()) throw new MetaException("No results from date query");
      return rs.getTimestamp(1).getTime();
    } catch (SQLException e) {
      String msg = "Unable to determine current time: " + e.getMessage();
      LOG.error(msg);
      throw new MetaException(msg);
    } finally {
      closeStmt(stmt);
    }
  }

  protected String isWithinCheckInterval(String expr, long interval) throws MetaException {
    return dbProduct.isWithinCheckInterval(expr, interval);
  }

  /**
   * Determine the String that should be used to quote identifiers.
   * @param conn Active connection
   * @return quotes
   * @throws SQLException
   */
  protected String getIdentifierQuoteString(Connection conn) throws SQLException {
    if (identifierQuoteString == null) {
      identifierQuoteString = conn.getMetaData().getIdentifierQuoteString();
    }
    return identifierQuoteString;
  }


  private void determineDatabaseProduct(Connection conn) {
    if (dbProduct != null) return;
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

  private static class LockInfo {
    private final long extLockId;
    private final long intLockId;
    //0 means there is no transaction, i.e. it a select statement which is not part of
    //explicit transaction or a IUD statement that is not writing to ACID table
    private final long txnId;
    private final String db;
    private final String table;
    private final String partition;
    private final LockState state;
    private final LockType type;

    // Assumes the result set is set to a valid row
    LockInfo(ResultSet rs) throws SQLException, MetaException {
      extLockId = rs.getLong("HL_LOCK_EXT_ID"); // can't be null
      intLockId = rs.getLong("HL_LOCK_INT_ID"); // can't be null
      db = rs.getString("HL_DB"); // can't be null
      String t = rs.getString("HL_TABLE");
      table = (rs.wasNull() ? null : t);
      String p = rs.getString("HL_PARTITION");
      partition = (rs.wasNull() ? null : p);
      switch (rs.getString("HL_LOCK_STATE").charAt(0)) {
        case LOCK_WAITING: state = LockState.WAITING; break;
        case LOCK_ACQUIRED: state = LockState.ACQUIRED; break;
        default:
          throw new MetaException("Unknown lock state " + rs.getString("HL_LOCK_STATE").charAt(0));
      }
      char lockChar = rs.getString("HL_LOCK_TYPE").charAt(0);
      type = LockTypeUtil.getLockTypeFromEncoding(lockChar)
              .orElseThrow(() -> new MetaException("Unknown lock type: " + lockChar));
      txnId = rs.getLong("HL_TXNID"); //returns 0 if value is NULL
    }

    LockInfo(ShowLocksResponseElement e) {
      extLockId = e.getLockid();
      intLockId = e.getLockIdInternal();
      txnId = e.getTxnid();
      db = e.getDbname();
      table = e.getTablename();
      partition = e.getPartname();
      state = e.getState();
      type = e.getType();
    }

    public boolean equals(Object other) {
      if (!(other instanceof LockInfo)) return false;
      LockInfo o = (LockInfo)other;
      // Lock ids are unique across the system.
      return extLockId == o.extLockId && intLockId == o.intLockId;
    }

    @Override
    public String toString() {
      return JavaUtils.lockIdToString(extLockId) + " intLockId:" +
        intLockId + " " + JavaUtils.txnIdToString(txnId)
        + " db:" + db + " table:" + table + " partition:" +
        partition + " state:" + (state == null ? "null" : state.toString())
        + " type:" + (type == null ? "null" : type.toString());
    }
    private boolean isDbLock() {
      return db != null && table == null && partition == null;
    }
    private boolean isTableLock() {
      return db != null && table != null && partition == null;
    }
    private boolean isPartitionLock() {
      return !(isDbLock() || isTableLock());
    }
  }

  private static class LockInfoComparator implements Comparator<LockInfo>, Serializable {
    private LockTypeComparator lockTypeComparator = new LockTypeComparator();

    public boolean equals(Object other) {
      return this == other;
    }

    public int compare(LockInfo info1, LockInfo info2) {
      // We sort by state (acquired vs waiting) and then by LockType, then by id
      if (info1.state == LockState.ACQUIRED &&
        info2.state != LockState .ACQUIRED) {
        return -1;
      }
      if (info1.state != LockState.ACQUIRED &&
        info2.state == LockState .ACQUIRED) {
        return 1;
      }

      int sortByType = lockTypeComparator.compare(info1.type, info2.type);
      if(sortByType != 0) {
        return sortByType;
      }
      if (info1.extLockId < info2.extLockId) {
        return -1;
      } else if (info1.extLockId > info2.extLockId) {
        return 1;
      } else {
        if (info1.intLockId < info2.intLockId) {
          return -1;
        } else if (info1.intLockId > info2.intLockId) {
          return 1;
        } else {
          return 0;
        }
      }
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

  private int abortTxns(Connection dbConn, List<Long> txnids, boolean skipCount) throws SQLException, MetaException {
    return abortTxns(dbConn, txnids, false, skipCount);
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
  private int abortTxns(Connection dbConn, List<Long> txnids, boolean checkHeartbeat, boolean skipCount)
      throws SQLException, MetaException {
    Statement stmt = null;
    if (txnids.isEmpty()) {
      return 0;
    }
    removeAbortedTxnsFromMinHistoryLevel(dbConn, txnids);
    try {
      stmt = dbConn.createStatement();
      //This is an update statement, thus at any Isolation level will take Write locks so will block
      //all other ops using S4U on TXNS row.
      List<String> queries = new ArrayList<>();
      StringBuilder prefix = new StringBuilder();
      StringBuilder suffix = new StringBuilder();

      // add update txns queries to query list
      prefix.append("UPDATE \"TXNS\" SET \"TXN_STATE\" = ").append(TxnStatus.ABORTED)
              .append(" WHERE \"TXN_STATE\" = ").append(TxnStatus.OPEN).append(" AND ");
      if (checkHeartbeat) {
        suffix.append(" AND \"TXN_LAST_HEARTBEAT\" < ")
                .append(TxnDbUtil.getEpochFn(dbProduct)).append("-").append(timeout);
      }
      TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix, txnids, "\"TXN_ID\"", true, false);
      int numUpdateQueries = queries.size();

      // add delete hive locks queries to query list
      prefix.setLength(0);
      suffix.setLength(0);
      prefix.append("DELETE FROM \"HIVE_LOCKS\" WHERE ");
      TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix, txnids, "\"HL_TXNID\"", false, false);

      // execute all queries in the list in one batch
      if (skipCount) {
        executeQueriesInBatchNoCount(dbProduct, stmt, queries, maxBatchSize);
        return 0;
      } else {
        List<Integer> affectedRowsByQuery = executeQueriesInBatch(stmt, queries, maxBatchSize);
        return getUpdateCount(numUpdateQueries, affectedRowsByQuery);
      }
    } finally {
      closeStmt(stmt);
    }
  }

  private int getUpdateCount(int numUpdateQueries, List<Integer> affectedRowsByQuery) {
    return affectedRowsByQuery.stream()
            .limit(numUpdateQueries)
            .mapToInt(Integer::intValue)
            .sum();
  }

  private static boolean isValidTxn(long txnId) {
    return txnId != 0;
  }
  /**
   * Lock acquisition is meant to be fair, so every lock can only block on some lock with smaller
   * hl_lock_ext_id by only checking earlier locks.
   *
   * For any given SQL statement all locks required by it are grouped under single extLockId and are
   * granted all at once or all locks wait.
   *
   * This is expected to run at READ_COMMITTED.
   *
   * If there is a concurrent commitTxn/rollbackTxn, those can only remove rows from HIVE_LOCKS.
   * If they happen to be for the same txnid, there will be a WW conflict (in MS DB), if different txnid,
   * checkLock() will in the worst case keep locks in Waiting state a little longer.
   */
  @RetrySemantics.SafeToRetry("See @SafeToRetry")
  private LockResponse checkLock(Connection dbConn, long extLockId, long txnId, boolean zeroWaitReadEnabled)
      throws NoSuchLockException, TxnAbortedException, MetaException, SQLException {
    Statement stmt = null;
    ResultSet rs = null;
    LockResponse response = new LockResponse();
    /**
     * todo: Longer term we should pass this from client somehow - this would be an optimization;  once
     * that is in place make sure to build and test "writeSet" below using OperationType not LockType
     * With Static Partitions we assume that the query modifies exactly the partitions it locked.  (not entirely
     * realistic since Update/Delete may have some predicate that filters out all records out of
     * some partition(s), but plausible).  For DP, we acquire locks very wide (all known partitions),
     * but for most queries only a fraction will actually be updated.  #addDynamicPartitions() tells
     * us exactly which ones were written to.  Thus using this trick to kill a query early for
     * DP queries may be too restrictive.
     */
    boolean isPartOfDynamicPartitionInsert = true;
    try {
      List<LockInfo> locksBeingChecked = getLocksFromLockId(dbConn, extLockId); //being acquired now
      response.setLockid(extLockId);

      //This is the set of entities that the statement represented by extLockId wants to update
      List<LockInfo> writeSet = new ArrayList<>();

      for (LockInfo info : locksBeingChecked) {
        if(!isPartOfDynamicPartitionInsert && info.type == LockType.SHARED_WRITE) {
          writeSet.add(info);
        }
      }
      if(!writeSet.isEmpty()) {
        if(writeSet.get(0).txnId == 0) {
          //Write operation always start a txn
          throw new IllegalStateException("Found Write lock for " + JavaUtils.lockIdToString(extLockId) + " but no txnid");
        }
        stmt = dbConn.createStatement();
        StringBuilder sb = new StringBuilder(" \"WS_DATABASE\", \"WS_TABLE\", \"WS_PARTITION\", " +
          "\"WS_TXNID\", \"WS_COMMIT_ID\" " +
          "FROM \"WRITE_SET\" WHERE WS_COMMIT_ID >= " + writeSet.get(0).txnId + " AND (");//see commitTxn() for more info on this inequality
        for(LockInfo info : writeSet) {
          sb.append("(\"WS_DATABASE\" = ").append(quoteString(info.db)).append(" AND \"WS_TABLE\" = ")
            .append(quoteString(info.table)).append(" AND \"WS_PARTITION\" ")
            .append(info.partition == null ? "IS NULL" : "= " + quoteString(info.partition)).append(") OR ");
        }
        sb.setLength(sb.length() - 4);//nuke trailing " or "
        sb.append(")");
        //1 row is sufficient to know we have to kill the query
        rs = stmt.executeQuery(sqlGenerator.addLimitClause(1, sb.toString()));
        if(rs.next()) {
          /**
           * if here, it means we found an already committed txn which overlaps with the current one and
           * it updated the same resource the current txn wants to update.  By First-committer-wins
           * rule, current txn will not be allowed to commit so  may as well kill it now;  This is just an
           * optimization to prevent wasting cluster resources to run a query which is known to be DOA.
           * {@link #commitTxn(CommitTxnRequest)} has the primary responsibility to ensure this.
           * checkLock() runs at READ_COMMITTED so you could have another (Hive) txn running commitTxn()
           * in parallel and thus writing to WRITE_SET.  commitTxn() logic is properly mutexed to ensure
           * that we don't "miss" any WW conflicts. We could've mutexed the checkLock() and commitTxn()
           * as well but this reduces concurrency for very little gain.
           * Note that update/delete (which runs as dynamic partition insert) acquires a lock on the table,
           * but WRITE_SET has entries for actual partitions updated.  Thus this optimization will "miss"
           * the WW conflict but it will be caught in commitTxn() where actual partitions written are known.
           * This is OK since we want 2 concurrent updates that update different sets of partitions to both commit.
           */
          String resourceName = rs.getString(1) + '/' + rs.getString(2);
          String partName = rs.getString(3);
          if(partName != null) {
            resourceName += '/' + partName;
          }

          String msg = "Aborting " + JavaUtils.txnIdToString(writeSet.get(0).txnId) +
            " since a concurrent committed transaction [" + JavaUtils.txnIdToString(rs.getLong(4)) + "," + rs.getLong(5) +
            "] has already updated resource '" + resourceName + "'";
          LOG.info(msg);
          if (abortTxns(dbConn, Collections.singletonList(writeSet.get(0).txnId), false) != 1) {
            throw new IllegalStateException(msg + " FAILED!");
          }
          dbConn.commit();
          throw new TxnAbortedException(msg);
        }
        close(rs, stmt, null);
      }

      String queryStr =
        " \"EX\".*, \"REQ\".\"HL_LOCK_INT_ID\" \"LOCK_INT_ID\", \"REQ\".\"HL_LOCK_TYPE\" \"LOCK_TYPE\" FROM (" +
            " SELECT \"HL_LOCK_EXT_ID\", \"HL_LOCK_INT_ID\", \"HL_TXNID\", \"HL_DB\", \"HL_TABLE\", \"HL_PARTITION\"," +
                " \"HL_LOCK_STATE\", \"HL_LOCK_TYPE\" FROM \"HIVE_LOCKS\"" +
            " WHERE \"HL_LOCK_EXT_ID\" < " + extLockId + ") \"EX\"" +
        " INNER JOIN (" +
            " SELECT \"HL_LOCK_INT_ID\", \"HL_TXNID\", \"HL_DB\", \"HL_TABLE\", \"HL_PARTITION\"," +
                " \"HL_LOCK_TYPE\" FROM \"HIVE_LOCKS\"" +
            " WHERE \"HL_LOCK_EXT_ID\" = " + extLockId + ") \"REQ\"" +
        " ON \"EX\".\"HL_DB\" = \"REQ\".\"HL_DB\"" +
            " AND (\"EX\".\"HL_TABLE\" IS NULL OR \"REQ\".\"HL_TABLE\" IS NULL" +
                " OR \"EX\".\"HL_TABLE\" = \"REQ\".\"HL_TABLE\"" +
                " AND (\"EX\".\"HL_PARTITION\" IS NULL OR \"REQ\".\"HL_PARTITION\" IS NULL" +
                " OR \"EX\".\"HL_PARTITION\" = \"REQ\".\"HL_PARTITION\"))" +
        /*different locks from same txn should not conflict with each other,
          txnId=0 means it's a select or IUD which does not write to ACID table*/
        " WHERE (\"REQ\".\"HL_TXNID\" = 0 OR \"EX\".\"HL_TXNID\" != \"REQ\".\"HL_TXNID\")" +
            " AND ";

      /*EXCLUSIVE lock on partition should prevent SHARED_READ on the table, however there is no reason
        for an EXCLUSIVE on a table to prevent SHARED_READ on a database. Similarly, EXCLUSIVE on a partition
        should not conflict with SHARED_READ on a database.
        SHARED_READ is usually acquired on a database to make sure it's not dropped, while some operation
        is performed on that db (e.g. show tables, created table, etc).
        EXCLUSIVE on an object may mean it's being dropped or overwritten.*/
      String[] whereStr = {
        // shared-read
        " \"REQ\".\"HL_LOCK_TYPE\"=" + LockTypeUtil.sharedRead() + " AND \"EX\".\"HL_LOCK_TYPE\"=" +
          LockTypeUtil.exclusive() + " AND NOT (\"EX\".\"HL_TABLE\" IS NOT NULL AND \"REQ\".\"HL_TABLE\" IS NULL)",
        // exclusive
        " \"REQ\".\"HL_LOCK_TYPE\"=" + LockTypeUtil.exclusive() +
        " AND NOT (\"EX\".\"HL_TABLE\" IS NULL AND \"EX\".\"HL_LOCK_TYPE\"=" +
          LockTypeUtil.sharedRead() + " AND \"REQ\".\"HL_TABLE\" IS NOT NULL)",
        // shared-write
        " \"REQ\".\"HL_LOCK_TYPE\"=" + LockTypeUtil.sharedWrite() + " AND \"EX\".\"HL_LOCK_TYPE\" IN (" +
          LockTypeUtil.exclWrite() + "," + LockTypeUtil.exclusive() + ")",
        // excl-write
        " \"REQ\".\"HL_LOCK_TYPE\"=" + LockTypeUtil.exclWrite() + " AND \"EX\".\"HL_LOCK_TYPE\"!=" +
          LockTypeUtil.sharedRead()
      };

      List<String> subQuery = new ArrayList<>();
      for (String subCond : whereStr) {
        subQuery.add("(" + sqlGenerator.addLimitClause(1, queryStr + subCond) + ")");
      }
      String query = String.join(" UNION ALL ", subQuery);

      stmt = dbConn.createStatement();
      LOG.debug("Going to execute query <" + query + ">");
      rs = stmt.executeQuery(query);

      if (rs.next()) {
        // We acquire all locks for a given query atomically; if 1 blocks, all remain in Waiting state.
        LockInfo blockedBy = new LockInfo(rs);
        long intLockId = rs.getLong("LOCK_INT_ID");
        char lockChar = rs.getString("LOCK_TYPE").charAt(0);

        LOG.debug("Failure to acquire lock({} intLockId:{} {}), blocked by ({})", JavaUtils.lockIdToString(extLockId),
            intLockId, JavaUtils.txnIdToString(txnId), blockedBy);

        if (zeroWaitReadEnabled && isValidTxn(txnId)) {
          LockType lockType = LockTypeUtil.getLockTypeFromEncoding(lockChar)
              .orElseThrow(() -> new MetaException("Unknown lock type: " + lockChar));

          if (lockType == LockType.SHARED_READ) {
            String cleanupQuery = "DELETE FROM \"HIVE_LOCKS\" WHERE \"HL_LOCK_EXT_ID\" = " + extLockId;

            LOG.debug("Going to execute query: <" + cleanupQuery + ">");
            stmt.executeUpdate(cleanupQuery);
            dbConn.commit();

            response.setErrorMessage(String.format(
                "Unable to acquire read lock due to an exclusive lock {%s}", blockedBy));
            response.setState(LockState.NOT_ACQUIRED);
            return response;
          }
        }
        String updateBlockedByQuery = "UPDATE \"HIVE_LOCKS\"" +
            " SET \"HL_BLOCKEDBY_EXT_ID\" = " + blockedBy.extLockId +
            ", \"HL_BLOCKEDBY_INT_ID\" = " + blockedBy.intLockId +
            " WHERE \"HL_LOCK_EXT_ID\" = " + extLockId + " AND \"HL_LOCK_INT_ID\" = " + intLockId;

        LOG.debug("Going to execute query: <" + updateBlockedByQuery + ">");
        int updCnt = stmt.executeUpdate(updateBlockedByQuery);

        if (updCnt != 1) {
          LOG.error("Failure to update lock (extLockId={}, intLockId={}) with the blocking lock's IDs " +
              "(extLockId={}, intLockId={})", extLockId, intLockId, blockedBy.extLockId, blockedBy.intLockId);
          shouldNeverHappen(txnId, extLockId, intLockId);
        }
        dbConn.commit();

        response.setState(LockState.WAITING);
        return response;
      }
      // If here, there were no locks that would block any item from 'locksBeingChecked' - acquire them all
      acquire(dbConn, stmt, locksBeingChecked);

      // We acquired all the locks, so commit and return acquired.
      LOG.debug("Successfully acquired locks: " + locksBeingChecked);
      dbConn.commit();
      response.setState(LockState.ACQUIRED);
    } finally {
      close(rs, stmt, null);
    }
    return response;
  }

  private void acquire(Connection dbConn, Statement stmt, List<LockInfo> locksBeingChecked)
    throws SQLException, NoSuchLockException, MetaException {
    if (locksBeingChecked == null || locksBeingChecked.isEmpty()) {
      return;
    }
    long txnId = locksBeingChecked.get(0).txnId;
    long extLockId = locksBeingChecked.get(0).extLockId;
    String s = "UPDATE \"HIVE_LOCKS\" SET \"HL_LOCK_STATE\" = '" + LOCK_ACQUIRED + "', " +
      //if lock is part of txn, heartbeat info is in txn record
      "\"HL_LAST_HEARTBEAT\" = " + (isValidTxn(txnId) ? 0 : TxnDbUtil.getEpochFn(dbProduct)) +
      ",\"HL_ACQUIRED_AT\" = " + TxnDbUtil.getEpochFn(dbProduct) +
      ",\"HL_BLOCKEDBY_EXT_ID\"=NULL,\"HL_BLOCKEDBY_INT_ID\"=NULL" +
      " WHERE \"HL_LOCK_EXT_ID\" = " +  extLockId;
    LOG.debug("Going to execute update <" + s + ">");
    int rc = stmt.executeUpdate(s);
    if (rc < locksBeingChecked.size()) {
      LOG.error("Failure to acquire all locks (acquired: {}, total needed: {}).", rc, locksBeingChecked.size());
      dbConn.rollback();
      /*select all locks for this ext ID and see which ones are missing*/
      String errorMsgTemplate = "No such lock(s): (%s: %s) %s";
      Set<String> notFoundIds = locksBeingChecked.stream()
              .map(lockInfo -> Long.toString(lockInfo.intLockId))
              .collect(Collectors.toSet());
      String getIntIdsQuery = "SELECT \"HL_LOCK_INT_ID\" FROM \"HIVE_LOCKS\" WHERE \"HL_LOCK_EXT_ID\" = " + extLockId;
      LOG.debug("Going to execute query: <" + getIntIdsQuery + ">");
      try (ResultSet rs = stmt.executeQuery(getIntIdsQuery)) {
        while (rs.next()) {
          notFoundIds.remove(rs.getString(1));
        }
      }
      String errorMsg = String.format(errorMsgTemplate,
              JavaUtils.lockIdToString(extLockId), String.join(", ", notFoundIds), JavaUtils.txnIdToString(txnId));
      throw new NoSuchLockException(errorMsg);
    }
  }

  /**
   * Heartbeats on the lock table.  This commits, so do not enter it with any state.
   * Should not be called on a lock that belongs to transaction.
   */
  private void heartbeatLock(Connection dbConn, long extLockId)
    throws NoSuchLockException, SQLException, MetaException {
    // If the lock id is 0, then there are no locks in this heartbeat
    if (extLockId == 0) {
      return;
    }
    try (Statement stmt = dbConn.createStatement()) {
      String updateHeartbeatQuery = "UPDATE \"HIVE_LOCKS\" SET \"HL_LAST_HEARTBEAT\" = " +
          TxnDbUtil.getEpochFn(dbProduct) + " WHERE \"HL_LOCK_EXT_ID\" = " + extLockId;
      LOG.debug("Going to execute update <" + updateHeartbeatQuery + ">");
      int rc = stmt.executeUpdate(updateHeartbeatQuery);
      if (rc < 1) {
        LOG.error("Failure to update last heartbeat for extLockId={}.", extLockId);
        dbConn.rollback();
        throw new NoSuchLockException("No such lock: " + JavaUtils.lockIdToString(extLockId));
      }
      LOG.debug("Successfully heartbeated for extLockId={}", extLockId);
      dbConn.commit();
    }
  }

  // Heartbeats on the txn table.  This commits, so do not enter it with any state
  private void heartbeatTxn(Connection dbConn, long txnid)
    throws NoSuchTxnException, TxnAbortedException, SQLException, MetaException {
    // If the txnid is 0, then there are no transactions in this heartbeat
    if (txnid == 0) {
      return;
    }
    try (Statement stmt = dbConn.createStatement()) {
      String s = "UPDATE \"TXNS\" SET \"TXN_LAST_HEARTBEAT\" = " + TxnDbUtil.getEpochFn(dbProduct) +
          " WHERE \"TXN_ID\" = " + txnid + " AND \"TXN_STATE\" = " + TxnStatus.OPEN;
      LOG.debug("Going to execute update <" + s + ">");
      int rc = stmt.executeUpdate(s);
      if (rc < 1) {
        ensureValidTxn(dbConn, txnid, stmt); // This should now throw some useful exception.
        LOG.error("Can neither heartbeat txn (txnId={}) nor confirm it as invalid.", txnid);
        dbConn.rollback();
        throw new NoSuchTxnException("No such txn: " + txnid);
      }
      LOG.debug("Successfully heartbeated for txnId={}", txnid);
      dbConn.commit();
    }
  }

  /**
   * Returns the state of the transaction if it's able to determine it. Some cases where it cannot:
   * 1. txnid was Aborted/Committed and then GC'd (compacted)
   * 2. txnid was committed but it didn't modify anything (nothing in COMPLETED_TXN_COMPONENTS)
   */
  private TxnStatus findTxnState(long txnid, Statement stmt) throws SQLException, MetaException {
    String s = "SELECT \"TXN_STATE\" FROM \"TXNS\" WHERE \"TXN_ID\" = " + txnid;
    LOG.debug("Going to execute query <" + s + ">");
    try (ResultSet rs = stmt.executeQuery(s)) {
      if (!rs.next()) {
        s =
            sqlGenerator.addLimitClause(1, "1 FROM \"COMPLETED_TXN_COMPONENTS\" WHERE \"CTC_TXNID\" = "
                + txnid);
        LOG.debug("Going to execute query <" + s + ">");
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
      LOG.debug("Going to execute query <" + query + ">");
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
      LOG.debug("Going to execute query <" + query + ">");
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
      LOG.debug("Going to execute query <" + query + ">");
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
    LOG.debug("Going to execute query <" + s + ">");
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

  private Optional<LockInfo> getLockFromLockId(Connection dbConn, long extLockId) throws MetaException, SQLException {
    try (PreparedStatement pstmt = dbConn.prepareStatement(SELECT_LOCKS_FOR_LOCK_ID_QUERY)) {
      pstmt.setLong(1, extLockId);
      LOG.debug("Going to execute query <" + SELECT_LOCKS_FOR_LOCK_ID_QUERY + "> for extLockId={}", extLockId);
      try (ResultSet rs = pstmt.executeQuery()) {
        if (!rs.next()) {
          return Optional.empty();
        }
        LockInfo info = new LockInfo(rs);
        LOG.debug("getTxnIdFromLockId(" + extLockId + ") Return " + JavaUtils.txnIdToString(info.txnId));
        return Optional.of(info);
      }
    }
  }

  // NEVER call this function without first calling heartbeat(long, long)
  private List<LockInfo> getLocksFromLockId(Connection dbConn, long extLockId) throws MetaException, SQLException {
    try (PreparedStatement pstmt = dbConn.prepareStatement(SELECT_LOCKS_FOR_LOCK_ID_QUERY)) {
      List<LockInfo> locks = new ArrayList<>();
      pstmt.setLong(1, extLockId);
      LOG.debug("Going to execute query <" + SELECT_LOCKS_FOR_LOCK_ID_QUERY + "> for extLockId={}", extLockId);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          locks.add(new LockInfo(rs));
        }
      }
      if (locks.isEmpty()) {
        throw new MetaException("This should never happen!  We already " +
          "checked the lock(" + JavaUtils.lockIdToString(extLockId) + ") existed but now we can't find it!");
      }
      LOG.debug("Found {} locks for extLockId={}. Locks: {}", locks.size(), extLockId, locks);
      return locks;
    }
  }

  // Clean time out locks from the database not associated with a transactions, i.e. locks
  // for read-only autoCommit=true statements.  This does a commit,
  // and thus should be done before any calls to heartbeat that will leave
  // open transactions.
  private void timeOutLocks(Connection dbConn) {
    Set<Long> timedOutLockIds = new TreeSet<>();
    //doing a SELECT first is less efficient but makes it easier to debug things
    //when txnid is <> 0, the lock is associated with a txn and is handled by performTimeOuts()
    //want to avoid expiring locks for a txn w/o expiring the txn itself
    try (PreparedStatement pstmt = dbConn.prepareStatement(
            String.format(SELECT_TIMED_OUT_LOCKS_QUERY, TxnDbUtil.getEpochFn(dbProduct)))) {
      pstmt.setLong(1, timeout);
      LOG.debug("Going to execute query: <" + SELECT_TIMED_OUT_LOCKS_QUERY + ">");
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          timedOutLockIds.add(rs.getLong(1));
        }
        dbConn.commit();
        if (timedOutLockIds.isEmpty()) {
          LOG.debug("Did not find any timed-out locks, therefore retuning.");
          return;
        }
      }

      List<String> queries = new ArrayList<>();
      StringBuilder prefix = new StringBuilder();
      StringBuilder suffix = new StringBuilder();

      //include same hl_last_heartbeat condition in case someone heartbeated since the select
      prefix.append("DELETE FROM \"HIVE_LOCKS\" WHERE \"HL_LAST_HEARTBEAT\" < ");
      prefix.append(TxnDbUtil.getEpochFn(dbProduct)).append("-").append(timeout);
      prefix.append(" AND \"HL_TXNID\" = 0 AND ");

      TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix, timedOutLockIds,
              "\"HL_LOCK_EXT_ID\"", true, false);
      try (Statement stmt = dbConn.createStatement()) {
        int deletedLocks = 0;
        for (String query : queries) {
          LOG.debug("Going to execute update: <" + query + ">");
          deletedLocks += stmt.executeUpdate(query);
        }
        if (deletedLocks > 0) {
          LOG.info("Deleted {} locks due to timed-out. Lock ids: {}", deletedLocks, timedOutLockIds);
        }
        dbConn.commit();
      }
    }
    catch (SQLException ex) {
      LOG.error("Failed to purge timed-out locks: " + getMessage(ex), ex);
    }
    catch (Exception ex) {
      LOG.error("Failed to purge timed-out locks: " + ex.getMessage(), ex);
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
    Connection dbConn = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
      //We currently commit after selecting the TXNS to abort.  So whether SERIALIZABLE
      //READ_COMMITTED, the effect is the same.  We could use FOR UPDATE on Select from TXNS
      //and do the whole performTimeOuts() in a single huge transaction, but the only benefit
      //would be to make sure someone cannot heartbeat one of these txns at the same time.
      //The attempt to heartbeat would block and fail immediately after it's unblocked.
      //With current (RC + multiple txns) implementation it is possible for someone to send
      //heartbeat at the very end of the expire interval, and just after the Select from TXNS
      //is made, in which case heartbeat will succeed but txn will still be Aborted.
      //Solving this corner case is not worth the perf penalty.  The client should heartbeat in a
      //timely way.
      timeOutLocks(dbConn);
      while(true) {
        stmt = dbConn.createStatement();
        String s = " \"TXN_ID\" FROM \"TXNS\" WHERE \"TXN_STATE\" = " + TxnStatus.OPEN +
            " AND \"TXN_LAST_HEARTBEAT\" <  " + TxnDbUtil.getEpochFn(dbProduct) + "-" + timeout +
            " AND \"TXN_TYPE\" != " + TxnType.REPL_CREATED.getValue();
        //safety valve for extreme cases
        s = sqlGenerator.addLimitClause(10 * TIMED_OUT_TXN_ABORT_BATCH_SIZE, s);
        LOG.debug("Going to execute query <" + s + ">");
        rs = stmt.executeQuery(s);
        if(!rs.next()) {
          return;//no more timedout txns
        }
        List<List<Long>> timedOutTxns = new ArrayList<>();
        List<Long> currentBatch = new ArrayList<>(TIMED_OUT_TXN_ABORT_BATCH_SIZE);
        timedOutTxns.add(currentBatch);
        do {
          if(currentBatch.size() == TIMED_OUT_TXN_ABORT_BATCH_SIZE) {
            currentBatch = new ArrayList<>(TIMED_OUT_TXN_ABORT_BATCH_SIZE);
            timedOutTxns.add(currentBatch);
          }
          currentBatch.add(rs.getLong(1));
        } while(rs.next());
        dbConn.commit();
        close(rs, stmt, null);
        int numTxnsAborted = 0;
        for(List<Long> batchToAbort : timedOutTxns) {
          if (abortTxns(dbConn, batchToAbort, true, false) == batchToAbort.size()) {
            dbConn.commit();
            numTxnsAborted += batchToAbort.size();
            //todo: add TXNS.COMMENT filed and set it to 'aborted by system due to timeout'
            Collections.sort(batchToAbort);//easier to read logs
            LOG.info("Aborted the following transactions due to timeout: " + batchToAbort.toString());
          }
          else {
            //could not abort all txns in this batch - this may happen because in parallel with this
            //operation there was activity on one of the txns in this batch (commit/abort/heartbeat)
            //This is not likely but may happen if client experiences long pause between heartbeats or
            //unusually long/extreme pauses between heartbeat() calls and other logic in checkLock(),
            //lock(), etc.
            dbConn.rollback();
          }
        }
        LOG.info("Aborted " + numTxnsAborted + " transactions due to timeout");
      }
    } catch (SQLException ex) {
      LOG.warn("Aborting timed out transactions failed due to " + getMessage(ex), ex);
    } catch(MetaException e) {
      LOG.warn("Aborting timed out transactions failed due to " + e.getMessage(), e);
    }
    finally {
      close(rs, stmt, dbConn);
    }
  }
  @Override
  @RetrySemantics.ReadOnly
  public void countOpenTxns() throws MetaException {
    Connection dbConn = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        String s = "SELECT COUNT(*) FROM \"TXNS\" WHERE \"TXN_STATE\" = " + TxnStatus.OPEN;
        LOG.debug("Going to execute query <" + s + ">");
        rs = stmt.executeQuery(s);
        if (!rs.next()) {
          LOG.error("Transaction database not properly configured, " +
              "can't find txn_state from TXNS.");
        } else {
          Long numOpen = rs.getLong(1);
          if (numOpen > Integer.MAX_VALUE) {
            LOG.error("Open transaction count above " + Integer.MAX_VALUE +
                ", can't count that high!");
          } else {
            numOpenTxns.set(numOpen.intValue());
          }
        }
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        LOG.info("Failed to update number of open transactions");
        checkRetryable(dbConn, e, "countOpenTxns()");
      } finally {
        close(rs, stmt, dbConn);
      }
    } catch (RetryException e) {
      countOpenTxns();
    }
  }

  /**
   * Add min history level entry for each generated txn record
   * @param dbConn Connection
   * @param txnIds new transaction ids
   * @deprecated Remove this method when min_history_level table is dropped
   * @throws SQLException ex
   */
  @Deprecated
  private void addTxnToMinHistoryLevel(Connection dbConn, List<Long> txnIds, long minOpenTxnId) throws SQLException {
    if (!useMinHistoryLevel) {
      return;
    }
    // Need to register minimum open txnid for current transactions into MIN_HISTORY table.
    try (Statement stmt = dbConn.createStatement()) {

      List<String> rows = txnIds.stream().map(txnId -> txnId + ", " + minOpenTxnId).collect(Collectors.toList());

      // Insert transaction entries into MIN_HISTORY_LEVEL.
      List<String> inserts =
          sqlGenerator.createInsertValuesStmt("\"MIN_HISTORY_LEVEL\" (\"MHL_TXNID\", \"MHL_MIN_OPEN_TXNID\")", rows);
      for (String insert : inserts) {
        LOG.debug("Going to execute insert <" + insert + ">");
        stmt.execute(insert);
      }
      LOG.info("Added entries to MIN_HISTORY_LEVEL for current txns: (" + txnIds + ") with min_open_txn: " + minOpenTxnId);
    } catch (SQLException e) {
      if (dbProduct.isTableNotExistsError(e)) {
        // If the table does not exists anymore, we disable the flag and start to work the new way
        // This enables to switch to the new functionality without a restart
        useMinHistoryLevel = false;
      } else {
        throw e;
      }
    }
  }

  /**
   * Remove record from min_history_level for a committed transaction
   * @param dbConn connection
   * @param txnid committed transaction
   * @deprecated Remove this method when min_history_level table is dropped
   * @throws SQLException ex
   */
  @Deprecated
  private void removeCommittedTxnFromMinHistoryLevel(Connection dbConn, long txnid) throws SQLException {
    if (!useMinHistoryLevel) {
      return;
    }
    try (PreparedStatement pStmt = dbConn.prepareStatement("DELETE FROM \"MIN_HISTORY_LEVEL\" WHERE \"MHL_TXNID\" = ?")) {
      pStmt.setLong(1, txnid);
      pStmt.executeUpdate();
      LOG.debug("Removed committed transaction txnId: (" + txnid + ") from MIN_HISTORY_LEVEL");
    } catch (SQLException e) {
      if (dbProduct.isTableNotExistsError(e)) {
        // If the table does not exists anymore, we disable the flag and start to work the new way
        // This enables to switch to the new funcionality without a restart
        useMinHistoryLevel = false;
      } else {
        throw e;
      }
    }
  }

  /**
   * Remove aborted txns from min_history_level table
   * @param dbConn connection
   * @param abortedTxnids aborted transactions
   * @deprecated Remove this method when min_history_level table is dropped
   */
  @Deprecated
  private void removeAbortedTxnsFromMinHistoryLevel(Connection dbConn, List<Long> abortedTxnids) throws SQLException {
    if (!useMinHistoryLevel) {
      return;
    }
    try (Statement stmt = dbConn.createStatement()) {
      List<String> queries = new ArrayList<>();
      StringBuilder prefix = new StringBuilder();
      prefix.append("DELETE FROM \"MIN_HISTORY_LEVEL\" WHERE ");
      TxnUtils.buildQueryWithINClause(conf, queries, prefix, new StringBuilder(), abortedTxnids, "\"MHL_TXNID\"", false,
          false);
      executeQueriesInBatch(stmt, queries, maxBatchSize);
      LOG.info("Removed aborted transactions: (" + abortedTxnids + ") from MIN_HISTORY_LEVEL");
    } catch (SQLException e) {
      if (dbProduct.isTableNotExistsError(e)) {
        // If the table does not exists anymore, we disable the flag and start to work the new way
        // This enables to switch to the new funcionality without a restart
        useMinHistoryLevel = false;
      } else {
        throw e;
      }
    }
  }

  private static synchronized DataSource setupJdbcConnectionPool(Configuration conf, int maxPoolSize, long getConnectionTimeoutMs) throws SQLException {
    DataSourceProvider dsp = DataSourceProviderFactory.tryGetDataSourceProviderOrNull(conf);
    if (dsp != null) {
      return dsp.create(conf);
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

  static CompactionType dbCompactionType2ThriftType(char dbValue) throws MetaException {
    switch (dbValue) {
      case MAJOR_TYPE:
        return CompactionType.MAJOR;
      case MINOR_TYPE:
        return CompactionType.MINOR;
      default:
        throw new MetaException("Unexpected compaction type " + dbValue);
    }
  }
  static Character thriftCompactionType2DbType(CompactionType ct) throws MetaException {
    switch (ct) {
      case MAJOR:
        return MAJOR_TYPE;
      case MINOR:
        return MINOR_TYPE;
      default:
        throw new MetaException("Unexpected compaction type " + ct);
    }
  }

  /**
   * {@link #lockInternal()} and {@link #unlockInternal()} are used to serialize those operations that require
   * Select ... For Update to sequence operations properly.  In practice that means when running
   * with Derby database.  See more notes at class level.
   */
  private void lockInternal() {
    if(dbProduct.isDERBY()) {
      derbyLock.lock();
    }
  }
  private void unlockInternal() {
    if(dbProduct.isDERBY()) {
      derbyLock.unlock();
    }
  }
  @Override
  @RetrySemantics.Idempotent
  public MutexAPI getMutexAPI() {
    return this;
  }

  @Override
  public LockHandle acquireLock(String key) throws MetaException {
    /**
     * The implementation here is a bit kludgey but done so that code exercised by unit tests
     * (which run against Derby which has no support for select for update) is as similar to
     * production code as possible.
     * In particular, with Derby we always run in a single process with a single metastore and
     * the absence of For Update is handled via a Semaphore.  The later would strictly speaking
     * make the SQL statements below unnecessary (for Derby), but then they would not be tested.
     */
    Connection dbConn = null;
    Statement stmt = null;
    ResultSet rs = null;
    boolean needToCloseConn = true;
    try {
      try {
        String sqlStmt = sqlGenerator.addForUpdateClause("SELECT \"MT_COMMENT\" FROM \"AUX_TABLE\" WHERE \"MT_KEY1\"=" + quoteString(key) + " and \"MT_KEY2\"=0");
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED, connPoolMutex);
        stmt = dbConn.createStatement();
        if(LOG.isDebugEnabled()) {
          LOG.debug("About to execute SQL: " + sqlStmt);
        }
        rs = stmt.executeQuery(sqlStmt);
        if (!rs.next()) {
          close(rs);
          try {
            stmt.executeUpdate("INSERT INTO \"AUX_TABLE\" (\"MT_KEY1\", \"MT_KEY2\") VALUES(" + quoteString(key) + ", 0)");
            dbConn.commit();
          } catch (SQLException ex) {
            if (!isDuplicateKeyError(ex)) {
              throw new RuntimeException("Unable to lock " + quoteString(key) + " due to: " + getMessage(ex), ex);
            }
            //if here, it means a concrurrent acquireLock() inserted the 'key'

            //rollback is done for the benefit of Postgres which throws (SQLState=25P02, ErrorCode=0) if
            //you attempt any stmt in a txn which had an error.
            dbConn.rollback();
          }
          rs = stmt.executeQuery(sqlStmt);
          if (!rs.next()) {
            throw new IllegalStateException("Unable to lock " + quoteString(key) + ".  Expected row in AUX_TABLE is missing.");
          }
        }
        Semaphore derbySemaphore = null;
        if(dbProduct.isDERBY()) {
          derbyKey2Lock.putIfAbsent(key, new Semaphore(1));
          derbySemaphore =  derbyKey2Lock.get(key);
          derbySemaphore.acquire();
        }
        LOG.debug(quoteString(key) + " locked by " + quoteString(TxnHandler.hostname));
        needToCloseConn = false;  //The connection is good, we need not close it
        //OK, so now we have a lock
        return new LockHandleImpl(dbConn, stmt, rs, key, derbySemaphore);
      } catch (SQLException ex) {
        checkRetryable(dbConn, ex, "acquireLock(" + key + ")");
        throw new MetaException("Unable to lock " + quoteString(key) + " due to: " + getMessage(ex) + "; " + StringUtils.stringifyException(ex));
      }
      catch(InterruptedException ex) {
        throw new MetaException("Unable to lock " + quoteString(key) + " due to: " + ex.getMessage() + StringUtils.stringifyException(ex));
      }
      finally {
        if (needToCloseConn) {
          rollbackDBConn(dbConn);
          close(rs, stmt, dbConn);
        }
        unlockInternal();
      }
    }
    catch(RetryException ex) {
      return acquireLock(key);
    }
  }

  @Override
  public void acquireLock(String key, LockHandle handle) {
    //the idea is that this will use LockHandle.dbConn
    throw new NotImplementedException("acquireLock(String, LockHandle) is not implemented");
  }

  /**
   * Acquire the global txn lock, used to mutex the openTxn and commitTxn.
   * @param stmt Statement to execute the lock on
   * @param shared either SHARED_READ or EXCLUSIVE
   * @throws SQLException
   */
  private void acquireTxnLock(Statement stmt, boolean shared) throws SQLException, MetaException {
    String sqlStmt = sqlGenerator.createTxnLockStatement(shared);
    stmt.execute(sqlStmt);
    LOG.debug("TXN lock locked by {} in mode {}", quoteString(TxnHandler.hostname), shared);
  }

  private static final class LockHandleImpl implements LockHandle {
    private final Connection dbConn;
    private final Statement stmt;
    private final ResultSet rs;
    private final Semaphore derbySemaphore;
    private final List<String> keys = new ArrayList<>();
    LockHandleImpl(Connection conn, Statement stmt, ResultSet rs, String key, Semaphore derbySemaphore) {
      this.dbConn = conn;
      this.stmt = stmt;
      this.rs = rs;
      this.derbySemaphore = derbySemaphore;
      if(derbySemaphore != null) {
        //oterwise it may later release permit acquired by someone else
        assert derbySemaphore.availablePermits() == 0 : "Expected locked Semaphore";
      }
      keys.add(key);
    }
    void addKey(String key) {
      //keys.add(key);
      //would need a list of (stmt,rs) pairs - 1 for each key
      throw new NotImplementedException("addKey(String) is not implemented, would require a list of (stmt,rs) pairs / key");
    }

    @Override
    public void releaseLocks() {
      rollbackDBConn(dbConn);
      close(rs, stmt, dbConn);
      if(derbySemaphore != null) {
        derbySemaphore.release();
      }
      for(String key : keys) {
        LOG.debug(quoteString(key) + " unlocked by " + quoteString(TxnHandler.hostname));
      }
    }
  }


  private static class NoPoolConnectionPool implements DataSource {
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
          LOG.info("Going to load JDBC driver " + driverName);
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
        LOG.info("Connecting to transaction db with connection string " + connString);
        Properties connectionProps = new Properties();
        connectionProps.setProperty("user", username);
        connectionProps.setProperty("password", password);
        Connection conn = driver.connect(connString, connectionProps);
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

}
