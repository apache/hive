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
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Savepoint;
import java.sql.Statement;
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
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.datasource.DataSourceProvider;
import org.apache.hadoop.hive.metastore.datasource.DataSourceProviderFactory;
import org.apache.hadoop.hive.metastore.events.AbortTxnEvent;
import org.apache.hadoop.hive.metastore.events.AllocWriteIdEvent;
import org.apache.hadoop.hive.metastore.events.CommitTxnEvent;
import org.apache.hadoop.hive.metastore.events.OpenTxnEvent;
import org.apache.hadoop.hive.metastore.events.AcidWriteEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.StringableMap;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

  // Transaction states
  static final protected char TXN_ABORTED = 'a';
  static final protected char TXN_OPEN = 'o';
  //todo: make these like OperationType and remove above char constatns
  enum TxnStatus {OPEN, ABORTED, COMMITTED, UNKNOWN}

  public enum TxnType {
    DEFAULT(0), REPL_CREATED(1), READ_ONLY(2);

    private final int value;
    TxnType(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  // Lock states
  static final protected char LOCK_ACQUIRED = 'a';
  static final protected char LOCK_WAITING = 'w';

  // Lock types
  static final protected char LOCK_EXCLUSIVE = 'e';
  static final protected char LOCK_SHARED = 'r';
  static final protected char LOCK_SEMI_SHARED = 'w';

  static final private int ALLOWED_REPEATED_DEADLOCKS = 10;
  static final private Logger LOG = LoggerFactory.getLogger(TxnHandler.class.getName());

  static private DataSource connPool;
  private static DataSource connPoolMutex;
  static private boolean doRetryOnConnPool = false;

  private List<TransactionalMetaStoreEventListener> transactionalListeners;
  
  private enum OpertaionType {
    SELECT('s'), INSERT('i'), UPDATE('u'), DELETE('d');
    private final char sqlConst;
    OpertaionType(char sqlConst) {
      this.sqlConst = sqlConst;
    }
    public String toString() {
      return Character.toString(sqlConst);
    }
    public static OpertaionType fromString(char sqlConst) {
      switch (sqlConst) {
        case 's':
          return SELECT;
        case 'i':
          return INSERT;
        case 'u':
          return UPDATE;
        case 'd':
          return DELETE;
        default:
          throw new IllegalArgumentException(quoteChar(sqlConst));
      }
    }
    public static OpertaionType fromDataOperationType(DataOperationType dop) {
      switch (dop) {
        case SELECT:
          return OpertaionType.SELECT;
        case INSERT:
          return OpertaionType.INSERT;
        case UPDATE:
          return OpertaionType.UPDATE;
        case DELETE:
          return OpertaionType.DELETE;
        default:
          throw new IllegalArgumentException("Unexpected value: " + dop);
      }
    }
  }

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
  private static DatabaseProduct dbProduct;
  private static SQLGenerator sqlGenerator;

  // (End user) Transaction timeout, in milliseconds.
  private long timeout;

  private String identifierQuoteString; // quotes to use for quoting tables, where necessary
  private long retryInterval;
  private int retryLimit;
  private int retryNum;
  // Current number of open txns
  private AtomicInteger numOpenTxns;

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
  public void setConf(Configuration conf) {
    this.conf = conf;

    checkQFileTestHack();

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
    buildJumpTable();
    retryInterval = MetastoreConf.getTimeVar(conf, ConfVars.HMS_HANDLER_INTERVAL,
        TimeUnit.MILLISECONDS);
    retryLimit = MetastoreConf.getIntVar(conf, ConfVars.HMS_HANDLER_ATTEMPTS);
    deadlockRetryInterval = retryInterval / 10;
    maxOpenTxns = MetastoreConf.getIntVar(conf, ConfVars.MAX_OPEN_TXNS);

    try {
      transactionalListeners = MetaStoreUtils.getMetaStoreListeners(
              TransactionalMetaStoreEventListener.class,
                      conf, MetastoreConf.getVar(conf, ConfVars.TRANSACTIONAL_EVENT_LISTENERS));
    } catch(MetaException e) {
      String msg = "Unable to get transaction listeners, " + e.getMessage();
      LOG.error(msg);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  @RetrySemantics.ReadOnly
  public GetOpenTxnsInfoResponse getOpenTxnsInfo() throws MetaException {
    try {
      // We need to figure out the current transaction number and the list of
      // open transactions.  To avoid needing a transaction on the underlying
      // database we'll look at the current transaction number first.  If it
      // subsequently shows up in the open list that's ok.
      Connection dbConn = null;
      Statement stmt = null;
      ResultSet rs = null;
      try {
        /**
         * This method can run at READ_COMMITTED as long as long as
         * {@link #openTxns(org.apache.hadoop.hive.metastore.api.OpenTxnRequest)} is atomic.
         * More specifically, as long as advancing TransactionID in NEXT_TXN_ID is atomic with
         * adding corresponding entries into TXNS.  The reason is that any txnid below HWM
         * is either in TXNS and thus considered open (Open/Aborted) or it's considered Committed.
         */
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        String s = "select ntxn_next - 1 from NEXT_TXN_ID";
        LOG.debug("Going to execute query <" + s + ">");
        rs = stmt.executeQuery(s);
        if (!rs.next()) {
          throw new MetaException("Transaction tables not properly " +
            "initialized, no record found in next_txn_id");
        }
        long hwm = rs.getLong(1);
        if (rs.wasNull()) {
          throw new MetaException("Transaction tables not properly " +
            "initialized, null record found in next_txn_id");
        }
        close(rs);
        List<TxnInfo> txnInfos = new ArrayList<>();
        //need the WHERE clause below to ensure consistent results with READ_COMMITTED
        s = "select txn_id, txn_state, txn_user, txn_host, txn_started, txn_last_heartbeat from " +
            "TXNS where txn_id <= " + hwm;
        LOG.debug("Going to execute query<" + s + ">");
        rs = stmt.executeQuery(s);
        while (rs.next()) {
          char c = rs.getString(2).charAt(0);
          TxnState state;
          switch (c) {
            case TXN_ABORTED:
              state = TxnState.ABORTED;
              break;

            case TXN_OPEN:
              state = TxnState.OPEN;
              break;

            default:
              throw new MetaException("Unexpected transaction state " + c +
                " found in txns table");
          }
          TxnInfo txnInfo = new TxnInfo(rs.getLong(1), state, rs.getString(3), rs.getString(4));
          txnInfo.setStartedTime(rs.getLong(5));
          txnInfo.setLastHeartbeatTime(rs.getLong(6));
          txnInfos.add(txnInfo);
        }
        LOG.debug("Going to rollback");
        dbConn.rollback();
        return new GetOpenTxnsInfoResponse(hwm, txnInfos);
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "getOpenTxnsInfo");
        throw new MetaException("Unable to select from transaction database: " + getMessage(e)
          + StringUtils.stringifyException(e));
      } finally {
        close(rs, stmt, dbConn);
      }
    } catch (RetryException e) {
      return getOpenTxnsInfo();
    }
  }
  @Override
  @RetrySemantics.ReadOnly
  public GetOpenTxnsResponse getOpenTxns() throws MetaException {
    try {
      // We need to figure out the current transaction number and the list of
      // open transactions.  To avoid needing a transaction on the underlying
      // database we'll look at the current transaction number first.  If it
      // subsequently shows up in the open list that's ok.
      Connection dbConn = null;
      Statement stmt = null;
      ResultSet rs = null;
      try {
        /**
         * This runs at READ_COMMITTED for exactly the same reason as {@link #getOpenTxnsInfo()}
         */
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        String s = "select ntxn_next - 1 from NEXT_TXN_ID";
        LOG.debug("Going to execute query <" + s + ">");
        rs = stmt.executeQuery(s);
        if (!rs.next()) {
          throw new MetaException("Transaction tables not properly " +
            "initialized, no record found in next_txn_id");
        }
        long hwm = rs.getLong(1);
        if (rs.wasNull()) {
          throw new MetaException("Transaction tables not properly " +
            "initialized, null record found in next_txn_id");
        }
        close(rs);
        List<Long> openList = new ArrayList<>();
        //need the WHERE clause below to ensure consistent results with READ_COMMITTED
        s = "select txn_id, txn_state from TXNS where txn_id <= " + hwm + " order by txn_id";
        LOG.debug("Going to execute query<" + s + ">");
        rs = stmt.executeQuery(s);
        long minOpenTxn = Long.MAX_VALUE;
        BitSet abortedBits = new BitSet();
        while (rs.next()) {
          long txnId = rs.getLong(1);
          openList.add(txnId);
          char c = rs.getString(2).charAt(0);
          if(c == TXN_OPEN) {
            minOpenTxn = Math.min(minOpenTxn, txnId);
          } else if (c == TXN_ABORTED) {
            abortedBits.set(openList.size() - 1);
          }
        }
        LOG.debug("Going to rollback");
        dbConn.rollback();
        ByteBuffer byteBuffer = ByteBuffer.wrap(abortedBits.toByteArray());
        GetOpenTxnsResponse otr = new GetOpenTxnsResponse(hwm, openList, byteBuffer);
        if(minOpenTxn < Long.MAX_VALUE) {
          otr.setMin_open_txn(minOpenTxn);
        }
        return otr;
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "getOpenTxns");
        throw new MetaException("Unable to select from transaction database, "
          + StringUtils.stringifyException(e));
      } finally {
        close(rs, stmt, dbConn);
      }
    } catch (RetryException e) {
      return getOpenTxns();
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
        lockInternal();
        /**
         * To make {@link #getOpenTxns()}/{@link #getOpenTxnsInfo()} work correctly, this operation must ensure
         * that advancing the counter in NEXT_TXN_ID and adding appropriate entries to TXNS is atomic.
         * Also, advancing the counter must work when multiple metastores are running.
         * SELECT ... FOR UPDATE is used to prevent
         * concurrent DB transactions being rolled back due to Write-Write conflict on NEXT_TXN_ID.
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
        // Make sure the user has not requested an insane amount of txns.
        int maxTxns = MetastoreConf.getIntVar(conf, ConfVars.TXN_MAX_OPEN_BATCH);
        if (numTxns > maxTxns) numTxns = maxTxns;

        stmt = dbConn.createStatement();
        List<Long> txnIds = openTxns(dbConn, stmt, rqst);

        LOG.debug("Going to commit");
        dbConn.commit();
        return new OpenTxnsResponse(txnIds);
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "openTxns(" + rqst + ")");
        throw new MetaException("Unable to select from transaction database "
          + StringUtils.stringifyException(e));
      } finally {
        close(null, stmt, dbConn);
        unlockInternal();
      }
    } catch (RetryException e) {
      return openTxns(rqst);
    }
  }

  private List<Long> openTxns(Connection dbConn, Statement stmt, OpenTxnRequest rqst)
          throws SQLException, MetaException {
    int numTxns = rqst.getNum_txns();
    ResultSet rs = null;
    TxnType txnType = TxnType.DEFAULT;
    try {
      if (rqst.isSetReplPolicy()) {
        List<Long> targetTxnIdList = getTargetTxnIdList(rqst.getReplPolicy(), rqst.getReplSrcTxnIds(), stmt);

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

      String s = sqlGenerator.addForUpdateClause("select ntxn_next from NEXT_TXN_ID");
      LOG.debug("Going to execute query <" + s + ">");
      rs = stmt.executeQuery(s);
      if (!rs.next()) {
        throw new MetaException("Transaction database not properly " +
                "configured, can't find next transaction id.");
      }
      long first = rs.getLong(1);
      s = "update NEXT_TXN_ID set ntxn_next = " + (first + numTxns);
      LOG.debug("Going to execute update <" + s + ">");
      stmt.executeUpdate(s);

      long now = getDbTime(dbConn);
      List<Long> txnIds = new ArrayList<>(numTxns);

      List<String> rows = new ArrayList<>();
      for (long i = first; i < first + numTxns; i++) {
        txnIds.add(i);
        rows.add(i + "," + quoteChar(TXN_OPEN) + "," + now + "," + now + ","
                + quoteString(rqst.getUser()) + "," + quoteString(rqst.getHostname()) + "," + txnType.getValue());
      }
      List<String> queries = sqlGenerator.createInsertValuesStmt(
            "TXNS (txn_id, txn_state, txn_started, txn_last_heartbeat, txn_user, txn_host, txn_type)", rows);
      for (String q : queries) {
        LOG.debug("Going to execute update <" + q + ">");
        stmt.execute(q);
      }

      // Need to register minimum open txnid for current transactions into MIN_HISTORY table.
      s = "select min(txn_id) from TXNS where txn_state = " + quoteChar(TXN_OPEN);
      LOG.debug("Going to execute query <" + s + ">");
      rs = stmt.executeQuery(s);
      if (!rs.next()) {
        throw new IllegalStateException("Scalar query returned no rows?!?!!");
      }

      // TXNS table should have atleast one entry because we just inserted the newly opened txns.
      // So, min(txn_id) would be a non-zero txnid.
      long minOpenTxnId = rs.getLong(1);
      assert (minOpenTxnId > 0);
      rows.clear();
      for (long txnId = first; txnId < first + numTxns; txnId++) {
        rows.add(txnId + ", " + minOpenTxnId);
      }

      // Insert transaction entries into MIN_HISTORY_LEVEL.
      List<String> inserts = sqlGenerator.createInsertValuesStmt(
              "MIN_HISTORY_LEVEL (mhl_txnid, mhl_min_open_txnid)", rows);
      for (String insert : inserts) {
        LOG.debug("Going to execute insert <" + insert + ">");
        stmt.execute(insert);
      }
      LOG.info("Added entries to MIN_HISTORY_LEVEL for current txns: (" + txnIds
              + ") with min_open_txn: " + minOpenTxnId);

      if (rqst.isSetReplPolicy()) {
        List<String> rowsRepl = new ArrayList<>();

        for (int i = 0; i < numTxns; i++) {
          rowsRepl.add(
                  quoteString(rqst.getReplPolicy()) + "," + rqst.getReplSrcTxnIds().get(i) + "," + txnIds.get(i));
        }

        List<String> queriesRepl = sqlGenerator.createInsertValuesStmt(
                "REPL_TXN_MAP (RTM_REPL_POLICY, RTM_SRC_TXN_ID, RTM_TARGET_TXN_ID)", rowsRepl);

        for (String query : queriesRepl) {
          LOG.info("Going to execute insert <" + query + ">");
          stmt.execute(query);
        }
      }

      if (transactionalListeners != null) {
        MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners,
                EventMessage.EventType.OPEN_TXN, new OpenTxnEvent(txnIds, null), dbConn, sqlGenerator);
      }
      return txnIds;
    } finally {
      close(rs);
    }
  }

  private List<Long> getTargetTxnIdList(String replPolicy, List<Long> sourceTxnIdList, Statement stmt)
          throws SQLException {
    ResultSet rs = null;
    try {
      List<String> inQueries = new ArrayList<>();
      StringBuilder prefix = new StringBuilder();
      StringBuilder suffix = new StringBuilder();
      List<Long> targetTxnIdList = new ArrayList<>();
      prefix.append("select RTM_TARGET_TXN_ID from REPL_TXN_MAP where ");
      suffix.append(" and RTM_REPL_POLICY = " + quoteString(replPolicy));
      TxnUtils.buildQueryWithINClause(conf, inQueries, prefix, suffix, sourceTxnIdList,
              "RTM_SRC_TXN_ID", false, false);
      for (String query : inQueries) {
        LOG.debug("Going to execute select <" + query + ">");
        rs = stmt.executeQuery(query);
        while (rs.next()) {
          targetTxnIdList.add(rs.getLong(1));
        }
      }
      LOG.debug("targetTxnid for srcTxnId " + sourceTxnIdList.toString() + " is " + targetTxnIdList.toString());
      return targetTxnIdList;
    }  catch (SQLException e) {
      LOG.warn("failed to get target txn ids " + e.getMessage());
      throw e;
    } finally {
      close(rs);
    }
  }

  @Override
  @RetrySemantics.Idempotent
  public long getTargetTxnId(String replPolicy, long sourceTxnId) throws MetaException {
    try {
      Connection dbConn = null;
      Statement stmt = null;
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        List<Long> targetTxnIds = getTargetTxnIdList(replPolicy, Collections.singletonList(sourceTxnId), stmt);
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
        close(null, stmt, dbConn);
        unlockInternal();
      }
    } catch (RetryException e) {
      return getTargetTxnId(replPolicy, sourceTxnId);
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
                  Collections.singletonList(sourceTxnId), stmt);
          if (targetTxnIds.isEmpty()) {
            LOG.info("Target txn id is missing for source txn id : " + sourceTxnId +
                    " and repl policy " + rqst.getReplPolicy());
            return;
          }
          assert targetTxnIds.size() == 1;
          txnid = targetTxnIds.get(0);
        }

        if (abortTxns(dbConn, Collections.singletonList(txnid), true) != 1) {
          TxnStatus status = findTxnState(txnid,stmt);
          if(status == TxnStatus.ABORTED) {
            if (rqst.isSetReplPolicy()) {
              // in case of replication, idempotent is taken care by getTargetTxnId
              LOG.warn("Invalid state ABORTED for transactions started using replication replay task");
              String s = "delete from REPL_TXN_MAP where RTM_SRC_TXN_ID = " + sourceTxnId +
                      " and RTM_REPL_POLICY = " + quoteString(rqst.getReplPolicy());
              LOG.info("Going to execute  <" + s + ">");
              stmt.executeUpdate(s);
            }
            LOG.info("abortTxn(" + JavaUtils.txnIdToString(txnid) +
              ") requested by it is already " + TxnStatus.ABORTED);
            return;
          }
          raiseTxnUnexpectedState(status, txnid);
        }

        if (rqst.isSetReplPolicy()) {
          String s = "delete from REPL_TXN_MAP where RTM_SRC_TXN_ID = " + sourceTxnId +
              " and RTM_REPL_POLICY = " + quoteString(rqst.getReplPolicy());
          LOG.info("Going to execute  <" + s + ">");
          stmt.executeUpdate(s);
        }

        if (transactionalListeners != null) {
          MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners,
                  EventMessage.EventType.ABORT_TXN, new AbortTxnEvent(txnid, null), dbConn, sqlGenerator);
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
  public void abortTxns(AbortTxnsRequest rqst) throws NoSuchTxnException, MetaException {
    List<Long> txnids = rqst.getTxn_ids();
    try {
      Connection dbConn = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        int numAborted = abortTxns(dbConn, txnids, false);
        if (numAborted != txnids.size()) {
          LOG.warn("Abort Transactions command only aborted " + numAborted + " out of " +
              txnids.size() + " transactions. It's possible that the other " +
              (txnids.size() - numAborted) +
              " transactions have been aborted or committed, or the transaction ids are invalid.");
        }

        for (Long txnId : txnids) {
          if (transactionalListeners != null) {
            MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners,
                    EventMessage.EventType.ABORT_TXN, new AbortTxnEvent(txnId, null), dbConn, sqlGenerator);
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
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      abortTxns(rqst);
    }
  }

  /**
   * Concurrency/isolation notes:
   * This is mutexed with {@link #openTxns(OpenTxnRequest)} and other {@link #commitTxn(CommitTxnRequest)}
   * operations using select4update on NEXT_TXN_ID.  Also, mutexes on TXNX table for specific txnid:X
   * see more notes below.
   * In order to prevent lost updates, we need to determine if any 2 transactions overlap.  Each txn
   * is viewed as an interval [M,N]. M is the txnid and N is taken from the same NEXT_TXN_ID sequence
   * so that we can compare commit time of txn T with start time of txn S.  This sequence can be thought of
   * as a logical time counter.  If S.commitTime < T.startTime, T and S do NOT overlap.
   *
   * Motivating example:
   * Suppose we have multi-statment transactions T and S both of which are attempting x = x + 1
   * In order to prevent lost update problem, the the non-overlapping txns must lock in the snapshot
   * that they read appropriately.  In particular, if txns do not overlap, then one follows the other
   * (assumig they write the same entity), and thus the 2nd must see changes of the 1st.  We ensure
   * this by locking in snapshot after 
   * {@link #openTxns(OpenTxnRequest)} call is made (see org.apache.hadoop.hive.ql.Driver.acquireLocksAndOpenTxn)
   * and mutexing openTxn() with commit().  In other words, once a S.commit() starts we must ensure
   * that txn T which will be considered a later txn, locks in a snapshot that includes the result
   * of S's commit (assuming no other txns).
   * As a counter example, suppose we have S[3,3] and T[4,4] (commitId=txnid means no other transactions
   * were running in parallel).  If T and S both locked in the same snapshot (for example commit of
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
      ResultSet lockHandle = null;
      ResultSet commitIdRs = null, rs;
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();

        if (rqst.isSetReplPolicy()) {
          sourceTxnId = rqst.getTxnid();
          List<Long> targetTxnIds = getTargetTxnIdList(rqst.getReplPolicy(),
                  Collections.singletonList(sourceTxnId), stmt);
          if (targetTxnIds.isEmpty()) {
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
        lockHandle = lockTransactionRecord(stmt, txnid, TXN_OPEN);
        if (lockHandle == null) {
          //if here, txn was not found (in expected state)
          TxnStatus actualTxnStatus = findTxnState(txnid, stmt);
          if(actualTxnStatus == TxnStatus.COMMITTED) {
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
          shouldNeverHappen(txnid);
          //dbConn is rolled back in finally{}
        }

        String conflictSQLSuffix = null;
        if (rqst.isSetReplPolicy()) {
          rs = null;
        } else {
          conflictSQLSuffix = "from TXN_COMPONENTS where tc_txnid=" + txnid + " and tc_operation_type IN(" +
                  quoteChar(OpertaionType.UPDATE.sqlConst) + "," + quoteChar(OpertaionType.DELETE.sqlConst) + ")";
          rs = stmt.executeQuery(sqlGenerator.addLimitClause(1,
                  "tc_operation_type " + conflictSQLSuffix));
        }
        if (rs != null && rs.next()) {
          isUpdateDelete = 'Y';
          close(rs);
          //if here it means currently committing txn performed update/delete and we should check WW conflict
          /**
           * This S4U will mutex with other commitTxn() and openTxns(). 
           * -1 below makes txn intervals look like [3,3] [4,4] if all txns are serial
           * Note: it's possible to have several txns have the same commit id.  Suppose 3 txns start
           * at the same time and no new txns start until all 3 commit.
           * We could've incremented the sequence for commitId is well but it doesn't add anything functionally.
           */
          commitIdRs = stmt.executeQuery(sqlGenerator.addForUpdateClause("select ntxn_next - 1 from NEXT_TXN_ID"));
          if (!commitIdRs.next()) {
            throw new IllegalStateException("No rows found in NEXT_TXN_ID");
          }
          long commitId = commitIdRs.getLong(1);
          Savepoint undoWriteSetForCurrentTxn = dbConn.setSavepoint();
          /**
           * "select distinct" is used below because
           * 1. once we get to multi-statement txns, we only care to record that something was updated once
           * 2. if {@link #addDynamicPartitions(AddDynamicPartitions)} is retried by caller it my create
           *  duplicate entries in TXN_COMPONENTS
           * but we want to add a PK on WRITE_SET which won't have unique rows w/o this distinct
           * even if it includes all of it's columns
           */
          int numCompsWritten = stmt.executeUpdate(
            "insert into WRITE_SET (ws_database, ws_table, ws_partition, ws_txnid, ws_commit_id, ws_operation_type)" +
            " select distinct tc_database, tc_table, tc_partition, tc_txnid, " + commitId + ", tc_operation_type " + conflictSQLSuffix);
          /**
           * see if there are any overlapping txns wrote the same element, i.e. have a conflict
           * Since entire commit operation is mutexed wrt other start/commit ops,
           * committed.ws_commit_id <= current.ws_commit_id for all txns
           * thus if committed.ws_commit_id < current.ws_txnid, transactions do NOT overlap
           * For example, [17,20] is committed, [6,80] is being committed right now - these overlap
           * [17,20] committed and [21,21] committing now - these do not overlap.
           * [17,18] committed and [18,19] committing now - these overlap  (here 18 started while 17 was still running)
           */
          rs = stmt.executeQuery
            (sqlGenerator.addLimitClause(1, "committed.ws_txnid, committed.ws_commit_id, committed.ws_database," +
              "committed.ws_table, committed.ws_partition, cur.ws_commit_id cur_ws_commit_id, " +
              "cur.ws_operation_type cur_op, committed.ws_operation_type committed_op " +
              "from WRITE_SET committed INNER JOIN WRITE_SET cur " +
              "ON committed.ws_database=cur.ws_database and committed.ws_table=cur.ws_table " +
              //For partitioned table we always track writes at partition level (never at table)
              //and for non partitioned - always at table level, thus the same table should never
              //have entries with partition key and w/o
              "and (committed.ws_partition=cur.ws_partition or (committed.ws_partition is null and cur.ws_partition is null)) " +
              "where cur.ws_txnid <= committed.ws_commit_id" + //txns overlap; could replace ws_txnid
              // with txnid, though any decent DB should infer this
              " and cur.ws_txnid=" + txnid + //make sure RHS of join only has rows we just inserted as
              // part of this commitTxn() op
              " and committed.ws_txnid <> " + txnid + //and LHS only has committed txns
              //U+U and U+D is a conflict but D+D is not and we don't currently track I in WRITE_SET at all
              " and (committed.ws_operation_type=" + quoteChar(OpertaionType.UPDATE.sqlConst) +
              " OR cur.ws_operation_type=" + quoteChar(OpertaionType.UPDATE.sqlConst) + ")"));
          if (rs.next()) {
            //found a conflict
            String committedTxn = "[" + JavaUtils.txnIdToString(rs.getLong(1)) + "," + rs.getLong(2) + "]";
            StringBuilder resource = new StringBuilder(rs.getString(3)).append("/").append(rs.getString(4));
            String partitionName = rs.getString(5);
            if (partitionName != null) {
              resource.append('/').append(partitionName);
            }
            String msg = "Aborting [" + JavaUtils.txnIdToString(txnid) + "," + rs.getLong(6) + "]" + " due to a write conflict on " + resource +
              " committed by " + committedTxn + " " + rs.getString(7) + "/" + rs.getString(8);
            close(rs);
            //remove WRITE_SET info for current txn since it's about to abort
            dbConn.rollback(undoWriteSetForCurrentTxn);
            LOG.info(msg);
            //todo: should make abortTxns() write something into TXNS.TXN_META_INFO about this
            if (abortTxns(dbConn, Collections.singletonList(txnid), true) != 1) {
              throw new IllegalStateException(msg + " FAILED!");
            }
            dbConn.commit();
            close(null, stmt, dbConn);
            throw new TxnAbortedException(msg);
          } else {
            //no conflicting operations, proceed with the rest of commit sequence
          }
        }
        else {
          /**
           * current txn didn't update/delete anything (may have inserted), so just proceed with commit
           *
           * We only care about commit id for write txns, so for RO (when supported) txns we don't
           * have to mutex on NEXT_TXN_ID.
           * Consider: if RO txn is after a W txn, then RO's openTxns() will be mutexed with W's
           * commitTxn() because both do S4U on NEXT_TXN_ID and thus RO will see result of W txn.
           * If RO < W, then there is no reads-from relationship.
           * In replication flow we don't expect any write write conflict as it should have been handled at source.
           */
        }

        String s;
        if (!rqst.isSetReplPolicy()) {
          // Move the record from txn_components into completed_txn_components so that the compactor
          // knows where to look to compact.
          s = "insert into COMPLETED_TXN_COMPONENTS (ctc_txnid, ctc_database, " +
                  "ctc_table, ctc_partition, ctc_writeid, ctc_update_delete) select tc_txnid, tc_database, tc_table, " +
                  "tc_partition, tc_writeid, '" + isUpdateDelete + "' from TXN_COMPONENTS where tc_txnid = " + txnid;
          LOG.debug("Going to execute insert <" + s + ">");

          if ((stmt.executeUpdate(s)) < 1) {
            //this can be reasonable for an empty txn START/COMMIT or read-only txn
            //also an IUD with DP that didn't match any rows.
            LOG.info("Expected to move at least one record from txn_components to " +
                    "completed_txn_components when committing txn! " + JavaUtils.txnIdToString(txnid));
          }
        } else {
          if (rqst.isSetWriteEventInfos()) {
            List<String> rows = new ArrayList<>();
            for (WriteEventInfo writeEventInfo : rqst.getWriteEventInfos()) {
              rows.add(txnid + "," + quoteString(writeEventInfo.getDatabase()) + "," +
                      quoteString(writeEventInfo.getTable()) + "," +
                      quoteString(writeEventInfo.getPartition()) + "," +
                      writeEventInfo.getWriteId() + "," +
                      "'" + isUpdateDelete + "'");
            }
            List<String> queries = sqlGenerator.createInsertValuesStmt("COMPLETED_TXN_COMPONENTS " +
                    "(ctc_txnid," + " ctc_database, ctc_table, ctc_partition, ctc_writeid, ctc_update_delete)", rows);
            for (String q : queries) {
              LOG.debug("Going to execute insert  <" + q + "> ");
              stmt.execute(q);
            }
          }

          s = "delete from REPL_TXN_MAP where RTM_SRC_TXN_ID = " + sourceTxnId +
                  " and RTM_REPL_POLICY = " + quoteString(rqst.getReplPolicy());
          LOG.info("Repl going to execute  <" + s + ">");
          stmt.executeUpdate(s);
        }

        // cleanup all txn related metadata
        s = "delete from TXN_COMPONENTS where tc_txnid = " + txnid;
        LOG.debug("Going to execute update <" + s + ">");
        stmt.executeUpdate(s);
        s = "delete from HIVE_LOCKS where hl_txnid = " + txnid;
        LOG.debug("Going to execute update <" + s + ">");
        stmt.executeUpdate(s);
        s = "delete from TXNS where txn_id = " + txnid;
        LOG.debug("Going to execute update <" + s + ">");
        stmt.executeUpdate(s);
        s = "delete from MIN_HISTORY_LEVEL where mhl_txnid = " + txnid;
        LOG.debug("Going to execute update <" + s + ">");
        stmt.executeUpdate(s);
        LOG.info("Removed committed transaction: (" + txnid + ") from MIN_HISTORY_LEVEL");

        s = "delete from MATERIALIZATION_REBUILD_LOCKS where mrl_txn_id = " + txnid;
        LOG.debug("Going to execute update <" + s + ">");
        stmt.executeUpdate(s);

        if (transactionalListeners != null) {
          MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners,
                  EventMessage.EventType.COMMIT_TXN, new CommitTxnEvent(txnid, null), dbConn, sqlGenerator);
        }

        LOG.debug("Going to commit");
        close(rs);
        dbConn.commit();
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "commitTxn(" + rqst + ")");
        throw new MetaException("Unable to update transaction database "
          + StringUtils.stringifyException(e));
      } finally {
        close(commitIdRs);
        close(lockHandle, stmt, dbConn);
        unlockInternal();
      }
    } catch (RetryException e) {
      commitTxn(rqst);
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
      ResultSet rs = null;
      TxnStore.MutexAPI.LockHandle handle = null;
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();

        // Check if this txn state is already replicated for this given table. If yes, then it is
        // idempotent case and just return.
        String sql = "select nwi_next from NEXT_WRITE_ID where nwi_database = " + quoteString(dbName)
                        + " and nwi_table = " + quoteString(tblName);
        LOG.debug("Going to execute query <" + sql + ">");

        rs = stmt.executeQuery(sql);
        if (rs.next()) {
          LOG.info("Idempotent flow: WriteId state <" + validWriteIdList + "> is already applied for the table: "
                  + dbName + "." + tblName);
          rollbackDBConn(dbConn);
          return;
        }

        if (numAbortedWrites > 0) {
          // Allocate/Map one txn per aborted writeId and abort the txn to mark writeid as aborted.
          List<Long> txnIds = openTxns(dbConn, stmt,
                  new OpenTxnRequest(numAbortedWrites, rqst.getUser(), rqst.getHostName()));
          assert(numAbortedWrites == txnIds.size());

          // Map each aborted write id with each allocated txn.
          List<String> rows = new ArrayList<>();
          int i = 0;
          for (long txn : txnIds) {
            long writeId = abortedWriteIds.get(i++);
            rows.add(txn + ", " + quoteString(dbName) + ", " + quoteString(tblName) + ", " + writeId);
            LOG.info("Allocated writeID: " + writeId + " for txnId: " + txn);
          }

          // Insert entries to TXN_TO_WRITE_ID for aborted write ids
          List<String> inserts = sqlGenerator.createInsertValuesStmt(
                  "TXN_TO_WRITE_ID (t2w_txnid, t2w_database, t2w_table, t2w_writeid)", rows);
          for (String insert : inserts) {
            LOG.debug("Going to execute insert <" + insert + ">");
            stmt.execute(insert);
          }

          // Abort all the allocated txns so that the mapped write ids are referred as aborted ones.
          int numAborts = abortTxns(dbConn, txnIds, true);
          assert(numAborts == numAbortedWrites);
        }
        handle = getMutexAPI().acquireLock(MUTEX_KEY.WriteIdAllocator.name());

        // There are some txns in the list which has no write id allocated and hence go ahead and do it.
        // Get the next write id for the given table and update it with new next write id.
        // It is expected NEXT_WRITE_ID doesn't have entry for this table and hence directly insert it.
        long nextWriteId = validWriteIdList.getHighWatermark() + 1;

        // First allocation of write id (hwm+1) should add the table to the next_write_id meta table.
        sql = "insert into NEXT_WRITE_ID (nwi_database, nwi_table, nwi_next) values ("
                + quoteString(dbName) + "," + quoteString(tblName) + ","
                + Long.toString(nextWriteId) + ")";
        LOG.debug("Going to execute insert <" + sql + ">");
        stmt.execute(sql);

        LOG.info("WriteId state <" + validWriteIdList + "> is applied for the table: " + dbName + "." + tblName);
        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "replTableWriteIdState(" + rqst + ")");
        throw new MetaException("Unable to update transaction database "
                + StringUtils.stringifyException(e));
      } finally {
        close(rs, stmt, dbConn);
        if(handle != null) {
          handle.releaseLocks();
        }
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
    List<Long> abortedWriteIds = new ArrayList<>();
    for (long writeId : validWriteIdList.getInvalidWriteIds()) {
      if (validWriteIdList.isWriteIdAborted(writeId)) {
        abortedWriteIds.add(writeId);
      }
    }
    return abortedWriteIds;
  }

  @Override
  @RetrySemantics.ReadOnly
  public GetValidWriteIdsResponse getValidWriteIds(GetValidWriteIdsRequest rqst)
          throws NoSuchTxnException, MetaException {
    try {
      Connection dbConn = null;
      Statement stmt = null;
      ValidTxnList validTxnList;

      // We should prepare the valid write ids list based on validTxnList of current txn.
      // If no txn exists in the caller, then they would pass null for validTxnList and so it is
      // required to get the current state of txns to make validTxnList
      if (rqst.isSetValidTxnList()) {
        validTxnList = new ValidReadTxnList(rqst.getValidTxnList());
      } else {
        // Passing 0 for currentTxn means, this validTxnList is not wrt to any txn
        validTxnList = TxnUtils.createValidReadTxnList(getOpenTxns(), 0);
      }
      try {
        /**
         * This runs at READ_COMMITTED for exactly the same reason as {@link #getOpenTxnsInfo()}
         */
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();

        // Get the valid write id list for all the tables read by the current txn
        List<TableValidWriteIds> tblValidWriteIdsList = new ArrayList<>();
        for (String fullTableName : rqst.getFullTableNames()) {
          tblValidWriteIdsList.add(getValidWriteIdsForTable(stmt, fullTableName, validTxnList));
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
        close(null, stmt, dbConn);
      }
    } catch (RetryException e) {
      return getValidWriteIds(rqst);
    }
  }

  // Method to get the Valid write ids list for the given table
  // Input fullTableName is expected to be of format <db_name>.<table_name>
  private TableValidWriteIds getValidWriteIdsForTable(Statement stmt, String fullTableName,
                                               ValidTxnList validTxnList) throws SQLException {
    ResultSet rs = null;
    String[] names = TxnUtils.getDbTableName(fullTableName);
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
      String s = "select max(t2w_writeid) from TXN_TO_WRITE_ID where t2w_txnid <= " + txnHwm
              + " and t2w_database = " + quoteString(names[0])
              + " and t2w_table = " + quoteString(names[1]);
      LOG.debug("Going to execute query<" + s + ">");
      rs = stmt.executeQuery(s);
      if (rs.next()) {
        writeIdHwm = rs.getLong(1);
      }

      // If no writeIds allocated by txns under txnHwm, then find writeHwm from NEXT_WRITE_ID.
      if (writeIdHwm <= 0) {
        // Need to subtract 1 as nwi_next would be the next write id to be allocated but we need highest
        // allocated write id.
        s = "select nwi_next-1 from NEXT_WRITE_ID where nwi_database = " + quoteString(names[0])
                + " and nwi_table = " + quoteString(names[1]);
        LOG.debug("Going to execute query<" + s + ">");
        rs = stmt.executeQuery(s);
        if (rs.next()) {
          long maxWriteId = rs.getLong(1);
          if (maxWriteId > 0) {
            writeIdHwm = (writeIdHwm > 0) ? Math.min(maxWriteId, writeIdHwm) : maxWriteId;
          }
        }
      }

      // As writeIdHwm is known, query all writeIds under the writeId HWM.
      // If any writeId under HWM is allocated by txn > txnId HWM or belongs to open/aborted txns,
      // then will be added to invalid list. The results should be sorted in ascending order based
      // on write id. The sorting is needed as exceptions list in ValidWriteIdList would be looked-up
      // using binary search.
      s = "select t2w_txnid, t2w_writeid from TXN_TO_WRITE_ID where t2w_writeid <= " + writeIdHwm
              + " and t2w_database = " + quoteString(names[0])
              + " and t2w_table = " + quoteString(names[1])
              + " order by t2w_writeid asc";

      LOG.debug("Going to execute query<" + s + ">");
      rs = stmt.executeQuery(s);
      while (rs.next()) {
        long txnId = rs.getLong(1);
        long writeId = rs.getLong(2);
        if (validTxnList.isTxnValid(txnId)) {
          // Skip if the transaction under evaluation is already committed.
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

      ByteBuffer byteBuffer = ByteBuffer.wrap(abortedBits.toByteArray());
      TableValidWriteIds owi = new TableValidWriteIds(fullTableName, writeIdHwm, invalidWriteIdList, byteBuffer);
      if (minOpenWriteId < Long.MAX_VALUE) {
        owi.setMinOpenWriteId(minOpenWriteId);
      }
      return owi;
    } finally {
      close(rs);
    }
  }

  @Override
  @RetrySemantics.Idempotent
  public AllocateTableWriteIdsResponse allocateTableWriteIds(AllocateTableWriteIdsRequest rqst)
          throws NoSuchTxnException, TxnAbortedException, MetaException {
    List<Long> txnIds;
    String dbName = rqst.getDbName().toLowerCase();
    String tblName = rqst.getTableName().toLowerCase();
    try {
      Connection dbConn = null;
      Statement stmt = null;
      ResultSet rs = null;
      TxnStore.MutexAPI.LockHandle handle = null;
      List<TxnToWriteId> txnToWriteIds = new ArrayList<>();
      List<TxnToWriteId> srcTxnToWriteIds = null;
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();

        if (rqst.isSetReplPolicy()) {
          srcTxnToWriteIds = rqst.getSrcTxnToWriteIdList();
          List<Long> srcTxnIds = new ArrayList<>();
          assert (rqst.isSetSrcTxnToWriteIdList());
          assert (!rqst.isSetTxnIds());
          assert (!srcTxnToWriteIds.isEmpty());

          for (TxnToWriteId txnToWriteId :  srcTxnToWriteIds) {
            srcTxnIds.add(txnToWriteId.getTxnId());
          }
          txnIds = getTargetTxnIdList(rqst.getReplPolicy(), srcTxnIds, stmt);
          if (srcTxnIds.size() != txnIds.size()) {
            LOG.warn("Target txn id is missing for source txn id : " + srcTxnIds.toString() +
                    " and repl policy " + rqst.getReplPolicy());
            throw new RuntimeException("This should never happen for txnIds: " + txnIds);
          }
        } else {
          assert (!rqst.isSetSrcTxnToWriteIdList());
          assert (rqst.isSetTxnIds());
          txnIds = rqst.getTxnIds();
        }

        Collections.sort(txnIds); //easier to read logs and for assumption done in replication flow

        // Check if all the input txns are in open state. Write ID should be allocated only for open transactions.
        if (!isTxnsInOpenState(txnIds, stmt)) {
          ensureAllTxnsValid(dbName, tblName, txnIds, stmt);
          throw new RuntimeException("This should never happen for txnIds: " + txnIds);
        }

        long writeId;
        String s;
        long allocatedTxnsCount = 0;
        long txnId;
        List<String> queries = new ArrayList<>();
        StringBuilder prefix = new StringBuilder();
        StringBuilder suffix = new StringBuilder();

        // Traverse the TXN_TO_WRITE_ID to see if any of the input txns already have allocated a
        // write id for the same db.table. If yes, then need to reuse it else have to allocate new one
        // The write id would have been already allocated in case of multi-statement txns where
        // first write on a table will allocate write id and rest of the writes should re-use it.
        prefix.append("select t2w_txnid, t2w_writeid from TXN_TO_WRITE_ID where"
                        + " t2w_database = " + quoteString(dbName)
                        + " and t2w_table = " + quoteString(tblName) + " and ");
        suffix.append("");
        TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix,
                txnIds, "t2w_txnid", false, false);
        for (String query : queries) {
          LOG.debug("Going to execute query <" + query + ">");
          rs = stmt.executeQuery(query);
          while (rs.next()) {
            // If table write ID is already allocated for the given transaction, then just use it
            txnId = rs.getLong(1);
            writeId = rs.getLong(2);
            txnToWriteIds.add(new TxnToWriteId(txnId, writeId));
            allocatedTxnsCount++;
            LOG.info("Reused already allocated writeID: " + writeId + " for txnId: " + txnId);
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

        handle = getMutexAPI().acquireLock(MUTEX_KEY.WriteIdAllocator.name());

        // There are some txns in the list which does not have write id allocated and hence go ahead and do it.
        // Get the next write id for the given table and update it with new next write id.
        // This is select for update query which takes a lock if the table entry is already there in NEXT_WRITE_ID
        s = sqlGenerator.addForUpdateClause(
                "select nwi_next from NEXT_WRITE_ID where nwi_database = " + quoteString(dbName)
                        + " and nwi_table = " + quoteString(tblName));
        LOG.debug("Going to execute query <" + s + ">");
        rs = stmt.executeQuery(s);
        if (!rs.next()) {
          // First allocation of write id should add the table to the next_write_id meta table
          // The initial value for write id should be 1 and hence we add 1 with number of write ids allocated here
          writeId = 1;
          s = "insert into NEXT_WRITE_ID (nwi_database, nwi_table, nwi_next) values ("
                  + quoteString(dbName) + "," + quoteString(tblName) + "," + Long.toString(numOfWriteIds + 1) + ")";
          LOG.debug("Going to execute insert <" + s + ">");
          stmt.execute(s);
        } else {
          writeId = rs.getLong(1);
          // Update the NEXT_WRITE_ID for the given table after incrementing by number of write ids allocated
          s = "update NEXT_WRITE_ID set nwi_next = " + (writeId + numOfWriteIds)
                  + " where nwi_database = " + quoteString(dbName)
                  + " and nwi_table = " + quoteString(tblName);
          LOG.debug("Going to execute update <" + s + ">");
          stmt.executeUpdate(s);
        }

        // Map the newly allocated write ids against the list of txns which doesn't have pre-allocated
        // write ids
        List<String> rows = new ArrayList<>();
        for (long txn : txnIds) {
          rows.add(txn + ", " + quoteString(dbName) + ", " + quoteString(tblName) + ", " + writeId);
          txnToWriteIds.add(new TxnToWriteId(txn, writeId));
          LOG.info("Allocated writeID: " + writeId + " for txnId: " + txn);
          writeId++;
        }

        if (rqst.isSetReplPolicy()) {
          int lastIdx = txnToWriteIds.size()-1;
          if ((txnToWriteIds.get(0).getWriteId() != srcTxnToWriteIds.get(0).getWriteId()) ||
              (txnToWriteIds.get(lastIdx).getWriteId() != srcTxnToWriteIds.get(lastIdx).getWriteId())) {
            LOG.error("Allocated write id range {} is not matching with the input write id range {}.",
                    txnToWriteIds, srcTxnToWriteIds);
            throw new IllegalStateException("Write id allocation failed for: " + srcTxnToWriteIds);
          }
        }

        // Insert entries to TXN_TO_WRITE_ID for newly allocated write ids
        List<String> inserts = sqlGenerator.createInsertValuesStmt(
                "TXN_TO_WRITE_ID (t2w_txnid, t2w_database, t2w_table, t2w_writeid)", rows);
        for (String insert : inserts) {
          LOG.debug("Going to execute insert <" + insert + ">");
          stmt.execute(insert);
        }

        if (transactionalListeners != null) {
          MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners,
                  EventMessage.EventType.ALLOC_WRITE_ID,
                  new AllocWriteIdEvent(txnToWriteIds, rqst.getDbName(), rqst.getTableName(), null),
                  dbConn, sqlGenerator);
        }

        LOG.debug("Going to commit");
        dbConn.commit();
        return new AllocateTableWriteIdsResponse(txnToWriteIds);
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "allocateTableWriteIds(" + rqst + ")");
        throw new MetaException("Unable to update transaction database "
                + StringUtils.stringifyException(e));
      } finally {
        close(rs, stmt, dbConn);
        if(handle != null) {
          handle.releaseLocks();
        }
        unlockInternal();
      }
    } catch (RetryException e) {
      return allocateTableWriteIds(rqst);
    }
  }
  @Override
  public void seedWriteIdOnAcidConversion(InitializeTableWriteIdsRequest rqst)
      throws MetaException {
    try {
      Connection dbConn = null;
      Statement stmt = null;
      TxnStore.MutexAPI.LockHandle handle = null;
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();

        handle = getMutexAPI().acquireLock(MUTEX_KEY.WriteIdAllocator.name());
        //since this is on conversion from non-acid to acid, NEXT_WRITE_ID should not have an entry
        //for this table.  It also has a unique index in case 'should not' is violated

        // First allocation of write id should add the table to the next_write_id meta table
        // The initial value for write id should be 1 and hence we add 1 with number of write ids
        // allocated here
        String s = "insert into NEXT_WRITE_ID (nwi_database, nwi_table, nwi_next) values ("
            + quoteString(rqst.getDbName()) + "," + quoteString(rqst.getTblName()) + "," +
            Long.toString(rqst.getSeeWriteId() + 1) + ")";
        LOG.debug("Going to execute insert <" + s + ">");
        stmt.execute(s);
        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "seedWriteIdOnAcidConversion(" + rqst + ")");
        throw new MetaException("Unable to update transaction database "
            + StringUtils.stringifyException(e));
      } finally {
        close(null, stmt, dbConn);
        if(handle != null) {
          handle.releaseLocks();
        }
        unlockInternal();
      }
    } catch (RetryException e) {
      seedWriteIdOnAcidConversion(rqst);
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
  public void performWriteSetGC() {
    Connection dbConn = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
      stmt = dbConn.createStatement();
      rs = stmt.executeQuery("select ntxn_next - 1 from NEXT_TXN_ID");
      if(!rs.next()) {
        throw new IllegalStateException("NEXT_TXN_ID is empty: DB is corrupted");
      }
      long highestAllocatedTxnId = rs.getLong(1);
      close(rs);
      rs = stmt.executeQuery("select min(txn_id) from TXNS where txn_state=" + quoteChar(TXN_OPEN));
      if(!rs.next()) {
        throw new IllegalStateException("Scalar query returned no rows?!?!!");
      }
      long commitHighWaterMark;//all currently open txns (if any) have txnid >= than commitHighWaterMark
      long lowestOpenTxnId = rs.getLong(1);
      if(rs.wasNull()) {
        //if here then there are no Open txns and  highestAllocatedTxnId must be
        //resolved (i.e. committed or aborted), either way
        //there are no open txns with id <= highestAllocatedTxnId
        //the +1 is there because "delete ..." below has < (which is correct for the case when
        //there is an open txn
        //Concurrency: even if new txn starts (or starts + commits) it is still true that
        //there are no currently open txns that overlap with any committed txn with 
        //commitId <= commitHighWaterMark (as set on next line).  So plain READ_COMMITTED is enough.
        commitHighWaterMark = highestAllocatedTxnId + 1;
      }
      else {
        commitHighWaterMark = lowestOpenTxnId;
      }
      int delCnt = stmt.executeUpdate("delete from WRITE_SET where ws_commit_id < " + commitHighWaterMark);
      LOG.info("Deleted " + delCnt + " obsolete rows from WRTIE_SET");
      dbConn.commit();
    } catch (SQLException ex) {
      LOG.warn("WriteSet GC failed due to " + getMessage(ex), ex);
    }
    finally {
      close(rs, stmt, dbConn);
    }
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

    // Parse validTxnList
    final ValidReadTxnList validTxnList =
        new ValidReadTxnList(validTxnListStr);

    // Parse validReaderWriteIdList from creation metadata
    final ValidTxnWriteIdList validReaderWriteIdList =
        new ValidTxnWriteIdList(creationMetadata.getValidTxnList());

    // We are composing a query that returns a single row if an update happened after
    // the materialization was created. Otherwise, query returns 0 rows.
    Connection dbConn = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
      stmt = dbConn.createStatement();
      stmt.setMaxRows(1);
      StringBuilder query = new StringBuilder();
      // compose a query that select transactions containing an update...
      query.append("select ctc_update_delete from COMPLETED_TXN_COMPONENTS where ctc_update_delete='Y' AND (");
      int i = 0;
      for (String fullyQualifiedName : creationMetadata.getTablesUsed()) {
        // ...for each of the tables that are part of the materialized view,
        // where the transaction had to be committed after the materialization was created...
        if (i != 0) {
          query.append("OR");
        }
        String[] names = TxnUtils.getDbTableName(fullyQualifiedName);
        query.append(" (ctc_database=" + quoteString(names[0]) + " AND ctc_table=" + quoteString(names[1]));
        ValidWriteIdList tblValidWriteIdList =
            validReaderWriteIdList.getTableValidWriteIdList(fullyQualifiedName);
        if (tblValidWriteIdList == null) {
          LOG.warn("ValidWriteIdList for table {} not present in creation metadata, this should not happen");
          return null;
        }
        query.append(" AND (ctc_writeid > " + tblValidWriteIdList.getHighWatermark());
        query.append(tblValidWriteIdList.getInvalidWriteIds().length == 0 ? ") " :
            " OR ctc_writeid IN(" + StringUtils.join(",",
                Arrays.asList(ArrayUtils.toObject(tblValidWriteIdList.getInvalidWriteIds()))) + ") ");
        query.append(") ");
        i++;
      }
      // ... and where the transaction has already been committed as per snapshot taken
      // when we are running current query
      query.append(") AND ctc_txnid <= " + validTxnList.getHighWatermark());
      query.append(validTxnList.getInvalidTransactions().length == 0 ? " " :
          " AND ctc_txnid NOT IN(" + StringUtils.join(",",
              Arrays.asList(ArrayUtils.toObject(validTxnList.getInvalidTransactions()))) + ") ");

      // Execute query
      String s = query.toString();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to execute query <" + s + ">");
      }
      rs = stmt.executeQuery(s);

      return new Materialization(rs.next());
    } catch (SQLException ex) {
      LOG.warn("getMaterializationInvalidationInfo failed due to " + getMessage(ex), ex);
      throw new MetaException("Unable to retrieve materialization invalidation information due to " +
          StringUtils.stringifyException(ex));
    } finally {
      close(rs, stmt, dbConn);
    }
  }

  @Override
  public LockResponse lockMaterializationRebuild(String dbName, String tableName, long txnId)
      throws MetaException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Acquiring lock for materialization rebuild with txnId={} for {}", txnId, Warehouse.getQualifiedName(dbName,tableName));
    }

    TxnStore.MutexAPI.LockHandle handle = null;
    Connection dbConn = null;
    Statement stmt = null;
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
      stmt = dbConn.createStatement();

      String selectQ = "select mrl_txn_id from MATERIALIZATION_REBUILD_LOCKS where" +
          " mrl_db_name =" + quoteString(dbName) +
          " AND mrl_tbl_name=" + quoteString(tableName);
      LOG.debug("Going to execute query <" + selectQ + ">");
      rs = stmt.executeQuery(selectQ);
      if(rs.next()) {
        LOG.info("Ignoring request to rebuild " + dbName + "/" + tableName +
            " since it is already being rebuilt");
        return new LockResponse(txnId, LockState.NOT_ACQUIRED);
      }
      String insertQ = "insert into MATERIALIZATION_REBUILD_LOCKS " +
          "(mrl_txn_id, mrl_db_name, mrl_tbl_name, mrl_last_heartbeat) values (" + txnId +
          ", '" + dbName + "', '" + tableName + "', " + Instant.now().toEpochMilli() + ")";
      LOG.debug("Going to execute update <" + insertQ + ">");
      stmt.executeUpdate(insertQ);
      LOG.debug("Going to commit");
      dbConn.commit();
      return new LockResponse(txnId, LockState.ACQUIRED);
    } catch (SQLException ex) {
      LOG.warn("lockMaterializationRebuild failed due to " + getMessage(ex), ex);
      throw new MetaException("Unable to retrieve materialization invalidation information due to " +
          StringUtils.stringifyException(ex));
    } finally {
      close(rs, stmt, dbConn);
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
      Statement stmt = null;
      ResultSet rs = null;
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        String s = "update MATERIALIZATION_REBUILD_LOCKS" +
            " set mrl_last_heartbeat = " + Instant.now().toEpochMilli() +
            " where mrl_txn_id = " + txnId +
            " AND mrl_db_name =" + quoteString(dbName) +
            " AND mrl_tbl_name=" + quoteString(tableName);
        LOG.debug("Going to execute update <" + s + ">");
        int rc = stmt.executeUpdate(s);
        if (rc < 1) {
          LOG.debug("Going to rollback");
          dbConn.rollback();
          LOG.info("No lock found for rebuild of " + Warehouse.getQualifiedName(dbName, tableName) +
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
            "heartbeatLockMaterializationRebuild(" + Warehouse.getQualifiedName(dbName, tableName) + ", " + txnId + ")");
        throw new MetaException("Unable to heartbeat rebuild lock due to " +
            StringUtils.stringifyException(e));
      } finally {
        close(rs, stmt, dbConn);
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

        String selectQ = "select mrl_txn_id, mrl_last_heartbeat from MATERIALIZATION_REBUILD_LOCKS";
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
          String deleteQ = "delete from MATERIALIZATION_REBUILD_LOCKS where" +
              " mrl_txn_id IN(" + StringUtils.join(",", txnIds) + ") ";
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
   * connection (but separate transactions).  This avoid some flakiness in BONECP where if you
   * perform an operation on 1 connection and immediately get another from the pool, the 2nd one
   * doesn't see results of the first.
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
      return checkLockWithRetry(connAndLockId.dbConn, connAndLockId.extLockId, rqst.getTxnid());
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
   * There is no real reason to return the ResultSet here other than to make sure the reference to it
   * is retained for duration of intended lock scope and is not GC'd thus (unlikely) causing lock
   * to be released.
   * @param txnState the state this txn is expected to be in.  may be null
   * @return null if no row was found
   * @throws SQLException
   * @throws MetaException
   */
  private ResultSet lockTransactionRecord(Statement stmt, long txnId, Character txnState) throws SQLException, MetaException {
    String query = "select TXN_STATE from TXNS where TXN_ID = " + txnId + (txnState != null ? " AND TXN_STATE=" + quoteChar(txnState) : "");
    ResultSet rs = stmt.executeQuery(sqlGenerator.addForUpdateClause(query));
    if(rs.next()) {
      return rs;
    }
    close(rs);
    return null;
  }

  /**
   * This enters locks into the queue in {@link #LOCK_WAITING} mode.
   *
   * Isolation Level Notes:
   * 1. We use S4U (withe read_committed) to generate the next (ext) lock id.  This serializes
   * any 2 {@code enqueueLockWithRetry()} calls.
   * 2. We use S4U on the relevant TXNS row to block any concurrent abort/commit/etc operations
   * @see #checkLockWithRetry(Connection, long, long)
   */
  private ConnectionLockIdPair enqueueLockWithRetry(LockRequest rqst) throws NoSuchTxnException, TxnAbortedException, MetaException {
    boolean success = false;
    Connection dbConn = null;
    try {
      Statement stmt = null;
      ResultSet rs = null;
      ResultSet lockHandle = null;
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        long txnid = rqst.getTxnid();
        stmt = dbConn.createStatement();
        if (isValidTxn(txnid)) {
          //this also ensures that txn is still there in expected state
          lockHandle = lockTransactionRecord(stmt, txnid, TXN_OPEN);
          if(lockHandle == null) {
            ensureValidTxn(dbConn, txnid, stmt);
            shouldNeverHappen(txnid);
          }
        }
        /** Get the next lock id.
         * This has to be atomic with adding entries to HIVE_LOCK entries (1st add in W state) to prevent a race.
         * Suppose ID gen is a separate txn and 2 concurrent lock() methods are running.  1st one generates nl_next=7,
         * 2nd nl_next=8.  Then 8 goes first to insert into HIVE_LOCKS and acquires the locks.  Then 7 unblocks,
         * and add it's W locks but it won't see locks from 8 since to be 'fair' {@link #checkLock(java.sql.Connection, long)}
         * doesn't block on locks acquired later than one it's checking*/
        String s = sqlGenerator.addForUpdateClause("select nl_next from NEXT_LOCK_ID");
        LOG.debug("Going to execute query <" + s + ">");
        rs = stmt.executeQuery(s);
        if (!rs.next()) {
          LOG.debug("Going to rollback");
          dbConn.rollback();
          throw new MetaException("Transaction tables not properly " +
            "initialized, no record found in next_lock_id");
        }
        long extLockId = rs.getLong(1);
        s = "update NEXT_LOCK_ID set nl_next = " + (extLockId + 1);
        LOG.debug("Going to execute update <" + s + ">");
        stmt.executeUpdate(s);

        if (txnid > 0) {
          List<String> rows = new ArrayList<>();
          // For each component in this lock request,
          // add an entry to the txn_components table
          for (LockComponent lc : rqst.getComponent()) {
            if(lc.isSetIsTransactional() && !lc.isIsTransactional()) {
              //we don't prevent using non-acid resources in a txn but we do lock them
              continue;
            }
            boolean updateTxnComponents;
            if(!lc.isSetOperationType()) {
              //request came from old version of the client
              updateTxnComponents = true;//this matches old behavior
            }
            else {
              switch (lc.getOperationType()) {
                case INSERT:
                case UPDATE:
                case DELETE:
                  if(!lc.isSetIsDynamicPartitionWrite()) {
                    //must be old client talking, i.e. we don't know if it's DP so be conservative
                    updateTxnComponents = true;
                  }
                  else {
                    /**
                     * we know this is part of DP operation and so we'll get
                     * {@link #addDynamicPartitions(AddDynamicPartitions)} call with the list
                     * of partitions actually chaged.
                     */
                    updateTxnComponents = !lc.isIsDynamicPartitionWrite();
                  }
                  break;
                case SELECT:
                  updateTxnComponents = false;
                  break;
                case NO_TXN:
                  /*this constant is a bit of a misnomer since we now always have a txn context.  It
                   just means the operation is such that we don't care what tables/partitions it
                   affected as it doesn't trigger a compaction or conflict detection.  A better name
                   would be NON_TRANSACTIONAL.*/
                  updateTxnComponents = false;
                  break;
                default:
                  //since we have an open transaction, only 4 values above are expected 
                  throw new IllegalStateException("Unexpected DataOperationType: " + lc.getOperationType()
                    + " agentInfo=" + rqst.getAgentInfo() + " " + JavaUtils.txnIdToString(txnid));
              }
            }
            if(!updateTxnComponents) {
              continue;
            }
            String dbName = normalizeCase(lc.getDbname());
            String tblName = normalizeCase(lc.getTablename());
            String partName = normalizeCase(lc.getPartitionname());
            Long writeId = null;
            if (tblName != null) {
              // It is assumed the caller have already allocated write id for adding/updating data to
              // the acid tables. However, DDL operatons won't allocate write id and hence this query
              // may return empty result sets.
              // Get the write id allocated by this txn for the given table writes
              s = "select t2w_writeid from TXN_TO_WRITE_ID where"
                      + " t2w_database = " + quoteString(dbName)
                      + " and t2w_table = " + quoteString(tblName)
                      + " and t2w_txnid = " + txnid;
              LOG.debug("Going to execute query <" + s + ">");
              rs = stmt.executeQuery(s);
              if (rs.next()) {
                writeId = rs.getLong(1);
              }
            }
            rows.add(txnid + ", '" + dbName + "', " +
                    (tblName == null ? "null" : "'" + tblName + "'") + ", " +
                    (partName == null ? "null" : "'" + partName + "'")+ "," +
                    quoteString(OpertaionType.fromDataOperationType(lc.getOperationType()).toString())+ "," +
                    (writeId == null ? "null" : writeId));
          }
          List<String> queries = sqlGenerator.createInsertValuesStmt(
              "TXN_COMPONENTS (tc_txnid, tc_database, tc_table, tc_partition, tc_operation_type, tc_writeid)", rows);
          for(String query : queries) {
            LOG.debug("Going to execute update <" + query + ">");
            int modCount = stmt.executeUpdate(query);
          }
        }

        List<String> rows = new ArrayList<>();
        long intLockId = 0;
        for (LockComponent lc : rqst.getComponent()) {
          if(lc.isSetOperationType() && lc.getOperationType() == DataOperationType.UNSET &&
            (MetastoreConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST) || MetastoreConf.getBoolVar(conf, ConfVars.HIVE_IN_TEZ_TEST))) {
            //old version of thrift client should have (lc.isSetOperationType() == false) but they do not
            //If you add a default value to a variable, isSet() for that variable is true regardless of the where the
            //message was created (for object variables.
            // It works correctly for boolean vars, e.g. LockComponent.isTransactional).
            //in test mode, upgrades are not tested, so client version and server version of thrift always matches so
            //we see UNSET here it means something didn't set the appropriate value.
            throw new IllegalStateException("Bug: operationType=" + lc.getOperationType() + " for component "
              + lc + " agentInfo=" + rqst.getAgentInfo());
          }
          intLockId++;
          String dbName = normalizeCase(lc.getDbname());
          String tblName = normalizeCase(lc.getTablename());
          String partName = normalizeCase(lc.getPartitionname());
          LockType lockType = lc.getType();
          char lockChar = 'z';
          switch (lockType) {
            case EXCLUSIVE:
              lockChar = LOCK_EXCLUSIVE;
              break;
            case SHARED_READ:
              lockChar = LOCK_SHARED;
              break;
            case SHARED_WRITE:
              lockChar = LOCK_SEMI_SHARED;
              break;
          }
          long now = getDbTime(dbConn);
            rows.add(extLockId + ", " + intLockId + "," + txnid + ", " +
            quoteString(dbName) + ", " +
            valueOrNullLiteral(tblName) + ", " +
            valueOrNullLiteral(partName) + ", " +
            quoteChar(LOCK_WAITING) + ", " + quoteChar(lockChar) + ", " +
            //for locks associated with a txn, we always heartbeat txn and timeout based on that
            (isValidTxn(txnid) ? 0 : now) + ", " +
            valueOrNullLiteral(rqst.getUser()) + ", " +
            valueOrNullLiteral(rqst.getHostname()) + ", " +
            valueOrNullLiteral(rqst.getAgentInfo()));// + ")";
        }
        List<String> queries = sqlGenerator.createInsertValuesStmt(
          "HIVE_LOCKS (hl_lock_ext_id, hl_lock_int_id, hl_txnid, hl_db, " +
            "hl_table, hl_partition,hl_lock_state, hl_lock_type, " +
            "hl_last_heartbeat, hl_user, hl_host, hl_agent_info)", rows);
        for(String query : queries) {
          LOG.debug("Going to execute update <" + query + ">");
          int modCount = stmt.executeUpdate(query);
        }
        dbConn.commit();
        success = true;
        return new ConnectionLockIdPair(dbConn, extLockId);
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "enqueueLockWithRetry(" + rqst + ")");
        throw new MetaException("Unable to update transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        close(lockHandle);
        close(rs, stmt, null);
        if (!success) {
          /* This needs to return a "live" connection to be used by operation that follows it.
          Thus it only closes Connection on failure/retry. */
          closeDbConn(dbConn);
        }
        unlockInternal();
      }
    }
    catch(RetryException e) {
      return enqueueLockWithRetry(rqst);
    }
  }
  private static String normalizeCase(String s) {
    return s == null ? null : s.toLowerCase();
  }

  private LockResponse checkLockWithRetry(Connection dbConn, long extLockId, long txnId)
    throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, MetaException {
    try {
      try {
        lockInternal();
        if(dbConn.isClosed()) {
          //should only get here if retrying this op
          dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        }
        return checkLock(dbConn, extLockId);
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
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
      return checkLockWithRetry(dbConn, extLockId, txnId);
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
   * {@link #checkLock(java.sql.Connection, long)}  must run at SERIALIZABLE (make sure some lock we are checking
   * against doesn't move from W to A in another txn) but this method can heartbeat in
   * separate txn at READ_COMMITTED.
   * 
   * Retry-by-caller note:
   * Retryable because {@link #checkLock(Connection, long)} is
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
        LockInfo info = getTxnIdFromLockId(dbConn, extLockId);
        if(info == null) {
          throw new NoSuchLockException("No such lock " + JavaUtils.lockIdToString(extLockId));
        }
        if (info.txnId > 0) {
          heartbeatTxn(dbConn, info.txnId);
        }
        else {
          heartbeatLock(dbConn, extLockId);
        }
        //todo: strictly speaking there is a bug here.  heartbeat*() commits but both heartbeat and
        //checkLock() are in the same retry block, so if checkLock() throws, heartbeat is also retired
        //extra heartbeat is logically harmless, but ...
        return checkLock(dbConn, extLockId);
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "checkLock(" + rqst + " )");
        throw new MetaException("Unable to update transaction database " +
          JavaUtils.lockIdToString(extLockId) + " " + StringUtils.stringifyException(e));
      } finally {
        closeDbConn(dbConn);
        unlockInternal();
      }
    } catch (RetryException e) {
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
  public void unlock(UnlockRequest rqst)
    throws NoSuchLockException, TxnOpenException, MetaException {
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
        String s = "delete from HIVE_LOCKS where hl_lock_ext_id = " + extLockId + " AND (hl_txnid = 0 OR" +
          " (hl_txnid <> 0 AND hl_lock_state = '" + LOCK_WAITING + "'))";
        //(hl_txnid <> 0 AND hl_lock_state = '" + LOCK_WAITING + "') is for multi-statement txns where
        //some query attempted to lock (thus LOCK_WAITING state) but is giving up due to timeout for example
        LOG.debug("Going to execute update <" + s + ">");
        int rc = stmt.executeUpdate(s);
        if (rc < 1) {
          LOG.debug("Going to rollback");
          dbConn.rollback();
          LockInfo info = getTxnIdFromLockId(dbConn, extLockId);
          if(info == null) {
            //didn't find any lock with extLockId but at ReadCommitted there is a possibility that
            //it existed when above delete ran but it didn't have the expected state.
            LOG.info("No lock in " + LOCK_WAITING + " mode found for unlock(" +
              JavaUtils.lockIdToString(rqst.getLockid()) + ")");
            //bail here to make the operation idempotent
            return;
          }
          if(info.txnId != 0) {
            String msg = "Unlocking locks associated with transaction not permitted.  " + info;
            //if a lock is associated with a txn we can only "unlock" if if it's in WAITING state
            // which really means that the caller wants to give up waiting for the lock
            LOG.error(msg);
            throw new TxnOpenException(msg);
          }
          if(info.txnId == 0) {
            //we didn't see this lock when running DELETE stmt above but now it showed up
            //so should "should never happen" happened...
            String msg = "Found lock in unexpected state " + info;
            LOG.error(msg);
            throw new MetaException(msg);
          }
        }
        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
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
      Statement stmt = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();

        String s = "select hl_lock_ext_id, hl_txnid, hl_db, hl_table, hl_partition, hl_lock_state, " +
          "hl_lock_type, hl_last_heartbeat, hl_acquired_at, hl_user, hl_host, hl_lock_int_id," +
          "hl_blockedby_ext_id, hl_blockedby_int_id, hl_agent_info from HIVE_LOCKS";

        // Some filters may have been specified in the SHOW LOCKS statement. Add them to the query.
        String dbName = rqst.getDbname();
        String tableName = rqst.getTablename();
        String partName = rqst.getPartname();

        StringBuilder filter = new StringBuilder();
        if (dbName != null && !dbName.isEmpty()) {
          filter.append("hl_db=").append(quoteString(dbName));
        }
        if (tableName != null && !tableName.isEmpty()) {
          if (filter.length() > 0) {
            filter.append(" and ");
          }
          filter.append("hl_table=").append(quoteString(tableName));
        }
        if (partName != null && !partName.isEmpty()) {
          if (filter.length() > 0) {
            filter.append(" and ");
          }
          filter.append("hl_partition=").append(quoteString(partName));
        }
        String whereClause = filter.toString();

        if (!whereClause.isEmpty()) {
          s = s + " where " + whereClause;
        }

        LOG.debug("Doing to execute query <" + s + ">");
        ResultSet rs = stmt.executeQuery(s);
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
          switch (rs.getString(7).charAt(0)) {
            case LOCK_SEMI_SHARED: e.setType(LockType.SHARED_WRITE); break;
            case LOCK_EXCLUSIVE: e.setType(LockType.EXCLUSIVE); break;
            case LOCK_SHARED: e.setType(LockType.SHARED_READ); break;
            default: throw new MetaException("Unknown lock type " + rs.getString(6).charAt(0));
          }
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
        closeStmt(stmt);
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
          new StringBuilder("update TXNS set txn_last_heartbeat = " + getDbTime(dbConn) +
            " where txn_state = " + quoteChar(TXN_OPEN) + " and "),
          new StringBuilder(""), txnIds, "txn_id", true, false);
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
    String s = sqlGenerator.addForUpdateClause("select ncq_next from NEXT_COMPACTION_QUEUE_ID");
    LOG.debug("going to execute query <" + s + ">");
    try (ResultSet rs = stmt.executeQuery(s)) {
      if (!rs.next()) {
        throw new IllegalStateException("Transaction tables not properly initiated, "
            + "no record found in next_compaction_queue_id");
      }
      long id = rs.getLong(1);
      s = "update NEXT_COMPACTION_QUEUE_ID set ncq_next = " + (id + 1);
      LOG.debug("Going to execute update <" + s + ">");
      stmt.executeUpdate(s);
      return id;
    }
  }
  @Override
  @RetrySemantics.Idempotent
  public CompactionResponse compact(CompactionRequest rqst) throws MetaException {
    // Put a compaction request in the queue.
    try {
      Connection dbConn = null;
      Statement stmt = null;
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

        StringBuilder sb = new StringBuilder("select cq_id, cq_state from COMPACTION_QUEUE where").
          append(" cq_state IN(").append(quoteChar(INITIATED_STATE)).
            append(",").append(quoteChar(WORKING_STATE)).
          append(") AND cq_database=").append(quoteString(rqst.getDbname())).
          append(" AND cq_table=").append(quoteString(rqst.getTablename())).append(" AND ");
        if(rqst.getPartitionname() == null) {
          sb.append("cq_partition is null");
        }
        else {
          sb.append("cq_partition=").append(quoteString(rqst.getPartitionname()));
        }

        LOG.debug("Going to execute query <" + sb.toString() + ">");
        ResultSet rs = stmt.executeQuery(sb.toString());
        if(rs.next()) {
          long enqueuedId = rs.getLong(1);
          String state = compactorStateToResponse(rs.getString(2).charAt(0));
          LOG.info("Ignoring request to compact " + rqst.getDbname() + "/" + rqst.getTablename() +
            "/" + rqst.getPartitionname() + " since it is already " + quoteString(state) +
            " with id=" + enqueuedId);
          return new CompactionResponse(enqueuedId, state, false);
        }
        close(rs);
        StringBuilder buf = new StringBuilder("insert into COMPACTION_QUEUE (cq_id, cq_database, " +
          "cq_table, ");
        String partName = rqst.getPartitionname();
        if (partName != null) buf.append("cq_partition, ");
        buf.append("cq_state, cq_type");
        if (rqst.getProperties() != null) {
          buf.append(", cq_tblproperties");
        }
        if (rqst.getRunas() != null) buf.append(", cq_run_as");
        buf.append(") values (");
        buf.append(id);
        buf.append(", '");
        buf.append(rqst.getDbname());
        buf.append("', '");
        buf.append(rqst.getTablename());
        buf.append("', '");
        if (partName != null) {
          buf.append(partName);
          buf.append("', '");
        }
        buf.append(INITIATED_STATE);
        buf.append("', '");
        switch (rqst.getType()) {
          case MAJOR:
            buf.append(MAJOR_TYPE);
            break;

          case MINOR:
            buf.append(MINOR_TYPE);
            break;

          default:
            LOG.debug("Going to rollback");
            dbConn.rollback();
            throw new MetaException("Unexpected compaction type " + rqst.getType().toString());
        }
        if (rqst.getProperties() != null) {
          buf.append("', '");
          buf.append(new StringableMap(rqst.getProperties()).toString());
        }
        if (rqst.getRunas() != null) {
          buf.append("', '");
          buf.append(rqst.getRunas());
        }
        buf.append("')");
        String s = buf.toString();
        LOG.debug("Going to execute update <" + s + ">");
        stmt.executeUpdate(s);
        LOG.debug("Going to commit");
        dbConn.commit();
        return new CompactionResponse(id, INITIATED_RESPONSE, true);
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "compact(" + rqst + ")");
        throw new MetaException("Unable to select from transaction database " +
          StringUtils.stringifyException(e));
      } finally {
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
        String s = "select cq_database, cq_table, cq_partition, cq_state, cq_type, cq_worker_id, " +
          //-1 because 'null' literal doesn't work for all DBs...
          "cq_start, -1 cc_end, cq_run_as, cq_hadoop_job_id, cq_id from COMPACTION_QUEUE union all " +
          "select cc_database, cc_table, cc_partition, cc_state, cc_type, cc_worker_id, " +
          "cc_start, cc_end, cc_run_as, cc_hadoop_job_id, cc_id from COMPLETED_COMPACTIONS";
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
          switch (rs.getString(5).charAt(0)) {
            case MAJOR_TYPE: e.setType(CompactionType.MAJOR); break;
            case MINOR_TYPE: e.setType(CompactionType.MINOR); break;
            default:
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
    ResultSet lockHandle = null;
    try {
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        lockHandle = lockTransactionRecord(stmt, rqst.getTxnid(), TXN_OPEN);
        if(lockHandle == null) {
          //ensures txn is still there and in expected state
          ensureValidTxn(dbConn, rqst.getTxnid(), stmt);
          shouldNeverHappen(rqst.getTxnid());
        }
        //for RU this may be null so we should default it to 'u' which is most restrictive
        OpertaionType ot = OpertaionType.UPDATE;
        if(rqst.isSetOperationType()) {
          ot = OpertaionType.fromDataOperationType(rqst.getOperationType());
        }

        Long writeId = rqst.getWriteid();
        List<String> rows = new ArrayList<>();
        for (String partName : rqst.getPartitionnames()) {
          rows.add(rqst.getTxnid() + "," + quoteString(normalizeCase(rqst.getDbname()))
              + "," + quoteString(normalizeCase(rqst.getTablename())) +
              "," + quoteString(partName) + "," + quoteChar(ot.sqlConst) + "," + writeId);
        }
        int modCount = 0;
        //record partitions that were written to
        List<String> queries = sqlGenerator.createInsertValuesStmt(
            "TXN_COMPONENTS (tc_txnid, tc_database, tc_table, tc_partition, tc_operation_type, tc_writeid)", rows);
        for(String query : queries) {
          LOG.debug("Going to execute update <" + query + ">");
          modCount = stmt.executeUpdate(query);
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
        close(lockHandle, stmt, dbConn);
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
   * HIVE_LOCKS is (presumably) expected to be removed by AcidHouseKeeperServices
   * WS_SET is (presumably) expected to be removed by AcidWriteSetService
   */
  @Override
  @RetrySemantics.Idempotent
  public void cleanupRecords(HiveObjectType type, Database db, Table table,
                             Iterator<Partition> partitionIterator) throws MetaException {
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

            buff.append("delete from TXN_COMPONENTS where tc_database='");
            buff.append(dbName);
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("delete from COMPLETED_TXN_COMPONENTS where ctc_database='");
            buff.append(dbName);
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("delete from COMPACTION_QUEUE where cq_database='");
            buff.append(dbName);
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("delete from COMPLETED_COMPACTIONS where cc_database='");
            buff.append(dbName);
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("delete from TXN_TO_WRITE_ID where t2w_database='");
            buff.append(dbName.toLowerCase());
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("delete from NEXT_WRITE_ID where nwi_database='");
            buff.append(dbName.toLowerCase());
            buff.append("'");
            queries.add(buff.toString());

            break;
          }
          case TABLE: {
            dbName = table.getDbName();
            tblName = table.getTableName();

            buff.append("delete from TXN_COMPONENTS where tc_database='");
            buff.append(dbName);
            buff.append("' and tc_table='");
            buff.append(tblName);
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("delete from COMPLETED_TXN_COMPONENTS where ctc_database='");
            buff.append(dbName);
            buff.append("' and ctc_table='");
            buff.append(tblName);
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("delete from COMPACTION_QUEUE where cq_database='");
            buff.append(dbName);
            buff.append("' and cq_table='");
            buff.append(tblName);
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("delete from COMPLETED_COMPACTIONS where cc_database='");
            buff.append(dbName);
            buff.append("' and cc_table='");
            buff.append(tblName);
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("delete from TXN_TO_WRITE_ID where t2w_database='");
            buff.append(dbName.toLowerCase());
            buff.append("' and t2w_table='");
            buff.append(tblName.toLowerCase());
            buff.append("'");
            queries.add(buff.toString());

            buff.setLength(0);
            buff.append("delete from NEXT_WRITE_ID where nwi_database='");
            buff.append(dbName.toLowerCase());
            buff.append("' and nwi_table='");
            buff.append(tblName.toLowerCase());
            buff.append("'");
            queries.add(buff.toString());

            break;
          }
          case PARTITION: {
            dbName = table.getDbName();
            tblName = table.getTableName();
            List<FieldSchema> partCols = table.getPartitionKeys();  // partition columns
            List<String> partVals;                                  // partition values
            String partName;

            while (partitionIterator.hasNext()) {
              Partition p = partitionIterator.next();
              partVals = p.getValues();
              partName = Warehouse.makePartName(partCols, partVals);

              buff.append("delete from TXN_COMPONENTS where tc_database='");
              buff.append(dbName);
              buff.append("' and tc_table='");
              buff.append(tblName);
              buff.append("' and tc_partition='");
              buff.append(partName);
              buff.append("'");
              queries.add(buff.toString());

              buff.setLength(0);
              buff.append("delete from COMPLETED_TXN_COMPONENTS where ctc_database='");
              buff.append(dbName);
              buff.append("' and ctc_table='");
              buff.append(tblName);
              buff.append("' and ctc_partition='");
              buff.append(partName);
              buff.append("'");
              queries.add(buff.toString());

              buff.setLength(0);
              buff.append("delete from COMPACTION_QUEUE where cq_database='");
              buff.append(dbName);
              buff.append("' and cq_table='");
              buff.append(tblName);
              buff.append("' and cq_partition='");
              buff.append(partName);
              buff.append("'");
              queries.add(buff.toString());

              buff.setLength(0);
              buff.append("delete from COMPLETED_COMPACTIONS where cc_database='");
              buff.append(dbName);
              buff.append("' and cc_table='");
              buff.append(tblName);
              buff.append("' and cc_partition='");
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

        String update = "update TXN_COMPONENTS set ";
        String where = " where ";
        if(oldPartName != null) {
          update += "TC_PARTITION = " + quoteString(newPartName) + ", ";
          where += "TC_PARTITION = " + quoteString(oldPartName) + " AND ";
        }
        if(oldTabName != null) {
          update += "TC_TABLE = " + quoteString(normalizeCase(newTabName)) + ", ";
          where += "TC_TABLE = " + quoteString(normalizeCase(oldTabName)) + " AND ";
        }
        if(oldDbName != null) {
          update += "TC_DATABASE = " + quoteString(normalizeCase(newDbName));
          where += "TC_DATABASE = " + quoteString(normalizeCase(oldDbName));
        }
        queries.add(update + where);

        update = "update COMPLETED_TXN_COMPONENTS set ";
        where = " where ";
        if(oldPartName != null) {
          update += "CTC_PARTITION = " + quoteString(newPartName) + ", ";
          where += "CTC_PARTITION = " + quoteString(oldPartName) + " AND ";
        }
        if(oldTabName != null) {
          update += "CTC_TABLE = " + quoteString(normalizeCase(newTabName)) + ", ";
          where += "CTC_TABLE = " + quoteString(normalizeCase(oldTabName)) + " AND ";
        }
        if(oldDbName != null) {
          update += "CTC_DATABASE = " + quoteString(normalizeCase(newDbName));
          where += "CTC_DATABASE = " + quoteString(normalizeCase(oldDbName));
        }
        queries.add(update + where);

        update = "update HIVE_LOCKS set ";
        where = " where ";
        if(oldPartName != null) {
          update += "HL_PARTITION = " + quoteString(newPartName) + ", ";
          where += "HL_PARTITION = " + quoteString(oldPartName) + " AND ";
        }
        if(oldTabName != null) {
          update += "HL_TABLE = " + quoteString(normalizeCase(newTabName)) + ", ";
          where += "HL_TABLE = " + quoteString(normalizeCase(oldTabName)) + " AND ";
        }
        if(oldDbName != null) {
          update += "HL_DB = " + quoteString(normalizeCase(newDbName));
          where += "HL_DB = " + quoteString(normalizeCase(oldDbName));
        }
        queries.add(update + where);

        update = "update COMPACTION_QUEUE set ";
        where = " where ";
        if(oldPartName != null) {
          update += "CQ_PARTITION = " + quoteString(newPartName) + ", ";
          where += "CQ_PARTITION = " + quoteString(oldPartName) + " AND ";
        }
        if(oldTabName != null) {
          update += "CQ_TABLE = " + quoteString(normalizeCase(newTabName)) + ", ";
          where += "CQ_TABLE = " + quoteString(normalizeCase(oldTabName)) + " AND ";
        }
        if(oldDbName != null) {
          update += "CQ_DATABASE = " + quoteString(normalizeCase(newDbName));
          where += "CQ_DATABASE = " + quoteString(normalizeCase(oldDbName));
        }
        queries.add(update + where);

        update = "update COMPLETED_COMPACTIONS set ";
        where = " where ";
        if(oldPartName != null) {
          update += "CC_PARTITION = " + quoteString(newPartName) + ", ";
          where += "CC_PARTITION = " + quoteString(oldPartName) + " AND ";
        }
        if(oldTabName != null) {
          update += "CC_TABLE = " + quoteString(normalizeCase(newTabName)) + ", ";
          where += "CC_TABLE = " + quoteString(normalizeCase(oldTabName)) + " AND ";
        }
        if(oldDbName != null) {
          update += "CC_DATABASE = " + quoteString(normalizeCase(newDbName));
          where += "CC_DATABASE = " + quoteString(normalizeCase(oldDbName));
        }
        queries.add(update + where);

        update = "update WRITE_SET set ";
        where = " where ";
        if(oldPartName != null) {
          update += "WS_PARTITION = " + quoteString(newPartName) + ", ";
          where += "WS_PARTITION = " + quoteString(oldPartName) + " AND ";
        }
        if(oldTabName != null) {
          update += "WS_TABLE = " + quoteString(normalizeCase(newTabName)) + ", ";
          where += "WS_TABLE = " + quoteString(normalizeCase(oldTabName)) + " AND ";
        }
        if(oldDbName != null) {
          update += "WS_DATABASE = " + quoteString(normalizeCase(newDbName));
          where += "WS_DATABASE = " + quoteString(normalizeCase(oldDbName));
        }
        queries.add(update + where);

        update = "update TXN_TO_WRITE_ID set ";
        where = " where ";
        if(oldTabName != null) {
          update += "T2W_TABLE = " + quoteString(normalizeCase(newTabName)) + ", ";
          where += "T2W_TABLE = " + quoteString(normalizeCase(oldTabName)) + " AND ";
        }
        if(oldDbName != null) {
          update += "T2W_DATABASE = " + quoteString(normalizeCase(newDbName));
          where += "T2W_DATABASE = " + quoteString(normalizeCase(oldDbName));
        }
        queries.add(update + where);

        update = "update NEXT_WRITE_ID set ";
        where = " where ";
        if(oldTabName != null) {
          update += "NWI_TABLE = " + quoteString(normalizeCase(newTabName)) + ", ";
          where += "NWI_TABLE = " + quoteString(normalizeCase(oldTabName)) + " AND ";
        }
        if(oldDbName != null) {
          update += "NWI_DATABASE = " + quoteString(normalizeCase(newDbName));
          where += "NWI_DATABASE = " + quoteString(normalizeCase(oldDbName));
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
      String s = "select count(*) from HIVE_LOCKS";
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
    int rc = doRetryOnConnPool ? 10 : 1;
    Connection dbConn = null;
    while (true) {
      try {
        dbConn = connPool.getConnection();
        dbConn.setAutoCommit(false);
        dbConn.setTransactionIsolation(isolationLevel);
        return dbConn;
      } catch (SQLException e){
        closeDbConn(dbConn);
        if ((--rc) <= 0) throw e;
        LOG.error("There is a problem with a connection from the pool, retrying(rc=" + rc + "): " +
          getMessage(e), e);
      }
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
   * Determine if an exception was such that it makes sense to retry.  Unfortunately there is no standard way to do
   * this, so we have to inspect the error messages and catch the telltale signs for each
   * different database.  This method will throw {@code RetryException}
   * if the error is retry-able.
   * @param conn database connection
   * @param e exception that was thrown.
   * @param caller name of the method calling this (and other info useful to log)
   * @throws org.apache.hadoop.hive.metastore.txn.TxnHandler.RetryException when the operation should be retried
   */
  protected void checkRetryable(Connection conn,
                                SQLException e,
                                String caller) throws RetryException, MetaException {

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
      if (DatabaseProduct.isDeadlock(dbProduct, e)) {
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
      String s;
      switch (dbProduct) {
        case DERBY:
          s = "values current_timestamp";
          break;

        case MYSQL:
        case POSTGRES:
        case SQLSERVER:
          s = "select current_timestamp";
          break;

        case ORACLE:
          s = "select current_timestamp from dual";
          break;

        default:
          String msg = "Unknown database product: " + dbProduct.toString();
          LOG.error(msg);
          throw new MetaException(msg);
      }
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
      dbProduct = DatabaseProduct.determineDatabaseProduct(s);
      if (dbProduct == DatabaseProduct.OTHER) {
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
      extLockId = rs.getLong("hl_lock_ext_id"); // can't be null
      intLockId = rs.getLong("hl_lock_int_id"); // can't be null
      db = rs.getString("hl_db"); // can't be null
      String t = rs.getString("hl_table");
      table = (rs.wasNull() ? null : t);
      String p = rs.getString("hl_partition");
      partition = (rs.wasNull() ? null : p);
      switch (rs.getString("hl_lock_state").charAt(0)) {
        case LOCK_WAITING: state = LockState.WAITING; break;
        case LOCK_ACQUIRED: state = LockState.ACQUIRED; break;
        default:
          throw new MetaException("Unknown lock state " + rs.getString("hl_lock_state").charAt(0));
      }
      switch (rs.getString("hl_lock_type").charAt(0)) {
        case LOCK_EXCLUSIVE: type = LockType.EXCLUSIVE; break;
        case LOCK_SHARED: type = LockType.SHARED_READ; break;
        case LOCK_SEMI_SHARED: type = LockType.SHARED_WRITE; break;
        default:
          throw new MetaException("Unknown lock type " + rs.getString("hl_lock_type").charAt(0));
      }
      txnId = rs.getLong("hl_txnid");//returns 0 if value is NULL
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

  private static class LockInfoComparator implements Comparator<LockInfo> {
    private static final LockTypeComparator lockTypeComparator = new LockTypeComparator();
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

  /**
   * Sort more restrictive locks after less restrictive ones.  Why?
   * Consider insert overwirte table DB.T1 select ... from T2:
   * this takes X lock on DB.T1 and S lock on T2
   * Also, create table DB.T3: takes S lock on DB.
   * so the state of the lock manger is {X(T1), S(T2) S(DB)} all in acquired state.
   * This is made possible by HIVE-10242.
   * Now a select * from T1 will try to get S(T1) which according to the 'jumpTable' will
   * be acquired once it sees S(DB).  So need to check stricter locks first.
   */
  private final static class LockTypeComparator implements Comparator<LockType> {
    public boolean equals(Object other) {
      return this == other;
    }
    public int compare(LockType t1, LockType t2) {
      switch (t1) {
        case EXCLUSIVE:
          if(t2 == LockType.EXCLUSIVE) {
            return 0;
          }
          return 1;
        case SHARED_WRITE:
          switch (t2) {
            case EXCLUSIVE:
              return -1;
            case SHARED_WRITE:
              return 0;
            case SHARED_READ:
              return 1;
            default:
              throw new RuntimeException("Unexpected LockType: " + t2);
          }
        case SHARED_READ:
          if(t2 == LockType.SHARED_READ) {
            return 0;
          }
          return -1;
        default:
          throw new RuntimeException("Unexpected LockType: " + t1);
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

  private void checkQFileTestHack() {
    boolean hackOn = MetastoreConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST) ||
      MetastoreConf.getBoolVar(conf, ConfVars.HIVE_IN_TEZ_TEST);
    if (hackOn) {
      LOG.info("Hacking in canned values for transaction manager");
      // Set up the transaction/locking db in the derby metastore
      TxnDbUtil.setConfValues(conf);
      try {
        TxnDbUtil.prepDb(conf);
      } catch (Exception e) {
        // We may have already created the tables and thus don't need to redo it.
        if (e.getMessage() != null && !e.getMessage().contains("already exists")) {
          throw new RuntimeException("Unable to set up transaction database for" +
            " testing: " + e.getMessage(), e);
        }
      }
    }
  }

  private int abortTxns(Connection dbConn, List<Long> txnids, boolean isStrict) throws SQLException {
    return abortTxns(dbConn, txnids, -1, isStrict);
  }
  /**
   * TODO: expose this as an operation to client.  Useful for streaming API to abort all remaining
   * trasnactions in a batch on IOExceptions.
   * Caller must rollback the transaction if not all transactions were aborted since this will not
   * attempt to delete associated locks in this case.
   *
   * @param dbConn An active connection
   * @param txnids list of transactions to abort
   * @param max_heartbeat value used by {@link #performTimeOuts()} to ensure this doesn't Abort txn which were
   *                      hearbetated after #performTimeOuts() select and this operation.
   * @param isStrict true for strict mode, false for best-effort mode.
   *                 In strict mode, if all txns are not successfully aborted, then the count of
   *                 updated ones will be returned and the caller will roll back.
   *                 In best-effort mode, we will ignore that fact and continue deleting the locks.
   * @return Number of aborted transactions
   * @throws SQLException
   */
  private int abortTxns(Connection dbConn, List<Long> txnids, long max_heartbeat, boolean isStrict)
      throws SQLException {
    Statement stmt = null;
    int updateCnt = 0;
    if (txnids.isEmpty()) {
      return 0;
    }
    try {
      stmt = dbConn.createStatement();
      //This is an update statement, thus at any Isolation level will take Write locks so will block
      //all other ops using S4U on TXNS row.
      List<String> queries = new ArrayList<>();

      StringBuilder prefix = new StringBuilder();
      StringBuilder suffix = new StringBuilder();

      prefix.append("update TXNS set txn_state = " + quoteChar(TXN_ABORTED) +
        " where txn_state = " + quoteChar(TXN_OPEN) + " and ");
      if(max_heartbeat > 0) {
        suffix.append(" and txn_last_heartbeat < ").append(max_heartbeat);
      } else {
        suffix.append("");
      }

      TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix, txnids, "txn_id", true, false);

      for (String query : queries) {
        LOG.debug("Going to execute update <" + query + ">");
        updateCnt += stmt.executeUpdate(query);
      }

      // As current txn is aborted, this won't read any data from other txns. So, it is safe to unregister
      // the min_open_txnid from MIN_HISTORY_LEVEL for the aborted txns. Even if the txns in the list are
      // partially aborted, it is safe to delete from MIN_HISTORY_LEVEL as the remaining txns are either
      // already committed or aborted.
      queries.clear();
      prefix.setLength(0);
      suffix.setLength(0);

      prefix.append("delete from MIN_HISTORY_LEVEL where ");
      suffix.append("");

      TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix, txnids, "mhl_txnid", false, false);

      for (String query : queries) {
        LOG.debug("Going to execute update <" + query + ">");
        int rc = stmt.executeUpdate(query);
        LOG.debug("Deleted " + rc + " records from MIN_HISTORY_LEVEL");
      }
      LOG.info("Removed aborted transactions: (" + txnids + ") from MIN_HISTORY_LEVEL");

      if (updateCnt < txnids.size() && isStrict) {
        /**
         * have to bail in this case since we don't know which transactions were not Aborted and
         * thus don't know which locks to delete
         * This may happen due to a race between {@link #heartbeat(HeartbeatRequest)}  operation and
         * {@link #performTimeOuts()}
         */
        return updateCnt;
      }

      queries.clear();
      prefix.setLength(0);
      suffix.setLength(0);

      prefix.append("delete from HIVE_LOCKS where ");
      suffix.append("");

      TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix, txnids, "hl_txnid", false, false);

      for (String query : queries) {
        LOG.debug("Going to execute update <" + query + ">");
        int rc = stmt.executeUpdate(query);
        LOG.debug("Removed " + rc + " records from HIVE_LOCKS");
      }
    } finally {
      closeStmt(stmt);
    }
    return updateCnt;
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
  private LockResponse checkLock(Connection dbConn, long extLockId)
    throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, MetaException, SQLException {
    TxnStore.MutexAPI.LockHandle handle =  null;
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
      /**
       * checkLock() must be mutex'd against any other checkLock to make sure 2 conflicting locks
       * are not granted by parallel checkLock() calls.
       */
      handle = getMutexAPI().acquireLock(MUTEX_KEY.CheckLock.name());
      List<LockInfo> locksBeingChecked = getLockInfoFromLockId(dbConn, extLockId);//being acquired now
      response.setLockid(extLockId);

      LOG.debug("checkLock(): Setting savepoint. extLockId=" + JavaUtils.lockIdToString(extLockId));
      Savepoint save = dbConn.setSavepoint();
      StringBuilder query = new StringBuilder("select hl_lock_ext_id, " +
        "hl_lock_int_id, hl_db, hl_table, hl_partition, hl_lock_state, " +
        "hl_lock_type, hl_txnid from HIVE_LOCKS where hl_db in (");

      Set<String> strings = new HashSet<>(locksBeingChecked.size());

      //This the set of entities that the statement represented by extLockId wants to update
      List<LockInfo> writeSet = new ArrayList<>();

      for (LockInfo info : locksBeingChecked) {
        strings.add(info.db);
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
        StringBuilder sb = new StringBuilder(" ws_database, ws_table, ws_partition, " +
          "ws_txnid, ws_commit_id " +
          "from WRITE_SET where ws_commit_id >= " + writeSet.get(0).txnId + " and (");//see commitTxn() for more info on this inequality
        for(LockInfo info : writeSet) {
          sb.append("(ws_database = ").append(quoteString(info.db)).append(" and ws_table = ")
            .append(quoteString(info.table)).append(" and ws_partition ")
            .append(info.partition == null ? "is null" : "= " + quoteString(info.partition)).append(") or ");
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
          if(abortTxns(dbConn, Collections.singletonList(writeSet.get(0).txnId), true) != 1) {
            throw new IllegalStateException(msg + " FAILED!");
          }
          dbConn.commit();
          throw new TxnAbortedException(msg);
        }
        close(rs, stmt, null);
      }

      boolean first = true;
      for (String s : strings) {
        if (first) first = false;
        else query.append(", ");
        query.append('\'');
        query.append(s);
        query.append('\'');
      }
      query.append(")");

      // If any of the table requests are null, then I need to pull all the
      // table locks for this db.
      boolean sawNull = false;
      strings.clear();
      for (LockInfo info : locksBeingChecked) {
        if (info.table == null) {
          sawNull = true;
          break;
        } else {
          strings.add(info.table);
        }
      }
      if (!sawNull) {
        query.append(" and (hl_table is null or hl_table in(");
        first = true;
        for (String s : strings) {
          if (first) first = false;
          else query.append(", ");
          query.append('\'');
          query.append(s);
          query.append('\'');
        }
        query.append("))");

        // If any of the partition requests are null, then I need to pull all
        // partition locks for this table.
        sawNull = false;
        strings.clear();
        for (LockInfo info : locksBeingChecked) {
          if (info.partition == null) {
            sawNull = true;
            break;
          } else {
            strings.add(info.partition);
          }
        }
        if (!sawNull) {
          query.append(" and (hl_partition is null or hl_partition in(");
          first = true;
          for (String s : strings) {
            if (first) first = false;
            else query.append(", ");
            query.append('\'');
            query.append(s);
            query.append('\'');
          }
          query.append("))");
        }
      }
      query.append(" and hl_lock_ext_id < ").append(extLockId);

      LOG.debug("Going to execute query <" + query.toString() + ">");
      stmt = dbConn.createStatement();
      rs = stmt.executeQuery(query.toString());
      SortedSet<LockInfo> lockSet = new TreeSet<LockInfo>(new LockInfoComparator());
      while (rs.next()) {
        lockSet.add(new LockInfo(rs));
      }
      // Turn the tree set into an array so we can move back and forth easily
      // in it.
      LockInfo[] locks = lockSet.toArray(new LockInfo[lockSet.size()]);
      if(LOG.isTraceEnabled()) {
        LOG.trace("Locks to check(full): ");
        for(LockInfo info : locks) {
          LOG.trace("  " + info);
        }
      }

      for (LockInfo info : locksBeingChecked) {
        // If we've found it and it's already been marked acquired,
        // then just look at the other locks.
        if (info.state == LockState.ACQUIRED) {
          /**this is what makes this method @SafeToRetry*/
          continue;
        }

        // Look at everything in front of this lock to see if it should block
        // it or not.
        for (int i = locks.length - 1; i >= 0; i--) {
          // Check if we're operating on the same database, if not, move on
          if (!info.db.equals(locks[i].db)) {
            continue;
          }

          // If table is null on either of these, then they are claiming to
          // lock the whole database and we need to check it.  Otherwise,
          // check if they are operating on the same table, if not, move on.
          if (info.table != null && locks[i].table != null
            && !info.table.equals(locks[i].table)) {
            continue;
          }
          // if here, we may be checking a DB level lock against a Table level lock.  Alternatively,
          // we could have used Intention locks (for example a request for S lock on table would
          // cause an IS lock DB that contains the table).  Similarly, at partition level.

          // If partition is null on either of these, then they are claiming to
          // lock the whole table and we need to check it.  Otherwise,
          // check if they are operating on the same partition, if not, move on.
          if (info.partition != null && locks[i].partition != null
            && !info.partition.equals(locks[i].partition)) {
            continue;
          }

          // We've found something that matches what we're trying to lock,
          // so figure out if we can lock it too.
          LockAction lockAction = jumpTable.get(info.type).get(locks[i].type).get(locks[i].state);
          LOG.debug("desired Lock: " + info + " checked Lock: " + locks[i] + " action: " + lockAction);
          switch (lockAction) {
            case WAIT:
              if(!ignoreConflict(info, locks[i])) {
                /*we acquire all locks for a given query atomically; if 1 blocks, all go into (remain) in
                * Waiting state.  wait() will undo any 'acquire()' which may have happened as part of
                * this (metastore db) transaction and then we record which lock blocked the lock
                * we were testing ('info').*/
                wait(dbConn, save);
                String sqlText = "update HIVE_LOCKS" +
                  " set HL_BLOCKEDBY_EXT_ID=" + locks[i].extLockId +
                  ", HL_BLOCKEDBY_INT_ID=" + locks[i].intLockId +
                  " where HL_LOCK_EXT_ID=" + info.extLockId + " and HL_LOCK_INT_ID=" + info.intLockId;
                LOG.debug("Executing sql: " + sqlText);
                int updCnt = stmt.executeUpdate(sqlText);
                if(updCnt != 1) {
                  shouldNeverHappen(info.txnId, info.extLockId, info.intLockId);
                }
                LOG.debug("Going to commit");
                dbConn.commit();
                response.setState(LockState.WAITING);
                LOG.debug("Lock(" + info + ") waiting for Lock(" + locks[i] + ")");
                return response;
              }
              //fall through to ACQUIRE
            case ACQUIRE:
              break;
            case KEEP_LOOKING:
              continue;
          }
          //if we got here, it means it's ok to acquire 'info' lock
          break;// so exit the loop and check next lock
        }
      }
      //if here, ther were no locks that blocked any locks in 'locksBeingChecked' - acquire them all
      acquire(dbConn, stmt, locksBeingChecked);

      // We acquired all of the locks, so commit and return acquired.
      LOG.debug("Going to commit");
      dbConn.commit();
      response.setState(LockState.ACQUIRED);
    } finally {
      close(rs, stmt, null);
      if(handle != null) {
        handle.releaseLocks();
      }
    }
    return response;
  }
  private void acquire(Connection dbConn, Statement stmt, List<LockInfo> locksBeingChecked)
    throws SQLException, NoSuchLockException, MetaException {
    if(locksBeingChecked == null || locksBeingChecked.isEmpty()) {
      return;
    }
    long txnId = locksBeingChecked.get(0).txnId;
    long extLockId = locksBeingChecked.get(0).extLockId;
    long now = getDbTime(dbConn);
    String s = "update HIVE_LOCKS set hl_lock_state = '" + LOCK_ACQUIRED + "', " +
      //if lock is part of txn, heartbeat info is in txn record
      "hl_last_heartbeat = " + (isValidTxn(txnId) ? 0 : now) +
      ", hl_acquired_at = " + now + ",HL_BLOCKEDBY_EXT_ID=NULL,HL_BLOCKEDBY_INT_ID=null" +
      " where hl_lock_ext_id = " +  extLockId;
    LOG.debug("Going to execute update <" + s + ">");
    int rc = stmt.executeUpdate(s);
    if (rc < locksBeingChecked.size()) {
      LOG.debug("Going to rollback acquire(Connection dbConn, Statement stmt, List<LockInfo> locksBeingChecked)");
      dbConn.rollback();
      /*select all locks for this ext ID and see which ones are missing*/
      StringBuilder sb = new StringBuilder("No such lock(s): (" + JavaUtils.lockIdToString(extLockId) + ":");
      ResultSet rs = stmt.executeQuery("select hl_lock_int_id from HIVE_LOCKS where hl_lock_ext_id = " + extLockId);
      while(rs.next()) {
        int intLockId = rs.getInt(1);
        int idx = 0;
        for(; idx < locksBeingChecked.size(); idx++) {
          LockInfo expectedLock = locksBeingChecked.get(idx);
          if(expectedLock != null && expectedLock.intLockId == intLockId) {
            locksBeingChecked.set(idx, null);
            break;
          }
        }
      }
      for(LockInfo expectedLock : locksBeingChecked) {
        if(expectedLock != null) {
          sb.append(expectedLock.intLockId).append(",");
        }
      }
      sb.append(") ").append(JavaUtils.txnIdToString(txnId));
      close(rs);
      throw new NoSuchLockException(sb.toString());
    }
  }

  /**
   * the {@link #jumpTable} only deals with LockState/LockType.  In some cases it's not
   * sufficient.  For example, an EXCLUSIVE lock on partition should prevent SHARED_READ
   * on the table, but there is no reason for EXCLUSIVE on a table to prevent SHARED_READ
   * on a database.  Similarly, EXCLUSIVE on a partition should not conflict with SHARED_READ on
   * a database.  (SHARED_READ is usually acquired on a database to make sure it's not dropped
   * while some operation is performed on that db (e.g. show tables, created table, etc)
   * EXCLUSIVE on an object may mean it's being dropped or overwritten (for non-acid tables,
   * an Insert uses EXCLUSIVE as well)).
   */
  private boolean ignoreConflict(LockInfo desiredLock, LockInfo existingLock) {
    return
      ((desiredLock.isDbLock() && desiredLock.type == LockType.SHARED_READ &&
          existingLock.isTableLock() && existingLock.type == LockType.EXCLUSIVE) ||
        (existingLock.isDbLock() && existingLock.type == LockType.SHARED_READ &&
          desiredLock.isTableLock() && desiredLock.type == LockType.EXCLUSIVE) ||

        (desiredLock.isDbLock() && desiredLock.type == LockType.SHARED_READ &&
          existingLock.isPartitionLock() && existingLock.type == LockType.EXCLUSIVE) ||
        (existingLock.isDbLock() && existingLock.type == LockType.SHARED_READ &&
          desiredLock.isPartitionLock() && desiredLock.type == LockType.EXCLUSIVE))
        ||

      //different locks from same txn should not conflict with each other
      (desiredLock.txnId != 0 && desiredLock.txnId == existingLock.txnId) ||
      //txnId=0 means it's a select or IUD which does not write to ACID table, e.g
      //insert overwrite table T partition(p=1) select a,b from T and autoCommit=true
      // todo: fix comment as of HIVE-14988
      (desiredLock.txnId == 0 &&  desiredLock.extLockId == existingLock.extLockId);
  }

  private void wait(Connection dbConn, Savepoint save) throws SQLException {
    // Need to rollback because we did a select that acquired locks but we didn't
    // actually update anything.  Also, we may have locked some locks as
    // acquired that we now want to not acquire.  It's ok to rollback because
    // once we see one wait, we're done, we won't look for more.
    // Only rollback to savepoint because we want to commit our heartbeat
    // changes.
    LOG.debug("Going to rollback to savepoint");
    dbConn.rollback(save);
  }
  /**
   * Heartbeats on the lock table.  This commits, so do not enter it with any state.
   * Should not be called on a lock that belongs to transaction.
   */
  private void heartbeatLock(Connection dbConn, long extLockId)
    throws NoSuchLockException, SQLException, MetaException {
    // If the lock id is 0, then there are no locks in this heartbeat
    if (extLockId == 0) return;
    Statement stmt = null;
    try {
      stmt = dbConn.createStatement();
      long now = getDbTime(dbConn);

      String s = "update HIVE_LOCKS set hl_last_heartbeat = " +
        now + " where hl_lock_ext_id = " + extLockId;
      LOG.debug("Going to execute update <" + s + ">");
      int rc = stmt.executeUpdate(s);
      if (rc < 1) {
        LOG.debug("Going to rollback");
        dbConn.rollback();
        throw new NoSuchLockException("No such lock: " + JavaUtils.lockIdToString(extLockId));
      }
      LOG.debug("Going to commit");
      dbConn.commit();
    } finally {
      closeStmt(stmt);
    }
  }

  // Heartbeats on the txn table.  This commits, so do not enter it with any state
  private void heartbeatTxn(Connection dbConn, long txnid)
    throws NoSuchTxnException, TxnAbortedException, SQLException, MetaException {
    // If the txnid is 0, then there are no transactions in this heartbeat
    if (txnid == 0) return;
    Statement stmt = null;
    try {
      stmt = dbConn.createStatement();
      long now = getDbTime(dbConn);
      String s = "update TXNS set txn_last_heartbeat = " + now +
        " where txn_id = " + txnid + " and txn_state = '" + TXN_OPEN + "'";
      LOG.debug("Going to execute update <" + s + ">");
      int rc = stmt.executeUpdate(s);
      if (rc < 1) {
        ensureValidTxn(dbConn, txnid, stmt); // This should now throw some useful exception.
        LOG.warn("Can neither heartbeat txn nor confirm it as invalid.");
        dbConn.rollback();
        throw new NoSuchTxnException("No such txn: " + txnid);
      }
      LOG.debug("Going to commit");
      dbConn.commit();
    } finally {
      closeStmt(stmt);
    }
  }

  /**
   * Returns the state of the transaction iff it's able to determine it.  Some cases where it cannot:
   * 1. txnid was Aborted/Committed and then GC'd (compacted)
   * 2. txnid was committed but it didn't modify anything (nothing in COMPLETED_TXN_COMPONENTS)
   */
  private TxnStatus findTxnState(long txnid, Statement stmt) throws SQLException, MetaException {
    String s = "select txn_state from TXNS where txn_id = " + txnid;
    LOG.debug("Going to execute query <" + s + ">");
    try (ResultSet rs = stmt.executeQuery(s)) {
      if (!rs.next()) {
        s =
            sqlGenerator.addLimitClause(1, "1 from COMPLETED_TXN_COMPONENTS where CTC_TXNID = "
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
      char txnState = rs.getString(1).charAt(0);
      if (txnState == TXN_ABORTED) {
        return TxnStatus.ABORTED;
      }
      assert txnState == TXN_OPEN : "we found it in TXNS but it's not ABORTED, so must be OPEN";
    }
    return TxnStatus.OPEN;
  }

  /**
   * Checks if all the txns in the list are in open state.
   * @param txnIds list of txns to be evaluated for open state
   * @param stmt db statement
   * @return If all txns in open state, then return true else false
   */
  private boolean isTxnsInOpenState(List<Long> txnIds, Statement stmt) throws SQLException {
    List<String> queries = new ArrayList<>();
    StringBuilder prefix = new StringBuilder();
    StringBuilder suffix = new StringBuilder();

    // Get the count of txns from the given list are in open state. If the returned count is same as
    // the input number of txns, then it means, all are in open state.
    prefix.append("select count(*) from TXNS where txn_state = '" + TXN_OPEN + "' and ");
    suffix.append("");
    TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix,
            txnIds, "txn_id", false, false);

    long count = 0;
    for (String query : queries) {
      LOG.debug("Going to execute query <" + query + ">");
      ResultSet rs = stmt.executeQuery(query);
      if (rs.next()) {
        count += rs.getLong(1);
      }
    }
    return count == txnIds.size();
  }

  /**
   * Checks if all the txns in the list are in open state.
   * @param dbName Database name
   * @param tblName Table on which we try to allocate write id
   * @param txnIds list of txns to be evaluated for open state
   * @param stmt db statement
   */
  private void ensureAllTxnsValid(String dbName, String tblName, List<Long> txnIds, Statement stmt)
          throws SQLException {
    List<String> queries = new ArrayList<>();
    StringBuilder prefix = new StringBuilder();
    StringBuilder suffix = new StringBuilder();

    // Check if any of the txns in the list is aborted.
    prefix.append("select txn_id, txn_state from TXNS where ");
    suffix.append("");
    TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix,
            txnIds, "txn_id", false, false);
    Long txnId;
    char txnState;
    boolean isAborted = false;
    StringBuilder errorMsg = new StringBuilder();
    errorMsg.append("Write ID allocation on ")
            .append(Warehouse.getQualifiedName(dbName, tblName))
            .append(" failed for input txns: ");
    for (String query : queries) {
      LOG.debug("Going to execute query <" + query + ">");
      ResultSet rs = stmt.executeQuery(query);
      while (rs.next()) {
        txnId = rs.getLong(1);
        txnState = rs.getString(2).charAt(0);
        if (txnState != TXN_OPEN) {
          isAborted = true;
          errorMsg.append("{").append(txnId).append(",").append(txnState).append("}");
        }
      }
    }
    // Check if any of the txns in the list is committed.
    boolean isCommitted = checkIfTxnsCommitted(txnIds, stmt, errorMsg);
    if (isAborted || isCommitted) {
      LOG.error(errorMsg.toString());
      throw new IllegalStateException("Write ID allocation failed on "
              + Warehouse.getQualifiedName(dbName, tblName)
              + " as not all input txns in open state");
    }
  }

  /**
   * Checks if all the txns in the list are in committed. If yes, throw eception.
   * @param txnIds list of txns to be evaluated for committed
   * @param stmt db statement
   * @return true if any input txn is committed, else false
   */
  private boolean checkIfTxnsCommitted(List<Long> txnIds, Statement stmt, StringBuilder errorMsg)
          throws SQLException {
    List<String> queries = new ArrayList<>();
    StringBuilder prefix = new StringBuilder();
    StringBuilder suffix = new StringBuilder();

    // Check if any of the txns in the list is committed. If yes, throw exception.
    prefix.append("select ctc_txnid from COMPLETED_TXN_COMPONENTS where ");
    suffix.append("");
    TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix,
            txnIds, "ctc_txnid", false, false);
    Long txnId;
    boolean isCommitted = false;
    for (String query : queries) {
      LOG.debug("Going to execute query <" + query + ">");
      ResultSet rs = stmt.executeQuery(query);
      while (rs.next()) {
        isCommitted = true;
        txnId = rs.getLong(1);
        if (errorMsg != null) {
          errorMsg.append("{").append(txnId).append(",c}");
        }
      }
    }
    return isCommitted;
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
    String s = "select txn_state from TXNS where txn_id = " + txnid;
    LOG.debug("Going to execute query <" + s + ">");
    try (ResultSet rs = stmt.executeQuery(s)) {
      if (!rs.next()) {
        // todo: add LIMIT 1 instead of count - should be more efficient
        s = "select count(*) from COMPLETED_TXN_COMPONENTS where CTC_TXNID = " + txnid;
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
      if (rs.getString(1).charAt(0) == TXN_ABORTED) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        throw new TxnAbortedException("Transaction " + JavaUtils.txnIdToString(txnid)
            + " already aborted");// todo: add time of abort, which is not currently tracked.
                                  // Requires schema change
      }
    }
  }

  private LockInfo getTxnIdFromLockId(Connection dbConn, long extLockId)
    throws NoSuchLockException, MetaException, SQLException {
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = dbConn.createStatement();
      String s = "select hl_lock_ext_id, hl_lock_int_id, hl_db, hl_table, " +
        "hl_partition, hl_lock_state, hl_lock_type, hl_txnid from HIVE_LOCKS where " +
        "hl_lock_ext_id = " + extLockId;
      LOG.debug("Going to execute query <" + s + ">");
      rs = stmt.executeQuery(s);
      if (!rs.next()) {
        return null;
      }
      LockInfo info = new LockInfo(rs);
      LOG.debug("getTxnIdFromLockId(" + extLockId + ") Return " + JavaUtils.txnIdToString(info.txnId));
      return info;
    } finally {
      close(rs);
      closeStmt(stmt);
    }
  }

  // NEVER call this function without first calling heartbeat(long, long)
  private List<LockInfo> getLockInfoFromLockId(Connection dbConn, long extLockId)
    throws NoSuchLockException, MetaException, SQLException {
    Statement stmt = null;
    try {
      stmt = dbConn.createStatement();
      String s = "select hl_lock_ext_id, hl_lock_int_id, hl_db, hl_table, " +
        "hl_partition, hl_lock_state, hl_lock_type, hl_txnid from HIVE_LOCKS where " +
        "hl_lock_ext_id = " + extLockId;
      LOG.debug("Going to execute query <" + s + ">");
      ResultSet rs = stmt.executeQuery(s);
      boolean sawAtLeastOne = false;
      List<LockInfo> ourLockInfo = new ArrayList<>();
      while (rs.next()) {
        ourLockInfo.add(new LockInfo(rs));
        sawAtLeastOne = true;
      }
      if (!sawAtLeastOne) {
        throw new MetaException("This should never happen!  We already " +
          "checked the lock(" + JavaUtils.lockIdToString(extLockId) + ") existed but now we can't find it!");
      }
      return ourLockInfo;
    } finally {
      closeStmt(stmt);
    }
  }

  // Clean time out locks from the database not associated with a transactions, i.e. locks
  // for read-only autoCommit=true statements.  This does a commit,
  // and thus should be done before any calls to heartbeat that will leave
  // open transactions.
  private void timeOutLocks(Connection dbConn, long now) {
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = dbConn.createStatement();
      long maxHeartbeatTime = now - timeout;
      //doing a SELECT first is less efficient but makes it easier to debug things
      String s = "select distinct hl_lock_ext_id from HIVE_LOCKS where hl_last_heartbeat < " +
        maxHeartbeatTime + " and hl_txnid = 0";//when txnid is <> 0, the lock is
      //associated with a txn and is handled by performTimeOuts()
      //want to avoid expiring locks for a txn w/o expiring the txn itself
      List<Long> extLockIDs = new ArrayList<>();
      rs = stmt.executeQuery(s);
      while(rs.next()) {
        extLockIDs.add(rs.getLong(1));
      }
      rs.close();
      dbConn.commit();
      if(extLockIDs.size() <= 0) {
        return;
      }

      List<String> queries = new ArrayList<>();

      StringBuilder prefix = new StringBuilder();
      StringBuilder suffix = new StringBuilder();

      //include same hl_last_heartbeat condition in case someone heartbeated since the select
      prefix.append("delete from HIVE_LOCKS where hl_last_heartbeat < ");
      prefix.append(maxHeartbeatTime);
      prefix.append(" and hl_txnid = 0 and ");
      suffix.append("");

      TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix, extLockIDs, "hl_lock_ext_id", true, false);

      int deletedLocks = 0;
      for (String query : queries) {
        LOG.debug("Removing expired locks via: " + query);
        deletedLocks += stmt.executeUpdate(query);
      }
      if(deletedLocks > 0) {
        Collections.sort(extLockIDs);//easier to read logs
        LOG.info("Deleted " + deletedLocks + " int locks from HIVE_LOCKS due to timeout (" +
          "HL_LOCK_EXT_ID list:  " + extLockIDs + ") maxHeartbeatTime=" + maxHeartbeatTime);
      }
      LOG.debug("Going to commit");
      dbConn.commit();
    }
    catch(SQLException ex) {
      LOG.error("Failed to purge timedout locks due to: " + getMessage(ex), ex);
    }
    catch(Exception ex) {
      LOG.error("Failed to purge timedout locks due to: " + ex.getMessage(), ex);
    } finally {
      close(rs);
      closeStmt(stmt);
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
      long now = getDbTime(dbConn);
      timeOutLocks(dbConn, now);
      while(true) {
        stmt = dbConn.createStatement();
        String s = " txn_id from TXNS where txn_state = '" + TXN_OPEN +
            "' and txn_last_heartbeat <  " + (now - timeout) + " and txn_type != " + TxnType.REPL_CREATED.getValue();
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
          if(abortTxns(dbConn, batchToAbort, now - timeout, true) == batchToAbort.size()) {
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
      LOG.warn("Aborting timedout transactions failed due to " + getMessage(ex), ex);
    }
    catch(MetaException e) {
      LOG.warn("Aborting timedout transactions failed due to " + e.getMessage(), e);
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
        String s = "select count(*) from TXNS where txn_state = '" + TXN_OPEN + "'";
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

  private static synchronized DataSource setupJdbcConnectionPool(Configuration conf, int maxPoolSize, long getConnectionTimeoutMs) throws SQLException {
    DataSourceProvider dsp = DataSourceProviderFactory.getDataSourceProvider(conf);
    if (dsp != null) {
      doRetryOnConnPool = dsp.mayReturnClosedConnection();
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

  private static synchronized void buildJumpTable() {
    if (jumpTable != null) return;

    jumpTable = new HashMap<>(3);

    // SR: Lock we are trying to acquire is shared read
    Map<LockType, Map<LockState, LockAction>> m = new HashMap<>(3);
    jumpTable.put(LockType.SHARED_READ, m);

    // SR.SR: Lock we are examining is shared read
    Map<LockState, LockAction> m2 = new HashMap<>(2);
    m.put(LockType.SHARED_READ, m2);

    // SR.SR.acquired Lock we are examining is acquired;  We can acquire
    // because two shared reads can acquire together and there must be
    // nothing in front of this one to prevent acquisition.
    m2.put(LockState.ACQUIRED, LockAction.ACQUIRE);

    // SR.SR.wait Lock we are examining is waiting.  In this case we keep
    // looking, as it's possible that something in front is blocking it or
    // that the other locker hasn't checked yet and he could lock as well.
    m2.put(LockState.WAITING, LockAction.KEEP_LOOKING);

    // SR.SW: Lock we are examining is shared write
    m2 = new HashMap<>(2);
    m.put(LockType.SHARED_WRITE, m2);

    // SR.SW.acquired Lock we are examining is acquired;  We can acquire
    // because a read can share with a write, and there must be
    // nothing in front of this one to prevent acquisition.
    m2.put(LockState.ACQUIRED, LockAction.ACQUIRE);

    // SR.SW.wait Lock we are examining is waiting.  In this case we keep
    // looking, as it's possible that something in front is blocking it or
    // that the other locker hasn't checked yet and he could lock as well or
    // that something is blocking it that would not block a read.
    m2.put(LockState.WAITING, LockAction.KEEP_LOOKING);

    // SR.E: Lock we are examining is exclusive
    m2 = new HashMap<>(2);
    m.put(LockType.EXCLUSIVE, m2);

    // No matter whether it has acquired or not, we cannot pass an exclusive.
    m2.put(LockState.ACQUIRED, LockAction.WAIT);
    m2.put(LockState.WAITING, LockAction.WAIT);

    // SW: Lock we are trying to acquire is shared write
    m = new HashMap<>(3);
    jumpTable.put(LockType.SHARED_WRITE, m);

    // SW.SR: Lock we are examining is shared read
    m2 = new HashMap<>(2);
    m.put(LockType.SHARED_READ, m2);

    // SW.SR.acquired Lock we are examining is acquired;  We need to keep
    // looking, because there may or may not be another shared write in front
    // that would block us.
    m2.put(LockState.ACQUIRED, LockAction.KEEP_LOOKING);

    // SW.SR.wait Lock we are examining is waiting.  In this case we keep
    // looking, as it's possible that something in front is blocking it or
    // that the other locker hasn't checked yet and he could lock as well.
    m2.put(LockState.WAITING, LockAction.KEEP_LOOKING);

    // SW.SW: Lock we are examining is shared write
    m2 = new HashMap<>(2);
    m.put(LockType.SHARED_WRITE, m2);

    // Regardless of acquired or waiting, one shared write cannot pass another.
    m2.put(LockState.ACQUIRED, LockAction.WAIT);
    m2.put(LockState.WAITING, LockAction.WAIT);

    // SW.E: Lock we are examining is exclusive
    m2 = new HashMap<>(2);
    m.put(LockType.EXCLUSIVE, m2);

    // No matter whether it has acquired or not, we cannot pass an exclusive.
    m2.put(LockState.ACQUIRED, LockAction.WAIT);
    m2.put(LockState.WAITING, LockAction.WAIT);

    // E: Lock we are trying to acquire is exclusive
    m = new HashMap<>(3);
    jumpTable.put(LockType.EXCLUSIVE, m);

    // E.SR: Lock we are examining is shared read
    m2 = new HashMap<>(2);
    m.put(LockType.SHARED_READ, m2);

    // Exclusives can never pass
    m2.put(LockState.ACQUIRED, LockAction.WAIT);
    m2.put(LockState.WAITING, LockAction.WAIT);

    // E.SW: Lock we are examining is shared write
    m2 = new HashMap<>(2);
    m.put(LockType.SHARED_WRITE, m2);

    // Exclusives can never pass
    m2.put(LockState.ACQUIRED, LockAction.WAIT);
    m2.put(LockState.WAITING, LockAction.WAIT);

    // E.E: Lock we are examining is exclusive
    m2 = new HashMap<>(2);
    m.put(LockType.EXCLUSIVE, m2);

    // No matter whether it has acquired or not, we cannot pass an exclusive.
    m2.put(LockState.ACQUIRED, LockAction.WAIT);
    m2.put(LockState.WAITING, LockAction.WAIT);
  }
  /**
   * Returns true if {@code ex} should be retried
   */
  static boolean isRetryable(Configuration conf, Exception ex) {
    if(ex instanceof SQLException) {
      SQLException sqlException = (SQLException)ex;
      if("08S01".equalsIgnoreCase(sqlException.getSQLState())) {
        //in MSSQL this means Communication Link Failure
        return true;
      }
      if("ORA-08176".equalsIgnoreCase(sqlException.getSQLState()) ||
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
    switch (dbProduct) {
      case DERBY:
        if("23505".equals(ex.getSQLState())) {
          return true;
        }
        break;
      case MYSQL:
        //https://dev.mysql.com/doc/refman/5.5/en/error-messages-server.html
        if((ex.getErrorCode() == 1022 || ex.getErrorCode() == 1062 || ex.getErrorCode() == 1586)
          && "23000".equals(ex.getSQLState())) {
          return true;
        }
        break;
      case SQLSERVER:
        //2627 is unique constaint violation incl PK, 2601 - unique key
        if(ex.getErrorCode() == 2627 && "23000".equals(ex.getSQLState())) {
          return true;
        }
        break;
      case ORACLE:
        if(ex.getErrorCode() == 1 && "23000".equals(ex.getSQLState())) {
          return true;
        }
        break;
      case POSTGRES:
        //http://www.postgresql.org/docs/8.1/static/errcodes-appendix.html
        if("23505".equals(ex.getSQLState())) {
          return true;
        }
        break;
      default:
        throw new IllegalArgumentException("Unexpected DB type: " + dbProduct + "; " + getMessage(ex));
    }
    return false;
  }
  private static String getMessage(SQLException ex) {
    return ex.getMessage() + " (SQLState=" + ex.getSQLState() + ", ErrorCode=" + ex.getErrorCode() + ")";
  }
  /**
   * Useful for building SQL strings
   * @param value may be {@code null}
   */
  private static String valueOrNullLiteral(String value) {
    return value == null ? "null" : quoteString(value);
  }
  static String quoteString(String input) {
    return "'" + input + "'";
  }
  static String quoteChar(char c) {
    return "'" + c + "'";
  }
  static CompactionType dbCompactionType2ThriftType(char dbValue) {
    switch (dbValue) {
      case MAJOR_TYPE:
        return CompactionType.MAJOR;
      case MINOR_TYPE:
        return CompactionType.MINOR;
      default:
        LOG.warn("Unexpected compaction type " + dbValue);
        return null;
    }
  }
  static Character thriftCompactionType2DbType(CompactionType ct) {
    switch (ct) {
      case MAJOR:
        return MAJOR_TYPE;
      case MINOR:
        return MINOR_TYPE;
      default:
        LOG.warn("Unexpected compaction type " + ct);
        return null;
    }
  }

  /**
   * {@link #lockInternal()} and {@link #unlockInternal()} are used to serialize those operations that require
   * Select ... For Update to sequence operations properly.  In practice that means when running
   * with Derby database.  See more notes at class level.
   */
  private void lockInternal() {
    if(dbProduct == DatabaseProduct.DERBY) {
      derbyLock.lock();
    }
  }
  private void unlockInternal() {
    if(dbProduct == DatabaseProduct.DERBY) {
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
    try {
      try {
        String sqlStmt = sqlGenerator.addForUpdateClause("select MT_COMMENT from AUX_TABLE where MT_KEY1=" + quoteString(key) + " and MT_KEY2=0");
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
            stmt.executeUpdate("insert into AUX_TABLE(MT_KEY1,MT_KEY2) values(" + quoteString(key) + ", 0)");
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
        if(dbProduct == DatabaseProduct.DERBY) {
          derbyKey2Lock.putIfAbsent(key, new Semaphore(1));
          derbySemaphore =  derbyKey2Lock.get(key);
          derbySemaphore.acquire();
        }
        LOG.debug(quoteString(key) + " locked by " + quoteString(TxnHandler.hostname));
        //OK, so now we have a lock
        return new LockHandleImpl(dbConn, stmt, rs, key, derbySemaphore);
      } catch (SQLException ex) {
        rollbackDBConn(dbConn);
        close(rs, stmt, dbConn);
        checkRetryable(dbConn, ex, "acquireLock(" + key + ")");
        throw new MetaException("Unable to lock " + quoteString(key) + " due to: " + getMessage(ex) + "; " + StringUtils.stringifyException(ex));
      }
      catch(InterruptedException ex) {
        rollbackDBConn(dbConn);
        close(rs, stmt, dbConn);
        throw new MetaException("Unable to lock " + quoteString(key) + " due to: " + ex.getMessage() + StringUtils.stringifyException(ex));
      }
      finally {
        unlockInternal();
      }
    }
    catch(RetryException ex) {
      return acquireLock(key);
    }
  }
  public void acquireLock(String key, LockHandle handle) {
    //the idea is that this will use LockHandle.dbConn
    throw new NotImplementedException();
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
      throw new NotImplementedException();
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
  };

}
