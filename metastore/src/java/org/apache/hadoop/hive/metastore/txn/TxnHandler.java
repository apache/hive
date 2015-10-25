/**
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

import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.dbcp.PoolingDataSource;

import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.util.StringUtils;

import javax.sql.DataSource;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * A handler to answer transaction related calls that come into the metastore
 * server.
 *
 * Note on log messages:  Please include txnid:X and lockid info using
 * {@link org.apache.hadoop.hive.common.JavaUtils#txnIdToString(long)}
 * and {@link org.apache.hadoop.hive.common.JavaUtils#lockIdToString(long)} in all messages.
 * The txnid:X and lockid:Y matches how Thrift object toString() methods are generated,
 * so keeping the format consistent makes grep'ing the logs much easier.
 */
public class TxnHandler {
  // Compactor states
  static final public String INITIATED_RESPONSE = "initiated";
  static final public String WORKING_RESPONSE = "working";
  static final public String CLEANING_RESPONSE = "ready for cleaning";

  static final protected char INITIATED_STATE = 'i';
  static final protected char WORKING_STATE = 'w';
  static final protected char READY_FOR_CLEANING = 'r';

  // Compactor types
  static final protected char MAJOR_TYPE = 'a';
  static final protected char MINOR_TYPE = 'i';

  // Transaction states
  static final protected char TXN_ABORTED = 'a';
  static final protected char TXN_OPEN = 'o';

  // Lock states
  static final protected char LOCK_ACQUIRED = 'a';
  static final protected char LOCK_WAITING = 'w';

  // Lock types
  static final protected char LOCK_EXCLUSIVE = 'e';
  static final protected char LOCK_SHARED = 'r';
  static final protected char LOCK_SEMI_SHARED = 'w';

  static final private int ALLOWED_REPEATED_DEADLOCKS = 10;
  static final private int TIMED_OUT_TXN_ABORT_BATCH_SIZE = 100;
  static final private Log LOG = LogFactory.getLog(TxnHandler.class.getName());

  static private DataSource connPool;
  static private boolean doRetryOnConnPool = false;
  private final static Object lockLock = new Object(); // Random object to lock on for the lock
  // method

  /**
   * Number of consecutive deadlocks we have seen
   */
  private int deadlockCnt;
  private final long deadlockRetryInterval;
  protected HiveConf conf;
  protected DatabaseProduct dbProduct;

  // (End user) Transaction timeout, in milliseconds.
  private long timeout;

  private String identifierQuoteString; // quotes to use for quoting tables, where necessary
  private final long retryInterval;
  private final int retryLimit;
  private int retryNum;

  // DEADLOCK DETECTION AND HANDLING
  // A note to developers of this class.  ALWAYS access HIVE_LOCKS before TXNS to avoid deadlock
  // between simultaneous accesses.  ALWAYS access TXN_COMPONENTS before HIVE_LOCKS .
  //
  // Private methods should never catch SQLException and then throw MetaException.  The public
  // methods depend on SQLException coming back so they can detect and handle deadlocks.  Private
  // methods should only throw MetaException when they explicitly know there's a logic error and
  // they want to throw past the public methods.
  //
  // All public methods that write to the database have to check for deadlocks when a SQLException
  // comes back and handle it if they see one.  This has to be done with the connection pooling
  // in mind.  To do this they should call checkRetryable() AFTER rolling back the db transaction,
  // and then they should catch RetryException and call themselves recursively. See commitTxn for an example.

  public TxnHandler(HiveConf conf) {
    this.conf = conf;

    checkQFileTestHack();

    // Set up the JDBC connection pool
    try {
      setupJdbcConnectionPool(conf);
    } catch (SQLException e) {
      String msg = "Unable to instantiate JDBC connection pooling, " + e.getMessage();
      LOG.error(msg);
      throw new RuntimeException(e);
    }

    timeout = HiveConf.getTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_TIMEOUT, TimeUnit.MILLISECONDS);
    buildJumpTable();
    retryInterval = HiveConf.getTimeVar(conf, HiveConf.ConfVars.HMSHANDLERINTERVAL,
        TimeUnit.MILLISECONDS);
    retryLimit = HiveConf.getIntVar(conf, HiveConf.ConfVars.HMSHANDLERATTEMPTS);
    deadlockRetryInterval = retryInterval / 10;

  }

  public GetOpenTxnsInfoResponse getOpenTxnsInfo() throws MetaException {
    try {
      // We need to figure out the current transaction number and the list of
      // open transactions.  To avoid needing a transaction on the underlying
      // database we'll look at the current transaction number first.  If it
      // subsequently shows up in the open list that's ok.
      Connection dbConn = null;
      Statement stmt = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        String s = "select ntxn_next - 1 from NEXT_TXN_ID";
        LOG.debug("Going to execute query <" + s + ">");
        ResultSet rs = stmt.executeQuery(s);
        if (!rs.next()) {
          throw new MetaException("Transaction tables not properly " +
            "initialized, no record found in next_txn_id");
        }
        long hwm = rs.getLong(1);
        if (rs.wasNull()) {
          throw new MetaException("Transaction tables not properly " +
            "initialized, null record found in next_txn_id");
        }

        List<TxnInfo> txnInfo = new ArrayList<TxnInfo>();
        //need the WHERE clause below to ensure consistent results with READ_COMMITTED
        s = "select txn_id, txn_state, txn_user, txn_host from TXNS where txn_id <= " + hwm;
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
          txnInfo.add(new TxnInfo(rs.getLong(1), state, rs.getString(3), rs.getString(4)));
        }
        LOG.debug("Going to rollback");
        dbConn.rollback();
        return new GetOpenTxnsInfoResponse(hwm, txnInfo);
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "getOpenTxnsInfo");
        throw new MetaException("Unable to select from transaction database: " + getMessage(e)
          + StringUtils.stringifyException(e));
      } finally {
        closeStmt(stmt);
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      return getOpenTxnsInfo();
    }
  }

  public GetOpenTxnsResponse getOpenTxns() throws MetaException {
    try {
      // We need to figure out the current transaction number and the list of
      // open transactions.  To avoid needing a transaction on the underlying
      // database we'll look at the current transaction number first.  If it
      // subsequently shows up in the open list that's ok.
      Connection dbConn = null;
      Statement stmt = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        String s = "select ntxn_next - 1 from NEXT_TXN_ID";
        LOG.debug("Going to execute query <" + s + ">");
        ResultSet rs = stmt.executeQuery(s);
        if (!rs.next()) {
          throw new MetaException("Transaction tables not properly " +
            "initialized, no record found in next_txn_id");
        }
        long hwm = rs.getLong(1);
        if (rs.wasNull()) {
          throw new MetaException("Transaction tables not properly " +
            "initialized, null record found in next_txn_id");
        }

        Set<Long> openList = new HashSet<Long>();
        //need the WHERE clause below to ensure consistent results with READ_COMMITTED
        s = "select txn_id from TXNS where txn_id <= " + hwm;
        LOG.debug("Going to execute query<" + s + ">");
        rs = stmt.executeQuery(s);
        while (rs.next()) {
          openList.add(rs.getLong(1));
        }
        LOG.debug("Going to rollback");
        dbConn.rollback();
        return new GetOpenTxnsResponse(hwm, openList);
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "getOpenTxns");
        throw new MetaException("Unable to select from transaction database, "
          + StringUtils.stringifyException(e));
      } finally {
        closeStmt(stmt);
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      return getOpenTxns();
    }
  }

  /**
   * Transform a {@link org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse} to a
   * {@link org.apache.hadoop.hive.common.ValidTxnList}.  This assumes that the caller intends to
   * read the files, and thus treats both open and aborted transactions as invalid.
   * @param txns txn list from the metastore
   * @param currentTxn Current transaction that the user has open.  If this is greater than 0 it
   *                   will be removed from the exceptions list so that the user sees his own
   *                   transaction as valid.
   * @return a valid txn list.
   */
  public static ValidTxnList createValidReadTxnList(GetOpenTxnsResponse txns, long currentTxn) {
    long highWater = txns.getTxn_high_water_mark();
    Set<Long> open = txns.getOpen_txns();
    long[] exceptions = new long[open.size() - (currentTxn > 0 ? 1 : 0)];
    int i = 0;
    for(long txn: open) {
      if (currentTxn > 0 && currentTxn == txn) continue;
      exceptions[i++] = txn;
    }
    return new ValidReadTxnList(exceptions, highWater);
  }

  public OpenTxnsResponse openTxns(OpenTxnRequest rqst) throws MetaException {
    int numTxns = rqst.getNum_txns();
    try {
      Connection dbConn = null;
      Statement stmt = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_SERIALIZABLE);
        // Make sure the user has not requested an insane amount of txns.
        int maxTxns = HiveConf.getIntVar(conf,
          HiveConf.ConfVars.HIVE_TXN_MAX_OPEN_BATCH);
        if (numTxns > maxTxns) numTxns = maxTxns;

        stmt = dbConn.createStatement();
        String s = "select ntxn_next from NEXT_TXN_ID";
        LOG.debug("Going to execute query <" + s + ">");
        ResultSet rs = stmt.executeQuery(s);
        if (!rs.next()) {
          throw new MetaException("Transaction database not properly " +
            "configured, can't find next transaction id.");
        }
        long first = rs.getLong(1);
        s = "update NEXT_TXN_ID set ntxn_next = " + (first + numTxns);
        LOG.debug("Going to execute update <" + s + ">");
        stmt.executeUpdate(s);
        long now = getDbTime(dbConn);
        s = "insert into TXNS (txn_id, txn_state, txn_started, " +
          "txn_last_heartbeat, txn_user, txn_host) values (?, 'o', " + now + ", " +
          now + ", '" + rqst.getUser() + "', '" + rqst.getHostname() + "')";
        LOG.debug("Going to prepare statement <" + s + ">");
        PreparedStatement ps = dbConn.prepareStatement(s);
        List<Long> txnIds = new ArrayList<Long>(numTxns);
        for (long i = first; i < first + numTxns; i++) {
          ps.setLong(1, i);
          ps.executeUpdate();
          txnIds.add(i);
        }

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
        closeStmt(stmt);
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      return openTxns(rqst);
    }
  }

  public void abortTxn(AbortTxnRequest rqst) throws NoSuchTxnException, MetaException {
    long txnid = rqst.getTxnid();
    try {
      Connection dbConn = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_SERIALIZABLE);
        if (abortTxns(dbConn, Collections.singletonList(txnid)) != 1) {
          LOG.debug("Going to rollback");
          dbConn.rollback();
          throw new NoSuchTxnException("No such transaction " + JavaUtils.txnIdToString(txnid));
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
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      abortTxn(rqst);
    }
  }

  public void commitTxn(CommitTxnRequest rqst)
    throws NoSuchTxnException, TxnAbortedException,  MetaException {
    long txnid = rqst.getTxnid();
    try {
      Connection dbConn = null;
      Statement stmt = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_SERIALIZABLE);
        stmt = dbConn.createStatement();
        // Before we do the commit heartbeat the txn.  This is slightly odd in that we're going to
        // commit it, but it does two things.  One, it makes sure the transaction is still valid.
        // Two, it avoids the race condition where we time out between now and when we actually
        // commit the transaction below.  And it does this all in a dead-lock safe way by
        // committing the heartbeat back to the database.
        heartbeatTxn(dbConn, txnid);

        // Move the record from txn_components into completed_txn_components so that the compactor
        // knows where to look to compact.
        String s = "insert into COMPLETED_TXN_COMPONENTS select tc_txnid, tc_database, tc_table, " +
          "tc_partition from TXN_COMPONENTS where tc_txnid = " + txnid;
        LOG.debug("Going to execute insert <" + s + ">");
        if (stmt.executeUpdate(s) < 1) {
          //this can be reasonable for an empty txn START/COMMIT
          LOG.info("Expected to move at least one record from txn_components to " +
            "completed_txn_components when committing txn! " + JavaUtils.txnIdToString(txnid));
        }

        // Always access TXN_COMPONENTS before HIVE_LOCKS;
        s = "delete from TXN_COMPONENTS where tc_txnid = " + txnid;
        LOG.debug("Going to execute update <" + s + ">");
        stmt.executeUpdate(s);
        // Always access HIVE_LOCKS before TXNS
        s = "delete from HIVE_LOCKS where hl_txnid = " + txnid;
        LOG.debug("Going to execute update <" + s + ">");
        stmt.executeUpdate(s);
        s = "delete from TXNS where txn_id = " + txnid;
        LOG.debug("Going to execute update <" + s + ">");
        stmt.executeUpdate(s);
        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "commitTxn(" + rqst + ")");
        throw new MetaException("Unable to update transaction database "
          + StringUtils.stringifyException(e));
      } finally {
        closeStmt(stmt);
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      commitTxn(rqst);
    }
  }

  public LockResponse lock(LockRequest rqst)
    throws NoSuchTxnException, TxnAbortedException, MetaException {
    try {
      Connection dbConn = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_SERIALIZABLE);
        return lock(dbConn, rqst, true);
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "lock(" + rqst + ")");
        throw new MetaException("Unable to update transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      return lock(rqst);
    }
  }

  public LockResponse lockNoWait(LockRequest rqst)
    throws NoSuchTxnException,  TxnAbortedException, MetaException {
    try {
      Connection dbConn = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_SERIALIZABLE);
        return lock(dbConn, rqst, false);
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "lockNoWait(" + rqst + ")");
        throw new MetaException("Unable to update transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      return lockNoWait(rqst);
    }
  }

  public LockResponse checkLock(CheckLockRequest rqst)
    throws NoSuchTxnException, NoSuchLockException, TxnAbortedException, MetaException {
    try {
      Connection dbConn = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_SERIALIZABLE);
        long extLockId = rqst.getLockid();

        // Heartbeat on the lockid first, to assure that our lock is still valid.
        // Then look up the lock info (hopefully in the cache).  If these locks
        // are associated with a transaction then heartbeat on that as well.
        heartbeatLock(dbConn, extLockId);
        long txnid = getTxnIdFromLockId(dbConn, extLockId);
        if (txnid > 0)  heartbeatTxn(dbConn, txnid);
        return checkLock(dbConn, extLockId, true);
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "checkLock(" + rqst + " )");
        throw new MetaException("Unable to update transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      return checkLock(rqst);
    }

  }

  public void unlock(UnlockRequest rqst)
    throws NoSuchLockException, TxnOpenException, MetaException {
    try {
      Connection dbConn = null;
      Statement stmt = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_SERIALIZABLE);
        // Odd as it seems, we need to heartbeat first because this touches the
        // lock table and assures that our locks our still valid.  If they are
        // not, this will throw an exception and the heartbeat will fail.
        long extLockId = rqst.getLockid();
        heartbeatLock(dbConn, extLockId);
        long txnid = getTxnIdFromLockId(dbConn, extLockId);
        // If there is a valid txnid, throw an exception,
        // as locks associated with transactions should be unlocked only when the
        // transaction is committed or aborted.
        if (txnid > 0) {
          LOG.debug("Going to rollback");
          dbConn.rollback();
          String msg = "Unlocking locks associated with transaction" +
            " not permitted.  Lockid " + JavaUtils.lockIdToString(extLockId) + " is associated with " +
            "transaction " + JavaUtils.txnIdToString(txnid);
          LOG.error(msg);
          throw new TxnOpenException(msg);
        }
        stmt = dbConn.createStatement();
        String s = "delete from HIVE_LOCKS where hl_lock_ext_id = " + extLockId;
        LOG.debug("Going to execute update <" + s + ">");
        int rc = stmt.executeUpdate(s);
        if (rc < 1) {
          LOG.debug("Going to rollback");
          dbConn.rollback();
          throw new NoSuchLockException("No such lock " + JavaUtils.lockIdToString(extLockId));
        }
        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "unlock(" + rqst + ")");
        throw new MetaException("Unable to update transaction database " +
          StringUtils.stringifyException(e));
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
    LockInfoExt(ShowLocksResponseElement e, long intLockId) {
      super(e, intLockId);
      this.e = e;
    }
  }
  public ShowLocksResponse showLocks(ShowLocksRequest rqst) throws MetaException {
    try {
      Connection dbConn = null;
      ShowLocksResponse rsp = new ShowLocksResponse();
      List<ShowLocksResponseElement> elems = new ArrayList<ShowLocksResponseElement>();
      List<LockInfoExt> sortedList = new ArrayList<LockInfoExt>();
      Statement stmt = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();

        String s = "select hl_lock_ext_id, hl_txnid, hl_db, hl_table, hl_partition, hl_lock_state, " +
          "hl_lock_type, hl_last_heartbeat, hl_acquired_at, hl_user, hl_host, hl_lock_int_id from HIVE_LOCKS";
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
          sortedList.add(new LockInfoExt(e, rs.getLong(12)));
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

  public HeartbeatTxnRangeResponse heartbeatTxnRange(HeartbeatTxnRangeRequest rqst)
    throws MetaException {
    try {
      Connection dbConn = null;
      HeartbeatTxnRangeResponse rsp = new HeartbeatTxnRangeResponse();
      Set<Long> nosuch = new HashSet<Long>();
      Set<Long> aborted = new HashSet<Long>();
      rsp.setNosuch(nosuch);
      rsp.setAborted(aborted);
      try {
        dbConn = getDbConn(Connection.TRANSACTION_SERIALIZABLE);
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
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      return heartbeatTxnRange(rqst);
    }
  }

  public void compact(CompactionRequest rqst) throws MetaException {
    // Put a compaction request in the queue.
    try {
      Connection dbConn = null;
      Statement stmt = null;
      try {
        dbConn = getDbConn(Connection.TRANSACTION_SERIALIZABLE);
        stmt = dbConn.createStatement();

        // Get the id for the next entry in the queue
        String s = "select ncq_next from NEXT_COMPACTION_QUEUE_ID";
        LOG.debug("going to execute query <" + s + ">");
        ResultSet rs = stmt.executeQuery(s);
        if (!rs.next()) {
          LOG.debug("Going to rollback");
          dbConn.rollback();
          throw new MetaException("Transaction tables not properly initiated, " +
            "no record found in next_compaction_queue_id");
        }
        long id = rs.getLong(1);
        s = "update NEXT_COMPACTION_QUEUE_ID set ncq_next = " + (id + 1);
        LOG.debug("Going to execute update <" + s + ">");
        stmt.executeUpdate(s);

        StringBuilder buf = new StringBuilder("insert into COMPACTION_QUEUE (cq_id, cq_database, " +
          "cq_table, ");
        String partName = rqst.getPartitionname();
        if (partName != null) buf.append("cq_partition, ");
        buf.append("cq_state, cq_type");
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
        if (rqst.getRunas() != null) {
          buf.append("', '");
          buf.append(rqst.getRunas());
        }
        buf.append("')");
        s = buf.toString();
        LOG.debug("Going to execute update <" + s + ">");
        stmt.executeUpdate(s);
        LOG.debug("Going to commit");
        dbConn.commit();
      } catch (SQLException e) {
        LOG.debug("Going to rollback");
        rollbackDBConn(dbConn);
        checkRetryable(dbConn, e, "compact(" + rqst + ")");
        throw new MetaException("Unable to select from transaction database " +
          StringUtils.stringifyException(e));
      } finally {
        closeStmt(stmt);
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      compact(rqst);
    }
  }

  public ShowCompactResponse showCompact(ShowCompactRequest rqst) throws MetaException {
    ShowCompactResponse response = new ShowCompactResponse(new ArrayList<ShowCompactResponseElement>());
    Connection dbConn = null;
    Statement stmt = null;
    try {
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        String s = "select cq_database, cq_table, cq_partition, cq_state, cq_type, cq_worker_id, " +
          "cq_start, cq_run_as from COMPACTION_QUEUE";
        LOG.debug("Going to execute query <" + s + ">");
        ResultSet rs = stmt.executeQuery(s);
        while (rs.next()) {
          ShowCompactResponseElement e = new ShowCompactResponseElement();
          e.setDbname(rs.getString(1));
          e.setTablename(rs.getString(2));
          e.setPartitionname(rs.getString(3));
          switch (rs.getString(4).charAt(0)) {
            case INITIATED_STATE: e.setState(INITIATED_RESPONSE); break;
            case WORKING_STATE: e.setState(WORKING_RESPONSE); break;
            case READY_FOR_CLEANING: e.setState(CLEANING_RESPONSE); break;
            default: throw new MetaException("Unexpected compaction state " + rs.getString(4));
          }
          switch (rs.getString(5).charAt(0)) {
            case MAJOR_TYPE: e.setType(CompactionType.MAJOR); break;
            case MINOR_TYPE: e.setType(CompactionType.MINOR); break;
            default: throw new MetaException("Unexpected compaction type " + rs.getString(5));
          }
          e.setWorkerid(rs.getString(6));
          e.setStart(rs.getLong(7));
          e.setRunAs(rs.getString(8));
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

  public void addDynamicPartitions(AddDynamicPartitions rqst)
      throws NoSuchTxnException,  TxnAbortedException, MetaException {
    Connection dbConn = null;
    Statement stmt = null;
    try {
      try {
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();
        // Heartbeat this first to make sure the transaction is still valid.
        heartbeatTxn(dbConn, rqst.getTxnid());
        for (String partName : rqst.getPartitionnames()) {
          StringBuilder buff = new StringBuilder();
          buff.append("insert into TXN_COMPONENTS (tc_txnid, tc_database, tc_table, tc_partition) values (");
          buff.append(rqst.getTxnid());
          buff.append(", '");
          buff.append(rqst.getDbname());
          buff.append("', '");
          buff.append(rqst.getTablename());
          buff.append("', '");
          buff.append(partName);
          buff.append("')");
          String s = buff.toString();
          LOG.debug("Going to execute update <" + s + ">");
          stmt.executeUpdate(s);
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
        closeStmt(stmt);
        closeDbConn(dbConn);
      }
    } catch (RetryException e) {
      addDynamicPartitions(rqst);
    }
  }

  /**
   * For testing only, do not use.
   */
  int numLocksInLockTable() throws SQLException, MetaException {
    Connection dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
    Statement stmt = null;
    try {
      stmt = dbConn.createStatement();
      String s = "select count(*) from HIVE_LOCKS";
      LOG.debug("Going to execute query <" + s + ">");
      ResultSet rs = stmt.executeQuery(s);
      rs.next();
      int rc = rs.getInt(1);
      // Necessary to clean up the transaction in the db.
      dbConn.rollback();
      return rc;
    } finally {
      closeDbConn(dbConn);
      closeStmt(stmt);
    }
  }

  /**
   * For testing only, do not use.
   */
  long setTimeout(long milliseconds) {
    long previous_timeout = timeout;
    timeout = milliseconds;
    return previous_timeout;
  }

  protected class RetryException extends Exception {

  }

  protected Connection getDbConn(int isolationLevel) throws SQLException {
    int rc = doRetryOnConnPool ? 10 : 1;
    while (true) {
      try {
        Connection dbConn = connPool.getConnection();
        dbConn.setAutoCommit(false);
        dbConn.setTransactionIsolation(isolationLevel);
        return dbConn;
      } catch (SQLException e){
        if ((--rc) <= 0) throw e;
        LOG.error("There is a problem with a connection from the pool, retrying", e);
      }
    }
  }

  void rollbackDBConn(Connection dbConn) {
    try {
      if (dbConn != null && !dbConn.isClosed()) dbConn.rollback();
    } catch (SQLException e) {
      LOG.warn("Failed to rollback db connection " + getMessage(e));
    }
  }
  protected void closeDbConn(Connection dbConn) {
    try {
      if (dbConn != null && !dbConn.isClosed()) dbConn.close();
    } catch (SQLException e) {
      LOG.warn("Failed to close db connection " + getMessage(e));
    }
  }

  /**
   * Close statement instance.
   * @param stmt statement instance.
   */
  protected void closeStmt(Statement stmt) {
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
  void close(ResultSet rs) {
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
  void close(ResultSet rs, Statement stmt, Connection dbConn) {
    close(rs);
    closeStmt(stmt);
    closeDbConn(dbConn);
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
      if (dbProduct == null && conn != null) {
        determineDatabaseProduct(conn);
      }
      if (e instanceof SQLTransactionRollbackException ||
        ((dbProduct == DatabaseProduct.MYSQL || dbProduct == DatabaseProduct.POSTGRES ||
          dbProduct == DatabaseProduct.SQLSERVER) && e.getSQLState().equals("40001")) ||
        (dbProduct == DatabaseProduct.POSTGRES && e.getSQLState().equals("40P01")) ||
        (dbProduct == DatabaseProduct.ORACLE && (e.getMessage().contains("deadlock detected")
          || e.getMessage().contains("can't serialize access for this transaction")))) {
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
      } else if (isRetryable(e)) {
        //in MSSQL this means Communication Link Failure
        if (retryNum++ < retryLimit) {
          LOG.warn("Retryable error detected in " + caller + ".  Will wait " + retryInterval +
            "ms and retry up to " + (retryLimit - retryNum + 1) + " times.  Error: " + getMessage(e));
          try {
            Thread.sleep(retryInterval);
          } catch (InterruptedException ex) {
            //
          }
          sendRetrySignal = true;
        } else {
          LOG.error("Fatal error. Retry limit (" + retryLimit + ") reached. Last error: " + getMessage(e));
        }
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
      DatabaseProduct prod = determineDatabaseProduct(conn);
      switch (prod) {
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
          String msg = "Unknown database product: " + prod.toString();
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

  protected enum DatabaseProduct { DERBY, MYSQL, POSTGRES, ORACLE, SQLSERVER}

  /**
   * Determine the database product type
   * @param conn database connection
   * @return database product type
   * @throws MetaException if the type cannot be determined or is unknown
   */
  protected DatabaseProduct determineDatabaseProduct(Connection conn) throws MetaException {
    if (dbProduct == null) {
      try {
        String s = conn.getMetaData().getDatabaseProductName();
        if (s == null) {
          String msg = "getDatabaseProductName returns null, can't determine database product";
          LOG.error(msg);
          throw new MetaException(msg);
        } else if (s.equals("Apache Derby")) {
          dbProduct = DatabaseProduct.DERBY;
        } else if (s.equals("Microsoft SQL Server")) {
          dbProduct = DatabaseProduct.SQLSERVER;
        } else if (s.equals("MySQL")) {
          dbProduct = DatabaseProduct.MYSQL;
        } else if (s.equals("Oracle")) {
          dbProduct = DatabaseProduct.ORACLE;
        } else if (s.equals("PostgreSQL")) {
          dbProduct = DatabaseProduct.POSTGRES;
        } else {
          String msg = "Unrecognized database product name <" + s + ">";
          LOG.error(msg);
          throw new MetaException(msg);
        }

      } catch (SQLException e) {
        String msg = "Unable to get database product name: " + e.getMessage();
        LOG.error(msg);
        throw new MetaException(msg);
      }
    }
    return dbProduct;
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
    LockInfo(ShowLocksResponseElement e, long intLockId) {
      extLockId = e.getLockid();
      this.intLockId = intLockId;
      db = e.getDbname();
      table = e.getTablename();
      partition = e.getPartname();
      state = e.getState();
      type = e.getType();
      txnId = e.getTxnid();
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
  }

  private static class LockInfoComparator implements Comparator<LockInfo> {
    private static final LockTypeComparator lockTypeComparator = new LockTypeComparator();
    public boolean equals(Object other) {
      return this == other;
    }

    public int compare(LockInfo info1, LockInfo info2) {
      // We sort by state (acquired vs waiting) and then by LockType, they by id
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
   * Sort more restrictive locks after less restrictive ones
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
    boolean hackOn = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_IN_TEST) ||
      HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_IN_TEZ_TEST);
    if (hackOn) {
      LOG.info("Hacking in canned values for transaction manager");
      // Set up the transaction/locking db in the derby metastore
      TxnDbUtil.setConfValues(conf);
      try {
        TxnDbUtil.prepDb();
      } catch (Exception e) {
        // We may have already created the tables and thus don't need to redo it.
        if (!e.getMessage().contains("already exists")) {
          throw new RuntimeException("Unable to set up transaction database for" +
            " testing: " + e.getMessage());
        }
      }
    }
  }

  /**
   * Abort a group of txns
   * @param dbConn An active connection
   * @param txnids list of transactions to abort
   * @return Number of aborted transactions
   * @throws SQLException
   */
  private int abortTxns(Connection dbConn, List<Long> txnids) throws SQLException {
    Statement stmt = null;
    int updateCnt = 0;
    if (txnids.isEmpty()) {
      return 0;
    }
    try {
      stmt = dbConn.createStatement();

      // delete from HIVE_LOCKS first, we always access HIVE_LOCKS before TXNS
      StringBuilder buf = new StringBuilder("delete from HIVE_LOCKS where hl_txnid in (");
      boolean first = true;
      for (Long id : txnids) {
        if (first) first = false;
        else buf.append(',');
        buf.append(id);
      }
      buf.append(')');
      LOG.debug("Going to execute update <" + buf.toString() + ">");
      stmt.executeUpdate(buf.toString());

      buf = new StringBuilder("update TXNS set txn_state = '" + TXN_ABORTED +
        "' where txn_state = '" + TXN_OPEN + "' and txn_id in (");
      first = true;
      for (Long id : txnids) {
        if (first) first = false;
        else buf.append(',');
        buf.append(id);
      }
      buf.append(')');
      LOG.debug("Going to execute update <" + buf.toString() + ">");
      updateCnt = stmt.executeUpdate(buf.toString());

    } finally {
      closeStmt(stmt);
    }
    return updateCnt;
  }

  /**
   * Request a lock
   * @param dbConn database connection
   * @param rqst lock information
   * @param wait whether to wait for this lock.  The function will return immediately one way or
   *             another.  If true and the lock could not be acquired the response will have a
   *             state of  WAITING.  The caller will then need to poll using
   *             {@link #checkLock(org.apache.hadoop.hive.metastore.api.CheckLockRequest)}. If
   *             false and the  lock could not be acquired, then the response will have a state
   *             of NOT_ACQUIRED.  The caller will need to call
   *             {@link #lockNoWait(org.apache.hadoop.hive.metastore.api.LockRequest)} again to
   *             attempt another lock.
   * @return information on whether the lock was acquired.
   * @throws NoSuchTxnException
   * @throws TxnAbortedException
   */
  private LockResponse lock(Connection dbConn, LockRequest rqst, boolean wait)
    throws NoSuchTxnException,  TxnAbortedException, MetaException, SQLException {
    // We want to minimize the number of concurrent lock requests being issued.  If we do not we
    // get a large number of deadlocks in the database, since this method has to both clean
    // timedout locks and insert new locks.  This synchronization barrier will not eliminate all
    // deadlocks, and the code is still resilient in the face of a database deadlock.  But it
    // will reduce the number.  This could have been done via a lock table command in the
    // underlying database, but was not for two reasons.  One, different databases have different
    // syntax for lock table, making it harder to use.  Two, that would lock the HIVE_LOCKS table
    // and prevent other operations (such as committing transactions, showing locks,
    // etc.) that should not interfere with this one.
    synchronized (lockLock) {
      Statement stmt = null;
      try {
        stmt = dbConn.createStatement();

        // Get the next lock id.
        String s = "select nl_next from NEXT_LOCK_ID";
        LOG.debug("Going to execute query <" + s + ">");
        ResultSet rs = stmt.executeQuery(s);
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
        LOG.debug("Going to commit.");
        dbConn.commit();

        long txnid = rqst.getTxnid();
        if (txnid > 0) {
          // Heartbeat the transaction so we know it is valid and we avoid it timing out while we
          // are locking.
          heartbeatTxn(dbConn, txnid);

          // For each component in this lock request,
          // add an entry to the txn_components table
          // This must be done before HIVE_LOCKS is accessed
          for (LockComponent lc : rqst.getComponent()) {
            String dbName = lc.getDbname();
            String tblName = lc.getTablename();
            String partName = lc.getPartitionname();
            s = "insert into TXN_COMPONENTS " +
              "(tc_txnid, tc_database, tc_table, tc_partition) " +
              "values (" + txnid + ", '" + dbName + "', " +
              (tblName == null ? "null" : "'" + tblName + "'") + ", " +
              (partName == null ? "null" : "'" +  partName + "'") + ")";
            LOG.debug("Going to execute update <" + s + ">");
            stmt.executeUpdate(s);
          }
        }

        long intLockId = 0;
        for (LockComponent lc : rqst.getComponent()) {
          intLockId++;
          String dbName = lc.getDbname();
          String tblName = lc.getTablename();
          String partName = lc.getPartitionname();
          LockType lockType = lc.getType();
          char lockChar = 'z';
          switch (lockType) {
            case EXCLUSIVE: lockChar = LOCK_EXCLUSIVE; break;
            case SHARED_READ: lockChar = LOCK_SHARED; break;
            case SHARED_WRITE: lockChar = LOCK_SEMI_SHARED; break;
          }
          long now = getDbTime(dbConn);
          s = "insert into HIVE_LOCKS " +
            " (hl_lock_ext_id, hl_lock_int_id, hl_txnid, hl_db, hl_table, " +
            "hl_partition, hl_lock_state, hl_lock_type, hl_last_heartbeat, hl_user, hl_host)" +
            " values (" + extLockId + ", " +
            + intLockId + "," + (txnid >= 0 ? txnid : "null") + ", '" +
            dbName + "', " + (tblName == null ? "null" : "'" + tblName + "'" )
            + ", " + (partName == null ? "null" : "'" + partName + "'") +
            ", '" + LOCK_WAITING + "', " +  "'" + lockChar + "', " + now + ", '" +
            rqst.getUser() + "', '" + rqst.getHostname() + "')";
          LOG.debug("Going to execute update <" + s + ">");
          stmt.executeUpdate(s);
        }
        LockResponse rsp = checkLock(dbConn, extLockId, wait);
        if (!wait && rsp.getState() != LockState.ACQUIRED) {
          LOG.debug("Lock not acquired, going to rollback");
          dbConn.rollback();
          rsp = new LockResponse();
          rsp.setState(LockState.NOT_ACQUIRED);
        }
        return rsp;
      } catch (NoSuchLockException e) {
        // This should never happen, as we just added the lock id
        throw new MetaException("Couldn't find a lock we just created!");
      } finally {
        closeStmt(stmt);
      }
    }
  }

  private LockResponse checkLock(Connection dbConn,
                                 long extLockId,
                                 boolean alwaysCommit)
    throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, MetaException, SQLException {
    List<LockInfo> locksBeingChecked = getLockInfoFromLockId(dbConn, extLockId);//being acquired now
    LockResponse response = new LockResponse();
    response.setLockid(extLockId);

    LOG.debug("checkLock(): Setting savepoint. extLockId=" + JavaUtils.lockIdToString(extLockId));
    Savepoint save = dbConn.setSavepoint();
    StringBuilder query = new StringBuilder("select hl_lock_ext_id, " +
      "hl_lock_int_id, hl_db, hl_table, hl_partition, hl_lock_state, " +
      "hl_lock_type, hl_txnid from HIVE_LOCKS where hl_db in (");

    Set<String> strings = new HashSet<String>(locksBeingChecked.size());
    for (LockInfo info : locksBeingChecked) {
      strings.add(info.db);
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
    query.append(" and hl_lock_ext_id <= ").append(extLockId);

    LOG.debug("Going to execute query <" + query.toString() + ">");
    Statement stmt = null;
    try {
      stmt = dbConn.createStatement();
      ResultSet rs = stmt.executeQuery(query.toString());
      SortedSet<LockInfo> lockSet = new TreeSet<LockInfo>(new LockInfoComparator());
      while (rs.next()) {
        lockSet.add(new LockInfo(rs));
      }
      // Turn the tree set into an array so we can move back and forth easily
      // in it.
      LockInfo[] locks = lockSet.toArray(new LockInfo[lockSet.size()]);
      if(LOG.isDebugEnabled()) {
        LOG.debug("Locks to check(full): ");
        for(LockInfo info : locks) {
          LOG.debug("  " + info);
        }
      }

      for (LockInfo info : locksBeingChecked) {
        // Find the lock record we're checking
        int index = -1;
        for (int i = 0; i < locks.length; i++) {
          if (locks[i].equals(info)) {
            index = i;
            break;
          }
        }

        // If we didn't find the lock, then it must not be in the table
        if (index == -1) {
          LOG.debug("Going to rollback");
          dbConn.rollback();
          throw new MetaException("How did we get here, we heartbeated our lock before we started!");
        }


        // If we've found it and it's already been marked acquired,
        // then just look at the other locks.
        if (locks[index].state == LockState.ACQUIRED) {
          continue;
        }

        // Look at everything in front of this lock to see if it should block
        // it or not.
        boolean acquired = false;
        for (int i = index - 1; i >= 0; i--) {
          // Check if we're operating on the same database, if not, move on
          if (!locks[index].db.equals(locks[i].db)) {
            continue;
          }

          // If table is null on either of these, then they are claiming to
          // lock the whole database and we need to check it.  Otherwise,
          // check if they are operating on the same table, if not, move on.
          if (locks[index].table != null && locks[i].table != null
            && !locks[index].table.equals(locks[i].table)) {
            continue;
          }

          // If partition is null on either of these, then they are claiming to
          // lock the whole table and we need to check it.  Otherwise,
          // check if they are operating on the same partition, if not, move on.
          if (locks[index].partition != null && locks[i].partition != null
            && !locks[index].partition.equals(locks[i].partition)) {
            continue;
          }

          // We've found something that matches what we're trying to lock,
          // so figure out if we can lock it too.
          LockAction lockAction = jumpTable.get(locks[index].type).get(locks[i].type).get(locks[i].state);
          LOG.debug("desired Lock: " + info + " checked Lock: " + locks[i] + " action: " + lockAction);
          switch (lockAction) {
            case WAIT:
              if(!ignoreConflict(info, locks[i])) {
                wait(dbConn, save);
                if (alwaysCommit) {
                  // In the case where lockNoWait has been called we don't want to commit because
                  // it's going to roll everything back. In every other case we want to commit here.
                  LOG.debug("Going to commit");
                  dbConn.commit();
                }
                response.setState(LockState.WAITING);
                LOG.debug("Lock(" + info + ") waiting for Lock(" + locks[i] + ")");
                return response;
              }
              //fall through to ACQUIRE
            case ACQUIRE:
              acquire(dbConn, stmt, extLockId, info.intLockId);
              acquired = true;
              break;
            case KEEP_LOOKING:
              continue;
          }
          if (acquired) break; // We've acquired this lock component,
          // so get out of the loop and look at the next component.
        }

        // If we've arrived here and we have not already acquired, it means there's nothing in the
        // way of the lock, so acquire the lock.
        if (!acquired) acquire(dbConn, stmt, extLockId, info.intLockId);
      }

      // We acquired all of the locks, so commit and return acquired.
      LOG.debug("Going to commit");
      dbConn.commit();
      response.setState(LockState.ACQUIRED);
    } finally {
      closeStmt(stmt);
    }
    return response;
  }

  /**
   * the {@link #jumpTable} only deals with LockState/LockType.  In some cases it's not
   * sufficient.  For example, an EXCLUSIVE lock on partition should prevent SHARED_READ
   * on the table, but there is no reason for EXCLUSIVE on a table to prevent SHARED_READ
   * on a database.
   */
  private boolean ignoreConflict(LockInfo desiredLock, LockInfo existingLock) {
    return
      ((desiredLock.isDbLock() && desiredLock.type == LockType.SHARED_READ &&
          existingLock.isTableLock() && existingLock.type == LockType.EXCLUSIVE) ||
        (existingLock.isDbLock() && existingLock.type == LockType.SHARED_READ &&
          desiredLock.isTableLock() && desiredLock.type == LockType.EXCLUSIVE))
        ||
      //different locks from same txn should not conflict with each other
      (desiredLock.txnId != 0 && desiredLock.txnId == existingLock.txnId) ||
      //txnId=0 means it's a select or IUD which does not write to ACID table, e.g
      //insert overwrite table T partition(p=1) select a,b from T and autoCommit=true
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

  private void acquire(Connection dbConn, Statement stmt, long extLockId, long intLockId)
    throws SQLException, NoSuchLockException, MetaException {
    long now = getDbTime(dbConn);
    String s = "update HIVE_LOCKS set hl_lock_state = '" + LOCK_ACQUIRED + "', " +
      "hl_last_heartbeat = " + now + ", hl_acquired_at = " + now + " where hl_lock_ext_id = " +
      extLockId + " and hl_lock_int_id = " + intLockId;
    LOG.debug("Going to execute update <" + s + ">");
    int rc = stmt.executeUpdate(s);
    if (rc < 1) {
      LOG.debug("Going to rollback");
      dbConn.rollback();
      throw new NoSuchLockException("No such lock: (" + JavaUtils.lockIdToString(extLockId) + "," +
        + intLockId + ")");
    }
    // We update the database, but we don't commit because there may be other
    // locks together with this, and we only want to acquire one if we can
    // acquire all.
  }

  // Heartbeats on the lock table.  This commits, so do not enter it with any state
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
      ensureValidTxn(dbConn, txnid, stmt);
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
      //update locks for this txn to the same heartbeat
      s = "update HIVE_LOCKS set hl_last_heartbeat = " + now + " where hl_txnid = " + txnid;
      LOG.debug("Going to execute update <" + s + ">");
      stmt.executeUpdate(s);
      LOG.debug("Going to commit");
      dbConn.commit();
    } finally {
      closeStmt(stmt);
    }
  }

  private static void ensureValidTxn(Connection dbConn, long txnid, Statement stmt)
      throws SQLException, NoSuchTxnException, TxnAbortedException {
    // We need to check whether this transaction is valid and open
    String s = "select txn_state from TXNS where txn_id = " + txnid;
    LOG.debug("Going to execute query <" + s + ">");
    ResultSet rs = stmt.executeQuery(s);
    if (!rs.next()) {
      s = "select count(*) from COMPLETED_TXN_COMPONENTS where CTC_TXNID = " + txnid;
      ResultSet rs2 = stmt.executeQuery(s);
      boolean alreadyCommitted = rs2.next() && rs2.getInt(1) > 0;
      LOG.debug("Going to rollback");
      dbConn.rollback();
      if(alreadyCommitted) {
        //makes the message more informative - helps to find bugs in client code
        throw new NoSuchTxnException("Transaction " + JavaUtils.txnIdToString(txnid) + " is already committed.");
      }
      throw new NoSuchTxnException("No such transaction " + JavaUtils.txnIdToString(txnid));
    }
    if (rs.getString(1).charAt(0) == TXN_ABORTED) {
      LOG.debug("Going to rollback");
      dbConn.rollback();
      throw new TxnAbortedException("Transaction " + JavaUtils.txnIdToString(txnid) +
        " already aborted");//todo: add time of abort, which is not currently tracked
    }
  }

  // NEVER call this function without first calling heartbeat(long, long)
  private long getTxnIdFromLockId(Connection dbConn, long extLockId)
    throws NoSuchLockException, MetaException, SQLException {
    Statement stmt = null;
    try {
      stmt = dbConn.createStatement();
      String s = "select hl_txnid from HIVE_LOCKS where hl_lock_ext_id = " +
        extLockId;
      LOG.debug("Going to execute query <" + s + ">");
      ResultSet rs = stmt.executeQuery(s);
      if (!rs.next()) {
        throw new MetaException("This should never happen!  We already " +
          "checked the lock existed but now we can't find it!");
      }
      long txnid = rs.getLong(1);
      LOG.debug("Return " + JavaUtils.txnIdToString(rs.wasNull() ? -1 : txnid));
      return (rs.wasNull() ? -1 : txnid);
    } finally {
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
      List<LockInfo> ourLockInfo = new ArrayList<LockInfo>();
      while (rs.next()) {
        ourLockInfo.add(new LockInfo(rs));
        sawAtLeastOne = true;
      }
      if (!sawAtLeastOne) {
        throw new MetaException("This should never happen!  We already " +
          "checked the lock existed but now we can't find it!");
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
  private void timeOutLocks(Connection dbConn) {
    Statement stmt = null;
    try {
      long now = getDbTime(dbConn);
      stmt = dbConn.createStatement();
      // Remove any timed out locks from the table.
      String s = "delete from HIVE_LOCKS where hl_last_heartbeat < " +
        (now - timeout) + " and (hl_txnid = 0 or hl_txnid is NULL)";//when txnid is > 0, the lock is
      //associated with a txn and is handled by performTimeOuts()
      //want to avoid expiring locks for a txn w/o expiring the txn itself
      LOG.debug("Going to execute update <" + s + ">");
      int deletedLocks = stmt.executeUpdate(s);
      if(deletedLocks > 0) {
        LOG.info("Deleted " + deletedLocks + " locks from HIVE_LOCKS due to timeout");
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
      closeStmt(stmt);
    }
  }

  /**
   * Suppose you have a query "select a,b from T" and you want to limit the result set
   * to the first 5 rows.  The mechanism to do that differs in different DB.
   * Make {@code noSelectsqlQuery} to be "a,b from T" and this method will return the
   * appropriately modified row limiting query.
   */
  private String addLimitClause(Connection dbConn, int numRows, String noSelectsqlQuery) throws MetaException {
    DatabaseProduct prod = determineDatabaseProduct(dbConn);
    switch (prod) {
      case DERBY:
        //http://db.apache.org/derby/docs/10.7/ref/rrefsqljoffsetfetch.html
        return "select " + noSelectsqlQuery + " fetch first " + numRows + " rows only";
      case MYSQL:
        //http://www.postgresql.org/docs/7.3/static/queries-limit.html
      case POSTGRES:
        //https://dev.mysql.com/doc/refman/5.0/en/select.html
        return "select " + noSelectsqlQuery + " limit " + numRows;
      case ORACLE:
        //newer versions (12c and later) support OFFSET/FETCH
        return "select * from (select " + noSelectsqlQuery + ") where rownum <= " + numRows;
      case SQLSERVER:
        //newer versions (2012 and later) support OFFSET/FETCH
        //https://msdn.microsoft.com/en-us/library/ms189463.aspx
        return "select TOP(" + numRows + ") " + noSelectsqlQuery;
      default:
        String msg = "Unrecognized database product name <" + prod + ">";
        LOG.error(msg);
        throw new MetaException(msg);
    }
  }
  /**
   * This will find transactions that have timed out and abort them.
   * Will also delete locks which are not associated with a transaction and have timed out
   * Tries to keep transactions (against metastore db) small to reduce lock contention.
   */
  public void performTimeOuts() {
    Connection dbConn = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      dbConn = getDbConn(Connection.TRANSACTION_SERIALIZABLE);
      long now = getDbTime(dbConn);
      timeOutLocks(dbConn);
      while(true) {
        stmt = dbConn.createStatement();
        String s = " txn_id from TXNS where txn_state = '" + TXN_OPEN +
          "' and txn_last_heartbeat <  " + (now - timeout);
        s = addLimitClause(dbConn, 2500, s);
        LOG.debug("Going to execute query <" + s + ">");
        rs = stmt.executeQuery(s);
        if(!rs.next()) {
          return;//no more timedout txns
        }
        List<List<Long>> timedOutTxns = new ArrayList<>();
        List<Long> currentBatch = new ArrayList<>(TIMED_OUT_TXN_ABORT_BATCH_SIZE);
        timedOutTxns.add(currentBatch);
        do {
          currentBatch.add(rs.getLong(1));
          if(currentBatch.size() == TIMED_OUT_TXN_ABORT_BATCH_SIZE) {
            currentBatch = new ArrayList<>(TIMED_OUT_TXN_ABORT_BATCH_SIZE);
            timedOutTxns.add(currentBatch);
          }
        } while(rs.next());
        close(rs, stmt, null);
        dbConn.commit();
        for(List<Long> batchToAbort : timedOutTxns) {
          abortTxns(dbConn, batchToAbort);
          dbConn.commit();
          //todo: add TXNS.COMMENT filed and set it to 'aborted by system due to timeout'
          LOG.info("Aborted the following transactions due to timeout: " + batchToAbort.toString());
        }
        int numTxnsAborted = (timedOutTxns.size() - 1) * TIMED_OUT_TXN_ABORT_BATCH_SIZE +
          timedOutTxns.get(timedOutTxns.size() - 1).size();
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

  private static synchronized void setupJdbcConnectionPool(HiveConf conf) throws SQLException {
    if (connPool != null) return;

    String driverUrl = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORECONNECTURLKEY);
    String user = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME);
    String passwd;
    try {
      passwd = ShimLoader.getHadoopShims().getPassword(conf,
        HiveConf.ConfVars.METASTOREPWD.varname);
    } catch (IOException err) {
      throw new SQLException("Error getting metastore password", err);
    }
    String connectionPooler = HiveConf.getVar(conf,
      HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE).toLowerCase();

    if ("bonecp".equals(connectionPooler)) {
      BoneCPConfig config = new BoneCPConfig();
      config.setJdbcUrl(driverUrl);
      config.setMaxConnectionsPerPartition(10);
      config.setPartitionCount(1);
      config.setUser(user);
      config.setPassword(passwd);
      connPool = new BoneCPDataSource(config);
      doRetryOnConnPool = true;  // Enable retries to work around BONECP bug.
    } else if ("dbcp".equals(connectionPooler)) {
      ObjectPool objectPool = new GenericObjectPool();
      ConnectionFactory connFactory = new DriverManagerConnectionFactory(driverUrl, user, passwd);
      // This doesn't get used, but it's still necessary, see
      // http://svn.apache.org/viewvc/commons/proper/dbcp/branches/DBCP_1_4_x_BRANCH/doc/ManualPoolingDataSourceExample.java?view=markup
      PoolableConnectionFactory poolConnFactory =
        new PoolableConnectionFactory(connFactory, objectPool, null, null, false, true);
      connPool = new PoolingDataSource(objectPool);
    } else {
      throw new RuntimeException("Unknown JDBC connection pooling " + connectionPooler);
    }
  }

  private static synchronized void buildJumpTable() {
    if (jumpTable != null) return;

    jumpTable =
      new HashMap<LockType, Map<LockType, Map<LockState,  LockAction>>>(3);

    // SR: Lock we are trying to acquire is shared read
    Map<LockType, Map<LockState, LockAction>> m =
      new HashMap<LockType, Map<LockState, LockAction>>(3);
    jumpTable.put(LockType.SHARED_READ, m);

    // SR.SR: Lock we are examining is shared read
    Map<LockState, LockAction> m2 = new HashMap<LockState, LockAction>(2);
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
    m2 = new HashMap<LockState, LockAction>(2);
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
    m2 = new HashMap<LockState, LockAction>(2);
    m.put(LockType.EXCLUSIVE, m2);

    // No matter whether it has acquired or not, we cannot pass an exclusive.
    m2.put(LockState.ACQUIRED, LockAction.WAIT);
    m2.put(LockState.WAITING, LockAction.WAIT);

    // SW: Lock we are trying to acquire is shared write
    m = new HashMap<LockType, Map<LockState, LockAction>>(3);
    jumpTable.put(LockType.SHARED_WRITE, m);

    // SW.SR: Lock we are examining is shared read
    m2 = new HashMap<LockState, LockAction>(2);
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
    m2 = new HashMap<LockState, LockAction>(2);
    m.put(LockType.SHARED_WRITE, m2);

    // Regardless of acquired or waiting, one shared write cannot pass another.
    m2.put(LockState.ACQUIRED, LockAction.WAIT);
    m2.put(LockState.WAITING, LockAction.WAIT);

    // SW.E: Lock we are examining is exclusive
    m2 = new HashMap<LockState, LockAction>(2);
    m.put(LockType.EXCLUSIVE, m2);

    // No matter whether it has acquired or not, we cannot pass an exclusive.
    m2.put(LockState.ACQUIRED, LockAction.WAIT);
    m2.put(LockState.WAITING, LockAction.WAIT);

    // E: Lock we are trying to acquire is exclusive
    m = new HashMap<LockType, Map<LockState, LockAction>>(3);
    jumpTable.put(LockType.EXCLUSIVE, m);

    // E.SR: Lock we are examining is shared read
    m2 = new HashMap<LockState, LockAction>(2);
    m.put(LockType.SHARED_READ, m2);

    // Exclusives can never pass
    m2.put(LockState.ACQUIRED, LockAction.WAIT);
    m2.put(LockState.WAITING, LockAction.WAIT);

    // E.SW: Lock we are examining is shared write
    m2 = new HashMap<LockState, LockAction>(2);
    m.put(LockType.SHARED_WRITE, m2);

    // Exclusives can never pass
    m2.put(LockState.ACQUIRED, LockAction.WAIT);
    m2.put(LockState.WAITING, LockAction.WAIT);

    // E.E: Lock we are examining is exclusive
    m2 = new HashMap<LockState, LockAction>(2);
    m.put(LockType.EXCLUSIVE, m2);

    // No matter whether it has acquired or not, we cannot pass an exclusive.
    m2.put(LockState.ACQUIRED, LockAction.WAIT);
    m2.put(LockState.WAITING, LockAction.WAIT);
  }
  /**
   * Returns true if {@code ex} should be retried
   */
  private static boolean isRetryable(Exception ex) {
    if(ex instanceof SQLException) {
      SQLException sqlException = (SQLException)ex;
      if("08S01".equalsIgnoreCase(sqlException.getSQLState())) {
        //in MSSQL this means Communication Link Failure
        return true;
      }
      //see https://issues.apache.org/jira/browse/HIVE-9938
    }
    return false;
  }
  private static String getMessage(SQLException ex) {
    return ex.getMessage() + "(SQLState=" + ex.getSQLState() + ",ErrorCode=" + ex.getErrorCode() + ")";
  }
}
