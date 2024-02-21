/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */
package org.apache.hadoop.hive.ql.lockmgr;

import com.cronutils.utils.StringUtils;
import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.txn.TxnCommonUtils;
import org.apache.hadoop.hive.metastore.txn.TxnErrorMsg;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.ddl.database.lock.LockDatabaseDesc;
import org.apache.hadoop.hive.ql.ddl.database.unlock.UnlockDatabaseDesc;
import org.apache.hadoop.hive.ql.ddl.table.lock.LockTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.lock.UnlockTableDesc;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.ShutdownHookManager;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An implementation of HiveTxnManager that stores the transactions in the metastore database.
 * There should be 1 instance o {@link DbTxnManager} per {@link org.apache.hadoop.hive.ql.session.SessionState}
 * with a single thread accessing it at a time, with the exception of {@link #heartbeat()} method.
 * The later may (usually will) be called from a timer thread.
 * See {@link #getMS()} for more important concurrency/metastore access notes.
 *
 * Each statement that the TM (transaction manager) should be aware of should belong to a transaction.
 * Effectively, that means any statement that has side effects.  Exceptions are statements like
 * Show Compactions, Show Tables, Use Database foo, etc.  The transaction is started either
 * explicitly ( via Start Transaction SQL statement from end user - not fully supported) or
 * implicitly by the {@link org.apache.hadoop.hive.ql.Driver} (which looks exactly as autoCommit=true
 * from end user poit of view). See more at {@link #isExplicitTransaction}.
 */
@NotThreadSafe
public final class DbTxnManager extends HiveTxnManagerImpl {

  static final private String CLASS_NAME = DbTxnManager.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  public static final String GLOBAL_LOCKS = "__GLOBAL_LOCKS";

  private volatile DbLockManager lockMgr = null;
  /**
   * The Metastore TXNS sequence is initialized to 1.
   * Thus is 1 is first transaction id.
   */
  private volatile long txnId = 0;

  /**
   * The local cache of table write IDs allocated/created by the current transaction
   */
  private Map<String, Long> tableWriteIds = new HashMap<>();
  private boolean shouldReallocateWriteIds = false;

  /**
   * assigns a unique monotonically increasing ID to each statement
   * which is part of an open transaction.  This is used by storage
   * layer (see {@link org.apache.hadoop.hive.ql.io.AcidUtils#deltaSubdir(long, long, int)})
   * to keep apart multiple writes of the same data within the same transaction
   * Also see {@link org.apache.hadoop.hive.ql.io.AcidOutputFormat.Options}.
   */
  private int stmtId = -1;

  /**
   * counts number of statements in the current transaction.
   */
  private int numStatements = 0;

  /**
   * if {@code true} it means current transaction is started via START TRANSACTION which means it cannot
   * include any Operations which cannot be rolled back (drop partition; write to  non-acid table).
   * If false, it's a single statement transaction which can include any statement.  This is not a
   * contradiction from the user point of view who doesn't know anything about the implicit txn
   * and cannot call rollback (the statement of course can fail in which case there is nothing to
   * rollback (assuming the statement is well implemented)).
   *
   * This is done so that all commands run in a transaction which simplifies implementation and
   * allows a simple implementation of multi-statement txns which don't require a lock manager
   * capable of deadlock detection.  (todo: not fully implemented; elaborate on how this LM works)
   *
   * Also, critically important, ensuring that everything runs in a transaction assigns an order
   * to all operations in the system - needed for replication/DR.
   *
   * We don't want to allow non-transactional statements in a user demarcated txn because the effect
   * of such statement is "visible" immediately on statement completion, but the user may
   * issue a rollback but the action of the statement can't be undone (and has possibly already been
   * seen by another txn).  For example,
   * start transaction
   * insert into transactional_table values(1);
   * insert into non_transactional_table select * from transactional_table;
   * rollback
   *
   * The user would be in for a surprise especially if they are not aware of transactional
   * properties of the tables involved.
   *
   * As a side note: what should the lock manager do with locks for non-transactional resources?
   * Should it it release them at the end of the stmt or txn?
   * Some interesting thoughts: http://mysqlmusings.blogspot.com/2009/02/mixing-engines-in-transactions.html.
   */
  private boolean isExplicitTransaction = false;

  /**
   * To ensure transactions don't nest.
   */
  private int startTransactionCount = 0;

  // QueryId for the query in current transaction
  private String queryId;

  // ExecutorService for sending heartbeat to metastore periodically.
  private static ScheduledExecutorService heartbeatExecutorService = null;
  private ScheduledFuture<?> heartbeatTask = null;
  private static final int SHUTDOWN_HOOK_PRIORITY = 0;
  //Contains database under replication name for hive replication transactions (dump and load operation)
  private String replPolicy;

  /**
   * We do this on every call to make sure TM uses same MS connection as is used by the caller (Driver,
   * SemanticAnalyzer, etc).  {@code Hive} instances are cached using ThreadLocal and
   * {@link IMetaStoreClient} is cached within {@code Hive} with additional logic.  Futhermore, this
   * ensures that multiple threads are not sharing the same Thrift client (which could happen
   * if we had cached {@link IMetaStoreClient} here.
   *
   * ThreadLocal gets cleaned up automatically when its thread goes away
   * https://docs.oracle.com/javase/7/docs/api/java/lang/ThreadLocal.html.  This is especially
   * important for threads created by {@link #heartbeatExecutorService} threads.
   *
   * Embedded {@link DbLockManager} follows the same logic.
   * @return IMetaStoreClient
   * @throws LockException on any errors
   */
  IMetaStoreClient getMS() throws LockException {
    try {
      return Hive.get(conf).getMSC();
    }
    catch(HiveException|MetaException e) {
      String msg = "Unable to reach Hive Metastore: " + e.getMessage();
      LOG.error(msg, e);
      throw new LockException(e);
    }
  }

  DbTxnManager() {
  }

  @Override
  void setHiveConf(HiveConf conf) {
    super.setHiveConf(conf);
    if (!conf.getBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY)) {
      throw new RuntimeException(ErrorMsg.DBTXNMGR_REQUIRES_CONCURRENCY.getMsg());
    }
  }

  @Override
  public List<Long> replOpenTxn(String replPolicy, List<Long> srcTxnIds, String user)  throws LockException {
    try {
      return getMS().replOpenTxn(replPolicy, srcTxnIds, user, TxnType.REPL_CREATED);
    } catch (TException e) {
      throw new LockException(e, ErrorMsg.METASTORE_COMMUNICATION_FAILED);
    }
  }

  @Override
  public long openTxn(Context ctx, String user) throws LockException {
    return openTxn(ctx, user, TxnType.DEFAULT, 0);
  }

  @Override
  public long openTxn(Context ctx, String user, TxnType txnType) throws LockException {
    return openTxn(ctx, user, txnType, 0);
  }

  @Override
  public void clearCaches() {
    LOG.info("Clearing writeId cache for {}", JavaUtils.txnIdToString(txnId));
    tableWriteIds.clear();
    shouldReallocateWriteIds = true;
  }

  @VisibleForTesting
  long openTxn(Context ctx, String user, TxnType txnType, long delay) throws LockException {
    /*Q: why don't we lock the snapshot here???  Instead of having client make an explicit call
    whenever it chooses
    A: If we want to rely on locks for transaction scheduling we must get the snapshot after lock
    acquisition.  Relying on locks is a pessimistic strategy which works better under high
    contention.*/
    init();
    getLockManager();
    if(isTxnOpen()) {
      throw new LockException("Transaction already opened. " + JavaUtils.txnIdToString(txnId));
    }
    try {
      replPolicy = ctx.getReplPolicy();
      if (replPolicy != null) {
        txnId = getMS().replOpenTxn(replPolicy, null, user, txnType).get(0);
      } else {
        txnId = getMS().openTxn(user, txnType);
      }
      stmtId = 0;
      numStatements = 0;
      tableWriteIds.clear();
      shouldReallocateWriteIds = false;
      isExplicitTransaction = false;
      startTransactionCount = 0;
      this.queryId = ctx.getConf().get(HiveConf.ConfVars.HIVE_QUERY_ID.varname);
      LOG.info("Opened " + JavaUtils.txnIdToString(txnId));
      ctx.setHeartbeater(startHeartbeat(delay));
      return txnId;
    } catch (TException e) {
      throw new LockException(e, ErrorMsg.METASTORE_COMMUNICATION_FAILED);
    }
  }

  /**
   * we don't expect multiple threads to call this method concurrently but {@link #lockMgr} will
   * be read by a different threads than one writing it, thus it's {@code volatile}
   */
  @Override
  public HiveLockManager getLockManager() {
    init();
    if (lockMgr == null) {
      lockMgr = new DbLockManager(conf, this);
    }
    return lockMgr;
  }

  @Override
  public void acquireLocks(QueryPlan plan, Context ctx, String username) throws LockException {
    try {
      acquireLocksWithHeartbeatDelay(plan, ctx, username, 0);
    }
    catch(LockException e) {
      if(e.getCause() instanceof TxnAbortedException) {
        resetTxnInfo();
      }
      throw e;
    }
  }

  /**
   * Watermark to include in error msgs and logs
   * @param queryPlan
   * @return
   */
  private static String getQueryIdWaterMark(QueryPlan queryPlan) {
    return "queryId=" + queryPlan.getQueryId();
  }

  private void markExplicitTransaction(QueryPlan queryPlan) throws LockException {
    isExplicitTransaction = true;
    if(++startTransactionCount > 1) {
      throw new LockException(null, ErrorMsg.OP_NOT_ALLOWED_IN_TXN, queryPlan.getOperationName(),
        JavaUtils.txnIdToString(getCurrentTxnId()), queryPlan.getQueryId());
    }

  }
  /**
   * Ensures that the current SQL statement is appropriate for the current state of the
   * Transaction Manager (e.g. can call commit unless you called start transaction)
   *
   * Note that support for multi-statement txns is a work-in-progress so it's only supported in
   * HiveConf#HIVE_IN_TEST/HiveConf#TEZ_HIVE_IN_TEST.
   * @param queryPlan
   * @throws LockException
   */
  private void verifyState(QueryPlan queryPlan) throws LockException {
    if(!isTxnOpen()) {
      throw new LockException("No transaction context for operation: " + queryPlan.getOperationName() +
        " for " + getQueryIdWaterMark(queryPlan));
    }
    if(queryPlan.getOperation() == null) {
      throw new IllegalStateException("Unknown HiveOperation(null) for " + getQueryIdWaterMark(queryPlan));
    }
    numStatements++;
    switch (queryPlan.getOperation()) {
      case START_TRANSACTION:
        markExplicitTransaction(queryPlan);
        break;
      case COMMIT:
      case ROLLBACK:
        if(!isTxnOpen()) {
          throw new LockException(null, ErrorMsg.OP_NOT_ALLOWED_WITHOUT_TXN, queryPlan.getOperationName());
        }
        if(!isExplicitTransaction) {
          throw new LockException(null, ErrorMsg.OP_NOT_ALLOWED_IN_IMPLICIT_TXN, queryPlan.getOperationName());
        }
        break;
      default:
        if(!queryPlan.getOperation().isAllowedInTransaction() && isExplicitTransaction) {
          if(allowOperationInATransaction(queryPlan)) {
            break;
          }
          //look at queryPlan.outputs(WriteEntity.t - that's the table)
          //for example, drop table in an explicit txn is not allowed
          //in some cases this requires looking at more than just the operation
          //for example HiveOperation.LOAD - OK if target is MM table but not OK if non-acid table
          throw new LockException(null, ErrorMsg.OP_NOT_ALLOWED_IN_TXN, queryPlan.getOperationName(),
            JavaUtils.txnIdToString(getCurrentTxnId()), queryPlan.getQueryId());
        }
    }
    /*
    Should we allow writing to non-transactional tables in an explicit transaction?  The user may
    issue ROLLBACK but these tables won't rollback.
    Can do this by checking ReadEntity/WriteEntity to determine whether it's reading/writing
    any non acid and raise an appropriate error
    * Driver.acidSinks and Driver.transactionalInQuery can be used if any acid is in the query*/
  }

  /**
   * This modifies the logic wrt what operations are allowed in a transaction.  Multi-statement
   * transaction support is incomplete but it makes some Acid tests cases much easier to write.
   */
  private boolean allowOperationInATransaction(QueryPlan queryPlan) {
    //Acid and MM tables support Load Data with transactional semantics.  This will allow Load Data
    //in a txn assuming we can determine the target is a suitable table type.
    if(queryPlan.getOperation() == HiveOperation.LOAD && queryPlan.getOutputs() != null && queryPlan.getOutputs().size() == 1) {
      WriteEntity writeEntity = queryPlan.getOutputs().iterator().next();
      if(AcidUtils.isTransactionalTable(writeEntity.getTable())) {
        switch (writeEntity.getWriteType()) {
          case INSERT:
            //allow operation in a txn
            return true;
          case INSERT_OVERWRITE:
            //see HIVE-18154
            return false;
          default:
            //not relevant for LOAD
            return false;
        }
      }
    }
    //todo: handle Insert Overwrite as well: HIVE-18154
    return false;
  }

  @Override
  public void addWriteIdsToMinHistory(QueryPlan plan, ValidTxnWriteIdList txnWriteIds) {
    if (plan.getInputs().isEmpty()) {
      return;
    }
    Map<String, Long> writeIds = plan.getInputs().stream()
      .filter(input -> !input.isDummy() && AcidUtils.isTransactionalTable(input.getTable()))
      .map(input -> input.getTable().getFullyQualifiedName())
      .distinct()
      .collect(Collectors.toMap(Function.identity(), table -> getMinOpenWriteId(txnWriteIds, table)));

    if (!writeIds.isEmpty()) {
      try {
        getMS().addWriteIdsToMinHistory(txnId, writeIds);
      } catch (TException | LockException e) {
        throw new RuntimeException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(), e);
      }
    }
  }

  private Long getMinOpenWriteId(ValidTxnWriteIdList txnWriteIds, String table) {
    ValidWriteIdList tableValidWriteIdList = txnWriteIds.getTableValidWriteIdList(table);
    Long minOpenWriteId = tableValidWriteIdList.getMinOpenWriteId();
    return minOpenWriteId != null ? minOpenWriteId : tableValidWriteIdList.getHighWatermark() + 1;
  }

  /**
   * Normally client should call {@link #acquireLocks(org.apache.hadoop.hive.ql.QueryPlan, org.apache.hadoop.hive.ql.Context, String)}
   * @param isBlocking if false, the method will return immediately; thus the locks may be in LockState.WAITING
   * @return null if no locks were needed
   */
  @VisibleForTesting
  LockState acquireLocks(QueryPlan plan, Context ctx, String username, boolean isBlocking) throws LockException {
    init();
    // Make sure we've built the lock manager
    getLockManager();
    verifyState(plan);
    queryId = plan.getQueryId();
    
    if (plan.getOperation() == HiveOperation.SET_AUTOCOMMIT) {
      /**This is here for documentation purposes.  This TM doesn't support this - only has one
       * mode of operation documented at {@link DbTxnManager#isExplicitTransaction}*/
      return null;
    }
    LOG.info("Setting lock request transaction to " + JavaUtils.txnIdToString(txnId) + " for queryId=" + queryId);

    // Make sure we need locks. It's possible there's nothing to lock in this operation.
    if (plan.getInputs().isEmpty() && plan.getOutputs().isEmpty()) {
      LOG.debug("No locks needed for queryId=" + queryId);
      return null;
    }
    List<LockComponent> lockComponents = AcidUtils.makeLockComponents(
        plan.getOutputs(), plan.getInputs(),
        ctx.getOperation(), conf);
    lockComponents.addAll(getGlobalLocks(ctx.getConf()));

    //It's possible there's nothing to lock even if we have w/r entities.
    if (lockComponents.isEmpty()) {
      LOG.debug("No locks needed for queryId=" + queryId);
      return null;
    }

    LockRequest lockRqst = new LockRequestBuilder(queryId)
      .setTransactionId(txnId) //link queryId to txnId
      .setUser(username)
      .setExclusiveCTAS(AcidUtils.isExclusiveCTAS(plan.getOutputs(), conf))
      .setZeroWaitReadEnabled(
          !conf.getBoolVar(HiveConf.ConfVars.TXN_OVERWRITE_X_LOCK)
            || !conf.getBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK))
      .addLockComponents(lockComponents)
      .build();

    List<HiveLock> locks = new ArrayList<>(1);
    LockState lockState = lockMgr.lock(lockRqst, queryId, isBlocking, locks);
    
    ctx.setHiveLocks(locks);
    return lockState;
  }

  private Collection<LockComponent> getGlobalLocks(Configuration conf) {
    String lockNames = conf.get(Constants.HIVE_QUERY_EXCLUSIVE_LOCK);
    if (StringUtils.isEmpty(lockNames)) {
      return Collections.emptyList();
    }
    List<LockComponent> globalLocks = new ArrayList<LockComponent>();
    for (String lockName : lockNames.split(",")) {
      lockName = lockName.trim();
      if (StringUtils.isEmpty(lockName)) {
        continue;
      }
      LockComponentBuilder compBuilder = new LockComponentBuilder();
      compBuilder.setExclusive();
      compBuilder.setOperationType(DataOperationType.NO_TXN);
      compBuilder.setDbName(GLOBAL_LOCKS);
      compBuilder.setTableName(lockName);

      globalLocks.add(compBuilder.build());
      LOG.debug("Adding global lock: " + lockName);
    }
    return globalLocks;
  }
  
  /**
   * @param delay time to delay for first heartbeat
   */
  @VisibleForTesting
  void acquireLocksWithHeartbeatDelay(QueryPlan plan, Context ctx, String username, long delay) throws LockException {
    LockState ls = acquireLocks(plan, ctx, username, true);
    if (ls != null && !isTxnOpen()) { // If there's no lock, we don't need to do heartbeat
      // Start heartbeat for read-only queries which don't open transactions but requires locks.
      // For those that require transactions, the heartbeat has already been started in openTxn.
      ctx.setHeartbeater(startHeartbeat(delay));
    }
  }

  @Override
  public void releaseLocks(List<HiveLock> hiveLocks) {
    if (lockMgr != null) {
      stopHeartbeat();
      lockMgr.releaseLocks(hiveLocks);
    }
  }

  private void clearLocksAndHB() {
    lockMgr.clearLocalLockRecords();
    stopHeartbeat();
  }

  private void resetTxnInfo() {
    txnId = 0;
    stmtId = -1;
    numStatements = 0;
    tableWriteIds.clear();
    shouldReallocateWriteIds = false;
    queryId = null;
    replPolicy = null;
  }

  @Override
  public void replCommitTxn(CommitTxnRequest rqst) throws LockException {
    try {
      if (rqst.isSetReplLastIdInfo()) {
        if (!isTxnOpen()) {
          throw new RuntimeException("Attempt to commit before opening a transaction");
        }
        // For transaction started internally by repl load command, heartbeat needs to be stopped.
        clearLocksAndHB();
      }
      getMS().commitTxn(rqst);
    } catch (NoSuchTxnException e) {
      LOG.error("Metastore could not find " + JavaUtils.txnIdToString(rqst.getTxnid()));
      throw new LockException(e, ErrorMsg.TXN_NO_SUCH_TRANSACTION, JavaUtils.txnIdToString(rqst.getTxnid()));
    } catch (TxnAbortedException e) {
      LockException le = new LockException(e, ErrorMsg.TXN_ABORTED,
              JavaUtils.txnIdToString(rqst.getTxnid()), e.getMessage());
      LOG.error(le.getMessage());
      throw le;
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(), e);
    } finally {
      if (rqst.isSetReplLastIdInfo()) {
        // For transaction started internally by repl load command, needs to clear the txn info.
        resetTxnInfo();
      }
    }
  }

  @Override
  public void commitTxn() throws LockException {
    if (!isTxnOpen()) {
      throw new RuntimeException("Attempt to commit before opening a transaction");
    }
    try {
      // do all new clear in clearLocksAndHB method to make sure that same code is there for replCommitTxn flow.
      clearLocksAndHB();
      LOG.debug("Committing txn " + JavaUtils.txnIdToString(txnId));
      CommitTxnRequest commitTxnRequest = new CommitTxnRequest(txnId);
      commitTxnRequest.setExclWriteEnabled(conf.getBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK));
      if (replPolicy != null) {
        commitTxnRequest.setReplPolicy(replPolicy);
        commitTxnRequest.setTxn_type(TxnType.DEFAULT);
      }
      getMS().commitTxn(commitTxnRequest);
    } catch (NoSuchTxnException e) {
      LOG.error("Metastore could not find " + JavaUtils.txnIdToString(txnId));
      throw new LockException(e, ErrorMsg.TXN_NO_SUCH_TRANSACTION, JavaUtils.txnIdToString(txnId));
    } catch (TxnAbortedException e) {
      LockException le = new LockException(e, ErrorMsg.TXN_ABORTED, JavaUtils.txnIdToString(txnId), e.getMessage());
      LOG.error(le.getMessage());
      throw le;
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(),
          e);
    } finally {
      // do all new reset in resetTxnInfo method to make sure that same code is there for replCommitTxn flow.
      resetTxnInfo();
    }
  }
  @Override
  public void replRollbackTxn(String replPolicy, long srcTxnId) throws LockException {
    try {
      getMS().replRollbackTxn(srcTxnId, replPolicy, TxnType.REPL_CREATED);
    } catch (NoSuchTxnException e) {
      LOG.error("Metastore could not find " + JavaUtils.txnIdToString(srcTxnId));
      throw new LockException(e, ErrorMsg.TXN_NO_SUCH_TRANSACTION, JavaUtils.txnIdToString(srcTxnId));
    } catch (TxnAbortedException e) {
      LockException le = new LockException(e, ErrorMsg.TXN_ABORTED, JavaUtils.txnIdToString(srcTxnId), e.getMessage());
      LOG.error(le.getMessage());
      throw le;
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(), e);
    }
  }

  @Override
  public void rollbackTxn() throws LockException {
    if (!isTxnOpen()) {
      throw new RuntimeException("Attempt to rollback before opening a transaction");
    }
    try {
      clearLocksAndHB();
      LOG.debug("Rolling back " + JavaUtils.txnIdToString(txnId));
      
      if (replPolicy != null) {
        getMS().replRollbackTxn(txnId, replPolicy, TxnType.DEFAULT);
      } else {
        AbortTxnRequest abortTxnRequest = new AbortTxnRequest(txnId);
        abortTxnRequest.setErrorCode(TxnErrorMsg.ABORT_ROLLBACK.getErrorCode());
        getMS().rollbackTxn(abortTxnRequest);
      }
    } catch (NoSuchTxnException e) {
      LOG.error("Metastore could not find " + JavaUtils.txnIdToString(txnId));
      throw new LockException(e, ErrorMsg.TXN_NO_SUCH_TRANSACTION, JavaUtils.txnIdToString(txnId));
    
    } catch(TxnAbortedException e) {
      throw new LockException(e, ErrorMsg.TXN_ABORTED, JavaUtils.txnIdToString(txnId));
    
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(), e);
    } finally {
      resetTxnInfo();
    }
  }

  @Override
  public void replTableWriteIdState(String validWriteIdList, String dbName, String tableName, List<String> partNames)
          throws LockException {
    try {
      getMS().replTableWriteIdState(validWriteIdList, dbName, tableName, partNames);
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(), e);
    }
  }

  @Override
  public void heartbeat() throws LockException {
    List<HiveLock> locks;
    if(isTxnOpen()) {
      // Create one dummy lock so we can go through the loop below, though we only
      //really need txnId
      locks = Collections.singletonList(new DbLockManager.DbHiveLock(0L));
    }
    else {
      locks = lockMgr.getLocks(false, false);
    }
    if(LOG.isInfoEnabled()) {
      StringBuilder sb = new StringBuilder("Sending heartbeat for ")
        .append(JavaUtils.txnIdToString(txnId)).append(" and");
      for(HiveLock lock : locks) {
        sb.append(" ").append(lock.toString());
      }
      LOG.info(sb.toString());
    }
    if(!isTxnOpen() && locks.isEmpty()) {
      // No locks, no txn, we outta here.
      LOG.debug("No need to send heartbeat as there is no transaction and no locks.");
      return;
    }
    for (HiveLock lock : locks) {
      long lockId = ((DbLockManager.DbHiveLock)lock).lockId;
      try {
        /**
         * This relies on the ThreadLocal caching, which implies that the same {@link IMetaStoreClient},
         * in particular the Thrift connection it uses is never shared between threads
         */
        getMS().heartbeat(txnId, lockId);
      } catch (NoSuchLockException e) {
        LOG.error("Unable to find lock " + JavaUtils.lockIdToString(lockId));
        throw new LockException(e, ErrorMsg.LOCK_NO_SUCH_LOCK, JavaUtils.lockIdToString(lockId));
      } catch (NoSuchTxnException e) {
        LOG.error("Unable to find transaction " + JavaUtils.txnIdToString(txnId));
        throw new LockException(e, ErrorMsg.TXN_NO_SUCH_TRANSACTION, JavaUtils.txnIdToString(txnId));
      } catch (TxnAbortedException e) {
        LockException le = new LockException(e, ErrorMsg.TXN_ABORTED, JavaUtils.txnIdToString(txnId), e.getMessage());
        LOG.error(le.getMessage());
        throw le;
      } catch (TException e) {
        throw new LockException(
            ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg() + "(" + JavaUtils.txnIdToString(txnId)
              + "," + lock.toString() + ")", e);
      }
    }
  }

  /**
   * Start the heartbeater threadpool and return the task.
   * @param initialDelay time to delay before first execution, in milliseconds
   * @return heartbeater
   */
  private Heartbeater startHeartbeat(long initialDelay) throws LockException {
    long heartbeatInterval = getHeartbeatInterval(conf);
    assert heartbeatInterval > 0;
    UserGroupInformation currentUser;
    try {
      currentUser = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new LockException("error while getting current user,", e);
    }

    Heartbeater heartbeater = new Heartbeater(this, conf, queryId, currentUser);
    heartbeatTask = startHeartbeat(initialDelay, heartbeatInterval, heartbeater);
    LOG.debug("Started heartbeat with delay/interval = " + initialDelay + "/" + heartbeatInterval +
      " " + TimeUnit.MILLISECONDS + " for query: " + queryId);
    return heartbeater;
  }

  private ScheduledFuture<?> startHeartbeat(long initialDelay, long heartbeatInterval, Runnable heartbeater) {
    // For negative testing purpose..
    if(conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST) && conf.getBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_HEARTBEATER)) {
      initialDelay = 0;
    } else if (initialDelay == 0) {
      /*make initialDelay a random number in [0, 0.75*heartbeatInterval] so that if a lot
      of queries land on the server at the same time and all get blocked on lack of
      resources, that they all don't start heartbeating at the same time*/
      initialDelay = (long)Math.floor(heartbeatInterval * 0.75 * Math.random());
    }
    ScheduledFuture<?> task = heartbeatExecutorService.scheduleAtFixedRate(
        heartbeater, initialDelay, heartbeatInterval, TimeUnit.MILLISECONDS);
    return task;
  }

  // To prevent NullPointerException due to race condition, marking it as synchronized.
  private synchronized void stopHeartbeat() {
    if (heartbeatTask != null) {
      heartbeatTask.cancel(true);
      
      long startTime = System.currentTimeMillis();
      long sleepInterval = 100;
      
      while (!heartbeatTask.isCancelled() && !heartbeatTask.isDone()) {
        // We will wait for 30 seconds for the task to be cancelled.
        // If it's still not cancelled (unlikely), we will just move on.
        long now = System.currentTimeMillis();
        if (now - startTime > 30000) {
          LOG.error("Heartbeat task cannot be cancelled for unknown reason. QueryId: " + queryId);
          break;
        }
        try {
          Thread.sleep(sleepInterval);
        } catch (InterruptedException e) {
        }
        sleepInterval *= 2;
      }
      if (heartbeatTask.isCancelled() || heartbeatTask.isDone()) {
        LOG.info("Stopped heartbeat for query: " + queryId);
      }
      heartbeatTask = null;
      queryId = null;
    }
  }

  @Override
  public GetOpenTxnsResponse getOpenTxns() throws LockException {
    try {
      return getMS().getOpenTxns();
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(), e);
    }
  }

  @Override
  public ValidTxnList getValidTxns() throws LockException {
    assert isTxnOpen();
    init();
    try {
      return getMS().getValidTxns(txnId);
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(), e);
    }
  }

  @Override
  public ValidTxnList getValidTxns(List<TxnType> excludeTxnTypes) throws LockException {
    assert isTxnOpen();
    init();
    try {
      return getMS().getValidTxns(txnId, excludeTxnTypes);
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(), e);
    }
  }

  @Override
  public ValidTxnWriteIdList getValidWriteIds(List<String> tableList,
                                              String validTxnList) throws LockException {
    assert isTxnOpen();
    if (!StringUtils.isEmpty(validTxnList)) {
      try {
        return TxnCommonUtils.createValidTxnWriteIdList(txnId, getMS().getValidWriteIds(tableList, validTxnList));
      } catch (TException e) {
        throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(), e);
      }
    }
    return null;
  }

  @Override
  public String getTxnManagerName() {
    return CLASS_NAME;
  }

  @Override
  public boolean supportsExplicitLock() {
    return false;
  }
  @Override
  public int lockTable(Hive db, LockTableDesc lockTbl) throws HiveException {
    super.lockTable(db, lockTbl);
    throw new UnsupportedOperationException();
  }
  @Override
  public int unlockTable(Hive hiveDB, UnlockTableDesc unlockTbl) throws HiveException {
    super.unlockTable(hiveDB, unlockTbl);
    throw new UnsupportedOperationException();
  }
  @Override
  public int lockDatabase(Hive hiveDB, LockDatabaseDesc lockDb) throws HiveException {
    super.lockDatabase(hiveDB, lockDb);
    throw new UnsupportedOperationException();
  }
  @Override
  public int unlockDatabase(Hive hiveDB, UnlockDatabaseDesc unlockDb) throws HiveException {
    super.unlockDatabase(hiveDB, unlockDb);
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean useNewShowLocksFormat() {
    return true;
  }

  @Override
  public boolean supportsAcid() {
    return true;
  }
  /**
   * In an explicit txn start_transaction is the 1st statement and we record the snapshot at the
   * start of the txn for Snapshot Isolation.  For Read Committed (not supported yet) we'd record
   * it before executing each statement (but after lock acquisition if using lock based concurrency
   * control).
   * For implicit txn, the stmt that triggered/started the txn is the first statement
   */
  @Override
  public boolean recordSnapshot(QueryPlan queryPlan) {
    assert isTxnOpen();
    assert numStatements > 0 : "was acquireLocks() called already?";
    if(queryPlan.getOperation() == HiveOperation.START_TRANSACTION) {
      //here if start of explicit txn
      assert isExplicitTransaction;
      assert numStatements == 1;
      return true;
    }
    else if(!isExplicitTransaction) {
      assert numStatements == 1 : "numStatements=" + numStatements + " in implicit txn";
      if (queryPlan.hasAcidResourcesInQuery()) {
        //1st and only stmt in implicit txn and uses acid resource
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isImplicitTransactionOpen() {
    return isImplicitTransactionOpen(null);
  }

  @Override
  public boolean isImplicitTransactionOpen(Context ctx) {
    if(!isTxnOpen()) {
      //some commands like "show databases" don't start implicit transactions
      return false;
    }
    if(!isExplicitTransaction) {
      if (ctx == null || !ctx.isExplainSkipExecution()) {
        assert numStatements == 1 : "numStatements=" + numStatements;
      }
      return true;
    }
    return false;
  }
  
  @Override
  protected void destruct() {
    try {
      stopHeartbeat();
      if (isTxnOpen()) {
        rollbackTxn();
      }
      if (lockMgr != null) {
        lockMgr.close();
      }
    } catch (Exception e) {
      LOG.error("Caught exception " + e.getClass().getName() + " with message <" + e.getMessage()
      + ">, swallowing as there is nothing we can do with it.");
      // Not much we can do about it here.
    }
  }

  private void init() {
    if (conf == null) {
      throw new RuntimeException("Must call setHiveConf before any other methods.");
    }
    initHeartbeatExecutorService(conf.getIntVar(HiveConf.ConfVars.HIVE_TXN_HEARTBEAT_THREADPOOL_SIZE));
  }

  private synchronized static void initHeartbeatExecutorService(int corePoolSize) {
      if(heartbeatExecutorService != null) {
        return;
      }
      // The following code will be executed only once when the service is not initialized
      heartbeatExecutorService =
          Executors.newScheduledThreadPool(
              corePoolSize,
              new ThreadFactory() {
                private final AtomicInteger threadCounter = new AtomicInteger();

                @Override
                public Thread newThread(Runnable r) {
                  return new HeartbeaterThread(r, "Heartbeater-" + threadCounter.getAndIncrement());
                }
              });
      ((ScheduledThreadPoolExecutor) heartbeatExecutorService).setRemoveOnCancelPolicy(true);
      ShutdownHookManager.addShutdownHook(DbTxnManager::shutdownHeartbeatExecutorService, SHUTDOWN_HOOK_PRIORITY);
  }

  private synchronized static void shutdownHeartbeatExecutorService() {
    if (heartbeatExecutorService != null && !heartbeatExecutorService.isShutdown()) {
      LOG.info("Shutting down Heartbeater thread pool.");
      heartbeatExecutorService.shutdown();
    }
  }

  public static class HeartbeaterThread extends Thread {
    HeartbeaterThread(Runnable target, String name) {
      super(target, name);
      setDaemon(true);
    }
  }

  @Override
  public boolean isTxnOpen() {
    return txnId > 0;
  }
  @Override
  public long getCurrentTxnId() {
    return txnId;
  }
  @Override
  public int getStmtIdAndIncrement() {
    assert isTxnOpen();
    return stmtId++;
  }
  @Override
  public int getCurrentStmtId() {
    assert isTxnOpen();
    return stmtId;
  }

  @Override
  public long getTableWriteId(String dbName, String tableName) throws LockException {
    assert isTxnOpen();
    return getTableWriteId(dbName, tableName, true,  shouldReallocateWriteIds);
  }

  @Override
  public long getAllocatedTableWriteId(String dbName, String tableName) throws LockException {
    assert isTxnOpen();
    // Calls getTableWriteId() with allocateIfNotYet being false
    // to return 0 if the dbName:tableName's writeId is yet allocated.
    // This happens when the current context is before
    // Driver.acquireLocks() is called.
    return getTableWriteId(dbName, tableName, false, false);
  }

  @Override
  public void setTableWriteId(String dbName, String tableName, long writeId) throws LockException {
    String fullTableName = AcidUtils.getFullTableName(dbName, tableName);
    if (writeId > 0) {
      tableWriteIds.put(fullTableName, writeId);
    } else {
      getTableWriteId(dbName, tableName);
    }
  }

  private long getTableWriteId(String dbName, String tableName, boolean allocateIfNotYet,
		  boolean shouldReallocate) throws LockException {
    String fullTableName = AcidUtils.getFullTableName(dbName, tableName);
    if (tableWriteIds.containsKey(fullTableName)) {
      return tableWriteIds.get(fullTableName);
    } else if (!allocateIfNotYet) {
      return 0;
    }
    try {
      long writeId = getMS().allocateTableWriteId(txnId, dbName, tableName, shouldReallocate);
      LOG.info("Allocated write ID {} for {}.{} and {} (shouldReallocate: {}) ", writeId, dbName,
          tableName, JavaUtils.txnIdToString(txnId), shouldReallocate);
      tableWriteIds.put(fullTableName, writeId);
      return writeId;
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(), e);
    }
  }

  @Override
  public LockResponse acquireMaterializationRebuildLock(String dbName, String tableName, long txnId) throws LockException {
    // Acquire lock
    LockResponse lockResponse;
    try {
      lockResponse = getMS().lockMaterializationRebuild(dbName, tableName, txnId);
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(), e);
    }
    if (lockResponse.getState() == LockState.ACQUIRED) {
      // If lock response is ACQUIRED, we can create the heartbeater
      long initialDelay = 0L;
      long heartbeatInterval = getHeartbeatInterval(conf);
      assert heartbeatInterval > 0;
      MaterializationRebuildLockHeartbeater heartbeater = new MaterializationRebuildLockHeartbeater(
          this, dbName, tableName, queryId, txnId);
      ScheduledFuture<?> task = startHeartbeat(initialDelay, heartbeatInterval, heartbeater);
      heartbeater.task.set(task);
      LOG.debug("Started heartbeat for materialization rebuild lock for {} with delay/interval = {}/{} {} for query: {}",
          AcidUtils.getFullTableName(dbName, tableName), initialDelay, heartbeatInterval, TimeUnit.MILLISECONDS, queryId);
    }
    return lockResponse;
  }

  @Override
  public long getLatestTxnIdInConflict() throws LockException {
    try {
      return getMS().getLatestTxnIdInConflict(txnId);
    } catch (TException e) {
      throw new LockException(e);
    }
  }

  private boolean heartbeatMaterializationRebuildLock(String dbName, String tableName, long txnId) throws LockException {
    try {
      return getMS().heartbeatLockMaterializationRebuild(dbName, tableName, txnId);
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(), e);
    }
  }

  @Override
  public void replAllocateTableWriteIdsBatch(String dbName, String tableName, String replPolicy,
                                             List<TxnToWriteId> srcTxnToWriteIdList) throws LockException {
    try {
      getMS().replAllocateTableWriteIdsBatch(dbName, tableName, replPolicy, srcTxnToWriteIdList);
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(), e);
    }
  }

  public static long getHeartbeatInterval(Configuration conf) throws LockException {
    // Retrieve HIVE_TXN_TIMEOUT in MILLISECONDS (it's defined as SECONDS),
    // then divide it by 2 to give us a safety factor.
    long interval =
        HiveConf.getTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_TIMEOUT, TimeUnit.MILLISECONDS) / 2;
    if (interval == 0) {
      throw new LockException(HiveConf.ConfVars.HIVE_TXN_TIMEOUT.toString() + " not set," +
          " heartbeats won't be sent");
    }
    return interval;
  }

  /**
   * Heartbeater thread
   */
  public static class Heartbeater implements Runnable {
    private HiveTxnManager txnMgr;
    private HiveConf conf;
    private UserGroupInformation currentUser;
    LockException lockException;
    private final String queryId;

    public LockException getLockException() {
      return lockException;
    }
    /**
     *
     * @param txnMgr transaction manager for this operation
     * @param currentUser
     */
    Heartbeater(HiveTxnManager txnMgr, HiveConf conf, String queryId,
        UserGroupInformation currentUser) {
      this.txnMgr = txnMgr;
      this.conf = conf;
      this.currentUser = currentUser;
      lockException = null;
      this.queryId = queryId;
    }

    /**
     * Send a heartbeat to the metastore for locks and transactions.
     */
    @Override
    public void run() {
      try {
        // For negative testing purpose..
        if(conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST) && conf.getBoolVar(HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_HEARTBEATER)) {
          throw new LockException(HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_HEARTBEATER.name() + "=true");
        }
        LOG.debug("Heartbeating...for currentUser: " + currentUser);
        currentUser.doAs((PrivilegedExceptionAction<Object>) () -> {
          txnMgr.heartbeat();
          return null;
        });
      } catch (LockException e) {
        LOG.error("Failed trying to heartbeat queryId=" + queryId + ", currentUser: "
            + currentUser + ": " + e.getMessage());
        lockException = e;
      } catch (Throwable t) {
        String errorMsg = "Failed trying to heartbeat queryId=" + queryId + ", currentUser: "
            + currentUser + ": " + t.getMessage();
        LOG.error(errorMsg, t);
        lockException = new LockException(errorMsg, t);
      }
    }
  }

  /**
   * MaterializationRebuildLockHeartbeater is a runnable that will be run in a
   * ScheduledExecutorService in given intervals. Once the heartbeat cannot
   * refresh the lock anymore, it will interrupt itself.
   */
  private static class MaterializationRebuildLockHeartbeater implements Runnable {

    private final DbTxnManager txnMgr;
    private final String dbName;
    private final String tableName;
    private final String queryId;
    private final long txnId;
    private final AtomicReference<ScheduledFuture<?>> task;

    MaterializationRebuildLockHeartbeater(DbTxnManager txnMgr, String dbName, String tableName,
        String queryId, long txnId) {
      this.txnMgr = txnMgr;
      this.queryId = queryId;
      this.dbName = dbName;
      this.tableName = tableName;
      this.txnId = txnId;
      this.task = new AtomicReference<>();
    }

    /**
     * Send a heartbeat to the metastore for locks and transactions.
     */
    @Override
    public void run() {
      LOG.trace("Heartbeating materialization rebuild lock for {} for query: {}",
          AcidUtils.getFullTableName(dbName, tableName), queryId);
      boolean refreshed;
      try {
        refreshed = txnMgr.heartbeatMaterializationRebuildLock(dbName, tableName, txnId);
      } catch (LockException e) {
        LOG.error("Failed trying to acquire lock", e);
        throw new RuntimeException(e);
      }
      if (!refreshed) {
        // We could not heartbeat the lock, i.e., the operation has finished,
        // hence we interrupt this work
        ScheduledFuture<?> t = task.get();
        if (t != null) {
          t.cancel(false);
          LOG.debug("Stopped heartbeat for materialization rebuild lock for {} for query: {}",
              AcidUtils.getFullTableName(dbName, tableName), queryId);
        }
      }
    }
  }

  @Override
  public String getQueryid() {
    return queryId;
  }
}
