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
package org.apache.hadoop.hive.ql.lockmgr;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hive.common.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An implementation of HiveTxnManager that stores the transactions in the
 * metastore database.
 */
public class DbTxnManager extends HiveTxnManagerImpl {

  static final private String CLASS_NAME = DbTxnManager.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  private DbLockManager lockMgr = null;
  private SynchronizedMetaStoreClient client = null;
  /**
   * The Metastore NEXT_TXN_ID.NTXN_NEXT is initialized to 1; it contains the next available
   * transaction id.  Thus is 1 is first transaction id.
   */
  private long txnId = 0;
  /**
   * assigns a unique monotonically increasing ID to each statement
   * which is part of an open transaction.  This is used by storage
   * layer (see {@link org.apache.hadoop.hive.ql.io.AcidUtils#deltaSubdir(long, long, int)})
   * to keep apart multiple writes of the same data within the same transaction
   * Also see {@link org.apache.hadoop.hive.ql.io.AcidOutputFormat.Options}
   */
  private int statementId = -1;

  // QueryId for the query in current transaction
  private String queryId;

  // ExecutorService for sending heartbeat to metastore periodically.
  private static ScheduledExecutorService heartbeatExecutorService = null;
  private ScheduledFuture<?> heartbeatTask = null;
  private Runnable shutdownRunner = null;
  private static final int SHUTDOWN_HOOK_PRIORITY = 0;

  // SynchronizedMetaStoreClient object per heartbeater thread.
  private static ThreadLocal<SynchronizedMetaStoreClient> threadLocalMSClient =
      new ThreadLocal<SynchronizedMetaStoreClient>() {

        @Override
        protected SynchronizedMetaStoreClient initialValue() {
          return null;
        }

        @Override
        public synchronized void remove() {
          SynchronizedMetaStoreClient client = this.get();
          if (client != null) {
            client.close();
          }
          super.remove();
        }
      };

  private static AtomicInteger heartbeaterMSClientCount = new AtomicInteger(0);
  private static int heartbeaterThreadPoolSize = 0;

  private static SynchronizedMetaStoreClient getThreadLocalMSClient() {
    return threadLocalMSClient.get();
  }

  DbTxnManager() {
    shutdownRunner = new Runnable() {
      @Override
      public void run() {
        if (heartbeatExecutorService != null
            && !heartbeatExecutorService.isShutdown()
            && !heartbeatExecutorService.isTerminated()) {
          LOG.info("Shutting down Heartbeater thread pool.");
          heartbeatExecutorService.shutdown();
        }
      }
    };
    ShutdownHookManager.addShutdownHook(shutdownRunner, SHUTDOWN_HOOK_PRIORITY);
  }

  @Override
  void setHiveConf(HiveConf conf) {
    super.setHiveConf(conf);
    if (!conf.getBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY)) {
      throw new RuntimeException(ErrorMsg.DBTXNMGR_REQUIRES_CONCURRENCY.getMsg());
    }
  }

  @Override
  public long openTxn(String user) throws LockException {
    //todo: why don't we lock the snapshot here???  Instead of having client make an explicit call
    //whenever it chooses
    init();
    if(isTxnOpen()) {
      throw new LockException("Transaction already opened. " + JavaUtils.txnIdToString(txnId));
    }
    try {
      txnId = client.openTxn(user);
      statementId = 0;
      LOG.debug("Opened " + JavaUtils.txnIdToString(txnId));
      return txnId;
    } catch (TException e) {
      throw new LockException(e, ErrorMsg.METASTORE_COMMUNICATION_FAILED);
    }
  }

  @Override
  public HiveLockManager getLockManager() throws LockException {
    init();
    if (lockMgr == null) {
      lockMgr = new DbLockManager(client, conf);
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
        txnId = 0;
        statementId = -1;
      }
      throw e;
    }
  }

  /**
   * This is for testing only.  Normally client should call {@link #acquireLocks(org.apache.hadoop.hive.ql.QueryPlan, org.apache.hadoop.hive.ql.Context, String)}
   * @param isBlocking if false, the method will return immediately; thus the locks may be in LockState.WAITING
   * @return null if no locks were needed
   */
  LockState acquireLocks(QueryPlan plan, Context ctx, String username, boolean isBlocking) throws LockException {
    init();
        // Make sure we've built the lock manager
    getLockManager();

    boolean atLeastOneLock = false;
    queryId = plan.getQueryId();

    LockRequestBuilder rqstBuilder = new LockRequestBuilder(queryId);
    //link queryId to txnId
    LOG.info("Setting lock request transaction to " + JavaUtils.txnIdToString(txnId) + " for queryId=" + queryId);
    rqstBuilder.setTransactionId(txnId)
        .setUser(username);

    // For each source to read, get a shared lock
    for (ReadEntity input : plan.getInputs()) {
      if (!input.needsLock() || input.isUpdateOrDelete() ||
          (input.getType() == Entity.Type.TABLE && input.getTable().isTemporary())) {
        // We don't want to acquire read locks during update or delete as we'll be acquiring write
        // locks instead. Also, there's no need to lock temp tables since they're session wide
        continue;
      }
      LockComponentBuilder compBuilder = new LockComponentBuilder();
      compBuilder.setShared();
      compBuilder.setOperationType(DataOperationType.SELECT);

      Table t = null;
      switch (input.getType()) {
        case DATABASE:
          compBuilder.setDbName(input.getDatabase().getName());
          break;

        case TABLE:
          t = input.getTable();
          compBuilder.setDbName(t.getDbName());
          compBuilder.setTableName(t.getTableName());
          break;

        case PARTITION:
        case DUMMYPARTITION:
          compBuilder.setPartitionName(input.getPartition().getName());
          t = input.getPartition().getTable();
          compBuilder.setDbName(t.getDbName());
          compBuilder.setTableName(t.getTableName());
          break;

        default:
          // This is a file or something we don't hold locks for.
          continue;
      }
      if(t != null && AcidUtils.isAcidTable(t)) {
        compBuilder.setIsAcid(true);
      }
      LockComponent comp = compBuilder.build();
      LOG.debug("Adding lock component to lock request " + comp.toString());
      rqstBuilder.addLockComponent(comp);
      atLeastOneLock = true;
    }

    // For each source to write to, get the appropriate lock type.  If it's
    // an OVERWRITE, we need to get an exclusive lock.  If it's an insert (no
    // overwrite) than we need a shared.  If it's update or delete then we
    // need a SEMI-SHARED.
    for (WriteEntity output : plan.getOutputs()) {
      if (output.getType() == Entity.Type.DFS_DIR || output.getType() == Entity.Type.LOCAL_DIR ||
          (output.getType() == Entity.Type.TABLE && output.getTable().isTemporary())) {
        // We don't lock files or directories. We also skip locking temp tables.
        continue;
      }
      LockComponentBuilder compBuilder = new LockComponentBuilder();
      Table t = null;
      LOG.debug("output is null " + (output == null));
      switch (output.getWriteType()) {
        case DDL_EXCLUSIVE:
        case INSERT_OVERWRITE:
          compBuilder.setExclusive();
          compBuilder.setOperationType(DataOperationType.NO_TXN);
          break;

        case INSERT:
          t = getTable(output);
          if(AcidUtils.isAcidTable(t)) {
            compBuilder.setShared();
            compBuilder.setIsAcid(true);
          }
          else {
            compBuilder.setExclusive();
            compBuilder.setIsAcid(false);
          }
          compBuilder.setOperationType(DataOperationType.INSERT);
          break;
        case DDL_SHARED:
          compBuilder.setShared();
          compBuilder.setOperationType(DataOperationType.NO_TXN);
          break;

        case UPDATE:
          compBuilder.setSemiShared();
          compBuilder.setOperationType(DataOperationType.UPDATE);
          t = getTable(output);
          break;
        case DELETE:
          compBuilder.setSemiShared();
          compBuilder.setOperationType(DataOperationType.DELETE);
          t = getTable(output);
          break;

        case DDL_NO_LOCK:
          continue; // No lock required here

        default:
          throw new RuntimeException("Unknown write type " +
              output.getWriteType().toString());

      }
      switch (output.getType()) {
        case DATABASE:
          compBuilder.setDbName(output.getDatabase().getName());
          break;

        case TABLE:
        case DUMMYPARTITION:   // in case of dynamic partitioning lock the table
          t = output.getTable();
          compBuilder.setDbName(t.getDbName());
          compBuilder.setTableName(t.getTableName());
          break;

        case PARTITION:
          compBuilder.setPartitionName(output.getPartition().getName());
          t = output.getPartition().getTable();
          compBuilder.setDbName(t.getDbName());
          compBuilder.setTableName(t.getTableName());
          break;

        default:
          // This is a file or something we don't hold locks for.
          continue;
      }
      if(t != null && AcidUtils.isAcidTable(t)) {
        compBuilder.setIsAcid(true);
      }
      LockComponent comp = compBuilder.build();
      LOG.debug("Adding lock component to lock request " + comp.toString());
      rqstBuilder.addLockComponent(comp);
      atLeastOneLock = true;
    }
    //plan
    // Make sure we need locks.  It's possible there's nothing to lock in
    // this operation.
    if (!atLeastOneLock) {
      LOG.debug("No locks needed for queryId" + queryId);
      return null;
    }

    List<HiveLock> locks = new ArrayList<HiveLock>(1);
    if(isTxnOpen()) {
      statementId++;
    }
    LockState lockState = lockMgr.lock(rqstBuilder.build(), queryId, isBlocking, locks);
    ctx.setHiveLocks(locks);
    return lockState;
  }
  private static Table getTable(WriteEntity we) {
    Table t = we.getTable();
    if(t == null) {
      throw new IllegalStateException("No table info for " + we);
    }
    return t;
  }
  /**
   * @param delay time to delay for first heartbeat
   */
  @VisibleForTesting
  void acquireLocksWithHeartbeatDelay(QueryPlan plan, Context ctx, String username, long delay) throws LockException {
    LockState ls = acquireLocks(plan, ctx, username, true);
    if (ls != null) { // If there's no lock, we don't need to do heartbeat
      ctx.setHeartbeater(startHeartbeat(delay));
    }
  }


  @Override
  public void releaseLocks(List<HiveLock> hiveLocks) throws LockException {
    if (lockMgr != null) {
      stopHeartbeat();
      lockMgr.releaseLocks(hiveLocks);
    }
  }

  @Override
  public void commitTxn() throws LockException {
    if (!isTxnOpen()) {
      throw new RuntimeException("Attempt to commit before opening a transaction");
    }
    try {
      lockMgr.clearLocalLockRecords();
      stopHeartbeat();
      LOG.debug("Committing txn " + JavaUtils.txnIdToString(txnId));
      client.commitTxn(txnId);
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
      txnId = 0;
      statementId = -1;
    }
  }

  @Override
  public void rollbackTxn() throws LockException {
    if (!isTxnOpen()) {
      throw new RuntimeException("Attempt to rollback before opening a transaction");
    }
    try {
      lockMgr.clearLocalLockRecords();
      stopHeartbeat();
      LOG.debug("Rolling back " + JavaUtils.txnIdToString(txnId));
      client.rollbackTxn(txnId);
    } catch (NoSuchTxnException e) {
      LOG.error("Metastore could not find " + JavaUtils.txnIdToString(txnId));
      throw new LockException(e, ErrorMsg.TXN_NO_SUCH_TRANSACTION, JavaUtils.txnIdToString(txnId));
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(),
          e);
    } finally {
      txnId = 0;
      statementId = -1;
    }
  }

  @Override
  public void heartbeat() throws LockException {
    List<HiveLock> locks;
    if(isTxnOpen()) {
      // Create one dummy lock so we can go through the loop below, though we only
      //really need txnId
      DbLockManager.DbHiveLock dummyLock = new DbLockManager.DbHiveLock(0L);
      locks = new ArrayList<>(1);
      locks.add(dummyLock);
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
      if (LOG.isDebugEnabled()) {
        LOG.debug("No need to send heartbeat as there is no transaction and no locks.");
      }
      return;
    }
    for (HiveLock lock : locks) {
      long lockId = ((DbLockManager.DbHiveLock)lock).lockId;
      try {
        // Get the threadlocal metastore client for the heartbeat calls.
        SynchronizedMetaStoreClient heartbeaterClient = getThreadLocalMSClient();
        if (heartbeaterClient == null) {
          Hive db;
          try {
            db = Hive.get(conf);
            // Create a new threadlocal synchronized metastore client for use in hearbeater threads.
            // This makes the concurrent use of heartbeat thread safe, and won't cause transaction
            // abort due to a long metastore client call blocking the heartbeat call.
            heartbeaterClient = new SynchronizedMetaStoreClient(db.getMSC());
            threadLocalMSClient.set(heartbeaterClient);
          } catch (HiveException e) {
            LOG.error("Unable to create new metastore client for heartbeating", e);
            throw new LockException(e);
          }
          // Increment the threadlocal metastore client count
          if (heartbeaterMSClientCount.incrementAndGet() >= heartbeaterThreadPoolSize) {
            LOG.warn("The number of hearbeater metastore clients - + "
                + heartbeaterMSClientCount.get() + ", has exceeded the max limit - "
                + heartbeaterThreadPoolSize);
          }
        }
        heartbeaterClient.heartbeat(txnId, lockId);
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
    Heartbeater heartbeater = new Heartbeater(this, conf, queryId);
    // For negative testing purpose..
    if(conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST) && conf.getBoolVar(HiveConf.ConfVars.HIVETESTMODEFAILHEARTBEATER)) {
      initialDelay = 0;
    } else if (initialDelay == 0) {
      initialDelay = heartbeatInterval;
    }
    heartbeatTask = heartbeatExecutorService.scheduleAtFixedRate(
        heartbeater, initialDelay, heartbeatInterval, TimeUnit.MILLISECONDS);
    LOG.info("Started heartbeat with delay/interval = " + initialDelay + "/" + heartbeatInterval +
        " " + TimeUnit.MILLISECONDS + " for query: " + queryId);
    return heartbeater;
  }

  private void stopHeartbeat() throws LockException {
    if (heartbeatTask != null) {
      heartbeatTask.cancel(true);
      long startTime = System.currentTimeMillis();
      long sleepInterval = 100;
      while (!heartbeatTask.isCancelled() && !heartbeatTask.isDone()) {
        // We will wait for 30 seconds for the task to be cancelled.
        // If it's still not cancelled (unlikely), we will just move on.
        long now = System.currentTimeMillis();
        if (now - startTime > 30000) {
          LOG.warn("Heartbeat task cannot be cancelled for unknown reason. QueryId: " + queryId);
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
  public ValidTxnList getValidTxns() throws LockException {
    init();
    try {
      return client.getValidTxns(txnId);
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(),
          e);
    }
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
  public boolean useNewShowLocksFormat() {
    return true;
  }

  @Override
  public boolean supportsAcid() {
    return true;
  }

  @Override
  protected void destruct() {
    try {
      stopHeartbeat();
      if (shutdownRunner != null) {
        ShutdownHookManager.removeShutdownHook(shutdownRunner);
      }
      if (isTxnOpen()) rollbackTxn();
      if (lockMgr != null) lockMgr.close();
    } catch (Exception e) {
      LOG.error("Caught exception " + e.getClass().getName() + " with message <" + e.getMessage()
      + ">, swallowing as there is nothing we can do with it.");
      // Not much we can do about it here.
    }
  }

  private void init() throws LockException {
    if (client == null) {
      if (conf == null) {
        throw new RuntimeException("Must call setHiveConf before any other " +
            "methods.");
      }
      try {
        Hive db = Hive.get(conf);
        client = new SynchronizedMetaStoreClient(db.getMSC());
        initHeartbeatExecutorService();
      } catch (MetaException e) {
        throw new LockException(ErrorMsg.METASTORE_COULD_NOT_INITIATE.getMsg(), e);
      } catch (HiveException e) {
        throw new LockException(ErrorMsg.METASTORE_COULD_NOT_INITIATE.getMsg(), e);
      }
    }
  }

  private synchronized void initHeartbeatExecutorService() {
    if (heartbeatExecutorService != null && !heartbeatExecutorService.isShutdown()
        && !heartbeatExecutorService.isTerminated()) {
      return;
    }
    heartbeaterThreadPoolSize =
        conf.getIntVar(HiveConf.ConfVars.HIVE_TXN_HEARTBEAT_THREADPOOL_SIZE);
    heartbeatExecutorService =
        Executors.newScheduledThreadPool(heartbeaterThreadPoolSize, new ThreadFactory() {
          private final AtomicInteger threadCounter = new AtomicInteger();

          @Override
          public Thread newThread(Runnable r) {
            return new HeartbeaterThread(r, "Heartbeater-" + threadCounter.getAndIncrement());
          }
        });
    ((ScheduledThreadPoolExecutor) heartbeatExecutorService).setRemoveOnCancelPolicy(true);
  }

  public static class HeartbeaterThread extends Thread {
    public HeartbeaterThread(Runnable target, String name) {
      super(target, name);
      setDaemon(true);
    }

    @Override
    /**
     * We're overriding finalize so that we can do an orderly cleanup of resources held by
     * the threadlocal metastore client.
     */
    protected void finalize() throws Throwable {
      threadLocalMSClient.remove();
      // Adjust the metastore client count
      DbTxnManager.heartbeaterMSClientCount.decrementAndGet();
      super.finalize();
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
  public int getStatementId() {
    return statementId;
  }

  private static long getHeartbeatInterval(Configuration conf) throws LockException {
    // Retrieve HIVE_TXN_TIMEOUT in MILLISECONDS (it's defined as SECONDS),
    // then divide it by 2 to give us a safety factor.
    long interval =
        HiveConf.getTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_TIMEOUT, TimeUnit.MILLISECONDS) / 2;
    if (interval == 0) {
      throw new LockException(HiveConf.ConfVars.HIVE_TXN_MANAGER.toString() + " not set," +
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
    LockException lockException;
    private final String queryId;

    public LockException getLockException() {
      return lockException;
    }
    /**
     *
     * @param txnMgr transaction manager for this operation
     */
    Heartbeater(HiveTxnManager txnMgr, HiveConf conf, String queryId) {
      this.txnMgr = txnMgr;
      this.conf = conf;
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
        if(conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST) && conf.getBoolVar(HiveConf.ConfVars.HIVETESTMODEFAILHEARTBEATER)) {
          throw new LockException(HiveConf.ConfVars.HIVETESTMODEFAILHEARTBEATER.name() + "=true");
        }
        LOG.debug("Heartbeating...");
        txnMgr.heartbeat();
      } catch (LockException e) {
        LOG.error("Failed trying to heartbeat queryId=" + queryId + ": " + e.getMessage());
        lockException = e;
      } catch (Throwable t) {
        LOG.error("Failed trying to heartbeat queryId=" + queryId + ": " + t.getMessage(), t);
        lockException =
            new LockException("Failed trying to heartbeat queryId=" + queryId + ": "
                + t.getMessage(), t);
      }
    }
  }

  /**
   * Synchronized MetaStoreClient wrapper
   */
  final class SynchronizedMetaStoreClient {
    private final IMetaStoreClient client;
    SynchronizedMetaStoreClient(IMetaStoreClient client) {
      this.client = client;
    }

    synchronized long openTxn(String user) throws TException {
      return client.openTxn(user);
    }

    synchronized void commitTxn(long txnid) throws TException {
      client.commitTxn(txnid);
    }

    synchronized void rollbackTxn(long txnid) throws TException {
      client.rollbackTxn(txnid);
    }

    synchronized void heartbeat(long txnid, long lockid) throws TException {
      client.heartbeat(txnid, lockid);
    }

    synchronized ValidTxnList getValidTxns(long currentTxn) throws TException {
      return client.getValidTxns(currentTxn);
    }

    synchronized LockResponse lock(LockRequest request) throws TException {
      return client.lock(request);
    }

    synchronized LockResponse checkLock(long lockid) throws TException {
      return client.checkLock(lockid);
    }

    synchronized void unlock(long lockid) throws TException {
      client.unlock(lockid);
    }

    synchronized ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
      return client.showLocks(showLocksRequest);
    }

    synchronized void close() {
      client.close();
    }
  }
}
