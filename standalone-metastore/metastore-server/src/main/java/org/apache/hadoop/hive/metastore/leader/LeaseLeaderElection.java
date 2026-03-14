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

package org.apache.hadoop.hive.metastore.leader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

/**
 * Hive Lock based leader election.
 * If wins, the current instance becomes the leader,
 * and a heartbeat daemon will be spawned to refresh the lock before timeout.
 * If loses, a non-leader watcher will also be spawned to check the
 * lock periodically to see if he can grab the lock to be the leader.
 * The change of Leadership can be received by registering the
 * listeners through {@link LeaderElection#addStateListener}.
 */
public class LeaseLeaderElection implements LeaderElection<TableName> {

  private static final Logger LOG = LoggerFactory.getLogger(LeaseLeaderElection.class);

  protected static final AtomicLong ID = new AtomicLong();

  // Result of election
  protected volatile boolean isLeader;

  private TxnStore store;

  // Initial sleep time for locking the table at retrying.
  private long nextSleep = 50;

  // A daemon used for renewing the lock before timeout,
  // this happens when the current instance wins the election.
  protected LeaseWatcher heartbeater;

  // For non-leader instances to check the lock periodically to
  // see if there is a chance to take over the leadership.
  // At any time, either heartbeater or nonLeaderWatcher is alive.
  private LeaseWatcher nonLeaderWatcher;

  // Current lock id
  private volatile long lockId = -1;

  private volatile boolean stopped = false;

  // Leadership change listeners
  protected final List<LeadershipStateListener> listeners = new ArrayList<>();

  protected String name;
  private final String userName;
  private final String hostName;
  private boolean enforceMutex;

  public LeaseLeaderElection() throws IOException {
    userName = SecurityUtils.getUser();
    hostName = InetAddress.getLocalHost().getHostName();
  }

  private synchronized void doWork(LockResponse resp, Configuration conf,
      TableName tableName) throws LeaderException {
    long start = System.currentTimeMillis();
    lockId = resp.getLockid();
    assert resp.getState() == LockState.ACQUIRED || resp.getState() == LockState.WAITING;
    shutdownWatcher();

    switch (resp.getState()) {
    case ACQUIRED:
      heartbeater = new Heartbeater(conf, tableName);
      heartbeater.startWatch();
      if (!isLeader) {
        isLeader = true;
        notifyListener();
      }
      break;
    case WAITING:
      nonLeaderWatcher = new NonLeaderWatcher(conf, tableName);
      nonLeaderWatcher.startWatch();
      if (isLeader) {
        isLeader = false;
        notifyListener();
      }
      break;
    default:
      throw new IllegalStateException("Unexpected lock state: " + resp.getState());
    }
    LOG.debug("Spent {}ms to notify the listeners, isLeader: {}", System.currentTimeMillis() - start, isLeader);
  }

  protected void notifyListener() {
    listeners.forEach(listener -> {
      try {
        if (isLeader) {
          listener.takeLeadership(this);
        } else {
          listener.lossLeadership(this);
        }
      } catch (Exception e) {
        LOG.error("Error notifying the listener: {}, leader: {}", listener, isLeader, e);
      }
    });
  }

  @Override
  public void tryBeLeader(Configuration conf, TableName table) throws LeaderException {
    requireNonNull(conf, "conf is null");
    requireNonNull(table, "table is null");
    this.enforceMutex = conf.getBoolean(HIVE_TXN_ENFORCE_AUX_MUTEX, true);
    if (store == null) {
      store = TxnUtils.getTxnStore(conf);
    }
    
    List<LockComponent> components = new ArrayList<>();
    components.add(
        new LockComponentBuilder()
            .setCatName(table.getCat())
            .setDbName(table.getDb())
            .setTableName(table.getTable())
            .setLock(LockType.EXCL_WRITE)
            .setOperationType(DataOperationType.NO_TXN)
            .build());

    boolean lockable = false;
    Exception recentException = null;
    long start = System.currentTimeMillis();
    LockRequest req = new LockRequest(components, userName, hostName);
    int numRetries = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.LOCK_NUMRETRIES);
    long maxSleep = MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.LOCK_SLEEP_BETWEEN_RETRIES, TimeUnit.MILLISECONDS);
    for (int i = 0; i < numRetries && !stopped; i++) {
      try {
        LockResponse res = store.lock(req);
        if (res.getState() == LockState.WAITING || res.getState() == LockState.ACQUIRED) {
          lockable = true;
          LOG.debug("{} Spent {}ms to take part in election, retries: {}", getName(), System.currentTimeMillis() - start, i);
          doWork(res, conf, table);
          break;
        }
      } catch (NoSuchTxnException | TxnAbortedException e) {
        throw new AssertionError("This should not happen, we didn't open txn", e);
      } catch (MetaException e) {
        recentException = e;
        LOG.warn("Error while locking the table: {}, num retries: {}, max retries: {}",
            table, i, numRetries, e);
      }
      backoff(maxSleep);
    }
    if (!lockable) {
      throw new LeaderException("Error locking the table: " + table + " in " + numRetries +
          " retries, time spent: " + (System.currentTimeMillis() - start) + " ms", recentException);
    }
  }

  // Sleep before we send checkLock again, but do it with a back off
  // so we don't sit and hammer the metastore in a tight loop
  private void backoff(long maxSleep) {
    nextSleep *= 2;
    if (nextSleep > maxSleep)
      nextSleep = maxSleep;
    try {
      Thread.sleep(nextSleep);
    } catch (InterruptedException ignored) {
    }
  }

  protected void shutdownWatcher() {
    if (heartbeater != null) {
      heartbeater.shutDown();
      heartbeater = null;
    }
    if (nonLeaderWatcher != null) {
      nonLeaderWatcher.shutDown();
      nonLeaderWatcher = null;
    }
  }

  @Override
  public void addStateListener(LeadershipStateListener listener) {
    requireNonNull(listener, "listener is null");
    listeners.add(listener);
  }

  @Override
  public boolean isLeader() {
    return isLeader;
  }

  protected abstract class LeaseWatcher extends Thread {

    protected Configuration conf;

    protected TableName tableName;

    private volatile boolean stopped = false;

    protected LeaseWatcher(Configuration conf, TableName tableName) {
      this.conf = conf;
      this.tableName = tableName;
      setDaemon(true);
      StringBuilder builder = new StringBuilder("Lease-Watcher-")
          .append(name != null ? name + "-" : "")
          .append(ID.incrementAndGet());
      setName(builder.toString());
    }

    public void startWatch() {
      LOG.info("Starting a watcher: {} for {}", getClass().getName(), name);
      start();
    }

    @Override
    public void run() {
      beforeRun();
      do {
        try {
          runInternal();
        } finally {
          if (!stopped) {
            afterRun();
          }
        }
      } while (!stopped);
    }

    public void shutDown() {
      stopped = true;
      // interrupt();
    }

    public void beforeRun() {
    }

    public void afterRun() {
    }

    public abstract void runInternal();

    public void reclaim() {
      try {
        tryBeLeader(conf, tableName);
      } catch (Exception e) {
        LOG.error("Error reclaiming the lease, will retry in next cycle", e);
      }
    }
  }

  private class NonLeaderWatcher extends LeaseWatcher {
    private final long sleep;
    private int count;
    private final CheckLockRequest request;

    NonLeaderWatcher(Configuration conf, TableName table) {
      super(conf, table);
      this.request = new CheckLockRequest(lockId);
      this.sleep = MetastoreConf.getTimeVar(conf,
          MetastoreConf.ConfVars.LOCK_SLEEP_BETWEEN_RETRIES, TimeUnit.MILLISECONDS);
    }

    @Override
    public void runInternal() {
      try {
        if (count ++ % 3 > 0) {
          // For WAITING AND NOT_ACQUIRED, re-check the lock at next cycle
          LockResponse res = store.checkLock(request);
          if (res.getState() == LockState.ACQUIRED) {
            // the current thread would be terminated by shutdownWatcher
            doWork(res, conf, tableName);
          } else if (res.getState() == LockState.ABORT) {
            reclaim();
          }
        } else {
          // In case the leader crashes, the lock it holds will become timeout eventually.
          // The AcidHouseKeeperService would not clean the corrupt lock until a new leader is elected,
          // however a leader candidate should hold that lock firstly in order to be the new leader,
          // a deadlock occurs in such case.
          // For all non-leader instances, they should try to clean timeout locks if possible to
          // avoid such problem.
          store.performTimeOuts();
        }
      } catch (NoSuchTxnException | TxnAbortedException e) {
        throw new AssertionError("This should not happen, we didn't open txn", e);
      } catch (NoSuchLockException e) {
        LOG.info("No such lock {} for NonLeaderWatcher, try to obtain the lock again...", lockId);
        reclaim();
      } catch (Exception e) {
        // Wait for next cycle.
        LOG.warn("CheckLock failed with exception: " + e.getMessage(), e);
      }
    }

    @Override
    public void afterRun() {
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        // ignore
      }
    }
  }

  private class Heartbeater extends LeaseWatcher {
    private final HeartbeatRequest req;
    private final long heartbeatInterval;

    Heartbeater(Configuration conf, TableName table) {
      super(conf, table);
      this.req = new HeartbeatRequest();
      this.req.setLockid(lockId);
      // Retrieve TXN_TIMEOUT in MILLISECONDS (it's defined as SECONDS),
      // then divide it by 2 to give us a safety factor.
      long interval = MetastoreConf.getTimeVar(conf,
          MetastoreConf.ConfVars.TXN_TIMEOUT, TimeUnit.MILLISECONDS) / 2;
      if (interval == 0) {
        throw new RuntimeException(MetastoreConf.ConfVars.TXN_TIMEOUT + " not set," +
            " heartbeats won't be sent");
      }
      this.heartbeatInterval = interval;
    }

    @Override
    public void beforeRun() {
      //Make initialDelay a random number in [0, 0.75*heartbeatInterval] so that
      //All leaders don't start heartbeating at the same time
      long initialDelay = (long) Math.floor(heartbeatInterval * 0.75 * Math.random());
      try {
        Thread.sleep(initialDelay);
      } catch (InterruptedException e) {
        // ignore this
      }
    }

    @Override
    public void runInternal() {
      try {
        store.heartbeat(req);
      } catch (NoSuchTxnException | TxnAbortedException e) {
        throw new AssertionError("This should not happen, we didn't open txn", e);
      } catch (NoSuchLockException e) {
        LOG.info("No such lock {} for Heartbeater, try to obtain the lock again...", lockId);
        reclaim();
      } catch (Exception e) {
        // Wait for next cycle.
        LOG.warn("Heartbeat failed with exception: {}", e.getMessage(), e);
      }
    }

    @Override
    public void afterRun() {
      try {
        Thread.sleep(heartbeatInterval);
      } catch (InterruptedException e) {
        //ignore
      }
    }
  }

  @Override
  public void close() {
    stopped = true;
    shutdownWatcher();
    if (isLeader) {
      isLeader = false;
      notifyListener();
    }
    if (lockId > 0) {
      try {
        UnlockRequest request = new UnlockRequest(lockId);
        store.unlock(request);
      } catch (NoSuchLockException | TxnOpenException e) {
        // ignore
      } catch (Exception e) {
        LOG.error("Error while unlocking: {}", lockId, e);
      }
    }
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean enforceMutex() {
    return this.enforceMutex;
  }
}
