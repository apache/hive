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
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This is a lock handler implementation for the materializations rebuild.
 * It is lightweight: it does not persist any information to metastore db.
 * Its states are as follows:
 * 1) request lock -> 2) ACQUIRED -> 4) COMMIT_READY -> 6) release lock
 *                                -> 5) EXPIRED      ->
 *                 -> 3) NOT_ACQUIRED
 * First, the rebuild operation will ACQUIRE the lock. If other rebuild
 * operation for the same operation is already running, we lock status
 * will be NOT_ACQUIRED.
 * Before committing the rebuild, the txn handler will signal the handler
 * that it is ready to commit the resource (move state to COMMIT_READY).
 * We make sure the lock is still available before moving to the new state.
 * A lock will not be able to expire when it is in COMMIT_READY state.
 * The unlock method is always call by the txn handler, no matter whether
 * the transaction succeeds or not, e.g., due to an Exception.
 * From ACQUIRED, locks can be also moved to EXPIRED state when they
 * expire. From EXPIRED, they can only be released.
 */
public class MaterializationsRebuildLockHandler {

  /* Singleton */
  private static final MaterializationsRebuildLockHandler SINGLETON = new MaterializationsRebuildLockHandler();

  private final ConcurrentMap<String, ResourceLock> locks = new ConcurrentHashMap<>();

  private MaterializationsRebuildLockHandler() {
  }

  /**
   * Get instance of MaterializationsRebuildLockHandler.
   *
   * @return the singleton
   */
  public static MaterializationsRebuildLockHandler get() {
    return SINGLETON;
  }

  /**
   * Lock materialized view (first step for rebuild). Response contains a lock id
   * that corresponds to the input transaction id, and whether the lock was
   * ACQUIRED or NOT_ACQUIRED.
   * @param dbName the db name of the materialization
   * @param tableName the table name of the materialization
   * @param txnId the transaction id for the rebuild
   * @return the response to the lock request
   */
  public LockResponse lockResource(String dbName, String tableName, long txnId) {
    final ResourceLock prevResourceLock = locks.putIfAbsent(
        Warehouse.getQualifiedName(dbName, tableName),
        new ResourceLock(txnId, System.nanoTime(), State.ACQUIRED));
    if (prevResourceLock != null) {
      return new LockResponse(txnId, LockState.NOT_ACQUIRED);
    }
    return new LockResponse(txnId, LockState.ACQUIRED);
  }

  /**
   * Moves from ACQUIRED state to COMMIT_READY.
   * @param dbName the db name of the materialization
   * @param tableName the table name of the materialization
   * @param txnId the transaction id for the rebuild
   * @return true if the lock was still active and we could move the materialization
   * to COMMIT_READY state, false otherwise
   */
  public boolean readyToCommitResource(String dbName, String tableName, long txnId) {
    final ResourceLock prevResourceLock = locks.get(Warehouse.getQualifiedName(dbName, tableName));
    if (prevResourceLock == null || prevResourceLock.txnId != txnId) {
      // Lock was outdated and it was removed (then maybe another transaction picked it up)
      return false;
    }
    return prevResourceLock.state.compareAndSet(State.ACQUIRED, State.COMMIT_READY);
  }

  /**
   * Heartbeats a certain lock and refreshes its timer.
   * @param dbName the db name of the materialization
   * @param tableName the table name of the materialization
   * @param txnId the transaction id for the rebuild
   * @throws MetaException
   */
  public boolean refreshLockResource(String dbName, String tableName, long txnId) {
    final ResourceLock prevResourceLock = locks.get(Warehouse.getQualifiedName(dbName, tableName));
    if (prevResourceLock == null || prevResourceLock.txnId != txnId ||
        prevResourceLock.state.get() != State.ACQUIRED) {
      // Lock was outdated and it was removed (then maybe another transaction picked it up)
      // or changed its state
      return false;
    }
    prevResourceLock.lastHeartBeatTime.set(System.currentTimeMillis());
    return true;
  }

  /**
   * Releases a certain lock.
   * @param dbName the db name of the materialization
   * @param tableName the table name of the materialization
   * @param txnId the transaction id for the rebuild
   * @return true if the lock could be released properly, false otherwise
   * @throws MetaException
   */
  public boolean unlockResource(String dbName, String tableName, long txnId) {
    final String fullyQualifiedName = Warehouse.getQualifiedName(dbName, tableName);
    final ResourceLock prevResourceLock = locks.get(fullyQualifiedName);
    if (prevResourceLock == null || prevResourceLock.txnId != txnId) {
      return false;
    }
    return locks.remove(fullyQualifiedName, prevResourceLock);
  }

  /**
   * Method that removes from the handler those locks that have expired.
   * @param timeout time after which we consider the locks to have expired
   * @throws MetaException
   */
  public long cleanupResourceLocks(long timeout) {
    long removed = 0L;
    final long currentTime = System.currentTimeMillis();
    for (Iterator<Map.Entry<String, ResourceLock>> it = locks.entrySet().iterator(); it.hasNext();) {
      final ResourceLock resourceLock = it.next().getValue();
      if (currentTime - resourceLock.lastHeartBeatTime.get() > timeout) {
        if (resourceLock.state.compareAndSet(State.ACQUIRED, State.EXPIRED)) {
          it.remove();
          removed++;
        }
      }
    }
    return removed;
  }

  /**
   * This class represents a lock that consists of transaction id,
   * last refresh time, and state.
   */
  private class ResourceLock {
    final long txnId;
    final AtomicLong lastHeartBeatTime;
    final AtomicStateEnum state;

    ResourceLock(long txnId, long lastHeartBeatTime, State state) {
      this.txnId = txnId;
      this.lastHeartBeatTime = new AtomicLong(lastHeartBeatTime);
      this.state = new AtomicStateEnum(state);
    }
  }

  private enum State {
    // This is the initial state for a lock
    ACQUIRED,
    // This means that the lock is being committed at this instant, hence
    // the cleaner should not remove it even if it times out. If transaction
    // fails, the finally clause will remove the lock
    COMMIT_READY,
    // This means that the lock is ready to be cleaned, hence it cannot
    // be committed anymore
    EXPIRED;
  }

  /**
   * Wrapper class around State enum to make its operations atomic.
   */
  private class AtomicStateEnum {
    private final AtomicReference<State> ref;

    public AtomicStateEnum(final State initialValue) {
      this.ref = new AtomicReference<State>(initialValue);
    }

    public void set(final State newValue) {
      this.ref.set(newValue);
    }

    public State get() {
      return this.ref.get();
    }

    public State getAndSet(final State newValue) {
      return this.ref.getAndSet(newValue);
    }

    public boolean compareAndSet(final State expect, final State update) {
      return this.ref.compareAndSet(expect, update);
    }
  }

}
