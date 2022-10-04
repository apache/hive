/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hive;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Tasks;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveCommitLock {

  private static final Logger LOG = LoggerFactory.getLogger(HiveCommitLock.class);

  private static final String HIVE_ACQUIRE_LOCK_TIMEOUT_MS = "iceberg.hive.lock-timeout-ms";
  private static final String HIVE_LOCK_CHECK_MIN_WAIT_MS = "iceberg.hive.lock-check-min-wait-ms";
  private static final String HIVE_LOCK_CHECK_MAX_WAIT_MS = "iceberg.hive.lock-check-max-wait-ms";
  private static final String HIVE_TABLE_LEVEL_LOCK_EVICT_MS = "iceberg.hive.table-level-lock-evict-ms";
  private static final long HIVE_ACQUIRE_LOCK_TIMEOUT_MS_DEFAULT = 3 * 60 * 1000; // 3 minutes
  private static final long HIVE_LOCK_CHECK_MIN_WAIT_MS_DEFAULT = 50; // 50 milliseconds
  private static final long HIVE_LOCK_CHECK_MAX_WAIT_MS_DEFAULT = 5 * 1000; // 5 seconds

  private static final long HIVE_TABLE_LEVEL_LOCK_EVICT_MS_DEFAULT = TimeUnit.MINUTES.toMillis(10);

  private static Cache<String, ReentrantLock> commitLockCache;

  private static synchronized void initTableLevelLockCache(long evictionTimeout) {
    if (commitLockCache == null) {
      commitLockCache = Caffeine.newBuilder()
          .expireAfterAccess(evictionTimeout, TimeUnit.MILLISECONDS)
          .build();
    }
  }

  private final String fullName;
  private final String databaseName;
  private final String tableName;
  private final ClientPool<IMetaStoreClient, TException> metaClients;

  private final long lockAcquireTimeout;
  private final long lockCheckMinWaitTime;
  private final long lockCheckMaxWaitTime;

  private Optional<Long> hmsLockId = Optional.empty();
  private Optional<ReentrantLock> jvmLock = Optional.empty();

  public HiveCommitLock(Configuration conf, ClientPool<IMetaStoreClient, TException> metaClients,
      String catalogName, String databaseName, String tableName) {
    this.metaClients = metaClients;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.fullName = catalogName + "." + databaseName + "." + tableName;

    this.lockAcquireTimeout =
        conf.getLong(HIVE_ACQUIRE_LOCK_TIMEOUT_MS, HIVE_ACQUIRE_LOCK_TIMEOUT_MS_DEFAULT);
    this.lockCheckMinWaitTime =
        conf.getLong(HIVE_LOCK_CHECK_MIN_WAIT_MS, HIVE_LOCK_CHECK_MIN_WAIT_MS_DEFAULT);
    this.lockCheckMaxWaitTime =
        conf.getLong(HIVE_LOCK_CHECK_MAX_WAIT_MS, HIVE_LOCK_CHECK_MAX_WAIT_MS_DEFAULT);
    long tableLevelLockCacheEvictionTimeout =
        conf.getLong(HIVE_TABLE_LEVEL_LOCK_EVICT_MS, HIVE_TABLE_LEVEL_LOCK_EVICT_MS_DEFAULT);
    initTableLevelLockCache(tableLevelLockCacheEvictionTimeout);
  }

  public void acquire() throws UnknownHostException, TException, InterruptedException {
    // getting a process-level lock per table to avoid concurrent commit attempts to the same table from the same
    // JVM process, which would result in unnecessary and costly HMS lock acquisition requests
    acquireJvmLock();
    acquireLockFromHms();
  }

  public void release() {
    releaseHmsLock();
    releaseJvmLock();
  }

  // TODO add lock heart beating for cases where default lock timeout is too low.
  private void acquireLockFromHms() throws UnknownHostException, TException, InterruptedException {
    if (hmsLockId.isPresent()) {
      throw new IllegalArgumentException(String.format("HMS lock ID=%s already acquired for table %s.%s",
          hmsLockId.get(), databaseName, tableName));
    }
    final LockComponent lockComponent = new LockComponent(LockType.EXCL_WRITE, LockLevel.TABLE, databaseName);
    lockComponent.setTablename(tableName);
    final LockRequest lockRequest = new LockRequest(Lists.newArrayList(lockComponent),
        System.getProperty("user.name"),
        InetAddress.getLocalHost().getHostName());
    LockResponse lockResponse = metaClients.run(client -> client.lock(lockRequest));
    AtomicReference<LockState> state = new AtomicReference<>(lockResponse.getState());
    long lockId = lockResponse.getLockid();
    this.hmsLockId = Optional.of(lockId);

    final long start = System.currentTimeMillis();
    long duration = 0;
    boolean timeout = false;

    try {
      if (state.get().equals(LockState.WAITING)) {
        // Retry count is the typical "upper bound of retries" for Tasks.run() function. In fact, the maximum number of
        // attempts the Tasks.run() would try is `retries + 1`. Here, for checking locks, we use timeout as the
        // upper bound of retries. So it is just reasonable to set a large retry count. However, if we set
        // Integer.MAX_VALUE, the above logic of `retries + 1` would overflow into Integer.MIN_VALUE. Hence,
        // the retry is set conservatively as `Integer.MAX_VALUE - 100` so it doesn't hit any boundary issues.
        Tasks.foreach(lockId)
            .retry(Integer.MAX_VALUE - 100)
            .exponentialBackoff(
                lockCheckMinWaitTime,
                lockCheckMaxWaitTime,
                lockAcquireTimeout,
                1.5)
            .throwFailureWhenFinished()
            .onlyRetryOn(WaitingForHmsLockException.class)
            .run(id -> {
              try {
                LockResponse response = metaClients.run(client -> client.checkLock(id));
                LockState newState = response.getState();
                state.set(newState);
                if (newState.equals(LockState.WAITING)) {
                  throw new WaitingForHmsLockException("Waiting for lock.");
                }
              } catch (InterruptedException e) {
                Thread.interrupted(); // Clear the interrupt status flag
                LOG.warn("Interrupted while waiting for lock.", e);
              }
            }, TException.class);
      }
    } catch (WaitingForHmsLockException waitingForLockException) {
      timeout = true;
      duration = System.currentTimeMillis() - start;
    } finally {
      if (!state.get().equals(LockState.ACQUIRED)) {
        releaseHmsLock();
      }
    }

    // timeout and do not have lock acquired
    if (timeout && !state.get().equals(LockState.ACQUIRED)) {
      throw new CommitFailedException("Timed out after %s ms waiting for lock on %s.%s",
          duration, databaseName, tableName);
    }

    if (!state.get().equals(LockState.ACQUIRED)) {
      throw new CommitFailedException("Could not acquire the lock on %s.%s, " +
          "lock request ended in state %s", databaseName, tableName, state);
    }
  }

  private void releaseHmsLock() {
    if (hmsLockId.isPresent()) {
      try {
        metaClients.run(client -> {
          client.unlock(hmsLockId.get());
          return null;
        });
        hmsLockId = Optional.empty();
      } catch (Exception e) {
        LOG.warn("Failed to unlock {}.{}", databaseName, tableName, e);
      }
    }
  }

  private void acquireJvmLock() {
    if (jvmLock.isPresent()) {
      throw new IllegalStateException(String.format("JVM lock already acquired for table %s", fullName));
    }
    jvmLock = Optional.of(commitLockCache.get(fullName, t -> new ReentrantLock(true)));
    jvmLock.get().lock();
  }

  private void releaseJvmLock() {
    if (jvmLock.isPresent()) {
      jvmLock.get().unlock();
      jvmLock = Optional.empty();
    }
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  private static class WaitingForHmsLockException extends RuntimeException {
    WaitingForHmsLockException(String message) {
      super(message);
    }
  }
}
