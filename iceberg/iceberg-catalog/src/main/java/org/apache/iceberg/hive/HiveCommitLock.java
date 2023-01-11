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
import java.util.UUID;
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
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
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
  private static final String HIVE_LOCK_CREATION_TIMEOUT_MS = "iceberg.hive.lock-creation-timeout-ms";
  private static final String HIVE_LOCK_CREATION_MIN_WAIT_MS = "iceberg.hive.lock-creation-min-wait-ms";
  private static final String HIVE_LOCK_CREATION_MAX_WAIT_MS = "iceberg.hive.lock-creation-max-wait-ms";
  private static final String HIVE_TABLE_LEVEL_LOCK_EVICT_MS = "iceberg.hive.table-level-lock-evict-ms";
  private static final long HIVE_ACQUIRE_LOCK_TIMEOUT_MS_DEFAULT = 3 * 60 * 1000; // 3 minutes
  private static final long HIVE_LOCK_CHECK_MIN_WAIT_MS_DEFAULT = 50; // 50 milliseconds
  private static final long HIVE_LOCK_CHECK_MAX_WAIT_MS_DEFAULT = 5 * 1000; // 5 seconds
  private static final long HIVE_LOCK_CREATION_TIMEOUT_MS_DEFAULT = 3 * 60 * 1000; // 3 minutes
  private static final long HIVE_LOCK_CREATION_MIN_WAIT_MS_DEFAULT = 50; // 50 milliseconds
  private static final long HIVE_LOCK_CREATION_MAX_WAIT_MS_DEFAULT = 5 * 1000; // 5 seconds

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
  private final long lockCreationTimeout;
  private final long lockCreationMinWaitTime;
  private final long lockCreationMaxWaitTime;
  private final String agentInfo;

  private Optional<Long> hmsLockId = Optional.empty();
  private Optional<ReentrantLock> jvmLock = Optional.empty();

  public HiveCommitLock(Configuration conf, ClientPool<IMetaStoreClient, TException> metaClients,
      String catalogName, String databaseName, String tableName) {
    this.metaClients = metaClients;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.fullName = catalogName + "." + databaseName + "." + tableName;

    this.agentInfo = "Iceberg-" + UUID.randomUUID();

    this.lockAcquireTimeout =
        conf.getLong(HIVE_ACQUIRE_LOCK_TIMEOUT_MS, HIVE_ACQUIRE_LOCK_TIMEOUT_MS_DEFAULT);
    this.lockCheckMinWaitTime =
        conf.getLong(HIVE_LOCK_CHECK_MIN_WAIT_MS, HIVE_LOCK_CHECK_MIN_WAIT_MS_DEFAULT);
    this.lockCheckMaxWaitTime =
        conf.getLong(HIVE_LOCK_CHECK_MAX_WAIT_MS, HIVE_LOCK_CHECK_MAX_WAIT_MS_DEFAULT);
    this.lockCreationTimeout =
            conf.getLong(HIVE_LOCK_CREATION_TIMEOUT_MS, HIVE_LOCK_CREATION_TIMEOUT_MS_DEFAULT);
    this.lockCreationMinWaitTime =
            conf.getLong(HIVE_LOCK_CREATION_MIN_WAIT_MS, HIVE_LOCK_CREATION_MIN_WAIT_MS_DEFAULT);
    this.lockCreationMaxWaitTime =
            conf.getLong(HIVE_LOCK_CREATION_MAX_WAIT_MS, HIVE_LOCK_CREATION_MAX_WAIT_MS_DEFAULT);
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
    LockInfo lockInfo = tryLock();
    long lockId = lockInfo.lockId;
    AtomicReference<LockState> state = new AtomicReference<>(lockInfo.lockState);
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
//    if (hmsLockId.isPresent()) {
//      try {
//
//        metaClients.run(client -> {
//          client.unlock(hmsLockId.get());
//          return null;
//        });
//        hmsLockId = Optional.empty();
//      } catch (Exception e) {
//        LOG.warn("Failed to unlock {}.{}", databaseName, tableName, e);
//      }
//    }

    Long id = null;
    try {
      if (!hmsLockId.isPresent()) {
        // Try to find the lock based on agentInfo. Only works with Hive 2 or later.
        if (HiveVersion.min(HiveVersion.HIVE_2)) {
          LockInfo lockInfo = findLock();
          if (lockInfo == null) {
            // No lock found
            LOG.info("No lock found with {} agentInfo", agentInfo);
            return;
          }

          id = lockInfo.lockId;
        } else {
          LOG.warn("Could not find lock with HMSClient {}", HiveVersion.current());
          return;
        }
      } else {
        id = hmsLockId.get();
      }

      doUnlock(hmsLockId.get());

    } catch (InterruptedException ie) {
      if (id != null) {
        // Interrupted unlock. We try to unlock one more time if we have a lockId
        try {
          Thread.interrupted(); // Clear the interrupt status flag for now, so we can retry unlock
          LOG.warn("Interrupted unlock we try one more time {}.{}", databaseName, tableName, ie);
          doUnlock(id);
        } catch (Exception e) {
          LOG.warn("Failed to unlock even on 2nd attempt {}.{}", databaseName, tableName, e);
        } finally {
          Thread.currentThread().interrupt(); // Set back the interrupt status
        }
      } else {
        Thread.currentThread().interrupt(); // Set back the interrupt status
        LOG.warn("Interrupted finding locks to unlock {}.{}", databaseName, tableName, ie);
      }
    } catch (Exception e) {
      LOG.warn("Failed to unlock {}.{}", databaseName, tableName, e);
    }
  }

  @VisibleForTesting
  void doUnlock(long lockId) throws TException, InterruptedException {
    metaClients.run(
        client -> {
          client.unlock(lockId);
          return null;
        });
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

  private static class LockInfo {
    private long lockId;
    private LockState lockState;

    private LockInfo() {
      this.lockId = -1;
      this.lockState = null;
    }

    private LockInfo(long lockId, LockState lockState) {
      this.lockId = lockId;
      this.lockState = lockState;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
              .add("lockId", lockId)
              .add("lockState", lockState)
              .toString();
    }
  }

  /**
   * Tries to create a lock. If the lock creation fails, and it is possible then retries the lock
   * creation a few times. If the lock creation is successful then a {@link LockInfo} is returned,
   * otherwise an appropriate exception is thrown.
   *
   * @return The created lock
   * @throws UnknownHostException When we are not able to fill the hostname for lock creation
   * @throws TException When there is an error during lock creation
   */
  @SuppressWarnings("ReverseDnsLookup")
  private LockInfo tryLock() throws UnknownHostException, TException {
    LockInfo lockInfo = new LockInfo();

    final LockComponent lockComponent =
            new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, databaseName);
    lockComponent.setTablename(tableName);
    final LockRequest lockRequest =
            new LockRequest(
                    Lists.newArrayList(lockComponent),
                    System.getProperty("user.name"),
                    InetAddress.getLocalHost().getHostName());

    // Only works in Hive 2 or later.
    if (HiveVersion.min(HiveVersion.HIVE_2)) {
      lockRequest.setAgentInfo(agentInfo);
    }

    Tasks.foreach(lockRequest)
            .retry(Integer.MAX_VALUE - 100)
            .exponentialBackoff(
                    lockCreationMinWaitTime, lockCreationMaxWaitTime, lockCreationTimeout, 2.0)
            .shouldRetryTest(e -> e instanceof TException && HiveVersion.min(HiveVersion.HIVE_2))
            .throwFailureWhenFinished()
            .run(
                request -> {
                  try {
                    LockResponse lockResponse = metaClients.run(client -> client.lock(request));
                    lockInfo.lockId = lockResponse.getLockid();
                    lockInfo.lockState = lockResponse.getState();
                  } catch (TException te) {
                    LOG.warn("Failed to acquire lock {}", request, te);
                    try {
                      // If we can not check for lock, or we do not find it, then rethrow the exception
                      // Otherwise we are happy as the findLock sets the lockId and the state correctly
                      if (!HiveVersion.min(HiveVersion.HIVE_2)) {
                        LockInfo lockFound = findLock();
                        if (lockFound != null) {
                          lockInfo.lockId = lockFound.lockId;
                          lockInfo.lockState = lockFound.lockState;
                          LOG.info("Found lock {} by agentInfo {}", lockInfo, agentInfo);
                          return;
                        }
                      }

                      throw te;
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      LOG.warn(
                              "Interrupted while checking for lock on table {}.{}", databaseName, tableName, e);
                      throw new RuntimeException("Interrupted while checking for lock", e);
                    }
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.warn("Interrupted while acquiring lock on table {}.{}", databaseName, tableName, e);
                    throw new RuntimeException("Interrupted while acquiring lock", e);
                  }
                },
                TException.class);

    // This should be initialized always, or exception should be thrown.
    LOG.debug("Lock {} created for table {}.{}", lockInfo, databaseName, tableName);
    return lockInfo;
  }

  /**
   * Search for the locks using HMSClient.showLocks identified by the agentInfo. If the lock is
   * there, then a {@link LockInfo} object is returned. If the lock is not found <code>null</code>
   * is returned.
   *
   * @return The {@link LockInfo} for the found lock, or <code>null</code> if nothing found
   */
  private LockInfo findLock() throws TException, InterruptedException {
    Preconditions.checkArgument(
            HiveVersion.min(HiveVersion.HIVE_2),
            "Minimally Hive 2 HMS client is needed to find the Lock using the showLocks API call");
    ShowLocksRequest showLocksRequest = new ShowLocksRequest();
    showLocksRequest.setDbname(databaseName);
    showLocksRequest.setTablename(tableName);
    ShowLocksResponse response = metaClients.run(client -> client.showLocks(showLocksRequest));
    for (ShowLocksResponseElement lock : response.getLocks()) {
      if (lock.getAgentInfo().equals(agentInfo)) {
        // We found our lock
        return new LockInfo(lock.getLockid(), lock.getState());
      }
    }

    // Not found anything
    return null;
  }

  private static class WaitingForHmsLockException extends RuntimeException {
    WaitingForHmsLockException(String message) {
      super(message);
    }
  }
}
