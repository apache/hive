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
package org.apache.hadoop.hive.ql.txn.compactor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hive.common.util.ShutdownHookManager;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.hive.common.util.HiveStringUtils.SHUTDOWN_HOOK_PRIORITY;

/**
 * Singleton service responsible for heartbeating the compaction transactions.
 */
public class CompactionHeartbeatService {


  private static final Logger LOG = LoggerFactory.getLogger(CompactionHeartbeatService.class);

  private static volatile CompactionHeartbeatService instance;

  /**
   * Return the singleton instance of this class.
   * @param conf The {@link HiveConf} used to create the service. Used only during the firsst call
   * @return Returns the singleton {@link CompactionHeartbeatService}
   * @throws IllegalStateException Thrown when the service has already been destroyed.
   */
  public static CompactionHeartbeatService getInstance(HiveConf conf) {
    if (instance == null) {
      synchronized (CompactionHeartbeatService.class) {
        if (instance == null) {
          LOG.debug("Initializing compaction txn heartbeater service.");
          instance = new CompactionHeartbeatService(conf);
          ShutdownHookManager.addShutdownHook(() -> instance.shutdown(), SHUTDOWN_HOOK_PRIORITY);
        }
      }
    }
    if (instance.shuttingDown) {
      throw new IllegalStateException("CompactionHeartbeatService is already destroyed!");
    }
    return instance;
  }

  private final ObjectPool<IMetaStoreClient> clientPool;
  private volatile boolean shuttingDown = false;
  private final long initialDelay;
  private final long period;
  private final ConcurrentHashMap<Long, CompactionHeartbeater> tasks = new ConcurrentHashMap<>(30);

  /**
   * Starts the heartbeat for the given transaction
   * @param txnId The id of the compaction txn
   * @param lockId The id of the lock associated with the txn
   * @param tableName Required for logging only
   * @throws IllegalStateException Thrown when the heartbeat for the given txn has already been started.
   */
  public void startHeartbeat(long txnId, long lockId, String tableName) {
    if (shuttingDown) {
      throw new IllegalStateException("Service is shutting down, starting new heartbeats is not possible!");
    }
    if (tasks.containsKey(txnId)) {
      throw new IllegalStateException("Heartbeat was already started for TXN " + txnId);
    }
    LOG.info("Submitting heartbeat task for TXN {}", txnId);
    CompactionHeartbeater heartbeater = new CompactionHeartbeater(txnId, lockId, tableName);
    heartbeater.start();
    tasks.put(txnId, heartbeater);
  }

  /**
   * Stops the heartbeat for the given transaction
   * @param txnId The id of the compaction txn
   * @throws IllegalStateException Thrown when there is no {@link CompactionHeartbeater} task associated with the
   * given txnId.
   */
  public void stopHeartbeat(long txnId) throws InterruptedException {
    LOG.info("Stopping heartbeat task for TXN {}", txnId);
    CompactionHeartbeater heartbeater = tasks.get(txnId);
    if (heartbeater == null) {
      throw new IllegalStateException("No registered heartbeat found for TXN " + txnId);
    }
    try {
      heartbeater.stop();
    } finally {
      tasks.remove(txnId);
    }
  }

  /**
   * Shuts down the service, by closing its underlying resources. Be aware that after shutdown this service is no
   * longer usable, there is no way to re-initialize it.
   */
  void shutdown() {
    shuttingDown = true;
    LOG.info("Shutting down compaction txn heartbeater service.");
    for (CompactionHeartbeater heartbeater : tasks.values()) {
      try {
        heartbeater.stop();
      } catch (InterruptedException e) {
        LOG.warn("Shutdownhook thread was interrupted during shutting down the CompactionHeartbeatService.");
      }
    }
    tasks.clear();
    clientPool.close();
    LOG.info("Compaction txn heartbeater service is successfully stopped.");
  }

  private CompactionHeartbeatService(HiveConf conf) {
    int numberOfWorkers = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.COMPACTOR_WORKER_THREADS);
    GenericObjectPoolConfig<IMetaStoreClient> config = new GenericObjectPoolConfig<>();
    config.setMinIdle(1);
    config.setMaxIdle(2);
    config.setMaxTotal(numberOfWorkers);
    config.setMaxWaitMillis(2000);
    clientPool = new GenericObjectPool<>(new IMetaStoreClientFactory(conf), config);
    long txnTimeout = MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.TXN_TIMEOUT, TimeUnit.MILLISECONDS);
    initialDelay = txnTimeout / 4;
    period = txnTimeout / 2;
  }

  private final class CompactionHeartbeater {
    private final Logger LOG = LoggerFactory.getLogger(CompactionHeartbeater.class);
    private final long txnId;
    private final long lockId;
    private final String tableName;
    private final ScheduledThreadPoolExecutor heartbeatExecutor;

    private CompactionHeartbeater(long txnId, long lockId, String tableName) {
      heartbeatExecutor = new ScheduledThreadPoolExecutor(1);
      heartbeatExecutor.setThreadFactory(new ThreadFactoryBuilder()
          .setPriority(Thread.MIN_PRIORITY)
          .setDaemon(true)
          .setNameFormat("CompactionTxnHeartbeater-" + txnId)
          .build());
      this.tableName = Objects.requireNonNull(tableName);
      this.txnId = txnId;
      this.lockId = lockId;
    }

    void start() {
      heartbeatExecutor.scheduleAtFixedRate(() -> {
        IMetaStoreClient msc = null;
        try {
          LOG.debug("Heartbeating compaction transaction id {} for table: {}", txnId, tableName);
          // Create a metastore client for each thread since it is not thread safe
          msc = clientPool.borrowObject();
          msc.heartbeat(txnId, lockId);
        } catch (NoSuchElementException e) {
          LOG.error("Compaction transaction heartbeater pool exhausted, unable to heartbeat", e);
          // This heartbeat attempt failed, and there is no client to return to the pool.
          return;
        } catch (TException e) {
          LOG.error("Error while heartbeating compaction transaction id {} for table: {}", txnId, tableName, e);
          // Heartbeat failed, but the client is not broken, we can return it to the pool.
        } catch (Exception e) {
          LOG.error("Error while heartbeating compaction transaction id {} for table: {}", txnId, tableName, e);
          // Unknown error, invalidate the client, maybe it is broken.
          if (msc != null) {
            try {
              clientPool.invalidateObject(msc);
            } catch (Exception ex) {
              LOG.error("Error while invalidating a broken MetaStoreClient instance", e);
            }
          }
          return;
        }
        try {
          if (msc != null) {
            clientPool.returnObject(msc);
          }
        } catch (Exception e) {
          LOG.error("Error while returning back to the pool a MetaStoreClient instance", e);
        }
      }, initialDelay, period, TimeUnit.MILLISECONDS);
    }

    public void stop() throws InterruptedException {
      LOG.info("Shutting down compaction txn heartbeater instance.");
      heartbeatExecutor.shutdownNow();
      if (!heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        LOG.warn("Heartbeating for transaction {} did not stop in 5 seconds, do not wait any longer.", txnId);
        return;
      }
      LOG.info("Compaction txn heartbeater instance is successfully stopped.");
    }

  }

}