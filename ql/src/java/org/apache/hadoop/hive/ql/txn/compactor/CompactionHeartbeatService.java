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

import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hive.common.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.hive.common.util.HiveStringUtils.SHUTDOWN_HOOK_PRIORITY;

/**
 * Singleton service responsible for heartbeating the compaction transactions.
 */
class CompactionHeartbeatService {


  private static final Logger LOG = LoggerFactory.getLogger(CompactionHeartbeatService.class);

  private static volatile CompactionHeartbeatService instance;

  /**
   * Return the singleton instance of this class.
   * @param conf The {@link HiveConf} used to create the service. Used only during the firsst call
   * @return Returns the singleton {@link CompactionHeartbeatService}
   * @throws IllegalStateException Thrown when the service has already been destroyed.
   */
  static CompactionHeartbeatService getInstance(HiveConf conf) {
    if (instance == null) {
      synchronized (CompactionHeartbeatService.class) {
        if (instance == null) {
          LOG.debug("Initializing compaction txn heartbeater service.");
          instance = new CompactionHeartbeatService(conf);
          ShutdownHookManager.addShutdownHook(() -> {
            try {
              instance.shutdown();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }, SHUTDOWN_HOOK_PRIORITY);
        }
      }
    }
    if (instance.heartbeatExecutor.isShutdown()) {
      throw new IllegalStateException("The CompactionHeartbeatService is already destroyed!");
    }
    return instance;
  }

  private final ScheduledThreadPoolExecutor heartbeatExecutor;
  private final ObjectPool<IMetaStoreClient> clientPool;
  private final long initialDelay;
  private final long period;
  private final HashMap<Long, TaskWrapper> tasks = new HashMap<>(30);

  /**
   * Starts the heartbeat for the given transaction
   * @param txnId The id of the compaction txn
   * @param lockId The id of the lock associated with the txn
   * @param tableName Required for logging only
   * @throws IllegalStateException Thrown when the heartbeat for the given txn has already been started.
   */
  void startHeartbeat(long txnId, long lockId, String tableName) {
    if (tasks.containsKey(txnId)) {
      throw new IllegalStateException("Heartbeat was already started for TXN " + txnId);
    }
    LOG.info("Submitting heartbeat task for TXN {}", txnId);
    CompactionHeartbeater heartbeater = new CompactionHeartbeater(txnId, lockId, tableName);
    Future<?> submittedTask = heartbeatExecutor.scheduleAtFixedRate(heartbeater, initialDelay, period, TimeUnit.MILLISECONDS);
    tasks.put(txnId, new TaskWrapper(heartbeater, submittedTask));
  }

  /**
   * Stops the heartbeat for the given transaction
   * @param txnId The id of the compaction txn
   * @throws IllegalStateException Thrown when there is no {@link CompactionHeartbeater} task associated with the
   * given txnId.
   */
  void stopHeartbeat(long txnId) throws InterruptedException {
    LOG.info("Stopping heartbeat task for TXN {}", txnId);
    TaskWrapper wrapper = tasks.get(txnId);
    if (wrapper == null) {
      throw new IllegalStateException("No registered heartbeat found for TXN " + txnId);
    }
    wrapper.future.cancel(false);
    try {
      wrapper.heartbeater.waitUntilFinish(initialDelay);
    } finally {
      tasks.remove(txnId);
    }
  }

  /**
   * Shuts down the service, by closing its underlying resources. Be aware that after shutdown this service is no
   * longer usable, there is no way to re-initialize it.
   * @throws InterruptedException
   */
  void shutdown() throws InterruptedException {
    LOG.info("Shutting down compaction txn heartbeater service.");
    heartbeatExecutor.shutdownNow();
    try {
      heartbeatExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS);
    } finally {
      tasks.clear();
      clientPool.close();
    }
    LOG.info("Compaction txn heartbeater service is successfully stopped.");
  }

  private CompactionHeartbeatService(HiveConf conf) {
    int numberOfWorkers = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.COMPACTOR_WORKER_THREADS);
    heartbeatExecutor = new ScheduledThreadPoolExecutor(0);
    heartbeatExecutor.setRemoveOnCancelPolicy(true);
    GenericObjectPoolConfig<IMetaStoreClient> config = new GenericObjectPoolConfig<>();
    config.setMinIdle(1);
    config.setMaxIdle(2);
    config.setMaxTotal(numberOfWorkers);
    config.setBlockWhenExhausted(true);
    config.setMaxWaitMillis(2000);
    config.setTestOnBorrow(false);
    config.setTestOnCreate(false);
    config.setTestOnReturn(false);
    config.setTestWhileIdle(false);
    clientPool = new GenericObjectPool<>(new IMetaStoreClientFactory(conf), config);
    long txnTimeout = MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.TXN_TIMEOUT, TimeUnit.MILLISECONDS);
    initialDelay = txnTimeout / 4;
    period = txnTimeout / 2;
  }

  private final class CompactionHeartbeater implements Runnable {
    final private Logger LOG = LoggerFactory.getLogger(CompactionHeartbeater.class);
    private final long txnId;
    private final long lockId;
    private final String tableName;
    private final Object lock = new Object();
    private volatile boolean running = false;


    @Override
    public void run() {
      IMetaStoreClient msc = null;
      try {
        running = true;
        LOG.debug("Heartbeating compaction transaction id {} for table: {}", txnId, tableName);
        // Create a metastore client for each thread since it is not thread safe
        msc = clientPool.borrowObject();
        msc.heartbeat(txnId, lockId);
        clientPool.returnObject(msc);
      } catch (NoSuchElementException nsee) {
        LOG.error("Compaction transaction heartbeater pool exhausted, unable to heartbeat", nsee);
      } catch (Exception e) {
        LOG.error("Error while heartbeating compaction transaction id {} for table: {}", txnId, tableName, e);
        try {
          clientPool.invalidateObject(msc);
        } catch (Exception ex) {
          LOG.error("Error while invalidating a broken MetaStoreClient instance", e);
        }
      } finally {
        synchronized (lock) {
          running = false;
          lock.notifyAll();
        }
      }
    }

    public void waitUntilFinish(long timeout) throws InterruptedException {
      synchronized (lock) {
        if (running) {
          lock.wait(timeout);
        }
      }
    }

    private CompactionHeartbeater(long txnId, long lockId, String tableName) {
      this.tableName = Objects.requireNonNull(tableName);
      this.txnId = txnId;
      this.lockId = lockId;
    }

  }

  private static final class TaskWrapper {

    private final CompactionHeartbeater heartbeater;
    private final Future<?> future;

    private TaskWrapper(CompactionHeartbeater heartbeater, Future<?> future) {
      this.heartbeater = heartbeater;
      this.future = future;
    }
  }

}