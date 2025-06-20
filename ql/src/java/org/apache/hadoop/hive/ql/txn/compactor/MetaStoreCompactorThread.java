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
import org.apache.hadoop.hive.metastore.MetaStoreThread;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hive.metastore.HMSHandler.getMSForConf;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.COMPACTOR_USE_CUSTOM_POOL;

/**
 * Compactor threads that runs in the metastore. It uses a {@link TxnStore}
 * to access the internal database.
 */
public abstract class MetaStoreCompactorThread extends CompactorThread implements MetaStoreThread {

  protected TxnStore txnHandler;
  protected ScheduledExecutorService cycleUpdaterExecutorService;
  protected MetadataCache metadataCache;

  @Override
  public void init(AtomicBoolean stop) throws Exception {
    super.init(stop);

    // Get our own instance of the transaction handler
    txnHandler = TxnUtils.getTxnStore(conf);
    metadataCache = new MetadataCache(isCacheEnabled());
    // Initialize the RawStore, with the flag marked as true. Since its stored as a ThreadLocal variable in the
    // HMSHandlerContext, it will use the compactor related pool.
    MetastoreConf.setBoolVar(conf, COMPACTOR_USE_CUSTOM_POOL, true);
    getMSForConf(conf);
  }

  @Override Table resolveTable(CompactionInfo ci) throws MetaException {
    return CompactorUtil.resolveTable(conf, ci.dbname, ci.tableName);
  }

  protected abstract boolean isCacheEnabled();

  protected void startCycleUpdater(long updateInterval, Runnable taskToRun) {
    if (cycleUpdaterExecutorService == null) {
      if (updateInterval > 0) {
        cycleUpdaterExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
            .setPriority(Thread.currentThread().getPriority())
            .setDaemon(true)
            .setNameFormat("Cycle-Duration-Updater-%d")
            .build());
        cycleUpdaterExecutorService.scheduleAtFixedRate(
            taskToRun,
            updateInterval, updateInterval, TimeUnit.MILLISECONDS);
      }
    }
  }

  protected void stopCycleUpdater() {
    if (cycleUpdaterExecutorService != null) {
      cycleUpdaterExecutorService.shutdownNow();
      cycleUpdaterExecutorService = null;
    }
  }

  protected static long updateCycleDurationMetric(String metric, long startedAt) {
    if (startedAt >= 0) {
      long elapsed = System.currentTimeMillis() - startedAt;
      LOG.debug("Updating {} metric to {}", metric, elapsed);
      Metrics.getOrCreateGauge(metric)
          .set((int) elapsed);
      return elapsed;
    }
    return 0;
  }

}
