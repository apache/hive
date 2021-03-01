/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.metrics;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreThread;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Collect and publish ACID and compaction related metrics.
 */
public class AcidMetricService extends Thread implements MetaStoreThread {

  private static final Logger LOG = LoggerFactory.getLogger(AcidMetricService.class);
  private Configuration conf;
  private int threadId;
  private AtomicBoolean stop;
  private TxnStore txnHandler;
  private long checkInterval;

  @Override
  public void setThreadId(int threadId) {
    setPriority(MIN_PRIORITY);
    this.threadId = threadId;
  }

  @Override
  public void init(AtomicBoolean stop) throws Exception {
    setDaemon(true); // the process will exit without waiting for this thread

    this.stop = stop;
    checkInterval = MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_CHECK_INTERVAL, TimeUnit.MILLISECONDS);
    // Get our own instance of the transaction handler
    txnHandler = TxnUtils.getTxnStore(conf);
  }

  @Override
  public void run() {
    LOG.info("Starting AcidMetricService thread");
    try {
      do {
        long startedAt = System.currentTimeMillis();

        boolean metricsEnabled = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED) &&
            MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_THREAD_ON);
        if (!metricsEnabled) {
          // Make it possible to disable metrics collection without a restart
          LOG.debug("AcidMetricService is running but metric collection is not enabled");
          if (!stop.get()) {
            Thread.sleep(checkInterval);
          }
          continue;
        }
        try {
          collectMetrics();
        } catch (Exception ex) {
         LOG.error("Caught exception in AcidMetricService loop", ex);
        }

        long elapsedTime = System.currentTimeMillis() - startedAt;
        LOG.debug("AcidMetricService thread finished one loop in {} seconds.", elapsedTime / 1000);
        if (elapsedTime < checkInterval && !stop.get()) {
          Thread.sleep(checkInterval - elapsedTime);
        }

      } while (!stop.get());
    } catch (Throwable t) {
      LOG.error("Caught an exception in the main loop of AcidMetricService, exiting ", t);
    }
  }

  private void collectMetrics() throws MetaException {

    ShowCompactResponse currentCompactions = txnHandler.showCompact(new ShowCompactRequest());
    updateMetricsFromShowCompact(currentCompactions);

  }

  @VisibleForTesting
  static void updateMetricsFromShowCompact(ShowCompactResponse showCompactResponse) {
    Map<String, ShowCompactResponseElement> lastElements = new HashMap<>();
    long oldestEnqueueTime = Long.MAX_VALUE;

    // Get the last compaction for each db/table/partition
    for(ShowCompactResponseElement element : showCompactResponse.getCompacts()) {
      String key = element.getDbname() + "/" + element.getTablename() +
          (element.getPartitionname() != null ? "/" + element.getPartitionname() : "");
      // If new key, add the element, if there is an existing one, change to the element if the element.id is greater than old.id
      lastElements.compute(key, (k, old) -> (old == null) ? element : (element.getId() > old.getId() ? element : old));
      if (TxnStore.INITIATED_RESPONSE.equals(element.getState()) && oldestEnqueueTime > element.getEnqueueTime()) {
        oldestEnqueueTime = element.getEnqueueTime();
      }
    }

    // Get the current count for each state
    Map<String, Long> counts = lastElements.values().stream()
        .collect(Collectors.groupingBy(e -> e.getState(), Collectors.counting()));

    // Update metrics
    for (int i = 0; i < TxnStore.COMPACTION_STATES.length; ++i) {
      String key = MetricsConstants.COMPACTION_STATUS_PREFIX + TxnStore.COMPACTION_STATES[i];
      Long count = counts.get(TxnStore.COMPACTION_STATES[i]);
      if (count != null) {
        Metrics.getOrCreateGauge(key).set(count.intValue());
      } else {
        Metrics.getOrCreateGauge(key).set(0);
      }
    }
    if (oldestEnqueueTime == Long.MAX_VALUE) {
      Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_ENQUEUE_AGE).set(0);
    } else {
      Metrics.getOrCreateGauge(MetricsConstants.COMPACTION_OLDEST_ENQUEUE_AGE)
          .set((int) ((System.currentTimeMillis() - oldestEnqueueTime) / 1000L));
    }
  }

  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

}
