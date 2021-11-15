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

package org.apache.hadoop.hive.llap.tezplugins.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SetCapacityRequestProto;
import org.apache.hadoop.hive.llap.impl.LlapManagementProtocolClientImpl;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstance;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.llap.registry.impl.LlapZookeeperRegistryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of MetricsListener which blacklists slow nodes based on the statistics.
 */
public class BlacklistingLlapMetricsListener implements LlapMetricsListener {
  private static final Logger LOG = LoggerFactory.getLogger(BlacklistingLlapMetricsListener.class);
  private LlapRegistryService registry;
  private LlapManagementProtocolClientImplFactory clientFactory;
  private int minServedTasksNumber;
  private int maxBlacklistedNodes;
  private long minConfigChangeDelayMs;
  private float timeThreshold;
  private float emptyExecutorsThreshold;
  @VisibleForTesting
  long nextCheckTime = Long.MIN_VALUE;

  @VisibleForTesting
  void init(Configuration conf, LlapRegistryService registry, LlapManagementProtocolClientImplFactory clientFactory) {
    this.registry = registry;
    this.clientFactory = clientFactory;
    this.minServedTasksNumber =
        HiveConf.getIntVar(conf, HiveConf.ConfVars.LLAP_NODEHEALTHCHECKS_MINTASKS);
    this.minConfigChangeDelayMs =
        HiveConf.getTimeVar(conf, HiveConf.ConfVars.LLAP_NODEHEALTHCHECKS_MININTERVALDURATION,
            TimeUnit.MILLISECONDS);
    this.timeThreshold =
        HiveConf.getFloatVar(conf, HiveConf.ConfVars.LLAP_NODEHEALTHCHECKS_TASKTIMERATIO);
    this.emptyExecutorsThreshold =
        HiveConf.getFloatVar(conf,
            HiveConf.ConfVars.LLAP_NODEHEALTHCHECKS_EXECUTORRATIO);
    this.maxBlacklistedNodes =
        HiveConf.getIntVar(conf, HiveConf.ConfVars.LLAP_NODEHEALTHCHECKS_MAXNODES);

    Preconditions.checkArgument(minServedTasksNumber > 0,
        "Minimum served tasks should be greater than 0");
    Preconditions.checkArgument(minConfigChangeDelayMs > 0,
        "Minimum config change delay should be greater than 0");
    Preconditions.checkArgument(timeThreshold > 1.0f,
        "The time threshold should be greater than 1");
    Preconditions.checkArgument(maxBlacklistedNodes > 0,
        "The maximum number of blacklisted node should be greater than 1");
    Preconditions.checkNotNull(registry, "Registry should not be null");
    Preconditions.checkNotNull(clientFactory, "ClientFactory should not be null");

    LOG.info("BlacklistingLlapMetricsListener initialized with " +
                  "minServedTasksNumber={}, " +
                  "minConfigChangeDelayMs={}, " +
                  "timeThreshold={}, " +
                  "emptyExecutorsThreshold={}, " +
                  "maxBlacklistedNodes={}",
        minServedTasksNumber, minConfigChangeDelayMs, timeThreshold, emptyExecutorsThreshold, maxBlacklistedNodes);
  }

  @Override
  public void init(Configuration conf, LlapRegistryService registry) {
    init(conf, registry, LlapManagementProtocolClientImplFactory.basicInstance(conf));
  }

  @Override
  public void newDaemonMetrics(String workerIdentity, LlapMetricsCollector.LlapMetrics newMetrics) {
    // no op
  }

  @Override
  public void newClusterMetrics(Map<String, LlapMetricsCollector.LlapMetrics> newMetrics) {
    long sumAverageTime = 0;
    long sumEmptyExecutors = 0;
    long maxAverageTime = 0;
    long maxAverageTimeEmptyExecutors = 0;
    long maxAverageTimeMaxExecutors = 0;
    long workerNum = 0;
    int blacklistedNodes = 0;
    String maxAverageTimeIdentity = null;
    for (String workerIdentity : newMetrics.keySet()) {
      Map<String, Long> metrics = newMetrics.get(workerIdentity).getMetrics();
      long requestHandled = metrics.get(LlapDaemonExecutorInfo.ExecutorTotalRequestsHandled.name());
      long averageTime = metrics.get(LlapDaemonExecutorInfo.AverageResponseTime.name());
      long emptyExecutor =
          metrics.get(LlapDaemonExecutorInfo.ExecutorNumExecutorsAvailableAverage.name());
      long maxExecutors = metrics.get(LlapDaemonExecutorInfo.ExecutorNumExecutors.name());

      LOG.debug("Checking node {} with data: " +
                    "requestHandled={}, " +
                    "averageTime={}, " +
                    "emptyExecutors={}, " +
                    "maxExecutors={}",
          workerIdentity, requestHandled, averageTime, emptyExecutor, maxExecutors);

      if (maxExecutors == 0) {
        blacklistedNodes++;
        if (blacklistedNodes >= this.maxBlacklistedNodes) {
          LOG.info("Already enough blacklisted nodes {}. Skipping.", blacklistedNodes);
          return;
        } else {
          // We do not interested in the data for the blacklisted nodes
          continue;
        }
      }

      if (requestHandled > this.minServedTasksNumber) {
        workerNum++;
        sumAverageTime += averageTime;
        if (averageTime > maxAverageTime) {
          maxAverageTime = averageTime;
          maxAverageTimeEmptyExecutors = emptyExecutor;
          maxAverageTimeMaxExecutors = maxExecutors;
          maxAverageTimeIdentity = workerIdentity;
        }
        sumEmptyExecutors += emptyExecutor;
      }
    }

    // If we do not have enough data then return.
    if (workerNum < 2) {
      return;
    }

    LOG.debug("Found slowest node {} with data: " +
                  "sumAverageTime={}, " +
                  "sumEmptyExecutors={}, " +
                  "maxAverageTime={}, " +
                  "maxAverageTimeEmptyExecutors={}, " +
                  "maxAverageTimeMaxExecutors={}, " +
                  "workerNum={}, " +
                  "maxAverageTimeIdentity={}, " +
                  "blacklistedNodes={}",
        sumAverageTime, sumEmptyExecutors, maxAverageTime, maxAverageTimeEmptyExecutors,
        maxAverageTimeMaxExecutors, workerNum, maxAverageTimeIdentity, blacklistedNodes);
    // Check if the slowest node is at least timeThreshold times slower than the average
    double averageTimeWithoutSlowest = (double)(sumAverageTime - maxAverageTime) / (workerNum - 1);
    if (averageTimeWithoutSlowest * this.timeThreshold < maxAverageTime) {
      // We have a candidate, let's see if we have enough empty executors.
      long emptyExecutorsWithoutSlowest = sumEmptyExecutors - maxAverageTimeEmptyExecutors;
      if (emptyExecutorsWithoutSlowest > maxAverageTimeMaxExecutors * this.emptyExecutorsThreshold) {
        // Seems like a good candidate, let's try to blacklist
        try {
          LOG.debug("Trying to blacklist node: " + maxAverageTimeIdentity);
          setCapacity(maxAverageTimeIdentity, 0, 0);
        } catch (Throwable t) {
          LOG.debug("Can not blacklist node: " + maxAverageTimeIdentity, t);
        }
      }
    }
  }

  protected void setCapacity(String workerIdentity, int newExecutorNum, int newWaitQueueSize)
      throws IOException, ServiceException {
    long currentTime = System.currentTimeMillis();
    if (currentTime > nextCheckTime) {
      LlapZookeeperRegistryImpl.ConfigChangeLockResult lockResult =
          registry.lockForConfigChange(currentTime, currentTime + this.minConfigChangeDelayMs);

      LOG.debug("Got result for lock check: {}", lockResult);
      if (lockResult.isSuccess()) {
        LOG.info("Setting capacity for workerIdentity={} to newExecutorNum={}, newWaitQueueSize={}",
            workerIdentity, newExecutorNum, newWaitQueueSize);
        LlapServiceInstance serviceInstance = registry.getInstances().getInstance(workerIdentity);
        LlapManagementProtocolClientImpl client = clientFactory.create(serviceInstance);
        client.setCapacity(null,
            SetCapacityRequestProto.newBuilder()
                .setExecutorNum(newExecutorNum)
                .setQueueSize(newWaitQueueSize)
                .build());
      }
      if (lockResult.getNextConfigChangeTime() > -1L) {
        nextCheckTime = lockResult.getNextConfigChangeTime();
      }
    } else {
      LOG.debug("Skipping check. Current time {} and we are waiting for {}.", currentTime, nextCheckTime);
    }
  }
}
