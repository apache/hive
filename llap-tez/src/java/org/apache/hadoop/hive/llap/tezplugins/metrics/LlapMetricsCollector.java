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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.impl.LlapManagementProtocolClientImpl;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstance;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstanceSet;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.registry.ServiceInstanceStateChangeListener;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Collect metrics from the llap daemons in every given milliseconds.
 */
public class LlapMetricsCollector implements ServiceStateChangeListener,
        ServiceInstanceStateChangeListener<LlapServiceInstance> {

  private static final Logger LOG = LoggerFactory.getLogger(LlapMetricsCollector.class);
  private static final String THREAD_NAME = "LlapTaskSchedulerMetricsCollectorThread";
  private static final long INITIAL_DELAY_MSEC = 10000L;

  private final ScheduledExecutorService scheduledMetricsExecutor;
  private final LlapManagementProtocolClientImplFactory clientFactory;
  private final Map<String, LlapManagementProtocolClientImpl> llapClients;
  private final Map<String, LlapMetrics> instanceStatisticsMap;
  private final long metricsCollectionMs;
  @VisibleForTesting
  final LlapMetricsListener listener;


  public LlapMetricsCollector(Configuration conf, LlapRegistryService registry) {
    this(
            conf,
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(THREAD_NAME)
                            .build()),
            LlapManagementProtocolClientImplFactory.basicInstance(conf),
            registry);
  }

  @VisibleForTesting
  LlapMetricsCollector(Configuration conf, ScheduledExecutorService scheduledMetricsExecutor,
                       LlapManagementProtocolClientImplFactory clientFactory) {
    this(conf, scheduledMetricsExecutor, clientFactory, null);
  }

  @VisibleForTesting
  LlapMetricsCollector(Configuration conf, ScheduledExecutorService scheduledMetricsExecutor,
                       LlapManagementProtocolClientImplFactory clientFactory,
                       LlapRegistryService registry) {
    this.scheduledMetricsExecutor = scheduledMetricsExecutor;
    this.clientFactory = clientFactory;
    this.llapClients = new HashMap<>();
    this.instanceStatisticsMap = new ConcurrentHashMap<>();
    this.metricsCollectionMs = HiveConf.getTimeVar(conf,
            HiveConf.ConfVars.LLAP_TASK_SCHEDULER_AM_COLLECT_DAEMON_METRICS_MS, TimeUnit.MILLISECONDS);
    String listenerClass = HiveConf.getVar(conf,
        HiveConf.ConfVars.LLAP_TASK_SCHEDULER_AM_COLLECT_DAEMON_METRICS_LISTENER);
    if (listenerClass == null || listenerClass.isEmpty()) {
      listener = null;
    } else {
      try {
        listener = (LlapMetricsListener)Class.forName(listenerClass.trim()).newInstance();
        listener.init(conf, registry);
      } catch (Exception e) {
        throw new IllegalArgumentException("Wrong configuration for "
            + HiveConf.ConfVars.LLAP_TASK_SCHEDULER_AM_COLLECT_DAEMON_METRICS_LISTENER
            + " " + listenerClass, e);
      }
    }
  }

  public void start() {
    if (metricsCollectionMs > 0) {
      scheduledMetricsExecutor.scheduleAtFixedRate(() -> {
        collectMetrics();
      }, INITIAL_DELAY_MSEC, metricsCollectionMs, TimeUnit.MILLISECONDS);
    }
  }

  public void shutdown() {
    scheduledMetricsExecutor.shutdownNow();
  }

  @VisibleForTesting
  void collectMetrics() {
    for (Map.Entry<String, LlapManagementProtocolClientImpl> entry : llapClients.entrySet()) {
      String identity = entry.getKey();
      LlapManagementProtocolClientImpl client = entry.getValue();
      try {
        LlapDaemonProtocolProtos.GetDaemonMetricsResponseProto metrics =
                client.getDaemonMetrics(null,
                        LlapDaemonProtocolProtos.GetDaemonMetricsRequestProto.newBuilder().build());
        LlapMetrics newMetrics = new LlapMetrics(metrics);
        instanceStatisticsMap.put(identity, newMetrics);
        if (listener != null) {
          try {
            listener.newDaemonMetrics(identity, newMetrics);
          } catch (Throwable t) {
            LOG.warn("LlapMetricsListener thrown an unexpected exception", t);
          }
        }
      } catch (ServiceException ex) {
        LOG.error(ex.getMessage(), ex);
        instanceStatisticsMap.remove(identity);
      }
    }
    if (listener != null) {
      try {
        listener.newClusterMetrics(getMetrics());
      } catch (Throwable t) {
        LOG.warn("LlapMetricsListener thrown an unexpected exception", t);
      }
    }
  }

  @Override
  public void stateChanged(Service service) {
    if (service.getServiceState() == Service.STATE.STARTED) {
      if (service instanceof LlapRegistryService) {
        setupLlapRegistryService((LlapRegistryService) service);
      }
      start();
    } else if (service.getServiceState() == Service.STATE.STOPPED) {
      shutdown();
    }
  }

  private void setupLlapRegistryService(LlapRegistryService service) {
    try {
      consumeInitialInstances(service);
      service.registerStateChangeListener(this);
    } catch (IOException ex) {
      LOG.error(ex.getMessage(), ex);
    }
  }

  @VisibleForTesting
  void consumeInitialInstances(LlapRegistryService service) throws IOException {
    LlapServiceInstanceSet serviceInstances = service.getInstances();
    for (LlapServiceInstance serviceInstance :
            serviceInstances.getAll()) {
      onCreate(serviceInstance, -1);
    }
  }

  @Override
  public void onCreate(LlapServiceInstance serviceInstance, int ephSeqVersion) {
    LlapManagementProtocolClientImpl client = clientFactory.create(serviceInstance);
    llapClients.put(serviceInstance.getWorkerIdentity(), client);

  }

  @Override
  public void onUpdate(LlapServiceInstance serviceInstance, int ephSeqVersion) {
    //NOOP
  }

  @Override
  public void onRemove(LlapServiceInstance serviceInstance, int ephSeqVersion) {
    String workerIdentity = serviceInstance.getWorkerIdentity();
    llapClients.remove(workerIdentity);
    instanceStatisticsMap.remove(workerIdentity);
  }

  public LlapMetrics getMetrics(String workerIdentity) {
    return instanceStatisticsMap.get(workerIdentity);
  }

  public Map<String, LlapMetrics> getMetrics() {
    return Collections.unmodifiableMap(instanceStatisticsMap);
  }

  /**
   * Stores the metrics retrieved from the llap daemons, along with the retrieval timestamp.
   */
  public static class LlapMetrics {
    private final long timestamp;
    private final Map<String, Long> metrics;

    @VisibleForTesting
    LlapMetrics(long timestamp, Map<String, Long> metrics) {
      this.timestamp = timestamp;
      this.metrics = metrics;
    }

    public LlapMetrics(LlapDaemonProtocolProtos.GetDaemonMetricsResponseProto metrics) {
      this.timestamp = System.currentTimeMillis();
      this.metrics = new HashMap<String, Long>(metrics.getMetricsCount());
      metrics.getMetricsList().forEach(entry -> this.metrics.put(entry.getKey(), entry.getValue()));
    }

    public long getTimestamp() {
      return timestamp;
    }

    /**
     * The metric values in the map. The keys are the enum names (See: LlapDaemonExecutorInfo), and
     * the values are the actual values.
     * @return The metric map
     */
    public Map<String, Long> getMetrics() {
      return metrics;
    }
  }
}
