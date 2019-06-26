/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
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


  public LlapMetricsCollector(Configuration conf) {
    this(
            conf,
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(THREAD_NAME)
                            .build()),
            LlapManagementProtocolClientImplFactory.basicInstance(conf));
  }

  @VisibleForTesting
  LlapMetricsCollector(Configuration conf, ScheduledExecutorService scheduledMetricsExecutor,
                       LlapManagementProtocolClientImplFactory clientFactory) {
    this.scheduledMetricsExecutor = scheduledMetricsExecutor;
    this.clientFactory = clientFactory;
    this.llapClients = new HashMap<>();
    this.instanceStatisticsMap = new ConcurrentHashMap<>();
    this.metricsCollectionMs = HiveConf.getTimeVar(conf,
            HiveConf.ConfVars.LLAP_TASK_SCHEDULER_AM_COLLECT_DAEMON_METRICS_MS, TimeUnit.MILLISECONDS);
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
        instanceStatisticsMap.put(identity, new LlapMetrics(metrics));

      } catch (ServiceException ex) {
        LOG.error(ex.getMessage(), ex);
        instanceStatisticsMap.remove(identity);
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
   * Creates a LlapManagementProtocolClientImpl from a given LlapServiceInstance.
   */
  public static class LlapManagementProtocolClientImplFactory {
    private final Configuration conf;
    private final RetryPolicy retryPolicy;
    private final SocketFactory socketFactory;

    public LlapManagementProtocolClientImplFactory(Configuration conf, RetryPolicy retryPolicy,
                                                   SocketFactory socketFactory) {
      this.conf = conf;
      this.retryPolicy = retryPolicy;
      this.socketFactory = socketFactory;
    }

    private static LlapManagementProtocolClientImplFactory basicInstance(Configuration conf) {
      return new LlapManagementProtocolClientImplFactory(
              conf,
              RetryPolicies.retryUpToMaximumCountWithFixedSleep(5, 3000L, TimeUnit.MILLISECONDS),
              NetUtils.getDefaultSocketFactory(conf));
    }

    public LlapManagementProtocolClientImpl create(LlapServiceInstance serviceInstance) {
      LlapManagementProtocolClientImpl client = new LlapManagementProtocolClientImpl(conf, serviceInstance.getHost(),
              serviceInstance.getManagementPort(), retryPolicy,
              socketFactory);
      return client;
    }
  }

  /**
   * Stores the metrics retrieved from the llap daemons, along with the retrieval timestamp.
   */
  public static class LlapMetrics {
    private final long timestamp;
    private final LlapDaemonProtocolProtos.GetDaemonMetricsResponseProto metrics;

    public LlapMetrics(LlapDaemonProtocolProtos.GetDaemonMetricsResponseProto metrics) {
      this.timestamp = System.currentTimeMillis();
      this.metrics = metrics;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public LlapDaemonProtocolProtos.GetDaemonMetricsResponseProto getMetrics() {
      return metrics;
    }
  }
}
