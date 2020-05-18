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

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SetCapacityRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SetCapacityResponseProto;
import org.apache.hadoop.hive.llap.impl.LlapManagementProtocolClientImpl;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorInfo;
import org.apache.hadoop.hive.llap.registry.impl.LlapZookeeperRegistryImpl.ConfigChangeLockResult;
import org.apache.hadoop.hive.llap.tezplugins.metrics.LlapMetricsCollector.LlapMetrics;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstance;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstanceSet;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * Test class to test BlacklistingLlapMetricsListener object.
 */
public class TestBlacklistingLlapMetricsListener {
  private static final SetCapacityResponseProto TEST_RESPONSE =
      SetCapacityResponseProto.getDefaultInstance();

  private BlacklistingLlapMetricsListener listener;

  private Configuration conf;

  @Mock
  private LlapRegistryService mockRegistry;

  @Mock
  private LlapManagementProtocolClientImplFactory mockClientFactory;

  @Mock
  private LlapManagementProtocolClientImpl mockClient;

  @Mock
  private LlapServiceInstanceSet mockInstanceSet;

  @Mock
  private LlapServiceInstance mockLlapServiceInstance;

  @Before
  public void setUp() throws Exception {
    initMocks(this);

    conf = new HiveConf();
    when(mockRegistry.getInstances()).thenReturn(mockInstanceSet);
    when(mockRegistry.lockForConfigChange(anyLong(), anyLong())).thenReturn(
        new ConfigChangeLockResult(true, Long.MIN_VALUE));
    when(mockRegistry.getInstances().getInstance(anyString())).thenReturn(mockLlapServiceInstance);
    when(mockClientFactory.create(any(LlapServiceInstance.class))).thenReturn(mockClient);
    when(mockClient.setCapacity(
        any(),
        any(SetCapacityRequestProto.class))).thenReturn(TEST_RESPONSE);

    listener = new BlacklistingLlapMetricsListener();
    listener.init(conf, mockRegistry, mockClientFactory);
  }

  @Test(timeout = 2000)
  public void testBlacklist() throws ServiceException {
    Map<String, LlapMetrics> data = generateClusterMetrics();
    listener.newClusterMetrics(data);

    // Then
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);

    verify(mockClient, times(1)).setCapacity(any(), any(SetCapacityRequestProto.class));
    verify(mockInstanceSet, times(1)).getInstance(argumentCaptor.capture());
    assertEquals("3", argumentCaptor.getValue());
  }

  @Test(timeout = 2000)
  public void testNotEnoughCapacity() throws ServiceException {
    Map<String, LlapMetrics> data = generateClusterMetrics();
    data.get("0").getMetrics().put(LlapDaemonExecutorInfo.ExecutorNumExecutorsAvailableAverage.name(), 5L);
    listener.newClusterMetrics(data);

    verify(mockClient, never()).setCapacity(any(RpcController.class), any(SetCapacityRequestProto.class));
  }

  @Test(timeout = 2000)
  public void testNoOutstandingResponseTime() throws ServiceException {
    Map<String, LlapMetrics> data = generateClusterMetrics();
    data.get("3").getMetrics().put(LlapDaemonExecutorInfo.AverageResponseTime.name(), 1500L);
    listener.newClusterMetrics(data);

    verify(mockClient, never()).setCapacity(any(RpcController.class), any(SetCapacityRequestProto.class));
  }

  @Test(timeout = 2000)
  public void testAlreadyBlacklisted() throws ServiceException {
    Map<String, LlapMetrics> data = generateClusterMetrics();
    data.get("3").getMetrics().put(LlapDaemonExecutorInfo.ExecutorNumExecutors.name(), 0L);
    listener.newClusterMetrics(data);

    verify(mockClient, never()).setCapacity(any(RpcController.class), any(SetCapacityRequestProto.class));
  }

  @Test(timeout = 2000)
  public void testCheckTime() throws Exception {
    Map<String, LlapMetrics> data = generateClusterMetrics();

    // Return that we can not yet blacklist a node
    long targetTime = System.currentTimeMillis() + 10000;
    when(mockRegistry.lockForConfigChange(anyLong(), anyLong())).thenReturn(
        new ConfigChangeLockResult(false, targetTime));
    listener.newClusterMetrics(data);

    verify(mockClient, never()).setCapacity(any(RpcController.class), any(SetCapacityRequestProto.class));
    assertEquals(targetTime, listener.nextCheckTime);

    // reset mock stuff
    reset(mockRegistry);
    when(mockRegistry.getInstances()).thenReturn(mockInstanceSet);

    // We will not try to set the capacity, or even lock until the time is reached
    listener.newClusterMetrics(data);
    verify(mockRegistry, never()).lockForConfigChange(anyLong(), anyLong());
    verify(mockClient, never()).setCapacity(any(RpcController.class), any(SetCapacityRequestProto.class));

    // If the time is reached, then we lock and blacklist
    listener.nextCheckTime = System.currentTimeMillis() - 1;
    when(mockRegistry.lockForConfigChange(anyLong(), anyLong())).thenReturn(
        new ConfigChangeLockResult(true, targetTime));
    listener.newClusterMetrics(data);

    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockClient, times(1)).setCapacity(any(), any(SetCapacityRequestProto.class));
    verify(mockInstanceSet, times(1)).getInstance(argumentCaptor.capture());
    assertEquals("3", argumentCaptor.getValue());
  }

  private Map<String, LlapMetrics> generateClusterMetrics() {
    Map<String, LlapMetrics> data = new HashMap<>(4);
    data.put("0", generateSingleNodeMetrics(3000, 1000, 7, 10));
    data.put("1", generateSingleNodeMetrics(3000, 1000, 7, 10));
    data.put("2", generateSingleNodeMetrics(3000, 1000, 7, 10));
    data.put("3", generateSingleNodeMetrics(3000, 2000, 5, 10));
    return data;
  }

  private LlapMetrics generateSingleNodeMetrics(long totalRequests, long averageResponseTime,
      long availableExecutors, long allExecutors) {
    Map<String, Long> metricsMap = new HashMap<>(4);
    metricsMap.put(LlapDaemonExecutorInfo.ExecutorTotalRequestsHandled.name(), totalRequests);
    metricsMap.put(LlapDaemonExecutorInfo.AverageResponseTime.name(), averageResponseTime);
    metricsMap.put(LlapDaemonExecutorInfo.ExecutorNumExecutorsAvailableAverage.name(), availableExecutors);
    metricsMap.put(LlapDaemonExecutorInfo.ExecutorNumExecutors.name(), allExecutors);
    return new LlapMetrics(0, metricsMap);
  }
}
