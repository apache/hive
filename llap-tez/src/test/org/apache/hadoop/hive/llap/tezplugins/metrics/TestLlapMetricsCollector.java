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

import com.google.common.collect.Lists;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.impl.LlapManagementProtocolClientImpl;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstance;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstanceSet;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class TestLlapMetricsCollector {

  private static final long DEFAULT_TIMEOUT = 1000;
  private static final int TEST_SEQ_VERSION = -1;
  private static final String TEST_IDENTITY_1 = "testInstance_1";
  private static final String TEST_IDENTITY_2 = "testInstance_2";

  private static final LlapDaemonProtocolProtos.GetDaemonMetricsResponseProto TEST_RESPONSE =
          LlapDaemonProtocolProtos.GetDaemonMetricsResponseProto.getDefaultInstance();

  private LlapMetricsCollector collector;

  @Mock
  private Configuration mockConf;

  @Mock
  private LlapManagementProtocolClientImplFactory mockClientFactory;

  @Mock
  private LlapManagementProtocolClientImpl mockClient;

  @Mock
  private ScheduledExecutorService mockExecutor;

  @Before
  public void setUp() throws ServiceException {
    initMocks(this);

    when(mockConf.get(HiveConf.ConfVars.LLAP_TASK_SCHEDULER_AM_COLLECT_DAEMON_METRICS_MS.varname,
            HiveConf.ConfVars.LLAP_TASK_SCHEDULER_AM_COLLECT_DAEMON_METRICS_MS.defaultStrVal)).thenReturn("30000ms");
    when(mockConf.get(HiveConf.ConfVars.LLAP_TASK_SCHEDULER_AM_COLLECT_DAEMON_METRICS_LISTENER.varname,
        HiveConf.ConfVars.LLAP_TASK_SCHEDULER_AM_COLLECT_DAEMON_METRICS_LISTENER.defaultStrVal))
          .thenReturn(MockListener.class.getName());
    when(mockClientFactory.create(any(LlapServiceInstance.class))).thenReturn(mockClient);
    when(mockClient.getDaemonMetrics(
            any(), // can be NULL
            any(LlapDaemonProtocolProtos.GetDaemonMetricsRequestProto.class))).thenReturn(TEST_RESPONSE);
    collector = new LlapMetricsCollector(mockConf, mockExecutor, mockClientFactory);
  }

  @Test(timeout = DEFAULT_TIMEOUT)
  public void testAddService() {
    // Given
    LlapServiceInstance mockService = mock(LlapServiceInstance.class);
    when(mockService.getWorkerIdentity()).thenReturn(TEST_IDENTITY_1);

    assertTrue(collector != null);

    // When
    collector.onCreate(mockService, TEST_SEQ_VERSION);
    collector.collectMetrics();

    // Then
    assertEquals(1, collector.getMetrics().size());

  }


  @Test(timeout = DEFAULT_TIMEOUT)
  public void testRemoveService() {
    // Given
    LlapServiceInstance mockService = mock(LlapServiceInstance.class);
    when(mockService.getWorkerIdentity()).thenReturn(TEST_IDENTITY_1);

    // When
    collector.onCreate(mockService, TEST_SEQ_VERSION);
    collector.collectMetrics();
    collector.onRemove(mockService, TEST_SEQ_VERSION);

    // Then
    assertEquals(0, collector.getMetrics().size());
  }

  @Test(timeout = DEFAULT_TIMEOUT)
  public void testMultipleCollectOnSameInstance() {
    // Given
    LlapServiceInstance mockService = mock(LlapServiceInstance.class);
    when(mockService.getWorkerIdentity()).thenReturn(TEST_IDENTITY_1);

    // When
    collector.onCreate(mockService, TEST_SEQ_VERSION);
    collector.collectMetrics();
    LlapMetricsCollector.LlapMetrics metrics1 = collector.getMetrics(mockService.getWorkerIdentity());
    collector.collectMetrics();
    LlapMetricsCollector.LlapMetrics metrics2 = collector.getMetrics(mockService.getWorkerIdentity());

    // Then
    assertEquals(1, collector.getMetrics().size());
    assertNotEquals(metrics1, metrics2);
  }

  @Test(timeout = DEFAULT_TIMEOUT)
  public void testCollectOnMultipleInstances() {
    // Given
    LlapServiceInstance mockService1 = mock(LlapServiceInstance.class);
    when(mockService1.getWorkerIdentity()).thenReturn(TEST_IDENTITY_1);
    LlapServiceInstance mockService2 = mock(LlapServiceInstance.class);
    when(mockService2.getWorkerIdentity()).thenReturn(TEST_IDENTITY_2);

    // When
    collector.onCreate(mockService1, TEST_SEQ_VERSION);
    collector.onCreate(mockService2, TEST_SEQ_VERSION);
    collector.collectMetrics();

    // Then
    assertEquals(2, collector.getMetrics().size());
  }

  @Test(timeout = DEFAULT_TIMEOUT)
  public void testMultipleCollectOnMultipleInstances() {
    // Given
    LlapServiceInstance mockService1 = mock(LlapServiceInstance.class);
    when(mockService1.getWorkerIdentity()).thenReturn(TEST_IDENTITY_1);
    LlapServiceInstance mockService2 = mock(LlapServiceInstance.class);
    when(mockService2.getWorkerIdentity()).thenReturn(TEST_IDENTITY_2);

    // When
    collector.onCreate(mockService1, TEST_SEQ_VERSION);
    collector.onCreate(mockService2, TEST_SEQ_VERSION);
    collector.collectMetrics();
    collector.collectMetrics();
    collector.collectMetrics();

    // Then
    assertEquals(2, collector.getMetrics().size());
  }

  @Test(timeout = DEFAULT_TIMEOUT)
  public void testStartStartsScheduling() {
    // When
    collector.start();

    // Then
    verify(mockExecutor, times(1))
            .scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));

  }


  @Test(timeout = DEFAULT_TIMEOUT)
  public void testShutdown() {
    // When
    collector.shutdown();

    // Then
    verify(mockExecutor, times(1)).shutdownNow();
  }

  @Test(timeout = DEFAULT_TIMEOUT)
  public void testConsumeInitialInstances() throws IOException {
    // Given
    LlapServiceInstance mockService = mock(LlapServiceInstance.class);
    LlapServiceInstanceSet serviceInstances = mock(LlapServiceInstanceSet.class);
    LlapRegistryService mockRegistryService = mock(LlapRegistryService.class);

    when(mockService.getWorkerIdentity()).thenReturn(TEST_IDENTITY_1);
    when(serviceInstances.getAll()).thenReturn(Lists.newArrayList(mockService));
    when(mockRegistryService.getInstances()).thenReturn(serviceInstances);

    // When
    collector.consumeInitialInstances(mockRegistryService);
    collector.collectMetrics();

    // Then
    assertEquals(1, collector.getMetrics().size());
  }

  @Test(timeout = DEFAULT_TIMEOUT)
  public void testStartWontStartSchedulingIfTheConfigValueIsZeroMs() {
    // Given
    when(mockConf.get(HiveConf.ConfVars.LLAP_TASK_SCHEDULER_AM_COLLECT_DAEMON_METRICS_MS.varname,
            HiveConf.ConfVars.LLAP_TASK_SCHEDULER_AM_COLLECT_DAEMON_METRICS_MS.defaultStrVal)).thenReturn("0ms");
    collector = new LlapMetricsCollector(mockConf, mockExecutor, mockClientFactory);

    // When
    collector.start();

    // Then
    verify(mockExecutor, never())
            .scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));

  }

  /**
   * Check that the listener is created and called. The default config contains the mock listener.
   */
  @Test(timeout = DEFAULT_TIMEOUT)
  public void testListener() {
    // Given
    LlapServiceInstance mockService1 = mock(LlapServiceInstance.class);
    when(mockService1.getWorkerIdentity()).thenReturn(TEST_IDENTITY_1);
    LlapServiceInstance mockService2 = mock(LlapServiceInstance.class);
    when(mockService2.getWorkerIdentity()).thenReturn(TEST_IDENTITY_2);

    // When
    collector.onCreate(mockService1, TEST_SEQ_VERSION);
    collector.onCreate(mockService2, TEST_SEQ_VERSION);
    collector.collectMetrics();
    collector.collectMetrics();
    collector.collectMetrics();

    // Then
    assertNotNull(collector.listener);
    assertEquals(1, ((MockListener)collector.listener).initCount);
    assertEquals(3, ((MockListener)collector.listener).fullMetricsCount);
    assertEquals(6, ((MockListener)collector.listener).daemonMetricsCount);
  }

  /**
   * Check that the collector is working without the listener too.
   */
  @Test(timeout = DEFAULT_TIMEOUT)
  public void testWithoutListener() {
    // Given
    when(mockConf.get(HiveConf.ConfVars.LLAP_TASK_SCHEDULER_AM_COLLECT_DAEMON_METRICS_LISTENER.varname,
        HiveConf.ConfVars.LLAP_TASK_SCHEDULER_AM_COLLECT_DAEMON_METRICS_LISTENER.defaultStrVal)).thenReturn("");
    collector = new LlapMetricsCollector(mockConf, mockExecutor, mockClientFactory);

    LlapServiceInstance mockService1 = mock(LlapServiceInstance.class);
    when(mockService1.getWorkerIdentity()).thenReturn(TEST_IDENTITY_1);
    LlapServiceInstance mockService2 = mock(LlapServiceInstance.class);
    when(mockService2.getWorkerIdentity()).thenReturn(TEST_IDENTITY_2);

    // Check that there is no exception with start / create / remove / collect
    collector.start();
    collector.onCreate(mockService1, TEST_SEQ_VERSION);
    collector.onCreate(mockService2, TEST_SEQ_VERSION);
    collector.onRemove(mockService2, TEST_SEQ_VERSION);
    collector.collectMetrics();

    // Then
    assertNull(collector.listener);
  }

  /**
   * Just count the calls.
   */
  static class MockListener implements LlapMetricsListener {
    int initCount = 0;
    int daemonMetricsCount = 0;
    int fullMetricsCount = 0;

    @Override
    public void init(Configuration configuration, LlapRegistryService registry) {
      initCount++;
    }

    @Override
    public void newDaemonMetrics(String workerIdentity, LlapMetricsCollector.LlapMetrics newMetrics) {
      assertTrue("Init should be called first", initCount > 0);
      daemonMetricsCount++;
    }

    @Override
    public void newClusterMetrics(Map<String, LlapMetricsCollector.LlapMetrics> newMetrics) {
      assertTrue("Init should be called first", initCount > 0);
      fullMetricsCount++;
    }
  }
}
