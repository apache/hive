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

package org.apache.hadoop.hive.ql.exec.tez.monitoring;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.tez.TezSession;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.client.DAGClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test cases for TezJobMonitor queue metrics initialization.
 */
public class TestTezJobMonitorQueueMetrics {

  @Mock private TezSession mockSession;
  @Mock private DAGClient mockDagClient;
  @Mock private DAG mockDag;
  @Mock private Context mockContext;
  @Mock private PerfLogger mockPerfLogger;
  @Mock private YarnClient mockYarnClient;
  @Mock private TezCounters mockCounters;

  private HiveConf hiveConf;
  private List<BaseWork> topSortedWorks;
  private SessionState sessionState;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    hiveConf = new HiveConfForTest(TestTezJobMonitorQueueMetrics.class);
    hiveConf.set("hive.security.authorization.manager",
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdConfOnlyAuthorizerFactory");
    sessionState = SessionState.start(hiveConf);
    topSortedWorks = new ArrayList<>();
    when(mockDag.getName()).thenReturn("test-dag-1");
  }

  @After
  public void tearDown() throws Exception {
    if (sessionState != null) {
      sessionState.close();
    }
  }

  @Test
  public void testMetricsCollectorDisabledByDefault() throws Exception {
    when(mockSession.getYarnClient()).thenReturn(mockYarnClient);
    when(mockSession.getQueueName()).thenReturn("default");

    TezJobMonitor monitor = new TezJobMonitor(
        mockSession, topSortedWorks, mockDagClient, hiveConf,
        mockDag, mockContext, mockCounters, mockPerfLogger);

    assertNotNull("Monitor should be created", monitor);
    verify(mockSession, never()).getYarnClient();
    verify(mockYarnClient, never()).getQueueInfo(anyString());
  }

  @Test
  public void testMetricsCollectorEnabledWithInterval() {
    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_TEZ_QUEUE_METRICS_REFRESH_INTERVAL,
        10, TimeUnit.SECONDS);

    when(mockSession.getYarnClient()).thenReturn(mockYarnClient);
    when(mockSession.getQueueName()).thenReturn("default");

    TezJobMonitor monitor = new TezJobMonitor(
        mockSession, topSortedWorks, mockDagClient, hiveConf,
        mockDag, mockContext, mockCounters, mockPerfLogger);

    assertNotNull("Monitor should be created", monitor);
    verify(mockSession, atLeastOnce()).getYarnClient();
  }

  @Test
  public void testMetricsCollectorDisabledWithZeroInterval() throws Exception {
    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_TEZ_QUEUE_METRICS_REFRESH_INTERVAL,
        0, TimeUnit.SECONDS);

    when(mockSession.getYarnClient()).thenReturn(mockYarnClient);
    when(mockSession.getQueueName()).thenReturn("default");

    TezJobMonitor monitor = new TezJobMonitor(
        mockSession, topSortedWorks, mockDagClient, hiveConf,
        mockDag, mockContext, mockCounters, mockPerfLogger);

    assertNotNull("Monitor should be created", monitor);
    verify(mockSession, never()).getYarnClient();
    verify(mockYarnClient, never()).getQueueInfo(anyString());
  }

  @Test
  public void testMetricsCollectorDisabledWithNegativeInterval() throws Exception {
    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_TEZ_QUEUE_METRICS_REFRESH_INTERVAL,
        -1, TimeUnit.SECONDS);

    when(mockSession.getYarnClient()).thenReturn(mockYarnClient);
    when(mockSession.getQueueName()).thenReturn("default");

    TezJobMonitor monitor = new TezJobMonitor(
        mockSession, topSortedWorks, mockDagClient, hiveConf,
        mockDag, mockContext, mockCounters, mockPerfLogger);

    assertNotNull("Monitor should be created", monitor);
    verify(mockSession, never()).getYarnClient();
    verify(mockYarnClient, never()).getQueueInfo(anyString());
  }

  @Test
  public void testMetricsCollectorWithSmallInterval() {
    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_TEZ_QUEUE_METRICS_REFRESH_INTERVAL,
        500, TimeUnit.MILLISECONDS);

    when(mockSession.getYarnClient()).thenReturn(mockYarnClient);
    when(mockSession.getQueueName()).thenReturn("default");

    TezJobMonitor monitor = new TezJobMonitor(
        mockSession, topSortedWorks, mockDagClient, hiveConf,
        mockDag, mockContext, mockCounters, mockPerfLogger);

    assertNotNull("Monitor should be created with adjusted interval", monitor);
  }

  @Test
  public void testMetricsCollectorWithCustomQueue() {
    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_TEZ_QUEUE_METRICS_REFRESH_INTERVAL,
        15, TimeUnit.SECONDS);

    when(mockSession.getYarnClient()).thenReturn(mockYarnClient);
    when(mockSession.getQueueName()).thenReturn("production.analytics");

    TezJobMonitor monitor = new TezJobMonitor(
        mockSession, topSortedWorks, mockDagClient, hiveConf,
        mockDag, mockContext, mockCounters, mockPerfLogger);

    verify(mockSession, atLeastOnce()).getQueueName();
    assertNotNull("Monitor should be created with custom queue", monitor);
  }

  /**
   * Replaces three formerly copy-pasted tests:
   * <ul>
   *   <li>testMetricsCollectorWithNullYarnClient</li>
   *   <li>testMetricsCollectorWithNullQueueName</li>
   *   <li>testMetricsCollectorWithEmptyQueueName</li>
   * </ul>
   *
   * All three share identical structure — metrics enabled (10 s interval), one
   * degenerate input injected — so they are expressed as a single {@code @Test}
   * that iterates an inline parameter table instead of three copy-pasted methods
   * (eliminates SonarQube rule java:S5976).
   *
   * <pre>
   * label            | yarnClient  | getQueueName()
   * nullYarnClient   | null        | "default"
   * nullQueueName    | mock        | null
   * blankQueueName   | mock        | "  "
   * </pre>
   */
  @Test
  public void testMetricsCollectorWithDegenerateQueueNameOrYarnClient() {
    Object[][] cases = {
        {"nullYarnClient", null,            "default"},
        {"nullQueueName",  mockYarnClient,  null     },
        {"blankQueueName", mockYarnClient,  "  "     },
    };

    for (Object[] row : cases) {
      String     label      = (String)     row[0];
      YarnClient yarnClient = (YarnClient) row[1];
      String     queueName  = (String)     row[2];

      // Re-initialise mocks so state from the previous row does not leak.
      MockitoAnnotations.openMocks(this);
      when(mockDag.getName()).thenReturn("test-dag-1");

      hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_TEZ_QUEUE_METRICS_REFRESH_INTERVAL,
          10, TimeUnit.SECONDS);
      when(mockSession.getYarnClient()).thenReturn(yarnClient);
      when(mockSession.getQueueName()).thenReturn(queueName);

      TezJobMonitor monitor = new TezJobMonitor(
          mockSession, topSortedWorks, mockDagClient, hiveConf,
          mockDag, mockContext, mockCounters, mockPerfLogger);

      assertNotNull("Monitor must be created for case [" + label + "]", monitor);
      // With a positive interval the code must always reach the YarnClient gate.
      verify(mockSession, atLeastOnce()).getYarnClient();
      // When a non-null YarnClient is present, getQueueName() must also be called.
      if (yarnClient != null) {
        verify(mockSession, atLeastOnce()).getQueueName();
      }
    }
  }
}
