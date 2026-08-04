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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.exec.tez.monitoring;

import org.apache.hadoop.hive.ql.exec.tez.monitoring.yarnqueue.NoOpQueueMetricsCollector;
import org.apache.hadoop.hive.ql.exec.tez.monitoring.yarnqueue.QueueMetricsCollector;
import org.apache.hadoop.hive.ql.exec.tez.monitoring.yarnqueue.YarnQueueMetricsCollector;

import java.lang.reflect.Field;

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
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test cases for TezJobMonitor queue metrics initialization.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestTezJobMonitorQueueMetrics {

  @Mock
  private TezSession mockSession;
  @Mock
  private DAGClient mockDagClient;
  @Mock
  private DAG mockDag;
  @Mock
  private Context mockContext;
  @Mock
  private PerfLogger mockPerfLogger;
  @Mock
  private YarnClient mockYarnClient;
  @Mock
  private TezCounters mockCounters;

  private HiveConf hiveConf;
  private List<BaseWork> topSortedWorks;
  private SessionState sessionState;

  @Before
  public void setUp() {
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

    new TezJobMonitor(mockSession, topSortedWorks, mockDagClient, hiveConf, mockDag, mockContext, mockCounters,
        mockPerfLogger);

    // When metrics are disabled (interval=0), getYarnClient() is never called because
    // the check happens before attempting to retrieve the YarnClient
    verify(mockSession, never()).getYarnClient();
    verify(mockYarnClient, never()).getQueueInfo(anyString());
  }

  @Test
  public void testMetricsCollectorEnabledWithInterval() {
    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_TEZ_QUEUE_METRICS_REFRESH_INTERVAL, 10, TimeUnit.SECONDS);

    when(mockSession.getYarnClient()).thenReturn(mockYarnClient);
    when(mockSession.getQueueName()).thenReturn("default");

    new TezJobMonitor(mockSession, topSortedWorks, mockDagClient, hiveConf, mockDag, mockContext, mockCounters,
        mockPerfLogger);

    verify(mockSession, atLeastOnce()).getYarnClient();
  }

  @Test
  public void testMetricsCollectorDisabledWithZeroInterval() throws Exception {
    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_TEZ_QUEUE_METRICS_REFRESH_INTERVAL, 0, TimeUnit.SECONDS);

    new TezJobMonitor(mockSession, topSortedWorks, mockDagClient, hiveConf, mockDag, mockContext, mockCounters,
        mockPerfLogger);

    // When metrics are disabled (interval=0), getYarnClient() is never called
    verify(mockSession, never()).getYarnClient();
    verify(mockYarnClient, never()).getQueueInfo(anyString());
  }

  @Test
  public void testMetricsCollectorDisabledWithNegativeInterval() throws Exception {
    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_TEZ_QUEUE_METRICS_REFRESH_INTERVAL, -1, TimeUnit.SECONDS);

    new TezJobMonitor(mockSession, topSortedWorks, mockDagClient, hiveConf, mockDag, mockContext, mockCounters,
        mockPerfLogger);

    // When metrics are disabled (interval<0), getYarnClient() is never called
    verify(mockSession, never()).getYarnClient();
    verify(mockYarnClient, never()).getQueueInfo(anyString());
  }


  @Test
  public void testMetricsCollectorWithCustomQueue() {
    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_TEZ_QUEUE_METRICS_REFRESH_INTERVAL, 15, TimeUnit.SECONDS);

    when(mockSession.getYarnClient()).thenReturn(mockYarnClient);
    when(mockSession.getQueueName()).thenReturn("production.analytics");

    new TezJobMonitor(mockSession, topSortedWorks, mockDagClient, hiveConf, mockDag, mockContext, mockCounters,
        mockPerfLogger);

    verify(mockSession, atLeastOnce()).getQueueName();
  }

  private QueueMetricsCollector getCollector(TezJobMonitor monitor) throws Exception {
    Field collectorField = TezJobMonitor.class.getDeclaredField("metricsCollector");
    collectorField.setAccessible(true);
    return (QueueMetricsCollector) collectorField.get(monitor);
  }

  /**
   * When metrics are enabled but YarnClient is null, the monitor should fall back
   * to NoOpQueueMetricsCollector rather than throwing or attempting collection.
   */
  @Test
  public void testMetricsCollectorWithNullYarnClient() throws Exception {
    hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_TEZ_QUEUE_METRICS_REFRESH_INTERVAL, 10, TimeUnit.SECONDS);
    when(mockSession.getYarnClient()).thenReturn(null);

    TezJobMonitor monitor =
        new TezJobMonitor(mockSession, topSortedWorks, mockDagClient, hiveConf, mockDag, mockContext, mockCounters,
            mockPerfLogger);

    QueueMetricsCollector collector = getCollector(monitor);
    assertEquals("Should fall back to NoOpQueueMetricsCollector when YarnClient is null",
        NoOpQueueMetricsCollector.class, collector.getClass());
    assertFalse("Collector should be disabled when YarnClient is null", collector.isEnabled());
    assertEquals("Queue name should be empty for NoOp collector", "", collector.getQueueName());
  }

  /**
   * When metrics are enabled but queue name is null or blank, the monitor should fall back
   * to the default queue name rather than failing.
   * Tests null, blank ("  "), and explicitly "default" queue names.
   */
  @Test
  public void testMetricsCollectorWithDefaultQueueFallback() throws Exception {
    String[] queueNames = {null, "  ", "default"};
    String[] descriptions = {"null", "blank", "explicit default"};

    for (int i = 0; i < queueNames.length; i++) {
      String queueName = queueNames[i];
      String desc = descriptions[i];

      hiveConf.setTimeVar(HiveConf.ConfVars.HIVE_TEZ_QUEUE_METRICS_REFRESH_INTERVAL, 10, TimeUnit.SECONDS);
      when(mockSession.getYarnClient()).thenReturn(mockYarnClient);
      when(mockSession.getQueueName()).thenReturn(queueName);

      TezJobMonitor monitor =
          new TezJobMonitor(mockSession, topSortedWorks, mockDagClient, hiveConf, mockDag, mockContext, mockCounters,
              mockPerfLogger);

      QueueMetricsCollector collector = getCollector(monitor);
      assertEquals("Should use YarnQueueMetricsCollector with default queue when queue name is " + desc,
          YarnQueueMetricsCollector.class, collector.getClass());
      assertTrue("Collector should be enabled when queue name is " + desc, collector.isEnabled());
      assertEquals("Queue name should default to 'default' when " + desc, "default", collector.getQueueName());
    }
  }

  @Test
  public void testMetricsCollectorTypeWhenDisabled() throws Exception {
    // Default config has interval = 0 (disabled)
    TezJobMonitor monitor =
        new TezJobMonitor(mockSession, topSortedWorks, mockDagClient, hiveConf, mockDag, mockContext, mockCounters,
            mockPerfLogger);

    QueueMetricsCollector collector = getCollector(monitor);
    assertEquals("Should use NoOpQueueMetricsCollector when disabled",
        NoOpQueueMetricsCollector.class, collector.getClass());
    assertFalse("Collector should be disabled", collector.isEnabled());
    assertEquals("Queue name should be empty for NoOp collector", "", collector.getQueueName());
  }
}
