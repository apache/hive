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
package org.apache.hadoop.hive.ql.exec.tez;

import com.google.common.util.concurrent.AtomicDouble;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestTezSessionPoolManagerMetrics {
  private static final int NO_OF_SESSIONS = 2;
  private static final double NO_OF_RUNNING_TASKS_PER_SESSION = 10.0;
  private static final double NO_OF_PENDING_TASKS_PER_SESSION = 100.0;

  /**
   * This method tests the core metric calculation logic (refreshMetrics)
   * without involving the actual metric collection process.
   */
  @Test
  public void testRefreshMetrics(){
    TezSessionPoolManager poolManager = mock(TezSessionPoolManager.class);
    List<TezSessionState> sessions = testSessions();
    when(poolManager.getSessions()).thenReturn(sessions);

    TezSessionPoolManagerMetrics metrics = new TezSessionPoolManagerMetrics(poolManager);
    metrics.refreshMetrics(new AtomicDouble(9.0), new AtomicDouble(1.0));

    Assert.assertEquals(9.0, metrics.runningTasksCount.value, 0.0001);
    Assert.assertEquals(1.0, metrics.pendingTasksCount.value, 0.0001);
    Assert.assertEquals(0.1, metrics.taskBacklogRatio.value, 0.0001);
  }

  @Test
  public void testBasicMetricsUpdate() {
    TezSessionPoolManager poolManager = mock(TezSessionPoolManager.class);
    List<TezSessionState> sessions = testSessions();
    when(poolManager.getSessions()).thenReturn(sessions);

    TezSessionPoolManagerMetrics metrics = new TezSessionPoolManagerMetrics(poolManager);
    metrics.collectMetrics();

    double expectedRunningTasks = NO_OF_SESSIONS * NO_OF_RUNNING_TASKS_PER_SESSION;
    double expectedPendingTasks = NO_OF_SESSIONS * NO_OF_PENDING_TASKS_PER_SESSION;
    double expectedBacklog = expectedPendingTasks / (expectedRunningTasks + 1);

    Assert.assertEquals(expectedRunningTasks, metrics.runningTasksCount.value, 0.0001);
    Assert.assertEquals(expectedPendingTasks, metrics.pendingTasksCount.value, 0.0001);
    Assert.assertEquals(expectedBacklog, metrics.taskBacklogRatio.value, 0.0001);
  }

  @Test
  public void testEmptyMetrics(){
    TezSessionPoolManager poolManager = mock(TezSessionPoolManager.class);
    when(poolManager.getSessions()).thenReturn(new ArrayList<>()); // no open sessions
    TezSessionPoolManagerMetrics metrics = new TezSessionPoolManagerMetrics(poolManager);
    metrics.collectMetrics();

    Assert.assertEquals(0.0, metrics.runningTasksCount.value, 0.0001);
    Assert.assertEquals(0.0, metrics.pendingTasksCount.value, 0.0001);
    Assert.assertEquals(0.0, metrics.taskBacklogRatio.value, 0.0001);
  }

  @Test
  public void testMetricsBeforeFirstUpdateAndStart(){
    TezSessionPoolManager poolManager = mock(TezSessionPoolManager.class);
    TezSessionPoolManagerMetrics metrics = new TezSessionPoolManagerMetrics(poolManager);

    Assert.assertEquals(0.0, metrics.runningTasksCount.value, 0.0001);
    Assert.assertEquals(0.0, metrics.pendingTasksCount.value, 0.0001);
    Assert.assertEquals(0.0, metrics.taskBacklogRatio.value, 0.0001);
  }

  private List<TezSessionState> testSessions() {
    List<TezSessionState> sessions = new ArrayList<>();
    TezSessionState session = mock(TezSessionState.class);
    when(session.getMetrics()).thenReturn(testMetrics());
    IntStream.range(0, NO_OF_SESSIONS).forEach(i -> sessions.add(session));
    return sessions;
  }

  private Map<String, Double> testMetrics() {
    Map<String, Double> metrics = new HashMap<>();
    metrics.put(TezSessionPoolManagerMetrics.TEZ_SESSION_METRIC_RUNNING_TASKS, NO_OF_RUNNING_TASKS_PER_SESSION);
    metrics.put(TezSessionPoolManagerMetrics.TEZ_SESSION_METRIC_PENDING_TASKS, NO_OF_PENDING_TASKS_PER_SESSION);
    return metrics;
  }
}
