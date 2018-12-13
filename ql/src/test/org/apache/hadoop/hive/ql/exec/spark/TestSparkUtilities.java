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
package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManager;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the SparkUtilities class.
 */
public class TestSparkUtilities {

  @Test
  public void testGetSparkSessionUsingMultipleThreadsWithTheSameSession() throws HiveException, InterruptedException {

    // The only real state required from SessionState
    final AtomicReference<SparkSession> activeSparkSession = new AtomicReference<>();

    // Mocks
    HiveConf mockConf = mock(HiveConf.class);

    SparkSessionManager mockSessionManager = mock(SparkSessionManager.class);
    doAnswer(invocationOnMock -> {
      SparkSession sparkSession = invocationOnMock.getArgumentAt(0, SparkSession.class);
      if (sparkSession == null) {
        return mock(SparkSession.class);
      } else {
        return sparkSession;
      }
    }).when(mockSessionManager).getSession(any(SparkSession.class), eq(mockConf), eq(true));

    SessionState mockSessionState = mock(SessionState.class);
    when(mockSessionState.getConf()).thenReturn(mockConf);
    doAnswer(invocationOnMock -> {
      activeSparkSession.set(invocationOnMock.getArgumentAt(0, SparkSession.class));
      return null;
    }).when(mockSessionState).setSparkSession(any(SparkSession.class));
    doAnswer(invocationOnMock ->
      activeSparkSession.get()
    ).when(mockSessionState).getSparkSession();

    // When
    List<Callable<SparkSession>> callables = new ArrayList<>();
    callables.add(new GetSparkSessionTester(mockConf, mockSessionManager, mockSessionState));
    callables.add(new GetSparkSessionTester(mockConf, mockSessionManager, mockSessionState));
    callables.add(new GetSparkSessionTester(mockConf, mockSessionManager, mockSessionState));

    ExecutorService executorService = Executors.newFixedThreadPool(callables.size());
    List<Future<SparkSession>> results = executorService.invokeAll(callables);

    // Then
    results.stream().map(f -> resolve(f)).forEach(ss -> assertEquals(ss, activeSparkSession.get()));

  }

  private SparkSession resolve(Future<SparkSession> future) {
    try {
      return future.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private class GetSparkSessionTester implements Callable<SparkSession> {

    private HiveConf hiveConf;
    private SparkSessionManager sparkSessionManager;
    private SessionState sessionState;

    GetSparkSessionTester(HiveConf hiveConf, SparkSessionManager sparkSessionManager,
                                 SessionState sessionState) {
      this.hiveConf = hiveConf;
      this.sparkSessionManager = sparkSessionManager;
      this.sessionState = sessionState;
    }

    @Override
    public SparkSession call() throws Exception {
      SessionState.setCurrentSessionState(sessionState);
      return SparkUtilities.getSparkSession(hiveConf, sparkSessionManager);
    }
  }
}
