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

import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.VertexStatus;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestTezProgressMonitor {

  private static final String REDUCER = "Reducer";
  private static final String MAPPER = "Mapper";
  @Mock
  private DAGClient dagClient;
  @Mock
  private SessionState.LogHelper console;
  @Mock
  private DAGStatus dagStatus;
  @Mock
  private Progress mapperProgress;
  @Mock
  private Progress reducerProgress;
  @Mock
  private VertexStatus succeeded;
  @Mock
  private VertexStatus running;

  private Map<String, Progress> progressMap() {
    return new HashMap<String, Progress>() {{
      put(MAPPER, setup(mapperProgress, 2, 1, 3, 4, 5));
      put(REDUCER, setup(reducerProgress, 3, 2, 1, 0, 1));
    }};
  }

  private Progress setup(Progress progressMock, int total, int succeeded, int failedAttempt,
      int killedAttempt, int running) {
    when(progressMock.getTotalTaskCount()).thenReturn(total);
    when(progressMock.getSucceededTaskCount()).thenReturn(succeeded);
    when(progressMock.getFailedTaskAttemptCount()).thenReturn(failedAttempt);
    when(progressMock.getKilledTaskAttemptCount()).thenReturn(killedAttempt);
    when(progressMock.getRunningTaskCount()).thenReturn(running);
    return progressMock;
  }

  @Test
  public void setupInternalStateOnObjectCreation() throws IOException, TezException {
    when(dagStatus.getState()).thenReturn(DAGStatus.State.RUNNING);
    when(dagClient.getVertexStatus(eq(MAPPER), any())).thenReturn(succeeded);
    when(dagClient.getVertexStatus(eq(REDUCER), any())).thenReturn(running);

    TezProgressMonitor monitor =
        new TezProgressMonitor(dagClient, dagStatus, new ArrayList<BaseWork>(), progressMap(), console,
            Long.MAX_VALUE);

    verify(dagClient).getVertexStatus(eq(MAPPER), isNull());
    verify(dagClient).getVertexStatus(eq(REDUCER), isNull());
    verifyNoMoreInteractions(dagClient);

    assertThat(monitor.vertexStatusMap.keySet(), hasItems(MAPPER, REDUCER));
    assertThat(monitor.vertexStatusMap.get(MAPPER), is(sameInstance(succeeded)));
    assertThat(monitor.vertexStatusMap.get(REDUCER), is(sameInstance(running)));

    assertThat(monitor.progressCountsMap.keySet(), hasItems(MAPPER, REDUCER));
    TezProgressMonitor.VertexProgress expectedMapperState =
        new TezProgressMonitor.VertexProgress(2, 1, 3, 4, 5, DAGStatus.State.RUNNING);
    assertThat(monitor.progressCountsMap.get(MAPPER), is(equalTo(expectedMapperState)));

    TezProgressMonitor.VertexProgress expectedReducerState =
        new TezProgressMonitor.VertexProgress(3, 2, 1, 0, 1, DAGStatus.State.RUNNING);
    assertThat(monitor.progressCountsMap.get(REDUCER), is(equalTo(expectedReducerState)));


  }

}