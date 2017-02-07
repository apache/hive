package org.apache.hadoop.hive.ql.exec.tez.monitoring;

import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
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
    when(dagClient.getVertexStatus(eq(MAPPER), anySet())).thenReturn(succeeded);
    when(dagClient.getVertexStatus(eq(REDUCER), anySet())).thenReturn(running);

    TezProgressMonitor monitor =
        new TezProgressMonitor(dagClient, dagStatus, new HashMap<String, BaseWork>(), progressMap(), console,
            Long.MAX_VALUE);

    verify(dagClient).getVertexStatus(eq(MAPPER), isNull(Set.class));
    verify(dagClient).getVertexStatus(eq(REDUCER), isNull(Set.class));
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