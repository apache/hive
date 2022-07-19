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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty.EdgeType;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hive.common.util.Ref;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.SessionNotRunning;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestTezTask {

  DagUtils utils;
  MapWork[] mws;
  ReduceWork[] rws;
  TezWork work;
  TezTask task;
  TezClient session;
  TezSessionState sessionState;
  JobConf conf;
  LocalResource appLr;
  Operator<?> op;
  Path path;
  FileSystem fs;

  @Before
  public void setUp() throws Exception {
    utils = mock(DagUtils.class);
    fs = mock(FileSystem.class);
    path = mock(Path.class);
    when(path.getFileSystem(any())).thenReturn(fs);
    when(utils.getTezDir(any())).thenReturn(path);
    when(
        utils.createVertex(any(), any(BaseWork.class), any(Path.class),
            any(TezWork.class), anyMap())).thenAnswer(
        new Answer<Vertex>() {

          @Override
          public Vertex answer(InvocationOnMock invocation) throws Throwable {
            Object[] args = invocation.getArguments();
            return Vertex.create(((BaseWork)args[1]).getName(),
                mock(ProcessorDescriptor.class), 0, mock(Resource.class));
          }
        });

    when(utils.createEdge(any(), any(Vertex.class), any(Vertex.class),
            any(TezEdgeProperty.class), any(BaseWork.class), any(TezWork.class)))
            .thenAnswer(new Answer<Edge>() {
          @Override
          public Edge answer(InvocationOnMock invocation) throws Throwable {
            Object[] args = invocation.getArguments();
            return Edge.create((Vertex)args[1], (Vertex)args[2], mock(EdgeProperty.class));
          }
        });

    work = new TezWork("", null);

    mws = new MapWork[] { new MapWork(), new MapWork()};
    rws = new ReduceWork[] { new ReduceWork(), new ReduceWork() };

    work.addAll(mws);
    work.addAll(rws);

    int i = 0;
    for (BaseWork w: work.getAllWork()) {
      w.setName("Work "+(++i));
    }

    op = mock(Operator.class);

    Map<String, Operator<? extends OperatorDesc>> map
      = new LinkedHashMap<String,Operator<? extends OperatorDesc>>();
    map.put("foo", op);
    mws[0].setAliasToWork(map);
    mws[1].setAliasToWork(map);

    Map<Path, List<String>> pathMap = new LinkedHashMap<>();
    List<String> aliasList = new ArrayList<String>();
    aliasList.add("foo");
    pathMap.put(new Path("foo"), aliasList);

    mws[0].setPathToAliases(pathMap);
    mws[1].setPathToAliases(pathMap);

    rws[0].setReducer(op);
    rws[1].setReducer(op);

    TezEdgeProperty edgeProp = new TezEdgeProperty(EdgeType.SIMPLE_EDGE);
    work.connect(mws[0], rws[0], edgeProp);
    work.connect(mws[1], rws[0], edgeProp);
    work.connect(rws[0], rws[1], edgeProp);

    task = new TezTask(utils);
    task.setWork(work);
    task.setConsole(mock(LogHelper.class));
    QueryPlan mockQueryPlan = mock(QueryPlan.class);
    doReturn(UUID.randomUUID().toString()).when(mockQueryPlan).getQueryId();
    task.setQueryPlan(mockQueryPlan);

    conf = new JobConf();
    appLr = createResource("foo.jar");

    HiveConf hiveConf = new HiveConf();
    hiveConf
        .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
            "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    SessionState.start(hiveConf);
    session = mock(TezClient.class);
    sessionState = mock(TezSessionState.class);
    when(sessionState.getSession()).thenReturn(session);
    when(sessionState.reopen()).thenReturn(sessionState);
    when(session.submitDAG(any(DAG.class)))
      .thenThrow(new SessionNotRunning(""))
      .thenReturn(mock(DAGClient.class));
  }

  @After
  public void tearDown() throws Exception {
    SessionState.get().close();
    utils = null;
    work = null;
    task = null;
    path = null;
    fs = null;
  }

  @Test
  public void testBuildDag() throws Exception {
    DAG dag = task.build(conf, work, path, new Context(conf),
        DagUtils.createTezLrMap(appLr, null));
    for (BaseWork w: work.getAllWork()) {
      Vertex v = dag.getVertex(w.getName());
      assertNotNull(v);
      List<Vertex> outs = v.getOutputVertices();
      for (BaseWork x: work.getChildren(w)) {
        boolean found = false;
        for (Vertex u: outs) {
          if (u.getName().equals(x.getName())) {
            found = true;
            break;
          }
        }
        assertTrue(found);
      }
    }
  }

  @Test
  public void testEmptyWork() throws Exception {
    DAG dag = task.build(conf, new TezWork("", null), path, new Context(conf),
        DagUtils.createTezLrMap(appLr, null));
    assertEquals(dag.getVertices().size(), 0);
  }

  @Test
  public void testSubmit() throws Exception {
    DAG dag = DAG.create("test");
    task.submit(dag, Ref.from(sessionState));
    // validate close/reopen
    verify(sessionState, times(1)).reopen();
    verify(session, times(2)).submitDAG(any(DAG.class));
  }

  @Test
  public void testSubmitOnNonPoolSession() throws Exception {
    DAG dag = DAG.create("test");

    // Destroy session incase of non-pool tez session
    TezSessionState tezSessionState = mock(TezSessionState.class);
    TezClient tezClient = mock(TezClient.class);
    when(tezSessionState.reopen()).thenThrow(new HiveException("Dag cannot be submitted"));
    when(tezSessionState.getSession()).thenReturn(tezClient);
    when(tezClient.submitDAG(any(DAG.class))).thenThrow(new SessionNotRunning(""));
    doNothing().when(tezSessionState).destroy();
    boolean isException = false;
    try {
      task.submit(dag, Ref.from(tezSessionState));
    } catch (Exception e) {
      isException = true;
      verify(tezClient, times(1)).submitDAG(any(DAG.class));
      verify(tezSessionState, times(2)).reopen();
      verify(tezSessionState, times(1)).destroy();
      verify(tezSessionState, times(0)).returnToSessionManager();
    }
    assertTrue(isException);
  }

  @Test
  public void testSubmitOnPoolSession() throws Exception {
    DAG dag = DAG.create("test");
    // Move session to TezSessionPool, reopen will handle it
    SampleTezSessionState tezSessionPoolSession = mock(SampleTezSessionState.class);
    TezClient tezClient = mock(TezClient.class);
    when(tezSessionPoolSession.reopen()).thenThrow(new HiveException("Dag cannot be submitted"));
    doNothing().when(tezSessionPoolSession).returnToSessionManager();
    when(tezSessionPoolSession.getSession()).thenReturn(tezClient);
    when(tezSessionPoolSession.isDefault()).thenReturn(true);
    when(tezClient.submitDAG(any(DAG.class))).thenThrow(new SessionNotRunning(""));
    boolean isException = false;
    try {
      task.submit(dag, Ref.from(tezSessionPoolSession));
    } catch (Exception e) {
      isException = true;
      verify(tezClient, times(1)).submitDAG(any(DAG.class));
      verify(tezSessionPoolSession, times(2)).reopen();
      verify(tezSessionPoolSession, times(0)).destroy();
      verify(tezSessionPoolSession, times(1)).returnToSessionManager();
    }
    assertTrue(isException);
  }

  @Test
  public void testClose() throws HiveException {
    task.close(work, 0, null);
    verify(op, times(4)).jobClose(any(), eq(true));
  }

  @Test
  public void testExistingSessionGetsStorageHandlerResources() throws Exception {
    final String jarFilePath = "file:///tmp/foo.jar";
    final String[] inputOutputJars = new String[] {jarFilePath};
    LocalResource res = createResource(inputOutputJars[0]);
    final Map<String, LocalResource> resources = Collections.singletonMap(jarFilePath, res);

    when(utils.localizeTempFiles(anyString(), any(), eq(inputOutputJars),
        any(String[].class))).thenReturn(resources);
    when(sessionState.isOpen()).thenReturn(true);
    when(sessionState.isOpening()).thenReturn(false);
    task.ensureSessionHasResources(sessionState, inputOutputJars);
    // TODO: ideally we should have a test for session itself.
    verify(sessionState).ensureLocalResources(any(), eq(inputOutputJars));
  }

  private static LocalResource createResource(String url) {
    LocalResource res = mock(LocalResource.class);
    when(res.getResource()).thenReturn(URL.fromPath(new Path(url)));
    return res;
  }

  @Test
  public void testParseRightmostXmx() throws Exception {
    // Empty java opts
    String javaOpts = "";
    long heapSize = DagUtils.parseRightmostXmx(javaOpts);
    assertEquals("Unexpected maximum heap size", -1, heapSize);

    // Non-empty java opts without -Xmx specified
    javaOpts = "-Xms1024m";
    heapSize = DagUtils.parseRightmostXmx(javaOpts);
    assertEquals("Unexpected maximum heap size", -1, heapSize);

    // Non-empty java opts with -Xmx specified in GB
    javaOpts = "-Xms1024m -Xmx2g";
    heapSize = DagUtils.parseRightmostXmx(javaOpts);
    assertEquals("Unexpected maximum heap size", 2147483648L, heapSize);

    // Non-empty java opts with -Xmx specified in MB
    javaOpts = "-Xms1024m -Xmx1024m";
    heapSize = DagUtils.parseRightmostXmx(javaOpts);
    assertEquals("Unexpected maximum heap size", 1073741824, heapSize);

    // Non-empty java opts with -Xmx specified in KB
    javaOpts = "-Xms1024m -Xmx524288k";
    heapSize = DagUtils.parseRightmostXmx(javaOpts);
    assertEquals("Unexpected maximum heap size", 536870912, heapSize);

    // Non-empty java opts with -Xmx specified in B
    javaOpts = "-Xms1024m -Xmx1610612736";
    heapSize = DagUtils.parseRightmostXmx(javaOpts);
    assertEquals("Unexpected maximum heap size", 1610612736, heapSize);

    // Non-empty java opts with -Xmx specified twice
    javaOpts = "-Xmx1024m -Xmx1536m";
    heapSize = DagUtils.parseRightmostXmx(javaOpts);
    assertEquals("Unexpected maximum heap size", 1610612736, heapSize);

    // Non-empty java opts with bad -Xmx specification
    javaOpts = "pre-Xmx1024m";
    heapSize = DagUtils.parseRightmostXmx(javaOpts);
    assertEquals("Unexpected maximum heap size", -1, heapSize);

    // Non-empty java opts with bad -Xmx specification
    javaOpts = "-Xmx1024m-post";
    heapSize = DagUtils.parseRightmostXmx(javaOpts);
    assertEquals("Unexpected maximum heap size", -1, heapSize);
  }

  @Test
  public void tezTask_updates_Metrics() throws IOException {

    Metrics mockMetrics = Mockito.mock(Metrics.class);

    TezTask tezTask = new TezTask();
    tezTask.updateTaskMetrics(mockMetrics);

    verify(mockMetrics, times(1)).incrementCounter(MetricsConstant.HIVE_TEZ_TASKS);
    verify(mockMetrics, never()).incrementCounter(MetricsConstant.HIVE_MR_TASKS);
  }
}
